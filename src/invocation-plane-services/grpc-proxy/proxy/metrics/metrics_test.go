/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	otelmetric "go.opentelemetry.io/otel/metric"
)

func TestHTTPServerMetricsIgnoreHost(t *testing.T) {
	registry := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		t.Fatal(err)
	}
	provider := newMeterProvider(exporter)
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})

	// Unrelated metrics must retain their server address labels.
	clientDuration, err := provider.Meter("test").Float64Histogram("http.client.request.duration", otelmetric.WithUnit("s"))
	if err != nil {
		t.Fatal(err)
	}
	clientDuration.Record(context.Background(), 0.01, otelmetric.WithAttributes(attribute.String("server.address", "upstream.example")))
	applicationRequests := prometheus.NewCounter(prometheus.CounterOpts{Name: "application_requests_total", Help: "Application requests."})
	registry.MustRegister(applicationRequests)

	handler := otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
		}
		applicationRequests.Inc()
		_, _ = w.Write([]byte("ok"))
	}), "test", otelhttp.WithMeterProvider(provider))

	hosts := []string{"proxy.example", "proxy.example\xff", "p\xffroxy.example", "PROXY.EXAMPLE\x80", "another.example"}
	for _, host := range hosts {
		req := httptest.NewRequest(http.MethodPost, "http://proxy.example/invoke", strings.NewReader("request"))
		// Set Host directly to exercise instrumentation even for malformed
		// authority values that a front-end HTTP parser might reject.
		req.Host = host
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != http.StatusOK || response.Body.String() != "ok" {
			t.Fatalf("host %q: response = %d %q", host, response.Code, response.Body.String())
		}

		// Scrape after each request: a malformed host must not poison later scrapes.
		scrape := httptest.NewRecorder()
		promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if scrape.Code != http.StatusOK {
			t.Fatalf("host %q: scrape = %d %s", host, scrape.Code, scrape.Body.String())
		}
		if !strings.Contains(scrape.Body.String(), "application_requests_total ") {
			t.Fatal("application metrics missing from scrape")
		}
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"http_server_request_body_size_bytes":  false,
		"http_server_response_body_size_bytes": false,
		"http_server_request_duration_seconds": false,
	}
	clientFound := false
	for _, family := range families {
		if family.GetName() == "http_client_request_duration_seconds" {
			for _, sample := range family.Metric {
				for _, label := range sample.Label {
					if label.GetName() == "server_address" && label.GetValue() == "upstream.example" {
						clientFound = true
					}
				}
			}
		}
		if _, ok := want[family.GetName()]; !ok {
			continue
		}
		want[family.GetName()] = true
		if len(family.Metric) != 1 {
			t.Fatalf("%s: got %d series for %d hosts, want 1", family.GetName(), len(family.Metric), len(hosts))
		}
		sample := family.Metric[0]
		if sample.GetHistogram().GetSampleCount() != uint64(len(hosts)) {
			t.Errorf("%s: request observations lost", family.GetName())
		}
		labels := make(map[string]string)
		for _, label := range sample.Label {
			labels[label.GetName()] = label.GetValue()
		}
		if _, ok := labels["server_address"]; ok {
			t.Errorf("%s: server_address retained", family.GetName())
		}
		if labels["http_request_method"] != "POST" || labels["http_response_status_code"] != "200" {
			t.Errorf("%s: method/status labels lost: %v", family.GetName(), labels)
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("missing metric %s", name)
		}
	}
	if !clientFound {
		t.Error("HTTP client server_address label lost")
	}
}

func TestDefaultHTTPServerMetricsProvider(t *testing.T) {
	const subprocessEnv = "NVCF_TEST_DEFAULT_METRICS_PROVIDER"
	if os.Getenv(subprocessEnv) != "1" {
		// Package initialization runs in a fresh process so this test neither
		// replaces nor pollutes the other tests' global telemetry state.
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(executable, "-test.run=^TestDefaultHTTPServerMetricsProvider$", "-test.v")
		cmd.Env = append(os.Environ(), subprocessEnv+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("default provider subprocess: %v\n%s", err, output)
		}
		return
	}

	// Use the global provider installed by init(), as production does.
	handler := otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write([]byte("ok"))
	}), "test")
	scrapeHandler := promhttp.Handler()
	for _, host := range []string{"proxy.example\xff", "proxy.example"} {
		req := httptest.NewRequest(http.MethodPost, "http://proxy.example/invoke", strings.NewReader("request"))
		req.Host = host
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != http.StatusOK || response.Body.String() != "ok" {
			t.Fatalf("host %q: response = %d %q", host, response.Code, response.Body.String())
		}

		scrape := httptest.NewRecorder()
		scrapeHandler.ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if scrape.Code != http.StatusOK {
			t.Fatalf("host %q: scrape = %d %s", host, scrape.Code, scrape.Body.String())
		}
		for _, name := range []string{
			"http_server_request_body_size_bytes_count{",
			"http_server_response_body_size_bytes_count{",
			"http_server_request_duration_seconds_count{",
			RootNamespace + "_active_connections_total ",
		} {
			if !strings.Contains(scrape.Body.String(), name) {
				t.Errorf("host %q: missing metric %s", host, name)
			}
		}
	}
}
