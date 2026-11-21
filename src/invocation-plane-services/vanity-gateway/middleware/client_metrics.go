/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

package middleware

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Bounded values of the gateway_call_kind client metric label.
const (
	CallKindInference   = "inference"
	CallKindPoll        = "poll"
	CallKindHealth      = "health"
	CallKindModelAccess = "model_access"
)

// responseCompleteMetricName is the instrument name. The Prometheus exporter
// appends the unit, so it is exported as
// nvcf_vanity_gateway_upstream_response_complete_seconds.
const responseCompleteMetricName = "nvcf_vanity_gateway_upstream_response_complete"

type callKindContextKey struct{}

// ContextWithCallKind marks an outbound call for client metrics. Only
// CallKindPoll, CallKindHealth, and CallKindModelAccess change the label; any
// other value is recorded as CallKindInference.
func ContextWithCallKind(ctx context.Context, kind string) context.Context {
	return context.WithValue(ctx, callKindContextKey{}, kind)
}

func clientMetricContext(ctx context.Context) context.Context {
	// A server labeler may be inherited through the proxy. Copy its attributes
	// into a fresh labeler so results from this outbound call cannot leak back
	// into server metrics, shadow replays, or another call using the same context.
	labeler := &otelhttp.Labeler{}
	if parent, ok := otelhttp.LabelerFromContext(ctx); ok {
		labeler.Add(parent.Get()...)
	}
	kind := CallKindInference
	switch marked, _ := ctx.Value(callKindContextKey{}).(string); marked {
	case CallKindPoll, CallKindHealth, CallKindModelAccess:
		kind = marked
	}
	labeler.Add(attribute.String("gateway_call_kind", kind))
	return otelhttp.ContextWithLabeler(ctx, labeler)
}

// responseCompleteHistogramBoundaries are the buckets of the response complete
// histogram. They add finer edges (0.05, 1.5, 2.5 and 3 seconds) to the shared
// HTTP duration buckets so SLO thresholds such as a 1.5s p99 can be expressed
// without changing the existing HTTP duration histograms.
var responseCompleteHistogramBoundaries = []float64{
	0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 2.5,
	3, 5, 10, 15, 30, 60, 120, 300, 600, 900,
}

// newResponseCompleteHistogram creates the histogram that measures the time
// from sending the upstream request to the end of the response body. It
// returns nil if the instrument cannot be created, which disables recording.
func newResponseCompleteHistogram(mp metric.MeterProvider) metric.Float64Histogram {
	histogram, err := mp.Meter("vanity-gateway/middleware").Float64Histogram(
		responseCompleteMetricName,
		metric.WithUnit("s"),
		metric.WithDescription("Time from sending the upstream request to the end of the response body."),
		metric.WithExplicitBucketBoundaries(responseCompleteHistogramBoundaries...),
	)
	if err != nil {
		otel.Handle(err)
		return nil
	}
	return histogram
}

type clientMetricsRoundTripper struct {
	http.RoundTripper
	// complete records the duration to end of body. It may be nil.
	complete metric.Float64Histogram
}

func (rt clientMetricsRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	start := time.Now()
	response, err := rt.RoundTripper.RoundTrip(r)
	labeler, _ := otelhttp.LabelerFromContext(r.Context())
	if err != nil {
		kind := "transport_error"
		var networkError net.Error
		switch {
		case errors.Is(err, context.Canceled), errors.Is(r.Context().Err(), context.Canceled):
			kind = "canceled"
		case errors.Is(err, context.DeadlineExceeded), errors.Is(r.Context().Err(), context.DeadlineExceeded), errors.As(err, &networkError) && networkError.Timeout():
			kind = "timeout"
		}
		labeler.Add(attribute.String("error.type", kind))
	} else if response != nil && (response.StatusCode < 200 || response.StatusCode >= 300) {
		source := response.Header.Get(ErrorSourceHeader)
		switch source {
		case ErrorSourceInvocationService, ErrorSourceWorker:
		default:
			source = "unknown"
		}
		labeler.Add(attribute.String("error_source", source))
	}

	// otelhttp records http.client.request.duration when headers arrive, so
	// streamed bodies are invisible to it. Record the end-of-body duration
	// separately when the body reaches EOF or is closed.
	if rt.complete != nil && err == nil && response != nil && response.Body != nil {
		response.Body = &completeTimingBody{
			ReadCloser: response.Body,
			record: func() {
				rt.complete.Record(r.Context(), time.Since(start).Seconds(),
					metric.WithAttributes(completeAttributes(r, response, labeler)...))
			},
		}
	}
	return response, err
}

// completeTimingBody calls record once, on io.EOF or on Close, whichever comes
// first. Other read errors, such as io.ErrUnexpectedEOF, do not record so a
// later Close still can.
type completeTimingBody struct {
	io.ReadCloser
	once   sync.Once
	record func()
}

func (b *completeTimingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.once.Do(b.record)
	}
	return n, err
}

func (b *completeTimingBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.record)
	return err
}

// completeAttributes mirrors the labels otelhttp puts on
// http.client.request.duration so both series can be compared per label set.
func completeAttributes(r *http.Request, response *http.Response, labeler *otelhttp.Labeler) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("http.request.method", r.Method),
		attribute.String("url.scheme", r.URL.Scheme),
		attribute.String("server.address", r.URL.Hostname()),
		attribute.Int("http.response.status_code", response.StatusCode),
	}
	if port := r.URL.Port(); port != "" {
		if n, err := strconv.Atoi(port); err == nil && !isDefaultPort(r.URL.Scheme, n) {
			attrs = append(attrs, attribute.Int("server.port", n))
		}
	}
	if version, ok := strings.CutPrefix(r.Proto, "HTTP/"); ok && version != "" {
		attrs = append(attrs,
			attribute.String("network.protocol.name", "http"),
			attribute.String("network.protocol.version", version),
		)
	}
	attrs = append(attrs, shadowMetricAttributes(r)...)
	if labeler != nil {
		attrs = append(attrs, labeler.Get()...)
	}
	return attrs
}

// isDefaultPort reports whether port is the default for the URL scheme.
// otelhttp omits server.port in that case, so the response-complete
// histogram does the same to keep both label sets identical.
func isDefaultPort(scheme string, port int) bool {
	return (scheme == "https" && port == 443) || (scheme == "http" && port == 80)
}
