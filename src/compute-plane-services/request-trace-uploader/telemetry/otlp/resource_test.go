// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package otlp

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
)

// The deployment names the function and the pod through the standard OTel
// resource variables, so the collector can tell producers apart.
func TestOTLPResourceComesFromTheStandardVariables(t *testing.T) {
	tests := []struct {
		name        string
		attributes  string
		serviceName string
		want        map[string]string
	}{
		{
			name:        "both set",
			attributes:  "nvcf.function.id=fn-1,k8s.pod.name=pod-a",
			serviceName: "uploader-under-test",
			want:        map[string]string{"nvcf.function.id": "fn-1", "k8s.pod.name": "pod-a", "service.name": "uploader-under-test"},
		},
		{
			name:       "attributes only keep the default service name",
			attributes: "k8s.pod.name=pod-b",
			want:       map[string]string{"k8s.pod.name": "pod-b", "service.name": ServiceName},
		},
		{
			name: "unset keeps the default service name",
			want: map[string]string{"service.name": ServiceName},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", tt.attributes)
			t.Setenv("OTEL_SERVICE_NAME", tt.serviceName)
			r, endpoint := httpReceiver(t)
			exporter := newOTLP(t, endpoint, ProtocolHTTP)
			exporter.Add(telemetry.Uploads, 1, telemetry.Labels{"status": "success"})
			flush(t, exporter)
			for key, want := range tt.want {
				if got := r.resourceAttribute(key); got != want {
					t.Errorf("resource %s = %q, want %q", key, got, want)
				}
			}
		})
	}
}

// A malformed OTEL_RESOURCE_ATTRIBUTES costs only the attributes that do not
// parse: the exporter still starts, keeps the rest, and warns without
// printing the value.
func TestOTLPKeepsExportingWithMalformedResourceAttributes(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "k8s.pod.name=pod-c,secret-marker-without-equals")
	t.Setenv("OTEL_SERVICE_NAME", "")

	r, endpoint := httpReceiver(t)
	exporter := newOTLP(t, endpoint, ProtocolHTTP)
	exporter.Add(telemetry.Uploads, 1, telemetry.Labels{"status": "success"})
	flush(t, exporter)
	if got := r.resourceAttribute("k8s.pod.name"); got != "pod-c" {
		t.Errorf("resource k8s.pod.name = %q, want pod-c", got)
	}
	if got := r.resourceAttribute("service.name"); got != ServiceName {
		t.Errorf("resource service.name = %q, want %q", got, ServiceName)
	}
	if !strings.Contains(logs.String(), "OTEL_RESOURCE_ATTRIBUTES") || strings.Contains(logs.String(), "secret-marker") {
		t.Errorf("logs = %s, want a warning that names the variable and not its value", logs.String())
	}
}
