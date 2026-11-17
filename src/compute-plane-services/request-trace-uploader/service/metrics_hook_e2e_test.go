// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// panicMetrics fails on every observation, as a broken exporter can.
type panicMetrics struct{}

func (panicMetrics) Add(string, float64, telemetry.Labels)     { panic("exporter down") }
func (panicMetrics) Set(string, float64, telemetry.Labels)     { panic("exporter down") }
func (panicMetrics) Observe(string, float64, telemetry.Labels) { panic("exporter down") }

// receivingFake is a backend that records its own metrics.
type receivingFake struct {
	*backendtest.Fake
	metrics telemetry.Metrics
}

func (r *receivingFake) UseMetrics(m telemetry.Metrics) { r.metrics = m }

func resubmitSafeCaps() backend.Capabilities {
	caps := backendtest.NewFake().Capabilities()
	caps.ResubmitSafe = true
	return caps
}

func TestNewUploaderHandsABackendTheSharedRecorder(t *testing.T) {
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, nil))
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	client := &receivingFake{Fake: backendtest.NewFake()}
	client.SetCapabilities(resubmitSafeCaps())
	if _, err := service.NewUploader(cfg, client, service.Options{Metrics: metrics}); err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	if client.metrics == nil {
		t.Fatal("the backend got no recorder")
	}
	client.metrics.Add(telemetry.JobStatusUnknown, 1, nil)
	if got, _ := metrics.Value(telemetry.JobStatusUnknown, nil); got != 1 {
		t.Fatalf("job_status_unknown = %v, want the backend's observation recorded", got)
	}
}

// The backend gets the same recorder as the core: a broken exporter costs
// the backend's observations, never a panic in the backend.
func TestABackendRecorderSurvivesABrokenExporter(t *testing.T) {
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, nil))
	client := &receivingFake{Fake: backendtest.NewFake()}
	client.SetCapabilities(resubmitSafeCaps())
	if _, err := service.NewUploader(cfg, client, service.Options{Metrics: panicMetrics{}}); err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	client.metrics.Add(telemetry.JobStatusUnknown, 1, nil)
}
