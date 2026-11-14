// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

type panickingMetrics struct{}

func (panickingMetrics) Add(string, float64, Labels)     { panic("exporter down") }
func (panickingMetrics) Set(string, float64, Labels)     { panic("exporter down") }
func (panickingMetrics) Observe(string, float64, Labels) { panic("exporter down") }

func TestSafeRecoversRecorderPanicsAndWarnsOnce(t *testing.T) {
	var logs bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(original) })

	m := Safe(panickingMetrics{})
	m.Add(SubmitsAmbiguous, 1, nil)
	m.Set(PendingSegments, 1, nil)
	m.Observe(UploadDuration, 1, Labels{"status": "success"})

	if got := strings.Count(logs.String(), "telemetry recorder failed"); got != 1 {
		t.Fatalf("warnings = %d, want 1; logs %q", got, logs.String())
	}
}

func TestSafePassesObservationsThrough(t *testing.T) {
	memory := newMemory(t)
	m := Safe(memory)
	m.Add(SubmitsAmbiguous, 2, nil)
	m.Set(PendingSegments, 3, nil)
	m.Observe(UploadDuration, 4, Labels{"status": "success"})

	if got, _ := memory.Value(SubmitsAmbiguous, nil); got != 2 {
		t.Errorf("submits_ambiguous = %v, want 2", got)
	}
	if got, _ := memory.Value(PendingSegments, nil); got != 3 {
		t.Errorf("pending_segments = %v, want 3", got)
	}
	if count, sum, _ := memory.Histogram(UploadDuration, Labels{"status": "success"}); count != 1 || sum != 4 {
		t.Errorf("upload.duration = %d, %v; want 1, 4", count, sum)
	}
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(original) })
	return &logs
}

// leakyError is a panic value that names a path and an upload id, as a
// broken exporter wrapping its input could.
type leakyError struct{ text string }

func (e leakyError) Error() string { return "export " + e.text }

type leakyMetrics struct{ panickingMetrics }

func (leakyMetrics) Add(string, float64, Labels) {
	panic(leakyError{text: "sentinel-7f3a /spool/request-trace.000042.jsonl.gz"})
}

func TestSafeWarningNeverPrintsThePanicValue(t *testing.T) {
	logs := captureLogs(t)

	Safe(leakyMetrics{}).Add(SubmitsAmbiguous, 1, nil)

	output := logs.String()
	if strings.Contains(output, "sentinel-7f3a") || strings.Contains(output, "/spool/") {
		t.Fatalf("logs = %q, contain the recovered panic value", output)
	}
	if strings.Count(output, "telemetry recorder failed") != 1 || !strings.Contains(output, SubmitsAmbiguous) {
		t.Fatalf("logs = %q, want one warning that names the instrument", output)
	}
	if !strings.Contains(output, `"panic_type":"telemetry.leakyError"`) {
		t.Fatalf("logs = %q, want the panic's Go type", output)
	}
}
