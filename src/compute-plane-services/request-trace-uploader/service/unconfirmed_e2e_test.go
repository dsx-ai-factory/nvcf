// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// unconfirmedSubmit is a submit the destination could not confirm.
var unconfirmedSubmit = backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{Class: backend.ClassUncertain, Scope: backend.ScopeUnknown, Reason: backend.ReasonUploadUnconfirmed}}

// The limit comes from REQUEST_TRACE_UPLOADER_UNCONFIRMED_RESENDS, with no
// fault handling override, so the test covers the setting end to end.
func TestAnUnconfirmedUploadIsSentAgainUpToTheLimitThenQuarantined(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		resends int
	}{
		{name: "unset keeps the default of two resends", resends: 2},
		{name: "no resends", env: map[string]string{config.EnvUnconfirmedResends: "0"}, resends: 0},
		{name: "four resends", env: map[string]string{config.EnvUnconfirmedResends: "4"}, resends: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
			fake := backendtest.NewFake()
			fake.Script(unconfirmedSubmit, unconfirmedSubmit, unconfirmedSubmit, unconfirmedSubmit, unconfirmedSubmit, unconfirmedSubmit, unconfirmedSubmit)
			metrics, err := telemetry.NewMemory("fake")
			if err != nil {
				t.Fatal(err)
			}
			cfg := loadEnv(t, spoolEnv(t, spool.Dir, with(unpaced, tt.env)))
			uploader, err := service.NewUploader(cfg, fake, service.Options{Metrics: metrics})
			if err != nil {
				t.Fatalf("NewUploader() error = %v", err)
			}
			t.Cleanup(func() { _ = uploader.Close() })
			for range 8 {
				_ = uploader.Pass(context.Background())
			}
			if err := uploader.Close(); err != nil {
				t.Fatal(err)
			}
			if got, want := fake.SubmitCalls(), tt.resends+1; got != want {
				t.Fatalf("submit calls = %d, want %d: the upload and %d resends", got, want, tt.resends)
			}
			if got, _ := metrics.Value(telemetry.SubmitsAmbiguous, nil); got != float64(tt.resends) {
				t.Fatalf("submits_ambiguous = %v, want every resend counted", got)
			}
			if state := journalStates(t, spool)[filepath.Base(closed[0])]; state != journal.StateQuarantined {
				t.Fatalf("segment state = %q, want quarantined after the last resend", state)
			}
		})
	}
}

// An outage that times out or answers 503 is waited out and retried with no
// limit, and the work is exported once the destination recovers.
func TestARunOfTimeoutsKeepsRetryingUntilTheBackendRecovers(t *testing.T) {
	for name, reason := range map[string]backend.Reason{"timeout": backend.ReasonTimeout, "503": backend.ReasonUnavailable} {
		t.Run(name, func(t *testing.T) {
			spool, closed := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
			fake := backendtest.NewFake()
			outage := backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{Class: backend.ClassUncertain, Scope: backend.ScopeBackend, Reason: reason}}
			var steps []backendtest.SubmitStep
			for range 10 {
				steps = append(steps, outage)
			}
			fake.Script(append(steps, backendtest.SubmitStep{})...)
			uploader := newUploader(t, spool, fake, service.Options{}, nil)
			passUntil(t, uploader, func() bool { return gone(closed[0]) })
			if err := uploader.Close(); err != nil {
				t.Fatal(err)
			}
			if got := fake.SubmitCalls(); got != 11 {
				t.Fatalf("submit calls = %d, want ten failures and the export", got)
			}
			if state := journalStates(t, spool)[filepath.Base(closed[0])]; state != journal.StateDone {
				t.Fatalf("segment state = %q, want done: never retained or quarantined", state)
			}
		})
	}
}
