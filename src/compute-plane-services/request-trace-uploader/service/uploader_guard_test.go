// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// This pipeline submits each segment to one destination and sends it again
// on the next pass whenever it cannot confirm it, so it refuses a backend for
// which either would lose or duplicate data.
func TestNewUploaderRefusesBackendsItCannotServeSafely(t *testing.T) {
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, nil))
	tests := []struct {
		name    string
		caps    backend.Capabilities
		wantErr bool
	}{
		{name: "one destination, resubmit safe", caps: backend.Capabilities{ResubmitSafe: true, Exports: true}},
		{name: "one named destination, resubmit safe", caps: backend.Capabilities{ResubmitSafe: true, Exports: true, Destinations: []backend.Destination{"records"}}},
		{name: "two destinations", caps: backend.Capabilities{ResubmitSafe: true, Exports: true, Destinations: []backend.Destination{"requests", "payloads"}}, wantErr: true},
		{name: "not resubmit safe", caps: backend.Capabilities{TerminalOutcomeSync: true, Exports: true}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := backendtest.NewFake()
			fake.SetCapabilities(tt.caps)
			_, err := service.NewUploader(cfg, fake, service.Options{})
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewUploader() error = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

// A backend error can carry a path, a URL or a job id, so a failed submit is
// logged by its fault class, scope and reason only.
func TestBackendFailuresAreLoggedByClassNotText(t *testing.T) {
	logs := captureLogs(t)
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("a", 1)).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("x", 1)).Bytes())
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, nil))
	fake := resubmitSafeFake()
	fake.Script(backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: errors.New("put https://bucket.example.com/secret-path/job-4242: denied")})
	uploader, err := service.NewUploader(cfg, fake, service.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "secret-path") || strings.Contains(logs.String(), "job-4242") {
		t.Fatalf("logs carry the backend error text:\n%s", logs)
	}
	if !strings.Contains(logs.String(), "class=") {
		t.Fatalf("logs = %s, want the failure logged by class", logs)
	}
}
