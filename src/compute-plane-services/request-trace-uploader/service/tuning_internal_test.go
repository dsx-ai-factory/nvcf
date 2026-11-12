// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/batch"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/retry"
)

func loadTuning(t *testing.T, env map[string]string) config.Config {
	t.Helper()
	values := map[string]string{config.EnvSourceDir: t.TempDir(), config.EnvBackend: string(config.BackendDebug)}
	for name, value := range env {
		values[name] = value
	}
	cfg, err := config.LoadStrict(func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}, nil)
	if err != nil {
		t.Fatalf("LoadStrict() error = %v", err)
	}
	return cfg
}

// With no tuning setting, fault handling and batching are exactly what they
// were before the settings existed, and a zero Config built by hand gets
// the same.
func TestUnsetTuningKeepsTheFaultAndBatchDefaults(t *testing.T) {
	for name, cfg := range map[string]config.Config{"loaded": loadTuning(t, nil), "built by hand": {}} {
		t.Run(name, func(t *testing.T) {
			if got := faultConfig(cfg); got != retry.DefaultConfig() {
				t.Fatalf("faults = %+v, want %+v", got, retry.DefaultConfig())
			}
			got, _, err := batchSettings(cfg, backend.Capabilities{MaxBatchFiles: 1000})
			if err != nil || got != batch.DefaultConfig() {
				t.Fatalf("batch = %+v, %v; want %+v", got, err, batch.DefaultConfig())
			}
		})
	}
}

func TestTuningReachesFaultHandlingAndBatching(t *testing.T) {
	cfg := loadTuning(t, map[string]string{
		config.EnvCreatesPerMinute:     "120",
		config.EnvCreateBurst:          "4",
		config.EnvSegmentScopeFailures: "2",
		config.EnvAuthMaxBackoff:       "2h",
		config.EnvRetryAfterMax:        "5m",
		config.EnvBatchMaxSegments:     "8",
		config.EnvBatchMaxBytes:        "1048576",
		config.EnvUnconfirmedResends:   "0",
	})
	want := retry.DefaultConfig()
	want.CreatesPerMinute = 120
	want.CreateBurst = 4
	want.SegmentScopeFailures = 2
	want.Auth.Max = 2 * time.Hour
	want.RetryAfterMax = 5 * time.Minute
	want.UnconfirmedResends = 0
	if got := faultConfig(cfg); got != want {
		t.Fatalf("faults = %+v, want %+v", got, want)
	}
	if err := want.Validate(); err != nil {
		t.Fatalf("tuned faults do not validate: %v", err)
	}
	wantBatch := batch.DefaultConfig()
	wantBatch.MaxSegments = 8
	wantBatch.MaxBytes = 1 << 20
	if got, _, err := batchSettings(cfg, backend.Capabilities{MaxBatchFiles: 1000}); err != nil || got != wantBatch {
		t.Fatalf("batch = %+v, %v; want %+v", got, err, wantBatch)
	}
}

func TestUnsetBatchSettingsFollowTheBackendLimits(t *testing.T) {
	got, limits, err := batchSettings(loadTuning(t, nil), backend.Capabilities{MaxBatchFiles: 10, MaxBatchBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxBytes != 64<<20 || got.MaxFiles != 10 || !limits.Batching {
		t.Fatalf("batch = %+v limits = %+v, want the backend's 64 MiB and 10 files", got, limits)
	}
}
