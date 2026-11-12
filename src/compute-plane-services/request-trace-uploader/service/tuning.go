// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"fmt"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/batch"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/retry"
)

// faultConfig is retry.DefaultConfig with every fault setting cfg sets. A
// zero field keeps the default, and so does a nil resend count, for which
// zero is a real value.
func faultConfig(cfg config.Config) retry.Config {
	faults := retry.DefaultConfig()
	setIf(&faults.QuarantineTrip, cfg.QuarantineTrip)
	setIf(&faults.QuarantineResume, cfg.QuarantineResume)
	setIf(&faults.CreatesPerMinute, cfg.Faults.CreatesPerMinute)
	setIf(&faults.CreateBurst, cfg.Faults.CreateBurst)
	setIf(&faults.SegmentScopeFailures, cfg.Faults.SegmentScopeFailures)
	setIf(&faults.Auth.Max, cfg.Faults.AuthMaxBackoff)
	setIf(&faults.RetryAfterMax, cfg.Faults.RetryAfterMax)
	if resends := cfg.Faults.UnconfirmedResends; resends != nil {
		faults.UnconfirmedResends = *resends
	}
	return faults
}

// batchSettings is batch.DefaultConfig with the batch settings cfg sets,
// under the backend's limits, and those limits. A default above a backend
// limit is lowered to it, as before the settings existed; a set value above
// one stops startup, because the operator asked for batches the backend
// cannot take.
func batchSettings(cfg config.Config, caps backend.Capabilities) (batch.Config, batch.Limits, error) {
	limits := batch.Limits{
		Batching: caps.BatchFiles() > 1,
		MaxBytes: caps.MaxBatchBytes,
		MaxFiles: caps.BatchFiles(),
	}
	if limits.MaxBytes > 0 && cfg.Batch.MaxBytes > limits.MaxBytes {
		return batch.Config{}, batch.Limits{}, fmt.Errorf("%s must be at most the backend's batch limit of %d bytes", config.EnvBatchMaxBytes, limits.MaxBytes)
	}
	if cfg.Batch.MaxSegments > limits.MaxFiles {
		return batch.Config{}, batch.Limits{}, fmt.Errorf("%s must be at most the backend's limit of %d files per job", config.EnvBatchMaxSegments, limits.MaxFiles)
	}
	settings := batch.DefaultConfig()
	setIf(&settings.MaxSegments, cfg.Batch.MaxSegments)
	setIf(&settings.MaxBytes, cfg.Batch.MaxBytes)
	resolved, _ := batch.Resolve(settings, limits)
	return resolved, limits, nil
}

func setIf[T comparable](field *T, value T) {
	var zero T
	if value != zero {
		*field = value
	}
}
