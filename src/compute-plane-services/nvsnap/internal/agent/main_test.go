// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"os"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
)

// TestMain reads metrics as soon as they are observed: no first-sight hold
// (first_sight.go), which first_sight_test.go tests on its own.
func TestMain(m *testing.M) {
	metrics.SetFirstSightHold(0)
	os.Exit(m.Run())
}
