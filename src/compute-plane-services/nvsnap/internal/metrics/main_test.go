// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"os"
	"testing"
)

// TestMain reads metrics as soon as they are observed: no first-sight hold
// (first_sight.go), which first_sight_test.go tests on its own.
func TestMain(m *testing.M) {
	SetFirstSightHold(0)
	os.Exit(m.Run())
}
