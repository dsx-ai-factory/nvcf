// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
)

// fatalRecorder stands in for a test so a Fatalf from a helper can be read.
type fatalRecorder struct {
	testing.TB
	message string
}

func (f *fatalRecorder) Helper() {}

func (f *fatalRecorder) Fatalf(format string, args ...any) {
	f.message = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

// A setting the real startup path refuses must not quietly become its
// default in a test: loadEnv fails the test instead.
func TestLoadEnvFailsOnAValueThatWouldFallBack(t *testing.T) {
	recorder := &fatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		loadEnv(recorder, spoolEnv(t, t.TempDir(), map[string]string{config.EnvScanInterval: "soon"}))
	}()
	<-done
	if recorder.message == "" {
		t.Fatal("loadEnv accepted a value that falls back to its default")
	}
}
