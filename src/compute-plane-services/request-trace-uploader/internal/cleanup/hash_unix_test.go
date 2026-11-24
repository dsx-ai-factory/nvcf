// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package cleanup

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// The default hasher refuses what is not a regular file without waiting on
// it, so a source swapped for a pipe after its type was checked cannot hold
// cleanup.
func TestHashFileRefusesAPipeWithoutBlocking(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "request-trace.000001.jsonl.gz")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	t.Cleanup(func() {
		if w, err := os.OpenFile(fifo, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			w.Close()
		}
	})
	done := make(chan error, 1)
	go func() {
		_, err := hashFile(fifo)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("hashFile() error = nil, want a refusal")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hashFile blocked on a pipe")
	}
}
