// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package record

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestOpenLinesRefusesWhatIsNotARegularFile(t *testing.T) {
	directory := t.TempDir()
	target := writeRaw(t, member(t, "one"))
	link := filepath.Join(directory, "request-trace.000001.jsonl.gz")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(directory, "request-trace.000002.jsonl.gz")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	for _, path := range []string{link, fifo} {
		done := make(chan error, 1)
		go func() {
			lines, err := OpenLines(path, LineOptions{})
			if err == nil {
				lines.Close()
			}
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Errorf("OpenLines(%s) error = nil, want a refusal", filepath.Base(path))
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("OpenLines(%s) blocked", filepath.Base(path))
		}
	}
}
