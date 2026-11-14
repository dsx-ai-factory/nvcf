// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package service

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/segment"
)

// A discovered segment can be swapped before it is fingerprinted. The first
// read must refuse what is not a regular file, without following a symlink or
// waiting on a pipe.
func TestFingerprintRefusesWhatIsNotARegularFile(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(target, []byte("not ours"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "request-trace.000001.jsonl.gz")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(directory, "request-trace.000002.jsonl.gz")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	u := &Uploader{fingerprints: map[fileIdentity]string{}}
	for _, path := range []string{link, fifo} {
		done := make(chan error, 1)
		go func() {
			_, err := u.fingerprint(segment.Segment{Path: path})
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Errorf("fingerprint(%s) error = nil, want a refusal", filepath.Base(path))
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("fingerprint(%s) blocked", filepath.Base(path))
		}
	}
}
