// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package cleanup_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/cleanup"
)

// replaceWithFIFO swaps the source for a named pipe. A writer opened at the
// end of the test releases a reader that is still blocked on it.
func replaceWithFIFO(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	t.Cleanup(func() {
		if w, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			w.Close()
		}
	})
}

func TestCleanKeepsASourceReplacedByAPipeWithoutBlocking(t *testing.T) {
	f := newFixture(t, "uploaded records", 1)
	replaceWithFIFO(t, f.item.Source)
	// The default hasher, which reads the file.
	cleaner := cleanup.New(f.journal, f.spool, cleanup.Options{Counter: f.counter})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	type outcome struct {
		result cleanup.Result
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := cleaner.Clean(ctx, f.item)
		done <- outcome{result, err}
	}()
	var got outcome
	select {
	case got = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Clean blocked on a source replaced by a pipe")
	}
	if !errors.Is(got.err, cleanup.ErrSourceChanged) || got.result == cleanup.ResultDone {
		t.Fatalf("Clean = %s, %v; want ErrSourceChanged", got.result, got.err)
	}
	if _, err := os.Lstat(f.item.Source); err != nil {
		t.Fatal("the pipe at the source path was deleted, want it kept")
	}
	if f.counter[cleanup.CounterSourceChanged] != 1 {
		t.Fatalf("%s = %d, want 1", cleanup.CounterSourceChanged, f.counter[cleanup.CounterSourceChanged])
	}
	for _, step := range f.journal.steps {
		if strings.HasPrefix(step, "done") {
			t.Fatal("Done recorded for a source that is not a regular file")
		}
	}
}
