// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package atomicfile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func names(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, entry := range entries {
		out = append(out, entry.Name())
	}
	return out
}

func TestPublishMakesTheFileVisibleOnlyWhenComplete(t *testing.T) {
	dir := t.TempDir()
	file, err := Create(dir, ".out.*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(dir, "out")
	if _, err := os.Stat(final); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("final name before Publish: %v, want absent", err)
	}
	if err := file.Publish(final); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(final)
	if err != nil || string(data) != "hello" {
		t.Fatalf("published = %q, %v", data, err)
	}
	if got := names(t, dir); len(got) != 1 || got[0] != "out" {
		t.Fatalf("dir = %v, want only the published file", got)
	}
}

func TestDiscardRemovesTheTemporaryFileAndIsSafeTwice(t *testing.T) {
	dir := t.TempDir()
	file, err := Create(dir, ".out.*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := file.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := file.Discard(); err != nil {
		t.Fatalf("second Discard() = %v, want nil", err)
	}
	if got := names(t, dir); len(got) != 0 {
		t.Fatalf("dir = %v, want empty", got)
	}
}

// A failure after the rename must not leave a file that looks published but
// was never made durable.
func TestAFailedPublishLeavesNeitherName(t *testing.T) {
	dir := t.TempDir()
	original := syncDir
	t.Cleanup(func() { syncDir = original })
	syncDir = func(string) error { return errors.New("sync failed") }

	file, err := Create(dir, ".out.*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := file.Publish(filepath.Join(dir, "out")); err == nil {
		t.Fatal("Publish() error = nil, want the sync failure")
	}
	if got := names(t, dir); len(got) != 0 {
		t.Fatalf("dir = %v, want neither the temporary nor the final name", got)
	}
}

func TestErrorsCarryNoPaths(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-dir")
	if _, err := Create(missing, ".out.*.tmp"); err == nil || strings.Contains(err.Error(), "missing-dir") {
		t.Fatalf("Create() error = %v, want an error with no path", err)
	}
	dir := t.TempDir()
	file, err := Create(dir, ".out.*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	err = file.Publish(filepath.Join(missing, "out"))
	if err == nil || strings.Contains(err.Error(), "missing-dir") || strings.Contains(err.Error(), dir) {
		t.Fatalf("Publish() error = %v, want an error with no path", err)
	}
	if got := names(t, dir); len(got) != 0 {
		t.Fatalf("dir = %v, want the temporary file removed after a failed publish", got)
	}
}
