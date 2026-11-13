// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"compress/gzip"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func gunzip(t *testing.T, path string) string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestJSONLFileWritesGzipLinesOnClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transport.jsonl.gz")
	writer, err := CreateJSONLFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{`{"a":1}`, `{"b":2}`} {
		if err := writer.Write(Record{Payload: []byte(payload)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("transport visible before Close: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := gunzip(t, path); got != "{\"a\":1}\n{\"b\":2}\n" {
		t.Fatalf("transport = %q", got)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 {
		t.Fatalf("directory has %d entries (%v), want only the transport", len(entries), err)
	}
}

func TestJSONLFileAbortLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	writer, err := CreateJSONLFile(filepath.Join(dir, "transport.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(Record{Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Abort(); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("directory has %d entries after Abort, want none", len(entries))
	}
}

func TestRejectFileCreatesNothingWhenUnused(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "rejects")
	rejects := NewRejectFile(dir)
	committed, err := rejects.Commit("segment-id")
	if err != nil || committed {
		t.Fatalf("Commit() = %v, %v, want nothing to commit", committed, err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("reject directory created with nothing rejected: %v", err)
	}
}

func TestRejectFileCommitsUnderSegmentID(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "rejects")
	rejects := NewRejectFile(dir)
	if _, err := rejects.Write([]byte("bad line\n")); err != nil {
		t.Fatal(err)
	}
	committed, err := rejects.Commit("abc123")
	if err != nil || !committed {
		t.Fatalf("Commit() = %v, %v", committed, err)
	}
	path := filepath.Join(dir, "abc123.jsonl.gz")
	if got := gunzip(t, path); got != "bad line\n" {
		t.Fatalf("reject artifact = %q", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("mode = %v, want 0600 because rejects can hold user data", mode)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Errorf("directory has %d entries, want only the artifact", len(entries))
	}
}

func TestRejectFileDiscardRemovesPartialArtifact(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "rejects")
	rejects := NewRejectFile(dir)
	if _, err := rejects.Write([]byte("bad line\n")); err != nil {
		t.Fatal(err)
	}
	if err := rejects.Discard(); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("directory has %d entries after Discard, want none", len(entries))
	}
}

func TestRejectFileRejectsUnsafeName(t *testing.T) {
	for _, name := range []string{"", "../x", "a/b", ".", ".."} {
		rejects := NewRejectFile(t.TempDir())
		if _, err := rejects.Write([]byte("x\n")); err != nil {
			t.Fatal(err)
		}
		if _, err := rejects.Commit(name); err == nil {
			t.Errorf("Commit(%q) accepted an unsafe name", name)
		}
		if err := rejects.Discard(); err != nil {
			t.Fatal(err)
		}
	}
}

// The reject directory and any parent it needed are linked durably before
// the first artifact goes in, so a committed artifact survives a crash
// after its source is deleted.
func TestRejectFileSyncsTheParentOfEachDirectoryItCreates(t *testing.T) {
	root := t.TempDir()
	quarantine := filepath.Join(root, "quarantine")
	dir := filepath.Join(quarantine, "rejects")
	var synced []string
	real := syncDir
	syncDir = func(d string) error {
		synced = append(synced, d)
		return real(d)
	}
	t.Cleanup(func() { syncDir = real })

	rejects := NewRejectFile(dir)
	if _, err := rejects.Write([]byte("bad\n")); err != nil {
		t.Fatal(err)
	}
	if want := []string{root, quarantine}; !slices.Equal(synced, want) {
		t.Fatalf("synced %q before commit, want %q", synced, want)
	}

	synced = nil
	again := NewRejectFile(dir)
	if _, err := again.Write([]byte("bad\n")); err != nil {
		t.Fatal(err)
	}
	if want := []string{quarantine}; !slices.Equal(synced, want) {
		t.Fatalf("synced %q for an existing directory, want %q", synced, want)
	}
	if err := errors.Join(rejects.Discard(), again.Discard()); err != nil {
		t.Fatal(err)
	}
}

// A sync that failed after the directory was made is done again by the next
// reject file, which finds the directory already there.
func TestRejectFileRetriesTheDirectorySyncThatFailed(t *testing.T) {
	quarantine := t.TempDir()
	dir := filepath.Join(quarantine, "rejects")
	real := syncDir
	t.Cleanup(func() { syncDir = real })
	syncDir = func(string) error { return errors.New("sync failed") }
	if _, err := NewRejectFile(dir).Write([]byte("bad\n")); err == nil {
		t.Fatal("Write succeeded with a failing directory sync")
	}

	var synced []string
	syncDir = func(d string) error {
		synced = append(synced, d)
		return real(d)
	}
	retry := NewRejectFile(dir)
	if _, err := retry.Write([]byte("bad\n")); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(synced, quarantine) {
		t.Fatalf("synced %q on retry, want %q", synced, quarantine)
	}
	if err := retry.Discard(); err != nil {
		t.Fatal(err)
	}
}
