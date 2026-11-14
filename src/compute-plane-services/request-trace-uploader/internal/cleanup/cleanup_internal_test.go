// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cleanup

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// eventJournal records Done in the same sequence as the directory syncs.
type eventJournal struct{ events *[]string }

func (eventJournal) BeginCleaning(string) error { return nil }
func (j eventJournal) FinishCleaning(string) error {
	*j.events = append(*j.events, "done")
	return nil
}
func (eventJournal) FinishEmpty(string) error { return nil }

type lowSpool struct{}

func (lowSpool) HighestIndex(string) (int, bool, error) { return 9, true, nil }

// A first attempt can delete a file and then fail to sync its directory. The
// retry finds the file gone, and must still sync the directory before Done is
// recorded, or a power loss could bring the file back after Done.
func TestCleanSyncsTheDirectoryWhenARetryFindsTheFilesGone(t *testing.T) {
	spool := t.TempDir()
	transports := filepath.Join(t.TempDir(), "transport")
	if err := os.Mkdir(transports, 0o750); err != nil {
		t.Fatal(err)
	}
	item := Item{
		SegmentID: "seg-4",
		Source:    filepath.Join(spool, "request-trace.000004.jsonl.gz"),
		Prefix:    "request-trace",
		Index:     4,
		ModTime:   time.Now(),
		SHA256:    "00",
		Transports: []string{
			filepath.Join(transports, "seg-4.parquet"),
			// A transport directory that is gone holds nothing to sync.
			filepath.Join(t.TempDir(), "removed", "seg-4.parquet"),
		},
	}
	var events []string
	onSync = func(dir string) { events = append(events, dir) }
	t.Cleanup(func() { onSync = nil })

	result, err := New(eventJournal{&events}, lowSpool{}, Options{}).Clean(t.Context(), item)
	if err != nil || result != ResultDone {
		t.Fatalf("Clean = %s, %v; want done", result, err)
	}
	if want := []string{transports, spool, "done"}; !slices.Equal(events, want) {
		t.Fatalf("events = %v, want both directories synced before Done", events)
	}
}
