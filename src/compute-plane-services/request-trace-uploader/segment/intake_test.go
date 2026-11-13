// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package segment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

type recordingSink struct {
	accepted []Segment
	err      error
}

func (s *recordingSink) Accept(_ context.Context, item Segment) error {
	if s.err != nil {
		return s.err
	}
	s.accepted = append(s.accepted, item)
	return nil
}

type gateFunc func(Segment) bool

func (g gateFunc) Admit(item Segment) bool { return g(item) }

var admitAll = gateFunc(func(Segment) bool { return true })

func names(segments []Segment) []string {
	out := make([]string, 0, len(segments))
	for _, item := range segments {
		out = append(out, filepath.Base(item.Path))
	}
	return out
}

func newTestIntake(t *testing.T, directory string, cfg Config, sink Sink, gate Gate) *Intake {
	t.Helper()
	intake, err := NewIntake(directory, cfg, sink, gate)
	if err != nil {
		t.Fatalf("NewIntake() error = %v", err)
	}
	return intake
}

func setModTime(t *testing.T, path string, at time.Time) {
	t.Helper()
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
}

func TestScanLeavesHighestIndexActivePerPrefix(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory,
		"request-trace.000001.jsonl.gz",
		"request-trace.000002.jsonl.gz",
		"request-audit.000007.jsonl.gz",
		"request-audit.000008.jsonl.gz",
		"request-audit.000009.jsonl.gz",
	)

	scan, err := ScanDir(directory, []string{"request-trace", "request-audit"})
	if err != nil {
		t.Fatalf("ScanDir() error = %v", err)
	}
	if got := names(scan.Active); !slices.Equal(got, []string{"request-trace.000002.jsonl.gz", "request-audit.000009.jsonl.gz"}) {
		t.Errorf("active = %v, want the highest index of each prefix", got)
	}
	for _, item := range scan.Closed {
		if item.Index == 2 || item.Index == 9 {
			t.Errorf("active segment %s returned as closed", filepath.Base(item.Path))
		}
	}
}

func TestScanMergesPrefixesOldestFirstKeepingIndexOrder(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory,
		"request-trace.000001.jsonl.gz",
		"request-trace.000002.jsonl.gz",
		"request-trace.000003.jsonl.gz",
		"request-audit.000001.jsonl.gz",
		"request-audit.000002.jsonl.gz",
	)
	base := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	setModTime(t, filepath.Join(directory, "request-trace.000001.jsonl.gz"), base.Add(1*time.Minute))
	// Out of order on purpose: index order must win inside a prefix.
	setModTime(t, filepath.Join(directory, "request-trace.000002.jsonl.gz"), base.Add(0))
	setModTime(t, filepath.Join(directory, "request-audit.000001.jsonl.gz"), base.Add(30*time.Second))

	scan, err := ScanDir(directory, []string{"request-trace", "request-audit"})
	if err != nil {
		t.Fatalf("ScanDir() error = %v", err)
	}
	want := []string{"request-audit.000001.jsonl.gz", "request-trace.000001.jsonl.gz", "request-trace.000002.jsonl.gz"}
	if got := names(scan.Closed); !slices.Equal(got, want) {
		t.Errorf("closed = %v, want %v", got, want)
	}
}

func TestIntakeHandsClosedSegmentsOnceInOrder(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory,
		"request-trace.000003.jsonl.gz",
		"request-trace.000001.jsonl.gz",
		"request-trace.000002.jsonl.gz",
	)
	sink := &recordingSink{}
	intake := newTestIntake(t, directory, DefaultConfig("request-trace"), sink, admitAll)

	first, err := intake.Pass(context.Background())
	if err != nil {
		t.Fatalf("Pass() error = %v", err)
	}
	if got := names(sink.accepted); !slices.Equal(got, []string{"request-trace.000001.jsonl.gz", "request-trace.000002.jsonl.gz"}) {
		t.Fatalf("accepted = %v, want indexes 1 and 2 in order", got)
	}
	if first.Handed != 2 {
		t.Errorf("handed = %d, want 2", first.Handed)
	}

	second, err := intake.Pass(context.Background())
	if err != nil {
		t.Fatalf("second Pass() error = %v", err)
	}
	if len(sink.accepted) != 2 || second.Handed != 0 {
		t.Errorf("second pass handed %d (total %d), want no segment handed twice", second.Handed, len(sink.accepted))
	}
}

func TestIntakeClosesDeadWritersLastSegmentAfterRoll(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory, "request-trace.000004.jsonl.gz")
	sink := &recordingSink{}
	intake := newTestIntake(t, directory, DefaultConfig("request-trace"), sink, admitAll)

	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.accepted) != 0 {
		t.Fatalf("accepted = %v, want the only segment left active", names(sink.accepted))
	}

	// A restarted writer continues at the highest index plus one.
	writeFiles(t, directory, "request-trace.000005.jsonl.gz")
	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := names(sink.accepted); !slices.Equal(got, []string{"request-trace.000004.jsonl.gz"}) {
		t.Fatalf("accepted = %v, want the dead writer's segment once a higher index exists", got)
	}
	if _, err := os.Stat(filepath.Join(directory, "request-trace.000005.jsonl.gz")); err != nil {
		t.Fatalf("active segment touched: %v", err)
	}
}

func TestIntakeBoundsSegmentsAheadOfSubmission(t *testing.T) {
	directory := t.TempDir()
	for index := 0; index <= 20; index++ {
		writeFiles(t, directory, segmentName("request-trace", index))
	}
	sink := &recordingSink{}
	cfg := DefaultConfig("request-trace")
	intake := newTestIntake(t, directory, cfg, sink, admitAll)

	result, err := intake.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Handed != DefaultPrepareAhead || result.Deferred != 20-DefaultPrepareAhead {
		t.Fatalf("handed = %d deferred = %d, want %d and %d", result.Handed, result.Deferred, DefaultPrepareAhead, 20-DefaultPrepareAhead)
	}

	intake.Release(sink.accepted[0])
	intake.Release(sink.accepted[1])
	// A second release of the same segment must not free another slot.
	intake.Release(sink.accepted[1])
	result, err = intake.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Handed != 2 {
		t.Fatalf("handed after two releases = %d, want 2", result.Handed)
	}
	if got := sink.accepted[len(sink.accepted)-1].Index; got != DefaultPrepareAhead+1 {
		t.Errorf("last handed index = %d, want %d", got, DefaultPrepareAhead+1)
	}
}

func TestIntakeStopsHandingWhenGateRefuses(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory,
		"request-trace.000001.jsonl.gz",
		"request-trace.000002.jsonl.gz",
		"request-trace.000003.jsonl.gz",
		"request-trace.000004.jsonl.gz",
	)
	admitted := 1
	gate := gateFunc(func(Segment) bool {
		if admitted == 0 {
			return false
		}
		admitted--
		return true
	})
	sink := &recordingSink{}
	intake := newTestIntake(t, directory, DefaultConfig("request-trace"), sink, gate)

	result, err := intake.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Handed != 1 || result.Deferred != 2 || !result.GateClosed {
		t.Fatalf("result = %+v, want 1 handed, 2 deferred, gate closed", result)
	}

	admitted = 10
	result, err = intake.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := names(sink.accepted); !slices.Equal(got, []string{
		"request-trace.000001.jsonl.gz", "request-trace.000002.jsonl.gz", "request-trace.000003.jsonl.gz",
	}) {
		t.Fatalf("accepted = %v, want the deferred segments once the gate opens", got)
	}
}

func TestIntakeHandsReusedNameWithNewContentAgain(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory, "request-trace.000000.jsonl.gz", "request-trace.000001.jsonl.gz")
	sink := &recordingSink{}
	intake := newTestIntake(t, directory, DefaultConfig("request-trace"), sink, admitAll)
	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Cleanup removes everything after success; a new writer restarts at 0.
	for _, name := range []string{"request-trace.000000.jsonl.gz", "request-trace.000001.jsonl.gz"} {
		if err := os.Remove(filepath.Join(directory, name)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "request-trace.000000.jsonl.gz"), []byte("new content, longer"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeFiles(t, directory, "request-trace.000001.jsonl.gz")
	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := names(sink.accepted); !slices.Equal(got, []string{"request-trace.000000.jsonl.gz", "request-trace.000000.jsonl.gz"}) {
		t.Fatalf("accepted = %v, want the reused name handed again", got)
	}
	if sink.accepted[0].Size == sink.accepted[1].Size {
		t.Errorf("both hand-offs report size %d, want the new file's size", sink.accepted[0].Size)
	}
}

// A segment removed from the spool before Release may still be in the sink's
// hands, so it keeps its prepare-ahead slot until the sink releases it.
func TestIntakeKeepsSlotOfRemovedSegmentUntilRelease(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory, segmentName("request-trace", 1), segmentName("request-trace", 2), segmentName("request-trace", 3))
	sink := &recordingSink{}
	intake := newTestIntake(t, directory, Config{Prefixes: []string{"request-trace"}, PrepareAhead: 1, BatchSegments: 1}, sink, admitAll)
	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := sink.accepted[0]
	if err := os.Remove(first.Path); err != nil {
		t.Fatal(err)
	}

	result, err := intake.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Handed != 0 || result.Deferred != 1 {
		t.Fatalf("handed = %d deferred = %d after the removal, want the slot still held", result.Handed, result.Deferred)
	}

	intake.Release(first)
	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := names(sink.accepted); !slices.Equal(got, []string{segmentName("request-trace", 1), segmentName("request-trace", 2)}) {
		t.Fatalf("accepted after release = %v, want the next segment", got)
	}

	// A second Release of the removed segment frees nothing.
	intake.Release(first)
	writeFiles(t, directory, segmentName("request-trace", 4))
	result, err = intake.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Handed != 0 || result.Deferred != 1 {
		t.Fatalf("handed = %d deferred = %d after a second release, want the slot still held", result.Handed, result.Deferred)
	}
}

func TestIntakeRetriesSegmentTheSinkRefused(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory, "request-trace.000001.jsonl.gz", "request-trace.000002.jsonl.gz")
	sink := &recordingSink{err: errors.New("prepare queue closed")}
	intake := newTestIntake(t, directory, DefaultConfig("request-trace"), sink, admitAll)

	if _, err := intake.Pass(context.Background()); err == nil {
		t.Fatal("Pass() error = nil, want the sink error")
	}
	sink.err = nil
	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := names(sink.accepted); !slices.Equal(got, []string{"request-trace.000001.jsonl.gz"}) {
		t.Fatalf("accepted = %v, want the refused segment handed on the next pass", got)
	}
}

func TestIntakeStopsOnCanceledContext(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory, "request-trace.000001.jsonl.gz", "request-trace.000002.jsonl.gz")
	sink := &recordingSink{}
	intake := newTestIntake(t, directory, DefaultConfig("request-trace"), sink, admitAll)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := intake.Pass(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Pass() error = %v, want context.Canceled", err)
	}
	if len(sink.accepted) != 0 {
		t.Fatalf("accepted = %v, want none after cancel", names(sink.accepted))
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{name: "defaults", mutate: func(*Config) {}},
		{name: "ahead above batch", mutate: func(c *Config) { c.PrepareAhead = 32 }},
		{name: "ahead below batch", mutate: func(c *Config) { c.PrepareAhead = 8 }, wantErr: true},
		{name: "zero batch", mutate: func(c *Config) { c.BatchSegments = 0 }, wantErr: true},
		{name: "no prefix", mutate: func(c *Config) { c.Prefixes = nil }, wantErr: true},
		{name: "overlapping prefixes", mutate: func(c *Config) { c.Prefixes = []string{"a", "a.b"} }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig("request-trace")
			tt.mutate(&cfg)
			if err := cfg.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewIntakeRequiresSinkAndGate(t *testing.T) {
	if _, err := NewIntake(t.TempDir(), DefaultConfig("request-trace"), nil, admitAll); err == nil {
		t.Error("NewIntake() accepted a nil sink")
	}
	if _, err := NewIntake(t.TempDir(), DefaultConfig("request-trace"), &recordingSink{}, nil); err == nil {
		t.Error("NewIntake() accepted a nil gate")
	}
}

func segmentName(prefix string, index int) string {
	return fmt.Sprintf("%s.%06d.jsonl.gz", prefix, index)
}

// releasingSink releases each segment while Accept is still running, as a
// fast worker can.
type releasingSink struct {
	intake   *Intake
	accepted []Segment
}

func (s *releasingSink) Accept(_ context.Context, item Segment) error {
	s.accepted = append(s.accepted, item)
	s.intake.Release(item)
	return nil
}

func TestIntakeReleaseBeforeAcceptReturnsFreesTheSlot(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory,
		"request-trace.000001.jsonl.gz",
		"request-trace.000002.jsonl.gz",
		"request-trace.000003.jsonl.gz",
	)
	cfg := DefaultConfig("request-trace")
	cfg.PrepareAhead, cfg.BatchSegments = 1, 1
	sink := &releasingSink{}
	intake := newTestIntake(t, directory, cfg, sink, admitAll)
	sink.intake = intake

	result, err := intake.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Handed != 2 || result.Deferred != 0 {
		t.Fatalf("result = handed %d deferred %d, want both closed segments handed because each slot was released", result.Handed, result.Deferred)
	}
}

func TestScanKeepsEveryFileAtTheHighestIndexActive(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory, "request-trace.000001.jsonl.gz", "request-trace.0000001.jsonl.gz")

	scan, err := ScanDir(directory, []string{"request-trace"})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Closed) != 0 || len(scan.Active) != 2 {
		t.Fatalf("closed = %v active = %v, want both index 1 files active", names(scan.Closed), names(scan.Active))
	}

	writeFiles(t, directory, "request-trace.000002.jsonl.gz")
	scan, err = ScanDir(directory, []string{"request-trace"})
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Closed) != 2 || len(scan.Active) != 1 || scan.Active[0].Index != 2 {
		t.Fatalf("closed = %v active = %v, want both index 1 files closed once index 2 exists", names(scan.Closed), names(scan.Active))
	}
}

// A handed-off segment that changes on disk is still the same work: it is not
// handed off again, it keeps its slot until Release, and the change is
// counted.
func TestIntakeDoesNotHandOffAChangedSegmentTwice(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory, segmentName("request-trace", 1), segmentName("request-trace", 2), segmentName("request-trace", 3))
	sink := &recordingSink{}
	intake := newTestIntake(t, directory, Config{Prefixes: []string{"request-trace"}, PrepareAhead: 1, BatchSegments: 1}, sink, admitAll)
	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := sink.accepted[0]
	if err := os.WriteFile(first.Path, []byte("rewritten after hand-off"), 0o600); err != nil {
		t.Fatal(err)
	}
	setModTime(t, first.Path, time.Now().Add(time.Minute))

	result, err := intake.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := names(sink.accepted); !slices.Equal(got, []string{segmentName("request-trace", 1)}) {
		t.Fatalf("accepted = %v, want the changed segment handed off once", got)
	}
	if result.Changed != 1 || result.Deferred != 1 {
		t.Fatalf("changed = %d deferred = %d, want 1 and 1: the slot is still held", result.Changed, result.Deferred)
	}

	intake.Release(first)
	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := names(sink.accepted); !slices.Equal(got, []string{segmentName("request-trace", 1), segmentName("request-trace", 2)}) {
		t.Fatalf("accepted after release = %v, want the next segment", got)
	}
}

// lockedSink records accepted segments from concurrent passes.
type lockedSink struct {
	mu       sync.Mutex
	accepted []Segment
}

func (s *lockedSink) Accept(_ context.Context, item Segment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accepted = append(s.accepted, item)
	return nil
}

// meetingGate holds each Admit until a second caller arrives or a short wait
// ends, so two passes that both collected a segment admit it together.
type meetingGate struct {
	arrived chan struct{}
}

func (g meetingGate) Admit(Segment) bool {
	select {
	case g.arrived <- struct{}{}:
	case <-g.arrived:
	case <-time.After(100 * time.Millisecond):
	}
	return true
}

func TestIntakeConcurrentPassesHandEachSegmentOnce(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory, segmentName("request-trace", 1), segmentName("request-trace", 2))
	sink := &lockedSink{}
	intake := newTestIntake(t, directory, DefaultConfig("request-trace"), sink, meetingGate{arrived: make(chan struct{})})

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := intake.Pass(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if got := names(sink.accepted); !slices.Equal(got, []string{segmentName("request-trace", 1)}) {
		t.Fatalf("accepted = %v, want the segment once", got)
	}
	intake.Release(sink.accepted[0])
	intake.mu.Lock()
	ahead := intake.ahead
	intake.mu.Unlock()
	if ahead != 0 {
		t.Fatalf("slots held after release = %d, want 0", ahead)
	}
}

func TestIntakeCountsDeferredWhenThePassStopsEarly(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory, segmentName("request-trace", 1), segmentName("request-trace", 2), segmentName("request-trace", 3))
	sink := &recordingSink{err: errors.New("prepare queue closed")}
	intake := newTestIntake(t, directory, DefaultConfig("request-trace"), sink, admitAll)

	result, err := intake.Pass(context.Background())
	if err == nil {
		t.Fatal("Pass() error = nil, want the sink error")
	}
	if result.Deferred != 2 {
		t.Fatalf("deferred = %d, want both closed segments left for a later pass", result.Deferred)
	}
}

// A file that reuses the name of a removed, unreleased segment is new work:
// it waits for a free slot, is not counted as a change, and a late Release
// of the old file never frees the new file's slot.
func TestIntakeIgnoresAStaleReleaseAfterANameIsReused(t *testing.T) {
	directory := t.TempDir()
	writeFiles(t, directory, segmentName("request-trace", 1), segmentName("request-trace", 2))
	sink := &recordingSink{}
	intake := newTestIntake(t, directory, Config{Prefixes: []string{"request-trace"}, PrepareAhead: 1, BatchSegments: 1}, sink, admitAll)
	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := sink.accepted[0]
	if err := os.Remove(old.Path); err != nil {
		t.Fatal(err)
	}
	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(old.Path, []byte("new content under the same name"), 0o600); err != nil {
		t.Fatal(err)
	}
	setModTime(t, old.Path, time.Now().Add(time.Hour))
	result, err := intake.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Handed != 0 || result.Deferred != 1 || result.Changed != 0 {
		t.Fatalf("handed = %d deferred = %d changed = %d, want the reused name deferred as new work while the old slot is held", result.Handed, result.Deferred, result.Changed)
	}

	intake.Release(old)
	if _, err := intake.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.accepted) != 2 {
		t.Fatalf("accepted = %v, want the reused name handed off once the old slot is free", names(sink.accepted))
	}

	writeFiles(t, directory, segmentName("request-trace", 3))
	intake.Release(old)
	result, err = intake.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Handed != 0 || result.Deferred != 1 {
		t.Fatalf("handed = %d deferred = %d after a stale release, want the slot still held", result.Handed, result.Deferred)
	}
}
