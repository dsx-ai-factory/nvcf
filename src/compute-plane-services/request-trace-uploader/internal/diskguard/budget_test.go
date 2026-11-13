// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package diskguard_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/diskguard"
)

type fakeBudget struct{ size, held int64 }

func (b *fakeBudget) Held() (int64, int64, error) { return b.size, b.held, nil }

// withBudget gives the world a spool budget of size bytes with held bytes
// held.
func (w *world) withBudget(size, held int64) *world {
	w.budget = &fakeBudget{size: size, held: held}
	return w
}

// A spool on a large shared disk, such as an emptyDir with a size limit on
// the node's disk, is pruned at its budget's watermark. Without a budget
// only the filesystem counts, as before.
func TestABudgetBelowTheFilesystemPrunesAtTheBudgetsWatermark(t *testing.T) {
	cases := []struct {
		name        string
		budget      bool
		wantRemoved string
		wantPause   diskguard.PauseReason
	}{
		{"no budget measures the filesystem only", false, "", diskguard.PauseNone},
		// 950 of a 1000-byte budget is used; its watermark is 850, so 100
		// must go: the two oldest segments.
		{"budget", true, "segment:s-old,segment:s-mid", diskguard.PauseHighWatermark},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The filesystem is 1 GB and 10% used.
			w := newWorld(1_000_000_000, 900_000_000)
			if tc.budget {
				w.withBudget(1000, 950)
			}
			w.segments.items = []diskguard.Candidate{item("s-new", 70, 3*time.Hour), item("s-mid", 50, 4*time.Hour), item("s-old", 60, 5*time.Hour)}
			g := w.guard(t, diskguard.DefaultConfig())

			decision, err := g.AllowPrepare(1, 0)
			if err != nil || decision.Reason != tc.wantPause {
				t.Fatalf("AllowPrepare = %+v, %v; want reason %q", decision, err, tc.wantPause)
			}
			report, err := g.Enforce(now)
			if err != nil {
				t.Fatalf("Enforce: %v", err)
			}
			if got := strings.Join(w.removed, ","); got != tc.wantRemoved {
				t.Fatalf("evicted %q (report %+v), want %q", got, report, tc.wantRemoved)
			}
			if report.AboveWatermark != tc.budget {
				t.Fatalf("report = %+v, want above the watermark only with the budget", report)
			}
		})
	}
}

// The fuller of the budget and the filesystem sets how much is freed, so
// relief from either one alone is not enough.
func TestTheFullerOfBudgetAndFilesystemWins(t *testing.T) {
	cases := []struct {
		name                 string
		fsTotal, fsUsed      uint64
		budgetTotal, budgetU int64
		wantRemoved          string
	}{
		// The budget needs 100 bytes freed, the filesystem 30.
		{"budget fuller", 10_000, 8_530, 1000, 950, "segment:s1,segment:s2,segment:s3"},
		// The filesystem needs 100 bytes freed, the budget 30.
		{"filesystem fuller", 10_000, 8_600, 1000, 880, "segment:s1,segment:s2,segment:s3"},
		{"both below their watermarks", 10_000, 8_000, 1000, 800, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(tc.fsTotal, tc.fsTotal-tc.fsUsed).withBudget(tc.budgetTotal, tc.budgetU)
			w.segments.items = []diskguard.Candidate{
				item("s4", 40, time.Hour), item("s3", 40, 2*time.Hour), item("s2", 40, 3*time.Hour), item("s1", 40, 4*time.Hour),
			}
			if _, err := w.guard(t, diskguard.DefaultConfig()).Enforce(now); err != nil {
				t.Fatalf("Enforce: %v", err)
			}
			if got := strings.Join(w.removed, ","); got != tc.wantRemoved {
				t.Fatalf("evicted %q, want %q", got, tc.wantRemoved)
			}
		})
	}
}

// Free space is the least of the budget's remaining room and the
// filesystem's free space, so a transport must fit within the budget.
func TestFreeSpaceCountsTheBudgetsRemainingRoom(t *testing.T) {
	const mib = uint64(1) << 20
	const imib = int64(mib)
	cases := []struct {
		name                string
		fsTotal, fsFree     uint64
		budgetTotal, budgUs int64
		estimate            int64
		want                diskguard.PauseReason
	}{
		{"room in both", 100 * 1024 * mib, 50 * 1024 * mib, 2048 * imib, 1024 * imib, 30 * imib, diskguard.PauseNone},
		// 1700 of 2048 MiB is under the 85% watermark and leaves 348 MiB,
		// less than twice a 200 MiB transport.
		{"budget's room under twice the transport", 100 * 1024 * mib, 50 * 1024 * mib, 2048 * imib, 1700 * imib, 200 * imib, diskguard.PauseFreeSpace},
		{"filesystem below the floor", 100 * 1024 * mib, 500 * mib, 2048 * imib, 1024 * imib, 30 * imib, diskguard.PauseHighWatermark},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(tc.fsTotal, tc.fsFree).withBudget(tc.budgetTotal, tc.budgUs)
			got, err := w.guard(t, diskguard.DefaultConfig()).AllowPrepare(tc.estimate, 0)
			if err != nil {
				t.Fatalf("AllowPrepare: %v", err)
			}
			if got.Reason != tc.want {
				t.Fatalf("AllowPrepare = %+v, want reason %q", got, tc.want)
			}
		})
	}
}

// A small spool that relief brings back to its watermark can prepare again,
// with a budget or without one. The fixed 512 MiB floor is more than the 15%
// the watermark leaves free of a spool under about 3.3 GiB, so with it
// preparing stayed paused while relief kept deleting segments that were
// never exported.
func TestPreparingResumesOnceASmallSpoolIsBackAtItsWatermark(t *testing.T) {
	const gib = int64(1) << 30
	cases := []struct {
		name    string
		fsTotal int64
		// budget is the spool budget; zero is none.
		budget int64
	}{
		{"1 GiB filesystem, no budget", gib, 0},
		{"2 GiB filesystem, no budget", 2 * gib, 0},
		{"1 GiB budget", 1 << 40, gib},
		{"2 GiB budget", 1 << 40, 2 * gib},
		{"3 GiB budget", 1 << 40, 3 * gib},
		// The filesystem is the smaller and fuller volume, so relief brings
		// it, not the budget, back to the watermark.
		{"budget larger than its filesystem", gib, 4 * gib},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			smaller := tc.fsTotal
			if tc.budget > 0 {
				smaller = min(smaller, tc.budget)
			}
			// The spool holds 94% of the smaller volume, in closed segments of
			// 5% each, so relief deletes two to get back under 85%.
			held := smaller * 94 / 100
			w := newWorld(uint64(tc.fsTotal), uint64(tc.fsTotal-held))
			if tc.budget > 0 {
				w.withBudget(tc.budget, held)
			}
			for i := range 18 {
				w.segments.items = append(w.segments.items, item(fmt.Sprintf("s%02d", i), smaller/20, time.Duration(20-i)*time.Hour))
			}
			g := w.guard(t, diskguard.DefaultConfig())

			report, err := g.Enforce(now)
			if err != nil {
				t.Fatalf("Enforce: %v", err)
			}
			if report.SegmentsEvicted != 2 {
				t.Fatalf("evicted %d segments (%q), want the two oldest", report.SegmentsEvicted, w.removed)
			}
			w.volume.usage.Free += uint64(report.FreedBytes)
			if w.budget != nil {
				w.budget.held -= report.FreedBytes
			}

			decision, err := g.AllowPrepare(1<<20, 0)
			if err != nil || !decision.Allowed {
				t.Fatalf("AllowPrepare = %+v, %v; want preparing to resume at the watermark", decision, err)
			}
		})
	}
}

// Quarantine is bounded to its share of the spool, which with a budget is
// the budget and not the disk under it.
func TestQuarantineShareIsOfTheBudget(t *testing.T) {
	// The budget is 1000 bytes, so quarantine may hold 200.
	w := newWorld(1_000_000_000, 900_000_000).withBudget(1000, 300)
	w.quarantine.items = []diskguard.Candidate{item("newest", 100, time.Hour), item("oldest", 150, 3*time.Hour)}

	if _, err := w.guard(t, diskguard.DefaultConfig()).Enforce(now); err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "quarantine:oldest" {
		t.Fatalf("evicted %q, want the oldest entry to bring quarantine under 20%% of the budget", got)
	}
}

// The gauges show the volume the guard bounds: with a budget fuller than
// the disk, the bytes the spool holds and the room left in the budget.
func TestGaugesReportTheFullerVolumeAndTheLeastFreeSpace(t *testing.T) {
	w := newWorld(1_000_000_000, 900_000_000).withBudget(1000, 900)
	if err := w.guard(t, diskguard.DefaultConfig()).UpdateGauges(now, nil); err != nil {
		t.Fatalf("UpdateGauges: %v", err)
	}
	if used, free := w.metrics.gauges[diskguard.GaugeSpoolUsedBytes], w.metrics.gauges[diskguard.GaugeSpoolFreeBytes]; used != 900 || free != 100 {
		t.Fatalf("used = %v, free = %v; want the budget's 900 and 100", used, free)
	}
}

// A spool budget holds the files under its roots, each counted once when
// one root is inside another, and a root not created yet holds nothing.
func TestSpoolBudgetMeasuresTheFilesUnderItsRoots(t *testing.T) {
	spool := t.TempDir()
	quarantine := t.TempDir()
	write := func(path string, size int) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(spool, "request-trace.000001.jsonl.gz"), 100)
	write(filepath.Join(spool, "state", "transport", "seg.dest.jsonl.gz"), 50)
	write(filepath.Join(quarantine, "seg-1", "request-trace.000000.jsonl.gz"), 25)
	roots := []string{spool, filepath.Join(spool, "state"), quarantine, filepath.Join(t.TempDir(), "not-yet")}

	size, held, err := diskguard.SpoolBudget(1000, roots...).Held()
	if err != nil {
		t.Fatalf("Held: %v", err)
	}
	if size != 1000 || held != 175 {
		t.Fatalf("Held = %d, %d; want 1000 and 175", size, held)
	}
}

// A spool already above its budget, such as after the budget was lowered,
// has everything above the watermark freed in one pass.
func TestASpoolAboveItsBudgetIsBroughtToTheWatermarkInOnePass(t *testing.T) {
	// 290 bytes held in a 200-byte budget; the watermark is 170, so 120
	// must go.
	w := newWorld(1_000_000_000, 900_000_000).withBudget(200, 290)
	w.segments.items = []diskguard.Candidate{
		item("s4", 40, time.Hour), item("s3", 40, 2*time.Hour), item("s2", 40, 3*time.Hour), item("s1", 40, 4*time.Hour),
	}
	g := w.guard(t, diskguard.DefaultConfig())
	if _, err := g.Enforce(now); err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if got := strings.Join(w.removed, ","); got != "segment:s1,segment:s2,segment:s3" {
		t.Fatalf("evicted %q, want the three oldest", got)
	}
	if err := g.UpdateGauges(now, nil); err != nil {
		t.Fatalf("UpdateGauges: %v", err)
	}
	if used, free := w.metrics.gauges[diskguard.GaugeSpoolUsedBytes], w.metrics.gauges[diskguard.GaugeSpoolFreeBytes]; used != 290 || free != 0 {
		t.Fatalf("used = %v, free = %v; want 290 held and no room left", used, free)
	}
}

// A spool reached through a symlinked directory is measured at its target,
// with the state and quarantine directories under it counted once.
func TestSpoolBudgetFollowsASymlinkedSpoolDirectory(t *testing.T) {
	spool := t.TempDir()
	for path, size := range map[string]int{
		filepath.Join(spool, "request-trace.000001.jsonl.gz"):                                  100,
		filepath.Join(spool, "request-trace-uploader-state", "transport", "seg.dest.jsonl.gz"): 50,
		filepath.Join(spool, "request-trace-uploader-quarantine", "seg-1", "request-trace.gz"): 25,
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(t.TempDir(), "spool")
	if err := os.Symlink(spool, link); err != nil {
		t.Fatal(err)
	}

	_, held, err := diskguard.SpoolBudget(1000, link, filepath.Join(link, "request-trace-uploader-state"), filepath.Join(link, "request-trace-uploader-quarantine")).Held()
	if err != nil {
		t.Fatalf("Held: %v", err)
	}
	if held != 175 {
		t.Fatalf("held = %d, want 175 from the symlink's target", held)
	}
}
