// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package fnstatus

import (
	"testing"
	"time"
)

// restoreAt is the i-th restore, 3 minutes apart: beyond RestoreMatchWindow,
// so never taken for one another.
func restoreAt(i int) Restore {
	return Restore{At: time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC).Add(time.Duration(3*i) * time.Minute).Format(time.RFC3339),
		Namespace: "ns", Key: "k", Ranks: 4, Outcome: RestoreRestoring}
}

func TestHistoryMergeAddsCountsAndUpdates(t *testing.T) {
	r := restoreAt(1)
	h := History{}.Merge([]Restore{r})
	if h.Restores != 1 || len(h.Entries) != 1 {
		t.Fatalf("after the first sight: %+v", h)
	}
	// The same restore again (a later pass, a server restart): updated, not counted again.
	r.Outcome, r.ScaleUpSeconds = RestoreRestored, 150
	h = h.Merge([]Restore{r})
	if h.Restores != 1 || len(h.Entries) != 1 || h.Entries[0].Outcome != RestoreRestored || h.Entries[0].ScaleUpSeconds != 150 {
		t.Fatalf("after its pods were Ready: %+v", h)
	}
	// What a later pass knows more of is added; what it knows less of is kept.
	later := r
	later.ScaleUpSeconds, later.Teardown, later.TeardownSeconds = 0, TeardownReleased, 105
	h = h.Merge([]Restore{later})
	e := h.Entries[0]
	if e.ScaleUpSeconds != 150 || e.Teardown != TeardownReleased || e.TeardownSeconds != 105 {
		t.Fatalf("after its teardown: %+v", e)
	}
	// A final outcome or teardown is not overwritten.
	again := r
	again.Outcome, again.Teardown = RestoreGone, TeardownKilled
	h = h.Merge([]Restore{again})
	if e := h.Entries[0]; e.Outcome != RestoreRestored || e.Teardown != TeardownReleased || h.Gone != 0 || h.Killed != 0 {
		t.Fatalf("a final outcome was overwritten: %+v %+v", e, h)
	}
	if h.P50ScaleUpSeconds != 150 || h.P50TeardownSeconds != 105 {
		t.Fatalf("medians %+v", h)
	}
}

func TestHistoryMergeCountsGoneAndKilled(t *testing.T) {
	gone := restoreAt(1)
	h := History{}.Merge([]Restore{gone})
	gone.Outcome = RestoreGone
	h = h.Merge([]Restore{gone})
	killed := restoreAt(2)
	killed.Outcome, killed.Teardown = RestoreRestored, TeardownKilled
	h = h.Merge([]Restore{killed})
	if h.Restores != 2 || h.Gone != 1 || h.Killed != 1 {
		t.Fatalf("counts %+v", h)
	}
	// A killed teardown has no time.
	if h.P50TeardownSeconds != 0 {
		t.Fatalf("a killed teardown was timed: %v", h.P50TeardownSeconds)
	}
}

func TestHistoryKeepsTheNewestAndCountsAll(t *testing.T) {
	h := History{}
	for i := 0; i < HistoryMax+7; i++ {
		r := restoreAt(i)
		r.Outcome, r.ScaleUpSeconds = RestoreRestored, float64(100+i)
		h = h.Merge([]Restore{r})
	}
	if len(h.Entries) != HistoryMax || h.Restores != HistoryMax+7 {
		t.Fatalf("%d kept, %d counted", len(h.Entries), h.Restores)
	}
	if h.Entries[0].At != restoreAt(7).At || h.Entries[HistoryMax-1].At != restoreAt(HistoryMax+6).At {
		t.Fatalf("kept %s..%s, want the newest", h.Entries[0].At, h.Entries[HistoryMax-1].At)
	}
	// The median is over what is kept: 107..156.
	if h.P50ScaleUpSeconds != 131.5 {
		t.Fatalf("p50 %v", h.P50ScaleUpSeconds)
	}
	// Out of order: sorted by time.
	h = History{}.Merge([]Restore{restoreAt(9), restoreAt(2)})
	if h.Entries[0].At != restoreAt(2).At {
		t.Fatalf("not sorted: %v", h.Entries)
	}
}

func TestRecordRestoreSeconds(t *testing.T) {
	restored := time.Date(2026, 11, 12, 17, 57, 25, 0, time.UTC)
	// Three instances' restores ended within a minute: each restore's time
	// is its own stamp's, by its rank 0's pod, whichever is nearest.
	d := map[string]string{"restores": "2026-11-12T17:57:20Z=40.0=uid-a 2026-11-12T17:57:29Z=144.5=uid-b 2026-11-12T17:57:25Z=99.0=uid-c"}
	for leader, want := range map[string]float64{"uid-a": 40, "uid-b": 144.5, "uid-c": 99, "uid-d": 0, "": 0} {
		if got := RecordRestoreSeconds(d, leader, restored, 2*time.Minute); got != want {
			t.Errorf("leader %q: got %v, want %v", leader, got, want)
		}
	}
	// Stamps that name no restore (an older agent): one within the
	// tolerance is this restore's; two could be either's, so none.
	one := map[string]string{"restores": "2026-11-12T17:50:00Z=40.0 2026-11-12T17:57:29Z=144.5"}
	if got := RecordRestoreSeconds(one, "uid-b", restored, 2*time.Minute); got != 144.5 {
		t.Fatalf("got %v, want the one unnamed stamp within the tolerance (144.5)", got)
	}
	two := map[string]string{"restores": "2026-11-12T17:57:20Z=40.0 2026-11-12T17:57:29Z=144.5"}
	if got := RecordRestoreSeconds(two, "uid-b", restored, 2*time.Minute); got != 0 {
		t.Fatalf("got %v, want none from two unnamed stamps within the tolerance", got)
	}
	mixed := map[string]string{"restores": "2026-11-12T17:57:20Z=40.0=uid-a 2026-11-12T17:57:29Z=144.5"}
	if got := RecordRestoreSeconds(mixed, "uid-b", restored, 2*time.Minute); got != 0 {
		t.Fatalf("got %v, want none: another restore's stamp is as near", got)
	}
	if got := RecordRestoreSeconds(one, "uid-b", restored.Add(time.Hour), 2*time.Minute); got != 0 {
		t.Fatalf("got %v, want none within the tolerance", got)
	}
	if got := RecordRestoreSeconds(map[string]string{"restores": "junk 2026-11-12T17:57:29Z=x"}, "", restored, time.Hour); got != 0 {
		t.Fatalf("got %v from junk", got)
	}
}

// A restore still followed after it fell out of the kept 50 is not added
// back or counted again on later passes.
func TestHistoryFullDoesNotCountTheDroppedAgain(t *testing.T) {
	h := History{}
	old := restoreAt(0)
	h = h.Merge([]Restore{old})
	for i := 1; i <= HistoryMax; i++ {
		h = h.Merge([]Restore{restoreAt(i * 3)}) // 3 minutes apart: never the same restore
	}
	if len(h.Entries) != HistoryMax || h.Restores != HistoryMax+1 {
		t.Fatalf("%d kept, %d counted", len(h.Entries), h.Restores)
	}
	for pass := 0; pass < 5; pass++ {
		h = h.Merge([]Restore{old})
	}
	if h.Restores != HistoryMax+1 || h.Entries[0].At == old.At {
		t.Fatalf("a dropped restore was added back: %d counted, oldest %s", h.Restores, h.Entries[0].At)
	}
}

// A restore seen again with its first pod no longer there (a server
// restart) is dated a little later: within the window, of the same
// instance, it is the same restore.
func TestHistorySameRestoreWithinTheWindow(t *testing.T) {
	a := restoreAt(0)
	a.Instance = "g/0"
	b := a
	b.At = time.Date(2026, 11, 12, 0, 0, 40, 0, time.UTC).Format(time.RFC3339) // 40 s after a's
	b.Outcome = RestoreRestored
	h := History{}.Merge([]Restore{a}).Merge([]Restore{b})
	if h.Restores != 1 || len(h.Entries) != 1 || h.Entries[0].Outcome != RestoreRestored {
		t.Fatalf("%+v, want one restore, updated", h)
	}
	other := b
	other.Instance = "g/1"
	if h = h.Merge([]Restore{other}); h.Restores != 2 {
		t.Fatalf("another instance's restore was taken for this one: %+v", h)
	}
}

// A restore written while its pods were up has no end yet. When it ends
// and its replacement starts within the window, the pass that brings both
// keeps them apart in either order: the end is taken first.
func TestHistoryReplacementAfterAnUnendedEntry(t *testing.T) {
	a := restoreAt(0)
	a.Instance = "g/0"
	a.Outcome = RestoreRestored
	ended := a
	ended.EndedAt = time.Date(2026, 11, 12, 0, 0, 30, 0, time.UTC).Format(time.RFC3339)
	repl := a
	repl.At = time.Date(2026, 11, 12, 0, 0, 40, 0, time.UTC).Format(time.RFC3339) // 10 s after a's end
	repl.Outcome = RestoreRestoring
	for _, upd := range [][]Restore{{repl, ended}, {ended, repl}} {
		h := History{}.Merge([]Restore{a}).Merge(upd)
		if h.Restores != 2 || len(h.Entries) != 2 || h.Entries[0].EndedAt != ended.EndedAt {
			t.Fatalf("pass %v: %+v, want the ended restore and its replacement apart", []string{upd[0].At, upd[1].At}, h)
		}
	}
}

func TestGPURanks(t *testing.T) {
	if got := GPURanks(map[string]string{"size": "7", "companion.4": "true", "companion.5": "true", "companion.6": "true"}); got != 4 {
		t.Fatalf("GPURanks = %d, want 4 (7 members less 3 companions)", got)
	}
	if got := GPURanks(map[string]string{}); got != 0 {
		t.Fatalf("GPURanks of nothing = %d", got)
	}
}
