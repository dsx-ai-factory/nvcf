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
	// Another instance's restore ended at 17:55 and one at 17:58: the
	// closest stamp is this restore's.
	d := map[string]string{"restores": "2026-11-12T17:55:00Z=40.0 2026-11-12T17:57:29Z=144.5 2026-11-12T17:58:30Z=99.0"}
	if got := RecordRestoreSeconds(d, restored, 2*time.Minute); got != 144.5 {
		t.Fatalf("got %v, want the stamp closest to its last rank's restore (144.5)", got)
	}
	if got := RecordRestoreSeconds(d, restored.Add(time.Hour), 2*time.Minute); got != 0 {
		t.Fatalf("got %v, want none within the tolerance", got)
	}
	if got := RecordRestoreSeconds(map[string]string{"restores": "junk 2026-11-12T17:57:29Z=x"}, restored, time.Hour); got != 0 {
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

func TestGPURanks(t *testing.T) {
	if got := GPURanks(map[string]string{"size": "7", "companion.4": "true", "companion.5": "true", "companion.6": "true"}); got != 4 {
		t.Fatalf("GPURanks = %d, want 4 (7 members less 3 companions)", got)
	}
	if got := GPURanks(map[string]string{}); got != 0 {
		t.Fatalf("GPURanks of nothing = %d", got)
	}
}
