// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package fnstatus

// A function version's restore history (docs/design/FUNCTION-CACHE-STATUS.md,
// History): its last HistoryMax restores, each with how long it took to
// restore, to scale up and, once deleted, to tear down. nvsnap-server builds
// the entries from the restores' pods (internal/server/restore_history.go)
// and merges them into the object's status on each pass, so they outlive
// the pods and a recapture of the configuration.

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

// HistoryMax bounds a function version's history.
const HistoryMax = 50

// RestoreMatchWindow: the pods of one instance's restore are created
// within it of each other, and an entry seen again (a server restart that
// no longer sees the restore's first pod) within it of its first sight is
// the same restore.
const RestoreMatchWindow = 2 * time.Minute

// Restore outcomes and teardowns.
const (
	RestoreRestoring   = "restoring"    // its ranks are not all Ready yet
	RestoreRestored    = "restored"     // every rank became Ready
	RestoreGone        = "gone"         // deleted, or replaced, before every rank was Ready
	RestoreColdStarted = "cold-started" // a rank's restore failed and it started cold in place

	TeardownReleased = "released" // every rank's workload exited, then its pod went
	TeardownKilled   = "killed"   // a rank's pod went with its workload still running
)

// Restore is one restore of a configuration: its ranks' pods, created
// together in one namespace.
type Restore struct {
	// At is when the restore's first pod was created: with Namespace and
	// Key, what tells the restore apart.
	At        string `json:"at"`
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	// Instance tells the instances of one configuration in one namespace
	// apart (a Grove scaling group replica, a LeaderWorkerSet group, a
	// controller).
	Instance string `json:"instance,omitempty"`
	Ranks    int    `json:"ranks"`
	Outcome  string `json:"outcome"`
	// RestoredAt is when its last rank was restored.
	RestoredAt string `json:"restoredAt,omitempty"`
	// EndedAt is when its last pod went: a restore of the instance first
	// seen at or after it is another one, however soon it followed.
	EndedAt string `json:"endedAt,omitempty"`
	// RestoreSeconds is the record's time for the restore (placeholders
	// running to every rank restored), when it kept one.
	RestoreSeconds float64 `json:"restoreSeconds,omitempty"`
	// ScaleUpSeconds is the first rank's scheduling to the last rank Ready.
	ScaleUpSeconds float64 `json:"scaleUpSeconds,omitempty"`
	// TeardownSeconds is the first rank's delete request to the last
	// rank's workload exited, for a released teardown.
	TeardownSeconds float64 `json:"teardownSeconds,omitempty"`
	Teardown        string  `json:"teardown,omitempty"`
	// CapturedBy is the agent bundle that took the capture restored.
	CapturedBy string `json:"capturedBy,omitempty"`
	// Leader is the UID of its rank 0's pod: what names the restore in
	// the record's stamps. Not kept.
	Leader string `json:"-"`
}

// same reports whether r and u are one restore: one instance of one
// configuration in one namespace, first seen within RestoreMatchWindow,
// and neither first seen once the other had ended (a replacement that
// followed within the window is a restore of its own).
func (r Restore) same(u Restore) bool {
	if r.Namespace != u.Namespace || r.Key != u.Key || r.Instance != u.Instance {
		return false
	}
	if startedAfterEnd(u.At, r.EndedAt) || startedAfterEnd(r.At, u.EndedAt) {
		return false
	}
	a, err1 := time.Parse(time.RFC3339, r.At)
	b, err2 := time.Parse(time.RFC3339, u.At)
	if err1 != nil || err2 != nil {
		return r.At == u.At
	}
	d := a.Sub(b)
	return d <= RestoreMatchWindow && d >= -RestoreMatchWindow
}

// startedAfterEnd reports whether a restore first seen at at began once a
// restore that ended at ended was over (both to the second).
func startedAfterEnd(at, ended string) bool {
	if ended == "" {
		return false
	}
	a, err1 := time.Parse(time.RFC3339, at)
	e, err2 := time.Parse(time.RFC3339, ended)
	return err1 == nil && err2 == nil && !a.Before(e)
}

// History is the restore part of a status.
type History struct {
	// Restores counts every restore the history has had, not only those
	// kept; Gone the ones deleted or replaced before every rank was Ready;
	// Killed the teardowns that killed a rank.
	Restores int `json:"restoreCount,omitempty"`
	Gone     int `json:"goneCount,omitempty"`
	Killed   int `json:"killedTeardowns,omitempty"`
	// The medians over the kept restores that have the time.
	P50RestoreSeconds  float64   `json:"p50RestoreSeconds,omitempty"`
	P50ScaleUpSeconds  float64   `json:"p50ScaleUpSeconds,omitempty"`
	P50TeardownSeconds float64   `json:"p50TeardownSeconds,omitempty"`
	Entries            []Restore `json:"history,omitempty"`
}

// Merge adds the restores in upd to h: a new restore is appended and
// counted; one it has is updated with what upd knows more of. It keeps the
// newest HistoryMax: once full, a restore older than the oldest kept is
// not added (it was kept and dropped before, or is too old to keep). The
// restores in upd are taken oldest first: a restore's end is known before
// its replacement, first seen after it, is matched (same).
func (h History) Merge(upd []Restore) History {
	upd = append([]Restore(nil), upd...)
	sort.SliceStable(upd, func(a, b int) bool { return upd[a].At < upd[b].At })
	out := append([]Restore(nil), h.Entries...)
	oldest := ""
	if len(out) >= HistoryMax {
		oldest = out[0].At
	}
	find := func(u Restore) int {
		for i := range out {
			if out[i].same(u) {
				return i
			}
		}
		return -1
	}
	for _, u := range upd {
		i := find(u)
		if i < 0 {
			if oldest != "" && u.At < oldest {
				continue
			}
			out = append(out, u)
			h.Restores++
			if u.Outcome == RestoreGone {
				h.Gone++
			}
			if u.Teardown == TeardownKilled {
				h.Killed++
			}
			continue
		}
		prev := out[i]
		next := update(prev, u)
		if prev.Outcome != RestoreGone && next.Outcome == RestoreGone {
			h.Gone++
		}
		if prev.Teardown != TeardownKilled && next.Teardown == TeardownKilled {
			h.Killed++
		}
		out[i] = next
	}
	sort.SliceStable(out, func(a, b int) bool { return out[a].At < out[b].At })
	if len(out) > HistoryMax {
		out = out[len(out)-HistoryMax:]
	}
	h.Entries = out
	h.P50RestoreSeconds = median(out, func(r Restore) float64 { return r.RestoreSeconds })
	h.P50ScaleUpSeconds = median(out, func(r Restore) float64 { return r.ScaleUpSeconds })
	h.P50TeardownSeconds = median(out, func(r Restore) float64 { return r.TeardownSeconds })
	return h
}

// update is prev with what u adds: an outcome is final once restored or
// gone, a teardown once known, and a time once set.
func update(prev, u Restore) Restore {
	if prev.Outcome == RestoreRestoring || prev.Outcome == "" {
		prev.Outcome = u.Outcome
	}
	if prev.Teardown == "" {
		prev.Teardown = u.Teardown
		if u.Teardown == TeardownReleased {
			prev.TeardownSeconds = u.TeardownSeconds
		}
	}
	if prev.ScaleUpSeconds == 0 {
		prev.ScaleUpSeconds = u.ScaleUpSeconds
	}
	if prev.RestoreSeconds == 0 {
		prev.RestoreSeconds = u.RestoreSeconds
	}
	if prev.Ranks == 0 {
		prev.Ranks = u.Ranks
	}
	if prev.CapturedBy == "" {
		prev.CapturedBy = u.CapturedBy
	}
	if prev.RestoredAt == "" {
		prev.RestoredAt = u.RestoredAt
	}
	if prev.EndedAt == "" {
		prev.EndedAt = u.EndedAt
	}
	return prev
}

// median of the non-zero values f takes over rs, to the tenth; 0 for none.
func median(rs []Restore, f func(Restore) float64) float64 {
	var vs []float64
	for _, r := range rs {
		if v := f(r); v > 0 {
			vs = append(vs, v)
		}
	}
	if len(vs) == 0 {
		return 0
	}
	sort.Float64s(vs)
	m := vs[len(vs)/2]
	if len(vs)%2 == 0 {
		m = (vs[len(vs)/2-1] + vs[len(vs)/2]) / 2
	}
	return float64(int64(m*10+0.5)) / 10
}

// RecordStamp is one of a record's restores: "RFC3339=seconds=leader",
// stamped when the restore ends, leader the UID of its rank 0's pod (an
// agent before the leader was stamped wrote "RFC3339=seconds").
type RecordStamp struct {
	At      time.Time
	Seconds float64
	Leader  string
}

// RecordStamps are the restores record data d keeps, oldest first;
// malformed ones are skipped.
func RecordStamps(d map[string]string) []RecordStamp {
	var out []RecordStamp
	for _, e := range strings.Fields(d["restores"]) {
		parts := strings.SplitN(e, "=", 3)
		if len(parts) < 2 {
			continue
		}
		t, err1 := time.Parse(time.RFC3339, parts[0])
		v, err2 := strconv.ParseFloat(parts[1], 64)
		if err1 != nil || err2 != nil {
			continue
		}
		s := RecordStamp{At: t, Seconds: v}
		if len(parts) == 3 {
			s.Leader = parts[2]
		}
		out = append(out, s)
	}
	return out
}

// RecordRestoreSeconds is the time record data d keeps for the restore
// whose rank 0's pod is leader (all instances of a configuration share
// the record); 0 for none. A stamp that names no leader is taken only
// when it is the one stamp within tolerance of restored, the restore's
// last rank's restore: with two, the time could be another instance's.
func RecordRestoreSeconds(d map[string]string, leader string, restored time.Time, tolerance time.Duration) float64 {
	unnamed, near := 0.0, 0
	for _, s := range RecordStamps(d) {
		if s.Leader != "" {
			if leader != "" && s.Leader == leader {
				return s.Seconds
			}
		}
		off := s.At.Sub(restored)
		if off < 0 {
			off = -off
		}
		if off > tolerance {
			continue
		}
		near++
		if s.Leader == "" {
			unnamed = s.Seconds
		}
	}
	if near == 1 {
		return unnamed
	}
	return 0
}

// GPURanks is the number of GPU ranks of record data d: its members less
// its companions.
func GPURanks(d map[string]string) int {
	n, _ := strconv.Atoi(d["size"])
	for k, v := range d {
		if strings.HasPrefix(k, "companion.") && v == "true" {
			n--
		}
	}
	return n
}
