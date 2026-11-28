/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package rollout keeps the record that decides when label-trusted behavior and utils replicas may be enabled. Proving
// that the provenance webhook enforces is necessary but not sufficient: objects that carry reserved metadata and were
// created before enforcement was proven, or while it was not verified, could have been forged. This package remembers
// when enforcement was first verified and every window in which it was not, scans for reserved-metadata objects created
// in those windows, and opens the rollout gate only after a clean scan. A later loss of verification opens a new window
// and closes the gate again until the next clean scan.
package rollout

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance/readiness"
)

// ErrInventoryPending is returned by the rollout gate while the post-enforcement inventory is not cleared.
var ErrInventoryPending = errors.New("utils provenance inventory is not cleared")

// maxWindows bounds the recorded unverified windows. Beyond it the two oldest merge into one that spans both, which only
// widens what a scan treats as suspect.
const maxWindows = 50

// Window is a period in which enforcement was not verified. End is zero while the window is still open.
type Window struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end,omitempty"`
}

func (w Window) contains(t time.Time) bool {
	return !t.Before(w.Start) && (w.End.IsZero() || !t.After(w.End))
}

// State is the persisted record.
type State struct {
	// FirstVerifiedAt is when enforcement was first verified. Everything created before it is pre-enforcement.
	FirstVerifiedAt time.Time `json:"firstVerifiedAt,omitempty"`
	// LastVerifiedAt is the latest time enforcement was observed verified. It is where a gap is considered to start.
	LastVerifiedAt time.Time `json:"lastVerifiedAt,omitempty"`
	// Windows are the periods in which enforcement was not verified after FirstVerifiedAt.
	Windows []Window `json:"windows,omitempty"`
	// ClearedAt is the time of the last scan that found no suspect object. Zero when none, or after a new gap.
	ClearedAt time.Time `json:"clearedAt,omitempty"`
}

// Store persists the State.
type Store interface {
	Load(ctx context.Context) (State, error)
	Save(ctx context.Context, s State) error
}

// MemoryStore is a Store for tests and for runs without persistence.
type MemoryStore struct {
	mu sync.Mutex
	S  State
}

// Load implements Store.
func (m *MemoryStore) Load(context.Context) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.S, nil
}

// Save implements Store.
func (m *MemoryStore) Save(_ context.Context, s State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.S = s
	return nil
}

// Object is an object that carries reserved utils metadata.
type Object struct {
	Kind      string
	Namespace string
	Name      string
	Created   time.Time
	// Modified is the latest time a field manager wrote the reserved metadata (the role label, the infra annotation, an
	// owner reference or the template label) after the object was created, taken from its managed fields. The status
	// subresource is ignored. ModifiedUnknown is true when the object carries reserved metadata that no managed-fields
	// entry accounts for, which is what stripped or forged managed fields look like; it is always suspect.
	Modified        time.Time
	ModifiedUnknown bool
	// ContentVerified is true when the object's content matches the digests NVCA recorded for the instance (the
	// Deployment and ReplicaSet template, or the containers of a pod). It is the proof that an object that existed during
	// an enforcement gap was not changed in it: timestamps and managed fields are not a complete write history.
	ContentVerified bool
	// OnlyInfraAnnotation is true when the infra-object annotation is the only reserved key, as on the legacy utils pod.
	OnlyInfraAnnotation bool
}

func (o Object) String() string {
	return fmt.Sprintf("%s %s/%s created %s", o.Kind, o.Namespace, o.Name, o.Created.UTC().Format(time.RFC3339))
}

// knownInfra are the objects NVCA creates with only the infra annotation. They exist from before the provenance work
// and are expected. Any other object with only the annotation, created in a suspect period, is suspect.
var knownInfra = map[string]bool{"Pod/utils": true, "Pod/nvcf-smb-server": true}

// Suspect reports whether o was created, or had its reserved metadata written, at a time when enforcement was not known to
// hold: before it was first verified, or inside an unverified window. Known legacy infrastructure objects are never
// suspect.
func (s State) Suspect(o Object) bool {
	if o.OnlyInfraAnnotation && knownInfra[o.Kind+"/"+o.Name] {
		return false
	}
	if s.FirstVerifiedAt.IsZero() || o.ModifiedUnknown {
		return true
	}
	// An object created while enforcement held can still have had its reserved metadata changed while it did not, so both
	// the creation and the last write of the reserved metadata must fall in a period when enforcement was verified.
	for _, at := range []time.Time{o.Created, o.Modified} {
		if at.IsZero() {
			continue
		}
		if at.Before(s.FirstVerifiedAt) {
			return true
		}
		for _, w := range s.Windows {
			if w.contains(at) {
				return true
			}
		}
	}
	// An object that was alive during a gap may have been changed in it in ways no timestamp shows, such as a new image
	// in a template that kept its labels and owner. It stays trusted only if its content matches what NVCA recorded.
	if s.existedAcrossAGap(o) && !o.ContentVerified {
		return true
	}
	return false
}

// existedAcrossAGap reports whether o was alive at any time in an unverified window.
func (s State) existedAcrossAGap(o Object) bool {
	for _, w := range s.Windows {
		if w.End.IsZero() || !o.Created.After(w.End) {
			return true
		}
	}
	return false
}

func (s State) openWindow() bool {
	n := len(s.Windows)
	return n > 0 && s.Windows[n-1].End.IsZero()
}

// Cleared reports whether a clean scan covers the current state: enforcement was verified, no gap is open, and the scan
// was taken after the last gap closed.
func (s State) Cleared() (bool, string) {
	switch {
	case s.FirstVerifiedAt.IsZero():
		return false, "enforcement has never been verified"
	case s.openWindow():
		return false, "enforcement is not currently verified"
	case s.ClearedAt.IsZero():
		return false, "no clean inventory scan has been recorded since enforcement was verified"
	}
	if n := len(s.Windows); n > 0 && s.ClearedAt.Before(s.Windows[n-1].End) {
		return false, "the last clean scan is older than the last unverified window"
	}
	return true, ""
}

// Tracker updates the State from readiness observations and scan results, and persists it.
type Tracker struct {
	Store Store
	Now   func() time.Time
	// MaxGap is the longest time between two verified observations that is still treated as continuous. A longer silence
	// (the readiness check stopped, hung, or the process was down) is recorded as an unverified window, because nothing
	// proved enforcement during it. Zero means DefaultMaxGap.
	MaxGap time.Duration

	mu      sync.Mutex
	state   State
	loaded  bool
	version int // incremented on every change, so a scan that raced with a change does not clear
	saved   time.Time
}

// lastVerifiedSaveEvery is how often an unchanged state is persisted. The readiness check runs every two minutes, so every
// verified observation is recorded.
const lastVerifiedSaveEvery = time.Minute

// DefaultMaxGap is three times the default canary interval plus margin.
const DefaultMaxGap = 6 * time.Minute

func (t *Tracker) maxGap() time.Duration {
	if t.MaxGap > 0 {
		return t.MaxGap
	}
	return DefaultMaxGap
}

func (t *Tracker) now() time.Time {
	if t.Now != nil {
		return t.Now()
	}
	return time.Now()
}

func (t *Tracker) load(ctx context.Context) error {
	if t.loaded {
		return nil
	}
	s, err := t.Store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load rollout state: %w", err)
	}
	t.state, t.loaded = s, true
	return nil
}

// State returns a copy of the current state. If it cannot be loaded it returns the zero state, which is not cleared.
func (t *Tracker) State(ctx context.Context) State {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.load(ctx); err != nil {
		return State{}
	}
	return t.state
}

// Observe records whether enforcement is verified right now. Losing verification opens a window that starts at the last
// time it was verified, and invalidates the clearance. Regaining it closes the window. Persisting failures are returned,
// the in-memory state still advances so the gate stays correct.
func (t *Tracker) Observe(ctx context.Context, verified bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.load(ctx); err != nil {
		return err
	}
	now := t.now()
	changed := false
	s := &t.state
	if verified {
		if s.FirstVerifiedAt.IsZero() {
			s.FirstVerifiedAt, changed = now, true
		}
		if s.openWindow() {
			s.Windows[len(s.Windows)-1].End = now
			s.ClearedAt, changed = time.Time{}, true
		} else if !s.LastVerifiedAt.IsZero() && now.Sub(s.LastVerifiedAt) > t.maxGap() {
			// A silence between two verified observations: nothing proved enforcement in between, so it is a gap.
			s.Windows = append(s.Windows, Window{Start: s.LastVerifiedAt, End: now})
			s.ClearedAt, changed = time.Time{}, true
			t.mergeWindows()
		}
		s.LastVerifiedAt = now
	} else if !s.FirstVerifiedAt.IsZero() && !s.openWindow() {
		start := s.LastVerifiedAt
		if start.IsZero() {
			start = s.FirstVerifiedAt
		}
		s.Windows = append(s.Windows, Window{Start: start})
		s.ClearedAt, changed = time.Time{}, true
		t.mergeWindows()
	}
	if changed {
		t.version++
	}
	// Persist on a change, and otherwise at least every lastVerifiedSaveEvery to record LastVerifiedAt. After a restart
	// that value is where the gap starts, so a stale one widens the gap backwards and makes an object created minutes
	// before the restart, while enforcement was in fact verified, look as if it was created inside the gap.
	if changed || now.Sub(t.saved) >= lastVerifiedSaveEvery {
		if err := t.Store.Save(ctx, *s); err != nil {
			return fmt.Errorf("save rollout state: %w", err)
		}
		t.saved = now
	}
	return nil
}

// mergeWindows keeps the window list bounded by merging the two oldest, which only widens what is suspect.
func (t *Tracker) mergeWindows() {
	s := &t.state
	for len(s.Windows) > maxWindows {
		s.Windows = append([]Window{{Start: s.Windows[0].Start, End: s.Windows[1].End}}, s.Windows[2:]...)
	}
}

// Version returns a counter that changes whenever the state does. A scan records it before it lists objects.
func (t *Tracker) Version() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.version
}

// MarkCleared records a clean scan, but only if the state did not change since the scan began (version) and enforcement
// is verified now. Otherwise the clearance is refused.
func (t *Tracker) MarkCleared(ctx context.Context, version int) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.load(ctx); err != nil {
		return false, err
	}
	if version != t.version || t.state.FirstVerifiedAt.IsZero() || t.state.openWindow() {
		return false, nil
	}
	t.state.ClearedAt = t.now()
	t.version++
	if err := t.Store.Save(ctx, t.state); err != nil {
		return true, fmt.Errorf("save rollout state: %w", err)
	}
	t.saved = t.now()
	return true, nil
}

// revokeClearance forgets the last clean scan.
func (t *Tracker) revokeClearance(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.load(ctx); err != nil {
		return err
	}
	if t.state.ClearedAt.IsZero() {
		return nil
	}
	t.state.ClearedAt = time.Time{}
	t.version++
	if err := t.Store.Save(ctx, t.state); err != nil {
		return fmt.Errorf("save rollout state: %w", err)
	}
	t.saved = t.now()
	return nil
}

// Lister returns every object that carries reserved utils metadata in the instance namespaces.
type Lister interface {
	ReservedObjects(ctx context.Context) ([]Object, error)
}

// Scan lists the reserved-metadata objects and returns those created while enforcement was not known to hold. A scan
// with no suspects marks the inventory cleared. Suspects, a failed listing, or a state change during the scan leave it
// not cleared.
func (t *Tracker) Scan(ctx context.Context, l Lister) (suspects []Object, err error) {
	version := t.Version()
	objs, err := l.ReservedObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("list reserved objects: %w", err)
	}
	state := t.State(ctx)
	for _, o := range objs {
		if state.Suspect(o) {
			suspects = append(suspects, o)
		}
	}
	sort.Slice(suspects, func(i, j int) bool { return suspects[i].String() < suspects[j].String() })
	if len(suspects) > 0 {
		// A suspect found now revokes an earlier clearance: content can fail to match at any time, not only in a window.
		if err := t.revokeClearance(ctx); err != nil {
			return suspects, err
		}
		return suspects, nil
	}
	if _, err := t.MarkCleared(ctx, version); err != nil {
		return nil, err
	}
	return nil, nil
}

// Gate opens only when readiness passes and the inventory is cleared. It implements readiness.Checkable.
type Gate struct {
	Readiness readiness.Checkable
	Tracker   *Tracker
}

// Check returns nil only when enforcement and the instance RBAC are verified and fresh, and a clean inventory scan covers
// the current state. A nil Tracker means the inventory cannot be cleared, so the gate refuses.
func (g Gate) Check() error {
	if err := readiness.CheckOrMissing(g.Readiness); err != nil {
		return err
	}
	if g.Tracker == nil {
		return fmt.Errorf("%w: no inventory tracker is configured", ErrInventoryPending)
	}
	if ok, why := g.Tracker.State(context.Background()).Cleared(); !ok {
		return fmt.Errorf("%w: %s", ErrInventoryPending, why)
	}
	return nil
}
