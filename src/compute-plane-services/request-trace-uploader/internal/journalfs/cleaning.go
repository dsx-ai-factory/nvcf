// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package journalfs

import (
	"fmt"
	"slices"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
)

// BeginCleaning moves a Confirmed segment to Cleaning. Cleanup calls it
// before deleting anything, and calls it again after a restart, so it is a
// no-op on a segment already Cleaning.
func (s *Store) BeginCleaning(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seg, ok := s.segments[id]; ok && seg.State == journal.StateCleaning {
		return nil
	}
	return s.updateSegmentLocked(id, func(seg *journal.Segment) error {
		if err := transition(seg, journal.StateCleaning); err != nil {
			return err
		}
		seg.CleaningAt = s.now()
		return nil
	})
}

// FinishCleaning marks an uploaded segment Done once its transports and
// source are gone. The entry stays as a tombstone until Sweep removes it.
func (s *Store) FinishCleaning(id string) error {
	return s.finish(id, journal.ReasonUploaded, journal.StateCleaning)
}

// FinishNotExported marks a segment Done that a backend that does not export
// finished: from Cleaning after confirmed submits that did not export, or
// from Empty when nothing was left to send. Its transports are gone and its
// source is kept, so Reopen can send it back when a backend that exports
// runs.
func (s *Store) FinishNotExported(id string) error {
	return s.finish(id, journal.ReasonNotExported, journal.StateCleaning, journal.StateEmpty)
}

// Reopen sends a segment finished without an export back to Discovered with
// no backend, transports or receipts, so it is prepared and uploaded again.
// Only such a segment can be reopened: a dry-run receipt never stands for an
// export.
func (s *Store) Reopen(id string) (journal.Segment, error) {
	var reopened journal.Segment
	err := s.updateSegment(id, func(seg *journal.Segment) error {
		if seg.State != journal.StateDone || seg.Reason != journal.ReasonNotExported {
			return fmt.Errorf("%w: segment %s is %s (%s), not finished without an export", journal.ErrInvalidTransition, shortID(seg.ID), seg.State, seg.Reason)
		}
		seg.State = journal.StateDiscovered
		seg.Reason = ""
		seg.Backend = ""
		// Export latency starts again here, not at the first discovery.
		seg.DiscoveredAt = s.now()
		seg.Destinations = nil
		seg.PrepareStartedAt = time.Time{}
		seg.PreparedAt = time.Time{}
		seg.ConfirmedAt = time.Time{}
		seg.CleaningAt = time.Time{}
		seg.DoneAt = time.Time{}
		reopened = copySegment(seg)
		return nil
	})
	return reopened, err
}

// FinishEmpty marks an empty segment Done once its source is gone.
func (s *Store) FinishEmpty(id string) error {
	return s.finish(id, journal.ReasonEmpty, journal.StateEmpty)
}

func (s *Store) finish(id, reason string, from ...journal.State) error {
	return s.updateSegment(id, func(seg *journal.Segment) error {
		if !slices.Contains(from, seg.State) {
			return fmt.Errorf("%w: segment %s is %s, not %v", journal.ErrInvalidTransition, shortID(seg.ID), seg.State, from)
		}
		if err := transition(seg, journal.StateDone); err != nil {
			return err
		}
		seg.Reason = reason
		seg.DoneAt = s.now()
		return nil
	})
}

// Evict marks a segment Done after the disk guard deleted it to free space.
// This is the one transition outside the state machine table: it records a
// counted loss from any state. Evicting a Done segment is a no-op.
func (s *Store) Evict(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seg, ok := s.segments[id]; ok && seg.State == journal.StateDone {
		return nil
	}
	return s.updateSegmentLocked(id, func(seg *journal.Segment) error {
		seg.State = journal.StateDone
		seg.Reason = journal.ReasonEvicted
		seg.DoneAt = s.now()
		return nil
	})
}
