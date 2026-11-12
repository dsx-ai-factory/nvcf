// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strings"
	"sync"
	"time"
)

// A labelled series exists only from its first observation, and
// Prometheus's increase() and rate() cannot count the observation that
// created it: a counter first scraped at 1 has no 0 before it. On a cluster
// where a node captures or restores a few times a day, that is most of the
// events, every node's first failure among them.
//
// So a series' first observation is held: the series is created at zero at
// once, and that observation, with any other made while it is held, is
// applied once Prometheus has scraped the zero (FirstSightHold, several
// scrape intervals). Only series that occur exist, and every event counts.
// An agent that stops within the hold loses the held observations.

// FirstSightHold is how long a new series' observations are held: four
// scrapes at the chart's 30 s interval, three at the 40 s the chart allows
// at most (templates/agent-servicemonitor.yaml; first_sight_test.go keeps
// the two in step).
const FirstSightHold = 2 * time.Minute

type firstSight struct {
	mu    sync.Mutex
	until map[string]time.Time // series -> when its observations apply at once
	now   func() time.Time
	after func(time.Duration, func())
	hold  time.Duration
}

func newFirstSight() *firstSight {
	return &firstSight{
		until: map[string]time.Time{},
		now:   time.Now,
		after: func(d time.Duration, f func()) { time.AfterFunc(d, f) },
		hold:  FirstSightHold,
	}
}

// sight is shared by the lifecycle metrics.
var sight = newFirstSight()

// observe applies f to the series metric{labels}: at once for a series
// seen before its hold ended, else after the hold, creating the series
// first with create (at zero) on its first observation.
func (s *firstSight) observe(metric string, labels []string, create, f func()) {
	key := metric + "\xff" + strings.Join(labels, "\xff")
	s.mu.Lock()
	now := s.now()
	until, seen := s.until[key]
	if !seen {
		until = now.Add(s.hold)
		s.until[key] = until
	}
	s.mu.Unlock()
	if !seen {
		create()
	}
	if wait := until.Sub(now); wait > 0 {
		s.after(wait, f)
		return
	}
	f()
}

// SetFirstSightHold sets the hold and returns the one before: tests that
// read a metric right after observing it set 0.
func SetFirstSightHold(d time.Duration) time.Duration {
	sight.mu.Lock()
	defer sight.mu.Unlock()
	prev := sight.hold
	sight.hold = d
	return prev
}
