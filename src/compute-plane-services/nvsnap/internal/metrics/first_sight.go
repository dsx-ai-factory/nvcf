// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"net"
	"net/http"
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
// applied once Prometheus has scraped the zero. That is known from the
// scrapes themselves (Scraped): the held observations apply once every
// scraper seen scraping regularly has scraped since the series was
// created, about one scrape interval. FirstSightHold bounds the wait, for
// an agent no scraper has been seen to scrape yet. Only series that occur
// exist, and every event counts. An agent that stops while observations
// are held loses them, as it loses any increase no scrape has read.

// FirstSightHold is the longest a new series' observations are held: four
// scrapes at the chart's 30 s interval, three at the 40 s the chart allows
// at most (templates/agent-servicemonitor.yaml; first_sight_test.go keeps
// the two in step).
const FirstSightHold = 2 * time.Minute

// held is a series' first-sight state.
type held struct {
	created  time.Time
	released bool
	pending  []func()
}

// scraper is one client that scrapes /metrics, by address.
type scraper struct {
	last   time.Time // when its last scrape began
	before time.Time // when the scrape before that began
}

type firstSight struct {
	mu       sync.Mutex
	series   map[string]*held
	scrapers map[string]*scraper
	now      func() time.Time
	after    func(time.Duration, func())
	hold     time.Duration
}

func newFirstSight() *firstSight {
	return &firstSight{
		series:   map[string]*held{},
		scrapers: map[string]*scraper{},
		now:      time.Now,
		after:    func(d time.Duration, f func()) { time.AfterFunc(d, f) },
		hold:     FirstSightHold,
	}
}

// sight is shared by the lifecycle metrics.
var sight = newFirstSight()

// observe applies f to the series metric{labels}: at once for a series
// released, else once it is (a scrape has read its zero, or the hold
// ended), creating the series first with create (at zero) on its first
// observation.
func (s *firstSight) observe(metric string, labels []string, create, f func()) {
	key := metric + "\xff" + strings.Join(labels, "\xff")
	s.mu.Lock()
	h := s.series[key]
	if h == nil {
		// Created under the lock, so a scrape that began after created
		// read the zero.
		create()
		h = &held{created: s.now()}
		s.series[key] = h
		if s.hold > 0 {
			s.after(s.hold, func() { s.release(key) })
		} else {
			h.released = true
		}
	}
	if !h.released {
		h.pending = append(h.pending, f)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	f()
}

// release applies the observations held for the series key.
func (s *firstSight) release(key string) {
	s.mu.Lock()
	h := s.series[key]
	if h == nil || h.released {
		s.mu.Unlock()
		return
	}
	h.released = true
	fs := h.pending
	h.pending = nil
	s.mu.Unlock()
	for _, f := range fs {
		f()
	}
}

// scraped records a scrape by from that began at began and was served,
// and releases every held series each regular scraper has scraped since
// its creation. A scraper is regular once seen twice, its last scrape
// within the hold: so a one-off read (a person's curl) neither releases a
// series nor holds one, and a scraper that went away stops counting.
func (s *firstSight) scraped(from string, began time.Time) {
	s.mu.Lock()
	now := s.now()
	sc := s.scrapers[from]
	if sc == nil {
		sc = &scraper{}
		s.scrapers[from] = sc
	}
	sc.before, sc.last = sc.last, began
	var regular []*scraper
	for k, c := range s.scrapers {
		switch {
		case now.Sub(c.last) > s.hold:
			delete(s.scrapers, k)
		case !c.before.IsZero():
			regular = append(regular, c)
		}
	}
	var fs []func()
	if len(regular) > 0 {
		for _, h := range s.series {
			if h.released {
				continue
			}
			read := true
			for _, c := range regular {
				read = read && c.last.After(h.created)
			}
			if read {
				h.released = true
				fs = append(fs, h.pending...)
				h.pending = nil
			}
		}
	}
	s.mu.Unlock()
	for _, f := range fs {
		f()
	}
}

// Scraped wraps the /metrics handler h so held observations apply once
// the scrapers have read their series' zero (see firstSight.scraped). A
// scrape counts once served whole (status 200).
func Scraped(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		began := sight.now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(rec, r)
		if rec.status != http.StatusOK || r.Context().Err() != nil {
			return
		}
		from := r.RemoteAddr
		if host, _, err := net.SplitHostPort(from); err == nil {
			from = host
		}
		sight.scraped(from, began)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
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
