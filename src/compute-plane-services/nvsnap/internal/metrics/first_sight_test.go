// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// fakeSight is a firstSight on a clock the test moves; held observations
// run when the clock passes their time.
type fakeSight struct {
	*firstSight
	clock time.Time
	held  []heldObservation
}

type heldObservation struct {
	at time.Time
	f  func()
}

func newFakeSight(hold time.Duration) *fakeSight {
	s := &fakeSight{firstSight: newFirstSight(), clock: time.Unix(1_000_000, 0)}
	s.hold = hold
	s.now = func() time.Time { return s.clock }
	s.after = func(d time.Duration, f func()) { s.held = append(s.held, heldObservation{s.clock.Add(d), f}) }
	return s
}

func (s *fakeSight) advance(d time.Duration) {
	s.clock = s.clock.Add(d)
	var left []heldObservation
	for _, h := range s.held {
		if !h.at.After(s.clock) {
			h.f()
		} else {
			left = append(left, h)
		}
	}
	s.held = left
}

func TestFirstSightCreatesTheSeriesAtZeroBeforeItsFirstObservation(t *testing.T) {
	s := newFakeSight(2 * time.Minute)
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total"}, []string{"outcome"})
	inc := func() {
		s.observe("c", []string{"failed"}, func() { c.WithLabelValues("failed") }, func() { c.WithLabelValues("failed").Inc() })
	}

	inc()
	if n := len(series(t, c)); n != 1 {
		t.Fatalf("%d series after the first observation, want the one, at zero", n)
	}
	if v := value(t, c, "failed"); v != 0 {
		t.Fatalf("first observation applied at once (%v): a scrape would never see the series at 0, and increase() would not count it", v)
	}
	s.advance(time.Minute)
	inc() // a second one while the first is held is held too
	if v := value(t, c, "failed"); v != 0 {
		t.Fatalf("an observation within the hold applied at once (%v)", v)
	}
	s.advance(time.Minute)
	if v := value(t, c, "failed"); v != 2 {
		t.Fatalf("after the hold the series is %v, want both observations (2)", v)
	}
	inc()
	if v := value(t, c, "failed"); v != 3 {
		t.Fatalf("an observation after the hold is %v, want it applied at once (3)", v)
	}
}

func TestFirstSightHoldsEachSeriesOnItsOwn(t *testing.T) {
	s := newFakeSight(2 * time.Minute)
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "h_seconds"}, []string{"phase"})
	obs := func(phase string) {
		s.observe("h", []string{phase}, func() { h.WithLabelValues(phase) }, func() { h.WithLabelValues(phase).Observe(1) })
	}
	obs("gpu")
	s.advance(3 * time.Minute)
	obs("gpu")  // seen: at once
	obs("criu") // new: held
	if got := samples(t, h, "gpu"); got != 2 {
		t.Fatalf("gpu has %d samples, want 2", got)
	}
	if got := samples(t, h, "criu"); got != 0 {
		t.Fatalf("a new series' first sample applied at once (%d)", got)
	}
	// The same label values under another metric are another series.
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "c_total"}, []string{"phase"})
	s.observe("c", []string{"gpu"}, func() { c.WithLabelValues("gpu") }, func() { c.WithLabelValues("gpu").Inc() })
	if v := value(t, c, "gpu"); v != 0 {
		t.Fatalf("another metric's series with the same labels was not held (%v)", v)
	}
}

// servicemonitorTemplate is the chart's ServiceMonitor, whose guard
// refuses scrape intervals the hold could not cover.
const servicemonitorTemplate = "../../deploy/helm/nvsnap/templates/agent-servicemonitor.yaml"

func TestFirstSightHoldCoversSeveralScrapes(t *testing.T) {
	raw, err := os.ReadFile(servicemonitorTemplate)
	if err != nil {
		t.Fatal(err)
	}
	// The guard's pattern: whole seconds up to its last alternative.
	m := regexp.MustCompile(`regexMatch "\^\(\[1-9\]\|\[1-3\]\[0-9\]\|(\d+)\)s\$"`).FindSubmatch(raw)
	if m == nil {
		t.Fatalf("%s has no scrape-interval guard", servicemonitorTemplate)
	}
	maxInterval, _ := strconv.Atoi(string(m[1]))
	if FirstSightHold < 3*time.Duration(maxInterval)*time.Second {
		t.Fatalf("FirstSightHold %s covers fewer than three of the %ds scrapes the chart allows", FirstSightHold, maxInterval)
	}
}

func TestObservePodStartAndTeardown(t *testing.T) {
	start := func() uint64 { return samples(t, PodStartSeconds, MethodCRIU, "GB300", "2-4") }
	teardowns := func(o string) float64 { return value(t, TeardownOutcomes, o, "GB300", "2-4") }
	timed := func() uint64 { return samples(t, TeardownSeconds, "GB300", "2-4") }
	s0, r0, d0, t0 := start(), teardowns(TeardownReleased), teardowns(TeardownDeadline), timed()

	ObservePodStart(MethodCRIU, "GB300", 4, 95*time.Second)
	ObservePodStart(MethodCRIU, "GB300", 4, -time.Second)
	if got := start() - s0; got != 1 {
		t.Fatalf("pod start observed +%d, want +1 (the negative one dropped)", got)
	}
	ObserveTeardown(TeardownReleased, "GB300", 4, 125*time.Second)
	ObserveTeardown(TeardownDeadline, "GB300", 4, 0)
	if got := teardowns(TeardownReleased) - r0; got != 1 {
		t.Fatalf("released teardowns +%v, want +1", got)
	}
	if got := teardowns(TeardownDeadline) - d0; got != 1 {
		t.Fatalf("deadline teardowns +%v, want +1", got)
	}
	if got := timed() - t0; got != 1 {
		t.Fatalf("teardown times +%d, want only the released one's", got)
	}
}
