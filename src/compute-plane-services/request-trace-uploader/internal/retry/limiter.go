// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"context"
	"sync"
	"time"
)

// Limiter is the per-pod budget for creating jobs: a token bucket that
// refills at a fixed rate and holds at most burst tokens. It also caps how
// fast a backlog drains after a hold, since every create draws from it.
type Limiter struct {
	interval time.Duration
	burst    float64
	now      func() time.Time

	mu     sync.Mutex
	tokens float64
	last   time.Time
	// seq numbers reservations, so a canceled one is refunded only when no
	// later reservation is scheduled behind it.
	seq uint64
	// canceled holds canceled reservations that still had a later one
	// scheduled behind them. Each goes back once every reservation after it
	// is canceled too.
	canceled map[uint64]bool
}

// NewLimiter builds a budget of perMinute creates with the given burst.
// Config.Validate requires both to be at least 1; a lower value is treated
// as 1 here so a direct call cannot divide by zero. The bucket starts full.
func NewLimiter(perMinute, burst int) *Limiter {
	return &Limiter{
		interval: time.Minute / time.Duration(max(perMinute, 1)),
		burst:    float64(max(burst, 1)),
		now:      time.Now,
	}
}

// Reserve takes one create from the budget at now and returns how long the
// caller must wait before starting it.
func (l *Limiter) Reserve(now time.Time) time.Duration {
	wait, seq := l.reserve(now)
	// A reserved create cannot be canceled, so the canceled ones before it
	// can never go back.
	l.started(seq)
	return wait
}

func (l *Limiter) reserve(now time.Time) (time.Duration, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.last.IsZero() {
		l.tokens, l.last = l.burst, now
	}
	if elapsed := now.Sub(l.last); elapsed > 0 {
		l.tokens = min(l.burst, l.tokens+float64(elapsed)/float64(l.interval))
		l.last = now
	}
	l.tokens--
	l.seq++
	if l.tokens >= 0 {
		return 0, l.seq
	}
	return time.Duration(-l.tokens * float64(l.interval)), l.seq
}

// Wait takes one create from the budget and blocks until it may start. If ctx
// ends first, the create goes back to the budget once no later reservation
// is scheduled behind it: returning it earlier would give its slot to two
// creates.
func (l *Limiter) Wait(ctx context.Context) error {
	wait, seq := l.reserve(l.now())
	if wait > 0 {
		if err := sleep(ctx, wait); err != nil {
			l.cancel(seq)
			return err
		}
	}
	l.started(seq)
	return nil
}

// cancel records that reservation seq was canceled, then returns to the
// budget every canceled reservation at the end of the queue, latest first,
// so a backlog canceled in any order leaves no debt.
func (l *Limiter) cancel(seq uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.canceled == nil {
		l.canceled = map[uint64]bool{}
	}
	l.canceled[seq] = true
	for l.canceled[l.seq] {
		delete(l.canceled, l.seq)
		l.seq--
		l.tokens++
	}
}

// started records that reservation seq began its create. A canceled
// reservation before it can no longer go back, because its slot is behind
// one in use.
func (l *Limiter) started(seq uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for canceled := range l.canceled {
		if canceled < seq {
			delete(l.canceled, canceled)
		}
	}
}
