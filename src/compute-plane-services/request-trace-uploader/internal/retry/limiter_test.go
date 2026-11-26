// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLimiterBudget(t *testing.T) {
	tests := []struct {
		name  string
		burst int
		at    []time.Duration
		want  []time.Duration
	}{
		{
			name:  "default budget spaces creates two seconds apart",
			burst: 1,
			at:    []time.Duration{0, 0, 0},
			want:  []time.Duration{0, 2 * time.Second, 4 * time.Second},
		},
		{
			name:  "a minute of backlog drains at the budget",
			burst: 1,
			at:    make([]time.Duration, 30),
			want:  nil,
		},
		{
			name:  "burst starts back to back, then spaces",
			burst: 3,
			at:    []time.Duration{0, 0, 0, 0},
			want:  []time.Duration{0, 0, 0, 2 * time.Second},
		},
		{
			name:  "idle time refills up to the burst only",
			burst: 2,
			at:    []time.Duration{0, time.Hour, time.Hour, time.Hour},
			want:  []time.Duration{0, 0, 0, 2 * time.Second},
		},
		{
			name:  "time after a wait is not borrowed twice",
			burst: 1,
			at:    []time.Duration{0, 0, 10 * time.Second},
			want:  []time.Duration{0, 2 * time.Second, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := NewLimiter(30, tt.burst)
			var last time.Duration
			for i, at := range tt.at {
				last = limiter.Reserve(t0.Add(at))
				if tt.want != nil && last != tt.want[i] {
					t.Fatalf("create %d at +%v: wait %v, want %v", i+1, at, last, tt.want[i])
				}
			}
			if tt.want == nil && last != 58*time.Second {
				t.Fatalf("thirtieth create waits %v, want 58s", last)
			}
		})
	}
}

func TestLimiterWait(t *testing.T) {
	limiter := NewLimiter(6000, 1)
	start := time.Now()
	for i := 0; i < 3; i++ {
		if err := limiter.Wait(context.Background()); err != nil {
			t.Fatalf("Wait() = %v", err)
		}
	}
	if waited := time.Since(start); waited < 20*time.Millisecond {
		t.Fatalf("three creates at 10ms spacing took %v, want at least 20ms", waited)
	}

	slow := NewLimiter(1, 1)
	if err := slow.Wait(context.Background()); err != nil {
		t.Fatalf("first Wait() = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := slow.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait() past the budget = %v, want the context error", err)
	}
	if wait := slow.Reserve(time.Now()); wait > 61*time.Second {
		t.Fatalf("a canceled wait kept its create: next wait %v, want at most one interval", wait)
	}
}

func TestLimiterCancelDoesNotDoubleBook(t *testing.T) {
	limiter := NewLimiter(1, 1)
	if err := limiter.Wait(context.Background()); err != nil {
		t.Fatalf("first Wait() = %v", err)
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	firstDone := make(chan error, 1)
	go func() { firstDone <- limiter.Wait(firstCtx) }()
	waitForReservations(t, limiter, 2)
	go func() { _ = limiter.Wait(secondCtx) }()
	waitForReservations(t, limiter, 3)

	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Wait() = %v, want context.Canceled", err)
	}
	if wait := limiter.Reserve(time.Now()); wait < 170*time.Second {
		t.Fatalf("next create waits %v, want it after the still-pending one at about 120s", wait)
	}
}

// Queued creates canceled in any order all go back to the budget once none
// is left behind them, so a canceled backlog leaves no debt.
func TestLimiterReclaimsAQueueCanceledOutOfOrder(t *testing.T) {
	limiter := NewLimiter(1, 1)
	if wait, _ := limiter.reserve(t0); wait != 0 {
		t.Fatalf("first create waits %v, want 0", wait)
	}
	_, first := limiter.reserve(t0)
	_, second := limiter.reserve(t0)
	limiter.cancel(first)
	limiter.cancel(second)
	if wait := limiter.Reserve(t0); wait != time.Minute {
		t.Fatalf("next create waits %v, want one interval: both canceled slots are free", wait)
	}
}

// A canceled create whose slot a later create already used stays spent, so
// the budget never gives one slot to two creates.
func TestLimiterKeepsASlotALaterCreateUsed(t *testing.T) {
	limiter := NewLimiter(1, 1)
	limiter.Reserve(t0)
	_, first := limiter.reserve(t0)
	limiter.reserve(t0) // started at its slot, two intervals out
	limiter.cancel(first)
	if wait := limiter.Reserve(t0); wait != 3*time.Minute {
		t.Fatalf("next create waits %v, want three intervals: the slot after the canceled one is in use", wait)
	}
}

// waitForReservations blocks until the limiter has made n reservations, so
// a test acts on queued waits instead of guessing when their goroutines ran.
func waitForReservations(t *testing.T, l *Limiter, n uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		l.mu.Lock()
		made := l.seq
		l.mu.Unlock()
		if made >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("limiter made %d reservations, want %d", made, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// A create taken with Reserve can never be canceled, so canceled creates
// before it can never go back either and are not kept.
func TestLimiterReserveDropsCanceledCreatesBehindIt(t *testing.T) {
	limiter := NewLimiter(1, 1)
	limiter.Reserve(t0)
	for range 3 {
		_, seq := limiter.reserve(t0)
		limiter.reserve(t0)
		limiter.cancel(seq)
	}
	limiter.Reserve(t0)
	limiter.mu.Lock()
	kept := len(limiter.canceled)
	limiter.mu.Unlock()
	if kept != 0 {
		t.Fatalf("limiter keeps %d canceled creates behind a reserved one, want 0", kept)
	}
}
