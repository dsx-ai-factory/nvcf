/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

package ratelimit

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
)

func TestDoResourceLimitEnforcesRequestWindows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		dimension LimitDimension
		limit     ResourceLimit
	}{
		{"requests per second", RequestsPerSecond, ResourceLimit{SubjectKey: "r1", RequestsPerSecond: 1}},
		{"requests per minute", RequestsPerMinute, ResourceLimit{SubjectKey: "r2", RequestsPerMinute: 1}},
		{"requests per hour", RequestsPerHour, ResourceLimit{SubjectKey: "r3", RequestsPerHour: 1}},
		{"requests per day", RequestsPerDay, ResourceLimit{SubjectKey: "r4", RequestsPerDay: 1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			limiter, err := NewRateLimiter(newFakeStore())
			if err != nil {
				t.Fatalf("new rate limiter: %v", err)
			}
			request := ResourceRequest{Requests: 1}

			first, err := ConsumeResourceLimit(context.Background(), limiter, tc.limit, request, "req-1")
			if err != nil {
				t.Fatalf("first consume: %v", err)
			}
			if first[tc.dimension] == nil || !first[tc.dimension].Allowed() {
				t.Fatalf("first request in %s should be allowed: %#v", tc.dimension, first)
			}

			second, err := ConsumeResourceLimit(context.Background(), limiter, tc.limit, request, "req-2")
			if err != nil {
				t.Fatalf("second consume: %v", err)
			}
			if second[tc.dimension] == nil || second[tc.dimension].Allowed() {
				t.Fatalf("second request in %s should be refused: %#v", tc.dimension, second)
			}
		})
	}
}

func TestConsumeResourceLimitAllowsExactlyTheLimitUnderParallelRequests(t *testing.T) {
	t.Parallel()

	limiter, err := NewRateLimiter(newFakeStore())
	if err != nil {
		t.Fatalf("new rate limiter: %v", err)
	}
	limit := ResourceLimit{SubjectKey: "parallel", RequestsPerMinute: 5}

	var (
		allowed atomic.Int32
		wg      sync.WaitGroup
	)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results, err := ConsumeResourceLimit(context.Background(), limiter, limit, ResourceRequest{Requests: 1}, "req")
			if err != nil {
				t.Errorf("consume: %v", err)
				return
			}
			if results[RequestsPerMinute].Allowed() {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := allowed.Load(); got != 5 {
		t.Fatalf("allowed %d parallel requests, want exactly 5", got)
	}
}

func TestRefundResourceLimitGivesBackOnlyWhatAllowedWindowsTook(t *testing.T) {
	t.Parallel()

	limiter, err := NewRateLimiter(newFakeStore())
	if err != nil {
		t.Fatalf("new rate limiter: %v", err)
	}
	limit := ResourceLimit{SubjectKey: "refund", RequestsPerMinute: 1, RequestsPerDay: 10}
	request := ResourceRequest{Requests: 1}

	if _, err := ConsumeResourceLimit(context.Background(), limiter, limit, request, "req-1"); err != nil {
		t.Fatalf("first consume: %v", err)
	}
	refused, err := ConsumeResourceLimit(context.Background(), limiter, limit, request, "req-2")
	if err != nil {
		t.Fatalf("second consume: %v", err)
	}
	if refused[RequestsPerMinute].Allowed() || !refused[RequestsPerDay].Allowed() {
		t.Fatalf("second request should be refused by the minute window only: %#v", refused)
	}

	if err := RefundResourceLimit(context.Background(), limiter, limit, refused, "req-2"); err != nil {
		t.Fatalf("refund: %v", err)
	}
	if got := refused[RequestsPerDay].RemainingValue(); got != 9 {
		t.Fatalf("refunded day result reports %d remaining, want 9", got)
	}

	stored, err := TestResourceLimit(context.Background(), limiter, limit, request, "probe")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if got := stored[RequestsPerDay].CurrentValue; got != 9 {
		t.Fatalf("day window holds %d, want 9: the refused request must not stay charged", got)
	}
	if got := stored[RequestsPerMinute].CurrentValue; got != 0 {
		t.Fatalf("minute window holds %d, want 0: the window that refused took nothing, so it gets nothing back", got)
	}
}

func TestRefundResourceLimitSkipsAFailOpenResult(t *testing.T) {
	t.Parallel()

	limiter, err := NewRateLimiter(newFakeStore())
	if err != nil {
		t.Fatalf("new rate limiter: %v", err)
	}
	limit := ResourceLimit{SubjectKey: "refund-fail-open", RequestsPerDay: 10}
	if _, err := ConsumeResourceLimit(context.Background(), limiter, limit, ResourceRequest{Requests: 1}, "req-1"); err != nil {
		t.Fatalf("consume: %v", err)
	}

	// A fail-open check reports an unlimited budget and charges nothing.
	failedOpen := map[LimitDimension]*RateLimitResult{
		RequestsPerDay: {CurrentValue: math.MaxInt64, Requested: 1, RateLimit: RateLimit{Limit: 10, Period: day}},
	}
	if err := RefundResourceLimit(context.Background(), limiter, limit, failedOpen, "req-2"); err != nil {
		t.Fatalf("refund: %v", err)
	}

	stored, err := TestResourceLimit(context.Background(), limiter, limit, ResourceRequest{Requests: 1}, "probe")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if got := stored[RequestsPerDay].CurrentValue; got != 9 {
		t.Fatalf("day window holds %d, want 9: a fail-open result took nothing, so it gets nothing back", got)
	}
}
