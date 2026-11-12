// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/journal"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// unpaced lets every create start at once, for tests that are not about
// create pacing.
var unpaced = map[string]string{config.EnvCreateBurst: "100"}

// tunedUploader builds an uploader from env alone, with no fault handling
// override, so the settings under test are the ones that apply.
func tunedUploader(t *testing.T, spool *uploadertest.Spool, client backend.Client, env map[string]string) *service.Uploader {
	t.Helper()
	uploader, err := service.NewUploader(loadEnv(t, spoolEnv(t, spool.Dir, env)), client, service.Options{})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	t.Cleanup(func() { _ = uploader.Close() })
	return uploader
}

func with(base map[string]string, extra map[string]string) map[string]string {
	env := maps.Clone(base)
	maps.Copy(env, extra)
	return env
}

// jobSizes lists how many files each created job carried, largest first.
func jobSizes(fake *backendtest.Fake) []int {
	var sizes []int
	for _, job := range fake.Jobs() {
		sizes = append(sizes, len(job.Files))
	}
	slices.Sort(sizes)
	slices.Reverse(sizes)
	return sizes
}

func oneRecordSegments(n int) [][]string {
	segments := make([][]string, n)
	for i := range segments {
		segments[i] = []string{uploadertest.RequestEnd(strings.Repeat("r", i+1), uint64(i+1))}
	}
	return segments
}

func TestBatchMaxSegmentsLimitsABatch(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want []int
	}{
		{"unset keeps 16", nil, []int{5}},
		{"two", map[string]string{config.EnvBatchMaxSegments: "2"}, []int{2, 2, 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spool, _ := spoolWith(t, oneRecordSegments(5)...)
			fake := batchingFake()
			uploader := tunedUploader(t, spool, fake, with(unpaced, tc.env))
			if err := uploader.Pass(context.Background()); err != nil {
				t.Fatalf("Pass() error = %v", err)
			}
			if got := jobSizes(fake); !slices.Equal(got, tc.want) {
				t.Fatalf("files per job = %v, want %v", got, tc.want)
			}
		})
	}
}

// largeRecord is a request record whose text does not compress, so its
// transport is about size bytes.
func largeRecord(t *testing.T, id string, size int) string {
	t.Helper()
	raw := make([]byte, size)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	text := base64.StdEncoding.EncodeToString(raw)
	return `{"schema":"dynamo.request.trace.v1","event_type":"request_payload","event_time_unix_ms":2,"payload":{"request_id":"` + id +
		`","request":{"messages":[{"role":"user","content":"` + text + `"}]}}}`
}

func TestBatchMaxBytesLimitsABatch(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want []int
	}{
		{"unset keeps 1 GiB", nil, []int{3}},
		// Each transport is about 700 KB, so two do not fit in 1 MiB.
		{"1 MiB", map[string]string{config.EnvBatchMaxBytes: "1048576"}, []int{1, 1, 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spool, _ := spoolWith(t,
				[]string{largeRecord(t, "a", 700_000)},
				[]string{largeRecord(t, "b", 700_000)},
				[]string{largeRecord(t, "c", 700_000)},
			)
			fake := batchingFake()
			// Redaction would replace the random text, which looks like
			// inline media, with a short marker.
			env := with(unpaced, map[string]string{config.EnvRedactMultimodal: "false"})
			uploader := tunedUploader(t, spool, fake, with(env, tc.env))
			if err := uploader.Pass(context.Background()); err != nil {
				t.Fatalf("Pass() error = %v", err)
			}
			if got := jobSizes(fake); !slices.Equal(got, tc.want) {
				t.Fatalf("files per job = %v, want %v", got, tc.want)
			}
		})
	}
}

// The backend here takes at most 2 MiB and 8 files in one job.
func TestBatchSettingsAboveTheBackendLimitStopStartup(t *testing.T) {
	tests := []struct {
		name    string
		setting string
		value   string
		wantErr bool
	}{
		{"bytes unset uses the backend limit", config.EnvBatchMaxBytes, "", false},
		{"bytes at the backend limit", config.EnvBatchMaxBytes, "2097152", false},
		{"bytes above the backend limit", config.EnvBatchMaxBytes, "2097153", true},
		{"segments unset uses the backend limit", config.EnvBatchMaxSegments, "", false},
		{"segments at the backend limit", config.EnvBatchMaxSegments, "8", false},
		{"segments above the backend limit", config.EnvBatchMaxSegments, "9", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
			fake := batchingFake()
			caps := fake.Capabilities()
			caps.MaxBatchBytes = 2 << 20
			caps.MaxBatchFiles = 8
			fake.SetCapabilities(caps)
			env := map[string]string{}
			if tc.value != "" {
				env[tc.setting] = tc.value
			}
			uploader, err := service.NewUploader(loadEnv(t, spoolEnv(t, spool.Dir, env)), fake, service.Options{})
			if uploader != nil {
				t.Cleanup(func() { _ = uploader.Close() })
			}
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("NewUploader() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.setting) {
				t.Fatalf("NewUploader() error = %v, want a startup error that names %s", err, tc.setting)
			}
			// A one-character value can appear inside the limit the
			// message states, so only longer values are checked.
			if len(tc.value) > 1 && strings.Contains(err.Error(), tc.value) {
				t.Fatalf("NewUploader() error = %q, echoes the value", err)
			}
		})
	}
}

// timedClient records when each Submit starts.
type timedClient struct {
	*backendtest.Fake
	mu     sync.Mutex
	starts []time.Time
}

func (c *timedClient) Submit(ctx context.Context, request backend.SubmitRequest) (string, error) {
	c.mu.Lock()
	c.starts = append(c.starts, time.Now())
	c.mu.Unlock()
	return c.Fake.Submit(ctx, request)
}

// within counts the creates that started within window of the first.
func (c *timedClient) within(window time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, start := range c.starts {
		if start.Sub(c.starts[0]) <= window {
			n++
		}
	}
	return n
}

// TestCreatePacingFollowsTheSettings runs one pass over four segments, each
// its own job, and counts the creates that started within 1.5 s of the
// first, by their start times. Startup, preparing and when the test looks
// do not count. Under each setting the next create after that window is
// due at least 2 s after the first, and under the defaults the second is,
// so a setting that does not apply changes the count.
func TestCreatePacingFollowsTheSettings(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want int
	}{
		// 30 a minute is one every 2 s.
		{"unset keeps 30 a minute and no burst", nil, 1},
		// The fourth create is due a minute after the burst.
		{"a burst of 3 at 1 a minute", map[string]string{config.EnvCreatesPerMinute: "1", config.EnvCreateBurst: "3"}, 3},
		// One every 100 ms.
		{"600 a minute", map[string]string{config.EnvCreatesPerMinute: "600"}, 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spool, _ := spoolWith(t, oneRecordSegments(4)...)
			client := &timedClient{Fake: backendtest.NewFake()}
			uploader := tunedUploader(t, spool, client, tc.env)
			ctx, cancel := context.WithCancel(context.Background())
			passed := make(chan struct{})
			go func() {
				defer close(passed)
				_ = uploader.Pass(ctx)
			}()
			t.Cleanup(func() { cancel(); <-passed })
			waitFor(t, "the expected creates to start", func() bool { return client.SubmitCalls() >= tc.want })
			// Wait past the window, so a create that should not start in it
			// has had the time to.
			time.Sleep(1600 * time.Millisecond)
			if got := client.within(1500 * time.Millisecond); got != tc.want {
				t.Fatalf("creates started within 1.5 s of the first = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestADestinationWideRejectionHoldsInsteadOfQuarantining runs the case the
// lowest SEGMENT_SCOPE_FAILURES must still handle: after a segment was
// exported, the destination rejects every upload with a fault of unknown
// scope. Correlation needs each failing segment to stay counted until three
// have failed, so the backend is held and no healthy segment is
// quarantined. The hold shows as a new segment that is not submitted: the
// shortest hold is 5 s, and the pass that would submit it gets 1 s.
func TestADestinationWideRejectionHoldsInsteadOfQuarantining(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{"unset keeps 5", nil},
		{"the lowest setting", map[string]string{config.EnvSegmentScopeFailures: "2"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spool, exported := spoolWith(t, []string{uploadertest.RequestEnd("ok", 1)})
			fake := backendtest.NewFake()
			uploader := tunedUploader(t, spool, fake, with(unpaced, tc.env))
			if err := uploader.Pass(context.Background()); err != nil {
				t.Fatalf("first Pass() error = %v", err)
			}
			if !gone(exported[0]) {
				t.Fatal("the first segment was not exported, so no success is on record")
			}
			// Three more rolls close the old active segment and two new
			// ones: three segments, as many as backend correlation needs.
			for _, id := range []string{"b", "c", "d"} {
				spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd(id, 2)).Bytes())
			}
			rejected := backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{Class: backend.ClassPermanent, Scope: backend.ScopeUnknown, Reason: backend.ReasonRejected}}
			fake.Script(rejected, rejected, rejected)
			_ = uploader.Pass(context.Background())
			if calls := fake.SubmitCalls(); calls != 4 {
				t.Fatalf("submit calls = %d, want the export and three rejections", calls)
			}
			spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("e", 3)).Bytes())
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = uploader.Pass(ctx)
			if calls := fake.SubmitCalls(); calls != 4 {
				t.Fatalf("submit calls = %d after one more segment closed, want none: the backend should be held", calls)
			}
			if err := uploader.Close(); err != nil {
				t.Fatal(err)
			}
			for name, state := range journalStates(t, spool) {
				if state == journal.StateQuarantined {
					t.Fatalf("segment %s was quarantined, want the backend held instead", name)
				}
			}
		})
	}
}

func TestRetryAfterMaxCapsAHold(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		cap  time.Duration
	}{
		{"unset keeps 30m", nil, 30 * time.Minute},
		{"2m", map[string]string{config.EnvRetryAfterMax: "2m"}, 2 * time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spool, _ := spoolWith(t, []string{uploadertest.RequestEnd("a", 1)})
			fake := backendtest.NewFake()
			fake.Script(backendtest.SubmitStep{Outcome: backendtest.Refuse, Err: &backend.Fault{
				Class: backend.ClassRetryable, Scope: backend.ScopeBackend, Reason: backend.ReasonRateLimit, RetryAfter: 2 * time.Hour,
			}})
			uploader := tunedUploader(t, spool, fake, tc.env)
			before := time.Now()
			_ = uploader.Pass(context.Background())
			after := time.Now()
			if err := uploader.Close(); err != nil {
				t.Fatal(err)
			}
			batches := journalBatches(t, spool)
			if len(batches) != 1 || batches[0].State != journal.StateRetryWait {
				t.Fatalf("batches = %+v, want one waiting to retry", batches)
			}
			// The hold is the capped Retry-After plus up to 30 s of resume
			// jitter.
			next := batches[0].NextRetryAt
			if next.Before(before.Add(tc.cap)) || next.After(after.Add(tc.cap+30*time.Second)) {
				t.Fatalf("next retry in %v, want the %v cap plus at most 30 s of jitter", next.Sub(before), tc.cap)
			}
		})
	}
}
