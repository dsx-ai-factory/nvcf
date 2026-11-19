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

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hellofresh/health-go/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/consumer"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/nvcf"
)

func TestNewNatsConsumerWithRetry(t *testing.T) {
	errNatsDown := errors.New("context deadline exceeded")

	t.Run("recovers after transient failures", func(t *testing.T) {
		want := &consumer.GlobalNatsConsumer{}
		attempts := 0
		got, err := newNatsConsumerWithRetry(context.Background(), &backoff.ZeroBackOff{}, func() (*consumer.GlobalNatsConsumer, error) {
			attempts++
			if attempts < 3 {
				return nil, errNatsDown
			}
			return want, nil
		})
		require.NoError(t, err)
		assert.Same(t, want, got)
		assert.Equal(t, 3, attempts)
	})

	t.Run("returns the last error when the backoff stops", func(t *testing.T) {
		attempts := 0
		_, err := newNatsConsumerWithRetry(context.Background(), backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 2), func() (*consumer.GlobalNatsConsumer, error) {
			attempts++
			return nil, errNatsDown
		})
		require.ErrorIs(t, err, errNatsDown)
		assert.Equal(t, 3, attempts)
	})

	t.Run("keeps retrying through a long outage until NATS is back", func(t *testing.T) {
		attempts := 0
		got, err := newNatsConsumerWithRetry(context.Background(), &backoff.ZeroBackOff{}, func() (*consumer.GlobalNatsConsumer, error) {
			attempts++
			if attempts < 500 {
				return nil, errNatsDown
			}
			return &consumer.GlobalNatsConsumer{}, nil
		})
		require.NoError(t, err)
		assert.NotNil(t, got)
		assert.Equal(t, 500, attempts)
	})

	t.Run("stops when context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		attempts := 0
		_, err := newNatsConsumerWithRetry(ctx, &backoff.ZeroBackOff{}, func() (*consumer.GlobalNatsConsumer, error) {
			attempts++
			cancel()
			return nil, errNatsDown
		})
		require.Error(t, err)
		assert.Equal(t, 1, attempts)
	})

	t.Run("does not retry errors that retrying cannot fix", func(t *testing.T) {
		for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument} {
			t.Run(code.String(), func(t *testing.T) {
				rejected := fmt.Errorf("failed to create consumer for primary region ncp: %w", status.Error(code, "rejected"))
				attempts := 0
				_, err := newNatsConsumerWithRetry(context.Background(), &backoff.ZeroBackOff{}, func() (*consumer.GlobalNatsConsumer, error) {
					attempts++
					return nil, rejected
				})
				require.ErrorIs(t, err, rejected)
				assert.Equal(t, 1, attempts)
			})
		}
	})

	t.Run("retry warning includes the caller's log fields", func(t *testing.T) {
		core, logs := observer.New(zap.WarnLevel)
		t.Cleanup(zap.ReplaceGlobals(zap.New(core)))
		attempts := 0
		_, err := newNatsConsumerWithRetry(context.Background(), &backoff.ZeroBackOff{}, func() (*consumer.GlobalNatsConsumer, error) {
			attempts++
			if attempts < 2 {
				return nil, errNatsDown
			}
			return &consumer.GlobalNatsConsumer{}, nil
		}, zap.String("function_id", "fn-1"))
		require.NoError(t, err)
		entries := logs.FilterMessage("failed to set up nats consumer, retrying").All()
		require.Len(t, entries, 1)
		assert.Equal(t, "fn-1", entries[0].ContextMap()["function_id"])
		assert.EqualValues(t, 1, entries[0].ContextMap()["failedAttempts"])
	})

	t.Run("retries unavailable provisioning", func(t *testing.T) {
		attempts := 0
		_, err := newNatsConsumerWithRetry(context.Background(), &backoff.ZeroBackOff{}, func() (*consumer.GlobalNatsConsumer, error) {
			attempts++
			if attempts < 2 {
				return nil, status.Error(codes.Unavailable, "connection refused")
			}
			return &consumer.GlobalNatsConsumer{}, nil
		})
		require.NoError(t, err)
		assert.Equal(t, 2, attempts)
	})

	t.Run("logs recovery after retrying", func(t *testing.T) {
		core, logs := observer.New(zap.InfoLevel)
		t.Cleanup(zap.ReplaceGlobals(zap.New(core)))
		attempts := 0
		_, err := newNatsConsumerWithRetry(context.Background(), &backoff.ZeroBackOff{}, func() (*consumer.GlobalNatsConsumer, error) {
			attempts++
			if attempts < 3 {
				return nil, errNatsDown
			}
			return &consumer.GlobalNatsConsumer{}, nil
		}, zap.String("function_id", "fn-1"))
		require.NoError(t, err)
		entries := logs.FilterMessage("set up nats consumer after retrying").All()
		require.Len(t, entries, 1)
		assert.EqualValues(t, 2, entries[0].ContextMap()["failedAttempts"])
		assert.Equal(t, "fn-1", entries[0].ContextMap()["function_id"])
	})

	t.Run("does not log recovery when the first attempt succeeds", func(t *testing.T) {
		core, logs := observer.New(zap.InfoLevel)
		t.Cleanup(zap.ReplaceGlobals(zap.New(core)))
		_, err := newNatsConsumerWithRetry(context.Background(), &backoff.ZeroBackOff{}, func() (*consumer.GlobalNatsConsumer, error) {
			return &consumer.GlobalNatsConsumer{}, nil
		})
		require.NoError(t, err)
		assert.Empty(t, logs.FilterMessage("set up nats consumer after retrying").All())
	})
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func TestNatsConsumerSetupBackOff(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	b := newNatsConsumerSetupBackOffWithClock(clock)
	now := clock.now
	advance := func(d time.Duration) {
		now = now.Add(d)
		clock.now = now
	}

	maxFast := natsConsumerFastRetryMaxWait + natsConsumerFastRetryMaxWait/4
	for i := 0; now.Sub(time.Unix(0, 0)) < natsConsumerFastRetryPeriod; i++ {
		require.Less(t, i, 1000, "fast phase did not reach its end")
		wait := b.NextBackOff()
		require.NotEqual(t, backoff.Stop, wait)
		require.Positive(t, wait)
		require.LessOrEqual(t, wait, maxFast, "fast phase at %s", now.Sub(time.Unix(0, 0)))
		advance(wait)
	}

	minFirstSlow := natsConsumerFastRetryMaxWait - natsConsumerFastRetryMaxWait/4
	assert.GreaterOrEqual(t, b.NextBackOff(), minFirstSlow, "waits do not shrink when the slow phase starts")

	maxSlow := natsConsumerSlowRetryMaxWait + natsConsumerSlowRetryMaxWait/4
	sawSlow := false
	for i := 0; i < 1000; i++ {
		wait := b.NextBackOff()
		require.NotEqual(t, backoff.Stop, wait, "the backoff must never stop on its own")
		require.Positive(t, wait)
		require.LessOrEqual(t, wait, maxSlow)
		sawSlow = sawSlow || wait > maxFast
		advance(wait)
	}
	assert.True(t, sawSlow, "waits grow beyond the fast limit after the fast period")
	assert.Greater(t, now.Sub(time.Unix(0, 0)), 6*time.Hour)

}

func TestPhasedBackOffReset(t *testing.T) {
	now := time.Unix(0, 0)
	b := &phasedBackOff{
		fast:    backoff.NewConstantBackOff(time.Millisecond),
		slow:    backoff.NewConstantBackOff(time.Second),
		fastFor: time.Minute,
		now:     func() time.Time { return now },
	}
	assert.Equal(t, time.Millisecond, b.NextBackOff())
	now = now.Add(2 * time.Minute)
	assert.Equal(t, time.Second, b.NextBackOff())

	b.Reset()
	assert.Equal(t, time.Millisecond, b.NextBackOff(), "Reset starts over with fast retries")
	now = now.Add(2 * time.Minute)
	assert.Equal(t, time.Second, b.NextBackOff())
}

func TestServeReady(t *testing.T) {
	measured := health.Check{
		Status:    health.StatusOK,
		Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Component: health.Component{Name: "gdn-nvcf-worker-service"},
	}
	calls := 0
	measure := func(context.Context) health.Check {
		calls++
		c := measured
		c.Failures = maps.Clone(measured.Failures)
		return c
	}
	w := &NVCFWorker{}
	serve := func() (int, health.Check) {
		rec := httptest.NewRecorder()
		w.serveReady(rec, httptest.NewRequest(http.MethodGet, "/v1/health/ready", nil), measure)
		var got health.Check
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		return rec.Code, got
	}

	code, got := serve()
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, measured.Component, got.Component)
	assert.True(t, measured.Timestamp.Equal(got.Timestamp))
	assert.Empty(t, got.Failures)

	w.natsConsumerSetupFailing.Store(true)
	code, got = serve()
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, health.StatusUnavailable, got.Status)
	assert.Equal(t, map[string]string{"nats-consumer": "nats consumer setup is failing"}, got.Failures)
	assert.Equal(t, measured.Component, got.Component)

	measured.Status = health.StatusUnavailable
	measured.Failures = map[string]string{"inference": "connection refused"}
	code, got = serve()
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, map[string]string{
		"inference":     "connection refused",
		"nats-consumer": "nats consumer setup is failing",
	}, got.Failures, "keeps the other check failures")

	w.natsConsumerSetupFailing.Store(false)
	code, got = serve()
	assert.Equal(t, http.StatusServiceUnavailable, code, "other check failures still report not ready")
	assert.Equal(t, measured.Failures, got.Failures)

	measured.Status = health.StatusOK
	measured.Failures = nil
	code, _ = serve()
	assert.Equal(t, http.StatusOK, code, "ready again as soon as setup succeeds")
	assert.Equal(t, 5, calls, "the health checks run on every request")
}

func TestPublishNatsConsumer(t *testing.T) {
	r := func(primary string, secondaries ...string) *nvcf.ConnectionRegions {
		return &nvcf.ConnectionRegions{Primary: primary, Secondaries: secondaries}
	}
	c := &consumer.GlobalNatsConsumer{}
	trigger := func() {}

	newWorker := func(connected *nvcf.ConnectionRegions) *NVCFWorker {
		w := &NVCFWorker{nvcfClient: &nvcf.Client{}}
		w.nvcfClient.ConnectedRegions.Store(connected)
		w.natsConsumerSetupFailing.Store(true)
		return w
	}

	t.Run("regions unchanged", func(t *testing.T) {
		w := newWorker(r("a", "b"))
		assert.True(t, w.publishNatsConsumer(c, r("a", "b"), &trigger))
		assert.Same(t, c, w.nvcfClient.CurrentNatsConsumer.Load())
		assert.Same(t, &trigger, w.nvcfClient.TriggerNewNatsConsumer.Load())
		assert.False(t, w.natsConsumerSetupFailing.Load(), "an accepted consumer clears the readiness failure")
	})

	t.Run("regions changed after the consumer was built", func(t *testing.T) {
		w := newWorker(r("b", "a"))
		assert.False(t, w.publishNatsConsumer(c, r("a", "b"), &trigger))
		assert.Same(t, c, w.nvcfClient.CurrentNatsConsumer.Load(), "stored so a later region reload can trigger it")
		assert.Same(t, &trigger, w.nvcfClient.TriggerNewNatsConsumer.Load())
		assert.True(t, w.natsConsumerSetupFailing.Load(), "a rejected consumer keeps the readiness failure")
	})
}

func TestSameRegions(t *testing.T) {
	r := func(primary string, secondaries ...string) *nvcf.ConnectionRegions {
		return &nvcf.ConnectionRegions{Primary: primary, Secondaries: secondaries}
	}
	assert.True(t, sameRegions(r("a", "b", "c"), r("a", "c", "b")))
	assert.False(t, sameRegions(r("a", "b"), r("b", "a")))
	assert.False(t, sameRegions(r("a", "b"), r("a", "b", "c")))
	assert.False(t, sameRegions(r("a"), nil))
	assert.True(t, sameRegions(nil, nil))
}
