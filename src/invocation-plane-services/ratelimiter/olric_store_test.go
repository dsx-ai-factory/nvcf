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

package ratelimiter

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/olric-data/olric"
	olricConfig "github.com/olric-data/olric/config"
	"github.com/stretchr/testify/require"
	"github.com/ulule/limiter/v3"
)

// newTestStore boots a single-node embedded Olric instance and returns a
// Store on top of it. The counter path under test lives in the fork's
// CompareAndSwap primitive, so these tests run against a real node instead
// of a fake. This is the single-node variant of the multi-node harness in
// llm-api-gateway/ratelimit/olric_cluster_test.go, and like that suite it
// is skipped in -short mode.
func newTestStore(t *testing.T) *Store {
	t.Helper()

	started := make(chan struct{})
	cfg := olricConfig.New("local")
	cfg.BindAddr = "127.0.0.1"
	cfg.BindPort = freeTCPPort(t)
	cfg.MemberlistConfig.BindAddr = "127.0.0.1"
	cfg.MemberlistConfig.BindPort = freeTCPPort(t)
	cfg.DMaps.EvictionPolicy = olricConfig.LRUEviction
	cfg.LogLevel = "ERROR"
	cfg.LogOutput = io.Discard
	cfg.Started = func() { close(started) }

	db, err := olric.New(cfg)
	require.NoError(t, err)
	startErr := make(chan error, 1)
	go func() { startErr <- db.Start() }()
	select {
	case <-started:
	case err := <-startErr:
		t.Fatalf("olric failed to start: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("olric did not start within 30s")
	}
	t.Cleanup(func() {
		_ = db.Shutdown(context.Background())
	})

	store, err := NewStore(db.NewEmbeddedClient())
	require.NoError(t, err)
	return store.(*Store)
}

// freeTCPPort binds to 127.0.0.1:0 and returns the assigned port. The
// listener is closed immediately and the port is then reusable; races with
// unrelated processes are possible but negligible on a developer or CI box.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// TestStoreGetConcurrentIncrementsCounted is the regression test for issue
// 2394: N concurrent Store.Get calls on one key must all be counted. The
// stored counter must be exactly N and the per-call counters must cover
// 1..N exactly once, so neither lost nor duplicated increments pass.
func TestStoreGetConcurrentIncrementsCounted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping embedded Olric test in -short mode")
	}

	ctx := context.Background()
	store := newTestStore(t)
	rate := limiter.Rate{Limit: 1_000_000, Period: time.Hour}
	key := "2394-concurrent"

	const calls = 64
	contexts := make([]limiter.Context, calls)
	errs := make([]error, calls)
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			contexts[i], errs[i] = store.Get(ctx, key, rate)
		}(i)
	}
	wg.Wait()
	for i := range errs {
		require.NoError(t, errs[i])
	}

	var sum int64
	for _, lc := range contexts {
		sum += lc.Counter
	}

	resp, err := store.GetDMap().Get(ctx, store.getCacheKey(key))
	require.NoError(t, err)
	stored, err := resp.Int()
	require.NoError(t, err)
	require.Equal(t, calls, stored, "lost or duplicated increments under concurrency")
	require.EqualValues(t, calls*(calls+1)/2, sum, "per-call counters must cover 1..N exactly once")
}

// TestStoreGetWindowLifecycle pins the window semantics around the atomic
// counter: the creating check arms the window TTL in the same write, the
// counter restarts at 1 once the window expires, and a key that carries no
// TTL (legacy entry or a rate without a period) restarts its window too.
func TestStoreGetWindowLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping embedded Olric test in -short mode")
	}

	ctx := context.Background()
	store := newTestStore(t)
	dmap := store.GetDMap()
	key := "2394-window"

	rate := limiter.Rate{Limit: 1000, Period: 250 * time.Millisecond}
	first, err := store.Get(ctx, key, rate)
	require.NoError(t, err)
	require.EqualValues(t, 1, first.Counter)

	resp, err := dmap.Get(ctx, store.getCacheKey(key))
	require.NoError(t, err)
	require.Greater(t, resp.TTL(), int64(0), "the creating check must arm the window TTL")

	second, err := store.Get(ctx, key, rate)
	require.NoError(t, err)
	require.EqualValues(t, 2, second.Counter)

	time.Sleep(700 * time.Millisecond)
	restarted, err := store.Get(ctx, key, rate)
	require.NoError(t, err)
	require.EqualValues(t, 1, restarted.Counter, "the counter must restart after the window expires")
}

// TestStoreGetRestartsKeyWithoutTTL covers the branch that resets a TTL-less
// key to 1: without it a pre-existing key would be incremented forever.
func TestStoreGetRestartsKeyWithoutTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping embedded Olric test in -short mode")
	}

	ctx := context.Background()
	store := newTestStore(t)
	key := "2394-no-ttl"

	rate := limiter.Rate{Limit: 1000, Period: time.Hour}
	err := store.GetDMap().Put(ctx, store.getCacheKey(key), 5)
	require.NoError(t, err)

	got, err := store.Get(ctx, key, rate)
	require.NoError(t, err)
	require.EqualValues(t, 1, got.Counter, "a key without TTL must restart the window at 1")

	resp, err := store.GetDMap().Get(ctx, store.getCacheKey(key))
	require.NoError(t, err)
	require.Greater(t, resp.TTL(), int64(0), "the restart must arm the window TTL")
}
