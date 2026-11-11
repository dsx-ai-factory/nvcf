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

// newTestNode boots one embedded Olric node, joining the given memberlist
// peers when non-empty, and returns the node with its memberlist port. The
// counter path under test lives in the fork's CompareAndSwap primitive, so
// these tests run against real nodes instead of a fake. This is the
// single-node-per-store variant of the multi-node harness in
// llm-api-gateway/ratelimit/olric_cluster_test.go, and like that suite the
// tests are skipped in -short mode.
func newTestNode(t *testing.T, peers []string) (*olric.Olric, int) {
	t.Helper()

	memberlistPort := freeTCPPort(t)
	started := make(chan struct{})
	cfg := olricConfig.New("local")
	cfg.BindAddr = "127.0.0.1"
	cfg.BindPort = freeTCPPort(t)
	cfg.MemberlistConfig.BindAddr = "127.0.0.1"
	cfg.MemberlistConfig.BindPort = memberlistPort
	cfg.MemberlistConfig.AdvertisePort = memberlistPort
	cfg.Peers = peers
	cfg.DMaps.EvictionPolicy = olricConfig.LRUEviction
	cfg.DMaps.MaxInuse = 100_000_000 // 100 MB, mirrors cmd/main.go
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
	return db, memberlistPort
}

// newTestStore boots a single-node embedded Olric instance and returns a
// Store on top of it.
func newTestStore(t *testing.T) *Store {
	t.Helper()

	db, _ := newTestNode(t, nil)
	store, err := NewStore(db.NewEmbeddedClient())
	require.NoError(t, err)
	return store.(*Store)
}

// newTestStoreCluster boots size embedded Olric nodes joined into one
// cluster and returns one Store per node; all stores share the same DMap
// name, so a key written through one node is visible to the others.
func newTestStoreCluster(t *testing.T, size int) []*Store {
	t.Helper()
	require.Greater(t, size, 1, "multi-node test requires at least 2 nodes")

	var (
		stores  []*Store
		clients []*olric.EmbeddedClient
		peers   []string
	)
	for i := 0; i < size; i++ {
		db, memberlistPort := newTestNode(t, append([]string(nil), peers...))
		peers = append(peers, net.JoinHostPort("127.0.0.1", strconv.Itoa(memberlistPort)))
		client := db.NewEmbeddedClient()
		store, err := NewStore(client)
		require.NoError(t, err)
		stores = append(stores, store.(*Store))
		clients = append(clients, client)
	}

	// Wait until every node sees the full cluster, so the first checks do
	// not race the partition balancer.
	deadline := time.Now().Add(15 * time.Second)
	for {
		ready := true
		for _, client := range clients {
			members, err := client.Members(context.Background())
			if err != nil || len(members) < size {
				ready = false
				break
			}
		}
		if ready {
			return stores
		}
		if time.Now().After(deadline) {
			t.Fatal("olric cluster did not converge")
		}
		time.Sleep(50 * time.Millisecond)
	}
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

	// limiter.Context carries the accepted count as Limit-Remaining (it has
	// no Counter field in ulule/limiter v3).
	var sum int64
	for _, lc := range contexts {
		sum += lc.Limit - lc.Remaining
	}

	resp, err := store.GetDMap().Get(ctx, store.getCacheKey(key))
	require.NoError(t, err)
	stored, err := resp.Int()
	require.NoError(t, err)
	require.Equal(t, calls, stored, "lost or duplicated increments under concurrency")
	require.EqualValues(t, calls*(calls+1)/2, sum, "per-call counters must cover 1..N exactly once")
}

// TestStoreGetConcurrentIncrementsAcrossNodes runs concurrent Store.Get
// calls through two nodes of one embedded cluster, covering the remote
// compare-and-swap path (the fork serializes CAS cluster-wide on the
// partition owner). The stored counter must still be exactly N with per-call
// counters covering 1..N exactly once.
func TestStoreGetConcurrentIncrementsAcrossNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping embedded Olric test in -short mode")
	}

	ctx := context.Background()
	stores := newTestStoreCluster(t, 2)
	rate := limiter.Rate{Limit: 1_000_000, Period: time.Hour}
	key := "2394-two-node"

	const calls = 64
	contexts := make([]limiter.Context, calls)
	errs := make([]error, calls)
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		store := stores[i%len(stores)]
		go func(i int, store *Store) {
			defer wg.Done()
			contexts[i], errs[i] = store.Get(ctx, key, rate)
		}(i, store)
	}
	wg.Wait()
	for i := range errs {
		require.NoError(t, errs[i])
	}

	var sum int64
	for _, lc := range contexts {
		sum += lc.Limit - lc.Remaining
	}

	resp, err := stores[0].GetDMap().Get(ctx, stores[0].getCacheKey(key))
	require.NoError(t, err)
	stored, err := resp.Int()
	require.NoError(t, err)
	require.Equal(t, calls, stored, "lost or duplicated increments across nodes")
	require.EqualValues(t, calls*(calls+1)/2, sum, "per-call counters must cover 1..N exactly once across nodes")
}

// TestStoreGetOverCapContention pushes more concurrent checks through one
// key than the compare-and-swap attempt budget (100): with the retry
// backoff the losers must still be counted, none dropped.
func TestStoreGetOverCapContention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping embedded Olric test in -short mode")
	}

	ctx := context.Background()
	store := newTestStore(t)
	rate := limiter.Rate{Limit: 1_000_000, Period: time.Hour}
	key := "2394-over-cap"

	const calls = 128
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
		sum += lc.Limit - lc.Remaining
	}
	require.EqualValues(t, calls*(calls+1)/2, sum, "per-call counters must cover 1..N exactly once over the attempt cap")
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

	rate := limiter.Rate{Limit: 1000, Period: time.Second}
	first, err := store.Get(ctx, key, rate)
	require.NoError(t, err)
	require.EqualValues(t, 1, first.Limit-first.Remaining)

	resp, err := dmap.Get(ctx, store.getCacheKey(key))
	require.NoError(t, err)
	expiry := resp.TTL()
	require.Greater(t, expiry, int64(0), "the creating check must arm the window TTL")

	// Controlled delay inside the window. The stored TTL is an absolute Unix
	// millisecond timestamp, so the in-window increment must keep it exactly,
	// and Reset (whole seconds) must report that same expiration.
	time.Sleep(200 * time.Millisecond)
	second, err := store.Get(ctx, key, rate)
	require.NoError(t, err)
	require.EqualValues(t, 2, second.Limit-second.Remaining)

	resp2, err := dmap.Get(ctx, store.getCacheKey(key))
	require.NoError(t, err)
	require.Equal(t, expiry, resp2.TTL(), "the in-window increment must keep the original window expiration")
	require.Equal(t, time.UnixMilli(expiry).Unix(), second.Reset, "Reset must report the stored window expiration")

	time.Sleep(1100 * time.Millisecond)
	restarted, err := store.Get(ctx, key, rate)
	require.NoError(t, err)
	require.EqualValues(t, 1, restarted.Limit-restarted.Remaining, "the counter must restart after the window expires")
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
	require.EqualValues(t, 1, got.Limit-got.Remaining, "a key without TTL must restart the window at 1")

	resp, err := store.GetDMap().Get(ctx, store.getCacheKey(key))
	require.NoError(t, err)
	require.Greater(t, resp.TTL(), int64(0), "the restart must arm the window TTL")
}
