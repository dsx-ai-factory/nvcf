// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// gatewayContract runs the calls the identity restore makes against e, a
// registry with no lease id.
func gatewayContract(t *testing.T, e identityEtcd, id int64) {
	t.Helper()
	ctx := context.Background()
	if info, err := e.TimeToLive(ctx, id, true); err != nil || info.TTL != -1 {
		t.Fatalf("unknown lease: %+v %v", info, err)
	}
	if err := e.Grant(ctx, id, 30); err != nil {
		t.Fatal(err)
	}
	if err := e.Grant(ctx, id, 30); !errors.Is(err, errLeaseExists) {
		t.Fatalf("grant of a live lease: %v", err)
	}
	if err := e.Put(ctx, []byte("v1/instances/x/y/z/1"), []byte(`{"a":1}`), id); err != nil {
		t.Fatal(err)
	}
	info, err := e.TimeToLive(ctx, id, true)
	if err != nil || info.GrantedTTL != 30 || info.TTL <= 0 || len(info.Keys) != 1 || string(info.Keys[0]) != "v1/instances/x/y/z/1" {
		t.Fatalf("timetolive: %+v %v", info, err)
	}
	kvs, err := e.RangePrefix(ctx, []byte("v1/"))
	if err != nil || len(kvs) != 1 || kvs[0].Lease != id || string(kvs[0].Value) != `{"a":1}` {
		t.Fatalf("range: %+v %v", kvs, err)
	}
	if kv, err := e.Get(ctx, []byte("v1/instances/x/y/z/1")); err != nil || kv == nil {
		t.Fatalf("get: %v %v", kv, err)
	}
	if kv, err := e.Get(ctx, []byte("nope")); err != nil || kv != nil {
		t.Fatalf("get of a missing key: %v %v", kv, err)
	}
	if ttl, err := e.KeepAlive(ctx, id); err != nil || ttl != 30 {
		t.Fatalf("keepalive: %d %v", ttl, err)
	}
	if err := e.Revoke(ctx, id); err != nil {
		t.Fatal(err)
	}
	if kvs, _ := e.RangePrefix(ctx, []byte("v1/")); len(kvs) != 0 {
		t.Fatal("revoke left the lease's keys")
	}
	if err := e.Revoke(ctx, id); err != nil {
		t.Fatalf("revoke of a gone lease: %v", err)
	}
	if ttl, err := e.KeepAlive(ctx, id); err != nil || ttl != 0 {
		t.Fatalf("keepalive of a gone lease: %d %v", ttl, err)
	}
	if err := e.Put(ctx, []byte("k"), []byte("v"), id); err == nil {
		t.Fatal("put under a gone lease")
	}
	// The same ID can be granted again once it is gone.
	if err := e.Grant(ctx, id, 5); err != nil {
		t.Fatalf("grant again: %v", err)
	}
	_ = e.Revoke(ctx, id)
}

func TestEtcdGateway_Fake(t *testing.T) {
	gatewayContract(t, newFakeEtcd(t).client(), idLease)
}

// The same contract against a real etcd's gateway, when one is given:
// NVSNAP_TEST_ETCD_URL=http://127.0.0.1:2379 (etcd v3.5).
func TestEtcdGateway_RealEtcd(t *testing.T) {
	u := os.Getenv("NVSNAP_TEST_ETCD_URL")
	if u == "" {
		t.Skip("NVSNAP_TEST_ETCD_URL not set")
	}
	gatewayContract(t, &etcdGateway{bases: []string{u}, client: http.DefaultClient}, time.Now().UnixNano())
}

// A base that does not answer is passed over for the next.
func TestEtcdGateway_NextBase(t *testing.T) {
	f := newFakeEtcd(t)
	g := &etcdGateway{bases: []string{"http://127.0.0.1:1", f.srv.URL}, client: f.srv.Client()}
	if info, err := g.TimeToLive(context.Background(), 1, false); err != nil || info.TTL != -1 {
		t.Fatalf("%+v %v", info, err)
	}
}

func TestPrefixEnd(t *testing.T) {
	if string(prefixEnd([]byte("v1/"))) != "v10" || string(prefixEnd([]byte{'a', 0xff})) != "b" {
		t.Error("prefixEnd")
	}
}

// Capture, expiry and regrant of a worker's identity against a real
// etcd, when one is given (NVSNAP_TEST_ETCD_URL): the spike's sequence.
func TestIdentityRoundTrip_RealEtcd(t *testing.T) {
	u := os.Getenv("NVSNAP_TEST_ETCD_URL")
	if u == "" {
		t.Skip("NVSNAP_TEST_ETCD_URL not set")
	}
	fastIdentity(t)
	identityExpirySlack = 3 * time.Second // a real etcd expires leases on its own ticks
	ctx := context.Background()
	e := &etcdGateway{bases: []string{u}, client: http.DefaultClient}
	id := time.Now().UnixNano() &^ 0xff
	hex := strconv.FormatUint(uint64(id), 16) //nolint:gosec // a lease ID's bits
	if err := e.Grant(ctx, id, 2); err != nil {
		t.Fatal(err)
	}
	inst := "v1/instances/rt/backend/generate/" + hex
	mdc := "v1/mdc/rt/backend/generate/" + hex
	if err := e.Put(ctx, []byte(inst), []byte(`{"transport":{"nats_tcp":"rt.generate-`+hex+`"}}`), id); err != nil {
		t.Fatal(err)
	}
	if err := e.Put(ctx, []byte(mdc), []byte(`{"path":"http://`+idOldIP+`:9090/x"}`), id); err != nil {
		t.Fatal(err)
	}
	got, err := readIdentity(ctx, e, []string{idOldIP}, "etcd:2379")
	if err != nil || got.Lease != id || got.TTL != 2 || len(got.Keys) != 2 {
		t.Fatalf("read: %+v %v", got, err)
	}
	if err = regrant(ctx, e, got, idNewIP); identityDecision(err) != identityRefusedLiveLease {
		t.Fatalf("grant while live: %v", err)
	}
	if err = awaitLeaseFree(ctx, e, id, 2); err != nil { // the source is gone: expires
		t.Fatal(err)
	}
	if err = regrant(ctx, e, got, idNewIP); err != nil {
		t.Fatal(err)
	}
	kv, err := e.Get(ctx, []byte(mdc))
	if err != nil || kv == nil || kv.Lease != id || !strings.Contains(string(kv.Value), "//"+idNewIP+":") {
		t.Fatalf("rewritten key: %+v %v", kv, err)
	}
	_ = e.Revoke(ctx, id)
}

// PutOwned against a real etcd: absent and own keys are written, another
// lease's key is left as it is.
func TestPutOwned_RealEtcd(t *testing.T) {
	u := os.Getenv("NVSNAP_TEST_ETCD_URL")
	if u == "" {
		t.Skip("NVSNAP_TEST_ETCD_URL not set")
	}
	ctx := context.Background()
	e := &etcdGateway{bases: []string{u}, client: http.DefaultClient}
	mine, other := time.Now().UnixNano()&^0xff, time.Now().UnixNano()|0xff
	t.Cleanup(func() { _ = e.Revoke(ctx, mine); _ = e.Revoke(ctx, other) })
	for _, id := range []int64{mine, other} {
		if err := e.Grant(ctx, id, 30); err != nil {
			t.Fatal(err)
		}
	}
	k := []byte("v1/putowned/" + strconv.FormatInt(mine, 16))
	if ok, err := e.PutOwned(ctx, k, []byte("a"), mine); err != nil || !ok {
		t.Fatalf("absent key: %v %v", ok, err)
	}
	if ok, err := e.PutOwned(ctx, k, []byte("b"), mine); err != nil || !ok {
		t.Fatalf("own key: %v %v", ok, err)
	}
	if ok, err := e.PutOwned(ctx, k, []byte("c"), other); err != nil || ok {
		t.Fatalf("another lease's key: %v %v", ok, err)
	}
	if kv, _ := e.Get(ctx, k); kv == nil || string(kv.Value) != "b" || kv.Lease != mine {
		t.Fatalf("key now %+v", kv)
	}
}
