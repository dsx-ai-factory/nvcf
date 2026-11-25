// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// dynamoTCPWorkerKeys registers a Dynamo worker at ip under lease on the
// TCP request plane, Dynamo's default: each endpoint registration names
// the worker's request-plane server as {"tcp":"<ip>:<port>/<path>"}
// (lib/runtime/src/component/endpoint.rs tcp_transport_type), the model
// card names no address, and one ZMQ event channel names it as a URL host.
func (f *fakeEtcd) dynamoTCPWorkerKeys(lease int64, ip string, port int) []string {
	hex := strconv.FormatUint(uint64(lease), 16) //nolint:gosec // a lease ID's bits
	host := ip
	if strings.Contains(ip, ":") {
		host = "[" + ip + "]"
	}
	keys := []string{}
	for _, ep := range []string{"generate", "clear_kv_blocks"} {
		k := "v1/instances/idpool/backend/" + ep + "/" + hex
		f.put(k, `{"type":"Endpoint","component":"backend","endpoint":"`+ep+`","namespace":"idpool","instance_id":`+
			strconv.FormatUint(uint64(lease), 10)+`,"transport":{"tcp":"`+host+`:`+strconv.Itoa(port)+`/`+hex+`/idpool/backend/`+ep+`"}}`, lease) //nolint:gosec // a lease ID's bits
		keys = append(keys, k)
	}
	mdc := "v1/mdc/idpool/backend/generate/" + hex
	f.put(mdc, `{"type":"Model","card_json":{"display_name":"m"}}`, lease)
	ev := "v1/event_channels/namespace/idpool/component/backend/endpoint/generate/topic/kv_metrics/" + hex + "1"
	f.put(ev, `{"type":"EventChannel","transport":{"kind":"Zmq","config":{"endpoint":"tcp://`+host+`:39707"}}}`, lease)
	return append(keys, mdc, ev)
}

// regranted ends the captured lease with its keys, as the dead source's
// expiry does, and grants it again, as a restore does.
func regranted(f *fakeEtcd) {
	f.mu.Lock()
	f.dropLease(idLease)
	f.mu.Unlock()
	f.grant(idLease, 30)
}

// useRequestPlane makes the restore's dials to a restored worker's
// request plane answer with answer, and records them.
func useRequestPlane(t *testing.T, answer error) (dialed func() []string) {
	t.Helper()
	savedDial, savedWait := dialRequestPlane, requestPlaneWait
	var mu sync.Mutex
	var got []string
	dialRequestPlane = func(_ context.Context, addr string) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, addr)
		return answer
	}
	requestPlaneWait = 300 * time.Millisecond
	t.Cleanup(func() { dialRequestPlane, requestPlaneWait = savedDial, savedWait })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}
}

// The address of a TCP request-plane registration is rewritten like a URL
// host; any other bare form of the address is still refused.
func TestHostForms_TCPRequestPlane(t *testing.T) {
	v := []byte(`{"transport":{"tcp":"10.0.0.1:41234/7b2e/idpool/backend/generate"},"x":"tcp://10.0.0.1:9"}`)
	got := string(rewriteHost(v, "10.0.0.1", "10.0.0.2"))
	if want := `{"transport":{"tcp":"10.0.0.2:41234/7b2e/idpool/backend/generate"},"x":"tcp://10.0.0.2:9"}`; got != want {
		t.Errorf("rewrite: %s, want %s", got, want)
	}
	if got := string(rewriteHost([]byte(`{"tcp":"[fd00::1]:4/x"}`), "fd00::1", "fd00::2")); got != `{"tcp":"[fd00::2]:4/x"}` {
		t.Errorf("IPv6 rewrite: %s", got)
	}
	// Not the start of a longer address.
	if got := string(rewriteHost([]byte(`{"tcp":"10.0.0.12:4/x"}`), "10.0.0.1", "10.0.0.2")); got != `{"tcp":"10.0.0.12:4/x"}` {
		t.Errorf("another address rewritten: %s", got)
	}
	ips := []string{"10.0.0.1", "10.0.0.9"}
	for _, tc := range []struct {
		v     string
		stray bool
	}{
		{`{"transport":{"tcp":"10.0.0.1:41234/7b2e/a/b/c"}}`, false},
		{`{"transport":{"tcp":"[fd00::1]:1/x"}}`, false},
		// Another rank's address, or a bare one: not rewritten.
		{`{"transport":{"tcp":"10.0.0.9:41234/7b2e/a/b/c"}}`, true},
		{`{"a":"10.0.0.1:5600"}`, true},
		{`{"bootstrap_host":"10.0.0.1"}`, true},
		{`{"transport": {"tcp": "10.0.0.1:41234/x"}}`, true},
	} {
		if s := strayAddress([]byte(tc.v), "10.0.0.1", ips); s != tc.stray {
			t.Errorf("%s: stray %v, want %v", tc.v, s, tc.stray)
		}
	}
	if !mentions([]byte(`{"transport":{"tcp":"10.0.0.1:41234/x"}}`), "10.0.0.1") {
		t.Error("a TCP request-plane registration does not name its address")
	}
	if got := requestPlaneAddrs(v, "10.0.0.1"); !slices.Equal(got, []string{"10.0.0.1:41234"}) {
		t.Errorf("request-plane addresses: %v", got)
	}
	if got := requestPlaneAddrs([]byte(`{"tcp":"[fd00::2]:4/x"}`), "fd00::2"); !slices.Equal(got, []string{"[fd00::2]:4"}) {
		t.Errorf("IPv6 request-plane addresses: %v", got)
	}
}

// A worker on the TCP request plane, whose only address is that of its
// request-plane server and its event channel, is captured with its
// identity, and restored registered at its new address.
func TestReadIdentity_TCPRequestPlane(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ name, old, nw string }{
		{"IPv4", idOldIP, idNewIP},
		{"IPv6", "fd00:10::8", "fd00:10::4c"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeEtcd(t)
			f.grant(idLease, 30)
			keys := f.dynamoTCPWorkerKeys(idLease, tc.old, 41234)
			f.grant(idLease+1, 30)
			f.dynamoTCPWorkerKeys(idLease+1, "10.245.36.78", 41234) // another worker
			id, err := readIdentity(ctx, f.client(), []string{tc.old}, idEtcdEP)
			if err != nil {
				t.Fatalf("a TCP request-plane worker's identity was refused: %v", err)
			}
			if id.Lease != idLease || len(id.Keys) != len(keys) {
				t.Fatalf("identity %+v", id)
			}
			regranted(f)
			dialed := useRequestPlane(t, nil)
			if err := writeIdentityKeys(ctx, f.client(), id, tc.nw); err != nil {
				t.Fatal(err)
			}
			host := tc.nw
			if strings.Contains(host, ":") {
				host = "[" + host + "]"
			}
			for _, k := range keys[:2] {
				v, _ := f.value(k)
				if !strings.Contains(v, `"tcp":"`+host+`:41234/`) || strings.Contains(v, tc.old) {
					t.Errorf("%s not registered at the new address: %s", k, v)
				}
			}
			// One server serves every endpoint: dialed once, at the new
			// address.
			if got := dialed(); !slices.Equal(got, []string{host + ":41234"}) {
				t.Errorf("dialed %v, want the new address once", got)
			}
		})
	}
}

// A restored worker whose request plane does not answer at its new
// address is not registered there: the keeper ends it (keys_write_failed).
func TestWriteIdentityKeys_RequestPlaneUnreachable(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	f.grant(idLease, 30)
	keys := f.dynamoTCPWorkerKeys(idLease, idOldIP, 41234)
	id, err := readIdentity(ctx, f.client(), []string{idOldIP}, idEtcdEP)
	if err != nil {
		t.Fatal(err)
	}
	regranted(f)
	dialed := useRequestPlane(t, fmt.Errorf("dial tcp %s:41234: %w", idNewIP, syscall.ECONNREFUSED))
	err = writeIdentityKeys(ctx, f.client(), id, idNewIP)
	if err == nil || !strings.Contains(err.Error(), "does not accept connections") || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("an unreachable worker was registered: %v", err)
	}
	if strings.Contains(err.Error(), idNewIP) {
		t.Errorf("the error names the address: %v", err)
	}
	if len(dialed()) < 2 {
		t.Errorf("dialed %d times, want retries within the wait", len(dialed()))
	}
	for _, k := range keys {
		if _, ok := f.value(k); ok {
			t.Errorf("%s written for a worker that cannot be reached", k)
		}
	}
	// Through the keeper: the lease is revoked and the pod ended.
	fastIdentity(t)
	zombie := make(chan struct{})
	k := newLeaseKeeper(f.client(), idLease, 30, idLog())
	k.writeKeys = func(ctx context.Context) error { return writeIdentityKeys(ctx, f.client(), id, idNewIP) }
	k.onZombie = func() { close(zombie) }
	before := decisions(t, identityKeysFailed)
	go k.run(ctx)
	k.restored(nil)
	select {
	case <-k.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the keeper did not end")
	}
	select {
	case <-zombie:
	default:
		t.Error("the unreachable worker's pod was not ended")
	}
	if k.outcome != identityKeysFailed || decisions(t, identityKeysFailed) != before+1 {
		t.Errorf("outcome %q", k.outcome)
	}
	if f.live(idLease) {
		t.Error("the unreachable worker's lease was not revoked")
	}
}

// A worker on the NATS request plane names no request-plane address: no
// dial, as before.
func TestWriteIdentityKeys_NATSPlaneDialsNothing(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	f.grant(idLease, 30)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	id, err := readIdentity(ctx, f.client(), []string{idOldIP}, idEtcdEP)
	if err != nil {
		t.Fatal(err)
	}
	regranted(f)
	dialed := useRequestPlane(t, syscall.ECONNREFUSED)
	if err := writeIdentityKeys(ctx, f.client(), id, idNewIP); err != nil {
		t.Fatal(err)
	}
	if got := dialed(); len(got) != 0 {
		t.Errorf("dialed %v for a NATS request-plane worker", got)
	}
	keysName(t, f, keys, idNewIP, idOldIP)
}

// dynamo151WorkerKeys registers a worker as Dynamo 1.5.1 (vllm-runtime,
// etcd discovery) does, its values copied from a live registry (the model
// card shortened to its addresses): three endpoint registrations, a model
// card and two event channels, every key on the worker's own lease. An
// event channel's key and instance_id are its publisher's, not the lease.
// On the TCP plane the registration is "IP:port/<lease hex>/<endpoint>";
// on the NATS plane it names a subject and no address.
func (f *fakeEtcd) dynamo151WorkerKeys(lease int64, ip string, tcpPlane bool, publishers [2]string) []string {
	hex := strconv.FormatUint(uint64(lease), 16) //nolint:gosec // a lease ID's bits
	id := strconv.FormatUint(uint64(lease), 10)  //nolint:gosec // a lease ID's bits
	keys := []string{}
	for _, ep := range []string{"generate", "clear_kv_blocks", "get_perf_metrics"} {
		k := "v1/instances/identity/backend/" + ep + "/" + hex
		transport := `{"nats_tcp":"identity_backend.` + ep + `-` + hex + `"}`
		if tcpPlane {
			transport = `{"tcp":"` + ip + `:37209/` + hex + `/` + ep + `"}`
		}
		f.put(k, `{"type":"Endpoint","component":"backend","endpoint":"`+ep+`","namespace":"identity","instance_id":`+id+
			`,"transport":`+transport+`,"device_type":"cpu","request_plane_codec":"msgpack"}`, lease)
		keys = append(keys, k)
	}
	mdc := "v1/mdc/identity/backend/generate/" + hex
	f.put(mdc, `{"type":"Model","namespace":"identity","component":"backend","endpoint":"generate","instance_id":`+id+
		`,"card_json":{"display_name":"Qwen/Qwen2.5-0.5B-Instruct","model_info":{"hf_config_json":{"path":"http://`+ip+
		`:9090/v1/metadata/identity/backend/generate/qwen_qwen2_5-0_5b-instruct/_base/config.json","size":659}},"tokenizer":{"hf_tokenizer_json":{"path":"http://`+ip+
		`:9090/v1/metadata/identity/backend/generate/qwen_qwen2_5-0_5b-instruct/_base/tokenizer.json"}}}}`, lease)
	keys = append(keys, mdc)
	for i, topic := range []string{"kv_metrics", "active_sequences_events"} {
		k := "v1/event_channels/namespace/identity/component/backend/endpoint/generate/topic/" + topic + "/" + publishers[i]
		f.put(k, `{"type":"EventChannel","scope":{"kind":"endpoint","endpoint":{"namespace":"identity","component":"backend","name":"generate"}},"topic":"`+
			topic+`","instance_id":4668748419827915,"transport":{"kind":"Zmq","config":{"endpoint":"tcp://`+ip+`:3909`+strconv.Itoa(7+i)+`"}}}`, lease)
		keys = append(keys, k)
	}
	return keys
}

// Dynamo 1.5.1's registrations, both request planes, among other workers
// of the namespace: the capture saves all six keys of the worker's lease
// (the event channels too, named by their publishers), and the restore
// writes them back under the lease, the old address nowhere, the new one
// wherever the old was, after the request plane (TCP only) answered there.
func TestIdentity_Dynamo151Registrations(t *testing.T) {
	const lease, oldIP, newIP = int64(0x694da21504c2f314), "10.245.24.101", "10.245.24.140"
	ctx := context.Background()
	for _, tcpPlane := range []bool{true, false} {
		t.Run(map[bool]string{true: "tcp", false: "nats"}[tcpPlane], func(t *testing.T) {
			f := newFakeEtcd(t)
			f.grant(lease, 10)
			keys := f.dynamo151WorkerKeys(lease, oldIP, tcpPlane, [2]string{"109633b31c08cb", "12a79e8aeebb07"})
			f.grant(lease+2, 10)
			f.dynamo151WorkerKeys(lease+2, "10.245.24.102", tcpPlane, [2]string{"12aa591d19f847", "158f63895cc107"})
			f.grant(lease-0xe, 10) // the frontend's lease: no keys
			id, err := readIdentity(ctx, f.client(), []string{oldIP}, idEtcdEP)
			if err != nil {
				t.Fatalf("the worker's identity was refused: %v", err)
			}
			if id.Lease != lease || id.TTL != 10 || len(id.Keys) != 6 {
				t.Fatalf("identity lease %x TTL %d with %d keys; want the worker's six", id.Lease, id.TTL, len(id.Keys))
			}
			f.mu.Lock()
			f.dropLease(lease)
			f.mu.Unlock()
			f.grant(lease, 10)
			dialed := useRequestPlane(t, nil)
			if err := writeIdentityKeys(ctx, f.client(), id, newIP); err != nil {
				t.Fatal(err)
			}
			for _, k := range keys {
				v, ok := f.value(k)
				if !ok {
					t.Errorf("%s not written", k)
					continue
				}
				if strings.Contains(v, oldIP) {
					t.Errorf("%s still names the captured address: %s", k, v)
				}
				if strings.Contains(k, "/mdc/") || strings.Contains(k, "/event_channels/") || (tcpPlane && strings.Contains(k, "/instances/")) {
					if !strings.Contains(v, newIP+":") {
						t.Errorf("%s does not name the new address: %s", k, v)
					}
				}
			}
			if v, _ := f.value(keys[0]); tcpPlane && !strings.Contains(v, `"tcp":"`+newIP+`:37209/694da21504c2f314/generate"`) {
				t.Errorf("registration %s", v)
			}
			want := []string{}
			if tcpPlane {
				want = []string{newIP + ":37209"}
			}
			if got := dialed(); !slices.Equal(got, want) && !(len(got) == 0 && len(want) == 0) {
				t.Errorf("dialed %v, want %v", got, want)
			}
			// The other worker's keys are untouched.
			if v, _ := f.value("v1/mdc/identity/backend/generate/" + strconv.FormatUint(uint64(lease+2), 16)); !strings.Contains(v, "10.245.24.102") { //nolint:gosec // a lease ID's bits
				t.Errorf("the other worker's card changed: %s", v)
			}
		})
	}
}
