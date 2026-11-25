// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// An in-memory etcd behind the JSON gateway's routes, with etcd's lease
// semantics as measured on etcd v3.5.17: a lease expires after its TTL
// and takes its keys with it; a grant of a live ID is refused ("lease
// already exists", HTTP 400); a keep-alive of an unknown lease answers no
// TTL; a revoke of one answers 404. The tests of identity_etcd.go run the
// real gateway client against it.

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeLease struct {
	ttl     int64
	expires time.Time
}

type fakeEtcd struct {
	t   *testing.T
	srv *httptest.Server

	mu     sync.Mutex
	leases map[int64]*fakeLease
	kvs    map[string]etcdKV
	// calls counts requests by path; fail makes a path answer 500.
	calls map[string]int
	fail  map[string]bool
	// puts records every put, in order.
	puts []etcdKV
	// onTimeToLive, when set, runs (under the lock) before a timetolive
	// answer: a registration that changes between a capture's reads.
	onTimeToLive func()
}

func newFakeEtcd(t *testing.T) *fakeEtcd {
	f := &fakeEtcd{t: t, leases: map[int64]*fakeLease{}, kvs: map[string]etcdKV{}, calls: map[string]int{}, fail: map[string]bool{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeEtcd) client() identityEtcd {
	return &etcdGateway{bases: []string{f.srv.URL}, client: f.srv.Client()}
}

// expire drops the leases past their TTL, with their keys. Under mu.
func (f *fakeEtcd) expire() {
	now := time.Now()
	for id, l := range f.leases {
		if now.After(l.expires) {
			f.dropLease(id)
		}
	}
}

func (f *fakeEtcd) dropLease(id int64) {
	delete(f.leases, id)
	for k, kv := range f.kvs {
		if kv.Lease == id {
			delete(f.kvs, k)
		}
	}
}

// grant adds a live lease, as a worker's own grant would.
func (f *fakeEtcd) grant(id, ttl int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases[id] = &fakeLease{ttl: ttl, expires: time.Now().Add(time.Duration(ttl) * time.Second)}
}

// refresh renews a lease, as a live process's keep-alive would.
func (f *fakeEtcd) refresh(id int64) { //nolint:unparam // the lease of the test
	f.mu.Lock()
	defer f.mu.Unlock()
	if l := f.leases[id]; l != nil {
		l.expires = time.Now().Add(time.Duration(l.ttl) * time.Second)
	}
}

func (f *fakeEtcd) put(key, value string, lease int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kvs[key] = etcdKV{Key: []byte(key), Value: []byte(value), Lease: lease}
}

func (f *fakeEtcd) live(id int64) bool { //nolint:unparam // the lease of the test
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expire()
	return f.leases[id] != nil
}

func (f *fakeEtcd) value(key string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expire()
	kv, ok := f.kvs[key]
	return string(kv.Value), ok
}

func (f *fakeEtcd) setFail(path string, fail bool) { //nolint:unparam // tests turn failures on
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[path] = fail
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func unb64(t *testing.T, s string) []byte {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Errorf("fake etcd: bad base64 %q", s)
	}
	return b
}

func i64(s string) int64 { n, _ := strconv.ParseInt(s, 10, 64); return n }

func (f *fakeEtcd) serve(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	str := func(k string) string {
		switch v := body[k].(type) {
		case string:
			return v
		case float64:
			return strconv.FormatInt(int64(v), 10)
		}
		return ""
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[r.URL.Path]++
	if f.fail[r.URL.Path] {
		http.Error(w, `{"message":"injected failure"}`, http.StatusInternalServerError)
		return
	}
	f.expire()
	reply := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	fail := func(status int, msg string) {
		w.WriteHeader(status)
		reply(map[string]any{"error": msg, "message": msg})
	}
	switch r.URL.Path {
	case "/v3/kv/range":
		key := string(unb64(f.t, str("key")))
		end := ""
		if e := str("range_end"); e != "" {
			end = string(unb64(f.t, e))
		}
		var keys []string
		for k := range f.kvs {
			if (end == "" && k == key) || (end != "" && k >= key && k < end) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		var kvs []map[string]string
		for _, k := range keys {
			kv := f.kvs[k]
			m := map[string]string{"key": b64(kv.Key), "value": b64(kv.Value)}
			if kv.Lease != 0 {
				m["lease"] = strconv.FormatInt(kv.Lease, 10)
			}
			kvs = append(kvs, m)
		}
		reply(map[string]any{"kvs": kvs, "count": strconv.Itoa(len(kvs))})
	case "/v3/kv/put":
		lease := i64(str("lease"))
		if lease != 0 && f.leases[lease] == nil {
			fail(http.StatusNotFound, "etcdserver: requested lease not found")
			return
		}
		kv := etcdKV{Key: unb64(f.t, str("key")), Value: unb64(f.t, str("value")), Lease: lease}
		f.kvs[string(kv.Key)] = kv
		f.puts = append(f.puts, kv)
		reply(map[string]any{})
	case "/v3/kv/txn":
		var txn struct {
			Compare []struct {
				Key     string `json:"key"`
				Target  string `json:"target"`
				Version string `json:"version"`
				Lease   string `json:"lease"`
			} `json:"compare"`
			Success []struct {
				RequestPut struct {
					Key   string `json:"key"`
					Value string `json:"value"`
					Lease string `json:"lease"`
				} `json:"requestPut"`
			} `json:"success"`
		}
		b, _ := json.Marshal(body)
		_ = json.Unmarshal(b, &txn)
		ok := true
		for _, c := range txn.Compare {
			kv, exists := f.kvs[string(unb64(f.t, c.Key))]
			switch c.Target {
			case "VERSION":
				ok = ok && !exists && c.Version == "0"
			case "LEASE":
				ok = ok && exists && kv.Lease == i64(c.Lease)
			}
		}
		if ok {
			for _, op := range txn.Success {
				lease := i64(op.RequestPut.Lease)
				if lease != 0 && f.leases[lease] == nil {
					fail(http.StatusNotFound, "etcdserver: requested lease not found")
					return
				}
				kv := etcdKV{Key: unb64(f.t, op.RequestPut.Key), Value: unb64(f.t, op.RequestPut.Value), Lease: lease}
				f.kvs[string(kv.Key)] = kv
				f.puts = append(f.puts, kv)
			}
		}
		reply(map[string]any{"succeeded": ok})
	case "/v3/lease/grant":
		id, ttl := i64(str("ID")), i64(str("TTL"))
		if f.leases[id] != nil {
			fail(http.StatusBadRequest, "etcdserver: lease already exists")
			return
		}
		f.leases[id] = &fakeLease{ttl: ttl, expires: time.Now().Add(time.Duration(ttl) * time.Second)}
		reply(map[string]string{"ID": strconv.FormatInt(id, 10), "TTL": strconv.FormatInt(ttl, 10)})
	case "/v3/lease/timetolive":
		if f.onTimeToLive != nil {
			f.onTimeToLive()
		}
		id := i64(str("ID"))
		l := f.leases[id]
		if l == nil {
			reply(map[string]string{"ID": strconv.FormatInt(id, 10), "TTL": "-1"})
			return
		}
		out := map[string]any{"ID": strconv.FormatInt(id, 10), "grantedTTL": strconv.FormatInt(l.ttl, 10),
			"TTL": strconv.FormatInt(int64(time.Until(l.expires)/time.Second), 10)} // whole seconds, rounded down, as etcd
		if body["keys"] == true {
			var keys []string
			for _, kv := range f.kvs {
				if kv.Lease == id {
					keys = append(keys, b64(kv.Key))
				}
			}
			sort.Strings(keys)
			out["keys"] = keys
		}
		reply(out)
	case "/v3/lease/keepalive":
		id := i64(str("ID"))
		res := map[string]string{"ID": strconv.FormatInt(id, 10)}
		if l := f.leases[id]; l != nil {
			l.expires = time.Now().Add(time.Duration(l.ttl) * time.Second)
			res["TTL"] = strconv.FormatInt(l.ttl, 10)
		}
		_, _ = w.Write([]byte(mustJSON(map[string]any{"result": res}) + "\n"))
	case "/v3/lease/revoke":
		id := i64(str("ID"))
		if f.leases[id] == nil {
			fail(http.StatusNotFound, "etcdserver: requested lease not found")
			return
		}
		f.dropLease(id)
		reply(map[string]any{})
	default:
		http.NotFound(w, r)
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// dynamoWorkerKeys registers a Dynamo worker at ip under lease, as the
// identity spike saw one: three endpoint registrations (no address on the
// NATS request plane), a model card and two event channels naming the
// pod address.
func (f *fakeEtcd) dynamoWorkerKeys(lease int64, ip string) []string {
	hex := strconv.FormatUint(uint64(lease), 16) //nolint:gosec // a lease ID's bits
	keys := []string{}
	for _, ep := range []string{"generate", "clear_kv_blocks", "get_perf_metrics"} {
		k := "v1/instances/idpool/backend/" + ep + "/" + hex
		f.put(k, `{"type":"Endpoint","endpoint":"`+ep+`","instance_id":`+strconv.FormatInt(lease, 10)+`,"transport":{"nats_tcp":"idpool_backend.`+ep+`-`+hex+`"}}`, lease)
		keys = append(keys, k)
	}
	mdc := "v1/mdc/idpool/backend/generate/" + hex
	f.put(mdc, `{"type":"Model","card_json":{"path":"http://`+ip+`:9090/v1/metadata/m/config.json"}}`, lease)
	ev1 := "v1/event_channels/namespace/idpool/component/backend/endpoint/generate/topic/kv_metrics/" + hex + "1"
	f.put(ev1, `{"transport":{"kind":"Zmq","config":{"endpoint":"tcp://`+ip+`:39707"}}}`, lease)
	ev2 := "v1/event_channels/namespace/idpool/component/backend/endpoint/generate/topic/active_sequences_events/" + hex + "2"
	f.put(ev2, `{"transport":{"kind":"Zmq","config":{"endpoint":"tcp://`+ip+`:42505"}}}`, lease)
	return append(keys, mdc, ev1, ev2)
}

// keysWith reports whether every key of keys holds a value naming ip
// where the worker's did, and none names old.
func keysName(t *testing.T, f *fakeEtcd, keys []string, ip, old string) { //nolint:unparam // the address of the test
	t.Helper()
	for _, k := range keys {
		v, ok := f.value(k)
		if !ok {
			t.Errorf("key %s missing", k)
			continue
		}
		if strings.Contains(v, "//"+old+":") {
			t.Errorf("key %s still names the captured address: %s", k, v)
		}
		if strings.Contains(k, "/mdc/") && !strings.Contains(v, "//"+ip+":") {
			t.Errorf("key %s does not name the new address: %s", k, v)
		}
	}
}
