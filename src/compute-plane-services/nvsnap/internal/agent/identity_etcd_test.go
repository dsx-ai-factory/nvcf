// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/containerd"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelvolume"
	nvruntime "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/runtime"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

const (
	idLease  int64 = 0x7b2ea21103c6e9ac
	idOldIP        = "10.245.36.8"
	idNewIP        = "10.245.36.76"
	idEtcdEP       = "mini-service-etcd.reg.svc.cluster.local:2379"
	idKey          = "k1"
)

func idLog() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return logrus.NewEntry(l)
}

// fastIdentity shortens the restore side's timing for a test.
func fastIdentity(t *testing.T) {
	t.Helper()
	p, s, h, tol := identityPoll, identityExpirySlack, identityHandoffFloor, identityTolerance
	identityPoll, identityExpirySlack, identityHandoffFloor, identityTolerance = 20*time.Millisecond, 300*time.Millisecond, 100*time.Millisecond, time.Second
	t.Cleanup(func() { identityPoll, identityExpirySlack, identityHandoffFloor, identityTolerance = p, s, h, tol })
}

// useFakeEtcd makes every registry the identity restore opens f, and
// counts the opens and closes.
func useFakeEtcd(t *testing.T, f *fakeEtcd) (opened, closed *atomic.Int32) {
	t.Helper()
	saved := newIdentityEtcd
	var o, c atomic.Int32
	newIdentityEtcd = func(context.Context, *Agent, string, string, *corev1.Pod, string) (identityEtcd, func(), error) {
		o.Add(1)
		return f.client(), func() { c.Add(1) }, nil
	}
	t.Cleanup(func() { newIdentityEtcd = saved })
	return &o, &c
}

// idWorker is a Dynamo worker, a Deployment's replica, that registers in
// a shared etcd.
func idWorker(name, ip string) *corev1.Pod {
	ctrl := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "shared", UID: types.UID(name + "-uid"),
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "worker-rs", UID: "rs", Controller: &ctrl}},
			Annotations: map[string]string{
				cacheURIAnnotation:                     "cache://cfg",
				modelvolume.CacheOrdinalAnnotation:     "0",
				modelvolume.CacheGroupSizeAnnotation:   "1",
				webhook.AdmittedConfigAnnotation:       "cache://cfg",
				webhook.CRIURestoreContainerAnnotation: "",
			}},
		Spec: corev1.PodSpec{NodeName: "n0", Containers: []corev1.Container{{
			Name: "main",
			Env: []corev1.EnvVar{
				{Name: "DYN_NAMESPACE", Value: "idpool"},
				{Name: "ETCD_ENDPOINTS", Value: idEtcdEP},
				{Name: "NATS_SERVER", Value: "nats://mini-service-nats.reg.svc.cluster.local:4222"},
				{Name: "LD_PRELOAD", Value: webhook.GPUShareLibPath},
			},
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}},
		}}},
		Status: corev1.PodStatus{PodIP: ip, Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
}

// idPlaceholder is a restore placeholder of record idKey.
func idPlaceholder(name, ip string) *corev1.Pod {
	p := idWorker(name, ip)
	p.Annotations[webhook.CRIURestoreAnnotation] = "ck"
	p.Annotations[webhook.CRIUGroupAnnotation] = idKey
	p.Annotations[webhook.CRIURestoreContainerAnnotation] = "main"
	return p
}

func idAgent(kc *fake.Clientset, on bool) *Agent {
	dir, err := os.MkdirTemp("", "nvsnap-identity-test")
	if err != nil {
		panic(err)
	}
	return &Agent{kubeClient: kc, log: idLog().Logger, config: Config{IdentityRestore: on, CheckpointDir: dir}}
}

// idRecord is a group record of checkpoint ck holding the identity f's
// worker had at idOldIP.
func idRecord(t *testing.T, keys []string, f *fakeEtcd) *corev1.ConfigMap {
	const ck = "ck"
	t.Helper()
	id := &capturedIdentity{Lease: idLease, TTL: 2, PodIP: idOldIP, Endpoints: idEtcdEP}
	for _, k := range keys {
		v, ok := f.value(k)
		if !ok {
			t.Fatalf("no key %s", k)
		}
		id.Keys = append(id.Keys, identityKey{Key: []byte(k), Value: []byte(v)})
	}
	d := map[string]string{"state": criuGroupStateComplete, "size": "1", "checkpoint.0": ck, "node.0": "n0"}
	if err := id.write(d); err != nil {
		t.Fatal(err)
	}
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: criuGroupConfigMapName(idKey), Namespace: "nvsnap-system"}, Data: d}
}

func decisions(t *testing.T, d string) float64 {
	return metricValue(t, metrics.IdentityRestoreDecisions, map[string]string{"decision": d})
}

func TestEtcdGatewayBases(t *testing.T) {
	for _, tc := range []struct {
		in, want, err string
	}{
		{"etcd:2379", "http://etcd.fn.svc:2379", ""},
		{"etcd.reg:2379", "http://etcd.reg.svc:2379", ""},
		{"http://etcd.reg.svc.cluster.local:2379", "http://etcd.reg.svc.cluster.local:2379", ""},
		{"10.0.0.5", "http://10.0.0.5:2379", ""},
		{"etcd-0:2379, etcd-1:2379", "http://etcd-0.fn.svc:2379 http://etcd-1.fn.svc:2379", ""},
		{"https://etcd:2379", "", "not supported"},
		{"", "", "no etcd endpoint"},
	} {
		got, err := etcdGatewayBases(tc.in, "fn")
		if tc.err != "" {
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("%q: error %v, want %q", tc.in, err, tc.err)
			}
			continue
		}
		if err != nil || strings.Join(got, " ") != tc.want {
			t.Errorf("%q: %v %v, want %s", tc.in, got, err, tc.want)
		}
	}
}

// The one decision both backends share: who may be captured alone.
// Kubernetes discovery is refused until its backend lands.
func TestIdentityRestoreCandidate(t *testing.T) {
	setEnv := func(p *corev1.Pod, name, value string) *corev1.Pod {
		for i, e := range p.Spec.Containers[0].Env {
			if e.Name == name {
				p.Spec.Containers[0].Env[i].Value = value
				return p
			}
		}
		p.Spec.Containers[0].Env = append(p.Spec.Containers[0].Env, corev1.EnvVar{Name: name, Value: value})
		return p
	}
	hostNet := idWorker("w", idOldIP)
	hostNet.Spec.HostNetwork = true
	notDynamo := idWorker("w", idOldIP)
	notDynamo.Spec.Containers[0].Env = notDynamo.Spec.Containers[0].Env[1:]
	byRef := idWorker("w", idOldIP)
	byRef.Spec.Containers[0].Env[1] = corev1.EnvVar{Name: "ETCD_ENDPOINTS", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{Key: "x"}}}
	for name, tc := range map[string]struct {
		pod     *corev1.Pod
		backend string
	}{
		"etcd discovery unset":   {idWorker("w", idOldIP), identityBackendEtcd},
		"etcd discovery":         {setEnv(idWorker("w", idOldIP), "DYN_DISCOVERY_BACKEND", "etcd"), identityBackendEtcd},
		"kubernetes discovery":   {setEnv(idWorker("w", idOldIP), "DYN_DISCOVERY_BACKEND", "kubernetes"), ""},
		"host network":           {hostNet, ""},
		"not a Dynamo component": {notDynamo, ""},
		"etcd by reference":      {byRef, ""},
		"etcd over TLS":          {setEnv(idWorker("w", idOldIP), "ETCD_ENDPOINTS", "https://etcd:2379"), ""},
		"no etcd endpoint":       {setEnv(idWorker("w", idOldIP), "ETCD_ENDPOINTS", ""), ""},
	} {
		if b, why := identityRestoreCandidate(tc.pod); b != tc.backend || (b == "" && why == "") {
			t.Errorf("%s: backend %q (%s), want %q", name, b, why, tc.backend)
		}
	}
	a, b := idWorker("a", "10.0.0.1"), setEnv(idWorker("b", "10.0.0.2"), "ETCD_ENDPOINTS", "other:2379")
	if be, _ := identityRanksCandidate([]*corev1.Pod{a, b}); be != "" {
		t.Error("ranks naming different registries are a candidate")
	}
}

func TestIdentityLeaseOf(t *testing.T) {
	f := newFakeEtcd(t)
	f.dynamoWorkerKeys(idLease, idOldIP)
	f.dynamoWorkerKeys(idLease+1, "10.245.36.78") // worker A
	kvs, err := f.client().RangePrefix(context.Background(), []byte("v1/"))
	if err != nil {
		t.Fatal(err)
	}
	if l, m, err := identityLeaseOf(kvs, []string{idOldIP}); err != nil || l != idLease || m != 0 {
		t.Errorf("found %x member %d (%v), want the worker's lease", l, m, err)
	}
	if l, m, err := identityLeaseOf(kvs, []string{"10.9.9.9", idOldIP}); err != nil || l != idLease || m != 1 {
		t.Errorf("second rank: %x %d %v", l, m, err)
	}
	if _, _, err := identityLeaseOf(kvs, []string{"10.9.9.9"}); err == nil {
		t.Error("an address no registration names found a lease")
	}
	// A prefix of another address must not match: 10.245.36.7 is in
	// 10.245.36.78.
	if _, _, err := identityLeaseOf(kvs, []string{"10.245.36.7"}); err == nil {
		t.Error("a prefix of another worker's address found its lease")
	}
	// Two leases naming the address (two workers on one host address):
	// the identity cannot be told.
	f.dynamoWorkerKeys(idLease+2, idOldIP)
	kvs, _ = f.client().RangePrefix(context.Background(), []byte("v1/"))
	if _, _, err := identityLeaseOf(kvs, []string{idOldIP}); err == nil {
		t.Error("two leases naming the address were taken for one identity")
	}
	// A key naming the address under a lease with no instance
	// registration of its own (a frontend's, say) is not a worker's.
	g := newFakeEtcd(t)
	g.grant(5, 10)
	g.put("v1/mdc/x/y/z/5", `{"path":"http://`+idOldIP+`:9090/"}`, 5)
	kvs, _ = g.client().RangePrefix(context.Background(), []byte("v1/"))
	if _, _, err := identityLeaseOf(kvs, []string{idOldIP}); err == nil {
		t.Error("a lease without its own instance registration was taken for a worker")
	}
	// An instance registration named by another ID does not make the
	// lease a worker's own.
	g.put("v1/instances/x/y/z/ffff", `{}`, 5)
	kvs, _ = g.client().RangePrefix(context.Background(), []byte("v1/"))
	if _, _, err := identityLeaseOf(kvs, []string{idOldIP}); err == nil {
		t.Error("a registration named by another ID was taken for the lease's own")
	}
}

func TestRewriteAndStrayAddress(t *testing.T) {
	v := []byte(`{"a":"http://10.0.0.1:9090/x","b":"tcp://10.0.0.1:1","c":"http://10.0.0.12:1"}`)
	got := string(rewriteHost(v, "10.0.0.1", "10.0.0.2"))
	if got != `{"a":"http://10.0.0.2:9090/x","b":"tcp://10.0.0.2:1","c":"http://10.0.0.12:1"}` {
		t.Errorf("rewrite: %s", got)
	}
	ips := []string{"10.0.0.1"}
	for _, tc := range []struct {
		v     string
		stray bool
	}{
		{`{"a":"http://10.0.0.1:9090/x"}`, false},
		{`{"host":"10.0.0.1"}`, true},
		{`{"a":"10.0.0.1:5600"}`, true},
		{`{"a":"10.0.0.12"}`, false},
		{`{"a":"110.0.0.1"}`, false},
		{`{"a":"10.0.0.1.5"}`, false},
	} {
		if s := strayAddress([]byte(tc.v), "10.0.0.1", ips); s != tc.stray {
			t.Errorf("%s: stray %v, want %v", tc.v, s, tc.stray)
		}
	}
	if !strayAddress([]byte(`x`), "", nil) == false {
		t.Error("nothing named, yet stray")
	}
	if got := string(rewriteHost([]byte("http://[fd00::1]:80/"), "fd00::1", "fd00::2")); got != "http://[fd00::2]:80/" {
		t.Errorf("IPv6 rewrite: %s", got)
	}
}

func TestReadIdentity(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	f.grant(idLease, 30)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	f.grant(idLease+1, 30)
	f.dynamoWorkerKeys(idLease+1, "10.245.36.78")
	// A key outside the Dynamo root, under the worker's lease, is saved
	// too.
	f.put("other/x", "y", idLease)
	id, err := readIdentity(ctx, f.client(), []string{idOldIP}, idEtcdEP)
	if err != nil {
		t.Fatal(err)
	}
	if id.Lease != idLease || id.TTL != 30 || id.PodIP != idOldIP || len(id.Keys) != len(keys)+1 {
		t.Fatalf("identity %+v", id)
	}
	// Round trip through the record.
	d := map[string]string{identityHolderKey: "stale"}
	if werr := id.write(d); werr != nil {
		t.Fatal(werr)
	}
	if _, held := d[identityHolderKey]; held {
		t.Error("a new identity kept an old holder")
	}
	back, rerr := identityFromRecord(d)
	if err != nil || back.Lease != id.Lease || len(back.Keys) != len(id.Keys) || !bytes.Equal(back.Keys[0].Value, id.Keys[0].Value) {
		t.Fatalf("record round trip: %+v %v", back, rerr)
	}
	for _, k := range []string{identityLeaseKey, identityTTLKey, identityKeysKey, identityPodIPKey, identityBackendKey} {
		bad := map[string]string{}
		for kk, v := range d {
			bad[kk] = v
		}
		bad[k] = "x"
		if _, err := identityFromRecord(bad); err == nil {
			t.Errorf("a record with a bad %s was read", k)
		}
	}
	if id, err := identityFromRecord(map[string]string{"state": "complete"}); id != nil || err != nil {
		t.Error("a record without an identity read as one")
	}

	// Fail closed: a value naming the address in a form a restore cannot
	// rewrite.
	f.put("v1/event_channels/x/"+strconv.FormatUint(uint64(idLease), 16), `{"host":"`+idOldIP+`"}`, idLease)
	if _, err := readIdentity(ctx, f.client(), []string{idOldIP}, idEtcdEP); err == nil || !strings.Contains(err.Error(), "cannot rewrite") {
		t.Errorf("a bare address was saved: %v", err)
	}
	// Fail closed: the registry cannot be read.
	f.setFail("/v3/kv/range", true)
	if _, err := readIdentity(ctx, f.client(), []string{idOldIP}, idEtcdEP); err == nil {
		t.Error("an unreadable registry gave an identity")
	}
}

func TestAwaitLeaseFree(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	t.Run("never granted", func(t *testing.T) {
		f := newFakeEtcd(t)
		if err := awaitLeaseFree(ctx, f.client(), idLease, 1); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("expires within its TTL", func(t *testing.T) {
		f := newFakeEtcd(t)
		f.grant(idLease, 1)
		t0 := time.Now()
		if err := awaitLeaseFree(ctx, f.client(), idLease, 1); err != nil {
			t.Fatal(err)
		}
		if time.Since(t0) < 500*time.Millisecond {
			t.Error("did not wait for the lease to expire")
		}
	})
	t.Run("kept alive", func(t *testing.T) {
		f := newFakeEtcd(t)
		f.grant(idLease, 2)
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			for {
				select {
				case <-stop:
					return
				case <-time.After(200 * time.Millisecond):
					f.refresh(idLease)
				}
			}
		}()
		err := awaitLeaseFree(ctx, f.client(), idLease, 2)
		if identityDecision(err) != identityRefusedLiveLease {
			t.Fatalf("a lease kept alive: %v", err)
		}
	})
	t.Run("renewed: refused at once, not at the deadline", func(t *testing.T) {
		f := newFakeEtcd(t)
		f.grant(idLease, 3)
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			for {
				select {
				case <-stop:
					return
				case <-time.After(1200 * time.Millisecond):
					f.refresh(idLease)
				}
			}
		}()
		t0 := time.Now()
		err := awaitLeaseFree(ctx, f.client(), idLease, 30)
		if identityDecision(err) != identityRefusedLiveLease || time.Since(t0) > 5*time.Second {
			t.Fatalf("a renewed lease: %v after %s", err, time.Since(t0))
		}
	})
	t.Run("unreadable", func(t *testing.T) {
		f := newFakeEtcd(t)
		f.setFail("/v3/lease/timetolive", true)
		if err := awaitLeaseFree(ctx, f.client(), idLease, 1); identityDecision(err) != identityRefusedError {
			t.Fatalf("an unreadable lease: %v", err)
		}
	})
}

func TestRegrant(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	f.grant(idLease, 2)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	id, err := readIdentity(ctx, f.client(), []string{idOldIP}, idEtcdEP)
	if err != nil {
		t.Fatal(err)
	}
	// The lease is still live: etcd refuses the grant.
	if err := regrant(ctx, f.client(), id, idNewIP); identityDecision(err) != identityRefusedLiveLease {
		t.Fatalf("grant of a live lease: %v", err)
	}
	if err := f.client().Revoke(ctx, idLease); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.value(keys[len(keys)-1]); ok {
		t.Fatal("revoke left the keys")
	}
	if err := regrant(ctx, f.client(), id, idNewIP); err != nil {
		t.Fatal(err)
	}
	if !f.live(idLease) {
		t.Fatal("not granted")
	}
	keysName(t, f, keys, idNewIP, idOldIP)

	// A failed write revokes the grant: nothing half-written stays.
	g := newFakeEtcd(t)
	g.setFail("/v3/kv/txn", true)
	if err := regrant(ctx, g.client(), id, idNewIP); identityDecision(err) != identityRefusedError {
		t.Fatalf("failed put: %v", err)
	}
	if g.live(idLease) {
		t.Error("the lease of a failed write was not revoked")
	}
}

func TestClaimIdentity(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	me := idPlaceholder("w-new", idNewIP)
	other := idPlaceholder("w-other", "10.0.0.9")
	for _, tc := range []struct {
		name   string
		holder string
		pods   []runtime.Object
		getErr bool
		want   string
	}{
		{"free", "", nil, false, ""},
		{"held by itself", holderOf(me), nil, false, ""},
		{"held by a live pod", holderOf(other), []runtime.Object{other}, false, identityRefusedHeld},
		{"held by a pod gone", holderOf(other), nil, false, ""},
		{"held by a pod replaced under its name", "shared/w-other/old-uid", []runtime.Object{other}, false, ""},
		{"holder unreadable", holderOf(other), nil, true, identityRefusedError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := idRecord(t, keys, f)
			if tc.holder != "" {
				rec.Data[identityHolderKey] = tc.holder
			}
			kc := fake.NewSimpleClientset(append(tc.pods, rec, me)...)
			if tc.getErr {
				kc.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("api down")
				})
			}
			c := idAgent(kc, true).criuRestorer()
			err := c.claimIdentity(ctx, idKey, me)
			if identityDecision(err) != tc.want {
				t.Fatalf("claim: %v, want %q", err, tc.want)
			}
			cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
			if tc.want == "" && cm.Data[identityHolderKey] != holderOf(me) {
				t.Errorf("holder %q after a claim", cm.Data[identityHolderKey])
			}
			if tc.want != "" && cm.Data[identityHolderKey] != tc.holder {
				t.Errorf("a refused claim changed the holder to %q", cm.Data[identityHolderKey])
			}
		})
	}
}

// Two agents claim one identity at once: the record's version lets one
// win.
func TestClaimIdentity_Concurrent(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	rec.ResourceVersion = "0"
	a1, a2 := idPlaceholder("w1", "10.0.0.1"), idPlaceholder("w2", "10.0.0.2")
	kc := fake.NewSimpleClientset(rec, a1, a2)
	// Both read the record before either writes (the fake has no
	// versions of its own: the reactor gives them).
	var mu sync.Mutex
	version, conflicts := 0, 0
	kc.PrependReactor("update", "configmaps", func(act k8stesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		cm := act.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if cm.ResourceVersion != strconv.Itoa(version) {
			conflicts++
			return true, nil, apierrors.NewConflict(corev1.Resource("configmaps"), cm.Name, errors.New("stale"))
		}
		version++
		cm = cm.DeepCopy()
		cm.ResourceVersion = strconv.Itoa(version)
		if err := kc.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace); err != nil {
			return true, nil, err
		}
		return true, cm, nil
	})
	// Both claimants read the record before either writes: the first two
	// reads wait for each other.
	var gets sync.WaitGroup
	gets.Add(2)
	var getN atomic.Int32
	identityClaimReadHook = func() {
		if getN.Add(1) <= 2 {
			gets.Done()
			gets.Wait()
		}
	}
	t.Cleanup(func() { identityClaimReadHook = nil })
	c := idAgent(kc, true).criuRestorer()
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, p := range []*corev1.Pod{a1, a2} {
		wg.Add(1)
		go func(i int, p *corev1.Pod) {
			defer wg.Done()
			errs[i] = c.claimIdentity(ctx, idKey, p)
		}(i, p)
	}
	wg.Wait()
	won := 0
	for _, err := range errs {
		if err == nil {
			won++
		} else if identityDecision(err) != identityRefusedHeld {
			t.Errorf("loser: %v", err)
		}
	}
	if won != 1 || conflicts != 1 {
		t.Fatalf("%d claims won, %d conflicts: %v", won, conflicts, errs)
	}
}

// keeperCalls records which end callbacks a keeper ran.
type keeperCalls struct {
	mu                       sync.Mutex
	aborted, zombie, written int
}

func (k *keeperCalls) get() (aborted, zombie, written int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.aborted, k.zombie, k.written
}

func TestLeaseKeeper(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	start := func(f *fakeEtcd, ttl int64) (*leaseKeeper, *keeperCalls) {
		f.grant(idLease, ttl)
		calls := &keeperCalls{}
		k := newLeaseKeeper(f.client(), idLease, ttl, idLog())
		k.onAborted = func() { calls.mu.Lock(); calls.aborted++; calls.mu.Unlock() }
		k.onZombie = func() { calls.mu.Lock(); calls.zombie++; calls.mu.Unlock() }
		k.writeKeys = func(context.Context) error { calls.mu.Lock(); calls.written++; calls.mu.Unlock(); return nil }
		go k.run(ctx)
		return k, calls
	}
	wait := func(t *testing.T, k *leaseKeeper, within time.Duration) {
		t.Helper()
		select {
		case <-k.done:
		case <-time.After(within):
			t.Fatal("keeper did not end")
		}
	}
	refreshing := func(f *fakeEtcd, every time.Duration) func() {
		stop := make(chan struct{})
		go func() {
			for {
				select {
				case <-stop:
					return
				case <-time.After(every):
					f.refresh(idLease)
				}
			}
		}()
		return func() { close(stop) }
	}
	t.Run("keeps the lease alive through a long restore, registers, hands off", func(t *testing.T) {
		f := newFakeEtcd(t)
		k, calls := start(f, 2)
		time.Sleep(3 * time.Second) // longer than the TTL: the keeper renews
		if !f.live(idLease) {
			t.Fatal("the keeper let the lease expire during the restore")
		}
		if _, _, w := calls.get(); w != 0 {
			t.Fatal("keys written before the process resumed")
		}
		k.restored(nil)
		defer refreshing(f, 700*time.Millisecond)() // the restored process's keep-alive
		wait(t, k, 6*time.Second)
		ab, z, w := calls.get()
		if k.outcome != identityHandoffSeen || ab+z != 0 || w != 1 {
			t.Fatalf("outcome %s, aborted %d zombie %d written %d", k.outcome, ab, z, w)
		}
		if !f.live(idLease) {
			t.Error("the lease went with the handoff")
		}
	})
	t.Run("one renewal not its own is not a handoff", func(t *testing.T) {
		// etcd renews every lease on a leader change: one renewal alone
		// must not hand the lease to a process that does not keep it.
		f := newFakeEtcd(t)
		k, calls := start(f, 2)
		k.restored(nil)
		time.Sleep(1500 * time.Millisecond)
		f.refresh(idLease)
		wait(t, k, 8*time.Second)
		if _, z, _ := calls.get(); k.outcome != identityHandoffTimeout || z != 1 {
			t.Fatalf("outcome %s, zombie %d", k.outcome, z)
		}
	})
	t.Run("a renewal right after resume (a leader change) is not a handoff", func(t *testing.T) {
		f := newFakeEtcd(t)
		k, _ := start(f, 4)
		k.restored(nil)
		time.Sleep(2 * time.Second)
		f.refresh(idLease) // the TTL now runs ahead of the agent's own renewal
		select {
		case <-k.done:
			t.Fatalf("ended %s on one renewal not its own", k.outcome)
		case <-time.After(5 * time.Second):
		}
		k.restored(errors.New("stop")) // no effect: already resumed
	})
	t.Run("restored engine already running: handed off, not revoked", func(t *testing.T) {
		f := newFakeEtcd(t)
		k, calls := start(f, 2)
		defer refreshing(f, 700*time.Millisecond)()
		k.restored(fmt.Errorf("wrapped: %w", errNotPlaceholder))
		wait(t, k, 6*time.Second)
		if ab, _, w := calls.get(); k.outcome != identityHandoffSeen || ab != 0 || w != 1 || !f.live(idLease) {
			t.Fatalf("outcome %s aborted %d written %d", k.outcome, ab, w)
		}
	})
	t.Run("process never keeps it alive: revoked, pod ended, claim kept", func(t *testing.T) {
		f := newFakeEtcd(t)
		k, calls := start(f, 1)
		k.restored(nil)
		wait(t, k, 6*time.Second)
		ab, z, _ := calls.get()
		if k.outcome != identityHandoffTimeout || z != 1 || ab != 0 || f.live(idLease) {
			t.Fatalf("outcome %s, zombie %d aborted %d, live %v", k.outcome, z, ab, f.live(idLease))
		}
	})
	t.Run("keys cannot be written: revoked, pod ended", func(t *testing.T) {
		f := newFakeEtcd(t)
		k, calls := start(f, 5)
		k.writeKeys = func(context.Context) error { return errors.New("put failed") }
		k.restored(nil)
		wait(t, k, 2*time.Second)
		if _, z, _ := calls.get(); k.outcome != identityKeysFailed || z != 1 || f.live(idLease) {
			t.Fatalf("outcome %s, zombie %d", k.outcome, z)
		}
	})
	t.Run("restore failed: revoked, claim released", func(t *testing.T) {
		f := newFakeEtcd(t)
		k, calls := start(f, 5)
		k.restored(errors.New("criu failed"))
		k.restored(nil) // only the first counts
		wait(t, k, 2*time.Second)
		ab, z, w := calls.get()
		if k.outcome != identityRestoreAborted || ab != 1 || z != 0 || w != 0 || f.live(idLease) {
			t.Fatalf("outcome %s, aborted %d zombie %d written %d, live %v", k.outcome, ab, z, w, f.live(idLease))
		}
	})
	t.Run("lease lost: pod ended", func(t *testing.T) {
		f := newFakeEtcd(t)
		k, calls := start(f, 5)
		_ = f.client().Revoke(ctx, idLease)
		wait(t, k, 2*time.Second)
		if _, z, _ := calls.get(); k.outcome != identityHandoffLost || z != 1 {
			t.Fatalf("outcome %s, zombie %d", k.outcome, z)
		}
	})
	t.Run("agent stops", func(t *testing.T) {
		f := newFakeEtcd(t)
		f.grant(idLease, 5)
		cctx, cancel := context.WithCancel(ctx)
		k := newLeaseKeeper(f.client(), idLease, 5, idLog())
		go k.run(cctx)
		cancel()
		wait(t, k, 2*time.Second)
		if k.outcome != identityKeeperAgentStopped || !f.live(idLease) {
			t.Fatalf("outcome %s; an agent that stops leaves the lease to expire on its own", k.outcome)
		}
	})
	var nilKeeper *leaseKeeper
	nilKeeper.restored(nil) // nil-safe
}

// The lone restore path: the identity is granted again before the
// process is restored, and the restore's outcome reaches the keeper.
func TestLoneRestore_RegrantsBeforeRestoring(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP) // lease gone: keys only in the record
	rec := idRecord(t, keys, f)
	f.mu.Lock()
	f.kvs = map[string]etcdKV{}
	f.mu.Unlock()
	opened, closed := useFakeEtcd(t, f)
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(rec, pod)
	a := idAgent(kc, true)
	a.config.CheckpointDir = t.TempDir()
	c := a.criuRestorer()

	saved := restoreForRequest
	t.Cleanup(func() { restoreForRequest = saved })
	var grantedFirst, registeredEarly bool
	restoreForRequest = func(_ *Agent, _ context.Context, req RestoreRequest) (*RestoreResult, error) {
		grantedFirst = f.live(idLease)
		_, registeredEarly = f.value(keys[0])
		return &RestoreResult{}, nil
	}
	before := decisions(t, identityRegranted)
	c.restore(ctx, "shared", "w-new", pod.UID, "ck", "main", idKey)
	if !grantedFirst {
		t.Fatal("the process was restored before its lease was granted again")
	}
	if registeredEarly {
		t.Fatal("the worker was registered at its new address before anything listened there")
	}
	// Registered once the process resumed.
	deadline0 := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline0) {
		if _, ok := f.value(keys[len(keys)-1]); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	keysName(t, f, keys, idNewIP, idOldIP)
	if decisions(t, identityRegranted) != before+1 {
		t.Error("regrant not counted")
	}
	cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
	if cm.Data[identityHolderKey] != holderOf(pod) {
		t.Errorf("holder %q", cm.Data[identityHolderKey])
	}
	if opened.Load() != 1 {
		t.Errorf("%d registries opened", opened.Load())
	}
	// The keeper waits for the process; with none here it revokes.
	deadline := time.Now().Add(6 * time.Second)
	for f.live(idLease) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if f.live(idLease) {
		t.Error("a process that never kept its lease alive kept it")
	}
	for closed.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if closed.Load() != 1 {
		t.Error("the registry was not closed")
	}
	// The pod that lost its identity is ended; its claim stays until it
	// is gone.
	for time.Now().Before(deadline) {
		if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); apierrors.IsNotFound(err) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Error("the restored pod whose process never kept its lease alive was kept")
	}
	if cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{}); cm.Data[identityHolderKey] != holderOf(pod) {
		t.Errorf("claim released while the pod could still hold the identity: %q", cm.Data[identityHolderKey])
	}
}

// An agent that restarts after it restored a pod, before it marked it,
// finds the identity held by that very pod and the lease live: it adopts
// it, the restore finds the engine running, and the serving pod is kept,
// its lease handed to it rather than revoked.
func TestLoneRestore_AgentRestartAdoptsIdentity(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	pod := idPlaceholder("w-new", idNewIP)
	rec.Data[identityHolderKey] = holderOf(pod)
	rec.Data[identityGrantedKey] = holderOf(pod)
	f.grant(idLease, 2)
	stop := make(chan struct{})
	defer close(stop)
	go func() { // the restored process keeps its lease
		for {
			select {
			case <-stop:
				return
			case <-time.After(600 * time.Millisecond):
				f.refresh(idLease)
			}
		}
	}()
	useFakeEtcd(t, f)
	kc := fake.NewSimpleClientset(rec, pod)
	c := idAgent(kc, true).criuRestorer()
	saved := restoreForRequest
	t.Cleanup(func() { restoreForRequest = saved })
	restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
		return nil, fmt.Errorf("criu-v2: %w", errNotPlaceholder)
	}
	refused := decisions(t, identityRefusedLiveLease)
	handoffs := decisions(t, identityHandoffSeen)
	c.restore(ctx, "shared", "w-new", pod.UID, "ck", "main", idKey)
	if decisions(t, identityRefusedLiveLease) != refused {
		t.Fatal("the pod's own live lease was refused")
	}
	deadline := time.Now().Add(6 * time.Second)
	for decisions(t, identityHandoffSeen) == handoffs && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if decisions(t, identityHandoffSeen) != handoffs+1 || !f.live(idLease) {
		t.Fatal("the serving process's lease was not handed back to it")
	}
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); err != nil {
		t.Fatalf("the serving pod was deleted: %v", err)
	}
}

// With the flag off, a placeholder admitted while it was on (a record with
// an identity) is refused and starts cold, never restored without its
// lease.
func TestLoneRestore_FlagOffRefusesIdentityRecord(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	opened, _ := useFakeEtcd(t, f)
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(rec, pod)
	c := idAgent(kc, false).criuRestorer()
	saved := restoreForRequest
	t.Cleanup(func() { restoreForRequest = saved })
	restored := false
	restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
		restored = true
		return nil, nil
	}
	before := decisions(t, identityRefusedDisabled)
	c.restore(ctx, "shared", "w-new", pod.UID, "ck", "main", idKey)
	if restored || opened.Load() != 0 || decisions(t, identityRefusedDisabled) != before+1 {
		t.Fatalf("restored %v, registry opened %d", restored, opened.Load())
	}
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Error("the refused pod, which cannot start cold here, was kept")
	}
}

// A live lease refuses the restore: nothing is restored, the checkpoint
// is not blocked, the record counts no failure, and the pod starts cold
// (here, without a runtime, it is deleted for its controller to replace).
func TestLoneRestore_LiveLeaseRefused(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	f.grant(idLease, 2)
	stop := make(chan struct{})
	defer close(stop)
	go func() { // another restore of this identity serves
		for {
			select {
			case <-stop:
				return
			case <-time.After(300 * time.Millisecond):
				f.refresh(idLease)
			}
		}
	}()
	useFakeEtcd(t, f)
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(rec, pod)
	a := idAgent(kc, true)
	c := a.criuRestorer()
	saved := restoreForRequest
	t.Cleanup(func() { restoreForRequest = saved })
	restored := false
	restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
		restored = true
		return nil, nil
	}
	before := decisions(t, identityRefusedLiveLease)
	c.restore(ctx, "shared", "w-new", pod.UID, "ck", "main", idKey)
	if restored {
		t.Fatal("restored while the identity is live")
	}
	if decisions(t, identityRefusedLiveLease) != before+1 {
		t.Error("refusal not counted")
	}
	if c.Blocked(ctx, "ck") {
		t.Error("the checkpoint was blocked for a refusal")
	}
	cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
	// Deleted (it could not start cold here): its replacement may be
	// refused the same way, so the refusal counts toward the stall bound.
	if cm.Data["stalls"] != "1" || cm.Data["state"] != criuGroupStateComplete || cm.Data[identityHolderKey] != "" {
		t.Errorf("record after a refusal: stalls %q state %q holder %q", cm.Data["stalls"], cm.Data["state"], cm.Data[identityHolderKey])
	}
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("the refused pod, which cannot start cold here, was not deleted: %v", err)
	}
	for _, act := range kc.Actions() {
		if act.GetVerb() == "update" && act.GetResource().Resource == "configmaps" {
			if cmu := act.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap); cmu.Name == criuRestoreFailedConfigMap {
				t.Error("the refusal wrote the failed-checkpoint list")
			}
		}
	}
}

// The restore fails after the grant: the lease is revoked and the claim
// released, so the next restore of the identity can take it.
func TestLoneRestore_FailedRestoreRevokes(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	useFakeEtcd(t, f)
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(rec, pod)
	c := idAgent(kc, true).criuRestorer()
	saved := restoreForRequest
	t.Cleanup(func() { restoreForRequest = saved })
	restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
		return nil, fmt.Errorf("%w: test", errCheckpointUnavailable)
	}
	c.restore(ctx, "shared", "w-new", pod.UID, "ck", "main", idKey)
	deadline := time.Now().Add(3 * time.Second)
	for f.live(idLease) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if f.live(idLease) {
		t.Fatal("the lease of a failed restore was kept")
	}
	for time.Now().Before(deadline) {
		cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
		if cm.Data[identityHolderKey] == "" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("the claim of a failed restore was kept")
}

// With the flag off nothing of the identity restore runs on a restore,
// and admission never makes a placeholder of a record with an identity.
func TestIdentityRestore_FlagOff(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	opened, _ := useFakeEtcd(t, f)
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(rec, pod)
	a := idAgent(kc, false)
	c := a.criuRestorer()
	// A record without an identity: nothing happens, the registry is
	// never opened.
	plain0 := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: criuGroupConfigMapName("k0"), Namespace: "nvsnap-system"},
		Data: map[string]string{"state": criuGroupStateComplete, "size": "1", "checkpoint.0": "c0"}}
	_, _ = kc.CoreV1().ConfigMaps("nvsnap-system").Create(ctx, plain0, metav1.CreateOptions{})
	k, _, err := c.loneIdentityBeforeRestore(ctx, ctx, "k0", "shared", "w-new", "w-new-uid", "c0", idLog())
	if k != nil || err != nil || opened.Load() != 0 {
		t.Fatalf("flag off: keeper %v err %v, registry opened %d", k, err, opened.Load())
	}
	if _, ok := a.lookupCRIUGroup(ctx, idKey); ok {
		t.Error("admission restores a record with an identity with the flag off")
	}
	a.config.IdentityRestore = true
	if _, ok := a.lookupCRIUGroup(ctx, idKey); !ok {
		t.Error("admission does not restore a record with an identity with the flag on")
	}
	// A record without an identity is admitted as it always was.
	plain := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: criuGroupConfigMapName("k2"), Namespace: "nvsnap-system"},
		Data: map[string]string{"state": criuGroupStateComplete, "size": "1", "checkpoint.0": "c2", "node.0": "n0"}}
	_, _ = kc.CoreV1().ConfigMaps("nvsnap-system").Create(ctx, plain, metav1.CreateOptions{})
	a.config.IdentityRestore = false
	if _, ok := a.lookupCRIUGroup(ctx, "k2"); !ok {
		t.Error("a record without an identity is no longer admitted")
	}
}

// The shared-companions guard lets a placeholder of an identity record
// through, and only that: a record with companions, or without an
// identity, or with the flag off, is refused as before.
func TestSharedCompanionsReason_IdentityPlaceholder(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	other := idWorker("w-other", "10.0.0.9")
	for _, tc := range []struct {
		name     string
		on       bool
		identity bool
		comp     bool
		shared   bool // want the shared reason
	}{
		{"identity record, flag on", true, true, false, false},
		{"identity record, flag off", false, true, false, true},
		{"record without identity", true, false, false, true},
		{"identity record with companions", true, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := idRecord(t, keys, f)
			if !tc.identity {
				rec.Data = map[string]string{"state": criuGroupStateComplete, "checkpoint.0": "ck"}
			}
			if tc.comp {
				rec.Data["companion.1"] = "true"
			}
			pod := idPlaceholder("w-new", idNewIP)
			kc := fake.NewSimpleClientset(rec, pod, other)
			why, err := idAgent(kc, tc.on).sharedCompanionsReason(ctx, pod, nil)
			if err != nil || (why != "") != tc.shared {
				t.Fatalf("reason %q err %v, want shared %v", why, err, tc.shared)
			}
		})
	}
	// An unreadable record fails closed.
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(pod, other)
	kc.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("api down")
	})
	if _, err := idAgent(kc, true).sharedCompanionsReason(ctx, pod, nil); err == nil {
		t.Error("an unreadable record let the placeholder through")
	}
}

// The capture side: with the flag on, a worker in a namespace that
// shares its companions is no longer skipped by the shared guard but
// reaches the identity read (and is refused when its identity cannot be
// read); with the flag off it is skipped as before.
func TestCaptureGroup_SharedNamespaceReachesIdentity(t *testing.T) {
	saved := groupWarmupWait
	groupWarmupWait = 0
	t.Cleanup(func() { groupWarmupWait = saved })
	ctx := context.Background()
	for _, on := range []bool{false, true} {
		t.Run(fmt.Sprintf("flag %v", on), func(t *testing.T) {
			f := newFakeEtcd(t) // no registration names the worker
			opened, _ := useFakeEtcd(t, f)
			leader := idWorker("w-a", idOldIP)
			other := idWorker("w-b", "10.0.0.9")
			other.OwnerReferences[0].UID = "rs2"
			kc := fake.NewSimpleClientset(leader, other)
			a := idAgent(kc, on)
			a.seedDrivers()
			c := a.criuRestorer()
			before := decisions(t, identityCaptureRefused)
			if retry := c.captureGroup(ctx, leader, "", 1); retry {
				t.Fatal("retried")
			}
			reached := decisions(t, identityCaptureRefused) == before+1
			if reached != on || (opened.Load() > 0) != on {
				t.Fatalf("identity read reached %v (registry opened %d), want %v", reached, opened.Load(), on)
			}
			if cms, _ := kc.CoreV1().ConfigMaps("nvsnap-system").List(ctx, metav1.ListOptions{}); len(cms.Items) != 0 {
				t.Errorf("a refused capture wrote %d records", len(cms.Items))
			}
		})
	}
}

// checkpointInstance passes the group's addresses to the member's dump
// when the capture asks for the reset, and nothing otherwise.
func TestCheckpointInstance_PeerResetRequest(t *testing.T) {
	saved := checkpointMember
	t.Cleanup(func() { checkpointMember = saved })
	var got []string
	checkpointMember = func(_ *Agent, _ context.Context, req CheckpointRequest) (*CheckpointResult, error) {
		got = req.ResetPeersOutside
		return &CheckpointResult{CheckpointID: "ck", PeersReset: len(req.ResetPeersOutside) > 0}, nil
	}
	c := idAgent(fake.NewSimpleClientset(), true).criuRestorer()
	w := idWorker("w", idOldIP)
	if _, _, err := c.checkpointInstance(context.Background(), []*corev1.Pod{w}, nil, nil, idLog()); err != nil || got != nil {
		t.Fatalf("plain capture: reset %v (%v)", got, err)
	}
	ctx := withPeerReset(context.Background(), memberAddresses([]*corev1.Pod{w}))
	if _, _, err := c.checkpointInstance(ctx, []*corev1.Pod{w}, nil, nil, idLog()); err != nil || strings.Join(got, ",") != idOldIP {
		t.Fatalf("identity capture: reset %v (%v)", got, err)
	}
}

// The whole capture of a worker in a namespace that shares its
// companions, with the flag on: captured alone (no companion in the
// record), the dump asked to reset the source's outside connections, and
// the identity read before the dump kept in the record.
func TestCaptureGroup_IdentityRecorded(t *testing.T) {
	saved, savedCk, savedDel := groupWarmupWait, checkpointMember, deleteMember
	groupWarmupWait = 0
	t.Cleanup(func() { groupWarmupWait, checkpointMember, deleteMember = saved, savedCk, savedDel })
	ctx := context.Background()
	f := newFakeEtcd(t)
	f.grant(idLease, 30)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	f.grant(idLease+1, 30)
	f.dynamoWorkerKeys(idLease+1, "10.0.0.9") // the other worker
	useFakeEtcd(t, f)
	leader := idWorker("w-a", idOldIP)
	leader.Status.PodIPs = []corev1.PodIP{{IP: idOldIP}, {IP: "fd00::8"}} // dual-stack: both kept
	other := idWorker("w-b", "10.0.0.9")
	other.OwnerReferences[0].UID = "rs2"
	agentPod := ready(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "nvsnap-agent-x", Namespace: "nvsnap-system",
		Labels: map[string]string{"app": "nvsnap-agent"}}, Spec: corev1.PodSpec{NodeName: "n0"}})
	kc := fake.NewSimpleClientset(leader, other, agentPod)
	a := idAgent(kc, true)
	a.seedDrivers()
	var reqs []CheckpointRequest
	checkpointMember = func(_ *Agent, _ context.Context, req CheckpointRequest) (*CheckpointResult, error) {
		reqs = append(reqs, req)
		return &CheckpointResult{CheckpointID: "ck-a", PeersReset: len(req.ResetPeersOutside) > 0, IdentityChecked: req.IdentityCheck != nil}, nil
	}
	deleteMember = func(*Agent, context.Context, string, string, types.UID) error { return nil }
	before := decisions(t, identityCaptured)
	if retry := a.criuRestorer().captureGroup(ctx, leader, "", 1); retry {
		t.Fatal("retried")
	}
	if len(reqs) != 1 || reqs[0].PodName != "w-a" || strings.Join(reqs[0].ResetPeersOutside, ",") != idOldIP+",fd00::8" {
		t.Fatalf("checkpoint requests %+v; want the worker alone, its outside connections reset", reqs)
	}
	if c := reqs[0].IdentityCheck; c == nil || c.Lease != idLease || len(c.Keys) != len(keys) {
		t.Fatalf("identity check %+v; want the identity read before the freeze", c)
	}
	cms, _ := kc.CoreV1().ConfigMaps("nvsnap-system").List(ctx, metav1.ListOptions{})
	var rec map[string]string
	for _, cm := range cms.Items {
		if cm.Data["checkpoint.0"] == "ck-a" {
			rec = cm.Data
		}
	}
	if rec == nil {
		t.Fatal("no record")
	}
	if rec["state"] != criuGroupStateVerifying || rec["companion.1"] != "" || rec["checkpoint.1"] != "" {
		t.Fatalf("record %v; want the worker alone, being verified", rec)
	}
	id, err := identityFromRecord(rec)
	if err != nil || id == nil || id.Lease != idLease || id.TTL != 30 || id.PodIP != idOldIP || len(id.Keys) != len(keys) || id.Endpoints != idEtcdEP {
		t.Fatalf("recorded identity %+v (%v)", id, err)
	}
	if decisions(t, identityCaptured) != before+1 {
		t.Error("capture with identity not counted")
	}
}

// idGroupPlaceholders are the two running ranks of a group restore of
// record idKey, in a namespace with another Dynamo worker.
func idGroupPlaceholders() (r0, r1, other *corev1.Pod) {
	mk := func(name, ip, ord string) *corev1.Pod {
		p := idPlaceholder(name, ip)
		p.OwnerReferences[0].Kind, p.OwnerReferences[0].Name, p.OwnerReferences[0].UID = "StatefulSet", "w", "sts"
		p.Annotations[webhook.CRIURestoreAnnotation] = "ck" + ord
		p.Annotations[webhook.CRIUGroupOrdinalAnnotation] = ord
		p.Annotations[webhook.CRIUGroupSizeAnnotation] = "2"
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
		return p
	}
	other = idWorker("w-other", "10.0.0.9")
	other.OwnerReferences[0].UID = "rs2"
	return mk("w-0", "10.0.1.1", "0"), mk("w-1", idNewIP, "1"), other
}

// The group restore path: the identity of the rank that holds the lease
// (slot 1 here) is granted again, with that rank's new address, before the
// instance is restored; a live lease ends the restore as not run.
func TestGroupRestore_Identity(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	for _, live := range []bool{false, true} {
		t.Run(fmt.Sprintf("lease live %v", live), func(t *testing.T) {
			saved := restoreInstance
			t.Cleanup(func() { restoreInstance = saved })
			f := newFakeEtcd(t)
			keys := f.dynamoWorkerKeys(idLease, idOldIP)
			rec := idRecord(t, keys, f)
			rec.Data["size"], rec.Data[identityMemberKey] = "2", "1"
			rec.Data["checkpoint.0"], rec.Data["checkpoint.1"], rec.Data["node.1"] = "ck0", "ck1", "n0"
			f.mu.Lock()
			f.kvs = map[string]etcdKV{}
			f.mu.Unlock()
			stop := make(chan struct{})
			defer close(stop)
			if live {
				f.grant(idLease, 2)
				go func() {
					for {
						select {
						case <-stop:
							return
						case <-time.After(300 * time.Millisecond):
							f.refresh(idLease)
						}
					}
				}()
			}
			useFakeEtcd(t, f)
			r0, r1, other := idGroupPlaceholders()
			kc := fake.NewSimpleClientset(rec, r0, r1, other)
			c := idAgent(kc, true).criuRestorer()
			for _, p := range []*corev1.Pod{r0, r1} {
				c.verdicts.Store(string(p.UID)+"\x00"+p.Annotations[webhook.CRIURestoreAnnotation]+"\x00"+idKey, "")
			}
			called, grantedFirst := false, false
			restoreInstance = func(*Agent, context.Context, GroupRestoreRequest, *logrus.Entry) (*GroupRestoreResult, error) {
				called = true
				grantedFirst = f.live(idLease)
				if _, early := f.value(keys[0]); early {
					t.Error("registered before the instance was restored")
				}
				return &GroupRestoreResult{}, nil
			}
			refusals := decisions(t, identityRefusedLiveLease)
			c.restoreGroup(ctx, r0, idKey, 2)
			if live && decisions(t, identityRefusedLiveLease) != refusals+1 {
				t.Error("the refusal was not handled as one (counted, ranks started cold)")
			}
			cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
			if live {
				// Not run: nothing restored, no checkpoint blocked, and the
				// placeholders deleted for their replacements.
				if called || c.Blocked(ctx, "ck0") {
					t.Fatalf("restored %v, blocked %v; want not run", called, c.Blocked(ctx, "ck0"))
				}
				if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-1", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
					t.Errorf("placeholder kept: %v", err)
				}
				if cm.Data[identityHolderKey] != "" {
					t.Errorf("a refused restore kept the claim %q", cm.Data[identityHolderKey])
				}
				if cm.Data["stalls"] != "1" {
					t.Errorf("a refusal whose pods were deleted counted %q stalls; want 1, so the cycle is bounded", cm.Data["stalls"])
				}
				return
			}
			if !called || !grantedFirst {
				t.Fatalf("restored %v, granted first %v", called, grantedFirst)
			}
			if cm.Data[identityHolderKey] != holderOf(r1) {
				t.Errorf("holder %q; want the lease holder's slot", cm.Data[identityHolderKey])
			}
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if _, ok := f.value(keys[len(keys)-1]); ok {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			keysName(t, f, keys, idNewIP, idOldIP)
		})
	}
}

// Admission of a rank of a multi-pod instance captured alone with its
// identity: the pod carries no record annotations yet, so the record
// decides, and the namespace's other worker does not refuse it.
func TestCRIUGroupAdmits_IdentityRecord(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	for _, tc := range []struct {
		name     string
		on       bool
		identity bool
		want     bool
	}{
		{"identity record, flag on", true, true, true},
		{"identity record, flag off", false, true, false},
		{"record without identity", true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := idRecord(t, keys, f)
			if !tc.identity {
				rec.Data = map[string]string{"state": criuGroupStateComplete, "size": "2", "checkpoint.0": "ck0", "checkpoint.1": "ck1"}
			}
			pod := idWorker("w-0", "10.0.1.1") // as admission sees it: no restore annotations
			pod.OwnerReferences[0].Kind = "ReplicaSet"
			other := idWorker("w-other", "10.0.0.9")
			other.OwnerReferences[0].UID = "rs2"
			kc := fake.NewSimpleClientset(rec, other)
			ok, why := idAgent(kc, tc.on).criuGroupAdmits(ctx, pod, webhook.CRIUGroup{Key: idKey})
			if ok != tc.want {
				t.Fatalf("admitted %v (%s), want %v", ok, why, tc.want)
			}
		})
	}
}

// The check again before the hold: a capture that took its companions is
// skipped once another worker shares them, though that worker could be
// captured alone; a capture that is alone goes on.
func TestSharedRecheck(t *testing.T) {
	ctx := context.Background()
	leader := idWorker("w-a", idOldIP)
	other := idWorker("w-b", "10.0.0.9")
	other.OwnerReferences[0].UID = "rs2"
	c := idAgent(fake.NewSimpleClientset(leader, other), true).criuRestorer()
	if _, skip := c.sharedRecheck(ctx, leader, false, idLog()); !skip {
		t.Error("a capture with companions went on after another worker came to share them")
	}
	if _, skip := c.sharedRecheck(ctx, leader, true, idLog()); skip {
		t.Error("a capture alone was skipped")
	}
	c = idAgent(fake.NewSimpleClientset(leader), true).criuRestorer()
	if _, skip := c.sharedRecheck(ctx, leader, false, idLog()); skip {
		t.Error("a capture in an unshared namespace was skipped")
	}
}

// An adoption needs the grant on record: a claim alone (an agent that
// stopped before it had the lease) does not skip the wait for the source's
// lease or the grant.
func TestLoneRestore_ClaimAloneIsNotAdopted(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	pod := idPlaceholder("w-new", idNewIP)
	rec.Data[identityHolderKey] = holderOf(pod) // claimed, never granted
	f.grant(idLease, 2)
	stop := make(chan struct{})
	defer close(stop)
	go func() { // the source's lease, still renewed by someone
		for {
			select {
			case <-stop:
				return
			case <-time.After(300 * time.Millisecond):
				f.refresh(idLease)
			}
		}
	}()
	useFakeEtcd(t, f)
	kc := fake.NewSimpleClientset(rec, pod)
	c := idAgent(kc, true).criuRestorer()
	saved := restoreForRequest
	t.Cleanup(func() { restoreForRequest = saved })
	restored := false
	restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
		restored = true
		return &RestoreResult{}, nil
	}
	c.restore(ctx, "shared", "w-new", pod.UID, "ck", "main", idKey)
	if restored {
		t.Fatal("a claim alone adopted a live lease it never had granted")
	}
}

// A key another lease holds now is not taken over by the restore.
func TestWriteIdentityKeys_KeyOfAnotherLease(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	id, err := identityFromRecord(idRecord(t, keys, f).Data)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.kvs = map[string]etcdKV{}
	f.mu.Unlock()
	f.grant(idLease, 30)
	f.grant(99, 30)
	f.put(keys[3], "another worker's", 99)
	if err := writeIdentityKeys(ctx, f.client(), id, idNewIP); err == nil {
		t.Fatal("a key of another lease was overwritten")
	}
	if v, _ := f.value(keys[3]); v != "another worker's" {
		t.Fatalf("the other worker's key changed: %q", v)
	}
	// Absent keys are written; a key already under the lease is the
	// process's own, maybe newer (an adoption after a restart): kept.
	f.put(keys[3], "newer, the process's", idLease)
	if err := writeIdentityKeys(ctx, f.client(), id, idNewIP); err != nil {
		t.Fatal(err)
	}
	if v, _ := f.value(keys[3]); v != "newer, the process's" {
		t.Fatalf("a key under the lease was reverted: %q", v)
	}
	f.put(keys[3], "", 0)
	f.mu.Lock()
	delete(f.kvs, keys[3])
	f.mu.Unlock()
	if err := writeIdentityKeys(ctx, f.client(), id, idNewIP); err != nil {
		t.Fatal(err)
	}
	keysName(t, f, keys, idNewIP, idOldIP)
}

// A registration that changes between the reads is not saved.
func TestReadIdentity_ChangedWhileRead(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	f.grant(idLease, 30)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	f.onTimeToLive = func() {
		f.kvs[keys[3]] = etcdKV{Key: []byte(keys[3]), Value: []byte(`{"path":"http://` + idOldIP + `:9091/new"}`), Lease: idLease}
	}
	if _, err := readIdentity(ctx, f.client(), []string{idOldIP}, idEtcdEP); err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("a changing registration was saved: %v", err)
	}
}

// A worker that renews a short lease often enough that every poll reads
// the same whole-second TTL is still seen keeping it (no rise between
// polls is needed).
func TestLeaseKeeper_FlatSamples(t *testing.T) {
	fastIdentity(t)
	f := newFakeEtcd(t)
	f.grant(idLease, 2)
	k := newLeaseKeeper(f.client(), idLease, 2, idLog())
	zombie := false
	k.onZombie = func() { zombie = true }
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
				f.refresh(idLease) // the fake reads 1 s nearly always
			}
		}
	}()
	go k.run(context.Background())
	k.restored(nil)
	select {
	case <-k.done:
	case <-time.After(8 * time.Second):
		t.Fatal("keeper did not end")
	}
	if k.outcome != identityHandoffSeen || zombie {
		t.Fatalf("outcome %s, zombie %v", k.outcome, zombie)
	}
}

// A stale attempt for a pod since replaced under the same name touches
// neither the replacement nor the identity.
func TestLoneRestore_ReplacedPodLeftAlone(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	opened, _ := useFakeEtcd(t, f)
	pod := idPlaceholder("w-new", idNewIP) // the replacement, UID w-new-uid
	kc := fake.NewSimpleClientset(rec, pod)
	c := idAgent(kc, true).criuRestorer()
	saved := restoreForRequest
	t.Cleanup(func() { restoreForRequest = saved })
	restored := false
	restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
		restored = true
		return nil, nil
	}
	c.restore(ctx, "shared", "w-new", "stale-uid", "ck", "main", idKey)
	if restored || opened.Load() != 0 {
		t.Fatalf("restored %v, registry opened %d", restored, opened.Load())
	}
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); err != nil {
		t.Fatalf("the replacement was deleted: %v", err)
	}
	if cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{}); cm.Data[identityHolderKey] != "" {
		t.Errorf("claimed for a stale attempt: %q", cm.Data[identityHolderKey])
	}
}

// Every address of a dual-stack member is kept.
func TestMemberAddresses_DualStack(t *testing.T) {
	p := idWorker("w", "10.0.0.1")
	p.Status.PodIPs = []corev1.PodIP{{IP: "10.0.0.1"}, {IP: "fd00::1"}}
	if got := strings.Join(memberAddresses([]*corev1.Pod{p}), ","); got != "10.0.0.1,fd00::1" {
		t.Fatalf("addresses %s", got)
	}
}

// The second read covers the lease's whole key set: a worker whose
// instance registrations went away, or that gained a key, is not saved.
func TestReadIdentity_KeySetChangedWhileRead(t *testing.T) {
	ctx := context.Background()
	t.Run("instance registrations removed", func(t *testing.T) {
		f := newFakeEtcd(t)
		f.grant(idLease, 30)
		keys := f.dynamoWorkerKeys(idLease, idOldIP)
		calls := 0
		f.onTimeToLive = func() {
			calls++
			if calls == 1 {
				for _, k := range keys[:3] {
					delete(f.kvs, k)
				}
			}
		}
		if _, err := readIdentity(ctx, f.client(), []string{idOldIP}, idEtcdEP); err == nil {
			t.Fatal("an identity without its instance registration was saved")
		}
	})
	t.Run("key added under the lease", func(t *testing.T) {
		f := newFakeEtcd(t)
		f.grant(idLease, 30)
		f.dynamoWorkerKeys(idLease, idOldIP)
		calls := 0
		f.onTimeToLive = func() {
			calls++
			if calls == 2 {
				f.kvs["other/late"] = etcdKV{Key: []byte("other/late"), Value: []byte("x"), Lease: idLease}
			}
		}
		if _, err := readIdentity(ctx, f.client(), []string{idOldIP}, idEtcdEP); err == nil {
			t.Fatal("a key added while it was read was missed")
		}
	})
	t.Run("key removed from the lease", func(t *testing.T) {
		f := newFakeEtcd(t)
		f.grant(idLease, 30)
		keys := f.dynamoWorkerKeys(idLease, idOldIP)
		calls := 0
		f.onTimeToLive = func() {
			calls++
			if calls == 2 {
				delete(f.kvs, keys[len(keys)-1])
			}
		}
		if _, err := readIdentity(ctx, f.client(), []string{idOldIP}, idEtcdEP); err == nil {
			t.Fatal("a key removed while it was read was saved")
		}
	})
	t.Run("key outside the Dynamo root changed", func(t *testing.T) {
		f := newFakeEtcd(t)
		f.grant(idLease, 30)
		f.dynamoWorkerKeys(idLease, idOldIP)
		f.put("other/x", "1", idLease)
		calls := 0
		f.onTimeToLive = func() {
			calls++
			if calls == 2 {
				f.kvs["other/x"] = etcdKV{Key: []byte("other/x"), Value: []byte("2"), Lease: idLease}
			}
		}
		if _, err := readIdentity(ctx, f.client(), []string{idOldIP}, idEtcdEP); err == nil {
			t.Fatal("a changed key outside v1/ was saved")
		}
	})
}

// A pod that would be deleted for a refusal is deleted only once the
// stall is on record, so its replacement, admitted at once, sees it; when
// the stall cannot be recorded the pod is kept and the refusal is handed
// back to the next event.
func TestIdentityRefused_StallBeforeDelete(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	refusal := refuseIdentity(identityRefusedLiveLease, errors.New("live"))
	t.Run("stall first", func(t *testing.T) {
		rec := idRecord(t, keys, f)
		pod := idPlaceholder("w-new", idNewIP)
		kc := fake.NewSimpleClientset(rec, pod)
		stallAtDelete := ""
		kc.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			cm, _ := kc.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), "nvsnap-system", rec.Name)
			stallAtDelete = cm.(*corev1.ConfigMap).Data["stalls"]
			return false, nil, nil
		})
		again := false
		idAgent(kc, true).criuRestorer().identityRefused(ctx, idKey, "ck", []*corev1.Pod{pod}, refusal, func() { again = true }, idLog())
		if stallAtDelete != "1" || again {
			t.Fatalf("stall at delete %q, again %v", stallAtDelete, again)
		}
	})
	t.Run("stall cannot be recorded", func(t *testing.T) {
		rec := idRecord(t, keys, f)
		pod := idPlaceholder("w-new", idNewIP)
		kc := fake.NewSimpleClientset(rec, pod)
		kc.PrependReactor("update", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("api down")
		})
		again := false
		idAgent(kc, true).criuRestorer().identityRefused(ctx, idKey, "ck", []*corev1.Pod{pod}, refusal, func() { again = true }, idLog())
		if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); err != nil || !again {
			t.Fatalf("pod kept: %v, again %v", err, again)
		}
	})
	t.Run("a rank that cannot be read", func(t *testing.T) {
		setFinishRetry(t)
		rec := idRecord(t, keys, f)
		r0, r1, _ := idGroupPlaceholders()
		kc := fake.NewSimpleClientset(rec, r0, r1)
		var fail atomic.Bool
		fail.Store(true)
		kc.PrependReactor("get", "pods", func(act k8stesting.Action) (bool, runtime.Object, error) {
			if fail.Load() && act.(k8stesting.GetAction).GetName() == r1.Name {
				return true, nil, errors.New("api down")
			}
			return false, nil, nil
		})
		again := false
		c := idAgent(kc, true).criuRestorer()
		c.identityRefused(ctx, idKey, "ck", []*corev1.Pod{r0, r1}, refusal, func() { again = true }, idLog())
		// r0 was deleted: the restore cannot run again, so this agent
		// finishes the unreadable rank's refusal itself.
		if _, err := kc.CoreV1().Pods("shared").Get(ctx, r0.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("r0 kept: %v", err)
		}
		if again {
			t.Error("handed back to a restore that cannot run again")
		}
		fail.Store(false)
		if !idPodGone(t, kc, r1.Name, 5*time.Second) {
			t.Fatal("an unreadable rank was left without its refusal being finished")
		}
	})
}

// setFinishRetry shortens finishRefused's pacing for a test.
func setFinishRetry(t *testing.T) {
	t.Helper()
	identityFinishRetryMu.Lock()
	saved := identityFinishRetry
	identityFinishRetry.first, identityFinishRetry.max, identityFinishRetry.total = 10*time.Millisecond, 50*time.Millisecond, 10*time.Second
	identityFinishRetryMu.Unlock()
	t.Cleanup(func() {
		identityFinishRetryMu.Lock()
		identityFinishRetry = saved
		identityFinishRetryMu.Unlock()
	})
}

// idPodGone waits, at most within, for pod shared/name to be gone.
func idPodGone(t *testing.T, kc *fake.Clientset, name string, within time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if _, err := kc.CoreV1().Pods("shared").Get(context.Background(), name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Through the group path: when the leader is dealt with (deleted here,
// started cold alike) and a rank on another node cannot be read, the
// leader's next event can never restore again, so the leader's agent
// finishes the rank's refusal itself, with no event of the rank (its own
// agent knows nothing of the refusal), counting no second stall.
func TestGroupRestore_RefusalRetriedOnTheRank(t *testing.T) {
	fastIdentity(t)
	setFinishRetry(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	rec.Data["size"], rec.Data[identityMemberKey] = "2", "1"
	rec.Data["checkpoint.0"], rec.Data["checkpoint.1"], rec.Data["node.1"] = "ck0", "ck1", "n0"
	f.grant(idLease, 2)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(300 * time.Millisecond):
				f.refresh(idLease)
			}
		}
	}()
	useFakeEtcd(t, f)
	r0, r1, other := idGroupPlaceholders()
	r1.Spec.NodeName = "n1"
	kc := fake.NewSimpleClientset(rec, r0, r1, other)
	c := idAgent(kc, true).criuRestorer()
	for _, p := range []*corev1.Pod{r0, r1} {
		c.verdicts.Store(string(p.UID)+"\x00"+p.Annotations[webhook.CRIURestoreAnnotation]+"\x00"+idKey, "")
	}
	fail := atomic.Bool{}
	kc.PrependReactor("get", "pods", func(act k8stesting.Action) (bool, runtime.Object, error) {
		if fail.Load() && act.(k8stesting.GetAction).GetName() == r1.Name {
			return true, nil, errors.New("api down")
		}
		return false, nil, nil
	})
	c.attempted.Store("restore/"+string(r0.UID), struct{}{})
	saved := restoreInstance
	t.Cleanup(func() { restoreInstance = saved })
	restoreInstance = func(*Agent, context.Context, GroupRestoreRequest, *logrus.Entry) (*GroupRestoreResult, error) {
		return &GroupRestoreResult{}, nil
	}
	setDeleteRetry(t, time.Millisecond, 5*time.Millisecond, 50*time.Millisecond)
	fail.Store(true)
	c.restoreGroup(ctx, r0, idKey, 2)
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, r0.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the leader was kept: %v", err)
	}
	fail.Store(false)
	if !idPodGone(t, kc, r1.Name, 5*time.Second) {
		t.Fatal("the rank was left idle")
	}
	if cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{}); cm.Data["stalls"] != "1" {
		t.Errorf("stalls %q; want 1 for one refusal", cm.Data["stalls"])
	}
}

// Adoption fails closed when the adopted lease cannot be read.
func TestLoneRestore_AdoptUnreadableLeaseRefused(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	pod := idPlaceholder("w-new", idNewIP)
	rec.Data[identityHolderKey], rec.Data[identityGrantedKey] = holderOf(pod), holderOf(pod)
	f.setFail("/v3/lease/timetolive", true)
	useFakeEtcd(t, f)
	kc := fake.NewSimpleClientset(rec, pod)
	c := idAgent(kc, true).criuRestorer()
	k, _, err := c.loneIdentityBeforeRestore(ctx, ctx, idKey, "shared", "w-new", pod.UID, "ck", idLog())
	if k != nil || identityDecision(err) != identityRefusedError {
		t.Fatalf("adopted an unreadable lease: keeper %v, %v", k, err)
	}
}

// An agent that stops while it registers the restored worker leaves the
// identity as it is: no revoke, no pod ended, for the next agent to adopt.
func TestLeaseKeeper_StopDuringKeyWrite(t *testing.T) {
	fastIdentity(t)
	f := newFakeEtcd(t)
	f.grant(idLease, 5)
	ctx, cancel := context.WithCancel(context.Background())
	k := newLeaseKeeper(f.client(), idLease, 5, idLog())
	zombie, aborted := false, false
	k.onZombie, k.onAborted = func() { zombie = true }, func() { aborted = true }
	writing := make(chan struct{})
	k.writeKeys = func(ctx context.Context) error {
		close(writing)
		<-ctx.Done()
		return ctx.Err()
	}
	defer cancel()
	go k.run(ctx)
	k.restored(nil)
	select {
	case <-writing:
	case <-k.done:
		t.Fatalf("keeper ended (%s) before it registered the worker", k.outcome)
	case <-time.After(3 * time.Second):
		t.Fatal("keeper never registered the worker")
	}
	cancel()
	select {
	case <-k.done:
	case <-time.After(3 * time.Second):
		t.Fatal("keeper did not end")
	}
	if k.outcome != identityKeeperAgentStopped || zombie || aborted || !f.live(idLease) {
		t.Fatalf("outcome %s zombie %v aborted %v live %v", k.outcome, zombie, aborted, f.live(idLease))
	}
}

// A keep-alive sample whose reply comes late tells of the lease as of its
// request: a lease renewed once by someone else and then left to expire is
// not handed off on a reply that arrives after it expired.
func TestLeaseKeeper_LateSampleIsNotAHandoff(t *testing.T) {
	fastIdentity(t)
	f := newFakeEtcd(t)
	f.grant(idLease, 4)
	t0 := time.Now()
	delayed := false
	f.onTimeToLiveReply = func() { // under the fake's lock
		if !delayed && time.Since(t0) >= 3800*time.Millisecond {
			delayed = true // asked at 3.8 s (TTL 2), answered at 6.4 s
			time.Sleep(time.Until(t0.Add(6400 * time.Millisecond)))
		}
	}
	k := newLeaseKeeper(f.client(), idLease, 4, idLog())
	var zombie atomic.Bool
	k.onZombie = func() { zombie.Store(true) }
	go k.run(context.Background())
	k.restored(nil)
	time.Sleep(time.Until(t0.Add(2 * time.Second)))
	f.refresh(idLease) // one renewal not the agent's: the lease now expires at 6 s
	select {
	case <-k.done:
	case <-time.After(12 * time.Second):
		t.Fatal("keeper did not end")
	}
	if k.outcome != identityHandoffLost || !zombie.Load() {
		t.Fatalf("outcome %s, zombie %v; want the lost lease to end the pod", k.outcome, zombie.Load())
	}
}

// A placeholder replaced under its name while its identity is set up is
// not restored into: the restore finds its target by name, and the
// identity was set up for the replaced pod. The regranted lease is revoked
// and the claim released; the replacement is left for its own restore.
func TestLoneRestore_PlaceholderReplacedDuringIdentitySetup(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	f.mu.Lock()
	f.kvs = map[string]etcdKV{}
	f.mu.Unlock()
	useFakeEtcd(t, f)
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(rec, pod)
	c := idAgent(kc, true).criuRestorer()
	replaced := false
	f.onTimeToLive = func() { // the wait for the captured lease
		if replaced {
			return
		}
		replaced = true
		gvr := corev1.SchemeGroupVersion.WithResource("pods")
		if err := kc.Tracker().Delete(gvr, "shared", "w-new"); err != nil {
			t.Error(err)
		}
		r := pod.DeepCopy()
		r.UID = "w-new-replacement"
		if err := kc.Tracker().Create(gvr, r, "shared"); err != nil {
			t.Error(err)
		}
	}
	saved := restoreForRequest
	t.Cleanup(func() { restoreForRequest = saved })
	restored := false
	restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
		restored = true
		return &RestoreResult{}, nil
	}
	c.restore(ctx, "shared", "w-new", pod.UID, "ck", "main", idKey)
	if !replaced {
		t.Fatal("the pod was not replaced during the identity setup")
	}
	if restored {
		t.Fatal("restored into the replacement, whose identity was not set up")
	}
	got, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{})
	if err != nil || got.UID != "w-new-replacement" || coldStarted(got) {
		t.Fatalf("the replacement was touched: %v %+v", err, got)
	}
	// The lease is revoked, then the claim released: wait for both.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
		if !f.live(idLease) && cm.Data[identityHolderKey] == "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
	if f.live(idLease) || cm.Data[identityHolderKey] != "" {
		t.Fatalf("lease live %v, holder %q; want it revoked and released", f.live(idLease), cm.Data[identityHolderKey])
	}
}

// A refusal leaves a pod that no longer names the refused checkpoint as
// it is, even under its UID: it is not this restore's.
func TestLoneRestore_CheckpointChangedLeftAlone(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	useFakeEtcd(t, f)
	pod := idPlaceholder("w-new", idNewIP)
	pod.Annotations[webhook.CRIURestoreAnnotation] = "ck2"
	kc := fake.NewSimpleClientset(rec, pod)
	c := idAgent(kc, true).criuRestorer()
	saved := restoreForRequest
	t.Cleanup(func() { restoreForRequest = saved })
	restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
		t.Error("restored a pod that names another checkpoint")
		return &RestoreResult{}, nil
	}
	c.restore(ctx, "shared", "w-new", pod.UID, "ck", "main", idKey)
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); err != nil {
		t.Fatalf("the pod was deleted: %v", err)
	}
	if cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{}); cm.Data["stalls"] != "" {
		t.Errorf("stalls %q; want none", cm.Data["stalls"])
	}
}

// A refused pod that already runs its workload (a restored engine an
// earlier agent left) is kept: neither started cold nor deleted, nothing
// counted; a lone restore is handed back to its next event, which may
// adopt the identity once the lease can be read.
func TestIdentityRefused_RunningWorkloadKept(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	saved := podRunsWorkload
	t.Cleanup(func() { podRunsWorkload = saved })
	podRunsWorkload = func(context.Context, *Agent, *corev1.Pod, string) (bool, bool, error) { return true, true, nil }
	rec := idRecord(t, keys, f)
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(rec, pod)
	again := false
	refusal := refuseIdentity(identityRefusedError, errors.New("read the adopted lease: unreachable"))
	idAgent(kc, true).criuRestorer().identityRefused(ctx, idKey, "ck", []*corev1.Pod{pod}, refusal, func() { again = true }, idLog())
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); err != nil {
		t.Fatalf("a running worker was deleted: %v", err)
	}
	if cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{}); cm.Data["stalls"] != "" {
		t.Errorf("stalls %q; want none", cm.Data["stalls"])
	}
	if !again {
		t.Error("the restore was not handed back to its next event")
	}
	// When it cannot be told, a pod that cannot start cold is kept, and
	// the refusal tried again: it is never deleted unseen.
	podRunsWorkload = func(context.Context, *Agent, *corev1.Pod, string) (bool, bool, error) {
		return false, true, errors.New("nvml down")
	}
	again = false
	idAgent(kc, true).criuRestorer().identityRefused(ctx, idKey, "ck", []*corev1.Pod{pod}, refusal, func() { again = true }, idLog())
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); err != nil || !again {
		t.Fatalf("a pod that may run a workload: kept %v, tried again %v", err, again)
	}
	// One rank that runs a workload keeps the whole instance: the others
	// (a rank on another node cannot be inspected) are not deleted.
	podRunsWorkload = func(_ context.Context, _ *Agent, p *corev1.Pod, _ string) (bool, bool, error) {
		return p.Name == "w-0", p.Name == "w-0", nil
	}
	r0, r1, _ := idGroupPlaceholders()
	gkc := fake.NewSimpleClientset(idRecord(t, keys, f), r0, r1)
	idAgent(gkc, true).criuRestorer().identityRefused(ctx, idKey, "ck", []*corev1.Pod{r0, r1}, refusal, nil, idLog())
	for _, p := range []*corev1.Pod{r0, r1} {
		if _, err := gkc.CoreV1().Pods("shared").Get(ctx, p.Name, metav1.GetOptions{}); err != nil {
			t.Fatalf("a rank of a running instance was deleted: %v", err)
		}
	}
	// When the leader cannot be inspected, a rank on another node (which
	// cannot be either) is not deleted.
	podRunsWorkload = func(_ context.Context, _ *Agent, p *corev1.Pod, _ string) (bool, bool, error) {
		if p.Name == "w-0" {
			return false, true, errors.New("nvml down")
		}
		return false, false, nil
	}
	r0, r1, _ = idGroupPlaceholders()
	ukc := fake.NewSimpleClientset(idRecord(t, keys, f), r0, r1)
	setFinishRetry(t)
	idAgent(ukc, true).criuRestorer().identityRefused(ctx, idKey, "ck", []*corev1.Pod{r0, r1}, refusal, func() {}, idLog())
	for _, p := range []*corev1.Pod{r0, r1} {
		if _, err := ukc.CoreV1().Pods("shared").Get(ctx, p.Name, metav1.GetOptions{}); err != nil {
			t.Fatalf("a rank that could not be inspected was deleted: %v", err)
		}
	}
	podRunsWorkload = func(context.Context, *Agent, *corev1.Pod, string) (bool, bool, error) { return true, true, nil }
	// A refusal that will not pass (identity restore off) keeps the pod and
	// is not tried again.
	again = false
	disabled := refuseIdentity(identityRefusedDisabled, errors.New("off"))
	idAgent(kc, true).criuRestorer().identityRefused(ctx, idKey, "ck", []*corev1.Pod{pod}, disabled, func() { again = true }, idLog())
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); err != nil || again {
		t.Fatalf("kept: %v, tried again %v", err, again)
	}
}

// A refusal whose delete failed, tried again, is still one refusal: it
// counts one stall, not one per try.
func TestIdentityRefused_StallOncePerPod(t *testing.T) {
	ctx := context.Background()
	setDeleteRetry(t, time.Millisecond, 5*time.Millisecond, 20*time.Millisecond)
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(rec, pod)
	kc.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("api down")
	})
	c := idAgent(kc, true).criuRestorer()
	refusal := refuseIdentity(identityRefusedLiveLease, errors.New("live"))
	for i := range 3 {
		again := false
		c.identityRefused(ctx, idKey, "ck", []*corev1.Pod{pod}, refusal, func() { again = true }, idLog())
		if !again {
			t.Fatalf("try %d: a refusal whose delete failed was not tried again", i)
		}
	}
	if cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{}); cm.Data["stalls"] != "1" {
		t.Fatalf("stalls %q after three tries of one refusal; want 1", cm.Data["stalls"])
	}
	// Two instances restoring from one record, refused in turn, each
	// retried after its deletes failed: one stall each.
	r0, r1, _ := idGroupPlaceholders()
	s0, s1 := r0.DeepCopy(), r1.DeepCopy()
	s0.Name, s0.UID, s1.Name, s1.UID = "v-0", "v-0-uid", "v-1", "v-1-uid"
	two := fake.NewSimpleClientset(idRecord(t, keys, f), r0, r1, s0, s1)
	two.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("api down")
	})
	tc := idAgent(two, true).criuRestorer()
	for range 3 {
		for _, inst := range [][]*corev1.Pod{{r0, r1}, {s0, s1}} {
			tc.identityRefused(ctx, idKey, "ck", inst, refusal, func() {}, idLog())
		}
	}
	if cm, _ := two.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{}); cm.Data["stalls"] != "2" {
		t.Fatalf("stalls %q after three tries of two instances' refusals; want 2", cm.Data["stalls"])
	}
}

// The keeper does not count a lease ahead from before etcd read it: a
// sample asked early but read late, after a renewal not its own, starts
// the wait for the handoff at its reply. Two renewals not the process's
// own are not a handoff.
func TestLeaseKeeper_EarlyRequestReadLateIsNotAHandoff(t *testing.T) {
	fastIdentity(t)
	f := newFakeEtcd(t)
	f.grant(idLease, 8)
	t0 := time.Now()
	first, last := false, false
	f.onTimeToLive = func() { // under the fake's lock, before the TTL is read
		if !first && time.Since(t0) >= 3100*time.Millisecond {
			first = true // asked at 3.1 s, read at 7.3 s, after a renewal at 7.25 s
			time.Sleep(time.Until(t0.Add(7250 * time.Millisecond)))
			f.leases[idLease].expires = time.Now().Add(8 * time.Second)
			time.Sleep(time.Until(t0.Add(7300 * time.Millisecond)))
		}
	}
	f.onTimeToLiveReply = func() {
		if first && !last && time.Since(t0) >= 11150*time.Millisecond {
			last = true // asked at 11.15 s (TTL 4), answered at 11.5 s
			time.Sleep(time.Until(t0.Add(11500 * time.Millisecond)))
		}
	}
	k := newLeaseKeeper(f.client(), idLease, 8, idLog())
	k.onZombie = func() {}
	go k.run(context.Background())
	k.restored(nil)
	select {
	case <-k.done:
		t.Fatalf("ended %s; no process keeps the lease, so no handoff", k.outcome)
	case <-time.After(time.Until(t0.Add(14 * time.Second))):
	}
}

// A late reply may tell of a lease that expired since: the keeper renews
// it once more before it hands it off, and finds it lost.
func TestLeaseKeeper_LateReplyAfterExpiryIsNotAHandoff(t *testing.T) {
	fastIdentity(t)
	f := newFakeEtcd(t)
	f.grant(idLease, 8)
	t0 := time.Now()
	second, third := false, false
	f.onTimeToLiveReply = func() { // under the fake's lock
		switch since := time.Since(t0); {
		case !second && since >= 7600*time.Millisecond:
			second = true // asked at 7.6 s (TTL 4), answered at 11.6 s
			time.Sleep(time.Until(t0.Add(11600 * time.Millisecond)))
		case second && !third && since >= 11600*time.Millisecond:
			third = true // asked at 11.6 s (TTL 0), answered after the expiry at 11.7 s
			time.Sleep(time.Until(t0.Add(11900 * time.Millisecond)))
		}
	}
	k := newLeaseKeeper(f.client(), idLease, 8, idLog())
	var zombie atomic.Bool
	k.onZombie = func() { zombie.Store(true) }
	go k.run(context.Background())
	k.restored(nil)
	time.Sleep(time.Until(t0.Add(3500 * time.Millisecond)))
	f.refresh(idLease)
	time.Sleep(200 * time.Millisecond)
	f.refresh(idLease) // two renewals not the agent's: expires at 11.7 s
	select {
	case <-k.done:
	case <-time.After(15 * time.Second):
		t.Fatal("keeper did not end")
	}
	if k.outcome != identityHandoffLost || !zombie.Load() {
		t.Fatalf("outcome %s, zombie %v; want the lost lease to end the pod", k.outcome, zombie.Load())
	}
}

// The restore takes only the placeholder container of the pod its
// identity was set up for.
func TestPlaceholderOfPod(t *testing.T) {
	info := &containerd.ContainerInfo{Labels: map[string]string{"io.kubernetes.pod.uid": "u1"}}
	if err := placeholderOfPod(info, "u1"); err != nil {
		t.Fatal(err)
	}
	if err := placeholderOfPod(info, ""); err != nil {
		t.Fatal(err)
	}
	for _, i := range []*containerd.ContainerInfo{info, {}} {
		if err := placeholderOfPod(i, "u2"); !errors.Is(err, errPlaceholderReplaced) {
			t.Fatalf("another pod's container: %v", err)
		}
	}
}

// A replacement the restore finds under the placeholder's name, after the
// identity was set up and checked, is refused: nothing restored into it,
// nothing counted, the regranted lease revoked and the claim released.
func TestLoneRestore_ReplacedAfterTheCheck(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	f.mu.Lock()
	f.kvs = map[string]etcdKV{}
	f.mu.Unlock()
	useFakeEtcd(t, f)
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(rec, pod)
	c := idAgent(kc, true).criuRestorer()
	saved := restoreForRequest
	t.Cleanup(func() { restoreForRequest = saved })
	var asked string
	restoreForRequest = func(_ *Agent, _ context.Context, req RestoreRequest) (*RestoreResult, error) {
		asked = req.PlaceholderPodUID
		gvr := corev1.SchemeGroupVersion.WithResource("pods")
		_ = kc.Tracker().Delete(gvr, "shared", "w-new")
		r := pod.DeepCopy()
		r.UID = "w-new-replacement"
		_ = kc.Tracker().Create(gvr, r, "shared")
		return nil, fmt.Errorf("%w: test", errPlaceholderReplaced)
	}
	refusals := decisions(t, identityRefusedError)
	c.restore(ctx, "shared", "w-new", pod.UID, "ck", "main", idKey)
	if asked != string(pod.UID) {
		t.Fatalf("the restore was asked for pod %q; want it bound to %q", asked, pod.UID)
	}
	if decisions(t, identityRefusedError) != refusals+1 {
		t.Error("not handled as a refusal")
	}
	got, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{})
	if err != nil || got.UID != "w-new-replacement" || coldStarted(got) {
		t.Fatalf("the replacement was touched: %v %+v", err, got)
	}
	// The lease is revoked, then the claim released: wait for both.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
		if !f.live(idLease) && cm.Data[identityHolderKey] == "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
	if f.live(idLease) || cm.Data[identityHolderKey] != "" || cm.Data["stalls"] != "" || c.Blocked(ctx, "ck") {
		t.Fatalf("lease live %v, holder %q, stalls %q, blocked %v", f.live(idLease), cm.Data[identityHolderKey], cm.Data["stalls"], c.Blocked(ctx, "ck"))
	}
}

// uidRuntime answers FindContainerByPod with a container of pod uid (no
// label when uid is empty).
type uidRuntime struct {
	nvruntime.Runtime
	uid string
}

func (r uidRuntime) FindContainerByPod(context.Context, string, string, string) (*nvruntime.ContainerInfo, error) {
	labels := map[string]string{}
	if r.uid != "" {
		labels["io.kubernetes.pod.uid"] = r.uid
	}
	return &nvruntime.ContainerInfo{ID: "0123456789abcdef", Labels: labels}, nil
}

// Restore itself, at its production call site, refuses the container of
// another pod found under the placeholder's name, before it touches the
// placeholder (the pid reservation would run first otherwise, and fail on
// this fake's pid 0).
func TestRestore_PlaceholderOfAnotherPodRefused(t *testing.T) {
	a := idAgent(fake.NewSimpleClientset(), true)
	if err := os.MkdirAll(a.config.CheckpointDir+"/ck", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(a.config.CheckpointDir+"/ck/metadata.json", []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	req := RestoreRequest{CheckpointID: "ck", PlaceholderNamespace: "shared", PlaceholderPodName: "w-new",
		PlaceholderContainerName: "main", ReservePIDs: true, PlaceholderPodUID: "w-new-uid"}
	for _, uid := range []string{"w-new-replacement", ""} {
		a.runtime = uidRuntime{uid: uid}
		if _, err := a.Restore(context.Background(), req); !errors.Is(err, errPlaceholderReplaced) {
			t.Fatalf("container of pod %q: %v; want it refused", uid, err)
		}
	}
	a.runtime = uidRuntime{uid: "w-new-uid"}
	if _, err := a.Restore(context.Background(), req); errors.Is(err, errPlaceholderReplaced) {
		t.Fatalf("the pod's own container refused: %v", err)
	}
}

// An agent that restarts after a restore succeeded, but before the
// restored process was seen keeping its lease (its keys perhaps never
// written), adopts the restored pod's identity on the pod's event: no
// restore is tried in a pod marked restored, so the restore's own adoption
// never runs. It registers the keys and hands the lease off; with the
// lease lost, it ends the pod and counts the restore failed.
func TestConsider_RestartAdoptsRestoredIdentity(t *testing.T) {
	fastIdentity(t)
	for _, live := range []bool{true, false} {
		t.Run(fmt.Sprintf("lease live %v", live), func(t *testing.T) {
			ctx := context.Background()
			f := newFakeEtcd(t)
			keys := f.dynamoWorkerKeys(idLease, idOldIP)
			rec := idRecord(t, keys, f)
			f.mu.Lock()
			f.kvs = map[string]etcdKV{}
			f.mu.Unlock()
			pod := idPlaceholder("w-new", idNewIP)
			pod.Annotations[webhook.CRIURestoredAnnotation] = time.Now().UTC().Format(time.RFC3339)
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
			rec.Data[identityHolderKey], rec.Data[identityGrantedKey] = holderOf(pod), holderOf(pod)
			stop := make(chan struct{})
			defer close(stop)
			if live {
				f.grant(idLease, 2)
				go func() { // the restored process keeps its lease
					for {
						select {
						case <-stop:
							return
						case <-time.After(300 * time.Millisecond):
							f.refresh(idLease)
						}
					}
				}()
			}
			useFakeEtcd(t, f)
			kc := fake.NewSimpleClientset(rec, pod)
			c := idAgent(kc, true).criuRestorer() // a restarted agent: no keeper
			c.consider(ctx, pod)
			deadline := time.Now().Add(6 * time.Second)
			for time.Now().Before(deadline) {
				cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
				if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); cm.Data[identityHandoffKey] != "" || apierrors.IsNotFound(err) {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
			_, perr := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{})
			if live {
				if cm.Data[identityHandoffKey] != holderOf(pod) || perr != nil {
					t.Fatalf("handoff %q, pod %v; want the lease adopted and handed off", cm.Data[identityHandoffKey], perr)
				}
				keysName(t, f, keys, idNewIP, idOldIP)
				return
			}
			if !apierrors.IsNotFound(perr) || cm.Data[groupRestoreFailuresKey] != "1" {
				t.Fatalf("pod %v, restore failures %q; want the pod ended and the restore counted failed", perr, cm.Data[groupRestoreFailuresKey])
			}
		})
	}
}

// With identity restore on, a placeholder is restored only from the record
// of its own capture: a record gone, being captured again, or of another
// capture (which may hold another identity) says nothing of this
// checkpoint's identity, so the restore is refused and the pod starts cold.
func TestLoneRestore_RecordNotOfTheCheckpointRefused(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	useFakeEtcd(t, f)
	for _, tc := range []struct {
		name string
		rec  func() *corev1.ConfigMap
	}{
		{"record gone", func() *corev1.ConfigMap { return nil }},
		{"recapturing, no identity yet", func() *corev1.ConfigMap {
			return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: criuGroupConfigMapName(idKey), Namespace: "nvsnap-system"},
				Data: map[string]string{"state": criuGroupStateCapturing, "size": "1"}}
		}},
		{"another capture's identity", func() *corev1.ConfigMap {
			r := idRecord(t, keys, f)
			r.Data["checkpoint.0"] = "ck-newer"
			return r
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := idPlaceholder("w-new", idNewIP)
			objs := []runtime.Object{pod}
			if r := tc.rec(); r != nil {
				objs = append(objs, r)
			}
			kc := fake.NewSimpleClientset(objs...)
			c := idAgent(kc, true).criuRestorer()
			saved := restoreForRequest
			t.Cleanup(func() { restoreForRequest = saved })
			restored := false
			restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
				restored = true
				return &RestoreResult{}, nil
			}
			before := decisions(t, identityRefusedError)
			c.restore(ctx, "shared", "w-new", pod.UID, "ck", "main", idKey)
			if restored || decisions(t, identityRefusedError) != before+1 || f.live(idLease) {
				t.Fatalf("restored %v, refusals %v, lease live %v", restored, decisions(t, identityRefusedError)-before, f.live(idLease))
			}
		})
	}
}

// A holder being deleted still holds the identity until it is gone: a
// draining worker keeps its subscriptions meanwhile.
func TestClaimIdentity_TerminatingHolderHolds(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	rec := idRecord(t, f.dynamoWorkerKeys(idLease, idOldIP), f)
	old := idPlaceholder("w-old", idOldIP)
	now := metav1.Now()
	old.DeletionTimestamp = &now
	old.Finalizers = []string{"test"}
	rec.Data[identityHolderKey] = holderOf(old)
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(rec, old, pod)
	err := idAgent(kc, true).criuRestorer().claimIdentity(ctx, idKey, pod)
	if identityDecision(err) != identityRefusedHeld {
		t.Fatalf("claim beside a terminating holder: %v; want refused_held", err)
	}
}

// A refused pod that starts cold in place gives up its claim: it will
// never take the identity, and every later restore would be refused as
// held for as long as it runs.
func TestIdentityRefused_ColdStartReleasesClaim(t *testing.T) {
	cmd, _ := startPlaceholder(t, "/bin/true")
	ctx := context.Background()
	f := newFakeEtcd(t)
	rec := idRecord(t, f.dynamoWorkerKeys(idLease, idOldIP), f)
	pod := idPlaceholder("w-new", idNewIP)
	rec.Data[identityHolderKey], rec.Data[identityGrantedKey] = holderOf(pod), holderOf(pod)
	kc := fake.NewSimpleClientset(rec, pod)
	a := idAgent(kc, true)
	a.runtime = pidRuntime{pid: procPID(cmd)}
	saved := podRunsWorkload
	t.Cleanup(func() { podRunsWorkload = saved })
	podRunsWorkload = func(context.Context, *Agent, *corev1.Pod, string) (bool, bool, error) { return false, true, nil }
	refusal := refuseIdentity(identityRefusedError, errors.New("read the adopted lease: unreachable"))
	a.criuRestorer().identityRefused(ctx, idKey, "ck", []*corev1.Pod{pod}, refusal, func() {}, idLog())
	got, _ := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{})
	if got == nil || !coldStarted(got) {
		t.Fatalf("the pod did not start cold: %+v", got)
	}
	cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
	if cm.Data[identityHolderKey] != "" || cm.Data[identityGrantedKey] != "" {
		t.Fatalf("holder %q granted %q kept by a pod started cold", cm.Data[identityHolderKey], cm.Data[identityGrantedKey])
	}
}

// A refusal left unfinished by an agent that then restarts is finished by
// the pod's own agent on its next event, from the group record: a rank
// that could not be read stays a placeholder no leader restores.
func TestConsider_UnfinishedRefusalSurvivesARestart(t *testing.T) {
	ctx := context.Background()
	identityFinishRetryMu.Lock()
	saved := identityFinishRetry
	identityFinishRetry.first = time.Hour // the first agent never retries: it "restarted"
	identityFinishRetryMu.Unlock()
	t.Cleanup(func() { identityFinishRetryMu.Lock(); identityFinishRetry = saved; identityFinishRetryMu.Unlock() })
	f := newFakeEtcd(t)
	rec := idRecord(t, f.dynamoWorkerKeys(idLease, idOldIP), f)
	r0, r1, _ := idGroupPlaceholders()
	r1.Spec.NodeName = "n1"
	kc := fake.NewSimpleClientset(rec, r0, r1)
	var fail atomic.Bool
	fail.Store(true)
	kc.PrependReactor("get", "pods", func(act k8stesting.Action) (bool, runtime.Object, error) {
		if fail.Load() && act.(k8stesting.GetAction).GetName() == r1.Name {
			return true, nil, errors.New("api down")
		}
		return false, nil, nil
	})
	refusal := refuseIdentity(identityRefusedLiveLease, errors.New("live"))
	idAgent(kc, true).criuRestorer().identityRefused(ctx, idKey, "ck", []*corev1.Pod{r0, r1}, refusal, func() {}, idLog())
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, r0.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("r0 kept: %v", err)
	}
	fail.Store(false)
	other := idAgent(kc, true) // r1's agent, which never saw the refusal
	other.config.NodeName = "n1"
	other.criuRestorer().consider(ctx, r1)
	if !idPodGone(t, kc, r1.Name, 5*time.Second) {
		t.Fatal("the unfinished rank was left idle after the refusing agent restarted")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
		if cm.Data[identityRefusingKey] == "" && cm.Data["stalls"] == "1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("refusing %q, stalls %q; want the refusal cleared and counted once", cm.Data[identityRefusingKey], cm.Data["stalls"])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Admission asks for every pod; a capture refused with identity restore
// off is counted once.
func TestIdentityRecordVisible_CountedOnce(t *testing.T) {
	f := newFakeEtcd(t)
	rec := idRecord(t, f.dynamoWorkerKeys(idLease, idOldIP), f)
	a := idAgent(fake.NewSimpleClientset(), false)
	before := decisions(t, identityRefusedDisabled)
	for range 3 {
		if a.identityRecordVisible(idKey, rec.Data) {
			t.Fatal("visible with the flag off")
		}
	}
	if got := decisions(t, identityRefusedDisabled) - before; got != 1 {
		t.Fatalf("counted %v times; want once", got)
	}
}

// The lease is kept alive from its grant on: recording the grant may take
// API retries longer than a short TTL, and the lease must not expire
// before the restore.
func TestLoneRestore_LeaseKeptWhileTheGrantIsRecorded(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	f.mu.Lock()
	f.kvs = map[string]etcdKV{}
	f.mu.Unlock()
	useFakeEtcd(t, f)
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(rec, pod)
	var updates atomic.Int32
	kc.PrependReactor("update", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		if updates.Add(1) == 2 { // the grant's record (after the claim)
			time.Sleep(3 * time.Second) // longer than the 2 s TTL
		}
		return false, nil, nil
	})
	c := idAgent(kc, true).criuRestorer()
	saved := restoreForRequest
	t.Cleanup(func() { restoreForRequest = saved })
	liveAtRestore := false
	restoreForRequest = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
		liveAtRestore = f.live(idLease)
		return nil, errors.New("stop here")
	}
	c.restore(ctx, "shared", "w-new", pod.UID, "ck", "main", idKey)
	if !liveAtRestore {
		t.Fatal("the lease expired while its grant was recorded")
	}
}

// A member that was asked to reset its source's outside connections and
// does not say it did (an agent of an older version) fails the capture,
// whose stopped pods are then replaced.
func TestCheckpointInstance_PeerResetNotDoneFails(t *testing.T) {
	saved := checkpointMember
	t.Cleanup(func() { checkpointMember = saved })
	checkpointMember = func(*Agent, context.Context, CheckpointRequest) (*CheckpointResult, error) {
		return &CheckpointResult{CheckpointID: "ck"}, nil // an older agent: no peersReset
	}
	c := idAgent(fake.NewSimpleClientset(), true).criuRestorer()
	w := idWorker("w", idOldIP)
	ctx := withPeerReset(context.Background(), memberAddresses([]*corev1.Pod{w}))
	if _, stopped, err := c.checkpointInstance(ctx, []*corev1.Pod{w}, nil, nil, idLog()); err == nil || !stopped {
		t.Fatalf("stopped %v, %v; want the capture failed and its pods replaced", stopped, err)
	}
	reqs := []CheckpointRequest{{PodName: "r0", ResetPeersOutside: []string{idOldIP}}, {PodName: "c0"}}
	if peersResetDone(reqs, []*CheckpointResult{{PeersReset: true}, {}}) != nil {
		t.Fatal("a companion not asked to reset failed the capture")
	}
	if peersResetDone(reqs, []*CheckpointResult{{}, {}}) == nil {
		t.Fatal("a rank that did not reset passed")
	}
}

// podPIDRuntime answers FindContainerByPod with pid for the pod named
// name, and no container for any other.
type podPIDRuntime struct {
	nvruntime.Runtime
	name string
	pid  uint32
}

func (r podPIDRuntime) FindContainerByPod(_ context.Context, _, pod, _ string) (*nvruntime.ContainerInfo, error) {
	if pod != r.name {
		return nil, nvruntime.ErrNotFound
	}
	return &nvruntime.ContainerInfo{Name: "main", PID: r.pid}, nil
}

func (r podPIDRuntime) ContainerSpec(context.Context, string) (*specs.Spec, error) {
	return nil, nvruntime.ErrNotFound
}

// The leader starts cold in place, and a rank on another node, unreadable
// at first, is tried again alone: with no pod of its instance inspected
// beside it, it is never deleted, only kept for its own agent.
func TestIdentityRefused_RemoteRankNotDeletedOnRetry(t *testing.T) {
	setFinishRetry(t)
	cmd, _ := startPlaceholder(t, "/bin/true")
	ctx := context.Background()
	f := newFakeEtcd(t)
	rec := idRecord(t, f.dynamoWorkerKeys(idLease, idOldIP), f)
	r0, r1, _ := idGroupPlaceholders()
	r1.Spec.NodeName = "n1"
	kc := fake.NewSimpleClientset(rec, r0, r1)
	var fail atomic.Bool
	var retries atomic.Int32
	fail.Store(true)
	kc.PrependReactor("get", "pods", func(act k8stesting.Action) (bool, runtime.Object, error) {
		if act.(k8stesting.GetAction).GetName() != r1.Name {
			return false, nil, nil
		}
		retries.Add(1)
		if fail.Load() {
			return true, nil, errors.New("api down")
		}
		return false, nil, nil
	})
	a := idAgent(kc, true)
	a.config.NodeName = "n0"
	a.runtime = podPIDRuntime{name: r0.Name, pid: procPID(cmd)}
	saved := podRunsWorkload
	t.Cleanup(func() { podRunsWorkload = saved })
	podRunsWorkload = func(_ context.Context, _ *Agent, p *corev1.Pod, _ string) (bool, bool, error) {
		return false, p.Name == r0.Name, nil
	}
	refusal := refuseIdentity(identityRefusedLiveLease, errors.New("live"))
	a.criuRestorer().identityRefused(ctx, idKey, "ck", []*corev1.Pod{r0, r1}, refusal, func() {}, idLog())
	if got, _ := kc.CoreV1().Pods("shared").Get(ctx, r0.Name, metav1.GetOptions{}); got == nil || !coldStarted(got) {
		t.Fatal("the leader did not start cold")
	}
	fail.Store(false)
	before := retries.Load()
	deadline := time.Now().Add(3 * time.Second)
	for retries.Load() < before+1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if retries.Load() < before+1 {
		t.Fatal("the rank's refusal was not tried again")
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := kc.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), "shared", r1.Name); err != nil {
		t.Fatalf("a rank that could not be inspected was deleted on a retry: %v", err)
	}
	if cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{}); !strings.Contains(cm.Data[identityRefusingKey], string(r1.UID)+"=") {
		t.Fatalf("the rank's refusal is no longer recorded for its own agent: %q", cm.Data[identityRefusingKey])
	}
}

// A rank on another node is never deleted by the agent that refused its
// instance, even beside a leader seen idle: its own restore may have run,
// which only its own agent can see. Its refusal stays recorded for that
// agent.
func TestIdentityRefused_RemoteRankLeftToItsAgent(t *testing.T) {
	ctx := context.Background()
	setDeleteRetry(t, time.Millisecond, 5*time.Millisecond, 20*time.Millisecond)
	f := newFakeEtcd(t)
	rec := idRecord(t, f.dynamoWorkerKeys(idLease, idOldIP), f)
	r0, r1, _ := idGroupPlaceholders()
	r1.Spec.NodeName = "n1"
	kc := fake.NewSimpleClientset(rec, r0, r1)
	saved := podRunsWorkload
	t.Cleanup(func() { podRunsWorkload = saved })
	podRunsWorkload = func(_ context.Context, _ *Agent, p *corev1.Pod, _ string) (bool, bool, error) {
		return false, p.Name == r0.Name, nil // the leader seen idle; the rank not inspectable
	}
	refusal := refuseIdentity(identityRefusedLiveLease, errors.New("live"))
	idAgent(kc, true).criuRestorer().identityRefused(ctx, idKey, "ck", []*corev1.Pod{r0, r1}, refusal, func() {}, idLog())
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, r0.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the idle leader that cannot start cold was kept: %v", err)
	}
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, r1.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("a rank on another node was deleted: %v", err)
	}
	if cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{}); !strings.Contains(cm.Data[identityRefusingKey], string(r1.UID)+"=") {
		t.Fatalf("the rank's refusal is not recorded for its own agent: %q", cm.Data[identityRefusingKey])
	}
}

// With identity restore off, a group's rank whose record is gone and whose
// leader cannot restore it any more is started cold (here: deleted, as it
// cannot start cold) by its own agent; one whose leader may still restore
// it is left to that leader.
func TestGroupRestore_RecordGoneRankFinishedWithTheFlagOff(t *testing.T) {
	kc, _, r1, fail := groupRefusal(t, func(kc *fake.Clientset) {
		_ = kc.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("configmaps"), "nvsnap-system", criuGroupConfigMapName(idKey))
	})
	fail.Store(false)
	other := idAgent(kc, false)
	other.config.NodeName = "n1"
	other.criuRestorer().consider(context.Background(), r1)
	if !idPodGone(t, kc, r1.Name, 5*time.Second) {
		t.Fatal("the orphaned rank of a record gone was left idle by an agent with the flag off")
	}
	// A rank whose leader is a placeholder still: left to the leader.
	r0, r1b, other2 := idGroupPlaceholders()
	kc2 := fake.NewSimpleClientset(r0, r1b, other2)
	c := idAgent(kc2, false).criuRestorer()
	if c.orphanedRank(context.Background(), r1b) {
		t.Fatal("a rank whose leader may restore it counted as orphaned")
	}
}

// The counted refusals are never dropped to make room: a refusal awaiting
// its restore's next event keeps its entry however many others are
// counted, so it is not counted twice; past the bound a new refusal is not
// counted and its pod not deleted, until entries whose pods are gone go.
func TestChargeRefusal_NeverEvicts(t *testing.T) {
	ctx := context.Background()
	setDeleteRetry(t, time.Millisecond, 5*time.Millisecond, 20*time.Millisecond)
	f := newFakeEtcd(t)
	rec := idRecord(t, f.dynamoWorkerKeys(idLease, idOldIP), f)
	ours := idPlaceholder("w-new", idNewIP)
	objs := []runtime.Object{ours}
	counted := []string{holderOf(ours)}
	for i := range identityRefusalListMax - 1 {
		p := idPlaceholder(fmt.Sprintf("o-%d", i), idNewIP)
		p.UID = types.UID(fmt.Sprintf("o-%d-uid", i))
		objs = append(objs, p)
		counted = append(counted, holderOf(p))
	}
	rec.Data[identityRefusedPodsKey], rec.Data["stalls"] = strings.Join(counted, ","), "1"
	newer := idPlaceholder("w-newer", idNewIP)
	newer.UID = "w-newer-uid"
	kc := fake.NewSimpleClientset(append(objs, rec, newer)...)
	var failDel atomic.Bool
	failDel.Store(true)
	kc.PrependReactor("delete", "pods", func(act k8stesting.Action) (bool, runtime.Object, error) {
		if failDel.Load() || act.(k8stesting.DeleteAction).GetName() == ours.Name {
			return true, nil, errors.New("api down")
		}
		return false, nil, nil
	})
	c := idAgent(kc, true).criuRestorer()
	refusal := refuseIdentity(identityRefusedLiveLease, errors.New("live"))
	c.identityRefused(ctx, idKey, "ck", []*corev1.Pod{newer}, refusal, func() {}, idLog())
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, newer.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("a pod whose refusal could not be counted was deleted: %v", err)
	}
	c.identityRefused(ctx, idKey, "ck", []*corev1.Pod{ours}, refusal, func() {}, idLog())
	cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
	if cm.Data["stalls"] != "1" || !strings.Contains(cm.Data[identityRefusedPodsKey], holderOf(ours)) {
		t.Fatalf("stalls %q, ours listed %v; want our refusal kept and not counted again", cm.Data["stalls"], strings.Contains(cm.Data[identityRefusedPodsKey], holderOf(ours)))
	}
	// The others' pods go: their entries are dead, and the newer refusal
	// is counted.
	failDel.Store(false)
	for i := range identityRefusalListMax - 1 {
		_ = kc.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), "shared", fmt.Sprintf("o-%d", i))
	}
	c.identityRefused(ctx, idKey, "ck", []*corev1.Pod{newer}, refusal, func() {}, idLog())
	cm, _ = kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
	if cm.Data["stalls"] != "2" || !idPodGone(t, kc, newer.Name, time.Second) {
		t.Fatalf("stalls %q; want the newer refusal counted once room was made", cm.Data["stalls"])
	}
}

// The renewal the keeper makes before a handoff can itself answer late,
// after the lease it renewed expired: that is no handoff, and the lease
// is found lost.
func TestLeaseKeeper_LateRenewalReplyIsNotAHandoff(t *testing.T) {
	fastIdentity(t)
	f := newFakeEtcd(t)
	f.grant(idLease, 2)
	stop := make(chan struct{})
	var stopped atomic.Bool
	go func() { // the process keeps its lease, until the keeper's last renewal
		for {
			select {
			case <-stop:
				return
			case <-time.After(300 * time.Millisecond):
				if !stopped.Load() {
					f.refresh(idLease)
				}
			}
		}
	}()
	defer close(stop)
	delayed := false
	f.onKeepAliveReply = func() { // under the fake's lock
		if !delayed {
			delayed = true
			stopped.Store(true)                 // the process stops renewing
			time.Sleep(2500 * time.Millisecond) // answered after the 2 s it granted
		}
	}
	k := newLeaseKeeper(f.client(), idLease, 2, idLog())
	var zombie atomic.Bool
	k.onZombie = func() { zombie.Store(true) }
	handedOff := false
	k.onHandoff = func() { handedOff = true }
	go k.run(context.Background())
	k.restored(nil)
	select {
	case <-k.done:
	case <-time.After(15 * time.Second):
		t.Fatal("keeper did not end")
	}
	if handedOff || k.outcome == identityHandoffSeen || !zombie.Load() {
		t.Fatalf("outcome %s, handed off %v, zombie %v; want the lost lease to end the pod", k.outcome, handedOff, zombie.Load())
	}
}

// groupRefusal drives a group restore refused for a live lease, through
// restoreGroup: the leader cannot start cold and is deleted, rank 1 (on
// node n1) cannot be read, and the refusing agent never retries (it
// "restarts"). before runs once the restore has read its record.
func groupRefusal(t *testing.T, before func(kc *fake.Clientset)) (kc *fake.Clientset, r0, r1 *corev1.Pod, fail *atomic.Bool) {
	t.Helper()
	fastIdentity(t)
	identityFinishRetryMu.Lock()
	saved := identityFinishRetry
	identityFinishRetry.first = time.Hour
	identityFinishRetryMu.Unlock()
	t.Cleanup(func() { identityFinishRetryMu.Lock(); identityFinishRetry = saved; identityFinishRetryMu.Unlock() })
	ctx := context.Background()
	f := newFakeEtcd(t)
	keys := f.dynamoWorkerKeys(idLease, idOldIP)
	rec := idRecord(t, keys, f)
	rec.Data["size"], rec.Data[identityMemberKey] = "2", "1"
	rec.Data["checkpoint.0"], rec.Data["checkpoint.1"], rec.Data["node.1"] = "ck0", "ck1", "n1"
	f.grant(idLease, 2)
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(300 * time.Millisecond):
				f.refresh(idLease)
			}
		}
	}()
	useFakeEtcd(t, f)
	var other *corev1.Pod
	r0, r1, other = idGroupPlaceholders()
	r1.Spec.NodeName = "n1"
	kc = fake.NewSimpleClientset(rec, r0, r1, other)
	once := false
	f.onTimeToLive = func() {
		if !once && before != nil {
			once = true
			before(kc)
		}
	}
	c := idAgent(kc, true).criuRestorer()
	for _, p := range []*corev1.Pod{r0, r1} {
		c.verdicts.Store(string(p.UID)+"\x00"+p.Annotations[webhook.CRIURestoreAnnotation]+"\x00"+idKey, "")
	}
	fail = &atomic.Bool{}
	kc.PrependReactor("get", "pods", func(act k8stesting.Action) (bool, runtime.Object, error) {
		if fail.Load() && act.(k8stesting.GetAction).GetName() == r1.Name {
			return true, nil, errors.New("api down")
		}
		return false, nil, nil
	})
	saved2 := restoreInstance
	t.Cleanup(func() { restoreInstance = saved2 })
	restoreInstance = func(*Agent, context.Context, GroupRestoreRequest, *logrus.Entry) (*GroupRestoreResult, error) {
		t.Error("restored a refused instance")
		return &GroupRestoreResult{}, nil
	}
	setDeleteRetry(t, time.Millisecond, 5*time.Millisecond, 50*time.Millisecond)
	fail.Store(true)
	c.restoreGroup(ctx, r0, idKey, 2)
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, r0.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the leader was kept: %v", err)
	}
	return kc, r0, r1, fail
}

// After the refusing agent restarts with identity restore off, the rank
// it left unfinished is still finished by the rank's own agent: the
// refusal is on the group record, and settling it needs no identity.
func TestGroupRestore_UnfinishedRankFinishedWithTheFlagOff(t *testing.T) {
	kc, _, r1, fail := groupRefusal(t, nil)
	fail.Store(false)
	other := idAgent(kc, false)
	other.config.NodeName = "n1"
	other.criuRestorer().consider(context.Background(), r1)
	if !idPodGone(t, kc, r1.Name, 5*time.Second) {
		t.Fatal("the unfinished rank was left idle by an agent with the flag off")
	}
}

// A group record gone while the restore ran leaves no refusal on record;
// the rank's own agent refuses it all the same (any restore of it would
// be), so it is not left idle.
func TestGroupRestore_RecordGoneRankFinished(t *testing.T) {
	kc, _, r1, fail := groupRefusal(t, func(kc *fake.Clientset) {
		_ = kc.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("configmaps"), "nvsnap-system", criuGroupConfigMapName(idKey))
	})
	fail.Store(false)
	other := idAgent(kc, true)
	other.config.NodeName = "n1"
	other.criuRestorer().consider(context.Background(), r1)
	if !idPodGone(t, kc, r1.Name, 5*time.Second) {
		t.Fatal("the rank of a record gone was left idle")
	}
}

// One refusal counts one stall, whichever agent counts it first: ranks'
// own agents finishing the refusal before the refusing agent does, with
// every delete failing, still count one.
func TestIdentityRefused_RanksChargeBeforeTheLeader(t *testing.T) {
	ctx := context.Background()
	setDeleteRetry(t, time.Millisecond, 5*time.Millisecond, 20*time.Millisecond)
	f := newFakeEtcd(t)
	rec := idRecord(t, f.dynamoWorkerKeys(idLease, idOldIP), f)
	r0, r1, _ := idGroupPlaceholders()
	r2 := r1.DeepCopy()
	r2.Name, r2.UID = "w-2", "w-2-uid"
	r2.Annotations[webhook.CRIURestoreAnnotation] = "ck2"
	rec.Data["size"], rec.Data["checkpoint.0"], rec.Data["checkpoint.1"], rec.Data["checkpoint.2"] = "3", "ck0", "ck1", "ck2"
	kc := fake.NewSimpleClientset(rec, r0, r1, r2)
	kc.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("api down")
	})
	leader := idAgent(kc, true).criuRestorer()
	pods := []*corev1.Pod{r0, r1, r2}
	if err := leader.markRefusing(ctx, idKey, refusalID(pods), pods); err != nil {
		t.Fatal(err)
	}
	for _, p := range []*corev1.Pod{r1, r2} { // the ranks' own agents, first
		c := idAgent(kc, true).criuRestorer()
		c.consider(ctx, p)
		deadline := time.Now().Add(3 * time.Second)
		for {
			if _, busy := c.attempted.Load("refusing/" + string(p.UID)); !busy || time.Now().After(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{}); cm.Data["stalls"] != "1" {
		t.Fatalf("stalls %q after the ranks' agents; want 1 (refusing %q)", cm.Data["stalls"], cm.Data[identityRefusingKey])
	}
	refusal := refuseIdentity(identityRefusedLiveLease, errors.New("live"))
	leader.identityRefused(ctx, idKey, "ck0", pods, refusal, func() {}, idLog())
	if cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{}); cm.Data["stalls"] != "1" {
		t.Fatalf("stalls %q; want 1 for one refusal", cm.Data["stalls"])
	}
}

// The list of refusals in progress never drops one to make room: a
// refusal past its bound is not recorded, and so not acted on.
func TestMarkRefusing_FullListRefuses(t *testing.T) {
	ctx := context.Background()
	f := newFakeEtcd(t)
	rec := idRecord(t, f.dynamoWorkerKeys(idLease, idOldIP), f)
	var l []string
	for i := range identityRefusalListMax {
		l = append(l, fmt.Sprintf("u%d=r", i))
	}
	rec.Data[identityRefusingKey] = strings.Join(l, ",")
	pod := idPlaceholder("w-new", idNewIP)
	kc := fake.NewSimpleClientset(rec, pod)
	c := idAgent(kc, true).criuRestorer()
	if err := c.markRefusing(ctx, idKey, "w-new-uid", []*corev1.Pod{pod}); !errors.Is(err, errRefusalListFull) {
		t.Fatalf("a full list: %v", err)
	}
	again := false
	c.identityRefused(ctx, idKey, "ck", []*corev1.Pod{pod}, refuseIdentity(identityRefusedLiveLease, errors.New("live")), func() { again = true }, idLog())
	if _, err := kc.CoreV1().Pods("shared").Get(ctx, "w-new", metav1.GetOptions{}); err != nil || !again {
		t.Fatalf("acted on an unrecorded refusal: %v, again %v", err, again)
	}
	cm, _ := kc.CoreV1().ConfigMaps("nvsnap-system").Get(ctx, rec.Name, metav1.GetOptions{})
	if got := len(strings.Split(cm.Data[identityRefusingKey], ",")); got != identityRefusalListMax || !strings.HasPrefix(cm.Data[identityRefusingKey], "u0=r,") {
		t.Fatalf("the list changed: %d entries", got)
	}
}
