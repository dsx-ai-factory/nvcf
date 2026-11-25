// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelvolume"
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
	return &Agent{kubeClient: kc, log: idLog().Logger, config: Config{IdentityRestore: on}}
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
	if _, ok := f.value(keys[0]); ok {
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
	g.setFail("/v3/kv/put", true)
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
	version := 0
	kc.PrependReactor("update", "configmaps", func(act k8stesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		cm := act.(k8stesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if cm.ResourceVersion != strconv.Itoa(version) {
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
	if won != 1 {
		t.Fatalf("%d claims won: %v", won, errs)
	}
}

func TestLeaseKeeper(t *testing.T) {
	fastIdentity(t)
	ctx := context.Background()
	start := func(f *fakeEtcd, ttl int64) (*leaseKeeper, *bool) {
		f.grant(idLease, ttl)
		failed := false
		k := newLeaseKeeper(f.client(), idLease, ttl, func() {}, func() { failed = true }, idLog())
		go k.run(ctx)
		return k, &failed
	}
	wait := func(t *testing.T, k *leaseKeeper, within time.Duration) {
		t.Helper()
		select {
		case <-k.done:
		case <-time.After(within):
			t.Fatal("keeper did not end")
		}
	}
	t.Run("keeps the lease alive through a long restore, hands off", func(t *testing.T) {
		f := newFakeEtcd(t)
		k, failed := start(f, 2)
		time.Sleep(3 * time.Second) // longer than the TTL: the keeper renews
		if !f.live(idLease) {
			t.Fatal("the keeper let the lease expire during the restore")
		}
		k.restored(nil)
		// The restored process's keep-alive.
		stop := make(chan struct{})
		go func() {
			for {
				select {
				case <-stop:
					return
				case <-time.After(700 * time.Millisecond):
					f.refresh(idLease)
				}
			}
		}()
		defer close(stop)
		wait(t, k, 5*time.Second)
		if k.outcome != identityHandoffSeen || *failed {
			t.Fatalf("outcome %s, failed %v", k.outcome, *failed)
		}
		if !f.live(idLease) {
			t.Error("the lease went with the handoff")
		}
	})
	t.Run("process never keeps it alive: revoked", func(t *testing.T) {
		f := newFakeEtcd(t)
		k, failed := start(f, 1)
		k.restored(nil)
		wait(t, k, 6*time.Second)
		if k.outcome != identityHandoffTimeout || !*failed || f.live(idLease) {
			t.Fatalf("outcome %s, failed %v, live %v", k.outcome, *failed, f.live(idLease))
		}
	})
	t.Run("restore failed: revoked", func(t *testing.T) {
		f := newFakeEtcd(t)
		k, failed := start(f, 5)
		k.restored(errors.New("criu failed"))
		k.restored(nil) // only the first counts
		wait(t, k, 2*time.Second)
		if k.outcome != identityRestoreAborted || !*failed || f.live(idLease) {
			t.Fatalf("outcome %s, failed %v, live %v", k.outcome, *failed, f.live(idLease))
		}
	})
	t.Run("lease lost", func(t *testing.T) {
		f := newFakeEtcd(t)
		k, failed := start(f, 5)
		_ = f.client().Revoke(ctx, idLease)
		wait(t, k, 2*time.Second)
		if k.outcome != identityHandoffLost || !*failed {
			t.Fatalf("outcome %s, failed %v", k.outcome, *failed)
		}
	})
	t.Run("agent stops", func(t *testing.T) {
		f := newFakeEtcd(t)
		f.grant(idLease, 5)
		cctx, cancel := context.WithCancel(ctx)
		k := newLeaseKeeper(f.client(), idLease, 5, func() {}, nil, idLog())
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

	saved := restoreLone
	t.Cleanup(func() { restoreLone = saved })
	var grantedFirst bool
	restoreLone = func(_ *Agent, _ context.Context, req RestoreRequest) (*RestoreResult, error) {
		grantedFirst = f.live(idLease)
		keysName(t, f, keys, idNewIP, idOldIP)
		return &RestoreResult{}, nil
	}
	before := decisions(t, identityRegranted)
	c.restore(ctx, "shared", "w-new", pod.UID, "ck", "main", idKey)
	if !grantedFirst {
		t.Fatal("the process was restored before its lease was granted again")
	}
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
	saved := restoreLone
	t.Cleanup(func() { restoreLone = saved })
	restored := false
	restoreLone = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
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
	if cm.Data["stalls"] != "" || cm.Data["state"] != criuGroupStateComplete || cm.Data[identityHolderKey] != "" {
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
	saved := restoreLone
	t.Cleanup(func() { restoreLone = saved })
	restoreLone = func(*Agent, context.Context, RestoreRequest) (*RestoreResult, error) {
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
	k, err := c.loneIdentityBeforeRestore(ctx, idKey, "shared", "w-new", "ck", idLog())
	if k != nil || err != nil || opened.Load() != 0 || len(kc.Actions()) != 0 {
		t.Fatalf("flag off: keeper %v err %v, registry opened %d, API calls %d", k, err, opened.Load(), len(kc.Actions()))
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
		return &CheckpointResult{CheckpointID: "ck"}, nil
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
		return &CheckpointResult{CheckpointID: "ck-a"}, nil
	}
	deleteMember = func(*Agent, context.Context, string, string, types.UID) error { return nil }
	before := decisions(t, identityCaptured)
	if retry := a.criuRestorer().captureGroup(ctx, leader, "", 1); retry {
		t.Fatal("retried")
	}
	if len(reqs) != 1 || reqs[0].PodName != "w-a" || strings.Join(reqs[0].ResetPeersOutside, ",") != idOldIP {
		t.Fatalf("checkpoint requests %+v; want the worker alone, its outside connections reset", reqs)
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
				keysName(t, f, keys, idNewIP, idOldIP)
				return &GroupRestoreResult{}, nil
			}
			c.restoreGroup(ctx, r0, idKey, 2)
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
				return
			}
			if !called || !grantedFirst {
				t.Fatalf("restored %v, granted first %v", called, grantedFirst)
			}
			if cm.Data[identityHolderKey] != holderOf(r1) {
				t.Errorf("holder %q; want the lease holder's slot", cm.Data[identityHolderKey])
			}
		})
	}
}
