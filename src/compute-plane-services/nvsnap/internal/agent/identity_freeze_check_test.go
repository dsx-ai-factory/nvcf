// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// The identity read before the freeze is checked again once the holder
// is frozen, from the holder's own network namespace: a key added,
// removed or changed in between, or a registry that cannot be read,
// fails the dump before it runs.
func TestFrozenIdentityCheck(t *testing.T) {
	ctx := context.Background()
	a := idAgent(fake.NewSimpleClientset(), true)
	if h := a.frozenIdentityCheck(ctx, CheckpointRequest{}, new(bool)); h != nil {
		t.Fatal("a check without an identity to check")
	}
	for _, tc := range []struct {
		name   string
		change func(f *fakeEtcd, keys []string)
		ok     bool
	}{
		{"unchanged", func(*fakeEtcd, []string) {}, true},
		{"key added", func(f *fakeEtcd, _ []string) { f.put("v1/mdc/idpool/backend/generate/lora", `{}`, idLease) }, false},
		{"key removed", func(f *fakeEtcd, keys []string) {
			f.mu.Lock()
			delete(f.kvs, keys[len(keys)-1])
			f.mu.Unlock()
		}, false},
		{"value changed", func(f *fakeEtcd, keys []string) {
			f.put(keys[3], `{"type":"Model","card_json":{"path":"http://`+idOldIP+`:9090/v2"}}`, idLease)
		}, false},
		{"registry unreadable", func(f *fakeEtcd, _ []string) { f.setFail("/v3/lease/timetolive", true) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeEtcd(t)
			f.grant(idLease, 30)
			keys := f.dynamoWorkerKeys(idLease, idOldIP)
			id, err := readIdentity(ctx, f.client(), []string{idOldIP}, idEtcdEP)
			if err != nil {
				t.Fatal(err)
			}
			saved := newIdentityEtcd
			t.Cleanup(func() { newIdentityEtcd = saved })
			var via, container, endpoints string
			newIdentityEtcd = func(_ context.Context, _ *Agent, ep, _ string, p *corev1.Pod, c string) (identityEtcd, func(), error) {
				via, container, endpoints = p.Namespace+"/"+p.Name, c, ep
				return f.client(), func() {}, nil
			}
			tc.change(f, keys)
			checked := false
			req := CheckpointRequest{Namespace: "shared", PodName: "w", ContainerName: "main", IdentityCheck: id}
			err = a.frozenIdentityCheck(ctx, req, &checked)()
			if tc.ok != (err == nil) || checked != tc.ok {
				t.Fatalf("check: %v, checked %v; want ok %v", err, checked, tc.ok)
			}
			if via != "shared/w" || container != "main" || endpoints != idEtcdEP {
				t.Errorf("checked through %s %s %s; want the holder's own namespace and etcd", via, container, endpoints)
			}
		})
	}
}

// The check runs as the dump's frozen hook, before the peer reset lists
// the container's connections; a failure stops the hooks after it.
func TestFrozenHooks_IdentityCheckFirst(t *testing.T) {
	if frozenHooks(nil, nil) != nil {
		t.Error("hooks without a hook")
	}
	var ran []string
	check := func() error { ran = append(ran, "identity"); return errors.New("changed") }
	inventory := func() error { ran = append(ran, "inventory"); return nil }
	hooks := dumpHooksFor(dumpV2Opts{IdentityCheck: check}, nil)
	if hooks.Frozen == nil {
		t.Fatal("the dump's frozen hook does not check the identity")
	}
	if err := frozenHooks(hooks.Frozen, inventory)(); err == nil || strings.Join(ran, ",") != "identity" {
		t.Fatalf("ran %v (%v); want the check alone, failing", ran, err)
	}
	ran = nil
	if err := frozenHooks(nil, inventory)(); err != nil || strings.Join(ran, ",") != "inventory" {
		t.Fatalf("ran %v (%v)", ran, err)
	}
}

// The capture asks the agent that dumps the lease holder to check the
// identity under the freeze, and fails, its source dumped, when that
// agent does not confirm it (one that predates the check).
func TestCheckpointInstance_IdentityCheckRequest(t *testing.T) {
	saved := checkpointMember
	t.Cleanup(func() { checkpointMember = saved })
	var got *capturedIdentity
	confirm := true
	checkpointMember = func(_ *Agent, _ context.Context, req CheckpointRequest) (*CheckpointResult, error) {
		got = req.IdentityCheck
		return &CheckpointResult{CheckpointID: "ck", IdentityChecked: confirm && req.IdentityCheck != nil}, nil
	}
	c := idAgent(fake.NewSimpleClientset(), true).criuRestorer()
	w := idWorker("w", idOldIP)
	if _, _, err := c.checkpointInstance(context.Background(), []*corev1.Pod{w}, nil, nil, idLog()); err != nil || got != nil {
		t.Fatalf("plain capture: check %+v (%v)", got, err)
	}
	id := &capturedIdentity{Lease: idLease, TTL: 30, PodIP: idOldIP, Endpoints: idEtcdEP, Keys: []identityKey{{Key: []byte("k"), Value: []byte("v")}}}
	ctx := withIdentityCheck(context.Background(), id)
	if res, _, err := c.checkpointInstance(ctx, []*corev1.Pod{w}, nil, nil, idLog()); err != nil || got != id || len(res) != 1 {
		t.Fatalf("identity capture: check %+v (%v)", got, err)
	}
	confirm = false
	res, stopped, err := c.checkpointInstance(ctx, []*corev1.Pod{w}, nil, nil, idLog())
	if err == nil || !strings.Contains(err.Error(), "did not confirm") || !stopped || res != nil {
		t.Fatalf("an unconfirmed identity was captured: %v (stopped %v)", err, stopped)
	}
	// By slot: only the holder's result counts.
	id.Member = 1
	if identityUnchecked(id, []*CheckpointResult{{IdentityChecked: true}, {}}) == nil {
		t.Error("another rank's confirmation was taken for the holder's")
	}
	if identityUnchecked(id, []*CheckpointResult{{}, {IdentityChecked: true}}) != nil {
		t.Error("the holder's confirmation was not taken")
	}
	if identityUnchecked(id, []*CheckpointResult{{IdentityChecked: true}}) == nil {
		t.Error("a missing holder result was taken for a confirmation")
	}
}

// A capture whose holder's agent did not confirm the identity under the
// freeze records no identity: it fails, and the dumped instance's pods
// are replaced.
func TestCaptureGroup_IdentityNotConfirmedFails(t *testing.T) {
	saved, savedCk, savedDel := groupWarmupWait, checkpointMember, deleteMember
	groupWarmupWait = 0
	t.Cleanup(func() { groupWarmupWait, checkpointMember, deleteMember = saved, savedCk, savedDel })
	ctx := context.Background()
	f := newFakeEtcd(t)
	f.grant(idLease, 30)
	f.dynamoWorkerKeys(idLease, idOldIP)
	f.grant(idLease+1, 30)
	f.dynamoWorkerKeys(idLease+1, "10.0.0.9")
	useFakeEtcd(t, f)
	leader := idWorker("w-a", idOldIP)
	other := idWorker("w-b", "10.0.0.9")
	other.OwnerReferences[0].UID = "rs2"
	agentPod := ready(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "nvsnap-agent-x", Namespace: "nvsnap-system",
		Labels: map[string]string{"app": "nvsnap-agent"}}, Spec: corev1.PodSpec{NodeName: "n0"}})
	kc := fake.NewSimpleClientset(leader, other, agentPod)
	a := idAgent(kc, true)
	a.seedDrivers()
	checkpointMember = func(_ *Agent, _ context.Context, req CheckpointRequest) (*CheckpointResult, error) {
		if req.IdentityCheck == nil {
			t.Error("the holder's dump was not asked to check the identity")
		}
		// Peers reset, the identity not checked: an agent that predates the check.
		return &CheckpointResult{CheckpointID: "ck-a", PeersReset: len(req.ResetPeersOutside) > 0}, nil
	}
	var deleted []string
	deleteMember = func(_ *Agent, _ context.Context, _, name string, _ types.UID) error {
		deleted = append(deleted, name)
		return nil
	}
	before := decisions(t, identityCaptured)
	if retry := a.criuRestorer().captureGroup(ctx, leader, "", 1); retry {
		t.Fatal("retried")
	}
	cms, _ := kc.CoreV1().ConfigMaps("nvsnap-system").List(ctx, metav1.ListOptions{})
	for _, cm := range cms.Items {
		if hasIdentity(cm.Data) || cm.Data["state"] != criuGroupStateFailed || !strings.Contains(cm.Data["reason"], "did not confirm its etcd identity") {
			t.Fatalf("record %v; want a capture failed for the unconfirmed identity, without one", cm.Data)
		}
	}
	if decisions(t, identityCaptured) != before {
		t.Error("an unconfirmed identity was counted captured")
	}
	if strings.Join(deleted, ",") != "w-a" {
		t.Errorf("deleted %v; want the dumped worker replaced", deleted)
	}
}
