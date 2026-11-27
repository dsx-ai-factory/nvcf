// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func TestPoolSlotKeys(t *testing.T) {
	if poolSlotKey("abc", 0) != "abc" || poolSlotKey("abc", 3) != "abc-s3" {
		t.Fatalf("slot keys: %q %q", poolSlotKey("abc", 0), poolSlotKey("abc", 3))
	}
	for key, want := range map[string]int{"abc": 0, "abc-s1": 1, "abc-s31": 31} {
		if k, ok := poolSlotOf("abc", key); !ok || k != want {
			t.Errorf("poolSlotOf(%q) = %d %v, want %d", key, k, ok, want)
		}
	}
	for _, key := range []string{"abcd", "abc-s0", "abc-s01", "abc-s32", "abc-sx", "x-s1", "abc-s"} {
		if _, ok := poolSlotOf("abc", key); ok {
			t.Errorf("poolSlotOf(%q) accepted another configuration's key", key)
		}
	}
}

// poolRecord is a complete group record of one rank registering through
// Kubernetes with the spec of workerSpec(ip), whose instance ID is id.
func poolRecord(key, ip string, id int64) *corev1.ConfigMap {
	spec := workerSpec(ip)
	spec["data"].(map[string]any)["endpoints"].(map[string]any)["qwen/backend/generate/1c4c"].(map[string]any)["instance_id"] = id
	raw, _ := json.Marshal(spec)
	regs, _ := json.Marshal(rankRegistrations{PodIP: ip, Objects: []workerRegistration{{Controller: true, Spec: raw}}})
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: criuGroupConfigMapName(key), Namespace: "nvsnap-system"},
		Data: map[string]string{"state": criuGroupStateComplete, "size": "1", "kubeRegistrations.0": string(regs)}}
}

func livePod(ns, name, uid string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid)}}
}

func instanceClaim(ns, id, pod, uid string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: instanceClaimPrefix + id, Namespace: ns,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: pod, UID: types.UID(uid)}}},
		Data: map[string]string{"pod": pod}}
}

func TestRecordInstanceIDs(t *testing.T) {
	if ids := recordInstanceIDs(poolRecord("k", "10.0.0.1", 111).Data); len(ids) != 1 || ids[0] != "111" {
		t.Fatalf("ids %v, want [111]", ids)
	}
	if ids := recordInstanceIDs(map[string]string{"state": "complete"}); len(ids) != 0 {
		t.Fatalf("a record without registrations: %v", ids)
	}
}

// Admission takes the first complete slot whose identity no running pod of
// the namespace holds; with every slot held, none (the pod starts cold).
func TestPoolSlotForRestore(t *testing.T) {
	ctx := context.Background()
	objs := []runtime.Object{
		poolRecord("cfg", "10.0.0.1", 111), poolRecord("cfg-s1", "10.0.0.2", 222), poolRecord("cfg-s2", "10.0.0.3", 333),
		livePod("fn", "prefill-a", "ua"), instanceClaim("fn", "111", "prefill-a", "ua"), // slot 0 held
	}
	a := &Agent{kubeClient: k8sfake.NewSimpleClientset(objs...), dynClient: fakeDyn()}
	if got := a.poolSlotForRestore(ctx, "fn", "cfg"); got != "cfg-s1" {
		t.Fatalf("slot 0 held: %q, want cfg-s1", got)
	}
	if got := a.poolSlotForRestore(ctx, "other", "cfg"); got != "cfg" {
		t.Fatalf("another namespace: %q, want cfg (the claim is per namespace)", got)
	}
	// Slot 1 held by a running pod's own registration (no claim).
	a.dynClient = fakeDyn(workerCR("fn", "prefill-b", "prefill-b", "ub", true, func() map[string]any {
		s := workerSpec("10.0.0.9")
		s["data"].(map[string]any)["endpoints"].(map[string]any)["qwen/backend/generate/1c4c"].(map[string]any)["instance_id"] = int64(222)
		return s
	}()))
	if err := a.kubeClient.(*k8sfake.Clientset).Tracker().Add(livePod("fn", "prefill-b", "ub")); err != nil {
		t.Fatal(err)
	}
	if got := a.poolSlotForRestore(ctx, "fn", "cfg"); got != "cfg-s2" {
		t.Fatalf("slots 0 and 1 held: %q, want cfg-s2", got)
	}
	if err := a.kubeClient.(*k8sfake.Clientset).Tracker().Add(livePod("fn", "prefill-c", "uc")); err != nil {
		t.Fatal(err)
	}
	if err := a.kubeClient.(*k8sfake.Clientset).Tracker().Add(instanceClaim("fn", "333", "prefill-c", "uc")); err != nil {
		t.Fatal(err)
	}
	if got := a.poolSlotForRestore(ctx, "fn", "cfg"); got != "" {
		t.Fatalf("every slot held: %q, want none", got)
	}
	// A claim whose pod is gone holds nothing.
	if err := a.kubeClient.(*k8sfake.Clientset).Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), "fn", "prefill-a"); err != nil {
		t.Fatal(err)
	}
	if got := a.poolSlotForRestore(ctx, "fn", "cfg"); got != "cfg" {
		t.Fatalf("slot 0's holder gone: %q, want cfg", got)
	}
}

// A configuration without registrations is no pool: its record as before,
// or none when there is no record.
func TestPoolSlotForRestoreWithoutPool(t *testing.T) {
	ctx := context.Background()
	plain := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: criuGroupConfigMapName("cfg"), Namespace: "nvsnap-system"},
		Data: map[string]string{"state": criuGroupStateComplete, "size": "1"}}
	a := &Agent{kubeClient: k8sfake.NewSimpleClientset(plain), dynClient: fakeDyn()}
	if got := a.poolSlotForRestore(ctx, "fn", "cfg"); got != "cfg" {
		t.Fatalf("plain record: %q", got)
	}
	if got := a.poolSlotForRestore(ctx, "fn", "nothing"); got != "nothing" {
		t.Fatalf("no record: %q, want the key (the lookup finds nothing, as before)", got)
	}
}

// A capture goes into the first slot with no record, or one that may be
// captured again or replaced; none when the pool is full.
func TestPoolSlotForCapture(t *testing.T) {
	ctx := context.Background()
	c := (&Agent{kubeClient: k8sfake.NewSimpleClientset(poolRecord("cfg", "10.0.0.1", 111), poolRecord("cfg-s1", "10.0.0.2", 222))}).criuRestorer()
	if got, ok := c.poolSlotForCapture(ctx, "cfg", nil); !ok || got != "cfg-s2" {
		t.Fatalf("two slots taken: %q %v, want cfg-s2", got, ok)
	}
	if got, ok := c.poolSlotForCapture(ctx, "cfg", func(d map[string]string) bool { return d["kubeRegistrations.0"] != "" && d["size"] == "1" }); !ok || got != "cfg" {
		t.Fatalf("slot 0 replaceable: %q %v, want cfg", got, ok)
	}
	full := []runtime.Object{}
	for k := 0; k < identityPoolSlots; k++ {
		full = append(full, poolRecord(poolSlotKey("cfg", k), "10.0.0.1", int64(100+k)))
	}
	c = (&Agent{kubeClient: k8sfake.NewSimpleClientset(full...)}).criuRestorer()
	if got, ok := c.poolSlotForCapture(ctx, "cfg", nil); ok {
		t.Fatalf("full pool: %q", got)
	}
}

// The capture side's question: a free complete slot exists. No record at all
// is no free slot (the configuration's first replica is captured).
func TestPoolHasFreeSlot(t *testing.T) {
	ctx := context.Background()
	a := &Agent{kubeClient: k8sfake.NewSimpleClientset(), dynClient: fakeDyn()}
	if a.poolHasFreeSlot(ctx, "fn", "cfg") {
		t.Fatal("no record: reported a free slot, so the first replica would never be captured")
	}
	a = &Agent{kubeClient: k8sfake.NewSimpleClientset(poolRecord("cfg", "10.0.0.1", 111)), dynClient: fakeDyn()}
	if !a.poolHasFreeSlot(ctx, "fn", "cfg") {
		t.Fatal("slot 0 complete and unheld: no free slot")
	}
	a = &Agent{kubeClient: k8sfake.NewSimpleClientset(poolRecord("cfg", "10.0.0.1", 111),
		livePod("fn", "prefill-a", "ua"), instanceClaim("fn", "111", "prefill-a", "ua")), dynClient: fakeDyn()}
	if a.poolHasFreeSlot(ctx, "fn", "cfg") {
		t.Fatal("the only slot held: reported free, so the second replica would never be captured")
	}
	capturing := poolRecord("cfg", "10.0.0.1", 111)
	capturing.Data["state"] = criuGroupStateCapturing
	a = &Agent{kubeClient: k8sfake.NewSimpleClientset(capturing), dynClient: fakeDyn()}
	if a.poolHasFreeSlot(ctx, "fn", "cfg") {
		t.Fatal("a slot still capturing is not a free complete slot")
	}
}
