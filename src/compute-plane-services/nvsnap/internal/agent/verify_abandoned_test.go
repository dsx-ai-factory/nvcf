// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/fnstatus"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
)

// A capture being verified whose namespace is gone (deleted, or being
// deleted) is marked abandoned once, and its outcome counted once, however
// many sweeps and agents find it. Captures that can still be verified,
// and records in other states, are left alone.
func TestResolveAbandonedVerifies(t *testing.T) {
	c, _ := lifecycleAgent(t, "GB300-abandon")
	ctx := context.Background()
	kc := c.a.kubeClient
	for _, ns := range []*corev1.Namespace{
		{ObjectMeta: metav1.ObjectMeta{Name: "live"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "terminating", DeletionTimestamp: &metav1.Time{Time: time.Now()}, Finalizers: []string{"kubernetes"}}},
	} {
		if _, err := kc.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	started := time.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339)
	record := func(key string, d map[string]string) {
		t.Helper()
		d["size"], d["node.0"], d["startedAt"], d["checkpoint.0"] = "1", "n0", started, "c0"
		if _, err := kc.CoreV1().ConfigMaps(c.a.criuFailedNamespace()).Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: criuGroupConfigMapName(key), Labels: map[string]string{"app.kubernetes.io/managed-by": "nvsnap"}},
			Data:       d}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	record("gone", map[string]string{"state": criuGroupStateVerifying, "sourceNamespace": "deleted", "restores": "2026-11-07T17:55:33Z=5.6"})
	record("ending", map[string]string{"state": criuGroupStateVerifying, "sourceNamespace": "terminating"})
	record("live", map[string]string{"state": criuGroupStateVerifying, "sourceNamespace": "live"})
	record("done", map[string]string{"state": criuGroupStateComplete})
	record("failed", map[string]string{"state": criuGroupStateFailed, "sourceNamespace": "deleted", "reason": "CRIU dump failed"})

	labels := map[string]string{"outcome": metrics.CaptureAbandoned, "gpu": "GB300-abandon", "ranks": "1"}
	before := metricValue(t, metrics.CaptureOutcomes, labels)
	now := time.Now()
	c.resolveAbandonedVerifies(ctx, now)
	c.resolveAbandonedVerifies(ctx, now.Add(10*time.Minute)) // the next sweep
	other := &criuAutoRestorer{a: c.a}                       // another agent's
	other.resolveAbandonedVerifies(ctx, now.Add(20*time.Minute))
	other.abandonVerify(ctx, "gone", "deleted", now.Add(20*time.Minute))
	if got := metricValue(t, metrics.CaptureOutcomes, labels) - before; got != 2 {
		t.Fatalf("abandoned captures counted %v times, want 2 (once each)", got)
	}

	get := func(key string) map[string]string {
		t.Helper()
		cm, err := kc.CoreV1().ConfigMaps(c.a.criuFailedNamespace()).Get(ctx, criuGroupConfigMapName(key), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return cm.Data
	}
	for _, key := range []string{"gone", "ending"} {
		d := get(key)
		if d[groupAbandonedKey] != now.UTC().Format(time.RFC3339) || !strings.HasPrefix(d["reason"], "abandoned: namespace ") {
			t.Errorf("%s: not marked abandoned at the first sweep: %v", key, d)
		}
		if d["state"] != criuGroupStateVerifying || d["sourceNamespace"] == "" {
			t.Errorf("%s: the record's state changed: %v", key, d)
		}
	}
	for _, key := range []string{"live", "done", "failed"} {
		if d := get(key); d[groupAbandonedKey] != "" {
			t.Errorf("%s: marked abandoned: %v", key, d)
		}
	}
	if got := get("failed")["reason"]; got != "CRIU dump failed" {
		t.Errorf("a failed record's reason changed: %q", got)
	}

	// The next deployment still captures the configuration again.
	if !c.groupRecaptures(ctx, get("gone")) {
		t.Error("an abandoned capture is no longer captured again")
	}
	// nvsnap-server shows it as abandoned, with its reason, and admission
	// would not restore it anywhere else.
	cm, _ := kc.CoreV1().ConfigMaps(c.a.criuFailedNamespace()).Get(ctx, criuGroupConfigMapName("gone"), metav1.GetOptions{})
	cm.Labels[fnstatus.LabelNamespace] = "deleted"
	s := fnstatus.Project([]corev1.ConfigMap{*cm})["ns-deleted"]
	if s.State != fnstatus.StateAbandoned || s.Method != "" || !strings.HasPrefix(s.Reason, "abandoned: namespace deleted") {
		t.Errorf("status of an abandoned capture: state %q, method %q, reason %q", s.State, s.Method, s.Reason)
	}
}
