// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// gaugeValue is the value of the gauge series of c whose labels are want;
// ok false without one.
func gaugeValue(t *testing.T, c prometheus.Collector, want map[string]string) (float64, bool) {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			match := len(got) == len(want)
			for k, v := range want {
				match = match && got[k] == v
			}
			if match {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// histogramSum is the sum of every observation of h's series.
func histogramSum(t *testing.T, h prometheus.Collector) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(h)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	sum := 0.0
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			sum += m.GetHistogram().GetSampleSum()
		}
	}
	return sum
}

// The scale-up time is from the pod being scheduled, not created: a pod
// that waited for its GPUs did not take that long to start.
func TestObservePodReadyRecordsTheTimeFromScheduled(t *testing.T) {
	c, rec := lifecycleAgent(t, "GB300-start-cold")
	labels := map[string]string{"method": metrics.MethodCold, "gpu": "GB300-start-cold", "ranks": "2-4"}
	before := metricValue(t, metrics.PodStartSeconds, labels)
	pod := admittedPod(0, 4, 3*time.Minute)
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(pod.CreationTimestamp.Add(2 * time.Minute))})
	c.observePodReady(context.Background(), pod)
	if got := metricValue(t, metrics.PodStartSeconds, labels) - before; got != 1 {
		t.Fatalf("scheduled-to-Ready recorded %v times, want once", got)
	}
	if e := event(rec); !strings.Contains(e, "3m0s after creation, 1m0s after it was scheduled") {
		t.Fatalf("event %q does not give both times", e)
	}
	// Not scheduled as far as the status says: no start time, and the
	// event says so by leaving it out.
	pod2 := admittedPod(1, 4, time.Minute)
	pod2.UID = "other"
	c.observePodReady(context.Background(), pod2)
	if got := metricValue(t, metrics.PodStartSeconds, labels) - before; got != 1 {
		t.Fatalf("a pod with no scheduled time recorded a start (%v)", got)
	}
	if e := event(rec); strings.Contains(e, "scheduled") || !strings.Contains(e, "after creation") {
		t.Fatalf("event %q", e)
	}
}

// A released teardown is timed from the delete request; one whose grace
// period runs out is counted as a deadline.
func TestDriveTeardownRecordsItsOutcome(t *testing.T) {
	root := t.TempDir()
	old, oldPoll := cachedirPodsRoot, teardownPoll
	cachedirPodsRoot, teardownPoll = root, 20*time.Millisecond
	t.Cleanup(func() { cachedirPodsRoot, teardownPoll = old, oldPoll })
	outcome := func(o string) float64 {
		return metricValue(t, metrics.TeardownOutcomes, map[string]string{"outcome": o})
	}
	released, deadline := outcome(metrics.TeardownReleased), outcome(metrics.TeardownDeadline)
	timed := metricValue(t, metrics.TeardownSeconds, nil)
	timedSum := histogramSum(t, metrics.TeardownSeconds)

	arm := func(p *corev1.Pod, exited bool) string {
		dir := teardownWorkDir(p.UID)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := armTeardownHold(dir); err != nil {
			t.Fatal(err)
		}
		if exited {
			if err := os.WriteFile(filepath.Join(dir, webhook.TeardownExitedFile), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}

	// Every rank exited: released, and timed from the delete request (the
	// deletion time less the grace period the API server sets with it).
	self := terminating(restoredRank(0, 2), time.Now().Add(time.Minute))
	grace := int64(90)
	self.DeletionGracePeriodSeconds = &grace
	peer := exitedRank(terminating(restoredRank(1, 2), time.Now().Add(time.Minute)))
	c := &criuAutoRestorer{a: &Agent{kubeClient: fake.NewSimpleClientset(self, peer), log: logrus.New()}}
	arm(self, true)
	c.driveTeardown(t.Context(), self, logrus.NewEntry(quietLog()))
	if got := outcome(metrics.TeardownReleased) - released; got != 1 {
		t.Fatalf("released teardowns +%v, want +1", got)
	}
	if got := metricValue(t, metrics.TeardownSeconds, nil) - timed; got != 1 {
		t.Fatalf("teardown times +%v, want +1", got)
	}
	// Timed from the delete request (30 s ago), not the deletion time.
	if took := histogramSum(t, metrics.TeardownSeconds) - timedSum; took < 29 || took > 40 {
		t.Fatalf("the teardown took %.1fs by the metric, want about 30 (from the delete request)", took)
	}

	// The grace period runs out with the peer still running: the kubelet
	// kills the pod and removes its work dir, hold and all. A deadline,
	// not timed. The deadline is short here so the test does not wait.
	self = terminating(restoredRank(0, 2), time.Now().Add(-29*time.Second))
	self.UID = "killed-0"
	peer = terminating(restoredRank(1, 2), time.Now().Add(-29*time.Second))
	c = &criuAutoRestorer{a: &Agent{kubeClient: fake.NewSimpleClientset(self, peer), log: logrus.New()}}
	dir := arm(self, true)
	go func() { time.Sleep(200 * time.Millisecond); _ = os.RemoveAll(dir) }()
	c.driveTeardown(t.Context(), self, logrus.NewEntry(quietLog()))
	if got := outcome(metrics.TeardownDeadline) - deadline; got != 1 {
		t.Fatalf("a rank killed with its work dir removed: deadline teardowns +%v, want +1", got)
	}
	if got := metricValue(t, metrics.TeardownSeconds, nil) - timed; got != 1 {
		t.Fatalf("a killed rank's teardown was timed (+%v)", got)
	}

	// The hold still on at the deadline (the pod stuck terminating): a
	// deadline too.
	self = terminating(restoredRank(0, 2), time.Now().Add(-29*time.Second))
	self.UID = "stuck-0"
	c = &criuAutoRestorer{a: &Agent{kubeClient: fake.NewSimpleClientset(self, peer), log: logrus.New()}}
	arm(self, true)
	c.driveTeardown(t.Context(), self, logrus.NewEntry(quietLog()))
	if got := outcome(metrics.TeardownDeadline) - deadline; got != 2 {
		t.Fatalf("a rank held past its deadline: deadline teardowns +%v, want +2", got)
	}

	// An agent that first drives a rank already past its deadline (it
	// restarted) does not count it: the agent before it did, or nobody drove it.
	self = terminating(restoredRank(0, 2), time.Now().Add(-31*time.Second))
	self.UID = "late-0"
	c = &criuAutoRestorer{a: &Agent{kubeClient: fake.NewSimpleClientset(self, peer), log: logrus.New()}}
	arm(self, true)
	c.driveTeardown(t.Context(), self, logrus.NewEntry(quietLog()))
	if got := outcome(metrics.TeardownDeadline) - deadline; got != 2 {
		t.Fatalf("a rank first driven past its deadline was counted (+%v, want +2)", got)
	}

	// The agent stopping is neither.
	self = terminating(restoredRank(0, 2), time.Now().Add(time.Minute))
	self.UID = "stopped-0"
	c = &criuAutoRestorer{a: &Agent{kubeClient: fake.NewSimpleClientset(self, peer), log: logrus.New()}}
	arm(self, false)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c.driveTeardown(ctx, self, logrus.NewEntry(quietLog()))
	if outcome(metrics.TeardownReleased)-released != 1 || outcome(metrics.TeardownDeadline)-deadline != 2 {
		t.Fatal("a teardown the agent stopped driving was counted")
	}
}

func captureRecord(name string, created time.Time, data map[string]string) corev1.ConfigMap {
	return corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.NewTime(created)}, Data: data}
}

func TestTallyCaptureRecords(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	ts := func(d time.Duration) string { return now.Add(-d).Format(time.RFC3339) }
	cms := []corev1.ConfigMap{
		// verified, restored after: used
		captureRecord(criuGroupConfigMapPrefix+"a", now, map[string]string{"state": "complete", "agentBundle": "v2", "startedAt": ts(5 * time.Hour),
			"verifiedAt": ts(4 * time.Hour), "restores": ts(4*time.Hour+time.Minute) + "=120.0 " + ts(time.Hour) + "=118.0"}),
		// verified, its only restore the verifying one: unused
		captureRecord(criuGroupConfigMapPrefix+"b", now, map[string]string{"state": "complete", "agentBundle": "v2", "startedAt": ts(2 * time.Hour),
			"verifiedAt": ts(time.Hour), "restores": ts(time.Hour+time.Minute) + "=121.0"}),
		// verified by an agent that kept no verification time: not told apart
		captureRecord(criuGroupConfigMapPrefix+"c", now, map[string]string{"state": "complete", "agentBundle": "v1"}),
		// stuck verifying for a day; its age from startedAt
		captureRecord(criuGroupConfigMapPrefix+"d", now, map[string]string{"state": "verifying", "agentBundle": "v1", "startedAt": ts(24 * time.Hour)}),
		// its namespace went before it was verified
		captureRecord(criuGroupConfigMapPrefix+"g", now, map[string]string{"state": "verifying", "agentBundle": "v1", "startedAt": ts(48 * time.Hour),
			groupAbandonedKey: ts(47 * time.Hour)}),
		// an alias: no bundle of its own; its age from its creation
		captureRecord(criuGroupConfigMapPrefix+"e", now.Add(-3*time.Hour), map[string]string{"state": "alias", "alias": "a", "agentBundle": "v2"}),
		// no state, no bundle
		captureRecord(criuGroupConfigMapPrefix+"f", now.Add(-time.Minute), map[string]string{}),
		// not a capture record
		captureRecord("nvsnap-bundle-digests", now.Add(-48*time.Hour), map[string]string{"state": "complete"}),
	}
	got := tallyCaptureRecords(cms, now)
	want := map[[2]string]int{{"complete", "v2"}: 2, {"complete", "v1"}: 1, {"verifying", "v1"}: 1, {"alias", ""}: 1, {"unknown", "unknown"}: 1, {"abandoned", "v1"}: 1}
	if len(got.records) != len(want) {
		t.Fatalf("records %v, want %v", got.records, want)
	}
	for k, n := range want {
		if got.records[k] != n {
			t.Errorf("records%v = %d, want %d", k, got.records[k], n)
		}
	}
	if got.oldest["verifying"] != 24*time.Hour || got.oldest["abandoned"] != 48*time.Hour || got.oldest["alias"] != 3*time.Hour || got.oldest["complete"] != 5*time.Hour {
		t.Errorf("oldest %v", got.oldest)
	}
	if got.unrestored != 1 {
		t.Errorf("unrestored %d, want 1 (b)", got.unrestored)
	}
}

// The export replaces the series in place: a state or bundle gone from
// the records goes from the metrics, the rest are never absent.
func TestCaptureInventoryExport(t *testing.T) {
	inv := &captureInventory{}
	t.Cleanup(inv.clear)
	now := time.Unix(1_800_000_000, 0)
	// Not leading: nothing is exported.
	inv.export(recordTally{records: map[[2]string]int{{"complete", "v0"}: 1}}, now)
	if _, ok := gaugeValue(t, metrics.CaptureRecords, map[string]string{"state": "complete", "bundle": "v0"}); ok {
		t.Fatal("an agent that does not lead exported the inventory")
	}
	inv.lead()
	inv.export(recordTally{records: map[[2]string]int{{"complete", "v1"}: 3, {"verifying", "v1"}: 1},
		oldest: map[string]time.Duration{"complete": time.Hour, "verifying": time.Minute}, unrestored: 2}, now)
	if v, ok := gaugeValue(t, metrics.CaptureRecords, map[string]string{"state": "complete", "bundle": "v1"}); !ok || v != 3 {
		t.Fatalf("complete/v1 = %v %v", v, ok)
	}
	inv.export(recordTally{records: map[[2]string]int{{"complete", "v2"}: 1},
		oldest: map[string]time.Duration{"complete": 2 * time.Minute}}, now)
	if _, ok := gaugeValue(t, metrics.CaptureRecords, map[string]string{"state": "complete", "bundle": "v1"}); ok {
		t.Fatal("a bundle with no records left kept its series")
	}
	if _, ok := gaugeValue(t, metrics.CaptureRecordOldestSeconds, map[string]string{"state": "verifying"}); ok {
		t.Fatal("a state with no records left kept its oldest age")
	}
	if v, _ := gaugeValue(t, metrics.CaptureRecordOldestSeconds, map[string]string{"state": "complete"}); v != 120 {
		t.Fatalf("oldest complete = %v, want 120", v)
	}
	if v, _ := gaugeValue(t, metrics.CaptureRecordsUnrestored, map[string]string{}); v != 0 {
		t.Fatalf("unrestored = %v, want 0", v)
	}
	inv.clear()
	if _, ok := gaugeValue(t, metrics.CaptureRecords, map[string]string{"state": "complete", "bundle": "v2"}); ok {
		t.Fatal("an agent no longer the leader still exports the inventory")
	}
	// A scan that ends after leadership was lost exports nothing.
	inv.export(recordTally{records: map[[2]string]int{{"complete", "v3"}: 1}}, now)
	if _, ok := gaugeValue(t, metrics.CaptureRecords, map[string]string{"state": "complete", "bundle": "v3"}); ok {
		t.Fatal("a scan finished after leadership was lost exported its tally")
	}
	if v, _ := gaugeValue(t, metrics.InventoryScanTimestamp, map[string]string{}); v != 0 {
		t.Fatalf("scan time %v after leadership was lost, want 0", v)
	}
}

// One agent takes the inventory Lease and exports the records' inventory;
// stopping it gives the Lease up and drops its export.
func TestCaptureInventoryLeaderExports(t *testing.T) {
	ns := "nvsnap-system"
	kc := fake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: criuGroupConfigMapPrefix + "x", Namespace: ns},
			Data: map[string]string{"state": "complete", "agentBundle": "v-inv"}})
	a := &Agent{kubeClient: kc, log: quietLog(), config: Config{NodeName: "node-a"}}
	ctx, cancel := context.WithCancel(t.Context())
	a.startCaptureInventory(ctx)
	labels := map[string]string{"state": "complete", "bundle": "v-inv"}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if v, ok := gaugeValue(t, metrics.CaptureRecords, labels); ok && v == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the agent never exported the inventory")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if v, _ := gaugeValue(t, metrics.InventoryLeader, map[string]string{}); v != 1 {
		t.Fatalf("inventory leader = %v, want 1", v)
	}
	if v, _ := gaugeValue(t, metrics.InventoryScanTimestamp, map[string]string{}); v == 0 {
		t.Fatal("no scan time exported")
	}
	lease, err := kc.CoordinationV1().Leases(ns).Get(t.Context(), captureInventoryLease, metav1.GetOptions{})
	if err != nil || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "node-a" {
		t.Fatalf("lease %v, %v: want held by node-a", lease, err)
	}
	cancel()
	deadline = time.Now().Add(10 * time.Second)
	for {
		if _, ok := gaugeValue(t, metrics.CaptureRecords, labels); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the agent kept exporting the inventory after it stopped")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if v, _ := gaugeValue(t, metrics.InventoryLeader, map[string]string{}); v != 0 {
		t.Fatalf("inventory leader = %v after stopping, want 0", v)
	}
}
