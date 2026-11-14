// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/fnstatus"
)

var histT0 = time.Date(2026, 11, 12, 17, 54, 0, 0, time.UTC)

// rankPod is rank ordinal of a restore of group key k in namespace ns, of
// Grove instance replica inst, created at created.
func rankPod(ordinal int, ns, k, inst string, created time.Time) *corev1.Pod {
	name := "r" + strconv.Itoa(ordinal)
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: ns, UID: types.UID(ns + "-" + inst + "-" + name + "-" + created.Format("150405")),
		CreationTimestamp: metav1.NewTime(created),
		Labels: map[string]string{fnstatus.GroupLabel: k,
			fnstatus.GroveScalingGroupLabel: "g", fnstatus.GroveScalingGroupReplicaLabel: inst},
		Annotations: map[string]string{fnstatus.GroupAnnotation: k, fnstatus.RestoreAnnotation: "ckpt-" + name,
			fnstatus.OrdinalAnnotation: strconv.Itoa(ordinal)},
	}}
}

func withCondition(p *corev1.Pod, c corev1.PodConditionType, at time.Time) *corev1.Pod {
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: c, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(at)})
	return p
}

// up is a rank scheduled at s and Ready at r.
func up(p *corev1.Pod, s, r time.Time) *corev1.Pod {
	return withCondition(withCondition(p, corev1.PodScheduled, s), corev1.PodReady, r)
}

func deleting(p *corev1.Pod, requested time.Time) *corev1.Pod {
	grace := int64(180)
	dt := metav1.NewTime(requested.Add(180 * time.Second))
	p.DeletionTimestamp, p.DeletionGracePeriodSeconds = &dt, &grace
	return p
}

func exitedAt(p *corev1.Pod, at time.Time) *corev1.Pod {
	p.Annotations[fnstatus.TeardownExitedAnnotation] = at.Format(time.RFC3339)
	return p
}

func entries(t *testing.T, tr *restoreTracker, ranks int) ([]fnstatus.Restore, []string) {
	t.Helper()
	got, over := tr.restores(func(string) int { return ranks })
	return got["k"], over["k"]
}

func onlyEntry(t *testing.T, tr *restoreTracker, ranks int) (fnstatus.Restore, []string) {
	t.Helper()
	got, over := entries(t, tr, ranks)
	if len(got) != 1 {
		t.Fatalf("restores %+v, want one of k", got)
	}
	return got[0], over
}

// A restore of two ranks followed from its pods: scale-up once both are
// Ready, teardown once both exited, forgotten once its pods are gone and
// written. A companion is no rank.
func TestRestoreTrackerFollowsARestore(t *testing.T) {
	tr := newRestoreTracker()
	r0 := rankPod(0, "ns", "k", "0", histT0)
	r1 := rankPod(1, "ns", "k", "0", histT0.Add(time.Second))
	etcd := rankPod(2, "ns", "k", "0", histT0)
	etcd.Annotations[fnstatus.CompanionAnnotation] = "true"
	for _, p := range []*corev1.Pod{r0, r1, etcd} {
		tr.observePod(p, false, false, histT0)
	}
	e, _ := onlyEntry(t, tr, 2)
	if e.Outcome != fnstatus.RestoreRestoring || e.Ranks != 2 || e.At != "2026-11-12T17:54:00Z" || e.Instance != "g/0" {
		t.Fatalf("a restore under way: %+v", e)
	}
	up(r0, histT0.Add(2*time.Second), histT0.Add(150*time.Second))
	r0.Annotations[fnstatus.RestoredAnnotation] = histT0.Add(140 * time.Second).Format(time.RFC3339)
	tr.observePod(r0, false, false, histT0)
	tr.observePod(withCondition(etcd, corev1.PodReady, histT0.Add(10*time.Second)), false, false, histT0)
	if e, _ := onlyEntry(t, tr, 2); e.Outcome != fnstatus.RestoreRestoring {
		t.Fatalf("one rank of two Ready (and the companion): %+v", e)
	}
	up(r1, histT0.Add(5*time.Second), histT0.Add(170*time.Second))
	r1.Annotations[fnstatus.RestoredAnnotation] = histT0.Add(145 * time.Second).Format(time.RFC3339)
	tr.observePod(r1, false, false, histT0)
	e, over := onlyEntry(t, tr, 2)
	if e.Outcome != fnstatus.RestoreRestored || e.ScaleUpSeconds != 168 || e.RestoredAt != "2026-11-12T17:56:25Z" || len(over) != 0 {
		t.Fatalf("both ranks Ready: %+v, over %v (want scale-up 170-2 = 168, restored at the last rank's)", e, over)
	}

	del := histT0.Add(time.Hour)
	tr.observePod(deleting(r0, del), false, false, del)
	tr.observePod(deleting(r1, del.Add(time.Second)), false, false, del)
	tr.observePod(exitedAt(r0, del.Add(100*time.Second)), false, false, del)
	if e, _ := onlyEntry(t, tr, 2); e.Teardown != "" {
		t.Fatalf("one rank of two exited: %+v", e)
	}
	tr.observePod(exitedAt(r1, del.Add(105*time.Second)), true, false, del)
	tr.observePod(r0, true, false, del)
	e, over = onlyEntry(t, tr, 2)
	if e.Teardown != fnstatus.TeardownReleased || e.TeardownSeconds != 105 || len(over) != 1 {
		t.Fatalf("both ranks exited and gone: %+v over %v (want 105 from the first delete request)", e, over)
	}
	tr.forget(over, del)
	if got, _ := entries(t, tr, 2); len(got) != 0 {
		t.Fatalf("a written restore whose pods are gone is still followed: %v", got)
	}
}

// A rank whose pod went without its workload exiting: killed. One whose
// delete was seen only as the watch's last known state: no verdict.
func TestRestoreTrackerKilledTeardown(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		tr := newRestoreTracker()
		r0 := up(rankPod(0, "ns", "k", "0", histT0), histT0, histT0.Add(time.Minute))
		r1 := up(rankPod(1, "ns", "k", "0", histT0), histT0, histT0.Add(time.Minute))
		tr.observePod(r0, false, false, histT0)
		tr.observePod(r1, false, false, histT0)
		del := histT0.Add(time.Hour)
		tr.observePod(exitedAt(deleting(r0, del), del.Add(time.Minute)), true, false, del)
		tr.observePod(deleting(r1, del), true, unknown, del.Add(3*time.Minute))
		e, _ := onlyEntry(t, tr, 2)
		want := fnstatus.TeardownKilled
		if unknown {
			want = ""
		}
		if e.Teardown != want || e.TeardownSeconds != 0 {
			t.Fatalf("delete seen only as last known state %v: %+v, want teardown %q, untimed", unknown, e, want)
		}
	}
}

// A lone rank has no teardown barrier and records no exit: no teardown
// verdict, not killed.
func TestRestoreTrackerLoneRankHasNoTeardown(t *testing.T) {
	tr := newRestoreTracker()
	r0 := up(rankPod(0, "ns", "k", "0", histT0), histT0, histT0.Add(time.Minute))
	tr.observePod(r0, false, false, histT0)
	tr.observePod(deleting(r0, histT0.Add(time.Hour)), true, false, histT0.Add(time.Hour))
	if e, _ := onlyEntry(t, tr, 1); e.Outcome != fnstatus.RestoreRestored || e.Teardown != "" {
		t.Fatalf("%+v, want restored with no teardown", e)
	}
}

// A rank whose restore failed and started cold in place: the restore is
// cold-started, with no scale-up or teardown.
func TestRestoreTrackerColdStartedRank(t *testing.T) {
	tr := newRestoreTracker()
	r0 := up(rankPod(0, "ns", "k", "0", histT0), histT0, histT0.Add(time.Minute))
	r1 := up(rankPod(1, "ns", "k", "0", histT0), histT0, histT0.Add(20*time.Minute))
	r1.Annotations[fnstatus.ColdStartedAnnotation] = "restore failed"
	tr.observePod(r0, false, false, histT0)
	tr.observePod(r1, false, false, histT0)
	tr.observePod(deleting(r0, histT0.Add(time.Hour)), true, false, histT0.Add(time.Hour))
	tr.observePod(deleting(r1, histT0.Add(time.Hour)), true, false, histT0.Add(time.Hour))
	if e, _ := onlyEntry(t, tr, 2); e.Outcome != fnstatus.RestoreColdStarted || e.ScaleUpSeconds != 0 || e.Teardown != "" {
		t.Fatalf("%+v, want cold-started, no times", e)
	}
}

// A rank that failed early and was replaced counts once, as its
// replacement: the restore is restored when the replacement is Ready, and
// its clean teardown is released, not killed.
func TestRestoreTrackerReplacedRank(t *testing.T) {
	tr := newRestoreTracker()
	r0 := up(rankPod(0, "ns", "k", "0", histT0), histT0, histT0.Add(time.Minute))
	r1 := withCondition(rankPod(1, "ns", "k", "0", histT0), corev1.PodScheduled, histT0)
	tr.observePod(r0, false, false, histT0)
	tr.observePod(r1, true, false, histT0.Add(30*time.Second)) // failed, disposed
	r1b := rankPod(1, "ns", "k", "0", histT0.Add(40*time.Second))
	tr.observePod(r1b, false, false, histT0.Add(40*time.Second))
	if e, _ := onlyEntry(t, tr, 2); e.Outcome != fnstatus.RestoreRestoring {
		t.Fatalf("the replacement not yet Ready: %+v", e)
	}
	tr.observePod(up(r1b, histT0.Add(41*time.Second), histT0.Add(3*time.Minute)), false, false, histT0)
	e, _ := onlyEntry(t, tr, 2)
	if e.Outcome != fnstatus.RestoreRestored || e.ScaleUpSeconds != 180 {
		t.Fatalf("%+v, want restored once the replacement is Ready (180 s)", e)
	}
	del := histT0.Add(time.Hour)
	tr.observePod(exitedAt(deleting(r0, del), del.Add(time.Minute)), true, false, del)
	tr.observePod(exitedAt(deleting(r1b, del), del.Add(time.Minute)), true, false, del)
	if e, _ := onlyEntry(t, tr, 2); e.Teardown != fnstatus.TeardownReleased {
		t.Fatalf("%+v, want released: the disposed rank is no rank of the instance", e)
	}
}

// A rank the agent replaces late in the restore (its pod given an address
// another pod of the instance had at capture, seen once the instance's
// earlier pods went) is still of that restore, however late: one entry,
// restored, timed from the replacement; not two gone. A rank replaced
// after it was Ready, and a restore after every pod was deleted, are not.
func TestRestoreTrackerLateReplacedRank(t *testing.T) {
	tr := newRestoreTracker()
	var ranks []*corev1.Pod
	for i := 0; i < 4; i++ {
		ranks = append(ranks, withCondition(rankPod(i, "ns", "k", "0", histT0), corev1.PodScheduled, histT0))
		tr.observePod(ranks[i], false, false, histT0)
	}
	replaced := histT0.Add(2*time.Minute + 40*time.Second)
	tr.observePod(deleting(ranks[0], replaced), false, false, replaced)
	tr.observePod(ranks[0], true, false, replaced.Add(2*time.Second))
	leader := rankPod(0, "ns", "k", "0", replaced.Add(4*time.Second))
	tr.observePod(leader, false, false, replaced.Add(4*time.Second))
	if e, _ := onlyEntry(t, tr, 4); e.Outcome != fnstatus.RestoreRestoring || e.Leader != string(leader.UID) {
		t.Fatalf("%+v, want one restore, still restoring, led by the replacement", e)
	}
	ready := histT0.Add(5 * time.Minute)
	tr.observePod(up(leader, replaced.Add(5*time.Second), ready), false, false, ready)
	for _, p := range ranks[1:] {
		tr.observePod(withCondition(p, corev1.PodReady, ready), false, false, ready)
	}
	e, _ := onlyEntry(t, tr, 4)
	if e.Outcome != fnstatus.RestoreRestored || e.ScaleUpSeconds != 300 || e.At != histT0.Format(time.RFC3339) {
		t.Fatalf("%+v, want restored at %s in 300 s", e, histT0.Format(time.RFC3339))
	}

	// Its replacement deleted once Ready, and a third rank 0 after: a
	// restore of its own, though the first rank 0 never was Ready.
	third := histT0.Add(30 * time.Minute)
	tr.observePod(deleting(up(leader, replaced.Add(5*time.Second), ready), third), true, false, third)
	tr.observePod(rankPod(0, "ns", "k", "0", third.Add(time.Second)), false, false, third)
	if got, _ := entries(t, tr, 4); len(got) != 2 {
		t.Fatalf("%d restores, want 2: the newest rank 0 was Ready, so the third is not the restore's", len(got))
	}

	// A rank replaced after it was Ready: a restore of its own.
	crashed := histT0.Add(time.Hour)
	tr.observePod(deleting(ranks[1], crashed), true, false, crashed)
	tr.observePod(rankPod(1, "ns", "k", "0", crashed.Add(time.Second)), false, false, crashed)
	if got, _ := entries(t, tr, 4); len(got) != 3 {
		t.Fatalf("%d restores, want 3: a rank replaced once Ready is not the restore's", len(got))
	}

	// Every pod deleted, then the next restore minutes later: its own.
	tr = newRestoreTracker()
	for i := 0; i < 2; i++ {
		p := withCondition(rankPod(i, "ns", "k", "0", histT0), corev1.PodScheduled, histT0)
		tr.observePod(deleting(p, histT0.Add(time.Minute)), false, false, histT0.Add(time.Minute))
	}
	tr.observePod(rankPod(0, "ns", "k", "0", histT0.Add(3*time.Minute)), false, false, histT0.Add(3*time.Minute))
	if got, _ := entries(t, tr, 2); len(got) != 2 {
		t.Fatalf("%d restores, want 2: no pod of the first goes on", len(got))
	}
}

// Deleted before every rank was Ready: gone, and no teardown. A restore
// whose pods all went is final even when fewer ranks were seen.
func TestRestoreTrackerGoneBeforeReady(t *testing.T) {
	tr := newRestoreTracker()
	r0 := withCondition(rankPod(0, "ns", "k", "0", histT0), corev1.PodScheduled, histT0)
	r1 := withCondition(rankPod(1, "ns", "k", "0", histT0), corev1.PodScheduled, histT0)
	tr.observePod(r0, false, false, histT0)
	tr.observePod(r1, false, false, histT0)
	tr.observePod(deleting(r0, histT0.Add(time.Minute)), false, false, histT0)
	if e, _ := onlyEntry(t, tr, 2); e.Outcome != fnstatus.RestoreRestoring {
		t.Fatalf("one rank of two deleted: %+v, want still restoring", e)
	}
	tr.observePod(deleting(r1, histT0.Add(time.Minute)), true, false, histT0)
	if e, _ := onlyEntry(t, tr, 2); e.Outcome != fnstatus.RestoreGone || e.Teardown != "" {
		t.Fatalf("%+v, want gone with no teardown", e)
	}
	tr = newRestoreTracker()
	tr.observePod(rankPod(0, "ns", "k", "0", histT0), true, false, histT0)
	if e, _ := onlyEntry(t, tr, 4); e.Outcome != fnstatus.RestoreGone {
		t.Fatalf("one of 4 ranks seen, all gone: %+v, want gone, not restoring for ever", e)
	}
}

// Restores apart in time, in other namespaces or of other instances are
// told apart; pods that restore nothing are not followed.
func TestRestoreTrackerTellsRestoresApart(t *testing.T) {
	tr := newRestoreTracker()
	tr.observePod(rankPod(0, "ns", "k", "0", histT0), false, false, histT0)
	tr.observePod(rankPod(0, "ns", "k", "0", histT0.Add(10*time.Minute)), false, false, histT0)
	tr.observePod(rankPod(0, "ns2", "k", "0", histT0), false, false, histT0)
	tr.observePod(rankPod(0, "ns", "k", "1", histT0), false, false, histT0) // another instance, scaled up together
	cold := rankPod(1, "ns", "k", "0", histT0)
	delete(cold.Annotations, fnstatus.RestoreAnnotation)
	tr.observePod(cold, false, false, histT0)
	if got, _ := entries(t, tr, 1); len(got) != 4 {
		t.Fatalf("%d restores, want 4 (apart in time, another namespace, another instance; the cold pod none)", len(got))
	}
	// A restore whose pods are all gone, and a new one created after,
	// within the window: two restores.
	tr = newRestoreTracker()
	tr.observePod(rankPod(0, "ns", "k", "0", histT0), true, false, histT0.Add(30*time.Second))
	tr.observePod(rankPod(0, "ns", "k", "0", histT0.Add(time.Minute)), false, false, histT0.Add(time.Minute))
	if got, _ := entries(t, tr, 1); len(got) != 2 {
		t.Fatalf("%d restores, want 2 (the second made after the first's pods went)", len(got))
	}
	// A pod of the same restore seen first after a later one: one restore,
	// dated by its first pod.
	tr = newRestoreTracker()
	tr.observePod(rankPod(1, "ns", "k", "0", histT0.Add(3*time.Second)), false, false, histT0)
	tr.observePod(rankPod(0, "ns", "k", "0", histT0), false, false, histT0)
	if got, _ := entries(t, tr, 2); len(got) != 1 || got[0].At != "2026-11-12T17:54:00Z" {
		t.Fatalf("restores %v, want one at its first pod's creation", got)
	}
}

func historyRecord() *corev1.ConfigMap {
	return fsRecord("nvsnap-criu-group-k", fsVersion, map[string]string{"state": "complete", "size": "3",
		"checkpoint.0": "c0", "checkpoint.1": "c1", "checkpoint.2": "c2", "companion.2": "true",
		"restores": "2026-11-12T17:56:30Z=40.0=another-instance 2026-11-12T17:56:20Z=144.5=sr-1-0-r0-175400", "agentBundle": "v0.2.91-rc9"})
}

// The objects keep the history: merged in on each pass, carried across
// passes and server restarts without counting a restore twice.
func TestFunctionStatus_History(t *testing.T) {
	ctx := context.Background()
	f, _, dc := functionStatus(historyRecord())
	f.Restores = newRestoreTracker()
	f.Restores.synced = func() bool { return true }
	r0 := up(rankPod(0, "sr-1", "k", "0", histT0), histT0, histT0.Add(168*time.Second))
	r1 := up(rankPod(1, "sr-1", "k", "0", histT0.Add(10*time.Second)), histT0, histT0.Add(160*time.Second))
	r0.Annotations[fnstatus.RestoredAnnotation] = "2026-11-12T17:56:25Z"
	r1.Annotations[fnstatus.RestoredAnnotation] = "2026-11-12T17:56:29Z"
	f.Restores.observePod(r0, false, false, histT0)
	f.Restores.observePod(r1, false, false, histT0)
	if err := f.reconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	h := historyOf(getFunction(t, dc, "v-1"))
	if h.Restores != 1 || len(h.Entries) != 1 {
		t.Fatalf("history %+v", h)
	}
	e := h.Entries[0]
	// The record's two stamps: 144.5 names this restore's rank 0 pod; the
	// other instance's is nearer its last rank's restore.
	if e.Outcome != fnstatus.RestoreRestored || e.Ranks != 2 || e.ScaleUpSeconds != 168 || e.RestoreSeconds != 144.5 || e.CapturedBy != "v0.2.91-rc9" {
		t.Fatalf("entry %+v", e)
	}

	del := histT0.Add(time.Hour)
	for _, p := range []*corev1.Pod{r0, r1} {
		f.Restores.observePod(exitedAt(deleting(p, del), del.Add(100*time.Second)), true, false, del)
	}
	if err := f.reconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := entries(t, f.Restores, 2); len(got) != 0 {
		t.Fatalf("a written restore is still followed: %v", got)
	}

	// A new server whose watch sees only the restore's later pod (its first
	// went first): the same restore, not a new one.
	f.Restores = newRestoreTracker()
	f.Restores.synced = func() bool { return true }
	f.Restores.observePod(rankPod(1, "sr-1", "k", "0", histT0.Add(10*time.Second)), false, false, histT0)
	dc.ClearActions()
	if err := f.reconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	u := getFunction(t, dc, "v-1")
	h = historyOf(u)
	if h.Restores != 1 || len(h.Entries) != 1 || h.Entries[0].Teardown != fnstatus.TeardownReleased || h.Entries[0].TeardownSeconds != 100 {
		t.Fatalf("history after a restart %+v", h)
	}
	if n := writes(dc); n != 0 {
		t.Fatalf("a pass with nothing new wrote %d times", n)
	}
	if v, _, _ := unstructured.NestedFloat64(u.Object, "status", "p50ScaleUpSeconds"); v != 168 {
		t.Fatalf("p50ScaleUpSeconds = %v", v)
	}

	// Before the watch has listed the pods nothing is merged.
	f.Restores = newRestoreTracker()
	f.Restores.synced = func() bool { return false }
	f.Restores.observePod(rankPod(0, "sr-1", "k", "0", histT0.Add(2*time.Hour)), false, false, histT0)
	if err := f.reconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if h := historyOf(getFunction(t, dc, "v-1")); h.Restores != 1 {
		t.Fatalf("merged before the watch listed: %+v", h)
	}
}

// A finished restore whose object's write failed (a conflict, swallowed
// for the next pass) is not forgotten: the next pass writes it.
func TestFunctionStatus_HistoryKeptUntilWritten(t *testing.T) {
	ctx := context.Background()
	f, _, dc := functionStatus(historyRecord())
	f.Restores = newRestoreTracker()
	f.Restores.synced = func() bool { return true }
	f.Now = func() time.Time { return histT0.Add(2 * time.Hour) } // the restores' times, not the wall clock
	if err := f.reconcileOnce(ctx); err != nil {                  // the object, no history yet
		t.Fatal(err)
	}
	r0 := up(rankPod(0, "sr-1", "k", "0", histT0), histT0, histT0.Add(time.Minute))
	r1 := up(rankPod(1, "sr-1", "k", "0", histT0), histT0, histT0.Add(time.Minute))
	for _, p := range []*corev1.Pod{r0, r1} {
		f.Restores.observePod(p, false, false, histT0)
		f.Restores.observePod(exitedAt(deleting(p, histT0.Add(time.Hour)), histT0.Add(time.Hour+time.Minute)), true, false, histT0)
	}
	conflicts := 1
	dc.PrependReactor("update", "nvsnapfunctions", func(k8stesting.Action) (bool, runtime.Object, error) {
		if conflicts > 0 {
			conflicts--
			return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "nvsnap.io", Resource: "nvsnapfunctions"}, "v-1", nil)
		}
		return false, nil, nil
	})
	if err := f.reconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := entries(t, f.Restores, 2); len(got) != 1 {
		t.Fatalf("a restore whose write conflicted was forgotten (%d followed)", len(got))
	}
	if err := f.reconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if h := historyOf(getFunction(t, dc, "v-1")); h.Restores != 1 || h.Entries[0].Teardown != fnstatus.TeardownReleased {
		t.Fatalf("history %+v", h)
	}
	if got, _ := entries(t, f.Restores, 2); len(got) != 0 {
		t.Fatalf("a written restore is still followed: %v", got)
	}
}

// With the NvsnapFunction CRD missing (a steady state), no object is
// written, but the restores followed still age out.
func TestFunctionStatus_RestoresAgeOutWithoutTheCRD(t *testing.T) {
	ctx := context.Background()
	f, _, dc := functionStatus(historyRecord())
	f.Restores = newRestoreTracker()
	f.Restores.synced = func() bool { return true }
	now := histT0
	f.Now = func() time.Time { return now }
	dc.PrependReactor("list", "nvsnapfunctions", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "nvsnap.io", Resource: "nvsnapfunctions"}, "")
	})
	f.Restores.observePod(up(rankPod(0, "sr-1", "k", "0", histT0), histT0, histT0.Add(time.Minute)), false, false, histT0)
	if err := f.reconcileOnce(ctx); !apierrors.IsNotFound(err) {
		t.Fatalf("err %v, want not found", err)
	}
	if got, _ := entries(t, f.Restores, 2); len(got) != 1 {
		t.Fatalf("a restore heard of just now was dropped (%d followed)", len(got))
	}
	now = histT0.Add(restoreForget + time.Minute)
	if err := f.reconcileOnce(ctx); !apierrors.IsNotFound(err) {
		t.Fatalf("err %v, want not found", err)
	}
	f.Restores.mu.Lock()
	n, m := len(f.Restores.batches), len(f.Restores.byPod)
	f.Restores.mu.Unlock()
	if n != 0 || m != 0 {
		t.Fatalf("restores not heard of for %v are still followed without the CRD (%d batches, %d pods)", restoreForget, n, m)
	}
}

// A restore whose pods all went (T+30 s) followed by its replacement
// (first pod T+45 s), within the match window: two restores, each with its
// own outcome, whichever pass first writes either.
func TestRestoreHistoryReplacementWithinTheWindow(t *testing.T) {
	for _, together := range []bool{false, true} {
		tr := newRestoreTracker()
		r0 := withCondition(rankPod(0, "ns", "k", "0", histT0), corev1.PodScheduled, histT0)
		r1 := withCondition(rankPod(1, "ns", "k", "0", histT0), corev1.PodScheduled, histT0)
		tr.observePod(r0, false, false, histT0)
		tr.observePod(r1, false, false, histT0)
		tr.observePod(r0, true, false, histT0.Add(30*time.Second))
		tr.observePod(r1, true, false, histT0.Add(30*time.Second))
		var h fnstatus.History
		if !together {
			first, over := entries(t, tr, 2)
			h = h.Merge(first)
			tr.forget(over, histT0.Add(31*time.Second))
		}
		n0 := up(rankPod(0, "ns", "k", "0", histT0.Add(45*time.Second)), histT0.Add(46*time.Second), histT0.Add(3*time.Minute))
		n1 := up(rankPod(1, "ns", "k", "0", histT0.Add(45*time.Second)), histT0.Add(46*time.Second), histT0.Add(3*time.Minute))
		n0.UID, n1.UID = "n0", "n1"
		tr.observePod(n0, false, false, histT0.Add(3*time.Minute))
		tr.observePod(n1, false, false, histT0.Add(3*time.Minute))
		got, _ := entries(t, tr, 2)
		h = h.Merge(got)
		if h.Restores != 2 || h.Gone != 1 || len(h.Entries) != 2 ||
			h.Entries[0].Outcome != fnstatus.RestoreGone || h.Entries[1].Outcome != fnstatus.RestoreRestored {
			t.Fatalf("together=%v: %+v, want the gone restore and its restored replacement", together, h)
		}
	}
}
