// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

var teardownT0 = time.Date(2026, 11, 11, 22, 0, 0, 0, time.UTC)

// restoredRank is rank ordinal of a restored group of size ranks (one
// Grove instance), restored at teardownT0.
func restoredRank(ordinal, size int) *corev1.Pod {
	name := "rank-" + strconv.Itoa(ordinal)
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "ns", UID: types.UID("uid-" + name),
		Labels: map[string]string{lwsGroupKeyLabel: "inst"},
		Annotations: map[string]string{
			webhook.CRIUGroupAnnotation:        "g",
			webhook.CRIUGroupOrdinalAnnotation: strconv.Itoa(ordinal),
			webhook.CRIUGroupSizeAnnotation:    strconv.Itoa(size),
			webhook.CRIURestoreAnnotation:      "ckpt-" + name,
			webhook.CRIURestoredAnnotation:     teardownT0.Format(time.RFC3339),
		},
	}}
}

func terminating(p *corev1.Pod, at time.Time) *corev1.Pod {
	t := metav1.NewTime(at)
	p.DeletionTimestamp = &t
	return p
}

func exitedRank(p *corev1.Pod) *corev1.Pod {
	p.Annotations[webhook.TeardownExitedAnnotation] = teardownT0.Format(time.RFC3339)
	return p
}

func podList(ps ...*corev1.Pod) []corev1.Pod {
	out := make([]corev1.Pod, 0, len(ps))
	for _, p := range ps {
		out = append(out, *p)
	}
	return out
}

func TestTeardownRank(t *testing.T) {
	if !teardownRank(restoredRank(0, 4)) {
		t.Error("a restored rank of a group of 4 is not held")
	}
	lone := restoredRank(0, 1)
	if teardownRank(lone) {
		t.Error("a lone restore is held: it shares no fabric memory")
	}
	notRestored := restoredRank(1, 4)
	delete(notRestored.Annotations, webhook.CRIURestoredAnnotation)
	if teardownRank(notRestored) {
		t.Error("a placeholder never restored is held")
	}
	comp := restoredRank(2, 4)
	comp.Annotations[webhook.CRIUGroupCompanionAnnotation] = "true"
	if teardownRank(comp) {
		t.Error("a companion (no GPU state) is held")
	}
	cold := restoredRank(3, 4)
	cold.Annotations[webhook.ColdStartedAnnotation] = "now"
	if teardownRank(cold) {
		t.Error("a rank started cold is held")
	}
	plain := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"}}
	if teardownRank(plain) {
		t.Error("a pod nvsnap did not restore is held")
	}
}

func TestTeardownReleased(t *testing.T) {
	del := teardownT0.Add(time.Hour)
	self := terminating(restoredRank(0, 4), del)
	peers := func() []*corev1.Pod {
		return []*corev1.Pod{terminating(restoredRank(1, 4), del), terminating(restoredRank(2, 4), del), terminating(restoredRank(3, 4), del)}
	}

	t.Run("own workload still running holds", func(t *testing.T) {
		p := peers()
		if teardownReleased(self, podList(self, exitedRank(p[0]), exitedRank(p[1]), exitedRank(p[2])), false) {
			t.Error("released before this rank's own workload exited")
		}
	})
	t.Run("every rank exited releases", func(t *testing.T) {
		p := peers()
		if !teardownReleased(self, podList(self, exitedRank(p[0]), exitedRank(p[1]), exitedRank(p[2])), true) {
			t.Error("held after every rank exited")
		}
	})
	t.Run("one rank still running holds", func(t *testing.T) {
		p := peers()
		if teardownReleased(self, podList(self, exitedRank(p[0]), p[1], exitedRank(p[2])), true) {
			t.Error("released while a rank still runs its workload")
		}
	})
	t.Run("a rank whose pod is gone counts as exited", func(t *testing.T) {
		p := peers()
		if !teardownReleased(self, podList(self, exitedRank(p[0]), exitedRank(p[2])), true) {
			t.Error("held for a rank whose pod is gone")
		}
	})
	t.Run("a rank not yet deleted holds", func(t *testing.T) {
		// Deleted one after another: the ranks of this restore are all held
		// for, deleted yet or not.
		p := peers()
		alive := restoredRank(1, 4)
		if teardownReleased(self, podList(self, alive, exitedRank(p[1]), exitedRank(p[2])), true) {
			t.Error("released while a rank of the same restore runs, not yet deleted")
		}
	})
	t.Run("replacements restored after the delete do not hold", func(t *testing.T) {
		p := peers()
		repl := restoredRank(1, 4)
		repl.Name, repl.UID = "rank-1-new", "uid-rank-1-new"
		repl.Annotations[webhook.CRIURestoredAnnotation] = del.Add(time.Minute).Format(time.RFC3339)
		if !teardownReleased(self, podList(self, exitedRank(p[0]), exitedRank(p[1]), exitedRank(p[2]), repl), true) {
			t.Error("held for a replacement restored after this rank was deleted")
		}
	})
	t.Run("a replacement restored within the grace period does not hold", func(t *testing.T) {
		// DeletionTimestamp is the request plus the grace period: a
		// replacement restored after the request, before that deadline, is
		// another restore's.
		grace := int64(180)
		req := teardownT0.Add(time.Hour)
		self := terminating(restoredRank(0, 2), req.Add(time.Duration(grace)*time.Second))
		self.DeletionGracePeriodSeconds = &grace
		old := terminating(restoredRank(1, 2), req.Add(time.Duration(grace)*time.Second))
		old.DeletionGracePeriodSeconds = &grace
		repl := restoredRank(1, 2)
		repl.Name, repl.UID = "rank-1-new", "uid-rank-1-new"
		repl.Annotations[webhook.CRIURestoredAnnotation] = req.Add(time.Minute).Format(time.RFC3339)
		if !teardownReleased(self, podList(self, exitedRank(old), repl), true) {
			t.Error("held for a replacement restored after the delete was requested")
		}
		running := terminating(restoredRank(1, 2), req.Add(time.Duration(grace)*time.Second))
		if teardownReleased(self, podList(self, running), true) {
			t.Error("released while a rank restored before the request still runs")
		}
	})
	t.Run("other instances and companions do not hold", func(t *testing.T) {
		p := peers()
		other := restoredRank(1, 4)
		other.Name, other.UID = "other", "uid-other"
		other.Labels[lwsGroupKeyLabel] = "other-inst"
		comp := restoredRank(3, 4)
		comp.Name, comp.UID = "comp", "uid-comp"
		comp.Annotations[webhook.CRIUGroupCompanionAnnotation] = "true"
		if !teardownReleased(self, podList(self, exitedRank(p[0]), exitedRank(p[1]), exitedRank(p[2]), other, comp), true) {
			t.Error("held for a pod of another instance or a companion")
		}
	})
}

// The hold is a regular file the agent creates on the pod's work volume; a
// link the workload put there is refused, not followed.
func TestTeardownHoldFile(t *testing.T) {
	root := t.TempDir()
	old := cachedirPodsRoot
	cachedirPodsRoot = root
	t.Cleanup(func() { cachedirPodsRoot = old })
	uid := types.UID("u1")
	dir := teardownWorkDir(uid)
	if err := armTeardownHold(dir); err == nil {
		t.Fatal("armed a hold on a pod without the work volume")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := armTeardownHold(dir); err != nil {
		t.Fatal(err)
	}
	if err := armTeardownHold(dir); err != nil {
		t.Fatalf("arming again: %v", err)
	}
	if !teardownHeld(dir) {
		t.Fatal("no hold after arming")
	}
	if teardownExited(dir) {
		t.Fatal("exited before the placeholder said so")
	}
	if err := os.WriteFile(filepath.Join(dir, webhook.TeardownExitedFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if !teardownExited(dir) {
		t.Fatal("the placeholder's report is not seen")
	}
	if err := liftTeardownHold(dir); err != nil || teardownHeld(dir) {
		t.Fatalf("lift: %v, held %v", err, teardownHeld(dir))
	}
	if err := liftTeardownHold(dir); err != nil {
		t.Fatalf("lifting again: %v", err)
	}

	target := filepath.Join(t.TempDir(), "host-file")
	other := teardownWorkDir("u2")
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(other, webhook.TeardownHoldFile)); err != nil {
		t.Fatal(err)
	}
	if err := armTeardownHold(other); err == nil {
		t.Error("armed a hold through a link the workload placed")
	}
	if _, err := os.Stat(target); err == nil {
		t.Error("arming followed the link and created its target")
	}
}

// A terminating rank's agent reports its workload gone once the placeholder
// says so, and lifts the hold only once the last other rank has exited.
func TestDriveTeardownLiftsTheHoldOnceEveryRankHasExited(t *testing.T) {
	root := t.TempDir()
	old, oldPoll := cachedirPodsRoot, teardownPoll
	cachedirPodsRoot, teardownPoll = root, 20*time.Millisecond
	t.Cleanup(func() { cachedirPodsRoot, teardownPoll = old, oldPoll })

	del := time.Now()
	self := terminating(restoredRank(0, 2), del)
	peer := terminating(restoredRank(1, 2), del)
	kc := fake.NewSimpleClientset(self, peer)
	c := &criuAutoRestorer{a: &Agent{kubeClient: kc, log: logrus.New()}}
	dir := teardownWorkDir(self.UID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := armTeardownHold(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, webhook.TeardownExitedFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { c.driveTeardown(t.Context(), self, logrus.NewEntry(logrus.New())); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := kc.CoreV1().Pods("ns").Get(t.Context(), self.Name, metav1.GetOptions{})
		if err == nil && got.Annotations[webhook.TeardownExitedAnnotation] != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the rank's workload was never reported gone")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if !teardownHeld(dir) {
		t.Fatal("the hold was lifted while the other rank still runs")
	}
	p, err := kc.CoreV1().Pods("ns").Get(t.Context(), peer.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	exitedRank(p)
	if _, err := kc.CoreV1().Pods("ns").Update(t.Context(), p, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the hold was not lifted after every rank exited")
	}
	if teardownHeld(dir) {
		t.Error("the driver returned with the hold still on")
	}
}

// A rank the agent first sees already terminating (it started, or re-listed,
// meanwhile) is held too, while its placeholder has not yet reported its
// workload gone.
func TestConsiderTeardownHoldsARankFirstSeenTerminating(t *testing.T) {
	root := t.TempDir()
	old := cachedirPodsRoot
	cachedirPodsRoot = root
	t.Cleanup(func() { cachedirPodsRoot = old })

	self := terminating(restoredRank(0, 2), time.Now().Add(time.Minute))
	kc := fake.NewSimpleClientset(self)
	c := &criuAutoRestorer{a: &Agent{kubeClient: kc, log: logrus.New()}}
	dir := teardownWorkDir(self.UID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c.considerTeardown(ctx, self)
	if !teardownHeld(dir) {
		t.Fatal("a restored rank first seen terminating was not held")
	}

	// Its placeholder already reported the workload gone, with no hold:
	// nothing to hold any more.
	other := terminating(restoredRank(1, 2), time.Now().Add(time.Minute))
	odir := teardownWorkDir(other.UID)
	if err := os.MkdirAll(odir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(odir, webhook.TeardownExitedFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	c.considerTeardown(ctx, other)
	if teardownHeld(odir) {
		t.Error("held a rank whose placeholder had already reported its workload gone")
	}
}

// The exit report is bound to the rank's UID: a pod of the same name
// admitted after a force delete is not marked exited.
func TestDriveTeardownDoesNotMarkAPodOfTheSameName(t *testing.T) {
	root := t.TempDir()
	old, oldPoll := cachedirPodsRoot, teardownPoll
	cachedirPodsRoot, teardownPoll = root, 20*time.Millisecond
	t.Cleanup(func() { cachedirPodsRoot, teardownPoll = old, oldPoll })

	self := terminating(restoredRank(0, 2), time.Now().Add(time.Minute))
	repl := restoredRank(0, 2)
	repl.UID = "uid-rank-0-new"
	kc := fake.NewSimpleClientset(repl)
	c := &criuAutoRestorer{a: &Agent{kubeClient: kc, log: logrus.New()}}
	dir := teardownWorkDir(self.UID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := armTeardownHold(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, webhook.TeardownExitedFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	c.driveTeardown(ctx, self, logrus.NewEntry(logrus.New()))
	got, err := kc.CoreV1().Pods("ns").Get(t.Context(), repl.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Annotations[webhook.TeardownExitedAnnotation] != "" {
		t.Error("the exit report marked a different pod of the same name")
	}
}
