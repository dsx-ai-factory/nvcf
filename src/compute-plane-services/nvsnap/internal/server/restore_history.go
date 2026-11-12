// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

// The restore history of each function version
// (docs/design/FUNCTION-CACHE-STATUS.md, History). The webhook labels every
// pod it admits into a CRIU group with the group's key; the server watches
// those pods and follows each restore of an instance from its GPU ranks'
// pods: when they were scheduled, restored and Ready, when they were
// deleted, and when each rank's workload exited. FunctionStatus merges the
// restores into the objects on each pass.

import (
	"context"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/fnstatus"
)

// restoreForget drops a restore the server has heard nothing of for this
// long: its pods were deleted while the watch was down.
const restoreForget = 6 * time.Hour

// restoreTracker follows the restores of the pods it is shown.
type restoreTracker struct {
	mu      sync.Mutex
	batches map[string]*restoreBatch // namespace/key/instance/first creation -> restore
	byPod   map[types.UID]*restoreBatch
	synced  func() bool
}

// restoreBatch is one restore of one instance: its GPU ranks' pods.
type restoreBatch struct {
	ns, key, instance string
	created           time.Time
	seen              time.Time
	pods              map[types.UID]*restorePod
}

type restorePod struct {
	ordinal                     int
	created                     time.Time
	scheduled, ready, deleteReq time.Time
	restored, exited, goneAt    time.Time
	coldStarted                 bool
	gone                        bool
	// goneUnknown: its delete was seen only as the watch's last known
	// state, which may predate its exit being recorded.
	goneUnknown bool
}

func newRestoreTracker() *restoreTracker {
	return &restoreTracker{batches: map[string]*restoreBatch{}, byPod: map[types.UID]*restoreBatch{}}
}

// watch follows the labelled pods of the cluster until ctx ends.
func (t *restoreTracker) watch(ctx context.Context, kc kubernetes.Interface) {
	f := informers.NewSharedInformerFactoryWithOptions(kc, 10*time.Minute,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = fnstatus.GroupLabel }))
	inf := f.Core().V1().Pods().Informer()
	_, _ = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { t.observe(o, false) },
		UpdateFunc: func(_, o any) { t.observe(o, false) },
		DeleteFunc: func(o any) { t.observe(o, true) },
	})
	t.mu.Lock()
	t.synced = inf.HasSynced
	t.mu.Unlock()
	f.Start(ctx.Done())
}

// ready reports whether the watch has listed the pods: before, a pass
// would read restores as missing pods.
func (t *restoreTracker) ready() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.synced != nil && t.synced()
}

func (t *restoreTracker) observe(o any, deleted bool) {
	unknown := false
	if d, ok := o.(cache.DeletedFinalStateUnknown); ok {
		o, deleted, unknown = d.Obj, true, true
	}
	pod, ok := o.(*corev1.Pod)
	if !ok {
		return
	}
	t.observePod(pod, deleted, unknown, time.Now())
}

// instanceOf tells the instances of one configuration in one namespace
// apart, as the agent does (internal/agent/criu_group.go sameInstance): the
// Grove scaling group replica, else the LeaderWorkerSet group, else the
// controller.
func instanceOf(pod *corev1.Pod) string {
	if g, r := pod.Labels[fnstatus.GroveScalingGroupLabel], pod.Labels[fnstatus.GroveScalingGroupReplicaLabel]; g != "" && r != "" {
		return g + "/" + r
	}
	if g := pod.Labels[fnstatus.LWSGroupKeyLabel]; g != "" {
		return g
	}
	if c := metav1.GetControllerOf(pod); c != nil {
		return string(c.UID)
	}
	return ""
}

// observePod records what pod says of its restore at now; deleted when it
// is gone, unknown when only its last known state was seen.
func (t *restoreTracker) observePod(pod *corev1.Pod, deleted, unknown bool, now time.Time) {
	a := pod.Annotations
	key := a[fnstatus.GroupAnnotation]
	if key == "" || a[fnstatus.RestoreAnnotation] == "" || a[fnstatus.CompanionAnnotation] == "true" {
		return // not a GPU rank's restore (an instance's cold pods are labelled too; companions are not ranks)
	}
	ordinal, err := strconv.Atoi(a[fnstatus.OrdinalAnnotation])
	if err != nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	b := t.byPod[pod.UID]
	if b == nil {
		b = t.batchFor(pod.Namespace, key, instanceOf(pod), pod.CreationTimestamp.Time)
		t.byPod[pod.UID] = b
	}
	b.seen = now
	p := b.pods[pod.UID]
	if p == nil {
		p = &restorePod{ordinal: ordinal, created: pod.CreationTimestamp.Time}
		b.pods[pod.UID] = p
	}
	for _, c := range pod.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case corev1.PodScheduled:
			if p.scheduled.IsZero() {
				p.scheduled = c.LastTransitionTime.Time
			}
		case corev1.PodReady:
			if p.ready.IsZero() {
				p.ready = c.LastTransitionTime.Time
			}
		}
	}
	p.coldStarted = p.coldStarted || a[fnstatus.ColdStartedAnnotation] != ""
	if v := a[fnstatus.RestoredAnnotation]; v != "" && p.restored.IsZero() {
		if at, err := time.Parse(time.RFC3339, v); err == nil {
			p.restored = at
		}
	}
	if pod.DeletionTimestamp != nil && p.deleteReq.IsZero() {
		p.deleteReq = pod.DeletionTimestamp.Time
		if g := pod.DeletionGracePeriodSeconds; g != nil {
			p.deleteReq = p.deleteReq.Add(-time.Duration(*g) * time.Second)
		}
	}
	if v := a[fnstatus.TeardownExitedAnnotation]; v != "" && p.exited.IsZero() {
		if at, err := time.Parse(time.RFC3339, v); err == nil {
			p.exited = at
		}
	}
	if deleted && !p.gone {
		p.gone, p.goneAt, p.goneUnknown = true, now, unknown
		if p.deleteReq.IsZero() {
			p.deleteReq = now
		}
	}
}

// batchFor is the restore of instance of key in ns that a pod created at
// created belongs to: one created within fnstatus.RestoreMatchWindow of
// it, unless every pod of that one had gone before it was created (a new
// restore); else a new one. A pod seen after its restore's other pods went
// (a server restart, a re-list) still joins it.
func (t *restoreTracker) batchFor(ns, key, instance string, created time.Time) *restoreBatch {
	for _, b := range t.batches {
		if b.ns == ns && b.key == key && b.instance == instance && absDuration(created.Sub(b.created)) <= fnstatus.RestoreMatchWindow &&
			!(b.over() && created.After(b.lastGone())) {
			if created.Before(b.created) {
				delete(t.batches, b.id())
				b.created = created
				t.batches[b.id()] = b
			}
			return b
		}
	}
	b := &restoreBatch{ns: ns, key: key, instance: instance, created: created, pods: map[types.UID]*restorePod{}}
	if old := t.batches[b.id()]; old != nil {
		return old
	}
	t.batches[b.id()] = b
	return b
}

func (b *restoreBatch) id() string {
	return b.ns + "/" + b.key + "/" + b.instance + "/" + b.created.UTC().Format(time.RFC3339)
}

// over reports whether every pod of the restore is gone.
func (b *restoreBatch) over() bool {
	for _, p := range b.pods {
		if !p.gone {
			return false
		}
	}
	return len(b.pods) > 0
}

// lastGone is when the restore's last pod went.
func (b *restoreBatch) lastGone() time.Time {
	var last time.Time
	for _, p := range b.pods {
		if p.goneAt.After(last) {
			last = p.goneAt
		}
	}
	return last
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// restores returns every restore followed, by configuration key, and the
// ids of those whose pods are all gone, by key (forget them once
// written); ranksOf gives a key's GPU ranks (0 when not known: the ranks
// seen count).
func (t *restoreTracker) restores(ranksOf func(key string) int) (map[string][]fnstatus.Restore, map[string][]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := map[string][]fnstatus.Restore{}
	over := map[string][]string{}
	for id, b := range t.batches {
		out[b.key] = append(out[b.key], b.entry(ranksOf(b.key)))
		if b.over() {
			over[b.key] = append(over[b.key], id)
		}
	}
	return out, over
}

// current is the newest pod of each rank: a rank replaced during the
// restore counts once, as its replacement.
func (b *restoreBatch) current() map[int]*restorePod {
	cur := map[int]*restorePod{}
	for _, p := range b.pods {
		if c := cur[p.ordinal]; c == nil || p.created.After(c.created) {
			cur[p.ordinal] = p
		}
	}
	return cur
}

// entry is what the restore's pods say of it; ranks is how many GPU ranks
// its instance has.
func (b *restoreBatch) entry(ranks int) fnstatus.Restore {
	r := fnstatus.Restore{At: b.created.UTC().Format(time.RFC3339), Namespace: b.ns, Key: b.key, Instance: b.instance, Outcome: fnstatus.RestoreRestoring}
	cur := b.current()
	if ranks < len(cur) {
		ranks = len(cur)
	}
	r.Ranks = ranks
	var firstScheduled, lastReady, lastRestored, firstDelete, lastExited time.Time
	ready, deleting, exited := 0, 0, 0
	cold, killed := false, false
	for _, p := range cur {
		cold = cold || p.coldStarted
		if !p.scheduled.IsZero() && (firstScheduled.IsZero() || p.scheduled.Before(firstScheduled)) {
			firstScheduled = p.scheduled
		}
		if !p.ready.IsZero() {
			ready++
			if p.ready.After(lastReady) {
				lastReady = p.ready
			}
		}
		if p.restored.After(lastRestored) {
			lastRestored = p.restored
		}
		if !p.deleteReq.IsZero() {
			deleting++
			if firstDelete.IsZero() || p.deleteReq.Before(firstDelete) {
				firstDelete = p.deleteReq
			}
		}
		if !p.exited.IsZero() {
			exited++
			if p.exited.After(lastExited) {
				lastExited = p.exited
			}
		}
		if p.gone && p.exited.IsZero() && !p.goneUnknown {
			killed = true
		}
	}
	if !lastRestored.IsZero() {
		r.RestoredAt = lastRestored.UTC().Format(time.RFC3339)
	}
	switch {
	case cold:
		// A rank's restore failed and it started cold in place: not a
		// restore's time.
		r.Outcome = fnstatus.RestoreColdStarted
	case ready >= ranks && !firstScheduled.IsZero():
		r.Outcome = fnstatus.RestoreRestored
		r.ScaleUpSeconds = seconds(lastReady.Sub(firstScheduled))
	case b.over() || (len(cur) >= ranks && deleting >= len(cur)):
		r.Outcome = fnstatus.RestoreGone // deleted, or replaced, before every rank was Ready
	}
	// The teardown of a restored instance of more than one rank, held by
	// the group teardown barrier until every rank exited: known once every
	// rank is deleted and has exited, or once a rank went without exiting
	// (a pod whose delete was seen only as the watch's last known state
	// gives no verdict). A lone rank has no barrier and records no exit.
	if r.Outcome == fnstatus.RestoreRestored && ranks > 1 && deleting >= len(cur) {
		switch {
		case killed:
			r.Teardown = fnstatus.TeardownKilled
		case exited >= len(cur):
			r.Teardown = fnstatus.TeardownReleased
			r.TeardownSeconds = seconds(lastExited.Sub(firstDelete))
		}
	}
	return r
}

func seconds(d time.Duration) float64 {
	if d < 0 {
		return 0
	}
	return float64(int64(d.Seconds()*10+0.5)) / 10
}

// forget drops the restores of ids, whose final entries are written, and
// those not heard of for restoreForget.
func (t *restoreTracker) forget(ids []string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	drop := map[string]bool{}
	for _, id := range ids {
		drop[id] = true
	}
	for id, b := range t.batches {
		if drop[id] || now.Sub(b.seen) > restoreForget {
			delete(t.batches, id)
			for uid, pb := range t.byPod {
				if pb == b {
					delete(t.byPod, uid)
				}
			}
		}
	}
}
