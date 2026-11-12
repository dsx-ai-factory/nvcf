// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Group teardown barrier (docs/design/GROUP-TEARDOWN.md). The ranks of a
// restored multi-node group hold GPU fabric memory imported from each
// other through their nodes' IMEX daemons, which leave with their pods.
// Were one rank's pod to finish terminating while another still holds
// memory imported from it, that rank's channels are torn down and its exit
// waits in the driver for the lost node for minutes, with its GPUs still
// allocated. So each restored rank's placeholder is held (a file on its
// work volume that cmd/nvsnap-idle waits on) until every rank of its
// restore has exited; the kubelet's SIGKILL at the end of the grace period
// still ends it, as it would without the hold.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// teardownPoll is how often a terminating rank's agent looks again.
// Variable for tests.
var teardownPoll = time.Second

// teardownRank reports a pod whose teardown is held: a GPU rank of a
// multi-pod group that a restore made. A lone restore shares no fabric
// memory; a companion holds no GPU state; a rank started cold, or not
// restored, runs as any pod does.
func teardownRank(p *corev1.Pod) bool {
	if p.Annotations[webhook.CRIUGroupAnnotation] == "" || p.Annotations[webhook.CRIURestoredAnnotation] == "" ||
		isCompanion(p) || coldStarted(p) {
		return false
	}
	_, size, ok := podRank(p, webhook.CRIUGroupOrdinalAnnotation, webhook.CRIUGroupSizeAnnotation)
	return ok && size > 1
}

// teardownReleased reports whether terminating rank self may go: its own
// workload has exited (selfExited) and so has every other rank of the same
// restore in pods, its namespace's pods. A rank whose pod is gone has
// exited. The same restore: the instance's ranks restored before self's
// delete was requested, deleted yet or not; a replacement restored after
// is another restore's.
func teardownReleased(self *corev1.Pod, pods []corev1.Pod, selfExited bool) bool {
	if !selfExited {
		return false
	}
	key := self.Annotations[webhook.CRIUGroupAnnotation]
	for i := range pods {
		p := &pods[i]
		if p.UID == self.UID || p.Namespace != self.Namespace || p.Annotations[webhook.CRIUGroupAnnotation] != key ||
			!teardownRank(p) || !sameInstance(self, p) || !restoredBefore(p, deleteRequested(self)) {
			continue
		}
		if p.Annotations[webhook.TeardownExitedAnnotation] == "" {
			return false
		}
	}
	return true
}

// restoredBefore reports whether p was restored no later than t. A time it
// cannot read counts as before: the rank is waited for, at worst until the
// grace period ends.
func restoredBefore(p *corev1.Pod, t *metav1.Time) bool {
	if t == nil {
		return true
	}
	at, err := time.Parse(time.RFC3339, p.Annotations[webhook.CRIURestoredAnnotation])
	return err != nil || !at.After(t.Time)
}

// deleteRequested is when p's delete was asked for. The API server sets
// DeletionTimestamp to that time plus the grace period, when the kubelet
// kills what is left.
func deleteRequested(p *corev1.Pod) *metav1.Time {
	if p.DeletionTimestamp == nil || p.DeletionGracePeriodSeconds == nil {
		return p.DeletionTimestamp
	}
	t := metav1.NewTime(p.DeletionTimestamp.Add(-time.Duration(*p.DeletionGracePeriodSeconds) * time.Second))
	return &t
}

// teardownWorkDir is a pod's restore work volume in the agent's view of
// the host.
func teardownWorkDir(uid types.UID) string {
	return filepath.Join(cachedirPodsRoot, string(uid), "volumes", "kubernetes.io~empty-dir", webhook.RestoreWorkVolume)
}

// armTeardownHold puts the hold on the work volume dir. The volume is the
// workload's to write: a link there is refused, not followed.
func armTeardownHold(dir string) error {
	if fi, err := os.Lstat(dir); err != nil || !fi.IsDir() {
		if err == nil {
			err = errors.New("not a directory")
		}
		return fmt.Errorf("work volume %s: %w", dir, err)
	}
	f, err := os.OpenFile(filepath.Join(dir, webhook.TeardownHoldFile), os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o644)
	if errors.Is(err, fs.ErrExist) {
		if fi, lerr := os.Lstat(filepath.Join(dir, webhook.TeardownHoldFile)); lerr == nil && fi.Mode().IsRegular() {
			return nil
		}
		return fmt.Errorf("the work volume's %s is not a file the agent made", webhook.TeardownHoldFile)
	}
	if err != nil {
		return err
	}
	return f.Close()
}

func teardownHeld(dir string) bool {
	_, err := os.Lstat(filepath.Join(dir, webhook.TeardownHoldFile)) //nolint:gosec // a fixed name in the pod's work volume, dir built from its UID; Lstat follows no link
	return err == nil
}

func teardownExited(dir string) bool {
	_, err := os.Lstat(filepath.Join(dir, webhook.TeardownExitedFile)) //nolint:gosec // a fixed name in the pod's work volume, dir built from its UID; Lstat follows no link
	return err == nil
}

// liftTeardownHold removes the hold (a link is removed, not followed).
// teardownGone reports whether the work dir itself is gone: the pod's
// volumes were torn down.
func teardownGone(dir string) bool {
	_, err := os.Stat(dir)
	return errors.Is(err, fs.ErrNotExist)
}

func liftTeardownHold(dir string) error {
	if err := os.Remove(filepath.Join(dir, webhook.TeardownHoldFile)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// podReplaced reports whether pod's name is gone or now names another pod.
func (c *criuAutoRestorer) podReplaced(ctx context.Context, pod *corev1.Pod) bool {
	cur, err := c.a.kubeClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true
	}
	return err == nil && cur.UID != pod.UID
}

// considerTeardown holds a restored rank of this node: it arms the hold
// the first time it sees the rank restored, and drives its release once it
// terminates. A rank first seen already terminating (the agent started or
// re-listed meanwhile) is held too, unless its placeholder has already
// gone past the point where it looks for the hold.
func (c *criuAutoRestorer) considerTeardown(ctx context.Context, pod *corev1.Pod) {
	if !teardownRank(pod) {
		return
	}
	uid := string(pod.UID)
	log := c.a.log.WithFields(logrus.Fields{"pod": pod.Namespace + "/" + pod.Name, "criuGroup": pod.Annotations[webhook.CRIUGroupAnnotation]})
	if _, done := c.attempted.LoadOrStore("teardown-hold/"+uid, struct{}{}); !done {
		dir := teardownWorkDir(pod.UID)
		if pod.DeletionTimestamp == nil || !teardownExited(dir) {
			if err := armTeardownHold(dir); err != nil {
				log.WithError(err).Warn("group teardown: cannot hold the restored rank's pod; it may leave before its peers have exited")
				c.attempted.Delete("teardown-hold/" + uid)
			}
		}
	}
	if pod.DeletionTimestamp == nil {
		return
	}
	if _, done := c.attempted.LoadOrStore("teardown/"+uid, struct{}{}); done {
		return
	}
	go c.driveTeardown(ctx, pod.DeepCopy(), log)
}

// driveTeardown reports the terminating rank's workload gone once its
// placeholder says so, and lifts its hold once every rank of its restore
// has exited. It stops when the hold is gone (lifted, or the pod and its
// volume with it) or the grace period is over (DeletionTimestamp, with a
// margin), when the kubelet has killed the pod anyway.
func (c *criuAutoRestorer) driveTeardown(ctx context.Context, pod *corev1.Pod, log *logrus.Entry) {
	dir := teardownWorkDir(pod.UID)
	deadline := pod.DeletionTimestamp.Add(30 * time.Second)
	// The report names the pod by UID too: a pod of the same name admitted
	// after a force delete is not this rank and must not be marked exited.
	annotationPath := "/metadata/annotations/" + strings.NewReplacer("~", "~0", "/", "~1").Replace(webhook.TeardownExitedAnnotation)
	reported := pod.Annotations[webhook.TeardownExitedAnnotation] != ""
	t0 := time.Now()
	tick := time.NewTicker(teardownPoll)
	defer tick.Stop()
	// The pod was killed at the end of its grace period, its peers not all
	// exited, when the hold this driver found ended without it lifting it:
	// still on at the deadline, or gone with the pod's volumes (the kubelet
	// removes the work dir once it has killed the pod; only this driver
	// lifts a hold). Not for a rank first driven past its deadline: a
	// restarted agent would count it again, or count one it never drove.
	drove := teardownHeld(dir) && t0.Before(deadline)
	lifted := false
	defer func() {
		if drove && !lifted && ctx.Err() == nil && (teardownHeld(dir) || teardownGone(dir)) {
			metrics.ObserveTeardown(metrics.TeardownDeadline, c.a.podGPU(ctx, pod), c.instanceRanks(ctx, pod), 0)
			log.WithField("held", time.Since(t0).Round(time.Millisecond).String()).Warn("group teardown: the grace period ran out before every rank exited; the pod is killed")
			c.a.podEvent(pod, true, eventTeardownDeadline, "the grace period ran out before every rank of the restore exited; the pod is killed")
		}
	}()
	for teardownHeld(dir) && time.Now().Before(deadline) {
		exited := teardownExited(dir)
		if exited && !reported {
			patch := fmt.Sprintf(`[{"op":"test","path":"/metadata/uid","value":%q},{"op":"add","path":%q,"value":%q}]`,
				pod.UID, annotationPath, time.Now().UTC().Format(time.RFC3339))
			_, err := c.a.kubeClient.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
			// Not found, or found under another UID (the test fails, 422): this
			// rank's pod is gone, which its peers count as exited. Any other
			// invalid patch is retried.
			gone := apierrors.IsNotFound(err) || (apierrors.IsInvalid(err) && c.podReplaced(ctx, pod))
			reported = err == nil || gone
			if err != nil && !gone {
				log.WithError(err).Warn("group teardown: cannot report the rank's workload gone; trying again")
			}
		}
		// Lift only once the peers can see this rank exited: a failed report
		// is tried again next tick, and holds the pod at worst to its deadline.
		if exited && reported {
			pods, err := c.a.kubeClient.CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
			if err == nil && teardownReleased(pod, pods.Items, true) {
				if err := liftTeardownHold(dir); err != nil {
					log.WithError(err).Warn("group teardown: cannot lift the hold; the pod goes at the end of its grace period")
					return
				}
				lifted = true
				took := time.Since(deleteRequested(pod).Time)
				metrics.ObserveTeardown(metrics.TeardownReleased, c.a.podGPU(ctx, pod), c.instanceRanks(ctx, pod), took)
				log.WithFields(logrus.Fields{"held": time.Since(t0).Round(time.Millisecond).String(), "teardownSeconds": int(took.Seconds())}).
					Info("group teardown: every rank has exited; the rank's pod may go")
				return
			}
		}
		select {
		case <-ctx.Done():
			return // the agent stops; the next one picks the pod up again
		case <-tick.C:
		}
	}
}
