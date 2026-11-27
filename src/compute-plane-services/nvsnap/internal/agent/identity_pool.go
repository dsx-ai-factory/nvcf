// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Identity pool. A Dynamo worker that registers through Kubernetes has an
// identity, its instance ID (a hash of its pod name), that it computes once
// and keeps: every copy restored from one capture carries its source's ID,
// so two copies in one namespace would register the same worker twice, and
// the restore refuses the second (instanceIDsFree). To restore several
// replicas of one configuration in one namespace, the configuration keeps
// a pool of captures, each of a different source pod and so of a different
// ID: slot 0 is the configuration's record (its key unchanged), slot k its
// key with "-s<k>". A replica of such a worker that started cold while every
// slot was in use is captured into the next free slot; admission restores a
// replica from the first complete slot whose IDs no pod of the namespace
// holds. The restore's own claim (claimInstanceID) stays the authority: two
// replicas that pick the same slot at once resolve there, the second
// starting cold.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// identityPoolSlots bounds the slots of one configuration in one tenant
// scope: past it a replica starts cold and is not captured.
const identityPoolSlots = 32

// poolSlotKey is slot k of the configuration whose record key is base.
func poolSlotKey(base string, k int) string {
	if k == 0 {
		return base
	}
	return base + "-s" + strconv.Itoa(k)
}

// poolSlotOf reports whether key is a slot of base, and which.
func poolSlotOf(base, key string) (int, bool) {
	if key == base {
		return 0, true
	}
	rest, ok := strings.CutPrefix(key, base+"-s")
	if !ok {
		return 0, false
	}
	k, err := strconv.Atoi(rest)
	if err != nil || k < 1 || k >= identityPoolSlots || strconv.Itoa(k) != rest {
		return 0, false
	}
	return k, true
}

// recordInstanceIDs returns the Dynamo instance IDs the registrations of
// record data name (kube_discovery.go), sorted; none for a record of
// workers that do not register through Kubernetes.
func recordInstanceIDs(d map[string]string) []string {
	seen := map[string]bool{}
	for k, v := range d {
		if !strings.HasPrefix(k, "kubeRegistrations.") {
			continue
		}
		var r rankRegistrations
		if json.Unmarshal([]byte(v), &r) != nil {
			continue
		}
		for _, o := range r.Objects {
			for _, id := range instanceIDs(o.Spec) {
				seen[id] = true
			}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// instanceIDHeld returns an ID of ids that a running pod of ns holds: by the
// claim a restore made for it, or by its own registration. "" when none is.
func (a *Agent) instanceIDHeld(ctx context.Context, ns string, ids []string) (string, error) {
	nobody := &corev1.Pod{}
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
		cm, err := a.kubeClient.CoreV1().ConfigMaps(ns).Get(ctx, instanceClaimPrefix+id, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("read the claim of instance %s: %w", id, err)
		}
		live, err := a.ownerLive(ctx, nobody, ns, cm.OwnerReferences)
		if err != nil {
			return "", err
		}
		if live {
			return id, nil
		}
	}
	if a.dynClient == nil {
		return "", nil
	}
	list, err := a.dynClient.Resource(dynamoWorkerMetadata).Namespace(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list DynamoWorkerMetadata in %s: %w", ns, err)
	}
	for i := range list.Items {
		o := &list.Items[i]
		raw, err := json.Marshal(o.Object["spec"])
		if err != nil {
			continue
		}
		for _, id := range instanceIDs(raw) {
			if !want[id] {
				continue
			}
			live, err := a.ownerLive(ctx, nobody, ns, o.GetOwnerReferences())
			if err != nil {
				return "", err
			}
			if live {
				return id, nil
			}
		}
	}
	return "", nil
}

// poolSlotForRestore returns the record key a pod of the configuration base
// in ns, of process configuration processConfig, restores from: base itself for a configuration without a pool (its
// record names no instance ID, or there is none), else the first complete
// slot whose instance IDs no running pod of ns holds. "" when every slot is
// in use: the pod starts cold, and is captured into a new slot.
func (a *Agent) poolSlotForRestore(ctx context.Context, ns, base, processConfig string) string {
	cms := a.kubeClient.CoreV1().ConfigMaps(a.criuFailedNamespace())
	for k := 0; k < identityPoolSlots; k++ {
		key := poolSlotKey(base, k)
		cm, err := cms.Get(ctx, criuGroupConfigMapName(key), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			if k == 0 {
				return base
			}
			return ""
		}
		if err != nil {
			return "" // cannot tell which record restores this pod: cold
		}
		ids := recordInstanceIDs(cm.Data)
		if len(ids) == 0 || cm.Data["size"] != "1" {
			// Not a pool: workers without Kubernetes registrations, or an
			// instance of several ranks (only single-rank configurations get
			// slots), whose ranks the restore checks per ordinal.
			return key
		}
		if !slotRestorableIn(key, cm.Data, ns) || !slotOfConfig(cm.Data, processConfig) {
			continue
		}
		held, err := a.instanceIDHeld(ctx, ns, ids)
		if err != nil {
			return "" // cannot tell whether its identity is free: cold
		}
		if held == "" {
			return key
		}
	}
	return ""
}

// slotOfConfig reports whether the single-rank slot record d was captured
// with process configuration pc (webhook.ProcessConfig): the one rule
// admission and capture share, so a pod restores only from a slot its
// restored process could run as, and a slot of another configuration is
// replaced by a new capture. A record or a pod without one matches.
func slotOfConfig(d map[string]string, pc string) bool {
	if pc == "" {
		return true
	}
	return len(webhook.ProcessConfigChanges(d["processConfig.0"], pc)) == 0
}

// slotRestorableIn reports whether the slot record key, with data d, can
// restore a pod of ns now: a record admission accepts (complete, or being
// verified by its own instance's restore, and not stalled), whose
// verification, if any, is ns's.
func slotRestorableIn(key string, d map[string]string, ns string) bool {
	g, ok := parseCRIUGroup(key, d)
	if !ok {
		return false
	}
	return g.VerifyNamespace == "" || g.VerifyNamespace == ns
}

// poolHasFreeSlot reports whether the configuration base has a slot pod
// could restore from in ns (slotRestorableIn, of pod's process
// configuration) whose instance IDs no running pod of ns holds: a replica
// that started cold anyway did so for another reason than a full pool, and
// is not captured into a new slot. False when it has no record at all.
func (a *Agent) poolHasFreeSlot(ctx context.Context, ns, base string, pod *corev1.Pod) bool {
	cms := a.kubeClient.CoreV1().ConfigMaps(a.criuFailedNamespace())
	for k := 0; k < identityPoolSlots; k++ {
		cm, err := cms.Get(ctx, criuGroupConfigMapName(poolSlotKey(base, k)), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false
		}
		if err != nil {
			return true // cannot tell: not captured now; a later event tries again
		}
		// Only a slot pod could restore from counts: one admission accepts in
		// ns (not a stalled verification, which a capture may replace) and of
		// pod's process configuration (an older one is replaced instead).
		if !slotRestorableIn(poolSlotKey(base, k), cm.Data, ns) ||
			!slotOfConfig(cm.Data, pod.Annotations[webhook.ProcessConfigAnnotation]) {
			continue
		}
		ids := recordInstanceIDs(cm.Data)
		if len(ids) == 0 {
			return true
		}
		if held, err := a.instanceIDHeld(ctx, ns, ids); err != nil || held == "" {
			return true
		}
	}
	return false
}

// poolSlotForCapture returns the slot key of the configuration base that
// a capture of a cold-started replica goes into: the first slot with no
// record, or whose record may be captured again (or replaced, by replace).
// false when the pool is full.
func (c *criuAutoRestorer) poolSlotForCapture(ctx context.Context, base string, replace func(map[string]string) bool) (string, bool) {
	cms := c.a.kubeClient.CoreV1().ConfigMaps(c.a.criuFailedNamespace())
	for k := 0; k < identityPoolSlots; k++ {
		key := poolSlotKey(base, k)
		cm, err := cms.Get(ctx, criuGroupConfigMapName(key), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return key, true
		}
		if err != nil {
			return "", false
		}
		if c.groupRecaptures(ctx, cm.Data) || (replace != nil && replace(cm.Data)) {
			return key, true
		}
	}
	return "", false
}
