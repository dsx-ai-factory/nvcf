/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mscontroller

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/util/k8sutil"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance/readiness"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
)

// ConditionUtilsProvenanceDegraded is set on a MiniService whose HA utils instance lost its safety proof (verified
// enforcement, instance RBAC, or a clean inventory). It is reported, never acted on by shrinking: the instance keeps
// serving with the replicas it has.
const ConditionUtilsProvenanceDegraded = "UtilsProvenanceDegraded"

// UtilsReplicaPlan is the decision for an instance's utils replicas.
type UtilsReplicaPlan struct {
	// Replicas is the number of utils pods the instance should have.
	Replicas int32
	// Paused is true when changes that rely on the provenance boundary are paused: scaling up, changing the utils
	// template, and creating new utils objects. Cleanup and deletion are never paused.
	Paused bool
	// Degraded is true when an HA instance exists and the rollout gate is closed.
	Degraded bool
	// Reason says why the gate is closed. Empty when it is open.
	Reason string
}

// CanScaleUp reports whether the number of replicas may grow.
func (p UtilsReplicaPlan) CanScaleUp() bool { return !p.Paused }

// CanChangeTemplate reports whether the utils pod template may change (image, env, resources).
func (p UtilsReplicaPlan) CanChangeTemplate() bool { return !p.Paused }

// CanCleanup reports whether instance deletion and cleanup may proceed. It is always true: losing readiness must never
// block removing an instance.
func (p UtilsReplicaPlan) CanCleanup() bool { return true }

// ConditionUtilsHA is the MiniService status condition that records an HA utils instance: status True, message the
// replica count. NVCA writes it, only while the rollout gate is open, after it created the utils Deployment. The
// instance service accounts cannot write MiniService status, so the record is trusted when readiness is later lost, and
// it is what keeps an existing HA instance recognized and unshrunk then. (An annotation would not do: the controller
// only patches status and finalizers.)
const ConditionUtilsHA = "UtilsHA"

// utilsHARecord is what NVCA records, in the UtilsHA condition message as JSON, when it creates an HA utils Deployment.
// Besides the replica count it keeps what cleanup needs after the Deployment is gone: the Deployment UID, so surviving
// ReplicaSets and pods can still be found through the ownership chain, and the termination grace period, so cleanup waits
// as long as it was configured to without reading a pod. Only NVCA writes it, and the instance service accounts cannot
// write MiniService status.
type utilsHARecord struct {
	Replicas      int32     `json:"replicas"`
	DeploymentUID types.UID `json:"deploymentUID,omitempty"`
	// TerminationGracePeriodSeconds is the utils pod's grace period, nil when the pod template did not set one.
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
	// PodSpecDigest and ContainersDigest are hashes of the Deployment template as NVCA created it (see
	// utilsprovenance.PodSpecDigest and ContainersDigest). They are the trusted desired state that the inventory scan
	// compares the live Deployment, ReplicaSets and pods with, so an object whose image or command was changed while
	// enforcement was not verified is found. They are written once and never replaced by a live object's digest.
	PodSpecDigest    string `json:"podSpecDigest,omitempty"`
	ContainersDigest string `json:"containersDigest,omitempty"`
	// DeploymentSpecDigest covers the rest of the Deployment spec (replicas, selector, strategy), so a changed replica
	// count or rolling strategy is found as well as a changed template.
	DeploymentSpecDigest string `json:"deploymentSpecDigest,omitempty"`
}

// recordedUtilsHA returns the HA record of ms, if there is a valid one. A bare number in the message, written by an
// earlier build, is read as a record with only the replica count.
func recordedUtilsHA(ms *v1alpha1.MiniService) (utilsHARecord, bool) {
	if ms == nil {
		return utilsHARecord{}, false
	}
	c := meta.FindStatusCondition(ms.Status.Conditions, ConditionUtilsHA)
	if c == nil || c.Status != metav1.ConditionTrue {
		return utilsHARecord{}, false
	}
	var rec utilsHARecord
	if err := json.Unmarshal([]byte(c.Message), &rec); err != nil {
		n, perr := strconv.ParseInt(c.Message, 10, 32)
		if perr != nil {
			return utilsHARecord{}, false
		}
		rec = utilsHARecord{Replicas: int32(n)}
	}
	if rec.Replicas < 2 {
		return utilsHARecord{}, false
	}
	return rec, true
}

// recordedUtilsHAReplicas returns the replica count NVCA recorded when it created an HA utils instance, or 0 for none.
func recordedUtilsHAReplicas(ms *v1alpha1.MiniService) int32 {
	rec, ok := recordedUtilsHA(ms)
	if !ok {
		return 0
	}
	return rec.Replicas
}

// recordUtilsHA records that an HA utils instance exists. It returns true if the condition changed.
func recordUtilsHA(ms *v1alpha1.MiniService, rec utilsHARecord) bool {
	msg, err := json.Marshal(rec)
	if err != nil {
		return false
	}
	return meta.SetStatusCondition(&ms.Status.Conditions, metav1.Condition{
		Type: ConditionUtilsHA, Status: metav1.ConditionTrue, Reason: "UtilsDeploymentCreated",
		Message: string(msg), ObservedGeneration: ms.Generation,
	})
}

// newUtilsHARecord builds the record from the Deployment NVCA created.
func newUtilsHARecord(replicas int32, dep *appsv1.Deployment) utilsHARecord {
	rec := utilsHARecord{Replicas: replicas}
	if dep != nil {
		rec.DeploymentUID = dep.UID
		rec.TerminationGracePeriodSeconds = dep.Spec.Template.Spec.TerminationGracePeriodSeconds
		rec.PodSpecDigest = utilsprovenance.PodSpecDigest(&dep.Spec.Template.Spec)
		rec.ContainersDigest = utilsprovenance.ContainersDigest(&dep.Spec.Template.Spec)
		rec.DeploymentSpecDigest = utilsprovenance.DeploymentSpecDigest(&dep.Spec)
	}
	return rec
}

// mergeTrustedDigests decides which template digests the new record carries. A digest that was already recorded is
// kept: it is the desired state, and must never be replaced by the digest of a live object that may have been changed.
// A fresh digest is taken only from a Deployment NVCA has just created, or, for an existing Deployment that has none
// recorded, while the rollout gate is open. Otherwise the record has no digests and the inventory cannot trust the
// object's content.
func mergeTrustedDigests(old utilsHARecord, hadOld bool, rec utilsHARecord, created, gateOpen bool) utilsHARecord {
	switch {
	case hadOld && old.hasDigests():
		rec.PodSpecDigest, rec.ContainersDigest, rec.DeploymentSpecDigest = old.PodSpecDigest, old.ContainersDigest, old.DeploymentSpecDigest
	case created || gateOpen:
		// keep the digests newUtilsHARecord computed
	default:
		rec.PodSpecDigest, rec.ContainersDigest, rec.DeploymentSpecDigest = "", "", ""
	}
	return rec
}

func (r utilsHARecord) hasDigests() bool {
	return r.PodSpecDigest != "" && r.ContainersDigest != "" && r.DeploymentSpecDigest != ""
}

// UtilsTrust is the desired state NVCA recorded for the HA utils of an instance: the digests of the pod spec, of the
// containers and of the rest of the Deployment spec, the replica count, and the UID of the Deployment NVCA created.
type UtilsTrust struct {
	PodSpecDigest, ContainersDigest, DeploymentSpecDigest string
	Replicas                                              int32
	DeploymentUID                                         types.UID
}

// UtilsHATrust returns the desired state NVCA recorded for the HA utils of ms. ok is false when there is no HA record or
// it carries no digests or no Deployment UID.
func UtilsHATrust(ms *v1alpha1.MiniService) (UtilsTrust, bool) {
	rec, has := recordedUtilsHA(ms)
	if !has || !rec.hasDigests() || rec.DeploymentUID == "" {
		return UtilsTrust{}, false
	}
	return UtilsTrust{
		PodSpecDigest: rec.PodSpecDigest, ContainersDigest: rec.ContainersDigest, DeploymentSpecDigest: rec.DeploymentSpecDigest,
		Replicas: rec.Replicas, DeploymentUID: rec.DeploymentUID,
	}, true
}

// planUtilsReplicas decides the utils replicas from the requested count, the recorded existing HA count, and the
// rollout gate.
//
//   - Gate open: the requested count (at least 1).
//   - Gate closed and no HA instance: one pod, the existing behavior. A request for more is refused and says why.
//   - Gate closed and an HA instance exists: keep what exists. Do not shrink, do not grow, pause template changes and
//     new utils objects, and report degraded. Losing readiness is not a reason to remove replicas that are serving.
func planUtilsReplicas(requested, existing int32, gate readiness.Checkable) UtilsReplicaPlan {
	if requested < 1 {
		requested = 1
	}
	err := readiness.CheckOrMissing(gate)
	if err == nil {
		return UtilsReplicaPlan{Replicas: requested}
	}
	if existing > 1 {
		return UtilsReplicaPlan{Replicas: existing, Paused: true, Degraded: true, Reason: err.Error()}
	}
	p := UtilsReplicaPlan{Replicas: 1}
	if requested > 1 {
		p.Paused, p.Reason = true, err.Error()
	}
	return p
}

// effectiveUtilsReplicas plans the replicas for ms and logs when the gate holds something back.
func (r *Reconciler) effectiveUtilsReplicas(log logr.Logger, ms *v1alpha1.MiniService) UtilsReplicaPlan {
	plan := planUtilsReplicas(r.UtilsReplicas, recordedUtilsHAReplicas(ms), r.UtilsProvenanceGate)
	switch {
	case plan.Degraded:
		log.Info("Utils provenance readiness lost for an HA instance, keeping its replicas and pausing changes",
			"replicas", plan.Replicas, "reason", plan.Reason)
	case plan.Paused:
		log.Info("Utils replicas not allowed, using one utils pod", "requested", r.UtilsReplicas, "reason", plan.Reason)
	}
	return plan
}

// labelDiscoveryAllowed reports whether live objects of ms may be recognized as utils pods by the pod-role label. It is
// true while the rollout gate is open, and for an instance NVCA recorded as HA, so losing readiness never turns an
// existing HA instance's utils pods into chart workloads. A forged label in any other namespace is not honored.
func (r *Reconciler) labelDiscoveryAllowed(ms *v1alpha1.MiniService) bool {
	return readiness.Open(r.UtilsProvenanceGate) || recordedUtilsHAReplicas(ms) > 1
}

// SetUtilsProvenanceDegraded sets or clears the degraded condition on ms from the plan. It returns true if the
// condition changed.
func SetUtilsProvenanceDegraded(ms *v1alpha1.MiniService, plan UtilsReplicaPlan) bool {
	cond := metav1.Condition{Type: ConditionUtilsProvenanceDegraded, ObservedGeneration: ms.Generation}
	if plan.Degraded {
		cond.Status, cond.Reason, cond.Message = metav1.ConditionTrue, "ReadinessLost", plan.Reason
	} else {
		cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, "ReadinessVerified", ""
		if meta.FindStatusCondition(ms.Status.Conditions, ConditionUtilsProvenanceDegraded) == nil {
			// Do not add a False condition to instances that never had one.
			return false
		}
	}
	return meta.SetStatusCondition(&ms.Status.Conditions, cond)
}

// utilsPodRecognized is the recognition rule for live pods of ms (see k8sutil.IsUtilsPodDiscovered).
func (r *Reconciler) utilsPodRecognized(ms *v1alpha1.MiniService, pod *corev1.Pod) bool {
	return k8sutil.IsUtilsPodDiscovered(pod, r.labelDiscoveryAllowed(ms))
}

// utilsDegradedRecheck is how often an HA instance that lost its provenance proof is reconciled again. Nothing else
// wakes a quiet instance when the gate reopens, so without it the degraded condition and the paused changes would stay
// until an unrelated event.
const utilsDegradedRecheck = 30 * time.Second

// requeueWhileUtilsDegraded makes a degraded instance reconcile again soon, keeping an earlier requeue.
func requeueWhileUtilsDegraded(plan UtilsReplicaPlan, res reconcile.Result) reconcile.Result {
	if !plan.Degraded {
		return res
	}
	if res.RequeueAfter == 0 || res.RequeueAfter > utilsDegradedRecheck {
		res.RequeueAfter = utilsDegradedRecheck
		res.Requeue = false
	}
	return res
}
