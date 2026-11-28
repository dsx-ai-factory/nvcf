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
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	nvcav2beta1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v2beta1"
)

// Instance health of an HA utils instance is derived from the live replicas only. It does not consult the rollout gate:
// whether NVCA may change HA resources (UtilsReplicaPlan) is a separate question from whether the instance is healthy.
// A closed gate pauses changes and reports UtilsProvenanceDegraded, it never makes a serving instance unhealthy.

// utilsReplicasSummary is the health of the utils replicas of one instance.
type utilsReplicasSummary struct {
	// Desired is the recorded replica count, Total the replicas found through the ownership chain.
	Desired, Total int32
	// Serving replicas have a ready worker container. Failed replicas are terminally bad. Starting is the rest.
	Serving, Failed, Starting int32
	// Terminating replicas have a deletion timestamp. They never count as serving, even with a ready worker container:
	// they are on their way out and a client must not rely on them.
	Terminating int32
	// Status is the combined status the instance health logic consumes in place of a single utils pod status.
	Status ObjectStatus
	// WorkerStatusPresent is false while no replica has container statuses yet, so the caller waits for pod events.
	WorkerStatusPresent bool
}

// workerReady reports whether the worker container of pod is ready, and whether its status is present at all.
func workerReady(pod *corev1.Pod) (ready, present bool) {
	cs, present, err := getFunctionWorkerContainerStatus(pod)
	if err != nil || !present {
		return false, false
	}
	return cs.Ready, true
}

// summarizeUtilsReplicas combines per-replica pod statuses into the status of the utils replica set.
//
//   - At least one serving replica: the instance is healthy, whatever the others do. A failing replacement next to a
//     serving replica is reported in the message, not as an instance failure (REQ-HA-14).
//   - No serving replica and every replica terminally bad: terminal. This feeds the existing failing-objects backoff and
//     failure path, so the grace period is the existing FailingObjectsBackoffTimeout (REQ-HA-15).
//   - Otherwise the instance is still starting, or has lost its serving replicas and a replacement is coming.
//
// statuses is aligned with pods.
func summarizeUtilsReplicas(desired int32, pods []*corev1.Pod, statuses []ObjectStatus) utilsReplicasSummary {
	s := utilsReplicasSummary{Desired: desired, Total: int32(len(pods))}
	var failed []ObjectStatus
	scheduling := false
	for i, p := range pods {
		st := statuses[i]
		if p.DeletionTimestamp != nil {
			s.Terminating++
			continue
		}
		ready, present := workerReady(p)
		s.WorkerStatusPresent = s.WorkerStatusPresent || present
		// A pod on an unreachable node keeps the last container status the kubelet reported, so its worker still looks
		// ready. Serving needs positive evidence: the pod Ready condition must be true. False (set by the node
		// controller), unknown and a missing condition all mean no.
		ready = ready && podReadyConditionTrue(p)
		switch {
		case st.TerminalBad:
			s.Failed++
			failed = append(failed, st)
		case ready:
			s.Serving++
		default:
			s.Starting++
			scheduling = scheduling || st.Scheduling
		}
	}

	combined := ObjectStatus{ChildObjects: statuses}
	if len(pods) > 0 {
		combined.Object = pods[0]
	}
	switch {
	case s.Serving > 0:
		combined.Status = statusRunning
		combined.Reason = fmt.Sprintf("%d of %d utils replicas serving", s.Serving, s.Total-s.Terminating)
		if s.Failed > 0 {
			combined.Reason += fmt.Sprintf(", %d failing", s.Failed)
		}
		if s.Terminating > 0 {
			combined.Reason += fmt.Sprintf(", %d terminating", s.Terminating)
		}
		// A replica that is not terminal but not serving yet is not pending the instance: it already serves.
	case s.Total-s.Terminating > 0 && s.Failed == s.Total-s.Terminating:
		combined.TerminalBad = true
		combined.Status = statusFailed
		combined.Reason = fmt.Sprintf("all %d utils replicas failed", s.Failed)
		allDegraded := true
		for _, f := range failed {
			allDegraded = allDegraded && f.Status == statusDegradedWorker
		}
		if allDegraded {
			combined.Status = statusDegradedWorker
		}
		// The worker issue reasons come from the first failed replica, as they do for a single utils pod.
		combined.AdditionalPodStatus = failed[0].AdditionalPodStatus
	default:
		combined.Pending = true
		combined.Scheduling = scheduling || s.Total-s.Terminating == 0
		combined.Status = statusStarting
		combined.Reason = fmt.Sprintf("waiting for a utils replica (%d starting, %d failed, %d terminating of %d)",
			s.Starting, s.Failed, s.Terminating, s.Total)
	}
	s.Status = combined
	return s
}

// haUtilsView is the live view of an HA instance's utils replicas.
type haUtilsView struct {
	pods    []*corev1.Pod
	summary utilsReplicasSummary
}

// haUtilsView returns the replica view when the instance is an HA instance, and nil when it is not. An instance is HA
// when NVCA recorded it, or when its utils Deployment exists (NVCA may have been restarted between creating the
// Deployment and recording it). tryDeployment lets the caller ask the second question when the bare pod is missing.
// Replicas are found through the ownership chain by UID, never by label alone.
func (r *Reconciler) haUtilsView(ctx context.Context, ms *v1alpha1.MiniService, icmsReq *nvcav2beta1.ICMSRequest,
	tryDeployment bool) (*haUtilsView, error) {
	desired := recordedUtilsHAReplicas(ms)
	if desired == 0 && !tryDeployment {
		return nil, nil
	}
	dep, err := getOwnedUtilsDeployment(ctx, r.Client, ms)
	if err != nil {
		return nil, err
	}
	if dep == nil {
		if desired > 0 {
			// Recorded as HA but the Deployment is gone: no replica can serve. The existing paths decide the instance's
			// health; this keeps the degraded signal from showing a stale healthy state.
			logf.FromContext(ctx).Info("Utils Deployment of an HA instance not found", "desired", desired)
			setUtilsReplicasDegraded(ms, utilsReplicasSummary{Desired: desired}, "utils Deployment not found")
			r.Metrics.SetUtilsReplicasDegradedInstances(r.utilsDegraded.set(client.ObjectKeyFromObject(ms), true))
		}
		return nil, nil
	}
	pods, err := listHAUtilsPods(ctx, r.Client, ms)
	if err != nil {
		return nil, err
	}
	if desired == 0 && dep.Spec.Replicas != nil {
		desired = *dep.Spec.Replicas
	}
	statuses := make([]ObjectStatus, 0, len(pods))
	for _, p := range pods {
		st, err := r.checkPodStatus(ctx, p, icmsReq)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, st)
	}
	summary := summarizeUtilsReplicas(desired, pods, statuses)
	if summary.Serving < desired {
		// Capacity is reduced while a replica is lost or replaced. The serving count is the evidence that a replica on an
		// unreachable node no longer counts.
		logf.FromContext(ctx).Info("Utils replicas serving fewer than desired", "desired", desired, "serving", summary.Serving,
			"starting", summary.Starting, "failed", summary.Failed, "terminating", summary.Terminating)
	}
	setUtilsReplicasDegraded(ms, summary, "")
	r.Metrics.SetUtilsReplicasDegradedInstances(r.utilsDegraded.set(client.ObjectKeyFromObject(ms), summary.Serving < desired))
	return &haUtilsView{pods: pods, summary: summary}, nil
}

// podReadyConditionTrue reports whether the pod Ready condition is present and true.
func podReadyConditionTrue(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// ConditionUtilsReplicasDegraded is set on an HA utils instance while fewer replicas serve than desired. The instance
// stays healthy (WorkersHealthy True) while one replica serves, so without this condition a lost replica that is never
// replaced, for example one whose replacement cannot authenticate, is visible only in the NVCA log. The condition is
// added only once an instance degrades; it is set False with reason ReplicasServing when all desired replicas serve again.
const ConditionUtilsReplicasDegraded = "UtilsReplicasDegraded"

// setUtilsReplicasDegraded updates ConditionUtilsReplicasDegraded from summary and reports whether it changed.
// missingDeployment, when set, says why no replica was found (reason DeploymentMissing instead of ReplicasMissing).
func setUtilsReplicasDegraded(ms *v1alpha1.MiniService, s utilsReplicasSummary, missingDeployment string) bool {
	cond := metav1.Condition{Type: ConditionUtilsReplicasDegraded, ObservedGeneration: ms.Generation}
	switch {
	case missingDeployment != "":
		cond.Status, cond.Reason = metav1.ConditionTrue, "DeploymentMissing"
		cond.Message = fmt.Sprintf("%s: 0 of %d desired utils replicas serving", missingDeployment, s.Desired)
	case s.Serving < s.Desired:
		cond.Status, cond.Reason = metav1.ConditionTrue, "ReplicasMissing"
		cond.Message = fmt.Sprintf("%d of %d desired utils replicas serving (starting %d, failed %d, terminating %d)",
			s.Serving, s.Desired, s.Starting, s.Failed, s.Terminating)
	default:
		if meta.FindStatusCondition(ms.Status.Conditions, ConditionUtilsReplicasDegraded) == nil {
			// Do not add a False condition to instances that never degraded.
			return false
		}
		cond.Status, cond.Reason = metav1.ConditionFalse, "ReplicasServing"
		cond.Message = fmt.Sprintf("%d of %d desired utils replicas serving", s.Serving, s.Desired)
	}
	return meta.SetStatusCondition(&ms.Status.Conditions, cond)
}

// utilsDegradedTracker counts the HA instances that currently serve fewer utils replicas than desired, for the
// cluster-level gauge (no per-instance label). The zero value is ready to use.
type utilsDegradedTracker struct {
	mu       sync.Mutex
	degraded map[types.NamespacedName]struct{}
}

// set records whether key is degraded and returns the number of degraded instances.
func (t *utilsDegradedTracker) set(key types.NamespacedName, degraded bool) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if degraded {
		if t.degraded == nil {
			t.degraded = map[types.NamespacedName]struct{}{}
		}
		t.degraded[key] = struct{}{}
	} else {
		delete(t.degraded, key)
	}
	return len(t.degraded)
}
