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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/common"
	"github.com/prometheus/client_golang/prometheus"
	promdto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/miniservice/chartcache"
	nvcak8sutil "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/util/k8sutil"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance/readiness"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	nvcav2beta1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v2beta1"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/featureflag"
)

const statusHANS = "sr-status-ha"

func statusMS(byWorkerReadiness bool, recorded string) *v1alpha1.MiniService {
	ms := &v1alpha1.MiniService{
		ObjectMeta: metav1.ObjectMeta{Name: "status-ha-ms", UID: "status-ms-uid", Generation: 1},
		Spec:       v1alpha1.MiniServiceSpec{Namespace: statusHANS, ICMSRequestName: "status-ha-icms"},
	}
	if byWorkerReadiness {
		ms.Spec.WorkloadConfig = &v1alpha1.WorkloadConfig{FeatureFlags: map[string]bool{featureflag.StatusByWorkerReadiness: true}}
	}
	if recorded != "" {
		ms.Status.Conditions = []metav1.Condition{{Type: ConditionUtilsHA, Status: metav1.ConditionTrue, Reason: "UtilsDeploymentCreated", Message: recorded}}
	}
	return ms
}

func ownedUtilsDeployment() []client.Object {
	ctrl := func(kind, name string, uid types.UID) metav1.OwnerReference {
		return metav1.OwnerReference{Kind: kind, Name: name, UID: uid, Controller: ptr.To(true)}
	}
	return []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: UtilsDeploymentName, Namespace: statusHANS, UID: "status-dep-uid",
			OwnerReferences: []metav1.OwnerReference{ctrl("MiniService", "status-ha-ms", "status-ms-uid")}},
			Spec: appsv1.DeploymentSpec{Replicas: ptr.To(int32(2))}},
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "utils-h1", Namespace: statusHANS, UID: "status-rs-uid",
			OwnerReferences: []metav1.OwnerReference{ctrl("Deployment", UtilsDeploymentName, "status-dep-uid")}}},
	}
}

func replicaPod(name string, now time.Time, mutate func(p *corev1.Pod)) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: statusHANS,
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "utils-h1", UID: "status-rs-uid", Controller: ptr.To(true)}}},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyAlways, Containers: []corev1.Container{{Name: common.UtilsContainerName}}},
	}
	mutate(p)
	return p
}

// servingReplica is a running pod whose worker container is ready.
func servingReplica(name string, now time.Time) *corev1.Pod {
	return replicaPod(name, now, func(p *corev1.Pod) {
		p.Status = corev1.PodStatus{
			Phase:     corev1.PodRunning,
			StartTime: &metav1.Time{Time: now.Add(-3 * time.Hour)},
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Time{Time: now.Add(-time.Hour)}},
			},
			ContainerStatuses: []corev1.ContainerStatus{{Name: common.UtilsContainerName, Ready: true}},
		}
	})
}

// failingReplica is a pod whose worker has been in ImagePullBackOff for longer than the degradation period.
func failingReplica(name string, now time.Time) *corev1.Pod {
	return replicaPod(name, now, func(p *corev1.Pod) {
		p.Status = corev1.PodStatus{
			Phase:     corev1.PodRunning,
			StartTime: &metav1.Time{Time: now.Add(-2 * time.Hour)},
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
				{Type: corev1.ContainersReady, Status: corev1.ConditionFalse},
				{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.Time{Time: now.Add(-40 * time.Minute)}},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  common.UtilsContainerName,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "back-off"}},
			}},
		}
	})
}

// startingReplica is a pod that is scheduled and still starting.
func startingReplica(name string, now time.Time) *corev1.Pod {
	return replicaPod(name, now, func(p *corev1.Pod) {
		p.CreationTimestamp = metav1.NewTime(now.Add(-30 * time.Second))
		p.Status = corev1.PodStatus{
			Phase:      corev1.PodPending,
			Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: common.UtilsContainerName, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
			}},
		}
	})
}

type statusRun struct {
	ms  *v1alpha1.MiniService
	res reconcile.Result
	err error
}

func runStatus(t *testing.T, ms *v1alpha1.MiniService, now time.Time, gate readiness.Checkable, objs ...client.Object) statusRun {
	t.Helper()
	r, ctx, icmsReq := statusReconciler(t, ms, now, gate, nil, objs...)
	res, err := r.doStatus(ctx, ms, icmsReq)
	return statusRun{ms: ms, res: res, err: err}
}

// statusReconciler builds the Reconciler runStatus uses; m may be nil (no metrics).
func statusReconciler(t *testing.T, ms *v1alpha1.MiniService, now time.Time, gate readiness.Checkable, m *metrics.Metrics,
	objs ...client.Object) (*Reconciler, context.Context, *nvcav2beta1.ICMSRequest) {
	t.Helper()
	ctx := newTestContext()
	icmsReq := &nvcav2beta1.ICMSRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "status-ha-icms", Namespace: "nvcf-backend"},
		Spec:       nvcav2beta1.ICMSRequestSpec{Action: common.RequestICMSInstances},
	}
	all := append([]client.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: statusHANS}}}, objs...)
	c, _ := newFakeClient(mgrScheme, all...)
	cache := chartcache.New(t.TempDir())
	require.NoError(t, cache.Start(ctx))
	r := &Reconciler{
		ControllerOptions:     ControllerOptions{K8sTimeConfig: (&nvcak8sutil.TimeConfig{}).Complete(), UtilsProvenanceGate: gate, Metrics: m},
		Client:                c,
		Decoder:               serializer.NewCodecFactory(mgrScheme).UniversalDeserializer(),
		chartCache:            cache,
		newPermissionsChecker: newFakePermissionsChecker,
		now:                   func() time.Time { return now },
	}
	require.NoError(t, r.saveRenderedData(ctx, ms, []byte("[]")))
	return r, ctx, icmsReq
}

func cond(ms *v1alpha1.MiniService, t string) *metav1.Condition {
	return meta.FindStatusCondition(ms.Status.Conditions, t)
}

func isTerminalErr(err error) bool { return err != nil && errors.Is(err, reconcile.TerminalError(nil)) }

func TestSummarizeUtilsReplicas(t *testing.T) {
	now := time.Now()
	statuses := func(pods ...*corev1.Pod) []ObjectStatus {
		out := make([]ObjectStatus, len(pods))
		tc := (&nvcak8sutil.TimeConfig{}).Complete()
		for i, p := range pods {
			out[i] = getPodStatus(p, tc, time.Hour)
		}
		return out
	}
	sum := func(pods ...*corev1.Pod) utilsReplicasSummary {
		return summarizeUtilsReplicas(int32(len(pods)), pods, statuses(pods...))
	}

	t.Run("one serving replica next to a failing replacement is healthy", func(t *testing.T) {
		s := sum(servingReplica("a", now), failingReplica("b", now))
		assert.EqualValues(t, 1, s.Serving)
		assert.EqualValues(t, 1, s.Failed)
		assert.False(t, s.Status.TerminalBad, "a failing replacement must not fail the instance")
		assert.False(t, s.Status.Pending)
		assert.Contains(t, s.Status.Reason, "1 of 2 utils replicas serving, 1 failing")
	})
	t.Run("all replicas failed is terminal", func(t *testing.T) {
		s := sum(failingReplica("a", now), failingReplica("b", now))
		assert.True(t, s.Status.TerminalBad)
		assert.Contains(t, s.Status.Reason, "all 2 utils replicas failed")
		assert.NotNil(t, s.Status.AdditionalPodStatus, "worker issue reasons are carried from a failed replica")
	})
	t.Run("a failed replica with another starting is not terminal", func(t *testing.T) {
		s := sum(failingReplica("a", now), startingReplica("b", now))
		assert.False(t, s.Status.TerminalBad, "a replacement is coming")
		assert.True(t, s.Status.Pending)
	})
	t.Run("two serving", func(t *testing.T) {
		s := sum(servingReplica("a", now), servingReplica("b", now))
		assert.EqualValues(t, 2, s.Serving)
		assert.Equal(t, statusRunning, s.Status.Status)
	})
	t.Run("no replicas yet is pending, not terminal", func(t *testing.T) {
		s := summarizeUtilsReplicas(2, nil, nil)
		assert.True(t, s.Status.Pending)
		assert.True(t, s.Status.Scheduling)
		assert.False(t, s.Status.TerminalBad)
	})
	t.Run("a pod that is ready by status but terminally bad does not serve", func(t *testing.T) {
		p := servingReplica("a", now)
		bad := getPodStatus(p, (&nvcak8sutil.TimeConfig{}).Complete(), time.Hour)
		bad.TerminalBad = true
		s := summarizeUtilsReplicas(1, []*corev1.Pod{p}, []ObjectStatus{bad})
		assert.EqualValues(t, 0, s.Serving)
		assert.True(t, s.Status.TerminalBad)
	})
}

func TestHAStatusOneHealthyReplicaAlongsideAFailingReplacement(t *testing.T) {
	now := time.Now()
	objs := append(ownedUtilsDeployment(), servingReplica("utils-h1-a", now), failingReplica("utils-h1-b", now))

	t.Run("worker readiness mode: workers healthy, message says one is failing", func(t *testing.T) {
		run := runStatus(t, statusMS(true, "2"), now, nil, objs...)
		require.NoError(t, run.err)
		c := cond(run.ms, v1alpha1.MiniServiceConditionWorkersHealthy)
		require.NotNil(t, c)
		assert.Equal(t, metav1.ConditionTrue, c.Status)
		assert.Contains(t, c.Message, "1 of 2 utils replicas serving, 1 failing")
		assert.Equal(t, v1alpha1.MiniServiceRunning, run.ms.Status.Phase)
	})
	t.Run("aggressive mode: no terminal error, instance running", func(t *testing.T) {
		run := runStatus(t, statusMS(false, "2"), now, nil, objs...)
		require.NoError(t, run.err, "a failing replacement must not fail the instance")
		assert.Equal(t, v1alpha1.MiniServiceRunning, run.ms.Status.Phase)
		assert.Equal(t, metav1.ConditionTrue, cond(run.ms, v1alpha1.MiniServiceConditionObjectsHealthy).Status)
	})
}

func TestHAStatusAllReplicasUnavailable(t *testing.T) {
	now := time.Now()
	objs := append(ownedUtilsDeployment(), failingReplica("utils-h1-a", now), failingReplica("utils-h1-b", now))

	t.Run("worker readiness mode: terminal worker state", func(t *testing.T) {
		run := runStatus(t, statusMS(true, "2"), now, nil, objs...)
		require.Error(t, run.err)
		assert.True(t, isTerminalErr(run.err))
		c := cond(run.ms, v1alpha1.MiniServiceConditionWorkersHealthy)
		require.NotNil(t, c)
		assert.Equal(t, metav1.ConditionFalse, c.Status)
		assert.Equal(t, "WorkerTerminalBadState", c.Reason)
	})
	t.Run("aggressive mode, degraded workers: the existing degraded path, not healthy", func(t *testing.T) {
		run := runStatus(t, statusMS(false, "2"), now, nil, objs...)
		require.Error(t, run.err)
		assert.True(t, isTerminalErr(run.err))
		c := cond(run.ms, v1alpha1.MiniServiceConditionObjectsHealthy)
		require.NotNil(t, c)
		assert.Equal(t, metav1.ConditionFalse, c.Status)
		assert.Equal(t, v1alpha1.MiniServiceStatusReasonDegradedWorker, c.Reason, "all replicas degraded takes the existing degraded-worker path")
		assert.NotEqual(t, v1alpha1.MiniServiceRunning, run.ms.Status.Phase)
	})
	t.Run("aggressive mode, other failure: the existing backoff grace period, then the instance fails", func(t *testing.T) {
		podFailed := func(name string) *corev1.Pod {
			p := replicaPod(name, now, func(p *corev1.Pod) {})
			p.Status.Phase = corev1.PodFailed
			return p
		}
		failedObjs := append(ownedUtilsDeployment(), podFailed("utils-h1-a"), podFailed("utils-h1-b"))
		ms := statusMS(false, "2")

		first := runStatus(t, ms, now, nil, failedObjs...)
		assert.NoError(t, first.err, "the first failure is inside the existing backoff window")
		c := cond(first.ms, v1alpha1.MiniServiceConditionObjectsHealthy)
		require.NotNil(t, c)
		assert.Equal(t, v1alpha1.MiniServiceStatusReasonObjectsFailedWithinBackoffTimeout, c.Reason)
		assert.NotZero(t, first.res.RequeueAfter, "it requeues to check again")

		// Past the existing backoff timeout the instance fails. No new timer was introduced.
		later := runStatus(t, ms, now.Add(24*time.Hour), nil, failedObjs...)
		require.Error(t, later.err)
		assert.True(t, isTerminalErr(later.err))
		assert.Equal(t, v1alpha1.MiniServiceStatusReasonObjectsFailed, cond(later.ms, v1alpha1.MiniServiceConditionObjectsHealthy).Reason)
	})
}

func TestHAStatusReplicasStillStartingIsNotAFailure(t *testing.T) {
	now := time.Now()
	t.Run("deployment exists with no pods yet", func(t *testing.T) {
		run := runStatus(t, statusMS(true, "2"), now, nil, ownedUtilsDeployment()...)
		assert.NoError(t, run.err)
		assert.NotEqual(t, v1alpha1.MiniServiceRunning, run.ms.Status.Phase)
	})
	t.Run("both replicas starting", func(t *testing.T) {
		objs := append(ownedUtilsDeployment(), startingReplica("utils-h1-a", now), startingReplica("utils-h1-b", now))
		run := runStatus(t, statusMS(true, "2"), now, nil, objs...)
		assert.False(t, isTerminalErr(run.err))
		assert.NotEqual(t, v1alpha1.MiniServiceRunning, run.ms.Status.Phase)
	})
	t.Run("one serving while the other is starting: running", func(t *testing.T) {
		objs := append(ownedUtilsDeployment(), servingReplica("utils-h1-a", now), startingReplica("utils-h1-b", now))
		run := runStatus(t, statusMS(true, "2"), now, nil, objs...)
		require.NoError(t, run.err)
		assert.Equal(t, v1alpha1.MiniServiceRunning, run.ms.Status.Phase)
	})
}

// TestHAStatusDoesNotDependOnReadiness keeps instance health separate from permission to change HA resources: whatever
// the rollout gate says, a serving instance is healthy.
func TestHAStatusDoesNotDependOnReadiness(t *testing.T) {
	now := time.Now()
	objs := append(ownedUtilsDeployment(), servingReplica("utils-h1-a", now), servingReplica("utils-h1-b", now))

	for name, gate := range map[string]readiness.Checkable{
		"no checker":   nil,
		"stale":        gateFor(staleS),
		"not verified": gateFor(failedS),
		"verified":     gateFor(verifiedS),
	} {
		t.Run(name, func(t *testing.T) {
			ms := statusMS(true, "2")
			run := runStatus(t, ms, now, gate, objs...)
			require.NoError(t, run.err)
			assert.Equal(t, metav1.ConditionTrue, cond(run.ms, v1alpha1.MiniServiceConditionWorkersHealthy).Status, "health is not gated")
			assert.Equal(t, v1alpha1.MiniServiceRunning, run.ms.Status.Phase)

			// The permission question is answered separately: with the gate closed the instance is degraded, with it
			// open it is not, and it is healthy either way.
			plan := planUtilsReplicas(2, recordedUtilsHAReplicas(run.ms), gate)
			assert.Equal(t, !readiness.Open(gate), plan.Degraded, "degraded exactly when the gate is closed")
		})
	}
}

// TestHAStatusAfterAnNVCARestart checks that status is derived from the cluster, not from memory: a fresh Reconciler
// reaches the same answer, with the HA record and without it (NVCA stopped between creating the Deployment and recording).
func TestHAStatusAfterAnNVCARestart(t *testing.T) {
	now := time.Now()
	objs := append(ownedUtilsDeployment(), servingReplica("utils-h1-a", now), failingReplica("utils-h1-b", now))

	for name, recorded := range map[string]string{"recorded": "2", "not recorded yet": ""} {
		t.Run(name, func(t *testing.T) {
			run := runStatus(t, statusMS(true, recorded), now, nil, objs...)
			require.NoError(t, run.err)
			c := cond(run.ms, v1alpha1.MiniServiceConditionWorkersHealthy)
			require.NotNil(t, c)
			assert.Equal(t, metav1.ConditionTrue, c.Status)
		})
	}

	t.Run("a recorded HA instance whose Deployment is gone is not found, as for a missing utils pod", func(t *testing.T) {
		run := runStatus(t, statusMS(true, "2"), now, nil)
		require.Error(t, run.err)
		assert.True(t, isTerminalErr(run.err))
		assert.Equal(t, "WorkerNotFound", cond(run.ms, v1alpha1.MiniServiceConditionWorkersHealthy).Reason)
	})
	t.Run("a chart Deployment named utils is not the utils Deployment", func(t *testing.T) {
		chart := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: UtilsDeploymentName, Namespace: statusHANS, UID: "chart-dep"}}
		run := runStatus(t, statusMS(true, ""), now, nil, chart, servingReplica("utils-h1-a", now))
		require.Error(t, run.err, "no bare utils pod and no owned Deployment")
		assert.Equal(t, "WorkerNotFound", cond(run.ms, v1alpha1.MiniServiceConditionWorkersHealthy).Reason)
	})
	t.Run("a pod outside the ownership chain that looks healthy is not a replica", func(t *testing.T) {
		forged := servingReplica("forged", now)
		forged.OwnerReferences = nil
		run := runStatus(t, statusMS(true, "2"), now, nil, append(ownedUtilsDeployment(), forged, failingReplica("utils-h1-b", now))...)
		require.Error(t, run.err, "the only real replica is failing, the forged pod must not keep the instance alive")
		assert.True(t, isTerminalErr(run.err))
	})
}

// terminating marks a pod as being deleted. The fake client requires a finalizer on an object with a deletion timestamp.
func terminating(p *corev1.Pod) *corev1.Pod {
	now := metav1.NewTime(time.Now())
	p.DeletionTimestamp = &now
	p.Finalizers = []string{"test/terminating"}
	return p
}

// TestTerminatingReplicasNeverCountAsServing is the regression for a ready worker container on a pod that is being
// deleted: it must not keep the instance reported healthy while its replacement is still pending.
func TestTerminatingReplicasNeverCountAsServing(t *testing.T) {
	now := time.Now()
	tc := (&nvcak8sutil.TimeConfig{}).Complete()
	statusesOf := func(pods ...*corev1.Pod) []ObjectStatus {
		out := make([]ObjectStatus, len(pods))
		for i, p := range pods {
			out[i] = getPodStatus(p, tc, time.Hour)
		}
		return out
	}

	t.Run("the only ready replica is terminating and its replacement is pending", func(t *testing.T) {
		old := terminating(servingReplica("utils-h1-a", now))
		replacement := startingReplica("utils-h1-b", now)
		s := summarizeUtilsReplicas(1, []*corev1.Pod{old, replacement}, statusesOf(old, replacement))
		assert.EqualValues(t, 0, s.Serving, "a terminating pod is not serving, whatever its Ready flag says")
		assert.EqualValues(t, 1, s.Terminating)
		assert.EqualValues(t, 1, s.Starting)
		assert.True(t, s.Status.Pending)
		assert.False(t, s.Status.TerminalBad, "a replacement is coming")
		assert.NotEqual(t, statusRunning, s.Status.Status)
		assert.Contains(t, s.Status.Reason, "1 terminating")
	})
	t.Run("through status: the instance is not running until the replacement serves", func(t *testing.T) {
		objs := append(ownedUtilsDeployment(), terminating(servingReplica("utils-h1-a", now)), startingReplica("utils-h1-b", now))
		for _, byWorker := range []bool{true, false} {
			run := runStatus(t, statusMS(byWorker, "2"), now, nil, objs...)
			assert.False(t, isTerminalErr(run.err), "byWorkerReadiness=%v", byWorker)
			assert.NotEqual(t, v1alpha1.MiniServiceRunning, run.ms.Status.Phase, "byWorkerReadiness=%v", byWorker)
			if byWorker {
				if c := cond(run.ms, v1alpha1.MiniServiceConditionWorkersHealthy); c != nil {
					assert.NotEqual(t, metav1.ConditionTrue, c.Status, "workers must not be healthy on a terminating replica")
				}
			}
		}

		// Once the replacement serves, the instance is healthy again and the terminating pod is only reported.
		objs = append(ownedUtilsDeployment(), terminating(servingReplica("utils-h1-a", now)), servingReplica("utils-h1-b", now))
		run := runStatus(t, statusMS(true, "2"), now, nil, objs...)
		require.NoError(t, run.err)
		c := cond(run.ms, v1alpha1.MiniServiceConditionWorkersHealthy)
		require.NotNil(t, c)
		assert.Equal(t, metav1.ConditionTrue, c.Status)
		assert.Contains(t, c.Message, "1 of 1 utils replicas serving, 1 terminating")
	})
	t.Run("a terminating replica next to a failing one: the failing one is the only active replica", func(t *testing.T) {
		old := terminating(servingReplica("utils-h1-a", now))
		bad := failingReplica("utils-h1-b", now)
		s := summarizeUtilsReplicas(1, []*corev1.Pod{old, bad}, statusesOf(old, bad))
		assert.True(t, s.Status.TerminalBad, "no active replica can serve")
		assert.Contains(t, s.Status.Reason, "all 1 utils replicas failed")
	})
	t.Run("every replica terminating with no replacement yet is pending, not a failure", func(t *testing.T) {
		a, b := terminating(servingReplica("utils-h1-a", now)), terminating(servingReplica("utils-h1-b", now))
		s := summarizeUtilsReplicas(2, []*corev1.Pod{a, b}, statusesOf(a, b))
		assert.EqualValues(t, 0, s.Serving)
		assert.True(t, s.Status.Pending)
		assert.False(t, s.Status.TerminalBad)
		assert.True(t, s.Status.Scheduling, "waiting for new replicas to be created and scheduled")
	})
	t.Run("a terminating replica with a ready worker does not let the install proceed", func(t *testing.T) {
		ms, icms := statusMS(false, "2"), &nvcav2beta1.ICMSRequest{}
		old := initCompletePod("utils-h1-a", now)
		old.OwnerReferences = servingReplica("x", now).OwnerReferences
		terminating(old)
		proceed, res, err := pathsReconciler(t, append(ownedUtilsDeployment(), old)...).haUtilsInitState(context.Background(), ms, icms)
		assert.False(t, proceed)
		assert.NoError(t, err)
		assert.False(t, isTerminalErr(err))
		_ = res
	})
}

// ownedByNewReplicaSet moves a replica pod to a second ReplicaSet of the same Deployment, as in a rolling update.
func ownedByNewReplicaSet(p *corev1.Pod) *corev1.Pod {
	p.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "utils-h2", UID: "status-rs2-uid", Controller: ptr.To(true)}}
	return p
}

// TestHAStatusDuringARollingUpdate walks the maxSurge 0, maxUnavailable 1 sequence for two replicas. Pods of the old
// and the new ReplicaSet are both replicas of the instance, and at least one serves at every step.
func TestHAStatusDuringARollingUpdate(t *testing.T) {
	now := time.Now()
	newRS := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "utils-h2", Namespace: statusHANS, UID: "status-rs2-uid",
		OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: UtilsDeploymentName, UID: "status-dep-uid", Controller: ptr.To(true)}}}}
	base := append(ownedUtilsDeployment(), newRS)

	steps := []struct {
		name        string
		pods        []client.Object
		wantRunning bool
	}{
		{"one old replica is replaced: the other old one serves, the new one starts", []client.Object{
			servingReplica("utils-h1-a", now), terminating(servingReplica("utils-h1-b", now)), ownedByNewReplicaSet(startingReplica("utils-h2-c", now)),
		}, true},
		{"the new replica serves, the second old one is next", []client.Object{
			servingReplica("utils-h1-a", now), ownedByNewReplicaSet(servingReplica("utils-h2-c", now)),
		}, true},
		{"the second old replica terminates while its replacement starts: the new one serves", []client.Object{
			terminating(servingReplica("utils-h1-a", now)), ownedByNewReplicaSet(servingReplica("utils-h2-c", now)), ownedByNewReplicaSet(startingReplica("utils-h2-d", now)),
		}, true},
		{"done: both replicas are new and serve", []client.Object{
			ownedByNewReplicaSet(servingReplica("utils-h2-c", now)), ownedByNewReplicaSet(servingReplica("utils-h2-d", now)),
		}, true},
		{"a stalled update that leaves only terminating and starting pods is not running", []client.Object{
			terminating(servingReplica("utils-h1-a", now)), ownedByNewReplicaSet(startingReplica("utils-h2-c", now)),
		}, false},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			for _, byWorker := range []bool{true, false} {
				run := runStatus(t, statusMS(byWorker, "2"), now, nil, append(append([]client.Object{}, base...), step.pods...)...)
				assert.False(t, isTerminalErr(run.err), "byWorkerReadiness=%v", byWorker)
				if step.wantRunning {
					assert.Equal(t, v1alpha1.MiniServiceRunning, run.ms.Status.Phase, "byWorkerReadiness=%v", byWorker)
				} else {
					assert.NotEqual(t, v1alpha1.MiniServiceRunning, run.ms.Status.Phase, "byWorkerReadiness=%v", byWorker)
				}
			}
		})
	}
}

// staleReady marks a pod as on an unreachable node: the kubelet's last container status still says ready, and the node
// controller has set the pod Ready condition to false.
func staleReady(p *corev1.Pod, now time.Time) *corev1.Pod {
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == corev1.PodReady {
			p.Status.Conditions[i].Status = corev1.ConditionFalse
			p.Status.Conditions[i].LastTransitionTime = metav1.Time{Time: now.Add(-time.Minute)}
		}
	}
	return p
}

// TestReplicaOnAnUnreachableNodeDoesNotServe is the regression for a live node-loss test: the lost replica kept a ready
// container status for minutes and NVCA counted it as serving.
func TestReplicaOnAnUnreachableNodeDoesNotServe(t *testing.T) {
	now := time.Now()
	tc := (&nvcak8sutil.TimeConfig{}).Complete()
	statusesOf := func(pods ...*corev1.Pod) []ObjectStatus {
		out := make([]ObjectStatus, len(pods))
		for i, p := range pods {
			out[i] = getPodStatus(p, tc, time.Hour)
		}
		return out
	}
	t.Run("the lost replica does not count, the survivor serves", func(t *testing.T) {
		lost, alive := staleReady(servingReplica("utils-h1-a", now), now), servingReplica("utils-h1-b", now)
		s := summarizeUtilsReplicas(2, []*corev1.Pod{lost, alive}, statusesOf(lost, alive))
		assert.EqualValues(t, 1, s.Serving)
		assert.EqualValues(t, 1, s.Starting)
		assert.False(t, s.Status.TerminalBad)
	})
	t.Run("when every replica is on a lost node nothing is serving", func(t *testing.T) {
		a, b := staleReady(servingReplica("utils-h1-a", now), now), staleReady(servingReplica("utils-h1-b", now), now)
		s := summarizeUtilsReplicas(2, []*corev1.Pod{a, b}, statusesOf(a, b))
		assert.EqualValues(t, 0, s.Serving)
		assert.NotEqual(t, statusRunning, s.Status.Status)
	})
	t.Run("through status the instance is not running on a lost replica alone", func(t *testing.T) {
		objs := append(ownedUtilsDeployment(), staleReady(servingReplica("utils-h1-a", now), now), staleReady(servingReplica("utils-h1-b", now), now))
		run := runStatus(t, statusMS(false, "2"), now, nil, objs...)
		assert.NotEqual(t, v1alpha1.MiniServiceRunning, run.ms.Status.Phase)
	})
	t.Run("unknown or missing Ready condition with a stale ready container is not serving", func(t *testing.T) {
		unknown := servingReplica("utils-h1-a", now)
		for i := range unknown.Status.Conditions {
			if unknown.Status.Conditions[i].Type == corev1.PodReady {
				unknown.Status.Conditions[i].Status = corev1.ConditionUnknown
			}
		}
		missing := servingReplica("utils-h1-b", now)
		kept := missing.Status.Conditions[:0]
		for _, c := range missing.Status.Conditions {
			if c.Type != corev1.PodReady {
				kept = append(kept, c)
			}
		}
		missing.Status.Conditions = kept
		s := summarizeUtilsReplicas(2, []*corev1.Pod{unknown, missing}, statusesOf(unknown, missing))
		assert.EqualValues(t, 0, s.Serving)
		assert.False(t, podReadyConditionTrue(unknown))
		assert.False(t, podReadyConditionTrue(missing))
	})
	t.Run("a pod with the Ready condition true and a ready container serves", func(t *testing.T) {
		p := servingReplica("utils-h1-a", now)
		s := summarizeUtilsReplicas(2, []*corev1.Pod{p}, statusesOf(p))
		assert.EqualValues(t, 1, s.Serving)
	})
}

// unschedulableReplica is a replica pod that no node can take, created age ago.
func unschedulableReplica(name string, now time.Time, age time.Duration) *corev1.Pod {
	return replicaPod(name, now, func(p *corev1.Pod) {
		p.CreationTimestamp = metav1.NewTime(now.Add(-age))
		p.Status = corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
				LastTransitionTime: metav1.NewTime(now.Add(-age))}},
		}
	})
}

// TestAllReplicasUnschedulableFollowsTheLegacyTimeout pins the intended recovery rule for an instance with no replica
// that can run. A pod that cannot be scheduled is terminal only after the pod scheduling threshold (10 minutes by
// default, measured from the pod creation time), exactly as for the bare utils pod. After that every replica is terminal,
// the instance takes the existing failure path, and the existing 90 second backoff applies to terminal statuses. Before
// it the instance is waiting, not failing. A live run held both replicas unschedulable for 3 minutes without failure,
// which is this rule, not a missed 90 second policy.
func TestAllReplicasUnschedulableFollowsTheLegacyTimeout(t *testing.T) {
	now := time.Now()
	tc := (&nvcak8sutil.TimeConfig{}).Complete()
	threshold := tc.PodScheduledThreshold
	require.Equal(t, 10*time.Minute, threshold, "the documented default")

	for _, tt := range []struct {
		name         string
		age          time.Duration
		wantTerminal bool
	}{
		{"3 minutes, as in the live run", 3 * time.Minute, false},
		{"just under the threshold", threshold - time.Minute, false},
		{"past the threshold", threshold + time.Minute, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bare := unschedulableReplica("utils", now, tt.age)
			legacy := getPodStatus(bare, tc, threshold)
			assert.Equal(t, tt.wantTerminal, legacy.TerminalBad, "the bare utils pod")

			a, b := unschedulableReplica("utils-h1-a", now, tt.age), unschedulableReplica("utils-h1-b", now, tt.age)
			sts := []ObjectStatus{getPodStatus(a, tc, threshold), getPodStatus(b, tc, threshold)}
			s := summarizeUtilsReplicas(2, []*corev1.Pod{a, b}, sts)
			assert.Equal(t, tt.wantTerminal, s.Status.TerminalBad, "all replicas unschedulable must match the bare pod")
			assert.NotEqual(t, statusRunning, s.Status.Status)
			if !tt.wantTerminal {
				assert.True(t, s.Status.Scheduling)
			}
		})
	}
}

// A lost replica that is never replaced (for example a replacement that cannot authenticate) must be visible in the
// MiniService status, while the instance stays healthy on the survivor.
func TestHAStatusReportsMissingReplicas(t *testing.T) {
	now := time.Now()

	t.Run("one of two serving, replacement failing: degraded, still healthy", func(t *testing.T) {
		objs := append(ownedUtilsDeployment(), servingReplica("utils-h1-a", now), failingReplica("utils-h1-b", now))
		run := runStatus(t, statusMS(true, "2"), now, nil, objs...)
		require.NoError(t, run.err)
		c := cond(run.ms, ConditionUtilsReplicasDegraded)
		require.NotNil(t, c)
		assert.Equal(t, metav1.ConditionTrue, c.Status)
		assert.Equal(t, "ReplicasMissing", c.Reason)
		assert.Contains(t, c.Message, "1 of 2 desired utils replicas serving")
		assert.Contains(t, c.Message, "failed 1")
		assert.Equal(t, metav1.ConditionTrue, cond(run.ms, v1alpha1.MiniServiceConditionWorkersHealthy).Status)
		assert.Equal(t, v1alpha1.MiniServiceRunning, run.ms.Status.Phase)
	})

	t.Run("replacement pod never created: counted against desired, not against pods found", func(t *testing.T) {
		objs := append(ownedUtilsDeployment(), servingReplica("utils-h1-a", now))
		run := runStatus(t, statusMS(false, "2"), now, nil, objs...)
		require.NoError(t, run.err)
		c := cond(run.ms, ConditionUtilsReplicasDegraded)
		require.NotNil(t, c)
		assert.Equal(t, metav1.ConditionTrue, c.Status)
		assert.Contains(t, c.Message, "1 of 2 desired utils replicas serving")
	})

	t.Run("all desired serving and never degraded: no condition added", func(t *testing.T) {
		objs := append(ownedUtilsDeployment(), servingReplica("utils-h1-a", now), servingReplica("utils-h1-b", now))
		run := runStatus(t, statusMS(true, "2"), now, nil, objs...)
		require.NoError(t, run.err)
		assert.Nil(t, cond(run.ms, ConditionUtilsReplicasDegraded))
	})

	t.Run("recovered: condition set False", func(t *testing.T) {
		ms := statusMS(true, "2")
		meta.SetStatusCondition(&ms.Status.Conditions, metav1.Condition{
			Type: ConditionUtilsReplicasDegraded, Status: metav1.ConditionTrue, Reason: "ReplicasMissing",
		})
		objs := append(ownedUtilsDeployment(), servingReplica("utils-h1-a", now), servingReplica("utils-h1-b", now))
		run := runStatus(t, ms, now, nil, objs...)
		require.NoError(t, run.err)
		c := cond(run.ms, ConditionUtilsReplicasDegraded)
		require.NotNil(t, c)
		assert.Equal(t, metav1.ConditionFalse, c.Status)
		assert.Equal(t, "ReplicasServing", c.Reason)
	})
}

func TestUtilsDegradedTracker(t *testing.T) {
	var tr utilsDegradedTracker
	a := types.NamespacedName{Name: "a"}
	b := types.NamespacedName{Name: "b"}
	assert.Equal(t, 0, tr.set(a, false), "clearing an unknown instance on the zero value is safe")
	assert.Equal(t, 1, tr.set(a, true))
	assert.Equal(t, 1, tr.set(a, true), "the same instance counts once")
	assert.Equal(t, 2, tr.set(b, true))
	assert.Equal(t, 1, tr.set(a, false))
	assert.Equal(t, 0, tr.set(b, false))
}

// A recorded HA instance whose Deployment is gone must not keep a stale healthy degraded signal, in both status modes.
func TestHAStatusDeploymentMissing(t *testing.T) {
	now := time.Now()
	for _, byWorkerReadiness := range []bool{true, false} {
		t.Run(fmt.Sprintf("worker readiness mode %v", byWorkerReadiness), func(t *testing.T) {
			ms := statusMS(byWorkerReadiness, "2")
			meta.SetStatusCondition(&ms.Status.Conditions, metav1.Condition{
				Type: ConditionUtilsReplicasDegraded, Status: metav1.ConditionFalse, Reason: "ReplicasServing",
			})
			run := runStatus(t, ms, now, nil)
			c := cond(run.ms, ConditionUtilsReplicasDegraded)
			require.NotNil(t, c)
			assert.Equal(t, metav1.ConditionTrue, c.Status)
			assert.Equal(t, "DeploymentMissing", c.Reason)
			assert.Contains(t, c.Message, "0 of 2 desired utils replicas serving")
		})
	}
}

// The degraded-instances gauge counts a recorded HA instance whose Deployment is missing, and drops it once the
// MiniService is deleted (Reconcile sees NotFound).
func TestHADeploymentMissingGauge(t *testing.T) {
	now := time.Now()
	m := metrics.NewDefaultMetrics("test-nca-id", "test-cluster", "test-group", "test-version",
		metrics.WithRegisterer(prometheus.NewRegistry()))
	gauge := func() float64 {
		var d promdto.Metric
		require.NoError(t, m.UtilsReplicasDegradedInstances.WithLabelValues(m.WithDefaultLabelValues()...).Write(&d))
		return d.GetGauge().GetValue()
	}
	require.Equal(t, 0.0, gauge(), "initialized to zero")

	ms := statusMS(true, "2")
	r, ctx, icmsReq := statusReconciler(t, ms, now, nil, m)
	_, _ = r.doStatus(ctx, ms, icmsReq)
	assert.Equal(t, 1.0, gauge(), "missing Deployment counts as degraded")
	_, _ = r.doStatus(ctx, ms, icmsReq)
	assert.Equal(t, 1.0, gauge(), "the same instance counts once")

	// The MiniService is not in the fake client, so Reconcile takes the deleted path and forgets the instance.
	_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(ms)})
	require.NoError(t, err)
	assert.Equal(t, 0.0, gauge(), "deleted MiniService no longer counted")
}
