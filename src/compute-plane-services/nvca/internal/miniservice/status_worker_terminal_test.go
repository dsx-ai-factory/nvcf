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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/miniservice/chartcache"
	nvcak8sutil "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/util/k8sutil"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	nvcav2beta1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v2beta1"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/featureflag"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/common"
)

// TestDoStatus_WorkerTerminalBadStateForImagePullBackOff is a regression test for
// NVBug 6640820: a RestartPolicy=Always function worker stuck in ImagePullBackOff
// past the worker degradation period (wdp) must transition WorkersHealthy to the
// terminal reason WorkerTerminalBadState. Previously this never fired for any
// function because IsPodDegraded's returned Containers list was only ever populated
// inside a RestartPolicy=Never-gated closure (a Go && short-circuit bug), so the
// worker-container attribution check in doStatusByWorkerReadiness always saw an
// empty list and silently fell through to WaitingOnObjectReadiness forever.
func TestDoStatus_WorkerTerminalBadStateForImagePullBackOff(t *testing.T) {
	const ns = "test-imagepull-ns"
	now := time.Now()

	// imagePullBackOffPod simulates the TC-10a repro: a previously-Ready utils pod
	// (RestartPolicy: Always) whose worker container has been stuck in
	// ImagePullBackOff, and thus PodReady=False, for 40 minutes -- longer than the
	// 30-minute default WorkerDegradationTimeout, but well within the 2-hour
	// WorkerStartupTimeout, and started 2 hours ago so it is not considered to be
	// in its initial startup period.
	imagePullBackOffPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.UtilsPodName,
			Namespace: ns,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{Name: common.UtilsContainerName},
			},
		},
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			StartTime: &metav1.Time{Time: now.Add(-2 * time.Hour)},
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
				{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, Reason: "ContainersNotReady"},
				{
					Type:               corev1.PodReady,
					Status:             corev1.ConditionFalse,
					LastTransitionTime: metav1.Time{Time: now.Add(-40 * time.Minute)},
				},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:  common.UtilsContainerName,
					Ready: false,
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "Back-off pulling image \"no-such-image:nosuchtag\"",
						},
					},
				},
			},
		},
	}

	ctx := newTestContext()

	ms := &v1alpha1.MiniService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "imagepull-test-ms",
			UID:  "imagepull-test-uid",
		},
		Spec: v1alpha1.MiniServiceSpec{
			Namespace:       ns,
			ICMSRequestName: "imagepull-test-icms",
			WorkloadConfig: &v1alpha1.WorkloadConfig{
				FeatureFlags: map[string]bool{featureflag.StatusByWorkerReadiness: true},
			},
		},
	}

	icmsReq := &nvcav2beta1.ICMSRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "imagepull-test-icms",
			Namespace: "nvcf-backend",
		},
		Spec: nvcav2beta1.ICMSRequestSpec{
			Action: common.RequestICMSInstances,
		},
	}

	c, _ := newFakeClient(mgrScheme,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
		imagePullBackOffPod,
	)

	cache := chartcache.New(t.TempDir())
	require.NoError(t, cache.Start(ctx))

	r := &Reconciler{
		ControllerOptions: ControllerOptions{
			K8sTimeConfig: (&nvcak8sutil.TimeConfig{}).Complete(),
		},
		Client:                c,
		Decoder:               serializer.NewCodecFactory(mgrScheme).UniversalDeserializer(),
		chartCache:            cache,
		newPermissionsChecker: newFakePermissionsChecker,
		now:                   time.Now,
	}

	require.NoError(t, r.saveRenderedData(ctx, ms, []byte("[]")))

	_, err := r.doStatus(ctx, ms, icmsReq)

	require.Error(t, err)
	assert.True(t, errors.Is(err, reconcile.TerminalError(nil)), "expected a terminal, non-retryable error")

	cond := meta.FindStatusCondition(ms.Status.Conditions, v1alpha1.MiniServiceConditionWorkersHealthy)
	if assert.NotNil(t, cond, "expected WorkersHealthy condition to be set") {
		assert.Equal(t, metav1.ConditionFalse, cond.Status)
		assert.Equal(t, "WorkerTerminalBadState", cond.Reason)
	}
}

// unscheduledUtilsPod returns a pending utils pod created at created whose
// PodScheduled=False condition last transitioned at transitioned. The KAI
// scheduler rewrites that transition time on every scheduling attempt, so in a
// cluster it stays within seconds of now while the pod waits.
func unscheduledUtilsPod(ns string, created, transitioned time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              common.UtilsPodName,
			Namespace:         ns,
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: common.UtilsContainerName}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.PodScheduled,
				Status:             corev1.ConditionFalse,
				Reason:             corev1.PodReasonUnschedulable,
				LastTransitionTime: metav1.NewTime(transitioned),
			}},
		},
	}
}

// doStatusAggressiveWithPod runs one aggressive-mode status check for a
// MiniService whose namespace holds only pod.
func doStatusAggressiveWithPod(t *testing.T, ns string, timeConfig *nvcak8sutil.TimeConfig,
	pod *corev1.Pod) (*v1alpha1.MiniService, reconcile.Result, error) {
	t.Helper()
	ctx := newTestContext()
	ms := &v1alpha1.MiniService{
		ObjectMeta: metav1.ObjectMeta{Name: "requeue-test-ms", UID: "requeue-test-uid"},
		Spec:       v1alpha1.MiniServiceSpec{Namespace: ns, ICMSRequestName: "requeue-test-icms"},
	}
	icmsReq := &nvcav2beta1.ICMSRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "requeue-test-icms", Namespace: "nvcf-backend"},
		Spec:       nvcav2beta1.ICMSRequestSpec{Action: common.RequestICMSInstances},
	}

	c, _ := newFakeClient(mgrScheme,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
		pod,
	)
	cache := chartcache.New(t.TempDir())
	require.NoError(t, cache.Start(ctx))

	r := &Reconciler{
		ControllerOptions:     ControllerOptions{K8sTimeConfig: timeConfig},
		Client:                c,
		Decoder:               serializer.NewCodecFactory(mgrScheme).UniversalDeserializer(),
		chartCache:            cache,
		newPermissionsChecker: newFakePermissionsChecker,
		now:                   time.Now,
	}
	require.NoError(t, r.saveRenderedData(ctx, ms, []byte("[]")))

	res, err := r.doStatus(ctx, ms, icmsReq)
	return ms, res, err
}

// TestDoStatusAggressive_RequeuesAtUnscheduledPodDeadline checks that a status
// check that finds an unscheduled pod requeues for that pod's own scheduling
// deadline, measured from the pod's creation time. A pod that becomes
// unschedulable just after the previous check would otherwise be judged stuck
// one full scheduling timeout late, and a deadline measured from the
// PodScheduled transition time would keep moving when the scheduler rewrites it.
func TestDoStatusAggressive_RequeuesAtUnscheduledPodDeadline(t *testing.T) {
	timeConfig := (&nvcak8sutil.TimeConfig{}).Complete()
	schedulingTimeout := timeConfig.PodScheduledThreshold
	untilDeadline := 30 * time.Second
	now := time.Now()

	ms, res, err := doStatusAggressiveWithPod(t, "test-requeue-ns", timeConfig,
		unscheduledUtilsPod("test-requeue-ns", now.Add(-schedulingTimeout+untilDeadline), now))
	require.NoError(t, err)

	cond := meta.FindStatusCondition(ms.Status.Conditions, v1alpha1.MiniServiceConditionObjectsHealthy)
	require.NotNil(t, cond)
	assert.Equal(t, v1alpha1.MiniServiceStatusReasonWaitingObjectReadiness, cond.Reason)
	// The stored creation time is truncated to whole seconds, so the
	// deadline can be up to a second earlier than requested.
	assert.Greater(t, res.RequeueAfter, untilDeadline-2*time.Second)
	assert.LessOrEqual(t, res.RequeueAfter, untilDeadline+podSchedulingRequeueSlack)
}

// TestDoStatusAggressive_UnscheduledPodTimesOutFromCreation checks that a pod
// that has been unschedulable since it was created longer ago than the
// scheduling timeout is reported as stuck, even though its PodScheduled=False
// transition time was rewritten a second ago.
func TestDoStatusAggressive_UnscheduledPodTimesOutFromCreation(t *testing.T) {
	timeConfig := (&nvcak8sutil.TimeConfig{}).Complete()
	schedulingTimeout := timeConfig.PodScheduledThreshold
	now := time.Now()

	ms, _, err := doStatusAggressiveWithPod(t, "test-stuck-ns", timeConfig,
		unscheduledUtilsPod("test-stuck-ns", now.Add(-schedulingTimeout-time.Minute), now.Add(-time.Second)))
	require.NoError(t, err)

	cond := meta.FindStatusCondition(ms.Status.Conditions, v1alpha1.MiniServiceConditionObjectsHealthy)
	require.NotNil(t, cond)
	assert.Equal(t, v1alpha1.MiniServiceStatusReasonObjectsFailedWithinBackoffTimeout, cond.Reason)
	assert.Contains(t, cond.Message, "PodStuckScheduling")
}
