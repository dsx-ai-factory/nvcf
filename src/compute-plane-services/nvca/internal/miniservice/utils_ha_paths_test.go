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
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/common"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcak8sutil "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/util/k8sutil"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	nvcav2beta1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v2beta1"
)

func pathsReconciler(t *testing.T, objs ...client.Object) *Reconciler {
	t.Helper()
	all := append([]client.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: statusHANS}}}, objs...)
	c, _ := newFakeClient(mgrScheme, all...)
	return &Reconciler{
		ControllerOptions: ControllerOptions{K8sTimeConfig: (&nvcak8sutil.TimeConfig{}).Complete()},
		Client:            c,
	}
}

func initCompletePod(name string, now time.Time) *corev1.Pod {
	p := servingReplica(name, now)
	p.OwnerReferences = nil
	p.Spec.InitContainers = []corev1.Container{{Name: "init"}}
	p.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "init", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
	return p
}

func TestUtilsInitStateBarePodBehaviorIsUnchanged(t *testing.T) {
	now := time.Now()
	ms, icms := statusMS(false, ""), &nvcav2beta1.ICMSRequest{}
	ctx := context.Background()

	t.Run("init complete proceeds", func(t *testing.T) {
		r := pathsReconciler(t, initCompletePod(common.UtilsPodName, now))
		proceed, _, err := r.utilsInitState(ctx, ms, icms, logr.Discard(), false)
		assert.True(t, proceed)
		assert.NoError(t, err)
	})
	t.Run("init running waits without error", func(t *testing.T) {
		p := startingReplica(common.UtilsPodName, now)
		p.OwnerReferences = nil
		p.Spec.InitContainers = []corev1.Container{{Name: "init"}}
		proceed, res, err := pathsReconciler(t, p).utilsInitState(ctx, ms, icms, logr.Discard(), false)
		assert.False(t, proceed)
		assert.False(t, res.Requeue)
		assert.NoError(t, err)
	})
	t.Run("a terminally bad utils pod is terminal", func(t *testing.T) {
		p := initCompletePod(common.UtilsPodName, now)
		p.Status.Phase = corev1.PodFailed
		_, _, err := pathsReconciler(t, p).utilsInitState(ctx, ms, icms, logr.Discard(), false)
		assert.True(t, isTerminalErr(err))
	})
	t.Run("no utils object: an install waits, an update is terminal", func(t *testing.T) {
		r := pathsReconciler(t)
		proceed, res, err := r.utilsInitState(ctx, ms, icms, logr.Discard(), false)
		assert.False(t, proceed)
		assert.True(t, res.Requeue, "an install waits for creation")
		assert.NoError(t, err)

		proceed, _, err = r.utilsInitState(ctx, ms, icms, logr.Discard(), true)
		assert.False(t, proceed)
		assert.True(t, isTerminalErr(err), "an update expects the utils objects to exist")
	})
}

func TestUtilsInitStateForAnHAInstance(t *testing.T) {
	now := time.Now()
	ms, icms := statusMS(false, "2"), &nvcav2beta1.ICMSRequest{}
	ctx := context.Background()
	ha := func(pods ...*corev1.Pod) []client.Object {
		objs := ownedUtilsDeployment()
		for _, p := range pods {
			objs = append(objs, p)
		}
		return objs
	}
	replica := func(name string) *corev1.Pod {
		p := initCompletePod(name, now)
		p.OwnerReferences = []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "utils-h1", UID: "status-rs-uid", Controller: ptr.To(true)}}
		return p
	}

	for _, missingIsTerminal := range []bool{false, true} {
		name := map[bool]string{false: "install", true: "update"}[missingIsTerminal]
		t.Run(name+": a replica with init complete proceeds", func(t *testing.T) {
			proceed, _, err := pathsReconciler(t, ha(replica("utils-h1-a"))...).utilsInitState(ctx, ms, icms, logr.Discard(), missingIsTerminal)
			assert.True(t, proceed)
			assert.NoError(t, err)
		})
		t.Run(name+": the Deployment exists but no replica yet waits, never terminal", func(t *testing.T) {
			proceed, res, err := pathsReconciler(t, ha()...).utilsInitState(ctx, ms, icms, logr.Discard(), missingIsTerminal)
			assert.False(t, proceed)
			assert.True(t, res.Requeue)
			assert.NoError(t, err)
		})
	}
	t.Run("a chart Deployment named utils is not the utils objects", func(t *testing.T) {
		chart := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: UtilsDeploymentName, Namespace: statusHANS, UID: "chart"}}
		_, _, err := pathsReconciler(t, chart).utilsInitState(ctx, ms, icms, logr.Discard(), true)
		assert.True(t, isTerminalErr(err), "an update with no real utils objects is terminal")
	})
}

func TestUtilsTerminationGracePeriod(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	ms := statusMS(false, "")

	t.Run("bare pod", func(t *testing.T) {
		p := servingReplica(common.UtilsPodName, now)
		p.OwnerReferences = nil
		p.Spec.TerminationGracePeriodSeconds = ptr.To(int64(90))
		d, ok := pathsReconciler(t, p).utilsTerminationGracePeriod(ctx, ms)
		assert.True(t, ok)
		assert.Equal(t, 90*time.Second, d)
	})
	t.Run("HA instance: from the Deployment template", func(t *testing.T) {
		objs := ownedUtilsDeployment()
		objs[0].(*appsv1.Deployment).Spec.Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(120))
		d, ok := pathsReconciler(t, objs...).utilsTerminationGracePeriod(ctx, ms)
		assert.True(t, ok)
		assert.Equal(t, 120*time.Second, d)
	})
	t.Run("HA instance whose Deployment is gone: from a replica", func(t *testing.T) {
		objs := ownedUtilsDeployment()
		objs[0].(*appsv1.Deployment).Spec.Template.Spec.TerminationGracePeriodSeconds = nil
		p := servingReplica("utils-h1-a", now)
		p.Spec.TerminationGracePeriodSeconds = ptr.To(int64(45))
		d, ok := pathsReconciler(t, append(objs, p)...).utilsTerminationGracePeriod(ctx, ms)
		assert.True(t, ok)
		assert.Equal(t, 45*time.Second, d)
	})
	t.Run("nothing found gives zero, as before", func(t *testing.T) {
		d, ok := pathsReconciler(t).utilsTerminationGracePeriod(ctx, ms)
		assert.False(t, ok)
		assert.Zero(t, d)
	})
	t.Run("a chart Deployment named utils is ignored", func(t *testing.T) {
		chart := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: UtilsDeploymentName, Namespace: statusHANS, UID: "chart"},
			Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{TerminationGracePeriodSeconds: ptr.To(int64(999))}}}}
		_, ok := pathsReconciler(t, chart).utilsTerminationGracePeriod(ctx, ms)
		assert.False(t, ok)
	})
}

func recordJSON(rec utilsHARecord) *v1alpha1.MiniService {
	ms := statusMS(false, "")
	recordUtilsHA(ms, rec)
	return ms
}

// TestCleanupFindsReplicasAfterTheDeploymentIsGone is the regression for cleanup after the Deployment was deleted while
// replicas still terminate: the recorded Deployment UID still identifies them, and the recorded grace period needs no pod.
func TestCleanupFindsReplicasAfterTheDeploymentIsGone(t *testing.T) {
	now := time.Now()
	ctx := context.Background()
	rs := ownedUtilsDeployment()[1] // the ReplicaSet owned by the (absent) Deployment with UID status-dep-uid
	replicaWithGrace := func(name string, grace int64) *corev1.Pod {
		p := servingReplica(name, now)
		p.Spec.TerminationGracePeriodSeconds = ptr.To(grace)
		return p
	}

	t.Run("surviving replicas are discovered by the recorded Deployment UID", func(t *testing.T) {
		ms := recordJSON(utilsHARecord{Replicas: 2, DeploymentUID: "status-dep-uid"})
		r := pathsReconciler(t, rs, replicaWithGrace("utils-h1-a", 30), replicaWithGrace("utils-h1-b", 30))
		pods, err := listHAUtilsPods(ctx, r.Client, ms)
		require.NoError(t, err)
		assert.Len(t, pods, 2)
	})
	t.Run("a ReplicaSet owned by another UID is not found", func(t *testing.T) {
		other := rs.DeepCopyObject().(*appsv1.ReplicaSet)
		other.OwnerReferences[0].UID = "someone-elses"
		ms := recordJSON(utilsHARecord{Replicas: 2, DeploymentUID: "status-dep-uid"})
		pods, err := listHAUtilsPods(ctx, pathsReconciler(t, other, replicaWithGrace("utils-h1-a", 30)).Client, ms)
		require.NoError(t, err)
		assert.Empty(t, pods)
	})
	t.Run("without a recorded UID and without the Deployment nothing is found", func(t *testing.T) {
		ms := recordJSON(utilsHARecord{Replicas: 2})
		pods, err := listHAUtilsPods(ctx, pathsReconciler(t, rs, replicaWithGrace("utils-h1-a", 30)).Client, ms)
		require.NoError(t, err)
		assert.Empty(t, pods)
	})
	t.Run("the grace period comes from the record, with no Deployment and no pods", func(t *testing.T) {
		ms := recordJSON(utilsHARecord{Replicas: 2, DeploymentUID: "status-dep-uid", TerminationGracePeriodSeconds: ptr.To(int64(120))})
		d, ok := pathsReconciler(t).utilsTerminationGracePeriod(ctx, ms)
		assert.True(t, ok)
		assert.Equal(t, 120*time.Second, d)
	})
	t.Run("the record wins over replica pods, which a chart could forge", func(t *testing.T) {
		ms := recordJSON(utilsHARecord{Replicas: 2, DeploymentUID: "status-dep-uid", TerminationGracePeriodSeconds: ptr.To(int64(60))})
		r := pathsReconciler(t, rs, replicaWithGrace("utils-h1-a", 99999))
		d, ok := r.utilsTerminationGracePeriod(ctx, ms)
		assert.True(t, ok)
		assert.Equal(t, 60*time.Second, d, "a replica must not set how long cleanup waits when the record has the value")
	})
	t.Run("a record with no grace period gives none, never a pod's value", func(t *testing.T) {
		ms := recordJSON(utilsHARecord{Replicas: 2, DeploymentUID: "status-dep-uid"})
		r := pathsReconciler(t, rs, replicaWithGrace("utils-h1-a", 99999))
		d, ok := r.utilsTerminationGracePeriod(ctx, ms)
		assert.False(t, ok)
		assert.Zero(t, d)
	})
}

func TestUtilsHARecordFormat(t *testing.T) {
	dep := ownedUtilsDeployment()[0].(*appsv1.Deployment)
	dep.Spec.Template.Spec.TerminationGracePeriodSeconds = ptr.To(int64(75))
	rec := newUtilsHARecord(3, dep)
	assert.NotEmpty(t, rec.PodSpecDigest)
	assert.NotEmpty(t, rec.ContainersDigest)
	assert.NotEmpty(t, rec.DeploymentSpecDigest)
	rec.PodSpecDigest, rec.ContainersDigest, rec.DeploymentSpecDigest = "", "", "" // see TestUtilsHARecordCarriesTheTemplateDigestsAndKeepsThem
	assert.Equal(t, utilsHARecord{Replicas: 3, DeploymentUID: "status-dep-uid", TerminationGracePeriodSeconds: ptr.To(int64(75))}, rec)

	ms := statusMS(false, "")
	assert.True(t, recordUtilsHA(ms, rec))
	assert.False(t, recordUtilsHA(ms, rec), "an unchanged record reports no change")
	got, ok := recordedUtilsHA(ms)
	require.True(t, ok)
	assert.Equal(t, rec, got)
	assert.EqualValues(t, 3, recordedUtilsHAReplicas(ms))

	t.Run("a bare number from an earlier build is read as the replica count", func(t *testing.T) {
		old := statusMS(false, "4")
		got, ok := recordedUtilsHA(old)
		require.True(t, ok)
		assert.Equal(t, utilsHARecord{Replicas: 4}, got)
	})
	t.Run("garbage and a count below two are not a record", func(t *testing.T) {
		for _, msg := range []string{"", "many", "1", `{"replicas":1}`, `{"replicas":"x"}`} {
			ms := statusMS(false, "")
			ms.Status.Conditions = []metav1.Condition{{Type: ConditionUtilsHA, Status: metav1.ConditionTrue, Message: msg}}
			_, ok := recordedUtilsHA(ms)
			assert.False(t, ok, "%q", msg)
		}
	})
	assert.Equal(t, utilsHARecord{Replicas: 2}, newUtilsHARecord(2, nil))
}
