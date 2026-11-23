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
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

func reservedMeta() metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:        "x",
		Labels:      map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils, "keep": "label"},
		Annotations: map[string]string{nvcatypes.InfraObjectAnnotationKey: "true", "keep": "annotation"},
		OwnerReferences: []metav1.OwnerReference{
			{Kind: "MiniService", Name: "ms", UID: "u"},
			{Kind: "ConfigMap", Name: "cm", UID: "c"},
		},
	}
}

func assertClean(t *testing.T, m metav1.ObjectMeta, what string) {
	t.Helper()
	assert.NotContains(t, m.Labels, nvcatypes.PodRoleLabelKey, what)
	assert.NotContains(t, m.Annotations, nvcatypes.InfraObjectAnnotationKey, what)
	for _, o := range m.OwnerReferences {
		assert.NotEqual(t, "MiniService", o.Kind, what)
	}
	assert.Equal(t, "label", m.Labels["keep"], what+": other labels are kept")
	assert.Equal(t, "annotation", m.Annotations["keep"], what+": other annotations are kept")
}

func chartObjects() map[string]client.Object {
	tmpl := func() corev1.PodTemplateSpec { return corev1.PodTemplateSpec{ObjectMeta: reservedMeta()} }
	return map[string]client.Object{
		"pod":        &corev1.Pod{ObjectMeta: reservedMeta()},
		"deployment": &appsv1.Deployment{ObjectMeta: reservedMeta(), Spec: appsv1.DeploymentSpec{Template: tmpl()}},
		"replicaset": &appsv1.ReplicaSet{ObjectMeta: reservedMeta(), Spec: appsv1.ReplicaSetSpec{Template: tmpl()}},
		"statefulset": &appsv1.StatefulSet{ObjectMeta: reservedMeta(), Spec: appsv1.StatefulSetSpec{
			Template: tmpl(), VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: reservedMeta()}}}},
		"job": &batchv1.Job{ObjectMeta: reservedMeta(), Spec: batchv1.JobSpec{Template: tmpl()}},
		"cronjob": &batchv1.CronJob{ObjectMeta: reservedMeta(), Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{
			ObjectMeta: reservedMeta(), Spec: batchv1.JobSpec{Template: tmpl()}}}},
	}
}

func TestReservedMetadataMutatorEnforceStripsEverywhere(t *testing.T) {
	m := newReservedMetadataMutator(ReservedMetadataModeEnforce, logr.Discard())
	for name, o := range chartObjects() {
		require.NoError(t, m.mutate(context.Background(), o), name)
		assertClean(t, metav1.ObjectMeta{
			Labels: o.GetLabels(), Annotations: o.GetAnnotations(), OwnerReferences: o.GetOwnerReferences()}, name)
		for _, tm := range metadataOf(o) {
			assertClean(t, *tm, name+" template")
		}
		assert.Len(t, o.GetOwnerReferences(), 1, name+": the unrelated owner reference is kept")
	}
}

func TestReservedMetadataMutatorAuditOnlyLogs(t *testing.T) {
	var logged []string
	log := funcr.New(func(prefix, args string) { logged = append(logged, args) }, funcr.Options{})
	m := newReservedMetadataMutator("", log) // empty means audit
	for name, o := range chartObjects() {
		before := o.DeepCopyObject()
		require.NoError(t, m.mutate(context.Background(), o), name)
		assert.Equal(t, before, o, name+": audit mode must not change the object")
	}
	require.NotEmpty(t, logged)
	assert.Contains(t, logged[0], "would strip it")
}

func TestReservedMetadataMutatorLeavesCleanObjectsAlone(t *testing.T) {
	var logged []string
	log := funcr.New(func(prefix, args string) { logged = append(logged, args) }, funcr.Options{})
	for _, mode := range []string{ReservedMetadataModeAudit, ReservedMetadataModeEnforce} {
		m := newReservedMetadataMutator(mode, log)
		o := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Labels: map[string]string{"a": "b"}}}
		before := o.DeepCopy()
		require.NoError(t, m.mutate(context.Background(), o))
		assert.Equal(t, before, o)
	}
	assert.Empty(t, logged, "a clean object must not log")
}

func TestChainMutatorsOrderAndError(t *testing.T) {
	var order []string
	mk := func(name string, err error) objectMutator {
		return objectMutatorFunc(func(context.Context, client.Object) error { order = append(order, name); return err })
	}
	require.NoError(t, chainMutators(mk("a", nil), mk("b", nil)).mutate(context.Background(), &corev1.Pod{}))
	assert.Equal(t, []string{"a", "b"}, order)

	order = nil
	boom := errors.New("boom")
	err := chainMutators(mk("a", boom), mk("b", nil)).mutate(context.Background(), &corev1.Pod{})
	assert.ErrorIs(t, err, boom)
	assert.Equal(t, []string{"a"}, order, "stops at the first error")
}
