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
	"strings"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

// Modes for chart-originated objects that carry reserved utils metadata. They mirror the provenance webhook modes.
const (
	ReservedMetadataModeAudit   = "audit"
	ReservedMetadataModeEnforce = "enforce"
)

// reservedUtilsMetadata reports which reserved utils keys metadata m carries. Only NVCA may set them, and chart
// objects must never carry them: the label and annotation grant utils treatment, and a MiniService owner reference
// claims to be the instance.
func reservedUtilsMetadata(m *metav1.ObjectMeta) (found []string) {
	if _, ok := m.Labels[nvcatypes.PodRoleLabelKey]; ok {
		found = append(found, "label:"+nvcatypes.PodRoleLabelKey)
	}
	if _, ok := m.Annotations[nvcatypes.InfraObjectAnnotationKey]; ok {
		found = append(found, "annotation:"+nvcatypes.InfraObjectAnnotationKey)
	}
	for _, o := range m.OwnerReferences {
		if o.Kind == "MiniService" {
			found = append(found, "ownerReference:MiniService")
			break
		}
	}
	return found
}

func stripReservedUtilsMetadata(m *metav1.ObjectMeta) {
	delete(m.Labels, nvcatypes.PodRoleLabelKey)
	delete(m.Annotations, nvcatypes.InfraObjectAnnotationKey)
	kept := m.OwnerReferences[:0]
	for _, o := range m.OwnerReferences {
		if o.Kind != "MiniService" {
			kept = append(kept, o)
		}
	}
	m.OwnerReferences = kept
}

// metadataOf returns the metadata of the object itself and of every pod template it embeds.
func metadataOf(o client.Object) []*metav1.ObjectMeta {
	metas := []*metav1.ObjectMeta{}
	switch t := o.(type) {
	case *appsv1.Deployment:
		metas = append(metas, &t.Spec.Template.ObjectMeta)
	case *appsv1.ReplicaSet:
		metas = append(metas, &t.Spec.Template.ObjectMeta)
	case *appsv1.StatefulSet:
		metas = append(metas, &t.Spec.Template.ObjectMeta)
		for i := range t.Spec.VolumeClaimTemplates {
			metas = append(metas, &t.Spec.VolumeClaimTemplates[i].ObjectMeta)
		}
	case *batchv1.Job:
		metas = append(metas, &t.Spec.Template.ObjectMeta)
	case *batchv1.CronJob:
		metas = append(metas, &t.Spec.JobTemplate.ObjectMeta, &t.Spec.JobTemplate.Spec.Template.ObjectMeta)
	case *corev1.Pod:
		// The pod is the object itself, handled by the caller.
	}
	return metas
}

// newReservedMetadataMutator returns a mutator for chart-originated objects. In audit mode it only logs what it finds.
// In enforce mode it removes the reserved keys from the object and its pod templates, so a chart cannot claim utils
// treatment. The provenance webhook is the enforcement boundary, this makes the refusal early and quiet.
func newReservedMetadataMutator(mode string, log logr.Logger) objectMutator {
	enforce := strings.EqualFold(mode, ReservedMetadataModeEnforce)
	return objectMutatorFunc(func(_ context.Context, o client.Object) error {
		self := &metav1.ObjectMeta{
			Labels: o.GetLabels(), Annotations: o.GetAnnotations(), OwnerReferences: o.GetOwnerReferences(),
		}
		var found []string
		found = append(found, reservedUtilsMetadata(self)...)
		for _, m := range metadataOf(o) {
			found = append(found, reservedUtilsMetadata(m)...)
		}
		if len(found) == 0 {
			return nil
		}
		fields := []any{
			"kind", o.GetObjectKind().GroupVersionKind().Kind, "name", o.GetName(), "namespace", o.GetNamespace(),
			"reserved", strings.Join(found, ","),
		}
		if !enforce {
			log.Info("Chart object carries reserved utils metadata, would strip it (audit mode)", fields...)
			return nil
		}
		log.Info("Stripped reserved utils metadata from a chart object", fields...)
		stripReservedUtilsMetadata(self)
		o.SetLabels(self.Labels)
		o.SetAnnotations(self.Annotations)
		o.SetOwnerReferences(self.OwnerReferences)
		for _, m := range metadataOf(o) {
			stripReservedUtilsMetadata(m)
		}
		return nil
	})
}

// chainMutators runs mutators in order and stops at the first error.
func chainMutators(ms ...objectMutator) objectMutator {
	return objectMutatorFunc(func(ctx context.Context, o client.Object) error {
		for _, m := range ms {
			if err := m.mutate(ctx, o); err != nil {
				return err
			}
		}
		return nil
	})
}
