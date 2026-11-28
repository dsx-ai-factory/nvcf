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

package rollout

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

const (
	// StateConfigMapName is the ConfigMap that holds the rollout State in the NVCA system namespace.
	StateConfigMapName = "nvca-utils-provenance-rollout"
	stateKey           = "state.json"
)

// ConfigMapStore persists the State in a ConfigMap, so the record survives NVCA restarts.
type ConfigMapStore struct {
	Client    kubernetes.Interface
	Namespace string
}

// Load implements Store. A missing ConfigMap is the zero State.
func (s ConfigMapStore) Load(ctx context.Context) (State, error) {
	cm, err := s.Client.CoreV1().ConfigMaps(s.Namespace).Get(ctx, StateConfigMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return State{}, nil
	}
	if err != nil {
		return State{}, err
	}
	raw, ok := cm.Data[stateKey]
	if !ok {
		return State{}, nil
	}
	var st State
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		return State{}, fmt.Errorf("decode %s: %w", StateConfigMapName, err)
	}
	return st, nil
}

// Save implements Store.
func (s ConfigMapStore) Save(ctx context.Context, st State) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: StateConfigMapName, Namespace: s.Namespace},
		Data:       map[string]string{stateKey: string(raw)},
	}
	_, err = s.Client.CoreV1().ConfigMaps(s.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) {
		_, err = s.Client.CoreV1().ConfigMaps(s.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	}
	return err
}

// TrustedDigests is the desired state NVCA recorded for the HA utils of one instance: the digests of the pod spec, of the
// containers and of the rest of the Deployment spec, and the replica count.
type TrustedDigests struct {
	PodSpec, Containers, DeploymentSpec string
	Replicas                            int32
	// DeploymentUID is the UID of the Deployment NVCA created. Only ReplicaSets it controls count towards its replicas.
	DeploymentUID types.UID
}

func (d TrustedDigests) complete() bool {
	return d.PodSpec != "" && d.Containers != "" && d.DeploymentSpec != "" && d.Replicas > 0 && d.DeploymentUID != ""
}

// controlledBy reports whether refs has a controller reference of the given kind and UID.
func controlledBy(refs []metav1.OwnerReference, kind string, uid types.UID) bool {
	for _, r := range refs {
		if r.Kind == kind && r.UID == uid && r.Controller != nil && *r.Controller {
			return true
		}
	}
	return false
}

// Trust returns the digests recorded for the instance in namespace. ok is false when there is none.
type Trust interface {
	UtilsTrust(ctx context.Context, namespace string) (d TrustedDigests, ok bool, err error)
}

// KubeLister lists the reserved-metadata objects of every instance namespace. Trust may be nil, then no object's content
// is verified.
type KubeLister struct {
	Client kubernetes.Interface
	Trust  Trust
}

type reservedKeys struct{ label, annotation, owner, templateLabel bool }

func (r reservedKeys) any() bool { return r.label || r.annotation || r.owner || r.templateLabel }
func (r reservedKeys) onlyAnnotation() bool {
	return r.annotation && !r.label && !r.owner && !r.templateLabel
}

func keysOf(m *metav1.ObjectMeta, template *metav1.ObjectMeta) reservedKeys {
	var k reservedKeys
	_, k.label = m.Labels[nvcatypes.PodRoleLabelKey]
	k.annotation = m.Annotations[nvcatypes.InfraObjectAnnotationKey] != ""
	for _, o := range m.OwnerReferences {
		if o.Kind == "MiniService" {
			k.owner = true
		}
	}
	if template != nil {
		_, k.templateLabel = template.Labels[nvcatypes.PodRoleLabelKey]
		if template.Annotations[nvcatypes.InfraObjectAnnotationKey] != "" {
			k.annotation = true
		}
	}
	return k
}

// ReservedObjects implements Lister.
func (l KubeLister) ReservedObjects(ctx context.Context) ([]Object, error) {
	selector := nvcatypes.WorkloadInstanceTypeLabel + "=" + nvcatypes.WorkloadInstanceTypeValueMiniService
	nss, err := l.Client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list instance namespaces: %w", err)
	}
	var out []Object
	var trusted TrustedDigests
	add := func(kind, ns string, m *metav1.ObjectMeta, template *metav1.ObjectMeta, verified bool) {
		if k := keysOf(m, template); k.any() {
			modified, known := reservedModifiedAt(m.ManagedFields)
			out = append(out, Object{
				Kind: kind, Namespace: ns, Name: m.Name, Created: m.CreationTimestamp.Time, OnlyInfraAnnotation: k.onlyAnnotation(),
				Modified: modified, ModifiedUnknown: !known, ContentVerified: verified,
			})
		}
	}
	for _, ns := range nss.Items {
		trusted = TrustedDigests{}
		if l.Trust != nil {
			d, ok, err := l.Trust.UtilsTrust(ctx, ns.Name)
			if err != nil {
				return nil, fmt.Errorf("read the trusted utils digests of %s: %w", ns.Name, err)
			}
			if ok {
				trusted = d
			}
		}
		pods, err := l.Client.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list pods in %s: %w", ns.Name, err)
		}
		for i := range pods.Items {
			add("Pod", ns.Name, &pods.Items[i].ObjectMeta, nil, podContentMatches(&pods.Items[i], trusted))
		}
		rss, err := l.Client.AppsV1().ReplicaSets(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list replicasets in %s: %w", ns.Name, err)
		}
		// Only the ReplicaSets of the recorded utils Deployment count. The function's own workloads run ReplicaSets in the
		// same namespace, and their replicas say nothing about the utils replicas.
		var rsTotal int32
		for i := range rss.Items {
			if controlledBy(rss.Items[i].OwnerReferences, "Deployment", trusted.DeploymentUID) {
				rsTotal += replicasOf(rss.Items[i].Spec.Replicas)
			}
		}
		for i := range rss.Items {
			rs := &rss.Items[i]
			add("ReplicaSet", ns.Name, &rs.ObjectMeta, &rs.Spec.Template.ObjectMeta, replicaSetContentMatches(rs, rsTotal, trusted))
		}
		deps, err := l.Client.AppsV1().Deployments(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list deployments in %s: %w", ns.Name, err)
		}
		for i := range deps.Items {
			add("Deployment", ns.Name, &deps.Items[i].ObjectMeta, &deps.Items[i].Spec.Template.ObjectMeta, deploymentContentMatches(&deps.Items[i], trusted))
		}
		sts, err := l.Client.AppsV1().StatefulSets(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list statefulsets in %s: %w", ns.Name, err)
		}
		for i := range sts.Items {
			add("StatefulSet", ns.Name, &sts.Items[i].ObjectMeta, &sts.Items[i].Spec.Template.ObjectMeta, false)
		}
		jobs, err := l.Client.BatchV1().Jobs(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list jobs in %s: %w", ns.Name, err)
		}
		for i := range jobs.Items {
			add("Job", ns.Name, &jobs.Items[i].ObjectMeta, &jobs.Items[i].Spec.Template.ObjectMeta, false)
		}
		cjs, err := l.Client.BatchV1().CronJobs(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list cronjobs in %s: %w", ns.Name, err)
		}
		for i := range cjs.Items {
			add("CronJob", ns.Name, &cjs.Items[i].ObjectMeta, &cjs.Items[i].Spec.JobTemplate.Spec.Template.ObjectMeta, false)
		}
	}
	return out, nil
}

// reservedFieldMarkers are the managed-fields paths that mean a manager wrote reserved metadata.
var reservedFieldMarkers = []string{
	`"f:` + nvcatypes.PodRoleLabelKey + `"`,
	`"f:` + nvcatypes.InfraObjectAnnotationKey + `"`,
	`"f:ownerReferences"`,
}

// reservedModifiedAt returns the latest time a field manager wrote reserved metadata, from the managed fields of an
// object, ignoring the status subresource. known is false when no entry accounts for the reserved metadata, which an
// honest API server never produces for an object that carries it.
func reservedModifiedAt(entries []metav1.ManagedFieldsEntry) (latest time.Time, known bool) {
	for _, e := range entries {
		if e.Subresource == "status" || e.FieldsV1 == nil || e.Time == nil {
			continue
		}
		raw := string(e.FieldsV1.Raw)
		for _, marker := range reservedFieldMarkers {
			if strings.Contains(raw, marker) {
				known = true
				if e.Time.After(latest) {
					latest = e.Time.Time
				}
				break
			}
		}
	}
	return latest, known
}

func replicasOf(r *int32) int32 {
	if r == nil {
		return 1 // the API default
	}
	return *r
}

// podContentMatches reports whether the containers of a pod are the ones NVCA recorded.
func podContentMatches(pod *corev1.Pod, d TrustedDigests) bool {
	return d.complete() && utilsprovenance.ContainersDigest(&pod.Spec) == d.Containers
}

// replicaSetContentMatches reports whether a ReplicaSet has the recorded pod spec and a replica count the Deployment could
// have given it. A Deployment with maxSurge 0 never runs more than its replica count across all its ReplicaSets, so
// neither one ReplicaSet nor the sum of the Deployment's ReplicaSets may exceed the recorded count. A ReplicaSet that
// the recorded Deployment does not control cannot be verified.
func replicaSetContentMatches(rs *appsv1.ReplicaSet, total int32, d TrustedDigests) bool {
	return d.complete() && controlledBy(rs.OwnerReferences, "Deployment", d.DeploymentUID) &&
		utilsprovenance.PodSpecDigest(&rs.Spec.Template.Spec) == d.PodSpec &&
		replicasOf(rs.Spec.Replicas) <= d.Replicas && total <= d.Replicas
}

// deploymentContentMatches reports whether the whole Deployment is what NVCA recorded: the pod spec, the replica count
// and the rest of the spec (selector, strategy).
func deploymentContentMatches(dep *appsv1.Deployment, d TrustedDigests) bool {
	return d.complete() && dep.UID == d.DeploymentUID && utilsprovenance.PodSpecDigest(&dep.Spec.Template.Spec) == d.PodSpec &&
		replicasOf(dep.Spec.Replicas) == d.Replicas && utilsprovenance.DeploymentSpecDigest(&dep.Spec) == d.DeploymentSpec
}
