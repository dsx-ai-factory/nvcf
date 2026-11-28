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

package readiness

import (
	"context"
	"fmt"
	"sort"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"

	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

const (
	// CanaryNamespace is the namespace the operator prepares for the canary. It carries the workload instance type
	// label with the value CanaryNamespaceLabelValue, so the provenance webhook applies to it, and the MiniService
	// controller, garbage collection and other NVCA code that select the miniservice value never see it.
	CanaryNamespace           = "nvca-provenance-canary"
	CanaryNamespaceLabelValue = "provenance-canary"
	// CanaryServiceAccount is the identity the canary request is made as. It may only create pods in CanaryNamespace.
	CanaryServiceAccount = "provenance-canary"
	canaryPodName        = "provenance-canary"
)

// CanaryUsername is the username the canary request is made as.
func CanaryUsername() string {
	return "system:serviceaccount:" + CanaryNamespace + ":" + CanaryServiceAccount
}

// KubeCanary sends the canary as a server-side dry run through a client that impersonates CanaryUsername.
type KubeCanary struct {
	// Impersonated is a client whose requests are made as CanaryUsername.
	Impersonated kubernetes.Interface
}

// DryRunForgedPod creates, as a dry run, a pod that claims the reserved utils label. Nothing is persisted.
func (c KubeCanary) DryRunForgedPod(ctx context.Context) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   canaryPodName,
			Labels: map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils},
		},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken: ptr.To(false),
			Containers: []corev1.Container{{
				Name:  "canary",
				Image: "registry.k8s.io/pause:3.10",
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr.To(false),
					RunAsNonRoot:             ptr.To(true),
					RunAsUser:                ptr.To(int64(65534)),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
			}},
		},
	}
	_, err := c.Impersonated.CoreV1().Pods(CanaryNamespace).Create(ctx, pod, metav1.CreateOptions{
		DryRun: []string{metav1.DryRunAll},
	})
	return err
}

// KubeReviewer answers permission questions with SubjectAccessReviews, which evaluate every RoleBinding and
// ClusterRoleBinding that applies to the user and its groups.
type KubeReviewer struct{ Client kubernetes.Interface }

// Allowed implements Reviewer.
func (r KubeReviewer) Allowed(ctx context.Context, user string, groups []string, namespace string, right Right, name string) (bool, error) {
	sar, err := r.Client.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User: user, Groups: groups,
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace: namespace, Verb: right.Verb, Group: right.Group, Resource: right.Resource, Subresource: right.Subresource,
				Name: name,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return false, fmt.Errorf("subject access review: %w", err)
	}
	if !sar.Status.Allowed && sar.Status.EvaluationError != "" {
		// A denial with an evaluation error is not a safe denial: an authorizer may have failed before it could grant the
		// right. Treat it as inconclusive so the RBAC check stays unverified and the gate stays closed.
		return false, fmt.Errorf("subject access review for %s could not be evaluated: %s", user, sar.Status.EvaluationError)
	}
	return sar.Status.Allowed, nil
}

// KubeAccounts lists the service accounts of every instance namespace, selected by the miniservice label.
type KubeAccounts struct{ Client kubernetes.Interface }

// InstanceAccounts implements Accounts.
func (a KubeAccounts) InstanceAccounts(ctx context.Context) ([]Account, error) {
	selector := nvcatypes.WorkloadInstanceTypeLabel + "=" + nvcatypes.WorkloadInstanceTypeValueMiniService
	nss, err := a.Client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list instance namespaces: %w", err)
	}
	var out []Account
	for _, ns := range nss.Items {
		sas, err := a.Client.CoreV1().ServiceAccounts(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list service accounts in %s: %w", ns.Name, err)
		}
		for _, sa := range sas.Items {
			out = append(out, Account{Namespace: ns.Name, Name: sa.Name})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// KubeObjects lists protected objects: Deployments, ReplicaSets and pods in instance namespaces that carry the reserved
// label, the infra-object annotation, or a MiniService owner reference. The legacy utils pod carries the annotation.
type KubeObjects struct{ Client kubernetes.Interface }

func carriesReserved(m *metav1.ObjectMeta) bool {
	if _, ok := m.Labels[nvcatypes.PodRoleLabelKey]; ok {
		return true
	}
	if m.Annotations[nvcatypes.InfraObjectAnnotationKey] != "" {
		return true
	}
	for _, o := range m.OwnerReferences {
		if o.Kind == "MiniService" {
			return true
		}
	}
	return false
}

// ProtectedObjects implements Objects.
func (o KubeObjects) ProtectedObjects(ctx context.Context) ([]ProtectedObject, error) {
	selector := nvcatypes.WorkloadInstanceTypeLabel + "=" + nvcatypes.WorkloadInstanceTypeValueMiniService
	nss, err := o.Client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list instance namespaces: %w", err)
	}
	var out []ProtectedObject
	for _, ns := range nss.Items {
		deps, err := o.Client.AppsV1().Deployments(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list deployments in %s: %w", ns.Name, err)
		}
		for i := range deps.Items {
			d := &deps.Items[i]
			if carriesReserved(&d.ObjectMeta) || carriesReserved(&d.Spec.Template.ObjectMeta) {
				out = append(out, ProtectedObject{ns.Name, "apps", "deployments", d.Name})
			}
		}
		rss, err := o.Client.AppsV1().ReplicaSets(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list replicasets in %s: %w", ns.Name, err)
		}
		for i := range rss.Items {
			r := &rss.Items[i]
			if carriesReserved(&r.ObjectMeta) || carriesReserved(&r.Spec.Template.ObjectMeta) {
				out = append(out, ProtectedObject{ns.Name, "apps", "replicasets", r.Name})
			}
		}
		pods, err := o.Client.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list pods in %s: %w", ns.Name, err)
		}
		for i := range pods.Items {
			if p := &pods.Items[i]; carriesReserved(&p.ObjectMeta) {
				out = append(out, ProtectedObject{ns.Name, "", "pods", p.Name})
			}
		}
	}
	return out, nil
}

// MetricsRecorder adapts the agent metrics to Recorder.
type MetricsRecorder struct {
	Inc func(check, outcome string)
	Set func(enforcementVerified, rbacVerified bool)
}

// IncCheck implements Recorder.
func (m MetricsRecorder) IncCheck(check, outcome string) {
	if m.Inc != nil {
		m.Inc(check, outcome)
	}
}

// SetReadiness implements Recorder.
func (m MetricsRecorder) SetReadiness(e, r bool) {
	if m.Set != nil {
		m.Set(e, r)
	}
}
