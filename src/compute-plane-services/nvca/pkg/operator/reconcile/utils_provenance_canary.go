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

package operator

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance/readiness"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

// canaryResources returns the namespace, service account, Role and RoleBinding the readiness canary uses. The namespace
// carries the workload instance type label with the canary value, so the provenance webhook applies to it while the
// MiniService controller, garbage collection and everything else that selects the miniservice value ignores it. The
// service account may only create pods there: the canary proves the provenance webhook refuses, so it must not be
// stopped earlier by RBAC.
func canaryResources() (*corev1.Namespace, *corev1.ServiceAccount, *rbacv1.Role, *rbacv1.RoleBinding) {
	labels := getAppLabels()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: readiness.CanaryNamespace,
		Labels: mergeStringMaps(labels, map[string]string{
			nvcatypes.WorkloadInstanceTypeLabel: readiness.CanaryNamespaceLabelValue,
		}),
	}}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: readiness.CanaryServiceAccount, Namespace: readiness.CanaryNamespace, Labels: labels,
	}}
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: readiness.CanaryServiceAccount, Namespace: readiness.CanaryNamespace, Labels: labels},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"create"},
		}},
	}
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: readiness.CanaryServiceAccount, Namespace: readiness.CanaryNamespace, Labels: labels},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: readiness.CanaryServiceAccount},
		Subjects: []rbacv1.Subject{{
			Kind: "ServiceAccount", Name: readiness.CanaryServiceAccount, Namespace: readiness.CanaryNamespace,
		}},
	}
	return ns, sa, role, rb
}

func mergeStringMaps(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// setupUtilsProvenanceCanary prepares the readiness canary resources when the provenance mode is enforce. In audit mode
// nothing is created: the canary only has a purpose once enforcement is on.
func (bc *BackendK8sCache) setupUtilsProvenanceCanary(ctx context.Context) error {
	if mode, _ := NormalizeUtilsProvenanceMode(bc.utilsProvenance.Mode); mode != UtilsProvenanceModeEnforce {
		return nil
	}
	ns, sa, role, rb := canaryResources()
	k := bc.clients.K8s

	if _, err := k.CoreV1().Namespaces().Get(ctx, ns.Name, metav1.GetOptions{}); k8serrors.IsNotFound(err) {
		if _, err := k.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil && !k8serrors.IsAlreadyExists(err) {
			return fmt.Errorf("create canary namespace: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("get canary namespace: %w", err)
	} else if _, err := k.CoreV1().Namespaces().Update(ctx, ns, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update canary namespace: %w", err)
	}

	if _, err := k.CoreV1().ServiceAccounts(sa.Namespace).Create(ctx, sa, metav1.CreateOptions{}); err != nil && !k8serrors.IsAlreadyExists(err) {
		return fmt.Errorf("create canary service account: %w", err)
	}
	if _, err := k.RbacV1().Roles(role.Namespace).Create(ctx, role, metav1.CreateOptions{}); k8serrors.IsAlreadyExists(err) {
		if _, err := k.RbacV1().Roles(role.Namespace).Update(ctx, role, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update canary role: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("create canary role: %w", err)
	}
	if _, err := k.RbacV1().RoleBindings(rb.Namespace).Create(ctx, rb, metav1.CreateOptions{}); k8serrors.IsAlreadyExists(err) {
		if _, err := k.RbacV1().RoleBindings(rb.Namespace).Update(ctx, rb, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update canary role binding: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("create canary role binding: %w", err)
	}
	return nil
}
