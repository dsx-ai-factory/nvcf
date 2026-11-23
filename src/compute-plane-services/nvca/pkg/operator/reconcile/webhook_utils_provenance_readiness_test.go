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
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	nvcaenvtest "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/envtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance/readiness"
	nvcav1alpha1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	nvidiaiov1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvcf/v1"
	nvcafake "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/client/clientset/versioned/fake"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/operator/internal/kubeclients"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/webhook"
	whmetrics "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/webhook/metrics"
)

type readinessEnv struct {
	admin  kubernetes.Interface
	canary readiness.Canary
	server *httptest.Server
}

// newReadinessEnv starts a real kube-apiserver, the provenance webhook in the given mode behind a TLS server, the
// production webhook definitions pointed at it, and the canary resources the operator prepares.
func newReadinessEnv(t *testing.T, mode string) *readinessEnv {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set")
	}
	cfg, admin, cleanup, err := nvcaenvtest.SetupEnvtest()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	ctx := context.Background()

	const nvcaUser = "test:nvca"
	engine := &utilsprovenance.Engine{
		Identities: utilsprovenance.Identities{
			NVCA:                 nvcaUser,
			ReplicaSetController: utilsprovenance.DefaultReplicaSetController,
			DeploymentController: utilsprovenance.DefaultDeploymentController,
		},
		Lookup: webhook.NewKubeProvenanceLookup(admin, nvcafake.NewSimpleClientset(&nvcav1alpha1.MiniService{
			ObjectMeta: metav1.ObjectMeta{Name: "ms1", UID: "u"}, Spec: nvcav1alpha1.MiniServiceSpec{Namespace: "x"},
		})),
	}
	whMode, err := webhook.ParseUtilsProvenanceMode(mode)
	require.NoError(t, err)
	hctx := whmetrics.WithDefaultMetrics(ctx, whmetrics.WithRegisterer(prometheus.NewRegistry()))
	handler, err := webhook.NewUtilsProvenanceWebhook(hctx, utilsprovenance.WebhookName, engine, whMode)
	require.NoError(t, err)
	srv := httptest.NewTLSServer(handler)
	t.Cleanup(srv.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})

	// The operator prepares the canary resources.
	bc := &BackendK8sCache{
		clients:         &kubeclients.KubeClients{K8s: admin},
		utilsProvenance: UtilsProvenanceSettings{Mode: mode},
	}
	if mode == UtilsProvenanceModeEnforce {
		require.NoError(t, bc.setupUtilsProvenanceCanary(ctx))
	} else {
		// Audit mode creates nothing: the canary namespace is created by hand so the audit canary can run.
		ns, sa, role, rb := canaryResources()
		_, err = admin.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
		require.NoError(t, err)
		_, err = admin.CoreV1().ServiceAccounts(sa.Namespace).Create(ctx, sa, metav1.CreateOptions{})
		require.NoError(t, err)
		_, err = admin.RbacV1().Roles(role.Namespace).Create(ctx, role, metav1.CreateOptions{})
		require.NoError(t, err)
		_, err = admin.RbacV1().RoleBindings(rb.Namespace).Create(ctx, rb, metav1.CreateOptions{})
		require.NoError(t, err)
	}

	nsSel := &metav1.LabelSelector{MatchExpressions: makeLabelSelectorRequirements(
		makeWorkloadNamespaceLabelSelectors(WorkloadInstanceTypeValueMiniService, readiness.CanaryNamespaceLabelValue))}
	hooks := makeUtilsProvenanceValidatingWebhooks(&nvidiaiov1.NVCFBackend{}, WebhookCert{CACertBytes: ca}, nsSel,
		UtilsProvenanceSettings{Mode: mode})
	for i := range hooks {
		url := srv.URL + utilsProvenanceWebhookPath
		hooks[i].ClientConfig = admissionregistrationv1.WebhookClientConfig{URL: &url, CABundle: ca}
	}
	_, err = admin.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(ctx,
		&admissionregistrationv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "readiness-test"}, Webhooks: hooks},
		metav1.CreateOptions{})
	require.NoError(t, err)

	icfg := rest.CopyConfig(cfg)
	icfg.Impersonate = rest.ImpersonationConfig{UserName: readiness.CanaryUsername()}
	impersonated, err := kubernetes.NewForConfig(icfg)
	require.NoError(t, err)
	return &readinessEnv{admin: admin, canary: readiness.KubeCanary{Impersonated: impersonated}, server: srv}
}

func TestReadinessCanaryProvesEnforcement(t *testing.T) {
	env := newReadinessEnv(t, UtilsProvenanceModeEnforce)
	checker := &readiness.Checker{Canary: env.canary}

	// The hook takes a moment to become active. Until it is, the canary is accepted and enforcement is NOT verified.
	require.Eventually(t, func() bool {
		return checker.CheckCanary(context.Background()).EnforcementVerified
	}, 30*time.Second, 200*time.Millisecond, "three consecutive refusals must verify enforcement")
	s := checker.Status()
	assert.Equal(t, readiness.OutcomeRefused, s.LastCanary)
	assert.GreaterOrEqual(t, s.ConsecutiveRefusals, 3)

	t.Run("an unavailable webhook leaves enforcement unverified", func(t *testing.T) {
		env.server.Close()
		got := checker.CheckCanary(context.Background())
		assert.False(t, got.EnforcementVerified, "an unavailable webhook is not proof of enforcement")
		assert.Equal(t, 0, got.ConsecutiveRefusals)
		assert.NotEqual(t, readiness.OutcomeRefused, got.LastCanary)
	})
}

func TestReadinessCanaryInAuditModeIsAccepted(t *testing.T) {
	env := newReadinessEnv(t, UtilsProvenanceModeAudit)
	checker := &readiness.Checker{Canary: env.canary}
	for i := 0; i < 6; i++ {
		s := checker.CheckCanary(context.Background())
		assert.False(t, s.EnforcementVerified, "audit mode must never verify enforcement")
		time.Sleep(100 * time.Millisecond)
	}
	assert.Equal(t, readiness.OutcomeAccepted, checker.Status().LastCanary)
}

func TestReadinessRBACDenialIsNotProofOfEnforcement(t *testing.T) {
	env := newReadinessEnv(t, UtilsProvenanceModeEnforce)
	// Remove the canary's right to create pods. The request is now denied by RBAC before any webhook runs.
	require.NoError(t, env.admin.RbacV1().RoleBindings(readiness.CanaryNamespace).Delete(context.Background(),
		readiness.CanaryServiceAccount, metav1.DeleteOptions{}))
	checker := &readiness.Checker{Canary: env.canary}
	require.Eventually(t, func() bool {
		return checker.CheckCanary(context.Background()).LastCanary == readiness.OutcomeOtherDenial
	}, 15*time.Second, 200*time.Millisecond)
	for i := 0; i < 4; i++ {
		assert.False(t, checker.CheckCanary(context.Background()).EnforcementVerified, "an RBAC denial must not count as a refusal")
	}
}

func TestReadinessRBACReviewSeesEffectivePermissions(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set")
	}
	_, admin, cleanup, err := nvcaenvtest.SetupEnvtest()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	ctx := context.Background()

	const ns = "sr-readiness"
	_, err = admin.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: ns, Labels: map[string]string{nvcatypes.WorkloadInstanceTypeLabel: nvcatypes.WorkloadInstanceTypeValueMiniService},
	}}, metav1.CreateOptions{})
	require.NoError(t, err)
	for _, name := range []string{"miniservice-instance-permissions", "chart-sa", "other-sa"} {
		_, err = admin.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	// The instance role: the Role NVCA generates, bound to the instance account only.
	role := generatedInstanceRole(t, nil, UtilsProvenanceModeEnforce)
	role.Name, role.Namespace, role.ResourceVersion = "miniservice-instance-permissions", ns, ""
	_, err = admin.RbacV1().Roles(ns).Create(ctx, role, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = admin.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "instance"},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: role.Name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "miniservice-instance-permissions", Namespace: ns}},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	newChecker := func() *readiness.Checker {
		return &readiness.Checker{
			Accounts: readiness.KubeAccounts{Client: admin}, Reviewer: readiness.KubeReviewer{Client: admin},
			Objects: readiness.KubeObjects{Client: admin},
		}
	}

	s := newChecker().CheckRBAC(ctx)
	assert.True(t, s.RBACVerified, "the generated role alone grants nothing protected: %v", s.RBACExposures)

	t.Run("a ClusterRoleBinding to an ordinary cluster role that includes scale is found", func(t *testing.T) {
		_, err := admin.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "scaler"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"apps"}, Resources: []string{"deployments/scale"}, Verbs: []string{"update"}}},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
		_, err = admin.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "scaler-chart"},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "scaler"},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "chart-sa", Namespace: ns}},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
		got := newChecker().CheckRBAC(ctx)
		assert.False(t, got.RBACVerified)
		assert.Equal(t, []string{"sr-readiness/chart-sa: update deployments/scale"}, got.RBACExposures)
	})

	t.Run("a binding to a group that every account in the namespace belongs to is found", func(t *testing.T) {
		_, err := admin.RbacV1().Roles(ns).Create(ctx, &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: "ephemeral"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"pods/ephemeralcontainers"}, Verbs: []string{"patch"}}},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
		_, err = admin.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "ephemeral-group"},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "ephemeral"},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "system:serviceaccounts:" + ns}},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
		got := newChecker().CheckRBAC(ctx)
		assert.False(t, got.RBACVerified)
		assert.Contains(t, got.RBACExposures, "sr-readiness/other-sa: patch pods/ephemeralcontainers",
			"a group binding must count: the review passes the account's groups")
	})

	t.Run("a binding restricted with resourceNames is found by name", func(t *testing.T) {
		_, err := admin.RbacV1().Roles(ns).Create(ctx, &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: "scale-utils-only"},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{"apps"}, Resources: []string{"deployments/scale"}, ResourceNames: []string{"utils"},
				Verbs: []string{"update"},
			}},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
		_, err = admin.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "scale-utils-only"},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "scale-utils-only"},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "other-sa", Namespace: ns}},
		}, metav1.CreateOptions{})
		require.NoError(t, err)

		// An unnamed review does not see it: the API server matches resourceNames only against a named request.
		sar, err := admin.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{
			Spec: authorizationv1.SubjectAccessReviewSpec{
				User: "system:serviceaccount:" + ns + ":other-sa",
				ResourceAttributes: &authorizationv1.ResourceAttributes{
					Namespace: ns, Verb: "update", Group: "apps", Resource: "deployments", Subresource: "scale",
				},
			},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
		assert.False(t, sar.Status.Allowed, "the unnamed review is blind to a resourceNames rule")

		got := newChecker().CheckRBAC(ctx)
		assert.False(t, got.RBACVerified, "the named review must find it")
		assert.Contains(t, got.RBACExposures, "sr-readiness/other-sa: update deployments/scale (name utils)")

		require.NoError(t, admin.RbacV1().RoleBindings(ns).Delete(ctx, "scale-utils-only", metav1.DeleteOptions{}))
	})

	t.Run("removing the bindings restores the verified state", func(t *testing.T) {
		require.NoError(t, admin.RbacV1().ClusterRoleBindings().Delete(ctx, "scaler-chart", metav1.DeleteOptions{}))
		require.NoError(t, admin.RbacV1().RoleBindings(ns).Delete(ctx, "ephemeral-group", metav1.DeleteOptions{}))
		assert.True(t, newChecker().CheckRBAC(ctx).RBACVerified)
	})
}

func TestCanaryResourcesAreIsolatedFromInstanceNamespaces(t *testing.T) {
	ns, sa, role, rb := canaryResources()
	assert.Equal(t, readiness.CanaryNamespaceLabelValue, ns.Labels[nvcatypes.WorkloadInstanceTypeLabel])
	assert.NotEqual(t, nvcatypes.WorkloadInstanceTypeValueMiniService, ns.Labels[nvcatypes.WorkloadInstanceTypeLabel],
		"the canary namespace must not look like an instance namespace to the MiniService controller or GC")
	assert.Equal(t, readiness.CanaryNamespace, sa.Namespace)
	require.Len(t, role.Rules, 1)
	assert.Equal(t, []string{"pods"}, role.Rules[0].Resources)
	assert.Equal(t, []string{"create"}, role.Rules[0].Verbs, "the canary account may only create pods")
	assert.Equal(t, role.Name, rb.RoleRef.Name)
}

func TestSetupUtilsProvenanceCanaryOnlyInEnforceMode(t *testing.T) {
	for mode, want := range map[string]bool{"": false, UtilsProvenanceModeAudit: false, UtilsProvenanceModeEnforce: true} {
		k := fake.NewSimpleClientset()
		bc := &BackendK8sCache{clients: &kubeclients.KubeClients{K8s: k}, utilsProvenance: UtilsProvenanceSettings{Mode: mode}}
		require.NoError(t, bc.setupUtilsProvenanceCanary(context.Background()))
		_, err := k.CoreV1().Namespaces().Get(context.Background(), readiness.CanaryNamespace, metav1.GetOptions{})
		assert.Equal(t, want, err == nil, "mode %q", mode)
		// Idempotent.
		require.NoError(t, bc.setupUtilsProvenanceCanary(context.Background()))
	}
}

var _ = http.StatusOK
