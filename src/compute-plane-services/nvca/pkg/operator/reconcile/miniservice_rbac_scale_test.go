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
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"

	nvcaconfig "github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/types/nvca/config"

	nvcaenvtest "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/envtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/miniservice"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/operator/internal/kubeclients"
)

// protectedSubresources are the rights chart workloads must never hold: they change replicas or attach to utils pods,
// which only NVCA may do (the provenance webhook's scale hook is Ignore, so RBAC is what makes this enforceable).
var protectedSubresources = []struct{ group, resource, sub string }{
	{"apps", "deployments", "scale"},
	{"apps", "replicasets", "scale"},
	{"apps", "statefulsets", "scale"},
	{"", "pods", "ephemeralcontainers"},
}

// ruleGrants reports whether any rule of the role grants verb on group/resource[/sub], following RBAC matching.
func ruleGrants(role *rbacv1.Role, group, resource, sub, verb string) bool {
	has := func(list []string, want string) bool {
		for _, v := range list {
			if v == want || v == "*" {
				return true
			}
		}
		return false
	}
	full := resource
	if sub != "" {
		full = resource + "/" + sub
	}
	for _, r := range role.Rules {
		if !has(r.APIGroups, group) || !has(r.Verbs, verb) {
			continue
		}
		for _, res := range r.Resources {
			if res == full || res == "*" || (sub != "" && (res == resource+"/*" || res == "*/"+sub)) {
				return true
			}
		}
	}
	return false
}

func generatedInstanceRole(t *testing.T, cfg *nvcaconfig.Config, mode string) *rbacv1.Role {
	t.Helper()
	bc := &BackendK8sCache{
		clients: &kubeclients.KubeClients{K8s: fake.NewSimpleClientset()}, operatorNamespace: NVCAOperatorNamespace,
		utilsProvenance: UtilsProvenanceSettings{Mode: mode},
	}
	if cfg != nil {
		cfgBytes, err := nvcaconfig.EncodeConfig(*cfg)
		require.NoError(t, err)
		cm := &corev1.ConfigMap{}
		cm.Name, cm.Namespace = agentConfigMergeConfigMapName, bc.operatorNamespace
		cm.Data = map[string]string{agentConfigFile: string(cfgBytes)}
		_, err = bc.clients.K8s.CoreV1().ConfigMaps(bc.operatorNamespace).Create(context.Background(), cm, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	data, err := bc.getMiniServiceRBACCmData(context.Background(), nil)
	require.NoError(t, err)
	role := &rbacv1.Role{}
	require.NoError(t, yaml.Unmarshal([]byte(stripSPDXHeaders(data[MiniServicesPermissionsRoleName])), role))
	return role
}

func TestIsProtectedExtraKubernetesType(t *testing.T) {
	for _, tt := range []struct {
		group, resource string
		want            bool
	}{
		{"foo.com", "foos", false},
		{"apps", "deployments", false},
		{"apps", "deployments/scale", true},
		{"apps", "replicasets/scale", true},
		{"", "pods/ephemeralcontainers", true},
		{"apps", "*", true},
		{"apps", "deployments/*", true},
		{"*", "foos", true},
		{"foo.com", "foos/status", false},
	} {
		got := isProtectedExtraKubernetesType(nvcaconfig.AllowedExtraKubernetesTypeConfig{Group: tt.group, Resource: tt.resource})
		assert.Equal(t, tt.want, got, "%s %s", tt.group, tt.resource)
	}
}

// TestInstanceRoleNeverGrantsProtectedSubresources checks (in enforce mode) the generated Role for update and patch on every protected
// subresource, with the default config and with extra allowed types that try to add them.
func TestInstanceRoleNeverGrantsProtectedSubresources(t *testing.T) {
	forbidden := []nvcaconfig.AllowedExtraKubernetesTypeConfig{
		{Group: "apps", Resource: "deployments/scale"},
		{Group: "apps", Resource: "replicasets/scale"},
		{Group: "apps", Resource: "statefulsets/scale"},
		{Group: "", Resource: "pods/ephemeralcontainers"},
		{Group: "apps", Resource: "*"},
		{Group: "*", Resource: "deployments/scale"},
	}
	cfgWith := func(extra ...nvcaconfig.AllowedExtraKubernetesTypeConfig) *nvcaconfig.Config {
		return &nvcaconfig.Config{Cluster: nvcaconfig.NVCFClusterConfig{ValidationPolicy: &nvcaconfig.ValidationPolicyConfig{
			Name: "Default", AllowedExtraKubernetesTypes: extra,
		}}}
	}
	for name, cfg := range map[string]*nvcaconfig.Config{
		"default":                  nil,
		"forbidden extra types":    cfgWith(forbidden...),
		"benign extra type":        cfgWith(nvcaconfig.AllowedExtraKubernetesTypeConfig{Group: "foo.com", Resource: "foos"}),
		"benign and forbidden mix": cfgWith(append([]nvcaconfig.AllowedExtraKubernetesTypeConfig{{Group: "foo.com", Resource: "foos"}}, forbidden...)...),
	} {
		t.Run(name, func(t *testing.T) {
			role := generatedInstanceRole(t, cfg, UtilsProvenanceModeEnforce)
			// Positive controls: the role is not trivially empty.
			assert.True(t, ruleGrants(role, "apps", "deployments", "", "update"))
			assert.True(t, ruleGrants(role, "apps", "deployments", "", "patch"))
			for _, p := range protectedSubresources {
				for _, verb := range []string{"update", "patch"} {
					assert.False(t, ruleGrants(role, p.group, p.resource, p.sub, verb), "%s %s/%s must not be granted", verb, p.resource, p.sub)
				}
			}
			if cfg != nil && strings.Contains(name, "benign") {
				assert.True(t, ruleGrants(role, "foo.com", "foos", "", "update"), "a benign extra type is still granted")
			}
		})
	}
}

// TestInstanceRBACOnARealAPIServer evaluates the generated Role with a real API server: SubjectAccessReviews for the
// instance service account and for a service account the chart creates, and the RBAC escalation check that stops the
// instance account from granting itself more.
func TestInstanceRBACOnARealAPIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set")
	}
	cfg, admin, cleanup, err := nvcaenvtest.SetupEnvtest()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	ctx := context.Background()
	const ns = "rbac-scale-test"

	_, err = admin.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	require.NoError(t, err)

	role := generatedInstanceRole(t, nil, UtilsProvenanceModeEnforce)
	role.Name, role.Namespace = miniservice.InstanceServiceAccountName, ns
	role.ResourceVersion = ""
	_, err = admin.RbacV1().Roles(ns).Create(ctx, role, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = admin.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: miniservice.InstanceServiceAccountName, Namespace: ns},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: role.Name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: miniservice.InstanceServiceAccountName, Namespace: ns}},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	instanceUser := miniservice.InstanceServiceAccountUsername(ns)
	chartUser := "system:serviceaccount:" + ns + ":chart-sa"

	can := func(user, group, resource, sub, verb string) bool {
		sar, err := admin.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{
			Spec: authorizationv1.SubjectAccessReviewSpec{
				User: user,
				ResourceAttributes: &authorizationv1.ResourceAttributes{
					Namespace: ns, Verb: verb, Group: group, Resource: resource, Subresource: sub,
				},
			},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
		return sar.Status.Allowed
	}

	t.Run("instance account", func(t *testing.T) {
		assert.True(t, can(instanceUser, "apps", "deployments", "", "update"), "control: main resource update is allowed")
		assert.True(t, can(instanceUser, "apps", "deployments", "", "patch"), "control: main resource patch is allowed")
		for _, p := range protectedSubresources {
			for _, verb := range []string{"update", "patch"} {
				assert.False(t, can(instanceUser, p.group, p.resource, p.sub, verb), "%s %s/%s", verb, p.resource, p.sub)
			}
		}
	})

	as := func(user string) kubernetes.Interface {
		c := rest.CopyConfig(cfg)
		c.Impersonate = rest.ImpersonationConfig{UserName: user}
		k, err := kubernetes.NewForConfig(c)
		require.NoError(t, err)
		return k
	}

	t.Run("the instance account cannot grant scale rights, so a chart cannot obtain them", func(t *testing.T) {
		instance := as(instanceUser)
		for _, p := range protectedSubresources {
			_, err := instance.RbacV1().Roles(ns).Create(ctx, &rbacv1.Role{
				ObjectMeta: metav1.ObjectMeta{Name: "grant-" + p.resource + "-" + p.sub},
				Rules: []rbacv1.PolicyRule{{
					APIGroups: []string{p.group}, Resources: []string{p.resource + "/" + p.sub}, Verbs: []string{"update", "patch"},
				}},
			}, metav1.CreateOptions{})
			require.Error(t, err, "%s/%s", p.resource, p.sub)
			assert.True(t, apierrors.IsForbidden(err), "escalation must be refused for %s/%s, got %v", p.resource, p.sub, err)
		}
		// A wildcard grant is an escalation too.
		_, err := instance.RbacV1().Roles(ns).Create(ctx, &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: "grant-wildcard"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"apps"}, Resources: []string{"*"}, Verbs: []string{"*"}}},
		}, metav1.CreateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)
	})

	t.Run("a service account the chart creates holds at most what the instance account holds", func(t *testing.T) {
		instance := as(instanceUser)
		_, err := instance.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "chart-sa"}}, metav1.CreateOptions{})
		require.NoError(t, err)
		// The best the chart can do is copy the instance account's own rights.
		_, err = instance.RbacV1().Roles(ns).Create(ctx, &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: "chart-role"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"apps"}, Resources: []string{"deployments", "replicasets"}, Verbs: []string{"get", "update", "patch"}}},
		}, metav1.CreateOptions{})
		require.NoError(t, err, "granting rights the instance account holds is allowed")
		_, err = instance.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "chart-binding"},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "chart-role"},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "chart-sa", Namespace: ns}},
		}, metav1.CreateOptions{})
		require.NoError(t, err)

		assert.True(t, can(chartUser, "apps", "deployments", "", "update"), "control: the copied right works")
		for _, p := range protectedSubresources {
			for _, verb := range []string{"update", "patch"} {
				assert.False(t, can(chartUser, p.group, p.resource, p.sub, verb), "chart account %s %s/%s", verb, p.resource, p.sub)
			}
		}

		// Binding the instance account's role to the chart account is also bounded by what the instance account holds.
		_, err = instance.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "chart-binding-2"},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: role.Name},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "chart-sa", Namespace: ns}},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
		for _, p := range protectedSubresources {
			assert.False(t, can(chartUser, p.group, p.resource, p.sub, "update"), "%s/%s after binding the instance role", p.resource, p.sub)
		}
	})
}

// TestAuditModeKeepsConfiguredExtraTypes pins the compatibility behavior: before enforcement nothing a cluster has
// configured is removed, so existing workloads, such as a chart that autoscales its own Deployment, keep working. The
// protected entries are dropped only when the provenance mode is enforce.
func TestAuditModeKeepsConfiguredExtraTypes(t *testing.T) {
	cfg := &nvcaconfig.Config{Cluster: nvcaconfig.NVCFClusterConfig{ValidationPolicy: &nvcaconfig.ValidationPolicyConfig{
		Name: "Default",
		AllowedExtraKubernetesTypes: []nvcaconfig.AllowedExtraKubernetesTypeConfig{
			{Group: "apps", Resource: "deployments/scale"},
		},
	}}}
	for _, mode := range []string{"", UtilsProvenanceModeAudit} {
		role := generatedInstanceRole(t, cfg, mode)
		assert.True(t, ruleGrants(role, "apps", "deployments", "scale", "update"), "mode %q keeps the configured type", mode)
	}
	role := generatedInstanceRole(t, cfg, UtilsProvenanceModeEnforce)
	assert.False(t, ruleGrants(role, "apps", "deployments", "scale", "update"), "enforce drops it")
}
