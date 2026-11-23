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
	"bytes"
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	nvcaenvtest "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/envtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
	nvcav1alpha1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	nvidiaiov1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvcf/v1"
	nvcafake "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/client/clientset/versioned/fake"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/operator/internal/kubeclients"
	nvcaoptypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/operator/types"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/webhook"
	whmetrics "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/webhook/metrics"
)

const (
	admNVCA  = "test:nvca"
	admChart = "system:serviceaccount:sr-adm:instance-sa"
	admNS    = "sr-adm"
	admMSUID = types.UID("0e0e0e0e-0000-0000-0000-000000000001")
)

type hookCounter struct {
	mu    sync.Mutex
	calls int
}

func (c *hookCounter) inc() { c.mu.Lock(); c.calls++; c.mu.Unlock() }
func (c *hookCounter) n() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// TestUtilsProvenanceAdmission runs the production webhook configuration and the real handler behind a real
// kube-apiserver. It proves what the match conditions select, including the scale subresource, and that forged or
// unauthorized writes are refused end to end. There is no controller manager in envtest, so controller identities are
// impersonated where a chain needs them.
func TestUtilsProvenanceAdmission(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set")
	}
	cfg, admin, cleanup, err := nvcaenvtest.SetupEnvtest()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	ctx := context.Background()

	as := func(user string) kubernetes.Interface {
		c := rest.CopyConfig(cfg)
		c.Impersonate = rest.ImpersonationConfig{UserName: user, Groups: []string{"system:masters"}}
		k, err := kubernetes.NewForConfig(c)
		require.NoError(t, err)
		return k
	}
	nvca, chart := as(admNVCA), as(admChart)

	// The real handler, enforcing, backed by the real API server and a fake MiniService clientset.
	ms := &nvcav1alpha1.MiniService{
		ObjectMeta: metav1.ObjectMeta{Name: "ms1", UID: admMSUID},
		Spec:       nvcav1alpha1.MiniServiceSpec{Namespace: admNS},
	}
	engine := &utilsprovenance.Engine{
		Identities: utilsprovenance.Identities{
			NVCA:                 admNVCA,
			ReplicaSetController: utilsprovenance.DefaultReplicaSetController,
			DeploymentController: utilsprovenance.DefaultDeploymentController,
		},
		Lookup: webhook.NewKubeProvenanceLookup(admin, nvcafake.NewSimpleClientset(ms)),
	}
	hctx := whmetrics.WithDefaultMetrics(ctx, whmetrics.WithRegisterer(prometheus.NewRegistry()))
	handler, err := webhook.NewUtilsProvenanceWebhook(hctx, "validate-utils-provenance.test", engine, webhook.UtilsProvenanceEnforce)
	require.NoError(t, err)
	var calls hookCounter
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.inc()
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})

	// Production webhook definitions, pointed at the test server.
	nb := &nvidiaiov1.NVCFBackend{}
	nsSel := &metav1.LabelSelector{MatchExpressions: makeLabelSelectorRequirements(
		makeWorkloadNamespaceLabelSelectors(WorkloadInstanceTypeValueMiniService))}
	hooks := makeUtilsProvenanceValidatingWebhooks(nb, WebhookCert{CACertBytes: ca}, nsSel,
		UtilsProvenanceSettings{Mode: UtilsProvenanceModeEnforce})
	for i := range hooks {
		url := srv.URL + utilsProvenanceWebhookPath
		hooks[i].ClientConfig = admissionregistrationv1.WebhookClientConfig{URL: &url, CABundle: ca}
	}
	_, err = admin.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(ctx,
		&admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: "utils-provenance-test"}, Webhooks: hooks,
		}, metav1.CreateOptions{})
	require.NoError(t, err)

	_, err = admin.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: admNS, Labels: map[string]string{nvcatypes.WorkloadInstanceTypeLabel: nvcatypes.WorkloadInstanceTypeValueMiniService},
	}}, metav1.CreateOptions{})
	require.NoError(t, err)
	// An unlabeled namespace is outside the namespace selector.
	_, err = admin.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "plain"}}, metav1.CreateOptions{})
	require.NoError(t, err)

	pod := func(name string, labels, ann map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, Annotations: ann},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
		}
	}
	deploy := func(name string, owners []metav1.OwnerReference, labels map[string]string) *appsv1.Deployment {
		sel := map[string]string{"app": name}
		for k, v := range labels {
			sel[k] = v
		}
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, OwnerReferences: owners},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(int32(1)),
				Selector: &metav1.LabelSelector{MatchLabels: sel},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: sel},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
				},
			},
		}
	}
	roleLabel := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}
	msOwner := []metav1.OwnerReference{{APIVersion: "nvca.nvcf.nvidia.io/v1alpha1", Kind: "MiniService",
		Name: "ms1", UID: admMSUID, Controller: ptr.To(true)}}

	t.Run("objects without reserved metadata never reach the webhook", func(t *testing.T) {
		before := calls.n()
		_, err := chart.CoreV1().Pods(admNS).Create(ctx, pod("plain-pod", map[string]string{"app": "x"}, nil), metav1.CreateOptions{})
		require.NoError(t, err)
		_, err = chart.AppsV1().Deployments(admNS).Create(ctx, deploy("web", nil, nil), metav1.CreateOptions{})
		require.NoError(t, err)
		assert.Equal(t, before, calls.n(), "the match condition must keep ordinary objects away from the webhook")
	})

	t.Run("a namespace outside the selector is never evaluated", func(t *testing.T) {
		before := calls.n()
		_, err := chart.CoreV1().Pods("plain").Create(ctx, pod("p", roleLabel, nil), metav1.CreateOptions{})
		require.NoError(t, err)
		assert.Equal(t, before, calls.n())
	})

	t.Run("F1 forged label by a chart identity is refused", func(t *testing.T) {
		before := calls.n()
		_, err := chart.CoreV1().Pods(admNS).Create(ctx, pod("forged", roleLabel, nil), metav1.CreateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)
		assert.Greater(t, calls.n(), before, "a pod with the reserved label must reach the webhook")
	})

	t.Run("a chart identity cannot claim the infra annotation on a new pod", func(t *testing.T) {
		_, err := chart.CoreV1().Pods(admNS).Create(ctx,
			pod("anno", nil, map[string]string{nvcatypes.InfraObjectAnnotationKey: "true"}), metav1.CreateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)
	})

	t.Run("F3 chart creates a Deployment with the label in its template", func(t *testing.T) {
		_, err := chart.AppsV1().Deployments(admNS).Create(ctx, deploy("fake-utils", nil, roleLabel), metav1.CreateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)
	})

	t.Run("F3 chart creates a Deployment with a MiniService owner reference", func(t *testing.T) {
		_, err := chart.AppsV1().Deployments(admNS).Create(ctx, deploy("fake-owned", msOwner, nil), metav1.CreateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)
	})

	var utils *appsv1.Deployment
	t.Run("NVCA creates the utils Deployment", func(t *testing.T) {
		var err error
		utils, err = nvca.AppsV1().Deployments(admNS).Create(ctx, deploy("utils", msOwner, roleLabel), metav1.CreateOptions{})
		require.NoError(t, err)
	})

	t.Run("F4 chart edits the utils Deployment template", func(t *testing.T) {
		cur, err := admin.AppsV1().Deployments(admNS).Get(ctx, "utils", metav1.GetOptions{})
		require.NoError(t, err)
		cur.Spec.Template.Spec.Containers[0].Image = "evil"
		_, err = chart.AppsV1().Deployments(admNS).Update(ctx, cur, metav1.UpdateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)
	})

	t.Run("F11 the scale subresource reaches the webhook and a chart identity is refused", func(t *testing.T) {
		before := calls.n()
		sc, err := chart.AppsV1().Deployments(admNS).GetScale(ctx, "utils", metav1.GetOptions{})
		require.NoError(t, err)
		sc.Spec.Replicas = 5
		_, err = chart.AppsV1().Deployments(admNS).UpdateScale(ctx, "utils", sc, metav1.UpdateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)
		assert.Greater(t, calls.n(), before, "the scale request must reach the scale webhook")
	})

	t.Run("a chart identity can scale its own Deployment, and NVCA can scale utils", func(t *testing.T) {
		sc, err := chart.AppsV1().Deployments(admNS).GetScale(ctx, "web", metav1.GetOptions{})
		require.NoError(t, err)
		sc.Spec.Replicas = 2
		_, err = chart.AppsV1().Deployments(admNS).UpdateScale(ctx, "web", sc, metav1.UpdateOptions{})
		require.NoError(t, err)

		sc, err = nvca.AppsV1().Deployments(admNS).GetScale(ctx, "utils", metav1.GetOptions{})
		require.NoError(t, err)
		sc.Spec.Replicas = 2
		_, err = nvca.AppsV1().Deployments(admNS).UpdateScale(ctx, "utils", sc, metav1.UpdateOptions{})
		require.NoError(t, err)
	})

	t.Run("F2 pod naming the real ReplicaSet created by a chart identity is refused", func(t *testing.T) {
		rs, err := admin.AppsV1().ReplicaSets(admNS).Create(ctx, &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: "utils-h1", OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "Deployment", Name: "utils", UID: utils.UID, Controller: ptr.To(true)}}},
			Spec: appsv1.ReplicaSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "utils", "pod-template-hash": "h1"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
						"app": "utils", "pod-template-hash": "h1", nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
				},
			},
		}, metav1.CreateOptions{})
		// admin is not NVCA nor the Deployment controller, so even this ReplicaSet creation is refused.
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)
		_ = rs
	})

	t.Run("a valid chain admits the pod created by the ReplicaSet controller", func(t *testing.T) {
		deployCtl := as(utilsprovenance.DefaultDeploymentController)
		rsCtl := as(utilsprovenance.DefaultReplicaSetController)
		labels := map[string]string{"app": "utils", "pod-template-hash": "h1", nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}
		// The Deployment template (from NVCA) is {app: utils, role}. The Deployment controller adds the hash.
		rs, err := deployCtl.AppsV1().ReplicaSets(admNS).Create(ctx, &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: "utils-h1", OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "Deployment", Name: "utils", UID: utils.UID, Controller: ptr.To(true)}}},
			Spec: appsv1.ReplicaSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
				},
			},
		}, metav1.CreateOptions{})
		require.NoError(t, err, "the Deployment controller may create the ReplicaSet of a verified Deployment")

		p := pod("utils-h1-a", labels, nil)
		p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID,
			Controller: ptr.To(true)}}
		_, err = rsCtl.CoreV1().Pods(admNS).Create(ctx, p, metav1.CreateOptions{})
		require.NoError(t, err, "the ReplicaSet controller creates the pod of a verified chain")

		// The same pod shape from a chart identity is refused (F2: a copied owner reference grants nothing).
		p2 := pod("utils-h1-b", labels, nil)
		p2.OwnerReferences = p.OwnerReferences
		_, err = chart.CoreV1().Pods(admNS).Create(ctx, p2, metav1.CreateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)

		// A pod with a wrong UID from the ReplicaSet controller is refused.
		p3 := pod("utils-h1-c", labels, nil)
		p3.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name,
			UID: "11111111-1111-1111-1111-111111111111", Controller: ptr.To(true)}}
		_, err = rsCtl.CoreV1().Pods(admNS).Create(ctx, p3, metav1.CreateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)

		// Chart identity edits to the protected pod are refused, including adding an ephemeral container.
		cur, err := admin.CoreV1().Pods(admNS).Get(ctx, "utils-h1-a", metav1.GetOptions{})
		require.NoError(t, err)
		cur.Labels["x"] = "y"
		_, err = chart.CoreV1().Pods(admNS).Update(ctx, cur, metav1.UpdateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)

		cur, err = admin.CoreV1().Pods(admNS).Get(ctx, "utils-h1-a", metav1.GetOptions{})
		require.NoError(t, err)
		cur.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{
			Name: "dbg", Image: "busybox"}}}
		_, err = chart.CoreV1().Pods(admNS).UpdateEphemeralContainers(ctx, "utils-h1-a", cur, metav1.UpdateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)
	})

	t.Run("a ReplicaSet not visible yet is retryable, not admitted", func(t *testing.T) {
		rsCtl := as(utilsprovenance.DefaultReplicaSetController)
		labels := map[string]string{"app": "utils", "pod-template-hash": "h9", nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}
		p := pod("utils-h9-a", labels, nil)
		p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "utils-h9",
			UID: "22222222-2222-2222-2222-222222222222", Controller: ptr.To(true)}}
		_, err := rsCtl.CoreV1().Pods(admNS).Create(ctx, p, metav1.CreateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsServiceUnavailable(err) || apierrors.IsInternalError(err) || apierrors.IsTimeout(err),
			"a lookup miss must be a retryable error, got %v", err)
	})

	t.Run("legacy utils pod: NVCA creates it, a chart identity cannot change the annotation", func(t *testing.T) {
		legacy := pod("utils", nil, map[string]string{nvcatypes.InfraObjectAnnotationKey: "true"})
		_, err := nvca.CoreV1().Pods(admNS).Create(ctx, legacy, metav1.CreateOptions{})
		require.NoError(t, err)
		cur, err := admin.CoreV1().Pods(admNS).Get(ctx, "utils", metav1.GetOptions{})
		require.NoError(t, err)
		delete(cur.Annotations, nvcatypes.InfraObjectAnnotationKey)
		_, err = chart.CoreV1().Pods(admNS).Update(ctx, cur, metav1.UpdateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)

		// An unchanged-annotation update by NVCA, as the restart baseline patch does, is accepted.
		cur, err = admin.CoreV1().Pods(admNS).Get(ctx, "utils", metav1.GetOptions{})
		require.NoError(t, err)
		cur.Annotations["nvca.nvcf.nvidia.io/worker-restart-count-baseline"] = "3"
		_, err = nvca.CoreV1().Pods(admNS).Update(ctx, cur, metav1.UpdateOptions{})
		require.NoError(t, err)
	})
}

// A guard that the expression and webhook set stay in step with the constants they are built from.
func TestUtilsProvenanceMatchExpressionMentionsReservedKeys(t *testing.T) {
	expr := utilsProvenanceMatchExpression()
	assert.Contains(t, expr, nvcatypes.PodRoleLabelKey)
	assert.Contains(t, expr, nvcatypes.InfraObjectAnnotationKey)
	assert.Contains(t, expr, "MiniService")
	assert.False(t, bytes.ContainsRune([]byte(expr), '\n'))
}

func TestNormalizeUtilsProvenanceMode(t *testing.T) {
	for in, want := range map[string]string{
		"": UtilsProvenanceModeAudit, "audit": UtilsProvenanceModeAudit, " Enforce ": UtilsProvenanceModeEnforce,
	} {
		got, err := NormalizeUtilsProvenanceMode(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	_, err := NormalizeUtilsProvenanceMode("block")
	assert.Error(t, err)
}

func TestUtilsProvenanceSettingsWebhookArgs(t *testing.T) {
	assert.Empty(t, UtilsProvenanceSettings{}.webhookArgs(), "defaults must not change the webhook container")
	assert.Empty(t, UtilsProvenanceSettings{Mode: UtilsProvenanceModeAudit}.webhookArgs())
	assert.Equal(t, []string{"--utils-provenance-mode", "enforce"},
		UtilsProvenanceSettings{Mode: UtilsProvenanceModeEnforce}.webhookArgs())
	assert.Equal(t, []string{
		"--utils-provenance-replicaset-controller-user", "rs",
		"--utils-provenance-deployment-controller-user", "dep",
	}, UtilsProvenanceSettings{ReplicaSetControllerUser: "rs", DeploymentControllerUser: "dep"}.webhookArgs())
}

func TestUtilsProvenanceFailurePolicyFollowsMode(t *testing.T) {
	nb := &nvidiaiov1.NVCFBackend{}
	sel := &metav1.LabelSelector{}
	for mode, want := range map[string]admissionregistrationv1.FailurePolicyType{
		"": admissionregistrationv1.Ignore, UtilsProvenanceModeAudit: admissionregistrationv1.Ignore,
		UtilsProvenanceModeEnforce: admissionregistrationv1.Fail,
	} {
		hooks := makeUtilsProvenanceValidatingWebhooks(nb, WebhookCert{}, sel, UtilsProvenanceSettings{Mode: mode})
		require.Len(t, hooks, 2)
		assert.Equal(t, want, *hooks[0].FailurePolicy, "main hook, mode %q", mode)
		assert.Equal(t, admissionregistrationv1.Ignore, *hooks[1].FailurePolicy, "the scale hook is always Ignore")
		assert.NotEmpty(t, hooks[0].MatchConditions, "fail closed only for the narrow set the match condition selects")
		assert.Empty(t, hooks[1].MatchConditions)
	}
}

func TestSetupMiniServiceValidatingWebhookRegistersUtilsProvenance(t *testing.T) {
	for _, mode := range []string{UtilsProvenanceModeAudit, UtilsProvenanceModeEnforce} {
		k8sClient := fake.NewSimpleClientset()
		bc := &BackendK8sCache{
			clients:         &kubeclients.KubeClients{K8s: k8sClient},
			utilsProvenance: UtilsProvenanceSettings{Mode: mode},
		}
		require.NoError(t, bc.setupMiniServiceValidatingWebhook(context.Background(), &nvidiaiov1.NVCFBackend{},
			WebhookCert{CACertBytes: []byte("ca")}))
		vw, err := k8sClient.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.Background(),
			nvcaoptypes.NVCAModuleName, metav1.GetOptions{})
		require.NoError(t, err)
		var names []string
		for _, w := range vw.Webhooks {
			names = append(names, w.Name)
			assert.Equal(t, []byte("ca"), w.ClientConfig.CABundle)
		}
		assert.Contains(t, names, "validate-helm-charts.nvca.nvcf.nvidia.io")
		assert.Contains(t, names, utilsProvenanceWebhookName)
		assert.Contains(t, names, utilsProvenanceScaleWebhookName)
	}
}

func TestWebhookContainerArgs(t *testing.T) {
	base := []string{"/usr/bin/webhook-server", "--config", agentConfigFilePath}
	assert.Equal(t, base, (&BackendK8sCache{}).webhookContainerArgs(), "defaults keep the container args unchanged")
	bc := &BackendK8sCache{utilsProvenance: UtilsProvenanceSettings{Mode: UtilsProvenanceModeEnforce}}
	assert.Equal(t, append(append([]string{}, base...), "--utils-provenance-mode", "enforce"), bc.webhookContainerArgs())
}

func TestUtilsProvenanceSettingsAgentEnv(t *testing.T) {
	assert.Empty(t, UtilsProvenanceSettings{}.agentEnv(), "defaults must not change the agent pod")
	assert.Empty(t, UtilsProvenanceSettings{Mode: UtilsProvenanceModeAudit}.agentEnv())
	env := UtilsProvenanceSettings{Mode: UtilsProvenanceModeEnforce}.agentEnv()
	require.Len(t, env, 1)
	assert.Equal(t, nvcatypes.UtilsProvenanceModeEnv, env[0].Name)
	assert.Equal(t, UtilsProvenanceModeEnforce, env[0].Value)
}

func TestUtilsProvenanceSettingsAgentRules(t *testing.T) {
	assert.Empty(t, UtilsProvenanceSettings{}.agentRules(), "defaults must not change the agent ClusterRole")
	assert.Empty(t, UtilsProvenanceSettings{Mode: UtilsProvenanceModeAudit}.agentRules())
	rules := UtilsProvenanceSettings{Mode: UtilsProvenanceModeEnforce}.agentRules()
	require.Len(t, rules, 1)
	assert.Equal(t, []string{"authorization.k8s.io"}, rules[0].APIGroups)
	assert.Equal(t, []string{"subjectaccessreviews"}, rules[0].Resources)
	assert.Equal(t, []string{"create"}, rules[0].Verbs)
}
