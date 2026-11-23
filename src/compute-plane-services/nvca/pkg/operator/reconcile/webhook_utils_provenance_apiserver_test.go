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
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"

	nvcaenvtest "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/envtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
	nvcav1alpha1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	nvidiaiov1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvcf/v1"
	nvcafake "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/client/clientset/versioned/fake"
	nvcastorage "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/storage"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/webhook"
	whmetrics "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/webhook/metrics"
)

// TestUtilsMutationAndValidationTogetherThroughTheAPIServer runs the miniservice mutating webhook (with the provenance
// classifier) and the provenance validating webhook, both built from the production definitions, behind a real
// kube-apiserver. A pod is created by the identity that would create it in a cluster. There is no controller manager in
// envtest, so the ReplicaSet and Deployment controllers are impersonated. It checks what the persisted pod looks like:
// a genuine utils pod gets the utils mutation, a forged one never persists, and a pod that cannot be verified is never
// admitted with the wrong mutation.
func TestUtilsMutationAndValidationTogetherThroughTheAPIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set")
	}
	cfg, admin, cleanup, err := nvcaenvtest.SetupEnvtest()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	ctx := context.Background()

	const (
		ns       = "sr-e2e"
		nvcaUser = "test:nvca"
		chartSA  = "system:serviceaccount:sr-e2e:miniservice-instance-permissions"
	)
	as := func(user string) kubernetes.Interface {
		c := rest.CopyConfig(cfg)
		c.Impersonate = rest.ImpersonationConfig{UserName: user, Groups: []string{"system:masters"}}
		k, err := kubernetes.NewForConfig(c)
		require.NoError(t, err)
		return k
	}
	nvca, depCtl, rsCtl, chart := as(nvcaUser), as(utilsprovenance.DefaultDeploymentController), as(utilsprovenance.DefaultReplicaSetController), as(chartSA)

	msUID := "0e0e0e0e-0000-0000-0000-0000000000a1"
	engine := &utilsprovenance.Engine{
		Identities: utilsprovenance.Identities{
			NVCA: nvcaUser, ReplicaSetController: utilsprovenance.DefaultReplicaSetController,
			DeploymentController: utilsprovenance.DefaultDeploymentController,
		},
		Lookup: webhook.NewKubeProvenanceLookup(admin, nvcafake.NewSimpleClientset(&nvcav1alpha1.MiniService{
			ObjectMeta: metav1.ObjectMeta{Name: "ms1", UID: "0e0e0e0e-0000-0000-0000-0000000000a1"},
			Spec:       nvcav1alpha1.MiniServiceSpec{Namespace: ns},
		})),
	}
	hctx := whmetrics.WithDefaultMetrics(ctx, whmetrics.WithRegisterer(prometheus.NewRegistry()))

	validating, err := webhook.NewUtilsProvenanceWebhook(hctx, utilsprovenance.WebhookName, engine, webhook.UtilsProvenanceEnforce)
	require.NoError(t, err)
	mutating, err := webhook.NewMiniserviceMutatingWebhook(hctx, "mutate-miniservice", admin, webhook.WithUtilsClassifier(engine))
	require.NoError(t, err)
	vSrv, mSrv := httptest.NewTLSServer(validating), httptest.NewTLSServer(mutating)
	t.Cleanup(vSrv.Close)
	t.Cleanup(mSrv.Close)
	caOf := func(s *httptest.Server) []byte {
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})
	}

	// Production definitions, pointed at the test servers. The namespace selector is the production one.
	nsSel := &metav1.LabelSelector{MatchExpressions: makeLabelSelectorRequirements(
		makeWorkloadNamespaceLabelSelectors(WorkloadInstanceTypeValueMiniService))}
	vHooks := makeUtilsProvenanceValidatingWebhooks(&nvidiaiov1.NVCFBackend{}, WebhookCert{}, nsSel, UtilsProvenanceSettings{Mode: UtilsProvenanceModeEnforce})
	for i := range vHooks {
		u := vSrv.URL + utilsProvenanceWebhookPath
		vHooks[i].ClientConfig = admissionregistrationv1.WebhookClientConfig{URL: &u, CABundle: caOf(vSrv)}
	}
	_, err = admin.AdmissionregistrationV1().ValidatingWebhookConfigurations().Create(ctx,
		&admissionregistrationv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "e2e-validating"}, Webhooks: vHooks}, metav1.CreateOptions{})
	require.NoError(t, err)

	bc := &BackendK8sCache{}
	mHooks := bc.makeMiniServiceMutatingWebhooks(&nvidiaiov1.NVCFBackend{}, WebhookCert{})
	for i := range mHooks {
		u := mSrv.URL + "/mutate-miniservice"
		mHooks[i].ClientConfig = admissionregistrationv1.WebhookClientConfig{URL: &u, CABundle: caOf(mSrv)}
	}
	_, err = admin.AdmissionregistrationV1().MutatingWebhookConfigurations().Create(ctx,
		&admissionregistrationv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "e2e-mutating"}, Webhooks: mHooks}, metav1.CreateOptions{})
	require.NoError(t, err)

	// The instance namespace with the metadata ConfigMap the mutating webhook reads for chart pods.
	_, err = admin.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: ns, Labels: map[string]string{nvcatypes.WorkloadInstanceTypeLabel: nvcatypes.WorkloadInstanceTypeValueMiniService},
	}}, metav1.CreateOptions{})
	require.NoError(t, err)
	data, err := nvcatypes.MiniserviceMetadata{
		MessageAction: common.FunctionCreationAction,
		Labels:        map[string]string{nvcatypes.FunctionIDKey: "f-1", nvcatypes.MiniserviceNameLabel: "ms1"},
		Annotations:   map[string]string{nvcatypes.ICMSRequestIDKey: "req-1"},
		EnvVars:       []corev1.EnvVar{{Name: "NVCF_FUNCTION_ID", Value: "f-1"}},
	}.ToConfigMapData()
	require.NoError(t, err)
	_, err = admin.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: nvcatypes.MiniserviceMetadataConfigMapName, Namespace: ns}, Data: data}, metav1.CreateOptions{})
	require.NoError(t, err)

	// The utils Deployment NVCA would create, and the ReplicaSet its controller creates.
	roleLabels := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils, nvcatypes.MiniserviceNameLabel: "ms1"}
	podSpec := corev1.PodSpec{Containers: []corev1.Container{{Name: "utils", Image: "busybox"}}}
	require.Eventually(t, func() bool {
		_, err = nvca.AppsV1().Deployments(ns).Create(ctx, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "utils", OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "nvca.nvcf.nvidia.io/v1alpha1", Kind: "MiniService", Name: "ms1", UID: "0e0e0e0e-0000-0000-0000-0000000000a1", Controller: ptr.To(true)}}},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(int32(2)),
				Selector: &metav1.LabelSelector{MatchLabels: roleLabels},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: roleLabels}, Spec: podSpec},
			},
		}, metav1.CreateOptions{})
		return err == nil
	}, 20*time.Second, 200*time.Millisecond, "NVCA creates the Deployment: %v", err)
	_ = msUID

	dep, err := admin.AppsV1().Deployments(ns).Get(ctx, "utils", metav1.GetOptions{})
	require.NoError(t, err)
	hashed := map[string]string{"pod-template-hash": "h1"}
	for k, v := range roleLabels {
		hashed[k] = v
	}
	rs, err := depCtl.AppsV1().ReplicaSets(ns).Create(ctx, &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "utils-h1", OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "Deployment", Name: "utils", UID: dep.UID, Controller: ptr.To(true)}}},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: hashed},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: hashed}, Spec: podSpec},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err, "the Deployment controller creates the ReplicaSet of a verified Deployment")

	utilsPod := func(name string, owner metav1.OwnerReference) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Labels: hashed, OwnerReferences: []metav1.OwnerReference{owner},
				Annotations: map[string]string{
					nvcastorage.HelmWebhookSharedStorageKNSReadWritePVCNameAnnotationKey:     "kns-pvc-rw",
					nvcastorage.HelmWebhookSharedStorageSecretsReadWritePVCNameAnnotationKey: "secrets-pvc-rw",
				},
			},
			Spec: podSpec,
		}
	}
	rsOwner := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: ptr.To(true)}
	volumeNames := func(p *corev1.Pod) []string {
		var out []string
		for _, v := range p.Spec.Volumes {
			out = append(out, v.Name)
		}
		return out
	}

	t.Run("a pod created by the ReplicaSet controller gets the utils mutation, not chart metadata", func(t *testing.T) {
		_, err := rsCtl.CoreV1().Pods(ns).Create(ctx, utilsPod("utils-h1-a", rsOwner), metav1.CreateOptions{})
		require.NoError(t, err)
		got, err := admin.CoreV1().Pods(ns).Get(ctx, "utils-h1-a", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Contains(t, volumeNames(got), "kns-data", "the shared storage volumes of the utils mutation")
		assert.Contains(t, volumeNames(got), "secrets-data")
		assert.NotContains(t, got.Labels, nvcatypes.FunctionIDKey, "no chart metadata labels")
		for _, e := range got.Spec.Containers[0].Env {
			assert.NotEqual(t, "NVCF_FUNCTION_ID", e.Name, "no chart metadata env")
		}
	})

	t.Run("a forged pod copying the real owner reference is rejected and never persists", func(t *testing.T) {
		_, err := chart.CoreV1().Pods(ns).Create(ctx, utilsPod("forged", rsOwner), metav1.CreateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "enforce mode must reject it, got %v", err)
		_, err = admin.CoreV1().Pods(ns).Get(ctx, "forged", metav1.GetOptions{})
		assert.True(t, apierrors.IsNotFound(err), "a rejected pod must not exist")
	})

	t.Run("a forged label with no owner chain is rejected", func(t *testing.T) {
		p := utilsPod("forged-bare", rsOwner)
		p.OwnerReferences = nil
		_, err := chart.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{})
		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err), "got %v", err)
	})

	t.Run("a pod whose ReplicaSet cannot be verified is never admitted with the wrong mutation", func(t *testing.T) {
		missing := rsOwner
		missing.Name, missing.UID = "utils-not-yet", "11111111-1111-1111-1111-111111111111"
		_, err := rsCtl.CoreV1().Pods(ns).Create(ctx, utilsPod("utils-unverified", missing), metav1.CreateOptions{})
		require.Error(t, err, "a lookup failure must refuse the pod, not admit it as a chart pod")
		assert.True(t, apierrors.IsServiceUnavailable(err) || apierrors.IsInternalError(err) || apierrors.IsForbidden(err),
			"a retryable or refusing error, got %v", err)
		_, err = admin.CoreV1().Pods(ns).Get(ctx, "utils-unverified", metav1.GetOptions{})
		assert.True(t, apierrors.IsNotFound(err), "nothing must persist for a pod that could not be verified")
	})

	t.Run("a chart pod is still mutated as a chart pod", func(t *testing.T) {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}}}
		_, err := chart.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{})
		require.NoError(t, err)
		got, err := admin.CoreV1().Pods(ns).Get(ctx, "web", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Contains(t, got.Labels, nvcatypes.FunctionIDKey, "chart metadata is injected into a chart pod")
	})

	t.Run("the legacy bare utils pod from NVCA keeps the utils mutation", func(t *testing.T) {
		p := utilsPod("utils", rsOwner)
		p.Labels, p.OwnerReferences = nil, nil
		p.Annotations[nvcatypes.InfraObjectAnnotationKey] = "true"
		_, err := nvca.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{})
		require.NoError(t, err)
		got, err := admin.CoreV1().Pods(ns).Get(ctx, "utils", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Contains(t, volumeNames(got), "kns-data")
		assert.NotContains(t, got.Labels, nvcatypes.FunctionIDKey)
	})
}
