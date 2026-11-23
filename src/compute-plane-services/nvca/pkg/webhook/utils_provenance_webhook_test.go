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

package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
	nvcaclientset "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/client/clientset/versioned"
	nvcafake "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/client/clientset/versioned/fake"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
	whmetrics "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/webhook/metrics"
)

const (
	upNS    = "sr-abc"
	upNVCA  = "system:serviceaccount:nvca-system:nvca"
	upChart = "system:serviceaccount:sr-abc:instance-sa"
)

type upLookup struct{ err error }

func (l upLookup) GetReplicaSet(context.Context, string, string) (*appsv1.ReplicaSet, error) {
	return nil, l.err
}
func (l upLookup) GetDeployment(context.Context, string, string) (*appsv1.Deployment, error) {
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "deployments"}, "x")
}
func (l upLookup) GetMiniService(context.Context, string) (*utilsprovenance.MiniServiceRef, error) {
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "miniservices"}, "x")
}
func (l upLookup) FindMiniServiceByNamespace(context.Context, string) (*utilsprovenance.MiniServiceRef, error) {
	return &utilsprovenance.MiniServiceRef{UID: "ms", Namespace: upNS}, nil
}

func newUPWebhook(t *testing.T, mode UtilsProvenanceMode, lookupErr error) (*utilsProvenanceWebhook, context.Context, *whmetrics.Metrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	ctx := whmetrics.WithDefaultMetrics(context.Background(), whmetrics.WithRegisterer(reg))
	m := whmetrics.FromContext(ctx)
	_, err := NewUtilsProvenanceWebhook(ctx, "test", &utilsprovenance.Engine{
		Identities: utilsprovenance.Identities{
			NVCA:                 upNVCA,
			ReplicaSetController: utilsprovenance.DefaultReplicaSetController,
			DeploymentController: utilsprovenance.DefaultDeploymentController,
		},
		Lookup: upLookup{err: lookupErr},
	}, mode)
	require.NoError(t, err)
	sch := runtime.NewScheme()
	registerScheme(sch, corev1.SchemeGroupVersion, &corev1.Pod{})
	registerScheme(sch, appsv1.SchemeGroupVersion, &appsv1.Deployment{}, &appsv1.ReplicaSet{})
	return &utilsProvenanceWebhook{
		engine: &utilsprovenance.Engine{
			Identities: utilsprovenance.Identities{
				NVCA:                 upNVCA,
				ReplicaSetController: utilsprovenance.DefaultReplicaSetController,
				DeploymentController: utilsprovenance.DefaultDeploymentController,
			},
			Lookup: upLookup{err: lookupErr},
		},
		mode:          mode,
		decoder:       serializer.NewCodecFactory(sch).UniversalDeserializer(),
		retryBackoffs: []time.Duration{time.Millisecond},
	}, ctx, m
}

func rawPod(t *testing.T, labels map[string]string, refs ...metav1.OwnerReference) runtime.RawExtension {
	t.Helper()
	p := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: upNS, Labels: labels, OwnerReferences: refs},
	}
	b, err := json.Marshal(p)
	require.NoError(t, err)
	return runtime.RawExtension{Raw: b}
}

func podReq(t *testing.T, user string, labels map[string]string, refs ...metav1.OwnerReference) admission.Request {
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Kind:      metav1.GroupVersionKind{Version: "v1", Kind: "Pod"},
		Resource:  metav1.GroupVersionResource{Version: "v1", Resource: "pods"},
		Namespace: upNS, Name: "p",
		UserInfo: authenticationv1.UserInfo{Username: user},
		Object:   rawPod(t, labels, refs...),
	}}
}

var upForgedLabels = map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}

func counter(t *testing.T, m *whmetrics.Metrics, kind, decision, reason string) float64 {
	t.Helper()
	var out dto.Metric
	require.NoError(t, m.UtilsProvenanceDecisions.WithLabelValues(kind, decision, reason).Write(&out))
	return out.GetCounter().GetValue()
}

func TestUtilsProvenanceAuditAllowsAndCounts(t *testing.T) {
	w, ctx, m := newUPWebhook(t, UtilsProvenanceAudit, nil)
	resp := w.Handle(ctx, podReq(t, upChart, upForgedLabels))
	assert.True(t, resp.Allowed, "audit mode must never refuse")
	assert.NotEmpty(t, resp.Warnings)
	assert.Equal(t, 1.0, counter(t, m, "Pod", decisionAuditWouldDeny, string(utilsprovenance.ReasonDenyWriter)))
}

func TestUtilsProvenanceEnforceDeniesForgedLabel(t *testing.T) {
	w, ctx, m := newUPWebhook(t, UtilsProvenanceEnforce, nil)
	resp := w.Handle(ctx, podReq(t, upChart, upForgedLabels))
	assert.False(t, resp.Allowed)
	assert.EqualValues(t, http.StatusForbidden, resp.Result.Code)
	assert.Equal(t, 1.0, counter(t, m, "Pod", decisionDenyHard, string(utilsprovenance.ReasonDenyWriter)))
}

func TestUtilsProvenanceEnforceRetryableLookupFailure(t *testing.T) {
	w, ctx, m := newUPWebhook(t, UtilsProvenanceEnforce, apierrors.NewNotFound(schema.GroupResource{Resource: "replicasets"}, "rs"))
	ref := metav1.OwnerReference{Kind: "ReplicaSet", Name: "rs", UID: "u", Controller: ptrBool(true)}
	resp := w.Handle(ctx, podReq(t, utilsprovenance.DefaultReplicaSetController, upForgedLabels, ref))
	assert.False(t, resp.Allowed, "a pod claiming utils must not be admitted on a cache or lookup miss")
	assert.EqualValues(t, http.StatusServiceUnavailable, resp.Result.Code)
	assert.Equal(t, metav1.StatusReasonServiceUnavailable, resp.Result.Reason)
	assert.Equal(t, 1.0, counter(t, m, "Pod", decisionDenyRetry, string(utilsprovenance.ReasonRetryLookup)))
}

func TestUtilsProvenanceOrdinaryPodAndOutOfScope(t *testing.T) {
	w, ctx, m := newUPWebhook(t, UtilsProvenanceEnforce, nil)

	resp := w.Handle(ctx, podReq(t, upChart, map[string]string{"app": "x"}))
	assert.True(t, resp.Allowed)
	assert.Equal(t, 1.0, counter(t, m, "Pod", decisionAllow, string(utilsprovenance.ReasonUnprotected)))

	del := podReq(t, upChart, upForgedLabels)
	del.Operation = admissionv1.Delete
	assert.True(t, w.Handle(ctx, del).Allowed, "delete is out of scope")

	other := podReq(t, upChart, upForgedLabels)
	other.Kind = metav1.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
	assert.True(t, w.Handle(ctx, other).Allowed, "other kinds are out of scope")

	bad := podReq(t, upChart, upForgedLabels)
	bad.Object = runtime.RawExtension{Raw: []byte("{not json")}
	assert.True(t, w.Handle(ctx, bad).Allowed, "an unreadable request is never blocked")
}

func TestUtilsProvenanceNVCAIsAllowed(t *testing.T) {
	w, ctx, _ := newUPWebhook(t, UtilsProvenanceEnforce, nil)
	assert.True(t, w.Handle(ctx, podReq(t, upNVCA, upForgedLabels)).Allowed)
}

func TestUtilsProvenanceScaleRequestMapping(t *testing.T) {
	w, ctx, _ := newUPWebhook(t, UtilsProvenanceEnforce, nil)
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation:   admissionv1.Update,
		Kind:        metav1.GroupVersionKind{Group: "autoscaling", Version: "v1", Kind: "Scale"},
		Resource:    metav1.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
		SubResource: "scale",
		Namespace:   upNS, Name: "utils",
		UserInfo: authenticationv1.UserInfo{Username: upChart},
	}}
	ereq, err := w.toEngineRequest(req)
	require.NoError(t, err)
	require.NotNil(t, ereq)
	assert.Equal(t, utilsprovenance.KindScale, ereq.Kind)
	assert.Equal(t, "utils", ereq.Name)
	// The target Deployment is not found by the fake lookup, so the engine allows it. The mapping is what matters.
	assert.True(t, w.Handle(ctx, req).Allowed)
}

func TestUtilsProvenanceMetricsPreInitialized(t *testing.T) {
	_, _, m := newUPWebhook(t, UtilsProvenanceAudit, nil)
	for _, k := range utilsProvenanceKinds {
		for _, d := range utilsProvenanceDecisions {
			for _, r := range utilsprovenance.AllReasons {
				assert.Equal(t, 0.0, counter(t, m, k, d, string(r)))
			}
		}
	}
}

func TestParseUtilsProvenanceMode(t *testing.T) {
	for in, want := range map[string]UtilsProvenanceMode{"": UtilsProvenanceAudit, "audit": UtilsProvenanceAudit, "enforce": UtilsProvenanceEnforce} {
		got, err := ParseUtilsProvenanceMode(in)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	_, err := ParseUtilsProvenanceMode("block")
	assert.Error(t, err)
}

func TestNewUtilsProvenanceWebhookValidation(t *testing.T) {
	_, err := NewUtilsProvenanceWebhook(context.Background(), "x", nil, UtilsProvenanceAudit)
	assert.Error(t, err)
	_, err = NewUtilsProvenanceWebhook(context.Background(), "x", &utilsprovenance.Engine{}, "weird")
	assert.Error(t, err)
}

func ptrBool(b bool) *bool { return &b }

// TestUtilsProvenanceHTTPHandler sends a real AdmissionReview through the standalone handler.
func TestUtilsProvenanceHTTPHandler(t *testing.T) {
	reg := prometheus.NewRegistry()
	ctx := whmetrics.WithDefaultMetrics(context.Background(), whmetrics.WithRegisterer(reg))
	h, err := NewUtilsProvenanceWebhook(ctx, "validate-utils-provenance.test", &utilsprovenance.Engine{
		Identities: utilsprovenance.Identities{NVCA: upNVCA},
		Lookup:     upLookup{},
	}, UtilsProvenanceEnforce)
	require.NoError(t, err)

	send := func(labels map[string]string) *admissionv1.AdmissionReview {
		areq := podReq(t, upChart, labels).AdmissionRequest
		areq.UID = "uid-1"
		review := admissionv1.AdmissionReview{
			TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
			Request:  &areq,
		}
		body, err := json.Marshal(review)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		hr := httptest.NewRequest(http.MethodPost, "/validate-utils-provenance", bytes.NewReader(body))
		hr.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, hr)
		require.Equal(t, http.StatusOK, rec.Code)
		var out admissionv1.AdmissionReview
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		return &out
	}

	ok := send(map[string]string{"app": "x"})
	assert.True(t, ok.Response.Allowed)
	bad := send(upForgedLabels)
	assert.False(t, bad.Response.Allowed)
	assert.EqualValues(t, http.StatusForbidden, bad.Response.Result.Code)
}

func TestNewUtilsProvenanceConfigFailures(t *testing.T) {
	tmpNVCA, tmpSelf := newNVCAClientset, selfUsername
	t.Cleanup(func() { newNVCAClientset, selfUsername = tmpNVCA, tmpSelf })

	newNVCAClientset = func(context.Context, string) (nvcaclientset.Interface, error) {
		return nil, assert.AnError
	}
	_, err := newUtilsProvenanceConfig(context.Background(), "", nil, UtilsProvenanceAudit, "rs", "dep")
	assert.ErrorIs(t, err, assert.AnError)

	newNVCAClientset = func(context.Context, string) (nvcaclientset.Interface, error) {
		return nvcafake.NewSimpleClientset(), nil
	}
	selfUsername = func(context.Context, kubernetes.Interface) (string, error) { return "", assert.AnError }
	_, err = newUtilsProvenanceConfig(context.Background(), "", nil, UtilsProvenanceAudit, "rs", "dep")
	assert.ErrorIs(t, err, assert.AnError)

	selfUsername = func(context.Context, kubernetes.Interface) (string, error) { return "me", nil }
	cfg, err := newUtilsProvenanceConfig(context.Background(), "", k8sfake.NewSimpleClientset(), UtilsProvenanceEnforce, "rs", "dep")
	require.NoError(t, err)
	assert.Equal(t, "me", cfg.engine.Identities.NVCA)
	assert.Equal(t, "rs", cfg.engine.Identities.ReplicaSetController)
	assert.Equal(t, UtilsProvenanceEnforce, cfg.mode)
}

func TestSelfUsernameResolvesFromSelfSubjectReview(t *testing.T) {
	kube := k8sfake.NewSimpleClientset()
	kube.PrependReactor("create", "selfsubjectreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authenticationv1.SelfSubjectReview{
			Status: authenticationv1.SelfSubjectReviewStatus{UserInfo: authenticationv1.UserInfo{Username: "system:serviceaccount:nvca-system:nvca"}},
		}, nil
	})
	got, err := selfUsername(context.Background(), kube)
	require.NoError(t, err)
	assert.Equal(t, "system:serviceaccount:nvca-system:nvca", got)
}

// flakyRSLookup serves the ReplicaSet chain only after missAttempts calls, like an object that is not visible yet.
type flakyRSLookup struct {
	upLookup
	missAttempts int
	calls        int
	rs           *appsv1.ReplicaSet
	dep          *appsv1.Deployment
	ms           *utilsprovenance.MiniServiceRef
}

func (f *flakyRSLookup) GetReplicaSet(context.Context, string, string) (*appsv1.ReplicaSet, error) {
	f.calls++
	if f.calls <= f.missAttempts {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "replicasets"}, "utils-h1")
	}
	return f.rs, nil
}

func (f *flakyRSLookup) GetDeployment(context.Context, string, string) (*appsv1.Deployment, error) {
	return f.dep, nil
}

func (f *flakyRSLookup) GetMiniService(context.Context, string) (*utilsprovenance.MiniServiceRef, error) {
	return f.ms, nil
}

func newFlakyChain(miss int) (*flakyRSLookup, admission.Request) {
	ctrl := func(kind, name, uid string) metav1.OwnerReference {
		return metav1.OwnerReference{Kind: kind, Name: name, UID: types.UID(uid), Controller: ptrBool(true)}
	}
	labels := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils, "app": "utils"}
	withHash := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils, "app": "utils", "pod-template-hash": "h1"}
	f := &flakyRSLookup{
		missAttempts: miss,
		rs: &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: "utils-h1", UID: "rs", OwnerReferences: []metav1.OwnerReference{ctrl("Deployment", "utils", "dep")}},
			Spec:       appsv1.ReplicaSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: withHash}}},
		},
		dep: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "utils", UID: "dep", OwnerReferences: []metav1.OwnerReference{ctrl("MiniService", "ms", "msuid")}},
			Spec:       appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}}},
		},
		ms: &utilsprovenance.MiniServiceRef{UID: "msuid", Namespace: upNS},
	}
	return f, admission.Request{} // the request is filled by the caller
}

func TestUtilsProvenanceInternalRetryResolvesCacheLag(t *testing.T) {
	ref := metav1.OwnerReference{Kind: "ReplicaSet", Name: "utils-h1", UID: "rs", Controller: ptrBool(true)}
	labels := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils, "app": "utils", "pod-template-hash": "h1"}

	for _, tt := range []struct {
		name      string
		miss      int
		allowed   bool
		retryable bool
		calls     int
	}{
		{name: "visible on the third look", miss: 2, allowed: true, calls: 3},
		{name: "never visible stays retryable after the bounded retries", miss: 100, retryable: true, calls: 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, _ := newFlakyChain(tt.miss)
			w := &utilsProvenanceWebhook{
				engine: &utilsprovenance.Engine{
					Identities: utilsprovenance.Identities{ReplicaSetController: utilsprovenance.DefaultReplicaSetController},
					Lookup:     f,
				},
				mode:          UtilsProvenanceEnforce,
				decoder:       mustDecoder(),
				retryBackoffs: []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond},
			}
			resp := w.Handle(context.Background(), podReq(t, utilsprovenance.DefaultReplicaSetController, labels, ref))
			assert.Equal(t, tt.allowed, resp.Allowed)
			if tt.retryable {
				assert.EqualValues(t, http.StatusServiceUnavailable, resp.Result.Code)
			}
			assert.Equal(t, tt.calls, f.calls, "number of ReplicaSet lookups")
		})
	}
}

func TestUtilsProvenanceRetryStopsOnContextEnd(t *testing.T) {
	f, _ := newFlakyChain(100)
	w := &utilsProvenanceWebhook{
		engine: &utilsprovenance.Engine{
			Identities: utilsprovenance.Identities{ReplicaSetController: utilsprovenance.DefaultReplicaSetController},
			Lookup:     f,
		},
		mode:          UtilsProvenanceEnforce,
		decoder:       mustDecoder(),
		retryBackoffs: []time.Duration{time.Hour},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	labels := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils, "pod-template-hash": "h1"}
	ref := metav1.OwnerReference{Kind: "ReplicaSet", Name: "utils-h1", UID: "rs", Controller: ptrBool(true)}
	start := time.Now()
	resp := w.Handle(ctx, podReq(t, utilsprovenance.DefaultReplicaSetController, labels, ref))
	assert.False(t, resp.Allowed)
	assert.Less(t, time.Since(start), 5*time.Second, "an ended context must not wait out the backoff")
}

func mustDecoder() runtime.Decoder {
	sch := runtime.NewScheme()
	registerScheme(sch, corev1.SchemeGroupVersion, &corev1.Pod{})
	registerScheme(sch, appsv1.SchemeGroupVersion, &appsv1.Deployment{}, &appsv1.ReplicaSet{})
	return serializer.NewCodecFactory(sch).UniversalDeserializer()
}
