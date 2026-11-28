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
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/core"
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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cmnnvcastorage "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/storage"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

const (
	clsRS    = utilsprovenance.DefaultReplicaSetController
	clsNVCA  = "system:serviceaccount:nvca-system:nvca"
	clsChart = "system:serviceaccount:" + testNamespace + ":miniservice-instance-permissions"
)

// clsLookup serves a genuine owner chain MiniService, Deployment, ReplicaSet for the classification tests.
type clsLookup struct {
	rs      *appsv1.ReplicaSet
	dep     *appsv1.Deployment
	ms      *utilsprovenance.MiniServiceRef
	rsCalls int
}

func newClsLookup() *clsLookup {
	labels := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils, "app": "utils"}
	withHash := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils, "app": "utils", "pod-template-hash": "h1"}
	ctrl := func(kind, name, uid string) metav1.OwnerReference {
		return metav1.OwnerReference{Kind: kind, Name: name, UID: types.UID(uid), Controller: ptr.To(true)}
	}
	return &clsLookup{
		rs: &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: "utils-h1", Namespace: testNamespace, UID: "rs-uid",
				OwnerReferences: []metav1.OwnerReference{ctrl("Deployment", "utils", "dep-uid")}},
			Spec: appsv1.ReplicaSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: withHash}}},
		},
		dep: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "utils", Namespace: testNamespace, UID: "dep-uid",
				OwnerReferences: []metav1.OwnerReference{ctrl("MiniService", "ms1", "ms-uid")}},
			Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}}},
		},
		ms: &utilsprovenance.MiniServiceRef{UID: "ms-uid", Namespace: testNamespace},
	}
}

func (l *clsLookup) GetReplicaSet(_ context.Context, _, _ string) (*appsv1.ReplicaSet, error) {
	l.rsCalls++
	if l.rs == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "replicasets"}, "utils-h1")
	}
	return l.rs, nil
}
func (l *clsLookup) GetDeployment(context.Context, string, string) (*appsv1.Deployment, error) {
	return l.dep, nil
}
func (l *clsLookup) GetMiniService(context.Context, string) (*utilsprovenance.MiniServiceRef, error) {
	return l.ms, nil
}
func (l *clsLookup) FindMiniServiceByNamespace(context.Context, string) (*utilsprovenance.MiniServiceRef, error) {
	return l.ms, nil
}

func clsEngine(l utilsprovenance.Lookup) *utilsprovenance.Engine {
	return &utilsprovenance.Engine{
		Identities: utilsprovenance.Identities{NVCA: clsNVCA, ReplicaSetController: clsRS},
		Lookup:     l,
	}
}

// haUtilsPod is a pod the ReplicaSet controller creates for NVCA's utils Deployment: it carries the reserved label and
// the shared storage annotations from the template, and no volumes yet (the webhook adds them).
func haUtilsPod() *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "utils-h1-x2k4q", Namespace: testNamespace,
			Labels: map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils, "app": "utils", "pod-template-hash": "h1"},
			Annotations: map[string]string{
				cmnnvcastorage.HelmWebhookSharedStorageKNSReadWritePVCNameAnnotationKey:     "kns-pvc-rw",
				cmnnvcastorage.HelmWebhookSharedStorageSecretsReadWritePVCNameAnnotationKey: "secrets-pvc-rw",
			},
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "utils-h1", UID: "rs-uid", Controller: ptr.To(true)}},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "utils", Image: "app:latest"}}},
	}
}

func clsRequest(t *testing.T, pod *corev1.Pod, user string, op admissionv1.Operation) admission.Request {
	t.Helper()
	raw, err := json.Marshal(pod)
	require.NoError(t, err)
	req := admission.Request{}
	req.Namespace = testNamespace
	req.Kind = metav1.GroupVersionKind{Version: "v1", Kind: "Pod"}
	req.Object = runtime.RawExtension{Raw: raw}
	req.Operation = op
	req.UserInfo = authenticationv1.UserInfo{Username: user}
	return req
}

func clsWebhook(t *testing.T, c utilsprovenance.Classifier) *miniserviceMutatingWebhook {
	t.Helper()
	wh := makeWebhook(t, testConfigMap(t, testNamespace))
	wh.classifier = c
	wh.classifyBackoffs = []time.Duration{time.Millisecond, time.Millisecond}
	return wh
}

func TestMiniserviceWebhookClassifiesHAUtilsPodsByProvenance(t *testing.T) {
	ctx := core.WithDefaultLogger(context.Background())

	t.Run("a verified HA utils pod gets the shared storage mutation, not chart metadata", func(t *testing.T) {
		wh := clsWebhook(t, clsEngine(newClsLookup()))
		req := clsRequest(t, haUtilsPod(), clsRS, admissionv1.Create)
		resp := wh.Handle(ctx, req)
		require.True(t, resp.Allowed, "%v", resp.Result)

		var got corev1.Pod
		require.NoError(t, json.Unmarshal(applyPatches(t, req.Object.Raw, resp.Patches), &got))
		require.Len(t, got.Spec.Volumes, 2, "the shared storage volumes are added")
		assert.Equal(t, "kns-data", got.Spec.Volumes[0].Name)
		assert.Empty(t, got.Spec.Containers[0].Env, "no chart metadata env injected into a utils pod")
		assert.NotContains(t, got.Labels, nvcatypes.FunctionIDKey, "no chart metadata labels injected")
	})

	t.Run("the same pod from a chart identity is not utils and gets no utils treatment", func(t *testing.T) {
		wh := clsWebhook(t, clsEngine(newClsLookup()))
		req := clsRequest(t, haUtilsPod(), clsChart, admissionv1.Create)
		resp := wh.Handle(ctx, req)
		require.True(t, resp.Allowed, "%v", resp.Result)

		var got corev1.Pod
		require.NoError(t, json.Unmarshal(applyPatches(t, req.Object.Raw, resp.Patches), &got))
		assert.Contains(t, got.Labels, nvcatypes.FunctionIDKey, "treated as a chart pod: chart metadata is injected")
		assert.NotEmpty(t, got.Spec.Containers[0].Env)
	})

	t.Run("a forged label with no owner chain is a chart pod", func(t *testing.T) {
		wh := clsWebhook(t, clsEngine(newClsLookup()))
		pod := haUtilsPod()
		pod.OwnerReferences = nil
		req := clsRequest(t, pod, clsRS, admissionv1.Create)
		resp := wh.Handle(ctx, req)
		require.True(t, resp.Allowed)
		var got corev1.Pod
		require.NoError(t, json.Unmarshal(applyPatches(t, req.Object.Raw, resp.Patches), &got))
		assert.Contains(t, got.Labels, nvcatypes.FunctionIDKey)
	})

	t.Run("a ReplicaSet not visible yet is refused with a retryable error, never classified as a chart pod", func(t *testing.T) {
		l := newClsLookup()
		l.rs = nil
		wh := clsWebhook(t, clsEngine(l))
		resp := wh.Handle(ctx, clsRequest(t, haUtilsPod(), clsRS, admissionv1.Create))
		assert.False(t, resp.Allowed)
		require.NotNil(t, resp.Result)
		assert.EqualValues(t, http.StatusServiceUnavailable, resp.Result.Code)
		assert.Contains(t, resp.Result.Message, "utils-provenance[")
		assert.Equal(t, 3, l.rsCalls, "one look and the bounded retries")
	})

	t.Run("a lookup that resolves during the retries is classified as utils", func(t *testing.T) {
		l := newClsLookup()
		good := l.rs
		l.rs = nil
		wh := clsWebhook(t, &flipClassifier{inner: clsEngine(l), flipAfter: 2, l: l, rs: good})
		req := clsRequest(t, haUtilsPod(), clsRS, admissionv1.Create)
		resp := wh.Handle(ctx, req)
		require.True(t, resp.Allowed, "%v", resp.Result)
		var got corev1.Pod
		require.NoError(t, json.Unmarshal(applyPatches(t, req.Object.Raw, resp.Patches), &got))
		assert.Len(t, got.Spec.Volumes, 2)
	})

	t.Run("an update of a verified HA utils pod is left alone, no chart metadata", func(t *testing.T) {
		wh := clsWebhook(t, clsEngine(newClsLookup()))
		resp := wh.Handle(ctx, clsRequest(t, haUtilsPod(), clsNVCA, admissionv1.Update))
		require.True(t, resp.Allowed)
		assert.Empty(t, resp.Patches, "a utils pod is only mutated on create")
	})

	t.Run("an update of a labeled pod with no chain is a chart pod", func(t *testing.T) {
		wh := clsWebhook(t, clsEngine(newClsLookup()))
		pod := haUtilsPod()
		pod.OwnerReferences = nil
		resp := wh.Handle(ctx, clsRequest(t, pod, clsChart, admissionv1.Update))
		require.True(t, resp.Allowed)
		assert.NotEmpty(t, resp.Patches, "chart metadata is applied, as for any chart pod")
	})
}

// flipClassifier makes the lookup succeed after a number of classifications, like an object that becomes visible.
type flipClassifier struct {
	inner     utilsprovenance.Classifier
	flipAfter int
	calls     int
	l         *clsLookup
	rs        *appsv1.ReplicaSet
}

func (f *flipClassifier) ClassifyUtilsPod(ctx context.Context, user, ns string, pod *corev1.Pod, create bool) (bool, utilsprovenance.Decision) {
	f.calls++
	if f.calls > f.flipAfter {
		f.l.rs = f.rs
	}
	return f.inner.ClassifyUtilsPod(ctx, user, ns, pod, create)
}

type countingClassifier struct{ calls int }

func (c *countingClassifier) ClassifyUtilsPod(context.Context, string, string, *corev1.Pod, bool) (bool, utilsprovenance.Decision) {
	c.calls++
	return false, utilsprovenance.Decision{}
}

func TestMiniserviceWebhookWithoutAClassifierKeepsTodaysBehavior(t *testing.T) {
	ctx := core.WithDefaultLogger(context.Background())

	t.Run("no classifier: a labeled pod is a chart pod, the legacy utils pod is utils", func(t *testing.T) {
		wh := clsWebhook(t, nil)
		req := clsRequest(t, haUtilsPod(), clsRS, admissionv1.Create)
		resp := wh.Handle(ctx, req)
		require.True(t, resp.Allowed)
		var got corev1.Pod
		require.NoError(t, json.Unmarshal(applyPatches(t, req.Object.Raw, resp.Patches), &got))
		assert.Contains(t, got.Labels, nvcatypes.FunctionIDKey)

		legacy := haUtilsPod()
		legacy.Name, legacy.Labels, legacy.OwnerReferences = "utils", nil, nil
		req = clsRequest(t, legacy, clsNVCA, admissionv1.Create)
		resp = wh.Handle(ctx, req)
		require.True(t, resp.Allowed)
		require.NoError(t, json.Unmarshal(applyPatches(t, req.Object.Raw, resp.Patches), &got))
		assert.Len(t, got.Spec.Volumes, 2)
	})

	t.Run("the classifier is only consulted for pods that claim the label and are not the legacy pod", func(t *testing.T) {
		cc := &countingClassifier{}
		wh := clsWebhook(t, cc)
		plain := &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: testNamespace, Labels: miniserviceNameLabels()},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "x"}}}}
		wh.Handle(ctx, clsRequest(t, plain, clsChart, admissionv1.Create))
		legacy := haUtilsPod()
		legacy.Name, legacy.Labels, legacy.OwnerReferences = "utils", nil, nil
		wh.Handle(ctx, clsRequest(t, legacy, clsNVCA, admissionv1.Create))
		assert.Equal(t, 0, cc.calls)
		wh.Handle(ctx, clsRequest(t, haUtilsPod(), clsRS, admissionv1.Create))
		assert.Equal(t, 1, cc.calls)
	})
}

// slowRetryable is a classifier whose every verification is retryable after a delay, like a slow, unhealthy API server.
type slowRetryable struct {
	delay time.Duration
	calls int
}

func (c *slowRetryable) ClassifyUtilsPod(ctx context.Context, _, _ string, _ *corev1.Pod, _ bool) (bool, utilsprovenance.Decision) {
	c.calls++
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
	}
	return false, utilsprovenance.Decision{Reason: utilsprovenance.ReasonRetryLookup, Retryable: true, Message: "lookup failed"}
}

func TestClassificationRetriesShareOneDeadline(t *testing.T) {
	ctx := core.WithDefaultLogger(context.Background())

	t.Run("the whole sequence is bounded by one budget, not one per attempt", func(t *testing.T) {
		cc := &slowRetryable{delay: 200 * time.Millisecond}
		wh := clsWebhook(t, cc)
		wh.classifyBackoffs = []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond}
		wh.classifyBudget = 300 * time.Millisecond

		start := time.Now()
		resp := wh.Handle(ctx, clsRequest(t, haUtilsPod(), clsRS, admissionv1.Create))
		elapsed := time.Since(start)

		assert.False(t, resp.Allowed)
		assert.EqualValues(t, http.StatusServiceUnavailable, resp.Result.Code)
		// Without a shared deadline the five attempts of 200ms plus waits would take over a second.
		assert.Less(t, elapsed, 700*time.Millisecond, "the sequence must stop at the overall budget")
		assert.Less(t, cc.calls, 5, "attempts stop when the budget is spent")
	})

	t.Run("cancellation stops at once and never starts another attempt", func(t *testing.T) {
		cc := &slowRetryable{delay: time.Millisecond}
		wh := clsWebhook(t, cc)
		wh.classifyBackoffs = []time.Duration{time.Hour, time.Hour}
		cctx, cancel := context.WithCancel(ctx)
		go func() { time.Sleep(30 * time.Millisecond); cancel() }()

		start := time.Now()
		resp := wh.Handle(cctx, clsRequest(t, haUtilsPod(), clsRS, admissionv1.Create))
		assert.False(t, resp.Allowed)
		assert.Less(t, time.Since(start), 2*time.Second, "an hour-long backoff must not be waited out")
		assert.Equal(t, 1, cc.calls, "no attempt after the cancellation")
	})

	t.Run("a verification that succeeds before the budget ends is not turned into a refusal", func(t *testing.T) {
		wh := clsWebhook(t, clsEngine(newClsLookup()))
		wh.classifyBudget = time.Second
		resp := wh.Handle(ctx, clsRequest(t, haUtilsPod(), clsRS, admissionv1.Create))
		assert.True(t, resp.Allowed)
	})
}
