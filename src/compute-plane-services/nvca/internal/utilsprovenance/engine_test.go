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

package utilsprovenance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

const (
	ns       = "sr-abc"
	nvcaUser = "system:serviceaccount:nvca-system:nvca"
	rsUser   = DefaultReplicaSetController
	depUser  = DefaultDeploymentController
	chartSA  = "system:serviceaccount:sr-abc:instance-sa"
)

type fakeLookup struct {
	rs       map[string]*appsv1.ReplicaSet
	deps     map[string]*appsv1.Deployment
	ms       map[string]*MiniServiceRef
	byNS     map[string]*MiniServiceRef
	rsErr    error
	delayRS  time.Duration
	rsCalls  int
	depCalls int
}

func (f *fakeLookup) GetReplicaSet(ctx context.Context, namespace, name string) (*appsv1.ReplicaSet, error) {
	f.rsCalls++
	if f.delayRS > 0 {
		select {
		case <-time.After(f.delayRS):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.rsErr != nil {
		return nil, f.rsErr
	}
	if rs, ok := f.rs[namespace+"/"+name]; ok {
		return rs, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "replicasets"}, name)
}

func (f *fakeLookup) GetDeployment(_ context.Context, namespace, name string) (*appsv1.Deployment, error) {
	f.depCalls++
	if d, ok := f.deps[namespace+"/"+name]; ok {
		return d, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "deployments"}, name)
}

func (f *fakeLookup) GetMiniService(_ context.Context, name string) (*MiniServiceRef, error) {
	if m, ok := f.ms[name]; ok {
		return m, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "miniservices"}, name)
}

func (f *fakeLookup) FindMiniServiceByNamespace(_ context.Context, namespace string) (*MiniServiceRef, error) {
	if m, ok := f.byNS[namespace]; ok {
		return m, nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "miniservices"}, namespace)
}

var roleLabels = map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils, "app": "utils"}

func withHash(l map[string]string, h string) map[string]string {
	out := map[string]string{}
	for k, v := range l {
		out[k] = v
	}
	out[PodTemplateHashLabel] = h
	return out
}

func ctrlRef(kind, name, uid string) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: "x/v1", Kind: kind, Name: name, UID: types.UID(uid), Controller: ptr.To(true)}
}

// chain builds a valid MiniService, Deployment and ReplicaSet and returns a lookup that serves them.
func chain() (*fakeLookup, *appsv1.Deployment, *appsv1.ReplicaSet) {
	ms := &MiniServiceRef{UID: "ms-uid", Namespace: ns}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "utils", Namespace: ns, UID: "dep-uid",
			OwnerReferences: []metav1.OwnerReference{ctrlRef("MiniService", "ms1", "ms-uid")}},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: roleLabels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: roleLabels}},
		},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "utils-h1", Namespace: ns, UID: "rs-uid",
			OwnerReferences: []metav1.OwnerReference{ctrlRef("Deployment", "utils", "dep-uid")}},
		Spec: appsv1.ReplicaSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: withHash(roleLabels, "h1")}}},
	}
	return &fakeLookup{
		rs:   map[string]*appsv1.ReplicaSet{ns + "/utils-h1": rs},
		deps: map[string]*appsv1.Deployment{ns + "/utils": dep},
		ms:   map[string]*MiniServiceRef{"ms1": ms},
		byNS: map[string]*MiniServiceRef{ns: ms},
	}, dep, rs
}

func engineFor(l *fakeLookup) *Engine {
	return &Engine{
		Identities: Identities{NVCA: nvcaUser, ReplicaSetController: rsUser, DeploymentController: depUser},
		Lookup:     l,
	}
}

func utilsPod(labels map[string]string, refs ...metav1.OwnerReference) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "utils-h1-x", Namespace: ns, Labels: labels, OwnerReferences: refs}}
}

func podCreate(user string, p *corev1.Pod) Request {
	return Request{Operation: OperationCreate, Kind: KindPod, Username: user, Namespace: ns, Name: p.Name, Object: p}
}

func TestPodCreate(t *testing.T) {
	goodRef := ctrlRef("ReplicaSet", "utils-h1", "rs-uid")
	tests := []struct {
		name   string
		mutate func(l *fakeLookup, d *appsv1.Deployment, rs *appsv1.ReplicaSet)
		req    func() Request
		allow  bool
		retry  bool
		reason Reason
	}{
		{
			name: "ordinary chart pod never reaches the rules",
			req: func() Request {
				return podCreate(chartSA, utilsPod(map[string]string{"app": "x"}))
			},
			allow: true, reason: ReasonUnprotected,
		},
		{
			name: "F1 forged label, no owner, chart identity",
			req: func() Request {
				return podCreate(chartSA, utilsPod(roleLabels))
			},
			reason: ReasonDenyWriter,
		},
		{
			name: "F2 forged label with owner reference to the real ReplicaSet, chart identity",
			req: func() Request {
				return podCreate(chartSA, utilsPod(withHash(roleLabels, "h1"), goodRef))
			},
			reason: ReasonDenyWriter,
		},
		{
			name: "valid chain from the ReplicaSet controller",
			req: func() Request {
				return podCreate(rsUser, utilsPod(withHash(roleLabels, "h1"), goodRef))
			},
			allow: true, reason: ReasonChainVerified,
		},
		{
			name:  "NVCA bare pod in a live MiniService namespace",
			req:   func() Request { return podCreate(nvcaUser, utilsPod(roleLabels)) },
			allow: true, reason: ReasonNVCAWriter,
		},
		{
			name:   "NVCA bare pod in a namespace without a MiniService",
			mutate: func(l *fakeLookup, _ *appsv1.Deployment, _ *appsv1.ReplicaSet) { l.byNS = nil },
			req:    func() Request { return podCreate(nvcaUser, utilsPod(roleLabels)) },
			retry:  true, reason: ReasonRetryLookup,
		},
		{
			name: "F7 owner UID differs from the ReplicaSet UID",
			req: func() Request {
				return podCreate(rsUser, utilsPod(withHash(roleLabels, "h1"), ctrlRef("ReplicaSet", "utils-h1", "other")))
			},
			reason: ReasonDenyUIDMismatch,
		},
		{
			name: "no controller owner",
			req: func() Request {
				return podCreate(rsUser, utilsPod(withHash(roleLabels, "h1")))
			},
			reason: ReasonDenyNoOwner,
		},
		{
			name:   "F8 ReplicaSet not visible yet is retryable, never admitted",
			mutate: func(l *fakeLookup, _ *appsv1.Deployment, _ *appsv1.ReplicaSet) { l.rs = nil },
			req: func() Request {
				return podCreate(rsUser, utilsPod(withHash(roleLabels, "h1"), goodRef))
			},
			retry: true, reason: ReasonRetryLookup,
		},
		{
			name: "F13 pod of an older revision is checked against its own ReplicaSet",
			mutate: func(l *fakeLookup, d *appsv1.Deployment, rs *appsv1.ReplicaSet) {
				// The Deployment template has moved on (new label), the old ReplicaSet keeps the old one.
				d.Spec.Template.Labels = map[string]string{
					nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils, "app": "utils", "rev": "2"}
			},
			req: func() Request {
				return podCreate(rsUser, utilsPod(withHash(roleLabels, "h1"), goodRef))
			},
			allow: true, reason: ReasonChainVerified,
		},
		{
			name: "pod labels differ from the ReplicaSet template",
			req: func() Request {
				l := withHash(roleLabels, "h1")
				l["extra"] = "x"
				return podCreate(rsUser, utilsPod(l, goodRef))
			},
			reason: ReasonDenyTemplate,
		},
		{
			name:   "F9 Deployment without a MiniService owner",
			mutate: func(_ *fakeLookup, d *appsv1.Deployment, _ *appsv1.ReplicaSet) { d.OwnerReferences = nil },
			req: func() Request {
				return podCreate(rsUser, utilsPod(withHash(roleLabels, "h1"), goodRef))
			},
			reason: ReasonDenyMiniService,
		},
		{
			name:   "MiniService UID differs",
			mutate: func(l *fakeLookup, _ *appsv1.Deployment, _ *appsv1.ReplicaSet) { l.ms["ms1"].UID = "forged" },
			req: func() Request {
				return podCreate(rsUser, utilsPod(withHash(roleLabels, "h1"), goodRef))
			},
			reason: ReasonDenyUIDMismatch,
		},
		{
			name:   "MiniService owns another namespace",
			mutate: func(l *fakeLookup, _ *appsv1.Deployment, _ *appsv1.ReplicaSet) { l.ms["ms1"].Namespace = "sr-other" },
			req: func() Request {
				return podCreate(rsUser, utilsPod(withHash(roleLabels, "h1"), goodRef))
			},
			reason: ReasonDenyMiniService,
		},
		{
			name: "chart Deployment pod with the label from the ReplicaSet controller but no NVCA Deployment",
			mutate: func(_ *fakeLookup, d *appsv1.Deployment, rs *appsv1.ReplicaSet) {
				rs.OwnerReferences = []metav1.OwnerReference{ctrlRef("Deployment", "chart-dep", "chart-uid")}
			},
			req: func() Request {
				return podCreate(rsUser, utilsPod(withHash(roleLabels, "h1"), goodRef))
			},
			retry: true, reason: ReasonRetryLookup,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, d, rs := chain()
			if tt.mutate != nil {
				tt.mutate(l, d, rs)
			}
			got := engineFor(l).Evaluate(context.Background(), tt.req())
			assert.Equal(t, tt.allow, got.Allowed, got.Message)
			assert.Equal(t, tt.retry, got.Retryable, got.Message)
			assert.Equal(t, tt.reason, got.Reason, got.Message)
		})
	}
}

func TestPodCreateLookupTimeoutIsRetryable(t *testing.T) {
	l, _, _ := chain()
	l.delayRS = time.Second
	e := engineFor(l)
	e.LookupTimeout = 20 * time.Millisecond
	got := e.Evaluate(context.Background(), podCreate(rsUser, utilsPod(withHash(roleLabels, "h1"),
		ctrlRef("ReplicaSet", "utils-h1", "rs-uid"))))
	assert.False(t, got.Allowed)
	assert.True(t, got.Retryable)
}

func TestPodCreateLookupErrorIsRetryable(t *testing.T) {
	l, _, _ := chain()
	l.rsErr = errors.New("apiserver unavailable")
	got := engineFor(l).Evaluate(context.Background(), podCreate(rsUser, utilsPod(withHash(roleLabels, "h1"),
		ctrlRef("ReplicaSet", "utils-h1", "rs-uid"))))
	assert.False(t, got.Allowed)
	assert.True(t, got.Retryable)
}

func TestPodUpdate(t *testing.T) {
	base := func() *corev1.Pod {
		return utilsPod(withHash(roleLabels, "h1"), ctrlRef("ReplicaSet", "utils-h1", "rs-uid"))
	}
	upd := func(user, sub string, old, cur *corev1.Pod) Request {
		return Request{Operation: OperationUpdate, Kind: KindPod, Subresource: sub, Username: user,
			Namespace: ns, Name: cur.Name, Object: cur, OldObject: old}
	}
	legacy := func(annotation string) *corev1.Pod {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "utils", Namespace: ns}}
		if annotation != "" {
			p.Annotations = map[string]string{nvcatypes.InfraObjectAnnotationKey: annotation}
		}
		return p
	}
	t.Run("F6 chart adds the label to an existing pod", func(t *testing.T) {
		old := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: ns}}
		cur := old.DeepCopy()
		cur.Labels = roleLabels
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(chartSA, "", old, cur))
		assert.False(t, got.Allowed)
		assert.Equal(t, ReasonDenyWriter, got.Reason)
	})
	t.Run("F6 chart removes the label from a utils pod", func(t *testing.T) {
		old := base()
		cur := old.DeepCopy()
		delete(cur.Labels, nvcatypes.PodRoleLabelKey)
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(chartSA, "", old, cur))
		assert.False(t, got.Allowed)
		assert.Equal(t, ReasonDenyProtectedEdit, got.Reason)
	})
	t.Run("F10 ephemeral container added by a chart identity", func(t *testing.T) {
		old := base()
		cur := old.DeepCopy()
		cur.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "dbg"}}}
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(chartSA, "ephemeralcontainers", old, cur))
		assert.False(t, got.Allowed)
		assert.Equal(t, ReasonDenyProtectedEdit, got.Reason)
	})
	t.Run("NVCA may change a protected pod", func(t *testing.T) {
		old := base()
		cur := old.DeepCopy()
		cur.Annotations = map[string]string{"nvca.nvcf.nvidia.io/worker-restart-count-baseline": "3"}
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(nvcaUser, "", old, cur))
		assert.True(t, got.Allowed)
		assert.Equal(t, ReasonNVCAWriter, got.Reason)
	})
	t.Run("ReplicaSet controller may adopt, adding only its owner reference", func(t *testing.T) {
		old := base()
		old.OwnerReferences = nil
		cur := base()
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(rsUser, "", old, cur))
		assert.True(t, got.Allowed)
		assert.Equal(t, ReasonControllerAllowed, got.Reason)
	})
	t.Run("ReplicaSet controller may not change labels or spec", func(t *testing.T) {
		old := base()
		cur := base()
		cur.Labels["x"] = "y"
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(rsUser, "", old, cur))
		assert.False(t, got.Allowed)
		assert.Equal(t, ReasonDenyControllerEdit, got.Reason)

		cur = base()
		cur.Spec.NodeName = "other"
		got = engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(rsUser, "", old, cur))
		assert.False(t, got.Allowed)

		cur = base()
		cur.OwnerReferences = append(cur.OwnerReferences, ctrlRef("MiniService", "ms1", "ms-uid"))
		got = engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(rsUser, "", old, cur))
		assert.False(t, got.Allowed, "a copied owner reference must not authorize other owner changes")
	})
	t.Run("legacy utils pod, unchanged annotation, kubelet-like update is not blocked", func(t *testing.T) {
		old := legacy("true")
		cur := old.DeepCopy()
		cur.Labels = map[string]string{"foo": "bar"}
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(chartSA, "", old, cur))
		assert.True(t, got.Allowed)
		assert.Equal(t, ReasonLegacyUnchanged, got.Reason)
	})
	t.Run("legacy utils pod, NVCA patch is allowed", func(t *testing.T) {
		old := legacy("true")
		cur := old.DeepCopy()
		cur.Annotations["nvca.nvcf.nvidia.io/worker-restart-count-baseline"] = "3"
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(nvcaUser, "", old, cur))
		assert.True(t, got.Allowed)
	})
	t.Run("chart adds the infra annotation to its own pod", func(t *testing.T) {
		old := legacy("")
		cur := legacy("true")
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(chartSA, "", old, cur))
		assert.False(t, got.Allowed)
		assert.Equal(t, ReasonDenyWriter, got.Reason)
	})
	t.Run("chart changes or removes the infra annotation", func(t *testing.T) {
		old := legacy("true")
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(chartSA, "", old, legacy("")))
		assert.False(t, got.Allowed)
		got = engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(chartSA, "", old, legacy("false")))
		assert.False(t, got.Allowed)
	})
	t.Run("ordinary chart pod update passes", func(t *testing.T) {
		old := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: ns}}
		cur := old.DeepCopy()
		cur.Labels = map[string]string{"a": "b"}
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), upd(chartSA, "", old, cur))
		assert.True(t, got.Allowed)
		assert.Equal(t, ReasonUnprotected, got.Reason)
	})
}

func TestPodCreateInfraAnnotationOnly(t *testing.T) {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: ns,
		Annotations: map[string]string{nvcatypes.InfraObjectAnnotationKey: "true"}}}
	got := engineFor(&fakeLookup{}).Evaluate(context.Background(), podCreate(chartSA, p))
	assert.False(t, got.Allowed, "a chart pod must not be able to claim the infra annotation")
	got = engineFor(&fakeLookup{}).Evaluate(context.Background(), podCreate(nvcaUser, p))
	assert.True(t, got.Allowed, "NVCA's legacy utils pod carries only the annotation")
}

func TestDeployment(t *testing.T) {
	create := func(user string, d *appsv1.Deployment) Request {
		return Request{Operation: OperationCreate, Kind: KindDeployment, Username: user, Namespace: ns, Name: d.Name, Object: d}
	}
	t.Run("NVCA creates a well formed utils Deployment", func(t *testing.T) {
		l, d, _ := chain()
		got := engineFor(l).Evaluate(context.Background(), create(nvcaUser, d))
		assert.True(t, got.Allowed, got.Message)
	})
	t.Run("NVCA Deployment missing the label in its selector is refused", func(t *testing.T) {
		l, d, _ := chain()
		d.Spec.Selector.MatchLabels = map[string]string{"app": "utils"}
		got := engineFor(l).Evaluate(context.Background(), create(nvcaUser, d))
		assert.False(t, got.Allowed)
		assert.Equal(t, ReasonDenyTemplate, got.Reason)
	})
	t.Run("F3 chart creates a Deployment with the label in its template", func(t *testing.T) {
		l, d, _ := chain()
		got := engineFor(l).Evaluate(context.Background(), create(chartSA, d))
		assert.False(t, got.Allowed)
		assert.Equal(t, ReasonDenyWriter, got.Reason)
	})
	t.Run("F3 chart creates a Deployment with a MiniService owner reference", func(t *testing.T) {
		d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "d", Namespace: ns,
			OwnerReferences: []metav1.OwnerReference{ctrlRef("MiniService", "ms1", "ms-uid")}}}
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), create(chartSA, d))
		assert.False(t, got.Allowed)
	})
	t.Run("ordinary chart Deployment is untouched", func(t *testing.T) {
		d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: ns}}
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), create(chartSA, d))
		assert.True(t, got.Allowed)
		assert.Equal(t, ReasonUnprotected, got.Reason)
	})
	t.Run("F4 chart edits the template, replicas, selector or labels", func(t *testing.T) {
		l, d, _ := chain()
		cur := d.DeepCopy()
		cur.Spec.Template.Spec.Containers = []corev1.Container{{Name: "x", Image: "evil"}}
		got := engineFor(l).Evaluate(context.Background(), Request{Operation: OperationUpdate, Kind: KindDeployment,
			Username: chartSA, Namespace: ns, Name: d.Name, Object: cur, OldObject: d})
		assert.False(t, got.Allowed)
		assert.Equal(t, ReasonDenyProtectedEdit, got.Reason)

		cur = d.DeepCopy()
		cur.Spec.Replicas = ptr.To(int32(9))
		got = engineFor(l).Evaluate(context.Background(), Request{Operation: OperationUpdate, Kind: KindDeployment,
			Username: chartSA, Namespace: ns, Name: d.Name, Object: cur, OldObject: d})
		assert.False(t, got.Allowed)
	})
	t.Run("NVCA edits its Deployment", func(t *testing.T) {
		l, d, _ := chain()
		cur := d.DeepCopy()
		cur.Spec.Replicas = ptr.To(int32(2))
		got := engineFor(l).Evaluate(context.Background(), Request{Operation: OperationUpdate, Kind: KindDeployment,
			Username: nvcaUser, Namespace: ns, Name: d.Name, Object: cur, OldObject: d})
		assert.True(t, got.Allowed)
	})
}

func TestReplicaSet(t *testing.T) {
	create := func(user string, rs *appsv1.ReplicaSet) Request {
		return Request{Operation: OperationCreate, Kind: KindReplicaSet, Username: user, Namespace: ns, Name: rs.Name, Object: rs}
	}
	update := func(user string, old, cur *appsv1.ReplicaSet) Request {
		return Request{Operation: OperationUpdate, Kind: KindReplicaSet, Username: user, Namespace: ns, Name: cur.Name,
			Object: cur, OldObject: old}
	}
	t.Run("Deployment controller creates the ReplicaSet of a verified Deployment", func(t *testing.T) {
		l, _, rs := chain()
		got := engineFor(l).Evaluate(context.Background(), create(depUser, rs))
		assert.True(t, got.Allowed, got.Message)
		assert.Equal(t, ReasonChainVerified, got.Reason)
	})
	t.Run("Deployment controller ReplicaSet with a template that differs from the Deployment", func(t *testing.T) {
		l, _, rs := chain()
		rs.Spec.Template.Labels["tampered"] = "true"
		got := engineFor(l).Evaluate(context.Background(), create(depUser, rs))
		assert.False(t, got.Allowed)
		assert.True(t, got.Retryable, "a mismatch may be a race with a Deployment update, so it is retryable")
	})
	t.Run("a differing pod spec, annotations or hash-only difference are compared in full", func(t *testing.T) {
		for name, mut := range map[string]func(r *appsv1.ReplicaSet){
			"image": func(r *appsv1.ReplicaSet) {
				r.Spec.Template.Spec.Containers = []corev1.Container{{Name: "c", Image: "evil"}}
			},
			"annotations": func(r *appsv1.ReplicaSet) { r.Spec.Template.Annotations = map[string]string{"x": "y"} },
		} {
			l, _, rs := chain()
			mut(rs)
			got := engineFor(l).Evaluate(context.Background(), create(depUser, rs))
			assert.False(t, got.Allowed, name)
			assert.True(t, got.Retryable, name)
		}
		l, _, rs := chain()
		rs.Spec.Template.Labels[PodTemplateHashLabel] = "another-hash"
		got := engineFor(l).Evaluate(context.Background(), create(depUser, rs))
		assert.True(t, got.Allowed, "only the controller-added hash label may differ")
	})
	t.Run("a Deployment template that moved on is retryable, the controller recreates from the new one", func(t *testing.T) {
		l, d, rs := chain()
		d.Spec.Template.Spec.Containers = []corev1.Container{{Name: "c", Image: "v2"}}
		got := engineFor(l).Evaluate(context.Background(), create(depUser, rs))
		assert.False(t, got.Allowed)
		assert.True(t, got.Retryable)
	})
	t.Run("a chart identity creates a protected ReplicaSet", func(t *testing.T) {
		l, _, rs := chain()
		got := engineFor(l).Evaluate(context.Background(), create(chartSA, rs))
		assert.False(t, got.Allowed)
		assert.Equal(t, ReasonDenyWriter, got.Reason)
	})
	t.Run("Deployment controller scales the ReplicaSet and bumps its annotations", func(t *testing.T) {
		_, _, rs := chain()
		cur := rs.DeepCopy()
		cur.Spec.Replicas = ptr.To(int32(2))
		cur.Annotations = map[string]string{DeploymentAnnotationPrefix + "desired-replicas": "2"}
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), update(depUser, rs, cur))
		assert.True(t, got.Allowed, got.Message)
		assert.Equal(t, ReasonControllerAllowed, got.Reason)
	})
	t.Run("Deployment controller changing the template, selector, labels or owners is denied", func(t *testing.T) {
		_, _, rs := chain()
		for name, mut := range map[string]func(r *appsv1.ReplicaSet){
			"template": func(r *appsv1.ReplicaSet) { r.Spec.Template.Spec.NodeName = "n" },
			"selector": func(r *appsv1.ReplicaSet) {
				r.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}}
			},
			"labels": func(r *appsv1.ReplicaSet) { r.Labels = map[string]string{"a": "b"} },
			"owners": func(r *appsv1.ReplicaSet) { r.OwnerReferences = nil },
			"foreign annotation": func(r *appsv1.ReplicaSet) {
				r.Annotations = map[string]string{"other": "x"}
			},
		} {
			cur := rs.DeepCopy()
			mut(cur)
			got := engineFor(&fakeLookup{}).Evaluate(context.Background(), update(depUser, rs, cur))
			assert.False(t, got.Allowed, name)
			assert.Equal(t, ReasonDenyControllerEdit, got.Reason, name)
		}
	})
	t.Run("a chart identity edits a protected ReplicaSet", func(t *testing.T) {
		_, _, rs := chain()
		cur := rs.DeepCopy()
		cur.Spec.Replicas = ptr.To(int32(0))
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), update(chartSA, rs, cur))
		assert.False(t, got.Allowed)
	})
	t.Run("ordinary chart ReplicaSet is untouched", func(t *testing.T) {
		rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: ns}}
		got := engineFor(&fakeLookup{}).Evaluate(context.Background(), create(chartSA, rs))
		assert.True(t, got.Allowed)
	})
}

func TestScale(t *testing.T) {
	scale := func(user, name string) Request {
		return Request{Operation: OperationUpdate, Kind: KindScale, Username: user, Namespace: ns, Name: name}
	}
	l, _, _ := chain()
	t.Run("F11 chart scales the utils Deployment", func(t *testing.T) {
		got := engineFor(l).Evaluate(context.Background(), scale(chartSA, "utils"))
		assert.False(t, got.Allowed)
		assert.Equal(t, ReasonDenyProtectedEdit, got.Reason)
	})
	t.Run("NVCA scales it", func(t *testing.T) {
		assert.True(t, engineFor(l).Evaluate(context.Background(), scale(nvcaUser, "utils")).Allowed)
	})
	t.Run("chart scales its own Deployment", func(t *testing.T) {
		l2, _, _ := chain()
		l2.deps[ns+"/web"] = &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: ns}}
		got := engineFor(l2).Evaluate(context.Background(), scale(chartSA, "web"))
		assert.True(t, got.Allowed)
		assert.Equal(t, ReasonUnprotected, got.Reason)
	})
	t.Run("scale of an unknown target is not blocked", func(t *testing.T) {
		assert.True(t, engineFor(l).Evaluate(context.Background(), scale(chartSA, "ghost")).Allowed)
	})
}

func TestEmptyIdentityNeverMatches(t *testing.T) {
	l, _, _ := chain()
	e := &Engine{Lookup: l} // no identities configured
	got := e.Evaluate(context.Background(), podCreate("", utilsPod(roleLabels)))
	require.False(t, got.Allowed, "an empty username must never be treated as NVCA")
	got = e.Evaluate(context.Background(), podCreate("", utilsPod(withHash(roleLabels, "h1"),
		ctrlRef("ReplicaSet", "utils-h1", "rs-uid"))))
	require.False(t, got.Allowed, "an empty username must never be treated as a controller")
}

func TestAllReasonsUnique(t *testing.T) {
	seen := map[Reason]bool{}
	for _, r := range AllReasons {
		require.False(t, seen[r], "duplicate reason %q", r)
		seen[r] = true
	}
	assert.Len(t, seen, len(AllReasons))
}

func TestDenialFormatRoundTrip(t *testing.T) {
	d := deny(ReasonDenyWriter, "only NVCA may create")
	msg := FormatDenial(d)
	got, ok := ParseDenial(`admission webhook "` + WebhookName + `" denied the request: ` + msg)
	require.True(t, ok)
	assert.Equal(t, ReasonDenyWriter, got)

	for _, bad := range []string{"", "forbidden: user cannot create pods", "utils-provenance[", "utils-provenance[]: x"} {
		_, ok := ParseDenial(bad)
		assert.False(t, ok, "%q", bad)
	}
}

func TestClassifyUtilsPod(t *testing.T) {
	goodRef := ctrlRef("ReplicaSet", "utils-h1", "rs-uid")
	labeled := func(refs ...metav1.OwnerReference) *corev1.Pod {
		return utilsPod(withHash(roleLabels, "h1"), refs...)
	}

	t.Run("an unlabeled pod does not claim to be utils", func(t *testing.T) {
		l, _, _ := chain()
		isUtils, d := engineFor(l).ClassifyUtilsPod(context.Background(), chartSA, ns, utilsPod(map[string]string{"app": "x"}), true)
		assert.False(t, isUtils)
		assert.Equal(t, ReasonUnprotected, d.Reason)
		assert.Equal(t, 0, l.rsCalls, "no lookup for an unlabeled pod")
	})
	t.Run("create by the ReplicaSet controller with a verified chain", func(t *testing.T) {
		l, _, _ := chain()
		isUtils, d := engineFor(l).ClassifyUtilsPod(context.Background(), rsUser, ns, labeled(goodRef), true)
		assert.True(t, isUtils, d.Message)
		assert.Equal(t, ReasonChainVerified, d.Reason)
	})
	t.Run("create by a chart identity naming the real ReplicaSet is not utils", func(t *testing.T) {
		l, _, _ := chain()
		isUtils, d := engineFor(l).ClassifyUtilsPod(context.Background(), chartSA, ns, labeled(goodRef), true)
		assert.False(t, isUtils, "a copied owner reference must not classify a pod as utils")
		assert.Equal(t, ReasonDenyWriter, d.Reason)
		assert.False(t, d.Retryable)
	})
	t.Run("a forged label with no chain is not utils", func(t *testing.T) {
		l, _, _ := chain()
		isUtils, d := engineFor(l).ClassifyUtilsPod(context.Background(), rsUser, ns, labeled(), true)
		assert.False(t, isUtils)
		assert.Equal(t, ReasonDenyNoOwner, d.Reason)
	})
	t.Run("a stolen UID is not utils", func(t *testing.T) {
		l, _, _ := chain()
		isUtils, d := engineFor(l).ClassifyUtilsPod(context.Background(), rsUser, ns,
			labeled(ctrlRef("ReplicaSet", "utils-h1", "other")), true)
		assert.False(t, isUtils)
		assert.Equal(t, ReasonDenyUIDMismatch, d.Reason)
	})
	t.Run("a ReplicaSet not visible yet is retryable, never ordinary", func(t *testing.T) {
		l, _, _ := chain()
		l.rs = nil
		isUtils, d := engineFor(l).ClassifyUtilsPod(context.Background(), rsUser, ns, labeled(goodRef), true)
		assert.False(t, isUtils)
		assert.True(t, d.Retryable, "the caller must retry, not fall back to treating it as a chart pod")
	})
	t.Run("on update the chain alone decides, whoever updates", func(t *testing.T) {
		l, _, _ := chain()
		for _, user := range []string{nvcaUser, rsUser, chartSA} {
			isUtils, d := engineFor(l).ClassifyUtilsPod(context.Background(), user, ns, labeled(goodRef), false)
			assert.True(t, isUtils, "%s: %s", user, d.Message)
		}
		isUtils, _ := engineFor(l).ClassifyUtilsPod(context.Background(), nvcaUser, ns, labeled(), false)
		assert.False(t, isUtils, "an update of a labeled pod with no chain is not utils")
	})
	t.Run("a bare pod labeled by NVCA in a live MiniService namespace", func(t *testing.T) {
		l, _, _ := chain()
		isUtils, _ := engineFor(l).ClassifyUtilsPod(context.Background(), nvcaUser, ns, utilsPod(roleLabels), true)
		assert.True(t, isUtils)
	})
}
