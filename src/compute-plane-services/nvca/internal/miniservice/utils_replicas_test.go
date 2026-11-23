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

package mscontroller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance/readiness"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance/rollout"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

type stubVerifier struct{ s readiness.Status }

func (v stubVerifier) Status() readiness.Status { return v.s }

func gateFor(s readiness.Status) readiness.Gate { return readiness.Gate{Verifier: stubVerifier{s}} }

var (
	nowT      = time.Unix(100, 0)
	verifiedS = readiness.Status{EnforcementVerified: true, RBACVerified: true, LastCanaryAt: nowT, LastRBACAt: nowT}
	failedS   = readiness.Status{RBACVerified: true, LastCanaryAt: nowT, LastRBACAt: nowT, Reason: "the canary was accepted"}
	staleS    = readiness.Status{Stale: true, LastCanaryAt: nowT, LastRBACAt: nowT, Reason: "the last canary result is too old"}
	neverRanS = readiness.Status{}
)

func msWithHA(replicas string) *v1alpha1.MiniService {
	ms := &v1alpha1.MiniService{ObjectMeta: metav1.ObjectMeta{Name: "ms", Generation: 3}}
	if replicas != "" {
		ms.Status.Conditions = []metav1.Condition{{
			Type: ConditionUtilsHA, Status: metav1.ConditionTrue, Reason: "UtilsDeploymentCreated", Message: replicas,
		}}
	}
	return ms
}

func TestPlanUtilsReplicas(t *testing.T) {
	for name, tt := range map[string]struct {
		requested, existing int32
		gate                readiness.Checkable
		want                UtilsReplicaPlan
	}{
		"default of one needs no readiness": {0, 0, nil, UtilsReplicaPlan{Replicas: 1}},
		"one replica needs no readiness":    {1, 0, nil, UtilsReplicaPlan{Replicas: 1}},
		"two replicas, gate open":           {2, 0, gateFor(verifiedS), UtilsReplicaPlan{Replicas: 2}},
		"three replicas, gate open":         {3, 0, gateFor(verifiedS), UtilsReplicaPlan{Replicas: 3}},
		"HA exists, gate open, scale down":  {2, 3, gateFor(verifiedS), UtilsReplicaPlan{Replicas: 2}},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.want, planUtilsReplicas(tt.requested, tt.existing, tt.gate))
		})
	}

	t.Run("no HA yet: a closed gate gives one pod and says why, never degraded", func(t *testing.T) {
		for name, g := range map[string]readiness.Checkable{
			"nil": nil, "never ran": gateFor(neverRanS), "failed": gateFor(failedS), "stale": gateFor(staleS),
		} {
			p := planUtilsReplicas(2, 0, g)
			assert.EqualValues(t, 1, p.Replicas, name)
			assert.True(t, p.Paused, name)
			assert.False(t, p.Degraded, name)
			assert.NotEmpty(t, p.Reason, name)
		}
		p := planUtilsReplicas(1, 0, nil)
		assert.False(t, p.Paused, "asking for one pod is not a refusal")
	})

	t.Run("HA exists: a closed gate keeps the replicas, pauses changes, reports degraded", func(t *testing.T) {
		for name, tt := range map[string]struct {
			gate readiness.Checkable
			err  error
		}{
			"missing":   {nil, readiness.ErrMissing},
			"never ran": {gateFor(neverRanS), readiness.ErrMissing},
			"failed":    {gateFor(failedS), readiness.ErrNotVerified},
			"stale":     {gateFor(staleS), readiness.ErrStale},
			"inventory": {rollout.Gate{Readiness: gateFor(verifiedS), Tracker: &rollout.Tracker{Store: &rollout.MemoryStore{}}}, rollout.ErrInventoryPending},
		} {
			for _, requested := range []int32{1, 2, 3, 5} {
				p := planUtilsReplicas(requested, 3, tt.gate)
				assert.EqualValues(t, 3, p.Replicas, "%s requested %d: never shrink and never grow", name, requested)
				assert.True(t, p.Paused, name)
				assert.True(t, p.Degraded, name)
				assert.Contains(t, p.Reason, tt.err.Error(), name)
				assert.False(t, p.CanScaleUp(), name)
				assert.False(t, p.CanChangeTemplate(), name)
				assert.True(t, p.CanCleanup(), "%s: cleanup is never paused", name)
			}
		}
	})
}

func TestLabelDiscoveryAllowedFollowsTheGateAndTheHARecord(t *testing.T) {
	for name, tt := range map[string]struct {
		gate readiness.Checkable
		ms   *v1alpha1.MiniService
		want bool
	}{
		"gate open, no HA":               {gateFor(verifiedS), msWithHA(""), true},
		"gate closed, no HA, nil":        {nil, msWithHA(""), false},
		"gate closed, no HA, failed":     {gateFor(failedS), msWithHA(""), false},
		"gate closed, no HA, stale":      {gateFor(staleS), msWithHA(""), false},
		"gate closed, recorded HA":       {gateFor(staleS), msWithHA("3"), true},
		"gate nil, recorded HA":          {nil, msWithHA("2"), true},
		"gate closed, record is one":     {gateFor(staleS), msWithHA("1"), false},
		"gate closed, record is garbage": {gateFor(staleS), msWithHA("many"), false},
	} {
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{ControllerOptions: ControllerOptions{UtilsProvenanceGate: tt.gate}}
			assert.Equal(t, tt.want, r.labelDiscoveryAllowed(tt.ms))
		})
	}
	assert.False(t, (&Reconciler{}).labelDiscoveryAllowed(msWithHA("")), "the zero value must refuse")
}

func TestUtilsPodRecognitionKeepsHAPodsAfterReadinessIsLost(t *testing.T) {
	labeled := func(ns string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "utils-7d9f8-x2k4q", Namespace: ns,
			Labels: map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}}}
	}
	closed := &Reconciler{ControllerOptions: ControllerOptions{UtilsProvenanceGate: gateFor(staleS)}}

	assert.True(t, closed.utilsPodRecognized(msWithHA("3"), labeled("sr-abc")),
		"an HA instance's utils pods must not become chart workloads when readiness is lost")
	assert.False(t, closed.utilsPodRecognized(msWithHA(""), labeled("sr-abc")),
		"a labeled pod in an instance with no HA record is not recognized while the gate is closed")
	assert.False(t, closed.utilsPodRecognized(msWithHA("3"), labeled("default")),
		"outside a MiniService namespace the label never counts")
}

func TestDegradedConditionLifecycle(t *testing.T) {
	ms := msWithHA("3")
	cond := func() *metav1.Condition {
		return meta.FindStatusCondition(ms.Status.Conditions, ConditionUtilsProvenanceDegraded)
	}

	assert.False(t, SetUtilsProvenanceDegraded(ms, planUtilsReplicas(3, 3, gateFor(verifiedS))), "healthy and never degraded: nothing is added")
	assert.Nil(t, cond())

	assert.True(t, SetUtilsProvenanceDegraded(ms, planUtilsReplicas(3, 3, gateFor(staleS))))
	require.NotNil(t, cond())
	assert.Equal(t, metav1.ConditionTrue, cond().Status)
	assert.Contains(t, cond().Message, "stale")
	assert.False(t, SetUtilsProvenanceDegraded(ms, planUtilsReplicas(3, 3, gateFor(staleS))), "an unchanged condition reports no change")

	assert.True(t, SetUtilsProvenanceDegraded(ms, planUtilsReplicas(3, 3, gateFor(verifiedS))), "recovery clears it")
	assert.Equal(t, metav1.ConditionFalse, cond().Status)
}

func TestEffectiveUtilsReplicasLogs(t *testing.T) {
	var logged []string
	log := funcr.New(func(prefix, args string) { logged = append(logged, args) }, funcr.Options{})

	r := &Reconciler{ControllerOptions: ControllerOptions{UtilsReplicas: 2}}
	assert.EqualValues(t, 1, r.effectiveUtilsReplicas(log, msWithHA("")).Replicas)
	require.NotEmpty(t, logged)
	assert.Contains(t, logged[0], "not allowed")

	logged = nil
	assert.EqualValues(t, 3, r.effectiveUtilsReplicas(log, msWithHA("3")).Replicas)
	require.NotEmpty(t, logged)
	assert.Contains(t, logged[0], "keeping its replicas")

	logged = nil
	r = &Reconciler{ControllerOptions: ControllerOptions{UtilsReplicas: 2, UtilsProvenanceGate: gateFor(verifiedS)}}
	assert.EqualValues(t, 2, r.effectiveUtilsReplicas(log, msWithHA("")).Replicas)
	assert.Empty(t, logged)

	r = &Reconciler{}
	assert.EqualValues(t, 1, r.effectiveUtilsReplicas(logr.Discard(), msWithHA("")).Replicas)
}

// refusingCanary is refused by the real provenance webhook name and reason, so a real Checker can verify.
type refusingCanary struct{}

func (refusingCanary) DryRunForgedPod(context.Context) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status: metav1.StatusFailure, Code: 403, Reason: metav1.StatusReasonForbidden,
		Message: `admission webhook "` + utilsprovenance.WebhookName + `" denied the request: ` +
			utilsprovenance.FormatDenial(utilsprovenance.Decision{Reason: utilsprovenance.ReasonDenyWriter, Message: "only NVCA may create"}),
	}}
}

var _ = schema.GroupResource{}
var _ = errors.New

type noAccounts struct{}

func (noAccounts) InstanceAccounts(context.Context) ([]readiness.Account, error) { return nil, nil }

type allowNothing struct{}

func (allowNothing) Allowed(context.Context, string, []string, string, readiness.Right, string) (bool, error) {
	return false, nil
}

type emptyLister struct{}

func (emptyLister) ReservedObjects(context.Context) ([]rollout.Object, error) { return nil, nil }

// TestReadinessExpiresAfterAnHAInstanceExistsThenRecovers drives a real readiness Checker and rollout tracker through
// verified, expired, and recovered, with an HA instance recorded, and checks the whole policy at each step: replicas are
// never shrunk, changes pause, the degraded condition is set and cleared, pods stay recognized, cleanup is always allowed.
func TestReadinessExpiresAfterAnHAInstanceExistsThenRecovers(t *testing.T) {
	now := time.Unix(50_000, 0)
	clock := func() time.Time { return now }
	ctx := context.Background()

	tracker := &rollout.Tracker{Store: &rollout.MemoryStore{}, Now: clock, MaxGap: 3 * time.Minute}
	checker := &readiness.Checker{
		Canary: refusingCanary{}, Accounts: noAccounts{}, Reviewer: allowNothing{}, CanaryInterval: time.Minute, Now: clock,
		OnStatus: func(s readiness.Status) { _ = tracker.Observe(ctx, s.EnforcementVerified) },
	}
	gate := rollout.Gate{Readiness: readiness.Gate{Verifier: checker}, Tracker: tracker}
	step := func() { checker.CheckCanary(ctx); checker.CheckRBAC(ctx) }

	// Bring the rollout to open: enforcement verified (three refusals), RBAC clean, one clean inventory scan.
	for i := 0; i < 3; i++ {
		step()
		now = now.Add(10 * time.Second)
	}
	_, err := tracker.Scan(ctx, emptyLister{})
	require.NoError(t, err)
	require.NoError(t, gate.Check(), "the rollout is open")

	ms := msWithHA("3") // an HA instance exists
	r := &Reconciler{ControllerOptions: ControllerOptions{UtilsReplicas: 3, UtilsProvenanceGate: gate}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "utils-7d9f8-x2k4q", Namespace: "sr-abc",
		Labels: map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}}}

	healthy := r.effectiveUtilsReplicas(logr.Discard(), ms)
	assert.EqualValues(t, 3, healthy.Replicas)
	assert.False(t, healthy.Paused)
	assert.False(t, SetUtilsProvenanceDegraded(ms, healthy))

	// The checker stops (a hung API server): the result expires.
	now = now.Add(10 * time.Minute)
	expired := r.effectiveUtilsReplicas(logr.Discard(), ms)
	assert.EqualValues(t, 3, expired.Replicas, "existing replicas are not shrunk when readiness expires")
	assert.True(t, expired.Paused)
	assert.True(t, expired.Degraded)
	assert.False(t, expired.CanScaleUp())
	assert.False(t, expired.CanChangeTemplate())
	assert.True(t, expired.CanCleanup(), "instance cleanup is never blocked")
	assert.True(t, r.utilsPodRecognized(ms, pod), "existing utils pods are still utils pods, not chart workloads")
	assert.True(t, SetUtilsProvenanceDegraded(ms, expired))
	assert.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(ms.Status.Conditions, ConditionUtilsProvenanceDegraded).Status)

	// A new instance with a forged label is not recognized while the gate is closed.
	assert.False(t, r.utilsPodRecognized(msWithHA(""), pod))

	// The checker resumes. Enforcement verifies again after three refusals, but the gap closed the inventory.
	for i := 0; i < 3; i++ {
		step()
		now = now.Add(10 * time.Second)
	}
	assert.True(t, checker.Status().Verified(), "readiness is back")
	recovering := r.effectiveUtilsReplicas(logr.Discard(), ms)
	assert.True(t, recovering.Degraded, "still degraded: readiness returned but no clean inventory covers the gap yet")
	assert.ErrorIs(t, gate.Check(), rollout.ErrInventoryPending)

	// A clean scan of the gap reopens the rollout and clears the condition.
	_, err = tracker.Scan(ctx, emptyLister{})
	require.NoError(t, err)
	recovered := r.effectiveUtilsReplicas(logr.Discard(), ms)
	assert.False(t, recovered.Paused)
	assert.False(t, recovered.Degraded)
	assert.EqualValues(t, 3, recovered.Replicas)
	assert.True(t, SetUtilsProvenanceDegraded(ms, recovered), "the degraded condition clears on recovery")
	assert.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(ms.Status.Conditions, ConditionUtilsProvenanceDegraded).Status)
}

func TestRequeueWhileUtilsDegraded(t *testing.T) {
	assert.Equal(t, reconcile.Result{}, requeueWhileUtilsDegraded(UtilsReplicaPlan{}, reconcile.Result{}),
		"an instance that is not degraded is left alone")
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Minute},
		requeueWhileUtilsDegraded(UtilsReplicaPlan{Paused: true}, reconcile.Result{RequeueAfter: time.Minute}),
		"paused without degradation (a new instance) does not poll")
	assert.Equal(t, reconcile.Result{RequeueAfter: utilsDegradedRecheck},
		requeueWhileUtilsDegraded(UtilsReplicaPlan{Degraded: true}, reconcile.Result{}))
	assert.Equal(t, reconcile.Result{RequeueAfter: utilsDegradedRecheck},
		requeueWhileUtilsDegraded(UtilsReplicaPlan{Degraded: true}, reconcile.Result{RequeueAfter: time.Hour}))
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second},
		requeueWhileUtilsDegraded(UtilsReplicaPlan{Degraded: true}, reconcile.Result{RequeueAfter: time.Second}),
		"an earlier requeue is kept")
	assert.Equal(t, reconcile.Result{RequeueAfter: utilsDegradedRecheck},
		requeueWhileUtilsDegraded(UtilsReplicaPlan{Degraded: true}, reconcile.Result{Requeue: true}),
		"a bare requeue becomes the timed one")
}

func TestUtilsHARecordCarriesTheTemplateDigestsAndKeepsThem(t *testing.T) {
	spec := corev1.PodSpec{Containers: []corev1.Container{{Name: "utils", Image: "utils:1"}}}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{UID: "dep-uid"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: spec}}}
	fresh := newUtilsHARecord(2, dep)
	require.NotEmpty(t, fresh.PodSpecDigest)
	require.NotEmpty(t, fresh.ContainersDigest)

	// A live object whose image was changed has other digests; they must never replace the recorded ones.
	tampered := dep.DeepCopy()
	tampered.Spec.Template.Spec.Containers[0].Image = "evil:1"
	live := newUtilsHARecord(2, tampered)
	require.NotEqual(t, fresh.PodSpecDigest, live.PodSpecDigest)
	scaled := dep.DeepCopy()
	n := int32(9)
	scaled.Spec.Replicas = &n
	require.NotEqual(t, fresh.DeploymentSpecDigest, newUtilsHARecord(2, scaled).DeploymentSpecDigest, "the replica count is part of the desired state")

	kept := mergeTrustedDigests(fresh, true, live, false, true)
	assert.Equal(t, fresh.PodSpecDigest, kept.PodSpecDigest, "the recorded digest wins, even with the gate open")
	assert.Equal(t, fresh.ContainersDigest, kept.ContainersDigest)

	t.Run("a Deployment NVCA just created records its digests", func(t *testing.T) {
		assert.Equal(t, fresh.PodSpecDigest, mergeTrustedDigests(utilsHARecord{}, false, fresh, true, false).PodSpecDigest)
	})
	t.Run("an existing Deployment with none recorded gets digests only while the gate is open", func(t *testing.T) {
		assert.NotEmpty(t, mergeTrustedDigests(utilsHARecord{Replicas: 2}, true, live, false, true).PodSpecDigest)
		closed := mergeTrustedDigests(utilsHARecord{Replicas: 2}, true, live, false, false)
		assert.Empty(t, closed.PodSpecDigest)
		assert.Empty(t, closed.ContainersDigest)
	})
	t.Run("the digests survive the status condition round trip and UtilsHATrust reads them", func(t *testing.T) {
		ms := statusMS(false, "")
		recordUtilsHA(ms, fresh)
		trust, ok := UtilsHATrust(ms)
		require.True(t, ok)
		assert.Equal(t, UtilsTrust{PodSpecDigest: fresh.PodSpecDigest, ContainersDigest: fresh.ContainersDigest,
			DeploymentSpecDigest: fresh.DeploymentSpecDigest, Replicas: 2, DeploymentUID: "dep-uid"}, trust)
		recordUtilsHA(ms, utilsHARecord{Replicas: 2})
		_, ok = UtilsHATrust(ms)
		assert.False(t, ok, "a record without digests gives no trust")
		partial := fresh
		partial.DeploymentSpecDigest = ""
		recordUtilsHA(ms, partial)
		_, ok = UtilsHATrust(ms)
		assert.False(t, ok, "a record that lacks one digest gives no trust")
		noUID := fresh
		noUID.DeploymentUID = ""
		recordUtilsHA(ms, noUID)
		_, ok = UtilsHATrust(ms)
		assert.False(t, ok, "a record without the Deployment UID gives no trust")
		_, ok = UtilsHATrust(statusMS(false, "2"))
		assert.False(t, ok, "a bare number from an earlier build gives no trust")
	})
}
