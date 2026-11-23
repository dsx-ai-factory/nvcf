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

package rollout

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance/readiness"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTracker(t *testing.T) (*Tracker, *clock, *MemoryStore) {
	t.Helper()
	c := &clock{t: time.Unix(10_000, 0)}
	store := &MemoryStore{}
	return &Tracker{Store: store, Now: c.now}, c, store
}

type fixedLister struct {
	objs []Object
	err  error
}

func (f fixedLister) ReservedObjects(context.Context) ([]Object, error) { return f.objs, f.err }

type fakeReadiness struct{ err error }

func (f fakeReadiness) Check() error { return f.err }

func TestInventoryNeverClearsBeforeEnforcementIsVerified(t *testing.T) {
	tr, _, _ := newTracker(t)
	ctx := context.Background()
	require.NoError(t, tr.Observe(ctx, false))
	suspects, err := tr.Scan(ctx, fixedLister{})
	require.NoError(t, err)
	assert.Empty(t, suspects)
	ok, why := tr.State(ctx).Cleared()
	assert.False(t, ok, "an empty scan before enforcement was ever verified must not clear")
	assert.Contains(t, why, "never been verified")
}

func TestPreEnforcementObjectsAreSuspectButLegacyInfraIsNot(t *testing.T) {
	tr, c, _ := newTracker(t)
	ctx := context.Background()
	before := c.now().Add(-time.Hour)
	require.NoError(t, tr.Observe(ctx, true)) // first verified now

	legacyUtils := Object{Kind: "Pod", Namespace: "sr-a", Name: "utils", Created: before, OnlyInfraAnnotation: true}
	smb := Object{Kind: "Pod", Namespace: "sr-a", Name: "nvcf-smb-server", Created: before, OnlyInfraAnnotation: true}
	forgedLabel := Object{Kind: "Pod", Namespace: "sr-a", Name: "fake-utils", Created: before}
	forgedAnnotation := Object{Kind: "Pod", Namespace: "sr-a", Name: "other", Created: before, OnlyInfraAnnotation: true}
	forgedDeployment := Object{Kind: "Deployment", Namespace: "sr-a", Name: "utils", Created: before}

	suspects, err := tr.Scan(ctx, fixedLister{objs: []Object{legacyUtils, smb, forgedLabel, forgedAnnotation, forgedDeployment}})
	require.NoError(t, err)
	names := []string{}
	for _, s := range suspects {
		names = append(names, s.Kind+"/"+s.Name)
	}
	assert.ElementsMatch(t, []string{"Pod/fake-utils", "Pod/other", "Deployment/utils"}, names)
	ok, _ := tr.State(ctx).Cleared()
	assert.False(t, ok, "suspects leave the inventory uncleared")
}

func TestCleanScanClearsAndObjectsCreatedWhileVerifiedAreTrusted(t *testing.T) {
	tr, c, _ := newTracker(t)
	ctx := context.Background()
	require.NoError(t, tr.Observe(ctx, true))
	c.advance(time.Minute)
	created := Object{Kind: "Deployment", Namespace: "sr-a", Name: "utils", Created: c.now()} // while verified
	legacy := Object{Kind: "Pod", Namespace: "sr-a", Name: "utils", Created: c.now().Add(-time.Hour), OnlyInfraAnnotation: true}

	suspects, err := tr.Scan(ctx, fixedLister{objs: []Object{created, legacy}})
	require.NoError(t, err)
	assert.Empty(t, suspects)
	ok, why := tr.State(ctx).Cleared()
	assert.True(t, ok, why)
}

func TestLosingVerificationClosesTheGateAndRecoveryNeedsANewScan(t *testing.T) {
	tr, c, _ := newTracker(t)
	ctx := context.Background()
	require.NoError(t, tr.Observe(ctx, true))
	c.advance(time.Minute)
	_, err := tr.Scan(ctx, fixedLister{})
	require.NoError(t, err)
	ok, _ := tr.State(ctx).Cleared()
	require.True(t, ok)

	// Enforcement is lost. The gap starts at the last verified time.
	c.advance(time.Minute)
	require.NoError(t, tr.Observe(ctx, true)) // still verified
	lastVerified := c.now()
	c.advance(5 * time.Minute)
	require.NoError(t, tr.Observe(ctx, false))
	ok, why := tr.State(ctx).Cleared()
	assert.False(t, ok)
	assert.Contains(t, why, "not currently verified")
	st := tr.State(ctx)
	require.Len(t, st.Windows, 1)
	assert.Equal(t, lastVerified, st.Windows[0].Start, "the gap starts at the last observed verified time")

	// An object appears during the gap.
	duringGap := Object{Kind: "Pod", Namespace: "sr-a", Name: "forged-in-gap", Created: lastVerified.Add(2 * time.Minute)}

	// Recovery closes the window and still leaves the inventory uncleared until a clean scan.
	c.advance(5 * time.Minute)
	require.NoError(t, tr.Observe(ctx, true))
	ok, why = tr.State(ctx).Cleared()
	assert.False(t, ok)
	assert.Contains(t, why, "no clean inventory scan")

	suspects, err := tr.Scan(ctx, fixedLister{objs: []Object{duringGap}})
	require.NoError(t, err)
	require.Len(t, suspects, 1, "an object created inside the gap is suspect")
	ok, _ = tr.State(ctx).Cleared()
	assert.False(t, ok)

	// After it is removed a clean scan clears again.
	suspects, err = tr.Scan(ctx, fixedLister{})
	require.NoError(t, err)
	assert.Empty(t, suspects)
	ok, why = tr.State(ctx).Cleared()
	assert.True(t, ok, why)
}

func TestObjectsFromBeforeAGapStayTrustedOnlyWithVerifiedContent(t *testing.T) {
	tr, c, _ := newTracker(t)
	ctx := context.Background()
	require.NoError(t, tr.Observe(ctx, true))
	c.advance(time.Minute)
	created := c.now()
	c.advance(time.Minute)
	require.NoError(t, tr.Observe(ctx, true))
	c.advance(10 * time.Minute)
	require.NoError(t, tr.Observe(ctx, false)) // gap opens after the HA objects exist
	c.advance(10 * time.Minute)
	require.NoError(t, tr.Observe(ctx, true))

	// Created while verified, content equal to what NVCA recorded: trusted although a later gap occurred.
	good := Object{Kind: "Deployment", Namespace: "sr-a", Name: "utils", Created: created, Modified: created, ContentVerified: true}
	suspects, err := tr.Scan(ctx, fixedLister{objs: []Object{good}})
	require.NoError(t, err)
	assert.Empty(t, suspects, "an HA object whose content matches the recorded digests is trusted across a gap")
	ok, why := tr.State(ctx).Cleared()
	assert.True(t, ok, why)

	// The same object with content that does not match (or nothing recorded) is suspect: its labels and owner are
	// unchanged but its template could have been edited during the gap, and timestamps would not show it.
	tampered := good
	tampered.ContentVerified = false
	suspects, err = tr.Scan(ctx, fixedLister{objs: []Object{tampered}})
	require.NoError(t, err)
	assert.Len(t, suspects, 1)
	ok, _ = tr.State(ctx).Cleared()
	assert.False(t, ok)
}

func TestAScanThatRacesAStateChangeDoesNotClear(t *testing.T) {
	tr, c, _ := newTracker(t)
	ctx := context.Background()
	require.NoError(t, tr.Observe(ctx, true))
	c.advance(time.Minute)
	version := tr.Version()
	// A gap opens while the scan is listing.
	require.NoError(t, tr.Observe(ctx, false))
	cleared, err := tr.MarkCleared(ctx, version)
	require.NoError(t, err)
	assert.False(t, cleared, "a scan begun before the state changed must not clear")
	ok, _ := tr.State(ctx).Cleared()
	assert.False(t, ok)
}

func TestAFailedListingDoesNotClear(t *testing.T) {
	tr, c, _ := newTracker(t)
	ctx := context.Background()
	require.NoError(t, tr.Observe(ctx, true))
	c.advance(time.Minute)
	_, err := tr.Scan(ctx, fixedLister{err: errors.New("api down")})
	require.Error(t, err)
	ok, _ := tr.State(ctx).Cleared()
	assert.False(t, ok)
}

func TestWindowsAreBoundedByMerging(t *testing.T) {
	tr, c, _ := newTracker(t)
	ctx := context.Background()
	require.NoError(t, tr.Observe(ctx, true))
	for i := 0; i < maxWindows+10; i++ {
		c.advance(time.Minute)
		require.NoError(t, tr.Observe(ctx, true))
		c.advance(time.Minute)
		require.NoError(t, tr.Observe(ctx, false))
		c.advance(time.Minute)
		require.NoError(t, tr.Observe(ctx, true))
	}
	st := tr.State(ctx)
	assert.LessOrEqual(t, len(st.Windows), maxWindows)
	// The merged window spans at least what the originals covered, so nothing becomes less suspect.
	assert.True(t, st.Suspect(Object{Kind: "Pod", Name: "x", Created: st.Windows[0].Start.Add(time.Second)}))
}

func TestRolloutGateNeedsReadinessAndInventory(t *testing.T) {
	tr, c, _ := newTracker(t)
	ctx := context.Background()

	t.Run("readiness failure wins", func(t *testing.T) {
		g := Gate{Readiness: fakeReadiness{err: readiness.ErrNotVerified}, Tracker: tr}
		assert.ErrorIs(t, g.Check(), readiness.ErrNotVerified)
	})
	t.Run("missing readiness and nil tracker refuse", func(t *testing.T) {
		assert.ErrorIs(t, Gate{Tracker: tr}.Check(), readiness.ErrMissing)
		assert.ErrorIs(t, Gate{Readiness: fakeReadiness{}}.Check(), ErrInventoryPending)
	})
	t.Run("readiness passes but the inventory is not cleared", func(t *testing.T) {
		g := Gate{Readiness: fakeReadiness{}, Tracker: tr}
		err := g.Check()
		assert.ErrorIs(t, err, ErrInventoryPending, "label discovery and replicas stay closed until the inventory is cleared")
		assert.False(t, readiness.Open(g))
	})
	t.Run("opens after a clean scan, closes again on a gap", func(t *testing.T) {
		require.NoError(t, tr.Observe(ctx, true))
		c.advance(time.Minute)
		_, err := tr.Scan(ctx, fixedLister{})
		require.NoError(t, err)
		g := Gate{Readiness: fakeReadiness{}, Tracker: tr}
		assert.NoError(t, g.Check())
		assert.True(t, readiness.Open(g))

		c.advance(time.Minute)
		require.NoError(t, tr.Observe(ctx, false))
		assert.ErrorIs(t, g.Check(), ErrInventoryPending)
	})
}

func TestOpenRefusesNil(t *testing.T) {
	assert.False(t, readiness.Open(nil))
	assert.ErrorIs(t, readiness.CheckOrMissing(nil), readiness.ErrMissing)
}

func TestConfigMapStoreSurvivesARestart(t *testing.T) {
	k := fake.NewSimpleClientset()
	store := ConfigMapStore{Client: k, Namespace: "nvca-system"}
	ctx := context.Background()
	c := &clock{t: time.Unix(20_000, 0)}

	first := &Tracker{Store: store, Now: c.now}
	require.NoError(t, first.Observe(ctx, true))
	c.advance(time.Minute)
	_, err := first.Scan(ctx, fixedLister{})
	require.NoError(t, err)

	// A new process loads the same record.
	second := &Tracker{Store: store, Now: c.now}
	ok, why := second.State(ctx).Cleared()
	assert.True(t, ok, why)
	// A time read back from the ConfigMap is in UTC whatever the zone of the process that wrote it.
	assert.True(t, first.State(ctx).FirstVerifiedAt.Equal(second.State(ctx).FirstVerifiedAt))

	cm, err := k.CoreV1().ConfigMaps("nvca-system").Get(ctx, StateConfigMapName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Contains(t, cm.Data["state.json"], "firstVerifiedAt")
}

func TestKubeListerFindsReservedObjectsInInstanceNamespaces(t *testing.T) {
	ctx := context.Background()
	created := metav1.NewTime(time.Unix(30_000, 0))
	roleLabels := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}
	k := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "sr-a", Labels: map[string]string{
			nvcatypes.WorkloadInstanceTypeLabel: nvcatypes.WorkloadInstanceTypeValueMiniService}}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "plain"}},
		// legacy utils pod: annotation only
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "utils", Namespace: "sr-a", CreationTimestamp: created,
			Annotations: map[string]string{nvcatypes.InfraObjectAnnotationKey: "true"}}},
		// ordinary chart pod
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "sr-a"}},
		// protected Deployment through its template label
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "utils", Namespace: "sr-a", CreationTimestamp: created},
			Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: roleLabels}}}},
		// reserved object outside an instance namespace is not listed
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "elsewhere", Namespace: "plain", Labels: roleLabels}},
	)
	got, err := KubeLister{Client: k}.ReservedObjects(ctx)
	require.NoError(t, err)
	byName := map[string]Object{}
	for _, o := range got {
		byName[o.Kind+"/"+o.Name] = o
	}
	require.Len(t, byName, 2)
	assert.True(t, byName["Pod/utils"].OnlyInfraAnnotation)
	assert.False(t, byName["Deployment/utils"].OnlyInfraAnnotation)
	assert.Equal(t, created.Time, byName["Deployment/utils"].Created)
}

// TestASilenceBetweenVerifiedObservationsIsAGap covers a checker that stops or hangs and then resumes still reporting
// verified: no observation says enforcement was lost, but nothing proved it either, so the silence is a gap.
func TestASilenceBetweenVerifiedObservationsIsAGap(t *testing.T) {
	tr, c, _ := newTracker(t)
	tr.MaxGap = 3 * time.Minute
	ctx := context.Background()
	require.NoError(t, tr.Observe(ctx, true))
	c.advance(time.Minute)
	require.NoError(t, tr.Observe(ctx, true))
	_, err := tr.Scan(ctx, fixedLister{})
	require.NoError(t, err)
	ok, _ := tr.State(ctx).Cleared()
	require.True(t, ok)

	last := c.now()
	c.advance(10 * time.Minute) // the checker was silent
	require.NoError(t, tr.Observe(ctx, true))
	st := tr.State(ctx)
	require.Len(t, st.Windows, 1)
	assert.Equal(t, last, st.Windows[0].Start)
	assert.Equal(t, c.now(), st.Windows[0].End)
	ok, why := st.Cleared()
	assert.False(t, ok, "the silence invalidates the clearance")
	assert.Contains(t, why, "no clean inventory scan")
	assert.True(t, st.Suspect(Object{Kind: "Pod", Name: "x", Created: last.Add(5 * time.Minute)}),
		"an object created during the silence is suspect")
}

func TestObservationsWithinMaxGapAreContinuous(t *testing.T) {
	tr, c, _ := newTracker(t)
	tr.MaxGap = 3 * time.Minute
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		require.NoError(t, tr.Observe(ctx, true))
		c.advance(2 * time.Minute)
	}
	assert.Empty(t, tr.State(ctx).Windows)
}

// managed builds a managed-fields entry that wrote the given field paths at the given time.
func managed(manager, op, subresource string, at time.Time, fields string) metav1.ManagedFieldsEntry {
	return metav1.ManagedFieldsEntry{
		Manager: manager, Operation: metav1.ManagedFieldsOperationType(op), Subresource: subresource,
		Time: &metav1.Time{Time: at}, FieldsV1: &metav1.FieldsV1{Raw: []byte(fields)},
	}
}

func roleLabelFields() string {
	return `{"f:metadata":{"f:labels":{"f:` + nvcatypes.PodRoleLabelKey + `":{}}}}`
}

// TestAModificationDuringAGapIsSuspectWhateverTheCreationTime is the regression for an object created while enforcement
// held whose reserved metadata was written while it did not: the creation time alone looked clean.
func TestAModificationDuringAGapIsSuspectWhateverTheCreationTime(t *testing.T) {
	state := State{
		FirstVerifiedAt: time.Unix(1_000, 0),
		Windows:         []Window{{Start: time.Unix(5_000, 0), End: time.Unix(6_000, 0)}},
	}
	created := time.Unix(2_000, 0) // inside verified enforcement
	for name, tt := range map[string]struct {
		o       Object
		suspect bool
	}{
		"created while verified, content verified":        {Object{Kind: "Pod", Name: "p", Created: created, Modified: created, ContentVerified: true}, false},
		"created before the gap, content not verified":    {Object{Kind: "Pod", Name: "p", Created: created, Modified: created}, true},
		"created after every gap needs no content check":  {Object{Kind: "Pod", Name: "p", Created: time.Unix(7_000, 0), Modified: time.Unix(7_000, 0)}, false},
		"modified inside the gap":                         {Object{Kind: "Pod", Name: "p", Created: created, Modified: time.Unix(5_500, 0)}, true},
		"modified after the gap, content verified":        {Object{Kind: "Pod", Name: "p", Created: created, Modified: time.Unix(7_000, 0), ContentVerified: true}, false},
		"modified before enforcement was ever verified":   {Object{Kind: "Pod", Name: "p", Created: created, Modified: time.Unix(500, 0)}, true},
		"reserved metadata no managed field accounts for": {Object{Kind: "Pod", Name: "p", Created: created, ModifiedUnknown: true}, true},
		"legacy infra object stays exempt":                {Object{Kind: "Pod", Name: "utils", Created: created, Modified: time.Unix(5_500, 0), OnlyInfraAnnotation: true}, false},
	} {
		t.Run(name, func(t *testing.T) { assert.Equal(t, tt.suspect, state.Suspect(tt.o)) })
	}
}

type stubTrust struct {
	d  TrustedDigests
	ok bool
}

func (s stubTrust) UtilsTrust(context.Context, string) (TrustedDigests, bool, error) {
	return s.d, s.ok, nil
}

func utilsSpec(image string) corev1.PodSpec {
	return corev1.PodSpec{Containers: []corev1.Container{{Name: "utils", Image: image, Command: []string{"/utils"}}}}
}

const (
	trustedReplicas      = 2
	trustedDeploymentUID = types.UID("utils-dep-uid")
)

func controllerOf(kind, name string, uid types.UID) metav1.OwnerReference {
	yes := true
	return metav1.OwnerReference{Kind: kind, Name: name, UID: uid, Controller: &yes}
}

func trustedDeploymentSpec(image string) appsv1.DeploymentSpec {
	two := int32(trustedReplicas)
	zero := intstr.FromInt32(0)
	one := intstr.FromInt32(1)
	return appsv1.DeploymentSpec{
		Replicas: &two,
		Strategy: appsv1.DeploymentStrategy{Type: appsv1.RollingUpdateDeploymentStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDeployment{MaxSurge: &zero, MaxUnavailable: &one}},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}},
			Spec:       utilsSpec(image),
		},
	}
}

// trustFor is what NVCA records for a Deployment it created with the given image.
func trustFor(spec corev1.PodSpec) TrustedDigests {
	dep := trustedDeploymentSpec(spec.Containers[0].Image)
	return TrustedDigests{
		PodSpec: utilsprovenance.PodSpecDigest(&spec), Containers: utilsprovenance.ContainersDigest(&spec),
		DeploymentSpec: utilsprovenance.DeploymentSpecDigest(&dep), Replicas: trustedReplicas, DeploymentUID: trustedDeploymentUID,
	}
}

func TestListerReadsTheLastWriteOfReservedMetadataFromManagedFields(t *testing.T) {
	ctx := context.Background()
	created := metav1.NewTime(time.Unix(2_000, 0))
	gapWrite := time.Unix(5_500, 0)
	roleLabels := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}
	pod := func(name string, entries ...metav1.ManagedFieldsEntry) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "sr-a", Labels: roleLabels, CreationTimestamp: created, ManagedFields: entries},
			Spec: utilsSpec("utils:1")}
	}
	k := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "sr-a", Labels: map[string]string{
			nvcatypes.WorkloadInstanceTypeLabel: nvcatypes.WorkloadInstanceTypeValueMiniService}}},
		// the label was written when the pod was created, and nothing since
		pod("clean", managed("kube-controller-manager", "Update", "", created.Time, roleLabelFields())),
		// the label was written again in the gap by someone else
		pod("rewritten", managed("kube-controller-manager", "Update", "", created.Time, roleLabelFields()),
			managed("someone", "Update", "", gapWrite, roleLabelFields())),
		// the kubelet wrote only status in the gap: not a write of reserved metadata
		pod("status only", managed("kube-controller-manager", "Update", "", created.Time, roleLabelFields()),
			managed("kubelet", "Update", "status", gapWrite, `{"f:status":{"f:phase":{}}}`)),
		// another manager touched unrelated metadata in the gap
		pod("other metadata", managed("kube-controller-manager", "Update", "", created.Time, roleLabelFields()),
			managed("someone", "Update", "", gapWrite, `{"f:metadata":{"f:annotations":{"f:note":{}}}}`)),
		// reserved label present, no managed field owns it
		pod("stripped"),
	)
	got, err := KubeLister{Client: k, Trust: stubTrust{trustFor(utilsSpec("utils:1")), true}}.ReservedObjects(ctx)
	require.NoError(t, err)
	byName := map[string]Object{}
	for _, o := range got {
		byName[o.Name] = o
	}
	require.Len(t, byName, 5)

	state := State{FirstVerifiedAt: time.Unix(1_000, 0), Windows: []Window{{Start: time.Unix(5_000, 0), End: time.Unix(6_000, 0)}}}
	assert.False(t, state.Suspect(byName["clean"]))
	assert.True(t, state.Suspect(byName["rewritten"]), "a write of the reserved label in the gap")
	assert.False(t, state.Suspect(byName["status only"]), "status writes are ignored")
	assert.False(t, state.Suspect(byName["other metadata"]), "only reserved metadata counts")
	assert.True(t, state.Suspect(byName["stripped"]), "stripped managed fields must not look clean")
}

// TestTemplateTamperingDuringAGapIsFoundByContent is the regression for a protected object changed during a gap while its
// labels, owner and reserved-metadata write times stayed as they were. Only content can show that. It covers the pod spec
// and, as importantly, the replica count and rolling strategy, which the pod spec digest does not.
func TestTemplateTamperingDuringAGapIsFoundByContent(t *testing.T) {
	ctx := context.Background()
	created := metav1.NewTime(time.Unix(2_000, 0))
	cleanFields := []metav1.ManagedFieldsEntry{managed("nvca", "Apply", "", created.Time, roleLabelFields())}
	roleLabels := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}
	obj := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: "sr-a", CreationTimestamp: created, ManagedFields: cleanFields}
	}
	podObj := func(name, image string) *corev1.Pod {
		m := obj(name)
		m.Labels = roleLabels
		return &corev1.Pod{ObjectMeta: m, Spec: utilsSpec(image)}
	}
	depObj := func(name string, mutate func(*appsv1.DeploymentSpec)) *appsv1.Deployment {
		spec := trustedDeploymentSpec("utils:1")
		mutate(&spec)
		m := obj(name)
		m.UID = trustedDeploymentUID
		return &appsv1.Deployment{ObjectMeta: m, Spec: spec}
	}
	rsObj := func(name, image string, replicas int32) *appsv1.ReplicaSet {
		m := obj(name)
		m.OwnerReferences = []metav1.OwnerReference{controllerOf("Deployment", "utils", trustedDeploymentUID)}
		return &appsv1.ReplicaSet{ObjectMeta: m, Spec: appsv1.ReplicaSetSpec{Replicas: &replicas, Template: trustedDeploymentSpec(image).Template}}
	}
	// chartRS is a ReplicaSet of the function's own workload, in the same namespace and owned by another Deployment.
	chartRS := func(name string, replicas int32) *appsv1.ReplicaSet {
		return &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "sr-a", CreationTimestamp: created,
			OwnerReferences: []metav1.OwnerReference{controllerOf("Deployment", "web", "chart-dep-uid")}},
			Spec: appsv1.ReplicaSetSpec{Replicas: &replicas}}
	}
	none := func(*appsv1.DeploymentSpec) {}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "sr-a", Labels: map[string]string{
		nvcatypes.WorkloadInstanceTypeLabel: nvcatypes.WorkloadInstanceTypeValueMiniService}}}
	state := State{FirstVerifiedAt: time.Unix(1_000, 0), Windows: []Window{{Start: time.Unix(5_000, 0), End: time.Unix(6_000, 0)}}}
	suspectNames := func(trust Trust, objs ...runtime.Object) map[string]bool {
		got, err := KubeLister{Client: fake.NewSimpleClientset(append([]runtime.Object{ns}, objs...)...), Trust: trust}.ReservedObjects(ctx)
		require.NoError(t, err)
		out := map[string]bool{}
		for _, o := range got {
			if state.Suspect(o) {
				out[o.Name] = true
			}
		}
		return out
	}
	genuine := stubTrust{trustFor(utilsSpec("utils:1")), true}

	t.Run("an image change in a Deployment, a ReplicaSet and a pod", func(t *testing.T) {
		got := suspectNames(genuine,
			depObj("good-dep", none),
			depObj("tampered-dep", func(s *appsv1.DeploymentSpec) { s.Template.Spec.Containers[0].Image = "evil:1" }),
			rsObj("good-rs", "utils:1", 1), rsObj("tampered-rs", "evil:1", 0),
			podObj("good-pod", "utils:1"), podObj("tampered-pod", "evil:1"))
		assert.Equal(t, map[string]bool{"tampered-dep": true, "tampered-rs": true, "tampered-pod": true}, got)
	})
	t.Run("the replica count of the Deployment was raised through the main resource", func(t *testing.T) {
		// Same template, same labels and owner: only spec.replicas differs.
		got := suspectNames(genuine,
			depObj("scaled-up", func(s *appsv1.DeploymentSpec) { n := int32(50); s.Replicas = &n }),
			depObj("scaled-down", func(s *appsv1.DeploymentSpec) { n := int32(1); s.Replicas = &n }),
			depObj("unset", func(s *appsv1.DeploymentSpec) { s.Replicas = nil }),
			depObj("genuine", none))
		assert.Equal(t, map[string]bool{"scaled-up": true, "scaled-down": true, "unset": true}, got)
	})
	t.Run("the rolling strategy was changed to surge past the replica count", func(t *testing.T) {
		got := suspectNames(genuine, depObj("surge", func(s *appsv1.DeploymentSpec) { n := intstr.FromInt32(5); s.Strategy.RollingUpdate.MaxSurge = &n }),
			depObj("genuine", none))
		assert.Equal(t, map[string]bool{"surge": true}, got)
	})
	t.Run("ReplicaSet replica counts follow the recorded count", func(t *testing.T) {
		assert.Empty(t, suspectNames(genuine, rsObj("only", "utils:1", 2)), "a ReplicaSet at the recorded count")
		assert.Empty(t, suspectNames(genuine, rsObj("old", "utils:1", 1), rsObj("new", "utils:1", 1)), "a rolling update with maxSurge 0: 1 and 1")
		assert.Equal(t, map[string]bool{"big": true}, suspectNames(genuine, rsObj("big", "utils:1", 9)), "one ReplicaSet above the count")
		assert.Equal(t, map[string]bool{"a": true, "b": true}, suspectNames(genuine, rsObj("a", "utils:1", 2), rsObj("b", "utils:1", 2)),
			"the ReplicaSets together run more than the recorded count")
	})
	t.Run("the function's own ReplicaSets do not count towards the utils replicas", func(t *testing.T) {
		// The normal Helm layout: two utils replicas and a chart Deployment in the same namespace. The chart's replica count
		// must not change how the utils objects are verified, whatever it is.
		for _, chartReplicas := range []int32{1, 3, 50} {
			got := suspectNames(genuine,
				depObj("utils-dep", none), rsObj("utils-rs-old", "utils:1", 1), rsObj("utils-rs-new", "utils:1", 1),
				chartRS("web-rs", chartReplicas))
			assert.Empty(t, got, "chart replicas %d", chartReplicas)
		}
		// A role-labelled ReplicaSet that the recorded Deployment does not control cannot be verified.
		foreign := rsObj("forged-rs", "utils:1", 1)
		foreign.OwnerReferences = []metav1.OwnerReference{controllerOf("Deployment", "web", "chart-dep-uid")}
		assert.Equal(t, map[string]bool{"forged-rs": true}, suspectNames(genuine, rsObj("utils-rs", "utils:1", 2), foreign))
		// A Deployment named utils with another UID is not the recorded one.
		other := depObj("other-uid", none)
		other.UID = "someone-elses"
		assert.Equal(t, map[string]bool{"other-uid": true}, suspectNames(genuine, other))
	})
	t.Run("without recorded digests nothing that existed across the gap is trusted", func(t *testing.T) {
		objs := []runtime.Object{depObj("d", none), rsObj("r", "utils:1", 1), podObj("p", "utils:1")}
		assert.Len(t, suspectNames(stubTrust{}, objs...), 3)
		assert.Len(t, suspectNames(nil, objs...), 3)
		incomplete := trustFor(utilsSpec("utils:1"))
		incomplete.DeploymentSpec = ""
		assert.Len(t, suspectNames(stubTrust{incomplete, true}, objs...), 3, "a record that lacks one digest gives no trust")
		noUID := trustFor(utilsSpec("utils:1"))
		noUID.DeploymentUID = ""
		assert.Len(t, suspectNames(stubTrust{noUID, true}, objs...), 3, "a record without the Deployment UID gives no trust")
	})
}

// TestARecoveryScanCatchesAModifiedObject runs the whole loop: enforcement is lost and comes back, and the scan that
// follows must not clear while an object written during the gap exists.
func TestARecoveryScanCatchesAModifiedObject(t *testing.T) {
	ctx := context.Background()
	tr, c, _ := newTracker(t)
	require.NoError(t, tr.Observe(ctx, true))
	created := metav1.NewTime(c.t.Add(time.Second))
	c.advance(time.Minute)
	require.NoError(t, tr.Observe(ctx, false)) // the gap opens
	c.advance(time.Minute)
	gapWrite := c.t
	c.advance(time.Minute)
	require.NoError(t, tr.Observe(ctx, true)) // and closes

	roleLabels := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}
	k := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "sr-a", Labels: map[string]string{
			nvcatypes.WorkloadInstanceTypeLabel: nvcatypes.WorkloadInstanceTypeValueMiniService}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "sr-a", Labels: roleLabels, CreationTimestamp: created,
			ManagedFields: []metav1.ManagedFieldsEntry{
				managed("kube-controller-manager", "Update", "", created.Time, roleLabelFields()),
				managed("someone", "Update", "", gapWrite, roleLabelFields())}}},
	)
	suspects, err := tr.Scan(ctx, KubeLister{Client: k})
	require.NoError(t, err)
	require.Len(t, suspects, 1)
	cleared, _ := tr.State(ctx).Cleared()
	assert.False(t, cleared, "the gate must stay closed")
}

// TestAGapStartsAtTheLastVerifiedObservationNotAStaleSave is the regression for a restart right after an instance was
// created: the persisted last verified time lagged by minutes, the gap started before the instance existed, and a
// genuine HA instance created while enforcement was verified was treated as created inside the gap.
func TestAGapStartsAtTheLastVerifiedObservationNotAStaleSave(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	c := &clock{t: time.Unix(100_000, 0)}
	first := &Tracker{Store: store, Now: c.now}
	require.NoError(t, first.Observe(ctx, true))
	var lastVerified time.Time
	for i := 0; i < 4; i++ { // the readiness check, every two minutes
		c.advance(2 * time.Minute)
		require.NoError(t, first.Observe(ctx, true))
		lastVerified = c.now()
	}
	created := lastVerified.Add(30 * time.Second) // an instance created just after the last verified observation

	// NVCA restarts: a new process loads the persisted record and cannot yet verify enforcement.
	c.advance(time.Minute)
	second := &Tracker{Store: store, Now: c.now}
	require.NoError(t, second.Observe(ctx, false))
	st := second.State(ctx)
	require.Len(t, st.Windows, 1)
	assert.Equal(t, lastVerified, st.Windows[0].Start, "the gap starts at the last verified observation, not at an older save")

	// An object created before that observation is not in the gap; one created after it is.
	assert.False(t, st.Suspect(Object{Kind: "Pod", Name: "before", Created: lastVerified.Add(-time.Minute), Modified: lastVerified.Add(-time.Minute), ContentVerified: true}))
	assert.True(t, st.Suspect(Object{Kind: "Pod", Name: "after", Created: created, Modified: created, ContentVerified: true}))
}
