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

package readiness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
)

func webhookDenial(name string, reason utilsprovenance.Reason) error {
	msg := fmt.Sprintf(`admission webhook %q denied the request: %s`, name,
		utilsprovenance.FormatDenial(utilsprovenance.Decision{Reason: reason, Message: "only NVCA may create"}))
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status: metav1.StatusFailure, Code: 403, Reason: metav1.StatusReasonForbidden, Message: msg,
	}}
}

func TestClassifyCanary(t *testing.T) {
	forbidden := func(msg string) error {
		return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "canary", errors.New(msg))
	}
	for name, tt := range map[string]struct {
		err  error
		want Outcome
	}{
		"accepted means enforcement is not active": {nil, OutcomeAccepted},
		"refused by the provenance webhook for the expected reason": {
			webhookDenial(utilsprovenance.WebhookName, utilsprovenance.ReasonDenyWriter), OutcomeRefused},
		"provenance webhook refused for a different reason": {
			webhookDenial(utilsprovenance.WebhookName, utilsprovenance.ReasonDenyProtectedEdit), OutcomeOtherDenial},
		"another webhook refused, even with the same marker text": {
			webhookDenial("some-other.example.com", utilsprovenance.ReasonDenyWriter), OutcomeOtherDenial},
		"RBAC denial": {forbidden(`User "system:serviceaccount:x:y" cannot create resource "pods"`), OutcomeOtherDenial},
		"forbidden with the marker text but not from the webhook": {
			forbidden("utils-provenance[deny_writer]: pretend"), OutcomeOtherDenial},
		"timeout":             {apierrors.NewTimeoutError("webhook", 5), OutcomeInconclusive},
		"server timeout":      {apierrors.NewServerTimeout(schema.GroupResource{Resource: "pods"}, "create", 5), OutcomeInconclusive},
		"service unavailable": {apierrors.NewServiceUnavailable("webhook down"), OutcomeInconclusive},
		"too many requests":   {apierrors.NewTooManyRequests("slow down", 1), OutcomeInconclusive},
		"internal error":      {apierrors.NewInternalError(errors.New("failed calling webhook")), OutcomeInconclusive},
		"context deadline":    {context.DeadlineExceeded, OutcomeInconclusive},
		"some other error":    {errors.New("boom"), OutcomeError},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.want, ClassifyCanary(tt.err))
		})
	}
}

func TestOutcomesMatchTheMetricLabelSet(t *testing.T) {
	var got []string
	for _, o := range AllOutcomes {
		got = append(got, string(o))
	}
	assert.ElementsMatch(t, metrics.AllUtilsProvenanceCheckOutcomes, got)
}

type fakeCanary struct {
	results []error
	calls   int
}

func (f *fakeCanary) DryRunForgedPod(context.Context) error {
	err := f.results[min(f.calls, len(f.results)-1)]
	f.calls++
	return err
}

type fakeAccounts struct {
	list []Account
	err  error
}

func (f fakeAccounts) InstanceAccounts(context.Context) ([]Account, error) { return f.list, f.err }

// fakeReviewer allows the listed "namespace/account|right" combinations, like extra RoleBindings would.
type fakeReviewer struct {
	allow map[string]bool
	err   error
	seen  []string
}

func (f *fakeReviewer) Allowed(_ context.Context, user string, groups []string, ns string, r Right, name string) (bool, error) {
	f.seen = append(f.seen, user+"|"+fmt.Sprint(groups))
	if f.err != nil {
		return false, f.err
	}
	key := user + "|" + r.String()
	if name != "" {
		key += "|" + name
	}
	return f.allow[key], nil
}

type fakeObjects struct {
	list []ProtectedObject
	err  error
}

func (f fakeObjects) ProtectedObjects(context.Context) ([]ProtectedObject, error) {
	return f.list, f.err
}

type fakeRecorder struct {
	checks    map[string]int
	enf, rbac bool
}

func (f *fakeRecorder) IncCheck(check, outcome string) {
	if f.checks == nil {
		f.checks = map[string]int{}
	}
	f.checks[check+"/"+outcome]++
}
func (f *fakeRecorder) SetReadiness(e, r bool) { f.enf, f.rbac = e, r }

func refused() error {
	return webhookDenial(utilsprovenance.WebhookName, utilsprovenance.ReasonDenyWriter)
}

func TestEnforcementNeedsConsecutiveRefusals(t *testing.T) {
	canary := &fakeCanary{results: []error{refused(), refused(), refused()}}
	rec := &fakeRecorder{}
	c := &Checker{Canary: canary, Recorder: rec}

	s := c.CheckCanary(context.Background())
	assert.False(t, s.EnforcementVerified, "one refusal is not proof")
	assert.Equal(t, 1, s.ConsecutiveRefusals)
	s = c.CheckCanary(context.Background())
	assert.False(t, s.EnforcementVerified)
	s = c.CheckCanary(context.Background())
	assert.True(t, s.EnforcementVerified, "three consecutive refusals prove enforcement")
	assert.True(t, rec.enf)
	assert.Equal(t, 3, rec.checks["canary/refused"])
}

func TestAnyOtherOutcomeResetsVerification(t *testing.T) {
	for name, bad := range map[string]error{
		"accepted":            nil,
		"rbac denial":         apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "c", errors.New("rbac")),
		"another webhook":     webhookDenial("other.example.com", utilsprovenance.ReasonDenyWriter),
		"timeout":             context.DeadlineExceeded,
		"service unavailable": apierrors.NewServiceUnavailable("down"),
	} {
		t.Run(name, func(t *testing.T) {
			canary := &fakeCanary{results: []error{refused(), refused(), refused(), bad, refused()}}
			c := &Checker{Canary: canary}
			for i := 0; i < 3; i++ {
				c.CheckCanary(context.Background())
			}
			require.True(t, c.Status().EnforcementVerified)
			s := c.CheckCanary(context.Background())
			assert.False(t, s.EnforcementVerified, "%s must leave enforcement unverified", name)
			assert.Equal(t, 0, s.ConsecutiveRefusals)
			s = c.CheckCanary(context.Background())
			assert.False(t, s.EnforcementVerified, "a single refusal after a reset is not enough")
		})
	}
}

func TestStaleResultIsNotVerified(t *testing.T) {
	now := time.Unix(1000, 0)
	c := &Checker{
		Canary: &fakeCanary{results: []error{refused()}}, CanaryInterval: time.Minute,
		Now: func() time.Time { return now },
	}
	for i := 0; i < 3; i++ {
		c.CheckCanary(context.Background())
	}
	require.True(t, c.Status().EnforcementVerified)
	now = now.Add(2 * time.Minute)
	assert.True(t, c.Status().EnforcementVerified, "inside three intervals it is still fresh")
	now = now.Add(2 * time.Minute)
	s := c.Status()
	assert.False(t, s.EnforcementVerified, "a stopped checker must not leave a stale verified")
	assert.Contains(t, s.Reason, "too old")
}

func TestRBACReviewFindsEffectivePermissions(t *testing.T) {
	accounts := fakeAccounts{list: []Account{
		{Namespace: "sr-a", Name: "miniservice-instance-permissions"},
		{Namespace: "sr-a", Name: "chart-sa"},
		{Namespace: "sr-b", Name: "miniservice-instance-permissions"},
	}}

	t.Run("nobody holds a protected right", func(t *testing.T) {
		rev := &fakeReviewer{}
		c := &Checker{Accounts: accounts, Reviewer: rev, Recorder: &fakeRecorder{}}
		s := c.CheckRBAC(context.Background())
		assert.True(t, s.RBACVerified)
		assert.Empty(t, s.RBACExposures)
		assert.Len(t, rev.seen, len(accounts.list)*len(ProtectedRights)+len(accounts.list)*namedTargetsPerAccount(),
			"every account is reviewed for every right, and by name where an expected name applies")
		assert.Contains(t, rev.seen[0], "system:serviceaccounts:sr-a", "group bindings are part of the review")
	})

	t.Run("an extra RoleBinding or ClusterRoleBinding exposes a chart account", func(t *testing.T) {
		rev := &fakeReviewer{allow: map[string]bool{
			"system:serviceaccount:sr-a:chart-sa|update deployments/scale":                               true,
			"system:serviceaccount:sr-b:miniservice-instance-permissions|patch pods/ephemeralcontainers": true,
		}}
		rec := &fakeRecorder{}
		c := &Checker{Accounts: accounts, Reviewer: rev, Recorder: rec}
		s := c.CheckRBAC(context.Background())
		assert.False(t, s.RBACVerified)
		assert.Equal(t, []string{
			"sr-a/chart-sa: update deployments/scale",
			"sr-b/miniservice-instance-permissions: patch pods/ephemeralcontainers",
		}, s.RBACExposures)
		assert.Equal(t, 1, rec.checks["rbac/exposed"])
		assert.False(t, rec.rbac)
	})

	t.Run("a failing review is unverified, not clean", func(t *testing.T) {
		c := &Checker{Accounts: accounts, Reviewer: &fakeReviewer{err: errors.New("api down")}}
		s := c.CheckRBAC(context.Background())
		assert.False(t, s.RBACVerified)
		assert.Equal(t, OutcomeUnverified, s.LastRBAC)
		c = &Checker{Accounts: fakeAccounts{err: errors.New("list failed")}, Reviewer: &fakeReviewer{}}
		assert.False(t, c.CheckRBAC(context.Background()).RBACVerified)
	})
}

func TestVerifiedNeedsBothParts(t *testing.T) {
	c := &Checker{
		Canary: &fakeCanary{results: []error{refused()}}, Accounts: fakeAccounts{}, Reviewer: &fakeReviewer{},
	}
	for i := 0; i < 3; i++ {
		c.CheckCanary(context.Background())
	}
	assert.False(t, c.Status().Verified(), "enforcement alone is not enough")
	c.CheckRBAC(context.Background())
	assert.True(t, c.Status().Verified())
}

func TestRunReportsUnverifiedOnExit(t *testing.T) {
	rec := &fakeRecorder{}
	c := &Checker{
		Canary: &fakeCanary{results: []error{refused()}}, Accounts: fakeAccounts{}, Reviewer: &fakeReviewer{},
		Recorder: rec, CanaryInterval: time.Millisecond, RBACInterval: time.Millisecond, RequiredRefusals: 2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	require.Eventually(t, func() bool { return c.Status().Verified() }, 2*time.Second, time.Millisecond)
	cancel()
	<-done
	assert.False(t, rec.enf || rec.rbac, "gauges must drop to zero when the checker stops")
	assert.False(t, c.Status().Verified())
}

func TestLogsOnlyOnChange(t *testing.T) {
	var buf bytes.Buffer
	log := logrus.New()
	log.SetOutput(&buf)
	log.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})

	rev := &fakeReviewer{allow: map[string]bool{"system:serviceaccount:sr-a:chart-sa|update deployments/scale": true}}
	c := &Checker{
		Accounts: fakeAccounts{list: []Account{{Namespace: "sr-a", Name: "chart-sa"}}}, Reviewer: rev, Log: log,
	}
	c.CheckRBAC(context.Background())
	first := buf.String()
	assert.Contains(t, first, "instance accounts hold protected rights")
	assert.Contains(t, first, "sr-a/chart-sa: update deployments/scale", "the exposed account and right are logged")

	buf.Reset()
	c.CheckRBAC(context.Background())
	assert.Empty(t, buf.String(), "an unchanged state is not logged again")

	rev.allow = nil
	c.CheckRBAC(context.Background())
	assert.NotEmpty(t, buf.String(), "a change is logged")
}

// namedTargetsPerAccount is the number of extra named questions per account with the default expected names: the
// deployments/scale and pods/ephemeralcontainers rights, each with update and patch, for the name "utils".
func namedTargetsPerAccount() int { return 4 * len(DefaultExpectedNames) }

// TestRBACReviewSeesRulesRestrictedByResourceName is the regression for a binding limited with resourceNames. Such a
// rule grants the right only on the named object, so a review that asks about the resource without a name reports no
// permission while the account can scale the utils Deployment.
func TestRBACReviewSeesRulesRestrictedByResourceName(t *testing.T) {
	accounts := fakeAccounts{list: []Account{{Namespace: "sr-a", Name: "chart-sa"}}}
	const user = "system:serviceaccount:sr-a:chart-sa"

	t.Run("the expected utils name is reviewed by name", func(t *testing.T) {
		rev := &fakeReviewer{allow: map[string]bool{user + "|update deployments/scale|utils": true}}
		s := (&Checker{Accounts: accounts, Reviewer: rev}).CheckRBAC(context.Background())
		assert.False(t, s.RBACVerified)
		assert.Equal(t, []string{"sr-a/chart-sa: update deployments/scale (name utils)"}, s.RBACExposures)
	})

	t.Run("the legacy utils pod is reviewed by name for ephemeral containers", func(t *testing.T) {
		rev := &fakeReviewer{allow: map[string]bool{user + "|patch pods/ephemeralcontainers|utils": true}}
		s := (&Checker{Accounts: accounts, Reviewer: rev}).CheckRBAC(context.Background())
		assert.Equal(t, []string{"sr-a/chart-sa: patch pods/ephemeralcontainers (name utils)"}, s.RBACExposures)
	})

	t.Run("an existing protected ReplicaSet is reviewed by its real name", func(t *testing.T) {
		rev := &fakeReviewer{allow: map[string]bool{user + "|patch replicasets/scale|utils-5b66585456": true}}
		objs := fakeObjects{list: []ProtectedObject{
			{Namespace: "sr-a", Group: "apps", Resource: "replicasets", Name: "utils-5b66585456"},
			{Namespace: "sr-other", Group: "apps", Resource: "replicasets", Name: "ignored-in-another-namespace"},
		}}
		s := (&Checker{Accounts: accounts, Reviewer: rev, Objects: objs}).CheckRBAC(context.Background())
		assert.Equal(t, []string{"sr-a/chart-sa: patch replicasets/scale (name utils-5b66585456)"}, s.RBACExposures)
	})

	t.Run("without names the same binding is invisible, which is why names are asked", func(t *testing.T) {
		rev := &fakeReviewer{allow: map[string]bool{user + "|update deployments/scale|utils": true}}
		c := &Checker{Accounts: accounts, Reviewer: rev, ExpectedNames: []string{}}
		assert.True(t, c.CheckRBAC(context.Background()).RBACVerified, "documents the blind spot of an unnamed review")
	})

	t.Run("a failing object listing is unverified, not clean", func(t *testing.T) {
		c := &Checker{Accounts: accounts, Reviewer: &fakeReviewer{}, Objects: fakeObjects{err: errors.New("list failed")}}
		s := c.CheckRBAC(context.Background())
		assert.False(t, s.RBACVerified)
		assert.Equal(t, OutcomeUnverified, s.LastRBAC)
	})
}

type fixedVerifier struct{ s Status }

func (f fixedVerifier) Status() Status { return f.s }

func TestGateFailsClosed(t *testing.T) {
	now := time.Unix(5000, 0)
	verified := Status{EnforcementVerified: true, RBACVerified: true, LastCanaryAt: now, LastRBACAt: now}

	t.Run("verified passes", func(t *testing.T) {
		assert.NoError(t, Gate{Verifier: fixedVerifier{verified}}.Check())
		assert.True(t, Gate{Verifier: fixedVerifier{verified}}.Allowed())
	})
	t.Run("missing: no checker at all", func(t *testing.T) {
		err := Gate{}.Check()
		assert.ErrorIs(t, err, ErrMissing)
		assert.False(t, Gate{}.Allowed())
	})
	t.Run("missing: the checker has not run yet", func(t *testing.T) {
		assert.ErrorIs(t, Gate{Verifier: fixedVerifier{Status{}}}.Check(), ErrMissing)
	})
	t.Run("failed: enforcement not verified", func(t *testing.T) {
		s := verified
		s.EnforcementVerified, s.Reason = false, "the canary was accepted"
		err := Gate{Verifier: fixedVerifier{s}}.Check()
		assert.ErrorIs(t, err, ErrNotVerified)
		assert.Contains(t, err.Error(), "the canary was accepted")
	})
	t.Run("failed: RBAC exposure", func(t *testing.T) {
		s := verified
		s.RBACVerified, s.Reason = false, "1 instance accounts hold protected rights"
		assert.ErrorIs(t, Gate{Verifier: fixedVerifier{s}}.Check(), ErrNotVerified)
	})
	t.Run("failed is not missing and not stale", func(t *testing.T) {
		s := verified
		s.EnforcementVerified = false
		err := Gate{Verifier: fixedVerifier{s}}.Check()
		assert.NotErrorIs(t, err, ErrMissing)
		assert.NotErrorIs(t, err, ErrStale)
	})
}

// TestGateWithARealCheckerGoesStale drives a real Checker through verified to stale to confirm the gate refuses once
// the result expires and the status really carries the stale marker.
func TestGateWithARealCheckerGoesStale(t *testing.T) {
	now := time.Unix(1000, 0)
	c := &Checker{
		Canary: &fakeCanary{results: []error{refused()}}, Accounts: fakeAccounts{}, Reviewer: &fakeReviewer{},
		CanaryInterval: time.Minute, Now: func() time.Time { return now },
	}
	gate := Gate{Verifier: c}
	assert.ErrorIs(t, gate.Check(), ErrMissing, "before any check")
	for i := 0; i < 3; i++ {
		c.CheckCanary(context.Background())
	}
	assert.ErrorIs(t, gate.Check(), ErrNotVerified, "enforcement alone is not enough, RBAC has not been reviewed")
	c.CheckRBAC(context.Background())
	require.NoError(t, gate.Check())

	now = now.Add(4 * time.Minute)
	err := gate.Check()
	assert.ErrorIs(t, err, ErrStale, "the canary result expired")
	assert.True(t, c.Status().Stale)

	// The checker recovers on its own: the refusals continue, so a fresh canary verifies again and the gate reopens.
	c.CheckCanary(context.Background())
	assert.NoError(t, gate.Check())
	assert.False(t, c.Status().Stale)
}

type errCanary struct{ err error }

func (c errCanary) DryRunForgedPod(context.Context) error { return c.err }

// TestCanaryReasonNamesTheCause keeps the cause of an inconclusive canary visible. A single timeout drops verification,
// so an operator needs to see what happened without searching other logs.
func TestCanaryReasonNamesTheCause(t *testing.T) {
	cause := errors.New("Internal error occurred: failed calling webhook: context deadline exceeded")
	c := &Checker{Canary: errCanary{err: apierrors.NewInternalError(cause)}}
	st := c.CheckCanary(context.Background())
	assert.Equal(t, OutcomeInconclusive, st.LastCanary)
	assert.False(t, st.EnforcementVerified)
	assert.Contains(t, st.Reason, "inconclusive")
	assert.Contains(t, st.Reason, "context deadline exceeded")

	long := &Checker{Canary: errCanary{err: apierrors.NewInternalError(errors.New(strings.Repeat("x", 1000)))}}
	assert.Less(t, len(long.CheckCanary(context.Background()).Reason), 400)
}
