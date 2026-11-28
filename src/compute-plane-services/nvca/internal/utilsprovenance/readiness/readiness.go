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

// Package readiness proves that the utils provenance webhook is enforcing and that no instance account can scale
// utils objects, before NVCA creates any object whose label it would trust. The proof is deliberately narrow: only a
// refusal made by the provenance webhook itself, for the expected reason, counts. An RBAC denial, another webhook's
// refusal, a timeout, or an unavailable webhook leaves enforcement unverified.
package readiness

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
)

// Outcome is the result of one check.
type Outcome string

// Check outcomes. They mirror metrics.AllUtilsProvenanceCheckOutcomes.
const (
	OutcomeRefused      Outcome = "refused"      // canary: refused by the provenance webhook for the expected reason
	OutcomeAccepted     Outcome = "accepted"     // canary: the forged object was accepted, so enforcement is not active
	OutcomeOtherDenial  Outcome = "other_denial" // canary: denied, but not by the provenance webhook (RBAC, another webhook)
	OutcomeInconclusive Outcome = "inconclusive" // timeout, unavailable webhook or API server
	OutcomeError        Outcome = "error"        // any other failure
	OutcomeVerified     Outcome = "verified"     // rbac: no account holds a protected right
	OutcomeExposed      Outcome = "exposed"      // rbac: at least one account holds a protected right
	OutcomeUnverified   Outcome = "unverified"   // rbac: the review itself failed
)

// AllOutcomes is every Outcome, for tests and metric pre-initialization.
var AllOutcomes = []Outcome{
	OutcomeRefused, OutcomeAccepted, OutcomeOtherDenial, OutcomeInconclusive, OutcomeError,
	OutcomeVerified, OutcomeExposed, OutcomeUnverified,
}

// ExpectedCanaryReason is the refusal reason of the canary: a non-NVCA identity creating an object with the reserved
// utils label.
const ExpectedCanaryReason = utilsprovenance.ReasonDenyWriter

// ClassifyCanary maps the error of the canary's dry-run create to an Outcome.
func ClassifyCanary(err error) Outcome {
	switch {
	case err == nil:
		return OutcomeAccepted
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled),
		apierrors.IsTimeout(err), apierrors.IsServerTimeout(err), apierrors.IsServiceUnavailable(err),
		apierrors.IsTooManyRequests(err), apierrors.IsInternalError(err):
		return OutcomeInconclusive
	case apierrors.IsForbidden(err):
		msg := err.Error()
		reason, ok := utilsprovenance.ParseDenial(msg)
		if ok && reason == ExpectedCanaryReason && strings.Contains(msg, `"`+utilsprovenance.WebhookName+`"`) {
			return OutcomeRefused
		}
		return OutcomeOtherDenial
	default:
		return OutcomeError
	}
}

// Canary sends one forged object as an unprivileged identity, as a server-side dry run, and returns the API error.
type Canary interface {
	DryRunForgedPod(ctx context.Context) error
}

// Account is a service account in an instance namespace.
type Account struct{ Namespace, Name string }

// Accounts lists the service accounts of every instance namespace.
type Accounts interface {
	InstanceAccounts(ctx context.Context) ([]Account, error)
}

// Right is a verb on a resource and subresource that no instance account may hold.
type Right struct{ Group, Resource, Subresource, Verb string }

func (r Right) String() string {
	res := r.Resource
	if r.Subresource != "" {
		res += "/" + r.Subresource
	}
	return r.Verb + " " + res
}

// ProtectedRights are the rights over utils objects that only NVCA may use. Both update and patch are checked.
var ProtectedRights = func() []Right {
	var out []Right
	for _, p := range []struct{ group, resource, sub string }{
		{"apps", "deployments", "scale"},
		{"apps", "replicasets", "scale"},
		{"apps", "statefulsets", "scale"},
		{"", "pods", "ephemeralcontainers"},
	} {
		for _, verb := range []string{"update", "patch"} {
			out = append(out, Right{p.group, p.resource, p.sub, verb})
		}
	}
	return out
}()

// Reviewer answers whether a user, with its groups, holds a right in a namespace on an object of the given name. An
// empty name asks about the resource as a whole. It must reflect the effective permissions: every RoleBinding and
// ClusterRoleBinding that applies, not only the Role NVCA generates. The name matters: a rule restricted with
// resourceNames grants the right only on those objects, and a review without a name does not see it.
type Reviewer interface {
	Allowed(ctx context.Context, user string, groups []string, namespace string, right Right, name string) (bool, error)
}

// ProtectedObject is an existing object a protected right is checked against by name.
type ProtectedObject struct {
	Namespace string
	Group     string
	Resource  string
	Name      string
}

// Objects lists the existing objects that carry reserved utils metadata, so the review can ask about them by name.
type Objects interface {
	ProtectedObjects(ctx context.Context) ([]ProtectedObject, error)
}

// DefaultExpectedNames are object names checked in every instance namespace whether or not the object exists yet: the
// legacy utils pod, and the name the utils Deployment is expected to use. A binding restricted to these names is the
// most likely way to hold the right.
var DefaultExpectedNames = []string{"utils"}

// Status is the readiness state. Verified is true only when both parts are verified and recent.
type Status struct {
	EnforcementVerified bool
	RBACVerified        bool
	// ConsecutiveRefusals counts canary refusals in a row. Any other outcome resets it.
	ConsecutiveRefusals int
	LastCanary          Outcome
	LastCanaryAt        time.Time
	LastRBAC            Outcome
	LastRBACAt          time.Time
	// RBACExposures lists "namespace/account: right" for every account that holds a protected right.
	RBACExposures []string
	Reason        string
	// Stale is true when a part that was verified has expired, because its check stopped or hung.
	Stale bool
}

// Verified reports whether NVCA may rely on enforcement and on RBAC.
func (s Status) Verified() bool { return s.EnforcementVerified && s.RBACVerified }

// Recorder receives check results for metrics. Both methods may be nil-safe no-ops.
type Recorder interface {
	IncCheck(check, outcome string)
	SetReadiness(enforcementVerified, rbacVerified bool)
}

// Checker runs the canary and the RBAC review and holds the latest status.
type Checker struct {
	Canary   Canary
	Accounts Accounts
	Reviewer Reviewer
	// Objects, when set, adds a review of each existing protected object by name.
	Objects Objects
	// ExpectedNames are reviewed by name in every instance namespace. Nil means DefaultExpectedNames.
	ExpectedNames []string
	Recorder      Recorder

	// CanaryInterval is how often the canary runs. RBACInterval is how often accounts are reviewed.
	CanaryInterval time.Duration
	RBACInterval   time.Duration
	// RequiredRefusals is the number of consecutive refusals that prove enforcement. Default 3.
	RequiredRefusals int
	// MaxAge is how long a result stays valid. Older results count as unverified. Default 3 canary intervals.
	MaxAge time.Duration
	Now    func() time.Time
	// OnStatus, when set, is called with the status after every check. The rollout tracker uses it to record gaps.
	OnStatus func(Status)
	// Log, when set, receives one line whenever the verified state, the reason, or the exposure list changes.
	Log logrus.FieldLogger

	mu       sync.Mutex
	status   Status
	lastLogd string
}

func (c *Checker) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *Checker) required() int {
	if c.RequiredRefusals > 0 {
		return c.RequiredRefusals
	}
	return 3
}

func (c *Checker) maxAge() time.Duration {
	if c.MaxAge > 0 {
		return c.MaxAge
	}
	if c.CanaryInterval > 0 {
		return 3 * c.CanaryInterval
	}
	return 6 * time.Minute
}

// Status returns the current status. Results older than MaxAge are reported as unverified, so a stopped checker or a
// hung API server cannot leave a stale "verified" in place.
func (c *Checker) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.status
	now := c.now()
	if s.EnforcementVerified && now.Sub(s.LastCanaryAt) > c.maxAge() {
		s.EnforcementVerified, s.Stale = false, true
		s.Reason = "the last canary result is too old"
	}
	if s.RBACVerified && now.Sub(s.LastRBACAt) > 3*c.maxAge() {
		s.RBACVerified, s.Stale = false, true
		s.Reason = "the last RBAC review is too old"
	}
	return s
}

// CheckCanary runs the canary once and updates the status.
func (c *Checker) CheckCanary(ctx context.Context) Status {
	canaryErr := c.Canary.DryRunForgedPod(ctx)
	outcome := ClassifyCanary(canaryErr)
	c.record("canary", outcome)

	c.mu.Lock()
	c.status.LastCanary, c.status.LastCanaryAt = outcome, c.now()
	if outcome == OutcomeRefused {
		c.status.ConsecutiveRefusals++
	} else {
		c.status.ConsecutiveRefusals = 0
	}
	c.status.EnforcementVerified = c.status.ConsecutiveRefusals >= c.required()
	switch {
	case c.status.EnforcementVerified:
		c.status.Reason = ""
	case outcome == OutcomeRefused:
		c.status.Reason = fmt.Sprintf("%d of %d consecutive refusals so far", c.status.ConsecutiveRefusals, c.required())
	default:
		c.status.Reason = "the canary was not refused by the provenance webhook: " + string(outcome)
		if canaryErr != nil && (outcome == OutcomeInconclusive || outcome == OutcomeError) {
			c.status.Reason += " (" + truncate(canaryErr.Error(), 200) + ")"
		}
	}
	c.mu.Unlock()
	return c.publish()
}

// CheckRBAC reviews every instance account once and updates the status.
func (c *Checker) CheckRBAC(ctx context.Context) Status {
	exposures, err := c.reviewAccounts(ctx)
	var outcome Outcome
	switch {
	case err != nil:
		outcome = OutcomeUnverified
	case len(exposures) > 0:
		outcome = OutcomeExposed
	default:
		outcome = OutcomeVerified
	}
	c.record("rbac", outcome)

	c.mu.Lock()
	c.status.LastRBAC, c.status.LastRBACAt = outcome, c.now()
	c.status.RBACExposures = exposures
	c.status.RBACVerified = outcome == OutcomeVerified
	switch outcome {
	case OutcomeUnverified:
		c.status.Reason = "the RBAC review failed: " + err.Error()
	case OutcomeExposed:
		c.status.Reason = fmt.Sprintf("%d instance accounts hold protected rights", len(exposures))
	}
	c.mu.Unlock()
	return c.publish()
}

// reviewTarget is one question for the reviewer: a right, optionally on one named object.
type reviewTarget struct {
	right Right
	name  string
}

func (t reviewTarget) String() string {
	if t.name == "" {
		return t.right.String()
	}
	return fmt.Sprintf("%s (name %s)", t.right, t.name)
}

// targets returns the questions to ask in a namespace: every protected right on the resource as a whole, and on each
// object of the matching kind by name (the expected names, and every existing protected object).
func (c *Checker) targets(namespace string, objects []ProtectedObject) []reviewTarget {
	names := c.ExpectedNames
	if names == nil {
		names = DefaultExpectedNames
	}
	named := map[[2]string][]string{} // group/resource -> names
	add := func(group, resource, name string) {
		k := [2]string{group, resource}
		for _, have := range named[k] {
			if have == name {
				return
			}
		}
		named[k] = append(named[k], name)
	}
	for _, n := range names {
		add("apps", "deployments", n)
		add("", "pods", n)
	}
	for _, o := range objects {
		if o.Namespace == namespace {
			add(o.Group, o.Resource, o.Name)
		}
	}

	var out []reviewTarget
	for _, right := range ProtectedRights {
		out = append(out, reviewTarget{right: right})
		ns := append([]string(nil), named[[2]string{right.Group, right.Resource}]...)
		sort.Strings(ns)
		for _, n := range ns {
			out = append(out, reviewTarget{right: right, name: n})
		}
	}
	return out
}

func (c *Checker) reviewAccounts(ctx context.Context) ([]string, error) {
	accounts, err := c.Accounts.InstanceAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("list instance accounts: %w", err)
	}
	var objects []ProtectedObject
	if c.Objects != nil {
		if objects, err = c.Objects.ProtectedObjects(ctx); err != nil {
			return nil, fmt.Errorf("list protected objects: %w", err)
		}
	}
	var exposures []string
	for _, a := range accounts {
		user := "system:serviceaccount:" + a.Namespace + ":" + a.Name
		groups := []string{"system:serviceaccounts", "system:serviceaccounts:" + a.Namespace, "system:authenticated"}
		exposedRights := map[Right]bool{}
		for _, t := range c.targets(a.Namespace, objects) {
			// A right held on the whole resource already covers every name, so skip the named questions for it.
			if t.name != "" && exposedRights[t.right] {
				continue
			}
			ok, err := c.Reviewer.Allowed(ctx, user, groups, a.Namespace, t.right, t.name)
			if err != nil {
				return nil, fmt.Errorf("review %s: %w", user, err)
			}
			if ok {
				exposedRights[t.right] = t.name == "" || exposedRights[t.right]
				exposures = append(exposures, fmt.Sprintf("%s/%s: %s", a.Namespace, a.Name, t))
			}
		}
	}
	sort.Strings(exposures)
	return exposures, nil
}

func (c *Checker) record(check string, outcome Outcome) {
	if c.Recorder != nil {
		c.Recorder.IncCheck(check, string(outcome))
	}
}

func (c *Checker) publish() Status {
	s := c.Status()
	if c.Recorder != nil {
		c.Recorder.SetReadiness(s.EnforcementVerified, s.RBACVerified)
	}
	c.logChange(s)
	if c.OnStatus != nil {
		c.OnStatus(s)
	}
	return s
}

// maxLoggedExposures bounds the exposure list in one log line.
const maxLoggedExposures = 20

// logChange logs when the state changes, so a steady state does not repeat every interval.
func (c *Checker) logChange(s Status) {
	if c.Log == nil {
		return
	}
	key := fmt.Sprintf("%t|%t|%s|%v", s.EnforcementVerified, s.RBACVerified, s.Reason, s.RBACExposures)
	c.mu.Lock()
	changed := key != c.lastLogd
	c.lastLogd = key
	c.mu.Unlock()
	if !changed {
		return
	}
	exposures := s.RBACExposures
	if len(exposures) > maxLoggedExposures {
		exposures = append(exposures[:maxLoggedExposures:maxLoggedExposures], fmt.Sprintf("... and %d more", len(s.RBACExposures)-maxLoggedExposures))
	}
	l := c.Log.WithFields(logrus.Fields{
		"enforcement_verified": s.EnforcementVerified, "rbac_verified": s.RBACVerified,
		"last_canary": s.LastCanary, "last_rbac": s.LastRBAC, "reason": s.Reason, "rbac_exposures": exposures,
	})
	if s.Verified() {
		l.Info("Utils provenance readiness verified")
		return
	}
	if len(s.RBACExposures) > 0 {
		l.Warn("Utils provenance readiness not verified: instance accounts hold protected rights")
		return
	}
	l.Info("Utils provenance readiness not verified")
}

// Run checks until ctx ends. It runs both checks first, then each on its own interval.
func (c *Checker) Run(ctx context.Context) {
	c.CheckCanary(ctx)
	c.CheckRBAC(ctx)
	canary := time.NewTicker(orDefault(c.CanaryInterval, 2*time.Minute))
	rbac := time.NewTicker(orDefault(c.RBACInterval, 10*time.Minute))
	defer canary.Stop()
	defer rbac.Stop()
	for {
		select {
		case <-ctx.Done():
			// Report unverified on the way out so a stale metric does not outlive the checker.
			c.mu.Lock()
			c.status.EnforcementVerified, c.status.RBACVerified = false, false
			c.mu.Unlock()
			c.publish()
			return
		case <-canary.C:
			c.CheckCanary(ctx)
		case <-rbac.C:
			c.CheckRBAC(ctx)
		}
	}
}

func orDefault(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// truncate shortens s to at most n bytes, so a long API error does not fill a status message.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
