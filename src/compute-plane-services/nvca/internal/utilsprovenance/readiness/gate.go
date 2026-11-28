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
	"errors"
	"fmt"
)

// Reasons a Gate refuses. They are distinct so callers and logs can tell a missing check from a failed or stale one.
var (
	// ErrMissing means no readiness check is running, for example in audit mode, or it has not produced a result yet.
	ErrMissing = errors.New("utils provenance readiness is not available")
	// ErrNotVerified means the check ran and enforcement or the instance RBAC is not verified.
	ErrNotVerified = errors.New("utils provenance readiness is not verified")
	// ErrStale means a verified result expired, because the check stopped or hung.
	ErrStale = errors.New("utils provenance readiness result is stale")
)

// Verifier reports the readiness status. *Checker implements it.
type Verifier interface {
	Status() Status
}

// Gate decides whether code that trusts the utils label, or creates more than one utils pod, may run. It fails closed:
// anything other than a fresh verified status refuses.
type Gate struct {
	// Verifier is nil when no readiness check is running. That refuses.
	Verifier Verifier
}

// Check returns nil only when enforcement and the instance RBAC are both verified and fresh.
func (g Gate) Check() error {
	if g.Verifier == nil {
		return fmt.Errorf("%w: no readiness check is running (audit mode, or it is not configured)", ErrMissing)
	}
	s := g.Verifier.Status()
	switch {
	case s.Verified():
		return nil
	case s.LastCanaryAt.IsZero() && s.LastRBACAt.IsZero():
		return fmt.Errorf("%w: the check has not produced a result yet", ErrMissing)
	case s.Stale:
		return fmt.Errorf("%w: %s", ErrStale, s.Reason)
	default:
		return fmt.Errorf("%w: %s", ErrNotVerified, s.Reason)
	}
}

// Allowed reports whether Check passes.
func (g Gate) Allowed() bool { return g.Check() == nil }

// Checkable is anything that can say whether the rollout is open. Gate and the rollout gate (which adds the inventory
// clearance) both implement it.
type Checkable interface {
	Check() error
}

// Open reports whether g allows the rollout. A nil g refuses: the zero value of a gate field is closed.
func Open(g Checkable) bool {
	return g != nil && g.Check() == nil
}

// CheckOrMissing returns g.Check(), or ErrMissing for a nil g.
func CheckOrMissing(g Checkable) error {
	if g == nil {
		return fmt.Errorf("%w: no rollout gate is configured", ErrMissing)
	}
	return g.Check()
}
