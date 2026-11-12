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

// Package k8serr classifies Kubernetes API errors. It imports only
// apimachinery and the standard library, so a small binary such as the
// cluster-validator can use it without the dependencies of k8sutil.
package k8serr

import (
	"context"
	"errors"
	"net"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// IsTransient returns true for Kubernetes API errors that are expected to
// resolve on retry without human intervention. Use this to decide whether to
// return a requeue (nil error) vs surfacing the error as a reconcile failure.
//
// Transient errors include: timeouts, rate limiting (429), service unavailable (503),
// internal server errors (500), conflicts (409), network-level errors, and context
// cancellation/deadline exceeded, plus CSI mount-path errors that are expected while
// a volume is first provisioned (DeadlineExceeded, or "device or resource busy" while
// mkfs runs).
//
// Non-transient errors that should be surfaced as reconcile failures include:
// Forbidden (403), Unauthorized (401), Invalid (422), Gone (410), etc.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}

	// API server transient responses
	if apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err) ||
		apierrors.IsInternalError(err) || apierrors.IsConflict(err) {
		return true
	}

	// Network-level transient errors (connection refused, reset, DNS, timeout, etc.)
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// CSI mount-path errors seen while a volume is still being provisioned
	if isTransientCSIMountError(err) {
		return true
	}

	// Context cancellation / deadline exceeded
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// isTransientCSIMountError reports whether err is a CSI mount failure that is
// expected to clear on retry while a volume is still being provisioned, such as
// NVMesh creating the XFS filesystem on a freshly provisioned volume. The kubelet
// volume manager surfaces these as plain strings rather than typed errors, so they
// are matched on message content; that also covers wrapped errors, since wrapping
// preserves the message.
func isTransientCSIMountError(err error) bool {
	msg := err.Error()
	if !strings.Contains(msg, "rpc error: code = ") {
		return false
	}

	return strings.Contains(msg, "code = DeadlineExceeded") ||
		strings.Contains(strings.ToLower(msg), "device or resource busy")
}
