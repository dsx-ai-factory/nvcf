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

package k8serr

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestIsTransientCSIMountPathErrors pins the NVMesh first-provision CSI mount
// failures reported in nvbugs#6471949: kubelet surfaces them as plain strings
// while mkfs.xfs runs on a freshly provisioned container-cache volume, they
// clear on retry once the filesystem exists, and so must be requeued quietly
// rather than escalated as hard reconcile failures. Permanent CSI rejections
// must stay non-transient.
func TestIsTransientCSIMountPathErrors(t *testing.T) {
	const vol = "pvc-cc401d5b-7bb2-400b-9e10-8f15c5c50efd"

	deadline := fmt.Errorf("MountVolume.MountDevice failed for volume %q: "+
		"rpc error: code = DeadlineExceeded desc = context deadline exceeded", vol)
	busy := fmt.Errorf("MountVolume.MountDevice failed for volume %q: "+
		"rpc error: code = Internal desc = mkfs failed 1  mkfs.xfs: cannot open "+
		"/dev/nvmesh/csi-cc401d5b-7bb2-400b: Device or resource busy", vol)

	tests := []struct {
		name      string
		err       error
		transient bool
	}{
		{name: "mount device deadline exceeded", err: deadline, transient: true},
		{name: "wrapped mount device deadline exceeded", err: fmt.Errorf("mounting cache volume: %w", deadline), transient: true},
		{name: "mkfs device or resource busy", err: busy, transient: true},
		{name: "wrapped mkfs device or resource busy", err: fmt.Errorf("mounting cache volume: %w", busy), transient: true},
		{
			name: "invalid argument",
			err: fmt.Errorf("MountVolume.MountDevice failed for volume %q: "+
				"rpc error: code = InvalidArgument desc = volume capability not supported", vol),
			transient: false,
		},
		{
			name: "permission denied",
			err: fmt.Errorf("MountVolume.MountDevice failed for volume %q: "+
				"rpc error: code = PermissionDenied desc = not authorized", vol),
			transient: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.transient, IsTransient(tt.err))
		})
	}
}

func TestIsTransient(t *testing.T) {
	gr := schema.GroupResource{Group: "test", Resource: "things"}

	tests := []struct {
		name      string
		err       error
		transient bool
	}{
		{
			name:      "nil error",
			err:       nil,
			transient: false,
		},
		{
			name:      "timeout",
			err:       apierrors.NewTimeoutError("timed out", 5),
			transient: true,
		},
		{
			name:      "server timeout",
			err:       apierrors.NewServerTimeout(gr, "get", 5),
			transient: true,
		},
		{
			name:      "too many requests",
			err:       apierrors.NewTooManyRequests("slow down", 5),
			transient: true,
		},
		{
			name:      "service unavailable",
			err:       apierrors.NewServiceUnavailable("unavailable"),
			transient: true,
		},
		{
			name:      "internal error",
			err:       apierrors.NewInternalError(fmt.Errorf("internal")),
			transient: true,
		},
		{
			name:      "conflict",
			err:       apierrors.NewConflict(gr, "thing", fmt.Errorf("conflict")),
			transient: true,
		},
		{
			name:      "context deadline exceeded",
			err:       context.DeadlineExceeded,
			transient: true,
		},
		{
			name:      "context canceled",
			err:       context.Canceled,
			transient: true,
		},
		{
			name:      "wrapped context deadline",
			err:       fmt.Errorf("getting pod: %w", context.DeadlineExceeded),
			transient: true,
		},
		{
			name:      "wrapped timeout",
			err:       fmt.Errorf("getting pod: %w", apierrors.NewTimeoutError("timed out", 5)),
			transient: true,
		},
		{
			name:      "net error",
			err:       &net.OpError{Op: "dial", Err: fmt.Errorf("connection refused")},
			transient: true,
		},
		// Non-transient errors
		{
			name:      "forbidden",
			err:       apierrors.NewForbidden(gr, "thing", fmt.Errorf("forbidden")),
			transient: false,
		},
		{
			name:      "unauthorized",
			err:       apierrors.NewUnauthorized("unauthorized"),
			transient: false,
		},
		{
			name:      "not found",
			err:       apierrors.NewNotFound(gr, "thing"),
			transient: false,
		},
		{
			name:      "invalid",
			err:       apierrors.NewInvalid(schema.GroupKind{Group: "test", Kind: "Thing"}, "thing", nil),
			transient: false,
		},
		{
			name:      "gone",
			err:       apierrors.NewGone("gone"),
			transient: false,
		},
		{
			name:      "generic error",
			err:       fmt.Errorf("something unexpected"),
			transient: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.transient, IsTransient(tt.err))
		})
	}
}

// TestIsTransientNVMeshFirstProvisionMountErrors covers the errors the NVMesh
// CSI node plugin surfaces through kubelet while it creates the XFS filesystem
// on a freshly provisioned container-cache volume (nvbugs#6471949). They are
// expected and transient - the volume comes up clean once mkfs finishes - so
// this classifier must not report them as hard failures, which is what makes a
// caller surface them as reconcile failures instead of a quiet requeue.
func TestIsTransientNVMeshFirstProvisionMountErrors(t *testing.T) {
	const vol = "pvc-cc401d5b-7bb2-400b-9e10-8f15c5c50efd"

	deadlineErr := fmt.Errorf(
		"MountVolume.MountDevice failed for volume %q: "+
			"rpc error: code = DeadlineExceeded desc = context deadline exceeded", vol)

	busyErr := fmt.Errorf(
		"MountVolume.MountDevice failed for volume %q: "+
			"rpc error: code = Internal desc = mkfs failed 1  mkfs.xfs: cannot open "+
			"/dev/nvmesh/csi-cc401d5b-7bb2-400b: Device or resource busy", vol)

	// Same message seen one wrapping level deeper, as a caller propagates it.
	wrappedBusyErr := fmt.Errorf("mounting model cache volume: %w", busyErr)

	assert.True(t, IsTransient(deadlineErr),
		"NVMesh first-provision MountDevice DeadlineExceeded must be transient, got hard failure")
	assert.True(t, IsTransient(busyErr),
		"NVMesh mkfs.xfs 'Device or resource busy' during filesystem creation must be transient, got hard failure")
	assert.True(t, IsTransient(wrappedBusyErr),
		"wrapped NVMesh mkfs.xfs 'Device or resource busy' must be transient, got hard failure")

	// Edge cases that must stay non-transient: a permanent CSI rejection and a
	// capability mismatch never resolve on retry.
	permanentErrs := []struct {
		name string
		err  error
	}{
		{
			name: "invalid argument",
			err: fmt.Errorf("MountVolume.MountDevice failed for volume %q: "+
				"rpc error: code = InvalidArgument desc = volume capability not supported", vol),
		},
		{
			name: "permission denied",
			err: fmt.Errorf("MountVolume.MountDevice failed for volume %q: "+
				"rpc error: code = PermissionDenied desc = not authorized", vol),
		},
	}
	for _, tt := range permanentErrs {
		t.Run(tt.name, func(t *testing.T) {
			assert.False(t, IsTransient(tt.err),
				"permanent CSI error must not be suppressed as transient")
		})
	}
}

// TestIsTransientNVMeshFirstProvisionMountErrors covers the errors the NVMesh
// CSI node plugin surfaces through kubelet while it creates the XFS filesystem
// on a freshly provisioned container-cache volume (nvbugs#6471949). They are
// expected and transient - the volume comes up clean once mkfs finishes - so
// this classifier must not report them as hard failures, which is what makes a
// caller surface them as reconcile failures instead of a quiet requeue.
func TestIsTransientNVMeshFirstProvisionMountErrors(t *testing.T) {
	const vol = "pvc-cc401d5b-7bb2-400b-9e10-8f15c5c50efd"

	deadlineErr := fmt.Errorf(
		"MountVolume.MountDevice failed for volume %q: "+
			"rpc error: code = DeadlineExceeded desc = context deadline exceeded", vol)

	busyErr := fmt.Errorf(
		"MountVolume.MountDevice failed for volume %q: "+
			"rpc error: code = Internal desc = mkfs failed 1  mkfs.xfs: cannot open "+
			"/dev/nvmesh/csi-cc401d5b-7bb2-400b: Device or resource busy", vol)

	// Same message seen one wrapping level deeper, as a caller propagates it.
	wrappedBusyErr := fmt.Errorf("mounting model cache volume: %w", busyErr)

	assert.True(t, IsTransient(deadlineErr),
		"NVMesh first-provision MountDevice DeadlineExceeded must be transient, got hard failure")
	assert.True(t, IsTransient(busyErr),
		"NVMesh mkfs.xfs 'Device or resource busy' during filesystem creation must be transient, got hard failure")
	assert.True(t, IsTransient(wrappedBusyErr),
		"wrapped NVMesh mkfs.xfs 'Device or resource busy' must be transient, got hard failure")

	// Edge cases that must stay non-transient: a permanent CSI rejection and a
	// capability mismatch never resolve on retry.
	permanentErrs := []struct {
		name string
		err  error
	}{
		{
			name: "invalid argument",
			err: fmt.Errorf("MountVolume.MountDevice failed for volume %q: "+
				"rpc error: code = InvalidArgument desc = volume capability not supported", vol),
		},
		{
			name: "permission denied",
			err: fmt.Errorf("MountVolume.MountDevice failed for volume %q: "+
				"rpc error: code = PermissionDenied desc = not authorized", vol),
		},
	}
	for _, tt := range permanentErrs {
		t.Run(tt.name, func(t *testing.T) {
			assert.False(t, IsTransient(tt.err),
				"permanent CSI error must not be suppressed as transient")
		})
	}
}
