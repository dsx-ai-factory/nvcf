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
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func specWith(image string, env ...corev1.EnvVar) *corev1.PodSpec {
	return &corev1.PodSpec{
		InitContainers: []corev1.Container{{Name: "init", Image: "init:1", Command: []string{"/init"}}},
		Containers:     []corev1.Container{{Name: "utils", Image: image, Command: []string{"/utils"}, Args: []string{"--a"}, Env: env}},
	}
}

func TestPodSpecDigestChangesWithAnyField(t *testing.T) {
	base := PodSpecDigest(specWith("utils:1"))
	assert.NotEmpty(t, base)
	assert.Equal(t, base, PodSpecDigest(specWith("utils:1")), "stable")
	assert.NotEqual(t, base, PodSpecDigest(specWith("utils:2")), "image")
	assert.NotEqual(t, base, PodSpecDigest(specWith("utils:1", corev1.EnvVar{Name: "X", Value: "1"})), "environment")
}

func TestContainersDigestIgnoresWhatAdmissionAddsButNotWhatRuns(t *testing.T) {
	base := ContainersDigest(specWith("utils:1"))
	withInjected := specWith("utils:1", corev1.EnvVar{Name: "INJECTED", Value: "x"})
	withInjected.NodeName = "node-1"
	withInjected.Volumes = []corev1.Volume{{Name: "v"}}
	assert.Equal(t, base, ContainersDigest(withInjected), "env, node and volumes are added by admission and the scheduler")

	changedImage := specWith("evil:1")
	assert.NotEqual(t, base, ContainersDigest(changedImage), "image")
	changedCmd := specWith("utils:1")
	changedCmd.Containers[0].Command = []string{"/bin/sh"}
	assert.NotEqual(t, base, ContainersDigest(changedCmd), "command")
	changedArgs := specWith("utils:1")
	changedArgs.Containers[0].Args = []string{"--b"}
	assert.NotEqual(t, base, ContainersDigest(changedArgs), "args")
	changedInit := specWith("utils:1")
	changedInit.InitContainers[0].Image = "init:2"
	assert.NotEqual(t, base, ContainersDigest(changedInit), "init container image")
	extra := specWith("utils:1")
	extra.Containers = append(extra.Containers, corev1.Container{Name: "sidecar", Image: "x"})
	assert.NotEqual(t, base, ContainersDigest(extra), "an added container")
}
