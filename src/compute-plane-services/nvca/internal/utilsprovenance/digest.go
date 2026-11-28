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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// PodSpecDigest is the hash of a whole pod spec. NVCA records it for the template of an HA utils Deployment it created.
// A Deployment template and the template of its ReplicaSet are the same spec, so both must match it.
func PodSpecDigest(spec *corev1.PodSpec) string {
	raw, err := json.Marshal(spec)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type containerIdentity struct {
	Init    bool     `json:"init,omitempty"`
	Name    string   `json:"name"`
	Image   string   `json:"image"`
	Command []string `json:"command,omitempty"`
	Args    []string `json:"args,omitempty"`
}

// ContainersDigest is the hash of what every container of a pod runs: its name, image, command and arguments, init
// containers included. It leaves out everything an admission webhook or the scheduler adds to a pod (volumes, node
// name, injected environment), so a pod that came from the recorded template always matches, and a pod whose image or
// command was changed does not.
func ContainersDigest(spec *corev1.PodSpec) string {
	var ids []containerIdentity
	for _, c := range spec.InitContainers {
		ids = append(ids, containerIdentity{Init: true, Name: c.Name, Image: c.Image, Command: c.Command, Args: c.Args})
	}
	for _, c := range spec.Containers {
		ids = append(ids, containerIdentity{Name: c.Name, Image: c.Image, Command: c.Command, Args: c.Args})
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].Init != ids[j].Init {
			return ids[i].Init
		}
		return ids[i].Name < ids[j].Name
	})
	raw, err := json.Marshal(ids)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// DeploymentSpecDigest is the hash of a Deployment spec without its template: replicas, selector, strategy, minimum
// ready seconds, paused and the progress deadline. The replica count and the rolling strategy decide how many utils
// pods can run, so they are part of the desired state, next to the pod spec.
func DeploymentSpecDigest(spec *appsv1.DeploymentSpec) string {
	c := spec.DeepCopy()
	c.Template = corev1.PodTemplateSpec{}
	raw, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
