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

package webhook

import (
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// modelCacheEnvFromPatches collects the container env a patch plan ends up
// with: later entries win, which is what kubelet does with a duplicate env
// name, so this is the value the workload actually sees.
func modelCacheEnvFromPatches(t *testing.T, patches []PatchOp) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, p := range patches {
		if !strings.Contains(p.Path, "/env") {
			continue
		}
		b, err := json.Marshal(p.Value)
		if err != nil {
			t.Fatalf("marshal patch value for %s: %v", p.Path, err)
		}
		var one corev1.EnvVar
		if err := json.Unmarshal(b, &one); err == nil && one.Name != "" {
			out[one.Name] = one.Value
			continue
		}
		var many []corev1.EnvVar
		if err := json.Unmarshal(b, &many); err == nil {
			for _, e := range many {
				if e.Name != "" {
					out[e.Name] = e.Value
				}
			}
		}
	}
	return out
}

// dynamoModelCacheWorkerPod is the shape the Dynamo aggregated smoke case
// deploys: one combined worker whose model-cache PVC is mounted at
// /opt/models, with HF_HOME and NIM_CACHE_PATH set by the DGD to that same
// path so downloads and reads land on the claim.
func dynamoModelCacheWorkerPod(modelVolumeName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "agg-worker-0",
			Namespace: "sr-841c8430-cf86-4be9-81ec-7fd5be36bece",
			Labels:    map[string]string{CaptureLabel: "true"},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: modelVolumeName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "model-cache",
					},
				},
			}},
			Containers: []corev1.Container{{
				Name:  "main",
				Image: "dynamo-worker:test",
				Env: []corev1.EnvVar{
					{Name: "HF_HOME", Value: "/opt/models"},
					{Name: "NIM_CACHE_PATH", Value: "/opt/models"},
				},
				VolumeMounts: []corev1.VolumeMount{{
					Name:      modelVolumeName,
					MountPath: "/opt/models",
				}},
			}},
		},
	}
}

// A worker whose model cache is a mounted PVC must keep the cache paths the
// deployment asked for. Rewriting them to the cachedir ({model} =
// <podCacheDir>/model) leaves the PVC mounted and unused, and HF downloads
// bypass it (nvbugs#6942082).
func TestCacheDirCapturePatchesKeepsModelCachePVCEnv(t *testing.T) {
	m := &Mutator{CacheDir: "/opt/nvsnap", MainContainer: 0}
	pod := dynamoModelCacheWorkerPod("model-cache")

	env := modelCacheEnvFromPatches(t, m.cacheDirCapturePatches(pod))

	for _, name := range []string{"HF_HOME", "NIM_CACHE_PATH"} {
		got, injected := env[name]
		if !injected {
			continue // left as the deployment set it — correct
		}
		if got != "/opt/models" {
			t.Errorf("worker %s is %q, want %q (HF downloads would bypass the model-cache PVC mounted there)",
				name, got, "/opt/models")
		}
	}
}

// The guard that already exists: a model served from the cluster model cache
// (NVCA's model-data claim) is left entirely alone. Pins that semantic so a
// fix for the case above does not quietly change it.
func TestCacheDirCapturePatchesSkipsNVCAModelDataPVC(t *testing.T) {
	m := &Mutator{CacheDir: "/opt/nvsnap", MainContainer: 0}
	pod := dynamoModelCacheWorkerPod(nvcaModelVolumeName)

	if patches := m.cacheDirCapturePatches(pod); len(patches) != 0 {
		t.Errorf("pod whose model comes from the cluster model cache got %d patches, want 0", len(patches))
	}
}
