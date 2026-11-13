/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/

package webhook

import (
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// nvcaEnvPatchValues collects the env vars a patch list would add to a
// container, keyed by name. Decoded through JSON so the assertion does not
// depend on whether the patch carries a corev1.EnvVar by value or pointer.
func nvcaEnvPatchValues(patches []PatchOp) map[string]string {
	out := map[string]string{}
	for _, p := range patches {
		if !strings.HasSuffix(p.Path, "/env/-") {
			continue
		}
		b, err := json.Marshal(p.Value)
		if err != nil {
			continue
		}
		var ev struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		}
		if json.Unmarshal(b, &ev) != nil || ev.Name == "" {
			continue
		}
		out[ev.Name] = ev.Value
	}
	return out
}

// nvbugs#6942082. NVSnap overriding HF_HOME/NIM_CACHE_PATH is intended for a
// pod that would otherwise download its model; cachedir.go states the
// exception itself -- "a model already served from a cluster model cache
// (NVCA rewrites the pod's model volume to its read-only claim) is left
// alone" (modelServedFromClusterCache). That opt-out recognises exactly one
// hard-coded volume name, "model-data", so an NVCA function whose model-cache
// claim is mounted under another name -- the Dynamo worker's "model-cache"
// PVC at /opt/models -- is not recognised, and the cachedir inject moves the
// model path to <CacheDir>/model while the bound PVC stays mounted and unused.
//
// Correct behaviour at this boundary: when the pod's own spec already points
// HF_HOME / NIM_CACHE_PATH at a PersistentVolumeClaim-backed mount, the
// cachedir inject must leave those two values alone.
func TestCacheDirRespectsNVCAModelCachePVC(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "agg-worker-0",
			Namespace: "sr-841c8430-cf86-4be9-81ec-7fd5be36bece",
			Labels:    map[string]string{CaptureLabel: "true"},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "model-cache",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "model-cache",
					},
				},
			}},
			Containers: []corev1.Container{{
				Name:  "main",
				Image: "dynamo-worker:latest",
				Env: []corev1.EnvVar{
					{Name: "HF_HOME", Value: "/opt/models"},
					{Name: "NIM_CACHE_PATH", Value: "/opt/models"},
				},
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "model-cache",
					MountPath: "/opt/models",
				}},
			}},
		},
	}

	m := &Mutator{CacheDir: "/opt/nvsnap", MainContainer: 0}
	env := nvcaEnvPatchValues(m.cacheDirCapturePatches(pod))

	for _, name := range []string{"HF_HOME", "NIM_CACHE_PATH"} {
		if got, ok := env[name]; ok && got != "/opt/models" {
			t.Errorf("webhook rewrote %s to %q; the pod's model-cache PVC is mounted at /opt/models, so the model path must stay there", name, got)
		}
	}
}

// Control: a pod with no claim-backed model path is still decorated, so the
// assertion above is about the model-cache PVC and not about cachedir mode
// being inert in this configuration.
func TestCacheDirStillInjectsWithoutModelCachePVC(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "n",
			Labels:    map[string]string{CaptureLabel: "true"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "img"}},
		},
	}
	m := &Mutator{CacheDir: "/opt/nvsnap", MainContainer: 0}
	if env := nvcaEnvPatchValues(m.cacheDirCapturePatches(pod)); env["HF_HOME"] != "/opt/nvsnap/model" {
		t.Fatalf("HF_HOME = %q, want /opt/nvsnap/model (cachedir inject must still run here)", env["HF_HOME"])
	}
}
