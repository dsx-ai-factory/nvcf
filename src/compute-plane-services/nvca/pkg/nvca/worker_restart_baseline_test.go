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

package nvca

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakek8sclient "k8s.io/client-go/kubernetes/fake"
	listersv1 "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/common"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/util/k8sutil"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

func TestRecordWorkerRestartBaselines(t *testing.T) {
	now := time.Now()
	newPod := func(name string, labels map[string]string, restarts int32, runFor time.Duration) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "nvcf-backend", Labels: labels},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: common.UtilsContainerName,
				Env:  []corev1.EnvVar{{Name: k8sutil.InstanceIDEnvKey, Value: "instance-1"}},
			}}},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
				Type:               corev1.PodReady,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(now.Add(-runFor)),
			}}, ContainerStatuses: []corev1.ContainerStatus{{
				Name:         common.UtilsContainerName,
				RestartCount: restarts,
				Ready:        true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{
					StartedAt: metav1.NewTime(now.Add(-runFor)),
				}},
			}}},
		}
	}
	reqLabels := map[string]string{nvcatypes.ICMSRequestIDKey: "req-1"}
	healthy := newPod("healthy", reqLabels, 4, time.Hour)
	recent := newPod("recent", reqLabels, 4, 5*time.Minute)
	unrelated := newPod("unrelated", nil, 4, time.Hour)

	client := fakek8sclient.NewSimpleClientset(healthy, recent, unrelated)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, p := range []*corev1.Pod{healthy, recent, unrelated} {
		require.NoError(t, indexer.Add(p))
	}

	recordWorkerRestartBaselines(context.Background(), client, listersv1.NewPodLister(indexer), now)

	got := map[string]string{}
	for _, name := range []string{"healthy", "recent", "unrelated"} {
		p, err := client.CoreV1().Pods("nvcf-backend").Get(context.Background(), name, metav1.GetOptions{})
		require.NoError(t, err)
		got[name] = p.Annotations[k8sutil.WorkerRestartCountBaselineAnnotationKey]
	}
	assert.Equal(t, map[string]string{"healthy": "4", "recent": "", "unrelated": ""}, got)

	t.Run("pod update records a long run that just ended", func(t *testing.T) {
		pod := newPod("crashed", reqLabels, 3, time.Minute)
		pod.Status.ContainerStatuses[0].State = corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
		}
		pod.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				ExitCode:   2,
				StartedAt:  metav1.NewTime(now.Add(-2 * time.Hour)),
				FinishedAt: metav1.NewTime(now.Add(-time.Minute)),
			},
		}
		client := fakek8sclient.NewSimpleClientset(pod)

		recordWorkerRestartBaseline(context.Background(), client, pod, now)

		got, err := client.CoreV1().Pods("nvcf-backend").Get(context.Background(), "crashed", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, "3", got.Annotations[k8sutil.WorkerRestartCountBaselineAnnotationKey])
	})

	t.Run("failed patch keeps the baseline after a short run replaces the long one", func(t *testing.T) {
		pod := newPod("patch-fails", reqLabels, 2, time.Minute)
		pod.UID = "patch-fails-uid"
		pod.CreationTimestamp = metav1.NewTime(now.Add(-3 * time.Hour))
		t.Cleanup(func() { k8sutil.ForgetWorkerRestartBaseline(pod.UID) })
		pod.Status.ContainerStatuses[0].State = corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
		}
		pod.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				ExitCode:   2,
				StartedAt:  metav1.NewTime(now.Add(-2 * time.Hour)),
				FinishedAt: metav1.NewTime(now.Add(-time.Minute)),
			},
		}
		client := fakek8sclient.NewSimpleClientset(pod)
		client.PrependReactor("patch", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("webhook unavailable")
		})

		recordWorkerRestartBaseline(context.Background(), client, pod, now)

		// The next run crashes quickly, so only that short run is left in the termination state.
		crashed := pod.DeepCopy()
		crashed.Status.ContainerStatuses[0].RestartCount = 4
		crashed.Status.ContainerStatuses[0].LastTerminationState.Terminated.StartedAt = metav1.NewTime(now.Add(-time.Minute))
		cfg := (&k8sutil.TimeConfig{WorkerContainerRestartCountToFailInstance: 3}).Complete()
		assert.False(t, k8sutil.IsContainerInRestartLoop(crashed, crashed.Status.ContainerStatuses[0], cfg))

		client.ReactionChain = client.ReactionChain[1:]
		recordWorkerRestartBaseline(context.Background(), client, crashed, now)
		got, err := client.CoreV1().Pods("nvcf-backend").Get(context.Background(), "patch-fails", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, "2", got.Annotations[k8sutil.WorkerRestartCountBaselineAnnotationKey])

		// Once the Pod shows the recorded baseline, the retained one is dropped and the annotation is used.
		recorded := crashed.DeepCopy()
		recorded.Annotations = got.Annotations
		_, ok := k8sutil.WorkerRestartCountBaselineToRecord(recorded, now)
		assert.False(t, ok)
		assert.False(t, k8sutil.IsContainerInRestartLoop(recorded, recorded.Status.ContainerStatuses[0], cfg))
		recorded.Annotations = nil
		assert.True(t, k8sutil.IsContainerInRestartLoop(recorded, recorded.Status.ContainerStatuses[0], cfg))
	})
}
