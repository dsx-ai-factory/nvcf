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
	"encoding/json"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/common"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/function"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	nvcametrics "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/util/k8sutil"
	nvcastorage "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/storage"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

func TestRecordWorkerRestartScore(t *testing.T) {
	newPod := func(labels map[string]string, restarts int32) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "fn-pod", Namespace: "nvcf-backend", Labels: labels},
			Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: common.UtilsContainerName, Env: []corev1.EnvVar{{Name: k8sutil.InstanceIDEnvKey, Value: "instance-1"}}},
			}},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
				{Name: common.UtilsContainerName, RestartCount: restarts},
			}},
		}
	}
	functionPod := map[string]string{nvcatypes.ICMSRequestIDKey: "req-1"}

	tests := []struct {
		name      string
		pod       *corev1.Pod
		wantScore *k8sutil.WorkerRestartScore
	}{
		{name: "worker restart is recorded", pod: newPod(functionPod, 2), wantScore: &k8sutil.WorkerRestartScore{Score: 2, RestartCount: 2}},
		{name: "worker without restarts is not patched", pod: newPod(functionPod, 0)},
		{name: "non-function pod is not patched", pod: newPod(nil, 2)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			client := k8sfake.NewClientset(tt.pod)
			recordWorkerRestartScore(client, (&k8sutil.TimeConfig{}).Complete())(ctx, tt.pod, tt.pod)

			got, err := client.CoreV1().Pods(tt.pod.Namespace).Get(ctx, tt.pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			value, ok := got.Annotations[k8sutil.WorkerRestartScoreAnnotationKey]
			if tt.wantScore == nil {
				assert.False(t, ok, "pod should not be patched")
				return
			}
			require.True(t, ok)
			var score k8sutil.WorkerRestartScore
			require.NoError(t, json.Unmarshal([]byte(value), &score))
			assert.Equal(t, tt.wantScore.Score, score.Score)
			assert.Equal(t, tt.wantScore.RestartCount, score.RestartCount)
		})
	}

	t.Run("deleted pod is ignored", func(t *testing.T) {
		pod := newPod(functionPod, 2)
		recordWorkerRestartScore(k8sfake.NewClientset(), (&k8sutil.TimeConfig{}).Complete())(context.Background(), pod, pod)
	})
}

func TestGetDefaultWorkloadContainerNamesToWatch(t *testing.T) {
	tests := []struct {
		name string
		want []string
	}{
		{
			name: "default",
			want: []string{
				"init",
				"ess",
				"ess-init",
				common.UtilsContainerName,
				nvcastorage.SMBServerContainerName,
				function.LLMWorkerContainerName,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetDefaultWorkloadContainerNamesToWatch()
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestReportPodCrashAndRestartMetrics(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	ctx = nvcametrics.WithDefaultMetrics(ctx,
		"my-nca-id", "my-cluster", "my-cluster", "1.0.0",
		nvcametrics.WithEventErrorTotalDefaultEvents(append(getAgentEvents(), getNVCAMetricEvents()...)),
		nvcametrics.WithContainerCrashAndRestartTotalDefaultContainerNames(GetDefaultWorkloadContainerNamesToWatch()),
		nvcametrics.WithRegisterer(reg))
	metrics := nvcametrics.FromContext(ctx)

	oldContainerStatuses := []corev1.ContainerStatus{
		{
			Name:         common.UtilsContainerName,
			RestartCount: 1,
			LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 0,
				},
			},
		},
	}

	newContainerStatuses := []corev1.ContainerStatus{
		{
			Name:         common.UtilsContainerName,
			RestartCount: 3,
			LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 1,
				},
			},
		},
	}

	podNamespacedName := "namespace/pod"

	t.Run("Test container restart and crash", func(t *testing.T) {
		metrics.ContainerCrashTotal.Reset()
		metrics.ContainerRestartTotal.Reset()
		reportPodCrashAndRestartMetrics(ctx, oldContainerStatuses, newContainerStatuses, podNamespacedName)
		assert.Equal(t, 1, promtestutil.CollectAndCount(metrics.ContainerCrashTotal))
		assert.Equal(t, 1, promtestutil.CollectAndCount(metrics.ContainerRestartTotal))
	})

	t.Run("Test no restart or crash", func(t *testing.T) {
		metrics.ContainerCrashTotal.Reset()
		metrics.ContainerRestartTotal.Reset()
		newContainerStatuses[0].RestartCount = 1
		newContainerStatuses[0].LastTerminationState.Terminated.ExitCode = 0
		reportPodCrashAndRestartMetrics(ctx, oldContainerStatuses, newContainerStatuses, podNamespacedName)
		assert.Equal(t, 0, promtestutil.CollectAndCount(metrics.ContainerCrashTotal))
		assert.Equal(t, 0, promtestutil.CollectAndCount(metrics.ContainerRestartTotal))
	})
}
