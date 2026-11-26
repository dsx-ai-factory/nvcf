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
	"fmt"
	"reflect"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/core"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/common"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/function"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	nvcametrics "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/util/k8sutil"
	nvcastorage "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/storage"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

var (
	defaultContainerNamesToWatch = map[string]any{
		"init":                             nil,
		"ess":                              nil,
		"ess-init":                         nil,
		common.UtilsContainerName:          nil,
		function.LLMWorkerContainerName:    nil,
		nvcastorage.SMBServerContainerName: nil,
	}
)

func GetDefaultWorkloadContainerNamesToWatch() []string {
	var containerNames []string
	for k := range defaultContainerNamesToWatch {
		containerNames = append(containerNames, k)
	}
	return containerNames
}

type workloadWatcher func(ctx context.Context, oldPod, newPod *corev1.Pod)

func watchForPodCrashesRestarts(ctx context.Context, oldPod, newPod *corev1.Pod) {
	_, hasICMSRequestLabel := newPod.Labels[nvcatypes.ICMSRequestIDKey]
	if hasICMSRequestLabel &&
		(len(newPod.Status.ContainerStatuses) == 0 ||
			!(newPod.Name == common.UtilsPodName ||
				newPod.Name == nvcastorage.SMBServerPodName ||
				newPod.Namespace == "nvcf-backend")) {
		return
	}

	podNamespacedName := fmt.Sprintf("%s/%s", newPod.Namespace, newPod.Name)
	reportPodCrashAndRestartMetrics(ctx, oldPod.Status.InitContainerStatuses, newPod.Status.InitContainerStatuses, podNamespacedName)
	reportPodCrashAndRestartMetrics(ctx, oldPod.Status.ContainerStatuses, newPod.Status.ContainerStatuses, podNamespacedName)
}

// recordWorkerRestartScore returns a workloadWatcher that records the NVCA worker container restart score on its
// function Pod when the worker's RestartCount changes. A failed patch loses nothing: the score is computed from the
// last recorded score plus the restarts since, and the next Pod update records it again.
func recordWorkerRestartScore(client kubernetes.Interface, k8sTimeConfig *k8sutil.TimeConfig) workloadWatcher {
	return func(ctx context.Context, _, newPod *corev1.Pod) {
		if _, ok := newPod.Labels[nvcatypes.ICMSRequestIDKey]; !ok {
			return
		}
		score, ok := k8sutil.WorkerRestartScoreToRecord(newPod, time.Now(), k8sTimeConfig)
		if !ok {
			return
		}
		patch, err := json.Marshal(map[string]any{
			"metadata": map[string]any{"annotations": map[string]string{k8sutil.WorkerRestartScoreAnnotationKey: score}},
		})
		if err != nil {
			return
		}
		_, err = client.CoreV1().Pods(newPod.Namespace).Patch(ctx, newPod.Name, apitypes.MergePatchType, patch,
			metav1.PatchOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			core.GetLogger(ctx).WithError(err).Warnf("failed to record the worker restart score on pod %s/%s",
				newPod.Namespace, newPod.Name)
		}
	}
}

func reportPodCrashAndRestartMetrics(ctx context.Context,
	oldContainerStatuses []corev1.ContainerStatus,
	newContainerStatuses []corev1.ContainerStatus,
	podNamespacedName string) {
	log := core.GetLogger(ctx)

	// Build a map of the oldPod container statuses for lookup
	oldContainerStatusesByName := map[string]corev1.ContainerStatus{}
	for _, cs := range oldContainerStatuses {
		oldContainerStatusesByName[cs.Name] = cs
	}

	// Iterate through the newPod container statuses and check if any container has restarted or crashed
	for _, cs := range newContainerStatuses {
		// Skip if not in the set of containers we want to watch
		if _, ok := defaultContainerNamesToWatch[cs.Name]; !ok {
			continue // Skip if not in the set of containers we want to watch
		}

		if cs.RestartCount > 0 {
			// Check if the container was not restarted in the oldPod
			if oldCs, ok := oldContainerStatusesByName[cs.Name]; !ok || oldCs.RestartCount < cs.RestartCount {
				log.Debugf("Container %s in pod %s has restarted %d times", cs.Name, podNamespacedName, cs.RestartCount)
				metrics := nvcametrics.FromContext(ctx)
				metrics.ContainerRestartTotal.WithLabelValues(metrics.WithDefaultLabelValues(cs.Name)...).Add(float64(cs.RestartCount - oldCs.RestartCount))
			}
		}

		// Check if the last state is terminated, and if that matches the newpod last state
		if cs.LastTerminationState.Terminated != nil && cs.LastTerminationState.Terminated.ExitCode != 0 {
			// Check if the container was not restarted in the oldPod, and diff the two to see if there is a delta, if not we should increment
			if oldCs, ok := oldContainerStatusesByName[cs.Name]; !ok ||
				!reflect.DeepEqual(oldCs.LastTerminationState.Terminated, cs.LastTerminationState.Terminated) {
				log.Debugf("Container %s in pod %s has terminated with exit code %d", cs.Name, podNamespacedName, cs.LastTerminationState.Terminated.ExitCode)
				metrics := nvcametrics.FromContext(ctx)
				metrics.ContainerCrashTotal.WithLabelValues(metrics.WithDefaultLabelValues(cs.Name)...).Inc()
			}
		}
	}
}
