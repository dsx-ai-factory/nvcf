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
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	otelattr "go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/core"

	nvcametrics "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/metrics"
	nvcaotel "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/otel"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/util/k8sutil"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

// workerRestartBaselineInterval is how often NVCA records worker restart baselines. It bounds how late a healthy
// period can be recorded after it ends.
const workerRestartBaselineInterval = time.Minute

// startWorkerRestartBaselineRecorder periodically records, on each function Pod, the NVCA worker container's
// RestartCount once the worker has been healthy for k8sutil.WorkerRestartCountResetPeriod, so earlier restarts stop
// counting toward the worker restart threshold.
func startWorkerRestartBaselineRecorder(ctx context.Context, client kubernetes.Interface, podLister listersv1.PodLister) {
	go func() {
		t := time.NewTicker(workerRestartBaselineInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if ctx.Err() != nil {
					return
				}
				recordWorkerRestartBaselines(ctx, client, podLister, time.Now())
			}
		}
	}()
}

func recordWorkerRestartBaselines(ctx context.Context, client kubernetes.Interface, podLister listersv1.PodLister,
	now time.Time,
) {
	log := core.GetLogger(ctx)
	reqIDExists, err := labels.NewRequirement(nvcatypes.ICMSRequestIDKey, selection.Exists, nil)
	if err != nil {
		log.WithError(err).Error("failed to build the selector for worker restart baselines")
		return
	}
	pods, err := podLister.List(labels.NewSelector().Add(*reqIDExists))
	if err != nil {
		log.WithError(err).Warn("failed to list pods to record worker restart baselines")
		return
	}
	for _, pod := range pods {
		recordWorkerRestartBaseline(ctx, client, pod, now)
	}
}

// recordWorkerRestartBaseline records the worker restart baseline on a function Pod when it changed. It is also
// called on every Pod update, so a healthy run that just ended is detected before the next restart replaces its
// termination state. The detected baseline is retained in memory until the Pod shows it, so the restart check uses
// it and later calls retry the patch with it even if the patch fails.
func recordWorkerRestartBaseline(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, now time.Time) {
	reqID, ok := pod.Labels[nvcatypes.ICMSRequestIDKey]
	if !ok {
		return
	}
	baseline, ok := k8sutil.WorkerRestartCountBaselineToRecord(pod, now)
	if !ok {
		return
	}
	k8sutil.RetainWorkerRestartBaseline(pod, baseline)
	log := core.GetLogger(ctx).WithFields(logrus.Fields{
		"icms_request_id": reqID,
		"namespace":       pod.Namespace,
		"pod":             pod.Name,
		"baseline":        baseline,
	})
	err := nvcaotel.InvokeWithSpan(ctx, nvcaotel.NewTracer(), "nvca.patchWorkerRestartBaseline",
		func(ctx context.Context) error {
			return patchWorkerRestartBaseline(ctx, client, pod, baseline)
		},
		oteltrace.WithSpanKind(oteltrace.SpanKindClient),
		oteltrace.WithAttributes(
			otelattr.String(nvcaotel.ICMSRequestIDAttributeKey, reqID),
			otelattr.String("k8s.namespace.name", pod.Namespace),
			otelattr.String("k8s.pod.name", pod.Name),
			otelattr.Int("nvca.worker.restart_count_baseline", int(baseline))))
	if err != nil {
		log.WithError(err).Warn("failed to record worker restart baseline")
		return
	}
	log.Info("recorded worker restart baseline")
}

func patchWorkerRestartBaseline(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, baseline int32) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				k8sutil.WorkerRestartCountBaselineAnnotationKey: strconv.Itoa(int(baseline)),
			},
		},
	})
	if err != nil {
		return err
	}
	_, err = client.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, apitypes.MergePatchType, patch, metav1.PatchOptions{})
	if metrics := nvcametrics.FromContext(ctx); metrics != nil {
		metrics.TrackK8sAPICall("pod", err)
	}
	return err
}
