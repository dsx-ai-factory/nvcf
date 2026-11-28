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

package mscontroller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/common"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/util/k8sutil"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	nvcav2beta1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v2beta1"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

const (
	// UtilsDeploymentName is the name of the utils Deployment. It matches the name of the bare utils pod: an instance has
	// either the pod (one replica) or the Deployment (HA), never both. Its pods are named utils-<hash>.
	UtilsDeploymentName = common.UtilsPodName

	// maxRequestConcurrencyEnv is the utils container env that limits requests in flight for one utils process.
	maxRequestConcurrencyEnv = "MAX_REQUEST_CONCURRENCY"
)

// ErrUtilsHAConcurrency means the utils pod has no usable concurrency limit to split across replicas.
var ErrUtilsHAConcurrency = errors.New("utils concurrency cannot be split across replicas")

// utilsHAPlan is the outcome of preparing an HA utils Deployment.
type utilsHAPlan struct {
	// Replicas is the number of replicas the Deployment runs. It is below the request when the total concurrency is
	// smaller than the request, so that no replica runs with a limit of zero.
	Replicas int32
	// TotalConcurrency is the instance limit from the deployment, PerReplica the limit each replica gets.
	TotalConcurrency int32
	PerReplica       int32
	// Reason is set when the plan differs from what was asked.
	Reason string
}

// splitUtilsConcurrency divides the total instance concurrency across replicas. Every replica of one Deployment has the
// same template, so each gets floor(total / replicas): the configured total is never exceeded, and rounding never goes
// up (5 over 2 replicas is 2 each, not 3). The worker clamps its limit to at least 1, so when the total is smaller than
// the request the replicas are reduced to the total.
func splitUtilsConcurrency(total, requested int32) (replicas, perReplica int32, reason string) {
	if total < 1 || requested < 1 {
		return 1, max(total, 1), ""
	}
	replicas = requested
	if total < requested {
		replicas = total
		reason = fmt.Sprintf("total concurrency %d is below the requested %d replicas, using %d", total, requested, total)
	}
	perReplica = total / replicas
	if r := total % replicas; r != 0 && reason == "" {
		reason = fmt.Sprintf("total concurrency %d does not divide by %d replicas, each replica gets %d and %d slots stay unused",
			total, replicas, perReplica, r)
	}
	return replicas, perReplica, reason
}

func utilsContainerIndex(spec *corev1.PodSpec) int {
	for i := range spec.Containers {
		if spec.Containers[i].Name == common.UtilsContainerName {
			return i
		}
	}
	return -1
}

// utilsTotalConcurrency reads the total concurrency from the utils container env.
func utilsTotalConcurrency(spec *corev1.PodSpec) (int32, error) {
	i := utilsContainerIndex(spec)
	if i < 0 {
		return 0, fmt.Errorf("%w: no %q container", ErrUtilsHAConcurrency, common.UtilsContainerName)
	}
	for _, e := range spec.Containers[i].Env {
		if e.Name != maxRequestConcurrencyEnv {
			continue
		}
		n, err := strconv.ParseInt(e.Value, 10, 32)
		if err != nil || n < 1 {
			return 0, fmt.Errorf("%w: %s=%q", ErrUtilsHAConcurrency, maxRequestConcurrencyEnv, e.Value)
		}
		return int32(n), nil
	}
	return 0, fmt.Errorf("%w: %s is not set", ErrUtilsHAConcurrency, maxRequestConcurrencyEnv)
}

func setEnv(c *corev1.Container, name, value string) {
	for i := range c.Env {
		if c.Env[i].Name == name {
			c.Env[i].Value, c.Env[i].ValueFrom = value, nil
			return
		}
	}
	c.Env = append(c.Env, corev1.EnvVar{Name: name, Value: value})
}

// utilsSelectorLabels are the labels the utils Deployment selects on. The reserved role label is in the selector, so a
// pod created by a chart, which can never carry that label (the provenance webhook refuses it), can never match: the
// ReplicaSet controller cannot adopt a forged pod and then delete a real utils pod as surplus.
func utilsSelectorLabels(ms *v1alpha1.MiniService) map[string]string {
	return map[string]string{
		nvcatypes.PodRoleLabelKey:      nvcatypes.PodRoleValueUtils,
		nvcatypes.MiniserviceNameLabel: ms.Name,
	}
}

// buildUtilsDeployment builds the HA utils Deployment from the fully prepared utils pod. The pod is the source of truth:
// NVCA has already set its env, resources and volumes, so the Deployment template carries the same init containers,
// containers and volumes, not a re-translated variant. What changes is only what replication needs:
//
//   - the reserved role label in the template and the selector, and a MiniService owner reference (the provenance
//     webhook requires both of NVCA's own Deployment),
//   - MAX_REQUEST_CONCURRENCY on the utils container, split so the instance total holds,
//   - a soft pod anti-affinity so replicas prefer different nodes (a preference, never a requirement),
//   - rolling update with maxSurge 0 and maxUnavailable 1, so old and new pods never exceed the replica count,
//   - no node pinning from the source pod.
//
// The infra annotation and the instance labels are added by the infra mutators when the object is created, as for the
// bare pod. All replicas carry the same instance id, so the control plane sees one instance served by several workers.
func buildUtilsDeployment(ms *v1alpha1.MiniService, pod *corev1.Pod, requested int32) (*appsv1.Deployment, utilsHAPlan, error) {
	total, err := utilsTotalConcurrency(&pod.Spec)
	if err != nil {
		return nil, utilsHAPlan{}, err
	}
	replicas, per, reason := splitUtilsConcurrency(total, requested)
	plan := utilsHAPlan{Replicas: replicas, TotalConcurrency: total, PerReplica: per, Reason: reason}
	if replicas < 2 {
		return nil, plan, nil
	}

	selector := utilsSelectorLabels(ms)
	labels := map[string]string{}
	for k, v := range pod.Labels {
		labels[k] = v
	}
	for k, v := range selector {
		labels[k] = v
	}
	annotations := map[string]string{}
	for k, v := range pod.Annotations {
		annotations[k] = v
	}

	spec := *pod.Spec.DeepCopy()
	spec.NodeName = ""
	setEnv(&spec.Containers[utilsContainerIndex(&spec)], maxRequestConcurrencyEnv, strconv.FormatInt(int64(per), 10))
	if spec.Affinity == nil {
		spec.Affinity = &corev1.Affinity{}
	}
	if spec.Affinity.PodAntiAffinity == nil {
		spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
	}
	spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
		corev1.WeightedPodAffinityTerm{
			Weight: 100,
			PodAffinityTerm: corev1.PodAffinityTerm{
				TopologyKey:   corev1.LabelHostname,
				LabelSelector: &metav1.LabelSelector{MatchLabels: selector},
			},
		})

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        UtilsDeploymentName,
			Namespace:   ms.Spec.Namespace,
			Labels:      labels,
			Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         v1alpha1.SchemeGroupVersion.String(),
				Kind:               "MiniService",
				Name:               ms.Name,
				UID:                ms.UID,
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			}},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas:             ptr.To(replicas),
			RevisionHistoryLimit: ptr.To(int32(2)),
			Selector:             &metav1.LabelSelector{MatchLabels: selector},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxSurge:       ptr.To(intstr.FromInt32(0)),
					MaxUnavailable: ptr.To(intstr.FromInt32(1)),
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
				Spec:       spec,
			},
		},
	}
	return dep, plan, nil
}

// listHAUtilsPods returns the utils pods of an HA instance, found through the ownership chain by UID: the Deployment
// must be owned by ms, the ReplicaSets by that Deployment, the pods by those ReplicaSets. A pod that only carries the
// label, or only names a ReplicaSet, is not returned, so a forged pod cannot be counted as a replica or hide a
// missing one.
func listHAUtilsPods(ctx context.Context, c client.Client, ms *v1alpha1.MiniService) ([]*corev1.Pod, error) {
	dep, err := getOwnedUtilsDeployment(ctx, c, ms)
	if err != nil {
		return nil, err
	}
	var depUID types.UID
	switch rec, recorded := recordedUtilsHA(ms); {
	case dep != nil:
		depUID = dep.UID
	case recorded && rec.DeploymentUID != "":
		// The Deployment is gone, for example during cleanup, but its ReplicaSets and pods may still be terminating.
		// The UID NVCA recorded still identifies them through the ownership chain.
		depUID = rec.DeploymentUID
	default:
		return nil, nil
	}
	rss := &appsv1.ReplicaSetList{}
	if err := c.List(ctx, rss, client.InNamespace(ms.Spec.Namespace)); err != nil {
		return nil, err
	}
	rsUIDs := map[string]bool{}
	for i := range rss.Items {
		if ownedBy(rss.Items[i].OwnerReferences, "Deployment", depUID) {
			rsUIDs[string(rss.Items[i].UID)] = true
		}
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(ms.Spec.Namespace)); err != nil {
		return nil, err
	}
	var out []*corev1.Pod
	for i := range pods.Items {
		for _, o := range pods.Items[i].OwnerReferences {
			if o.Kind == "ReplicaSet" && o.Controller != nil && *o.Controller && rsUIDs[string(o.UID)] {
				out = append(out, &pods.Items[i])
				break
			}
		}
	}
	return out, nil
}

// getOwnedUtilsDeployment returns the utils Deployment of ms, or nil when it does not exist or is not controlled by ms.
// A chart Deployment that happens to be named utils is not the utils Deployment.
func getOwnedUtilsDeployment(ctx context.Context, c client.Client, ms *v1alpha1.MiniService) (*appsv1.Deployment, error) {
	dep := &appsv1.Deployment{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ms.Spec.Namespace, Name: UtilsDeploymentName}, dep); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if !ownedBy(dep.OwnerReferences, "MiniService", ms.UID) {
		return nil, nil
	}
	return dep, nil
}

func ownedBy(refs []metav1.OwnerReference, kind string, uid types.UID) bool {
	for _, o := range refs {
		if o.Kind == kind && o.Controller != nil && *o.Controller && o.UID == uid {
			return true
		}
	}
	return false
}

// utilsHAImplemented is false until every path that looks at the utils pod by name is HA aware: status and
// WorkersHealthy (status.go), the update path, the cleanup grace period, and webhook classification of Deployment pods.
// While it is false the Deployment path never runs, whatever the options say. Tests set it to exercise the path.
var utilsHAImplemented = false

// utilsInfra is what an install reconciliation does about the utils object.
type utilsInfra struct {
	// Apply is the object to create or apply, or nil when nothing must be created this time.
	Apply client.Object
	// Deployment is the HA Deployment of the instance, new or already existing, or nil for a bare pod.
	Deployment *appsv1.Deployment
	Plan       utilsHAPlan
}

// utilsInfraObject decides the utils object NVCA creates: the bare pod, or the HA Deployment when the path is enabled,
// the rollout gate allows more than one replica, and the prepared pod has a concurrency limit to split. It falls back to
// the bare pod otherwise, and says why in the log. The plan's Replicas is above 1 only for a Deployment.
//
// An instance keeps the form it already has. One that has HA utils (a Deployment NVCA owns, or an HA record) never gets
// a bare pod, and one that has the bare pod never gets a Deployment, whatever the gate or the switches say: the next install reconciliation after a gate loss or a rollback would otherwise create a
// bare utils pod next to the replicas, exceeding the instance concurrency and duplicating metering. Its Deployment is
// left as it is (no template change), and a recorded Deployment that vanished is rebuilt only while the path is allowed.
func (r *Reconciler) utilsInfraObject(ctx context.Context, log logr.Logger, ms *v1alpha1.MiniService, pod *corev1.Pod) (utilsInfra, error) {
	existing, err := getOwnedUtilsDeployment(ctx, r.Client, ms)
	if err != nil {
		return utilsInfra{}, err
	}
	recorded := recordedUtilsHAReplicas(ms)
	if existing != nil {
		replicas := recorded
		if replicas < 2 && existing.Spec.Replicas != nil {
			replicas = *existing.Spec.Replicas
		}
		log.V(1).Info("Utils Deployment exists, leaving it as it is", "replicas", replicas)
		return utilsInfra{Deployment: existing, Plan: utilsHAPlan{Replicas: replicas}}, nil
	}
	hadHA := recorded > 1
	if !hadHA {
		// The reverse transition: the install created the bare pod while HA was not allowed, and the gate opened before
		// the install finished. Keep the form that exists, so the instance never has a bare pod and replicas together.
		bare := &corev1.Pod{}
		switch err := r.Client.Get(ctx, client.ObjectKey{Namespace: ms.Spec.Namespace, Name: common.UtilsPodName}, bare); {
		case err == nil:
			log.V(1).Info("A bare utils pod exists, keeping that form for this instance")
			return utilsInfra{Apply: pod, Plan: utilsHAPlan{Replicas: 1}}, nil
		case !apierrors.IsNotFound(err):
			return utilsInfra{}, err
		}
	}
	allowed := utilsHAImplemented && r.UtilsHAEnabled
	if !allowed {
		if hadHA {
			log.Info("Utils HA is switched off for an HA instance whose Deployment is gone, not creating a bare utils pod", "replicas", recorded)
			return utilsInfra{Plan: utilsHAPlan{Replicas: recorded}}, nil
		}
		return utilsInfra{Apply: pod, Plan: utilsHAPlan{Replicas: 1}}, nil
	}
	plan := r.effectiveUtilsReplicas(log, ms)
	if plan.Replicas < 2 || plan.Paused {
		if hadHA {
			log.Info("Utils HA is paused for an HA instance whose Deployment is gone, not creating a bare utils pod", "replicas", recorded)
			return utilsInfra{Plan: utilsHAPlan{Replicas: recorded}}, nil
		}
		return utilsInfra{Apply: pod, Plan: utilsHAPlan{Replicas: 1}}, nil
	}
	dep, haPlan, err := buildUtilsDeployment(ms, pod, plan.Replicas)
	if err != nil {
		log.Info("Utils replicas not possible, using one utils pod", "reason", err.Error())
		if hadHA {
			return utilsInfra{Plan: utilsHAPlan{Replicas: recorded}}, nil
		}
		return utilsInfra{Apply: pod, Plan: utilsHAPlan{Replicas: 1}}, nil
	}
	if haPlan.Reason != "" {
		log.Info("Utils replicas adjusted", "reason", haPlan.Reason, "replicas", haPlan.Replicas, "perReplica", haPlan.PerReplica)
	}
	if dep == nil {
		// The concurrency leaves room for one replica only.
		if hadHA {
			log.Info("Utils replicas reduced to one for an HA instance whose Deployment is gone, not creating a bare utils pod", "replicas", recorded)
			return utilsInfra{Plan: utilsHAPlan{Replicas: recorded}}, nil
		}
		return utilsInfra{Apply: pod, Plan: utilsHAPlan{Replicas: 1}}, nil
	}
	return utilsInfra{Apply: dep, Deployment: dep, Plan: haPlan}, nil
}

// haUtilsInitState decides whether the install may proceed past the utils init containers of an HA instance. It waits
// for the first utils replica whose init containers completed: the init containers write the files workload pods read,
// and any replica can have written them. It does not wait for a replica to be serving, which would be a startup
// cycle (REQ-HA-10), and a replica that is stuck does not block a healthy one (REQ-HA-11). It is terminal only when
// every replica is terminally bad. proceed is true when the install may continue, otherwise result says to wait.
func (r *Reconciler) haUtilsInitState(ctx context.Context, ms *v1alpha1.MiniService, icmsReq *nvcav2beta1.ICMSRequest) (
	proceed bool, result reconcile.Result, err error) {
	pods, err := listHAUtilsPods(ctx, r.Client, ms)
	switch {
	case err != nil && k8sutil.IsTransientK8sError(err):
		return false, reconcile.Result{Requeue: true}, nil
	case err != nil:
		return false, reconcile.Result{}, err
	case len(pods) == 0:
		return false, reconcile.Result{Requeue: true}, nil
	}
	timeout := getPodSchedulingTimeout(ctx, icmsReq, r.K8sTimeConfig)
	terminal := 0
	var lastReason string
	active := 0
	for _, p := range pods {
		if p.DeletionTimestamp != nil {
			// A terminating replica is on its way out. It must neither let the install proceed nor count as a failure.
			continue
		}
		active++
		if st := getPodStatus(p, r.K8sTimeConfig, timeout); st.TerminalBad {
			terminal++
			lastReason = fmt.Sprintf("%s: %s", st.Status, st.Reason)
			continue
		}
		if isInitContainersComplete(p) {
			return true, reconcile.Result{}, nil
		}
	}
	if active > 0 && terminal == active {
		return false, reconcile.Result{}, reconcile.TerminalError(fmt.Errorf("every utils replica is terminally bad, last: %s", lastReason))
	}
	return false, reconcile.Result{}, nil
}

// utilsInitState decides whether install or a workload update may proceed past the utils init containers, for a bare
// utils pod or an HA instance's replicas. It is the one place both paths use. When no utils object exists at all,
// missingIsTerminal says whether that is an error (an update, the utils objects must exist by then) or something to wait
// for (an install, they may still be creating).
func (r *Reconciler) utilsInitState(ctx context.Context, ms *v1alpha1.MiniService, icmsReq *nvcav2beta1.ICMSRequest,
	log logr.Logger, missingIsTerminal bool) (proceed bool, result reconcile.Result, err error) {
	pod := &corev1.Pod{}
	switch err := r.Client.Get(ctx, client.ObjectKey{Namespace: ms.Spec.Namespace, Name: common.UtilsPodName}, pod); {
	case err == nil:
		// Ensure the pod is in a healthy state so an initialized but errored pod does not result in a workload apply.
		timeout := getPodSchedulingTimeout(ctx, icmsReq, r.K8sTimeConfig)
		if status := getPodStatus(pod, r.K8sTimeConfig, timeout); status.TerminalBad {
			return false, reconcile.Result{}, reconcile.TerminalError(fmt.Errorf("utils pod is %s: %s", status.Status, status.Reason))
		}
		if !isInitContainersComplete(pod) {
			log.V(1).Info("Utils pod init containers have not completed, waiting for initialization event")
			return false, reconcile.Result{}, nil
		}
		log.V(1).Info("Utils pod init containers have completed")
		return true, reconcile.Result{}, nil
	case apierrors.IsNotFound(err):
		// No bare pod: this may be an HA instance. Only the owned utils Deployment makes it one.
		dep, derr := getOwnedUtilsDeployment(ctx, r.Client, ms)
		switch {
		case derr != nil && k8sutil.IsTransientK8sError(derr):
			return false, reconcile.Result{Requeue: true}, nil
		case derr != nil:
			return false, reconcile.Result{}, derr
		case dep != nil:
			return r.haUtilsInitState(ctx, ms, icmsReq)
		case missingIsTerminal:
			log.Error(err, "Utils pod expected to be found but was not")
			return false, reconcile.Result{}, reconcile.TerminalError(err)
		default:
			log.V(1).Info("Utils pod not found yet, waiting for creation")
			return false, reconcile.Result{Requeue: true}, nil
		}
	case k8sutil.IsTransientK8sError(err):
		log.V(1).Info("Transient error getting utils pod, will retry", "error", err)
		return false, reconcile.Result{Requeue: true}, nil
	default:
		log.Error(err, "Non-transient error getting utils pod")
		return false, reconcile.Result{}, err
	}
}

// utilsTerminationGracePeriod returns the termination grace period of the utils pod, which sets how long cleanup waits for
// workload objects to delete before deleting infrastructure. For an HA instance it comes from the utils Deployment
// template (the pods share it), or from a replica if the Deployment is already gone. ok is false when none is found.
func (r *Reconciler) utilsTerminationGracePeriod(ctx context.Context, ms *v1alpha1.MiniService) (time.Duration, bool) {
	pod := &corev1.Pod{}
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: ms.Spec.Namespace, Name: common.UtilsPodName}, pod); err == nil {
		if pod.Spec.TerminationGracePeriodSeconds != nil {
			return time.Duration(*pod.Spec.TerminationGracePeriodSeconds) * time.Second, true
		}
		return 0, false
	}
	if dep, err := getOwnedUtilsDeployment(ctx, r.Client, ms); err == nil && dep != nil {
		if g := dep.Spec.Template.Spec.TerminationGracePeriodSeconds; g != nil {
			return time.Duration(*g) * time.Second, true
		}
	}
	// The Deployment is gone. What NVCA recorded when it created it is trusted, a pod is not: a chart could create pods
	// that look like replicas, and they must not set how long cleanup waits.
	if rec, ok := recordedUtilsHA(ms); ok {
		if rec.TerminationGracePeriodSeconds != nil {
			return time.Duration(*rec.TerminationGracePeriodSeconds) * time.Second, true
		}
		return 0, false
	}
	// No record (an instance created before the record held it): replicas found through the Deployment, if it still exists.
	if pods, err := listHAUtilsPods(ctx, r.Client, ms); err == nil {
		for _, p := range pods {
			if p.Spec.TerminationGracePeriodSeconds != nil {
				return time.Duration(*p.Spec.TerminationGracePeriodSeconds) * time.Second, true
			}
		}
	}
	return 0, false
}
