// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook

// Shared Dynamo namespace for workers that register through Kubernetes.
//
// A Dynamo graph often names its Dynamo namespace after its Kubernetes
// namespace (DYN_NAMESPACE=<namespace>-<graph>), so every instance of a
// function has its own. A worker restored into another instance keeps the
// value it was captured with and would register under the captured
// instance's Dynamo namespace, where the new instance's frontend never
// looks. With Kubernetes discovery a frontend sees only the workers of its
// own Kubernetes namespace, so instances can share one Dynamo namespace
// without seeing each other: admission replaces the Kubernetes namespace in
// DYN_NAMESPACE and DYN_NAMESPACE_PREFIX with the fixed Dynamo namespace
// (--webhook-dynamo-namespace), on every Dynamo pod of the namespace,
// frontend included, so all of an instance agree and a restored worker
// registers where the frontend looks. The rest of the name is kept, so two
// graphs of one namespace stay apart.
//
// A running instance is never split: while a Dynamo pod of the namespace
// still runs under the original name (admitted before this, say), new pods
// keep it too, and the instance changes name only when it is redeployed
// whole.

import (
	"context"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// namespaceToken matches ns as a whole name or a part of one, as
// NamesOwnInstance does.
func namespaceToken(ns string) *regexp.Regexp {
	return regexp.MustCompile(`(^|[^a-z0-9])` + regexp.QuoteMeta(ns) + `([^a-z0-9]|$)`)
}

// kubeDiscoveryComponent reports a Dynamo component that registers through
// Kubernetes.
func kubeDiscoveryComponent(pod *corev1.Pod) bool {
	return dynamoComponent(pod) && DynamoDiscovery(pod) == "kubernetes"
}

// sharedNamespaceValues returns, for pod, the Dynamo namespace values to
// set by env name (container index, name, value): those of a component
// that registers through Kubernetes and names its Kubernetes namespace in
// them, with the namespace replaced by m.DynamoNamespace. None when there
// is nothing to replace, or the namespace still runs a Dynamo pod under
// the original name (or that cannot be read).
//
// One admission decides once (withSharedNamespaceDecision): the checks that
// judge the restored process (asAdmitted) and the patches the pod gets
// (sharedNamespacePatches) then agree even if a later read of the
// namespace would fail or see other pods.
func (m *Mutator) sharedNamespaceValues(ctx context.Context, pod *corev1.Pod) map[int]map[string]string {
	if d, ok := ctx.Value(sharedNamespaceKey{}).(*sharedNamespaceDecision); ok && d.pod == pod {
		if !d.done {
			d.vals, d.done = m.decideSharedNamespace(ctx, pod), true
		}
		return d.vals
	}
	return m.decideSharedNamespace(ctx, pod)
}

type sharedNamespaceKey struct{}

// sharedNamespaceDecision is the shared-namespace decision of one admitted
// pod, made on first use.
type sharedNamespaceDecision struct {
	pod  *corev1.Pod
	done bool
	vals map[int]map[string]string
}

// withSharedNamespaceDecision returns ctx under which every
// sharedNamespaceValues of pod returns the same decision.
func withSharedNamespaceDecision(ctx context.Context, pod *corev1.Pod) context.Context {
	return context.WithValue(ctx, sharedNamespaceKey{}, &sharedNamespaceDecision{pod: pod})
}

func (m *Mutator) decideSharedNamespace(ctx context.Context, pod *corev1.Pod) map[int]map[string]string {
	if m.DynamoNamespace == "" || pod.Namespace == "" || !kubeDiscoveryComponent(pod) {
		return nil
	}
	tok := namespaceToken(pod.Namespace)
	out := map[int]map[string]string{}
	originals := map[string]bool{}
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		for _, name := range []string{dynNamespaceEnv, dynNamespacePrefixEnv} {
			v := literalEnv(c, name)
			if v == "" || !tok.MatchString(v) {
				continue
			}
			if out[i] == nil {
				out[i] = map[string]string{}
			}
			out[i][name] = tok.ReplaceAllString(v, "${1}"+m.DynamoNamespace+"${2}")
			if name == dynNamespaceEnv {
				originals[v] = true
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	if m.Kube == nil {
		return nil // cannot tell whether a running pod keeps the original name
	}
	pods, err := m.Kube.CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		m.logger().WithError(err).WithField("namespace", pod.Namespace).
			Warn("dynamo: cannot list the namespace's pods; the Dynamo namespace is kept")
		return nil
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Name == pod.Name || p.DeletionTimestamp != nil || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		for j := range p.Spec.Containers {
			if originals[literalEnv(&p.Spec.Containers[j], dynNamespaceEnv)] {
				m.logger().WithField("pod", pod.Namespace+"/"+pod.Name).WithField("running", p.Name).
					Info("dynamo: a pod of the instance runs under the original Dynamo namespace; kept, so the instance is not split")
				return nil
			}
		}
	}
	return out
}

// sharedNamespacePatches sets the shared Dynamo namespace on pod
// (sharedNamespaceValues).
func (m *Mutator) sharedNamespacePatches(ctx context.Context, pod *corev1.Pod) []PatchOp {
	vals := m.sharedNamespaceValues(ctx, pod)
	var patches []PatchOp
	for i := range pod.Spec.Containers {
		if len(vals[i]) == 0 {
			continue
		}
		c := &pod.Spec.Containers[i]
		var vars []corev1.EnvVar
		for _, name := range []string{dynNamespaceEnv, dynNamespacePrefixEnv} {
			if v, ok := vals[i][name]; ok {
				vars = append(vars, corev1.EnvVar{Name: name, Value: v})
			}
		}
		patches = append(patches, setContainerEnv(i, c, vars)...)
	}
	if len(patches) > 0 {
		m.logger().WithField("pod", pod.Namespace+"/"+pod.Name).WithField("dynamoNamespace", m.DynamoNamespace).
			Info("dynamo: the instance's Dynamo namespace is shared, so a worker restored into another instance registers where its frontend looks")
	}
	return patches
}

// asAdmitted returns pod with the shared Dynamo namespace it is admitted
// with, for the checks that judge the process it will run.
func (m *Mutator) asAdmitted(ctx context.Context, pod *corev1.Pod) *corev1.Pod {
	vals := m.sharedNamespaceValues(ctx, pod)
	if len(vals) == 0 {
		return pod
	}
	p := pod.DeepCopy()
	for i, byName := range vals {
		for j := range p.Spec.Containers[i].Env {
			if v, ok := byName[p.Spec.Containers[i].Env[j].Name]; ok {
				p.Spec.Containers[i].Env[j].Value = v
			}
		}
	}
	return p
}

// kubeDiscoveryExempt reports env a component that registers through
// Kubernetes names its namespace in, but that a restored process does not
// act on: DYN_PARENT_DGD_K8S_NAMESPACE (read by neither Dynamo 1.5.1's
// runtime nor its vLLM worker), and GROVE_HEADLESS_SERVICE in a pod that is
// not one of several pods of a Grove scaling group (it names the leader
// a multi-pod engine joins). Such a pod's restore into another instance
// still registers correctly (kube_discovery.go writes its registration).
func kubeDiscoveryExempt(pod *corev1.Pod, c *corev1.Container, name string) bool {
	if !kubeDiscoveryComponent(pod) {
		return false
	}
	switch name {
	case "DYN_PARENT_DGD_K8S_NAMESPACE":
		return true
	case "GROVE_HEADLESS_SERVICE":
		v, _ := envValue(c, groveScalingGroupSizeEnv)
		return v == "" || v == "1"
	}
	return false
}
