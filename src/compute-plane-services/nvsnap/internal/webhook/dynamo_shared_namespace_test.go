// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/checkpointstore"
)

const deepseekNS = "sr-3d68984c-077b-46d0-9a33-6512f427a612"

// deepseekPod is a component of a DeepSeek-style graph: Kubernetes
// discovery, a Dynamo namespace named after the Kubernetes namespace, and
// the variables the operator and Grove set.
func deepseekPod(name string, gpus string, extra ...corev1.EnvVar) *corev1.Pod {
	env := append([]corev1.EnvVar{
		{Name: "DYN_DISCOVERY_BACKEND", Value: "kubernetes"},
		{Name: "DYN_NAMESPACE", Value: deepseekNS + "-dsv41-flash"},
		{Name: "DYN_PARENT_DGD_K8S_NAMESPACE", Value: deepseekNS},
		{Name: "GROVE_HEADLESS_SERVICE", Value: "dsv41-flash-0." + deepseekNS + ".svc.cluster.local"},
	}, extra...)
	c := corev1.Container{Name: "main", Env: env}
	if gpus != "" {
		c.Resources.Limits = corev1.ResourceList{"nvidia.com/gpu": resource.MustParse(gpus)}
	}
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: deepseekNS}, Spec: corev1.PodSpec{Containers: []corev1.Container{c}}}
}

func sharedMutator(objs ...*corev1.Pod) *Mutator {
	kc := fake.NewSimpleClientset()
	for _, p := range objs {
		_ = kc.Tracker().Add(p)
	}
	return &Mutator{DynamoNamespace: "nvcf-nvsnap", Kube: kc}
}

func mainEnv(p *corev1.Pod, name string) string {
	for _, e := range p.Spec.Containers[0].Env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

// A fresh instance: the worker and the frontend (no GPU) both get the shared
// name, the graph's part of it kept.
func TestSharedNamespaceFreshInstance(t *testing.T) {
	ctx := context.Background()
	m := sharedMutator()
	worker := deepseekPod("prefill-0", "2")
	got := applyPatches(t, worker, m.sharedNamespacePatches(ctx, worker))
	if v := mainEnv(got, "DYN_NAMESPACE"); v != "nvcf-nvsnap-dsv41-flash" {
		t.Fatalf("worker DYN_NAMESPACE %q, want nvcf-nvsnap-dsv41-flash", v)
	}
	frontend := deepseekPod("frontend-0", "", corev1.EnvVar{Name: "DYN_NAMESPACE_PREFIX", Value: deepseekNS + "-dsv41-flash"})
	got = applyPatches(t, frontend, m.sharedNamespacePatches(ctx, frontend))
	if mainEnv(got, "DYN_NAMESPACE") != "nvcf-nvsnap-dsv41-flash" || mainEnv(got, "DYN_NAMESPACE_PREFIX") != "nvcf-nvsnap-dsv41-flash" {
		t.Fatalf("frontend: %q / %q, want both shared", mainEnv(got, "DYN_NAMESPACE"), mainEnv(got, "DYN_NAMESPACE_PREFIX"))
	}
	if v := mainEnv(got, "DYN_PARENT_DGD_K8S_NAMESPACE"); v != deepseekNS {
		t.Errorf("DYN_PARENT_DGD_K8S_NAMESPACE changed to %q", v)
	}
}

// A running instance is not split: while a pod runs under the original
// name, a new one keeps it; once every running pod has the shared one, a
// new pod gets it too.
func TestSharedNamespaceDoesNotSplitAnInstance(t *testing.T) {
	ctx := context.Background()
	running := deepseekPod("frontend-0", "")
	running.Status.Phase = corev1.PodRunning
	m := sharedMutator(running)
	if ops := m.sharedNamespacePatches(ctx, deepseekPod("prefill-1", "2")); len(ops) != 0 {
		t.Fatalf("a new worker beside a frontend under the original name was renamed: %v", ops)
	}
	shared := deepseekPod("frontend-0", "")
	shared.Spec.Containers[0].Env[1].Value = "nvcf-nvsnap-dsv41-flash"
	shared.Status.Phase = corev1.PodRunning
	m = sharedMutator(shared)
	if ops := m.sharedNamespacePatches(ctx, deepseekPod("prefill-1", "2")); len(ops) == 0 {
		t.Fatal("a new worker beside a frontend already under the shared name kept the original")
	}
}

// Untouched: a component that registers through etcd (the fixed-namespace
// profile handles it), a name that does not include the namespace, a
// cluster without a fixed Dynamo namespace.
func TestSharedNamespaceLeavesOthers(t *testing.T) {
	ctx := context.Background()
	etcd := deepseekPod("w", "1")
	etcd.Spec.Containers[0].Env[0].Value = "etcd"
	if ops := sharedMutator().sharedNamespacePatches(ctx, etcd); len(ops) != 0 {
		t.Errorf("etcd component: %v", ops)
	}
	plain := deepseekPod("w", "1")
	plain.Spec.Containers[0].Env[1].Value = "qwen"
	if ops := sharedMutator().sharedNamespacePatches(ctx, plain); len(ops) != 0 {
		t.Errorf("a name without the namespace: %v", ops)
	}
	if ops := (&Mutator{Kube: fake.NewSimpleClientset()}).sharedNamespacePatches(ctx, deepseekPod("w", "1")); len(ops) != 0 {
		t.Errorf("no fixed Dynamo namespace configured: %v", ops)
	}
}

// What names the instance: a DeepSeek-style prefill pod, as admitted, names
// none (DYN_NAMESPACE shared, the other two exempt); as created it names
// DYN_NAMESPACE; a pod of a multi-pod Grove gang still names
// GROVE_HEADLESS_SERVICE (the leader it joins); an etcd component is judged
// as before.
func TestNamesOwnInstanceKubeDiscovery(t *testing.T) {
	ctx := context.Background()
	m := sharedMutator()
	pod := deepseekPod("prefill-0", "2")
	if got := NamesOwnInstance(pod); got != "DYN_NAMESPACE" {
		t.Errorf("as created: %q, want DYN_NAMESPACE", got)
	}
	if got := NamesOwnInstance(m.asAdmitted(ctx, pod)); got != "" {
		t.Errorf("as admitted: %q, want none", got)
	}
	gang := deepseekPod("decode-0", "4", corev1.EnvVar{Name: "GROVE_PCSG_TEMPLATE_NUM_PODS", Value: "4"})
	if got := NamesOwnInstance(m.asAdmitted(ctx, gang)); got != "GROVE_HEADLESS_SERVICE" {
		t.Errorf("pod of a 4-pod gang: %q, want GROVE_HEADLESS_SERVICE", got)
	}
	etcd := deepseekPod("w", "1")
	etcd.Spec.Containers[0].Env[0].Value = "etcd"
	etcd.Spec.Containers[0].Env[1].Value = "fixed"
	if got := NamesOwnInstance(etcd); got != "DYN_PARENT_DGD_K8S_NAMESPACE" {
		t.Errorf("etcd component: %q, want DYN_PARENT_DGD_K8S_NAMESPACE (no exemption)", got)
	}
}

// A pod admitted as a group restore's placeholder gets the shared Dynamo
// namespace too: a placeholder under the original name would read as naming
// its instance, and would keep the instance's later pods from sharing it.
func TestSharedNamespaceOnGroupRestorePlaceholder(t *testing.T) {
	g := CRIUGroup{Checkpoints: []string{"p0__1"}, Nodes: []string{"n0"}, GPUShare: true}
	m, _ := groupMutator(t, g, "")
	m.DynamoNamespace = "nvcf-nvsnap"
	m.Kube = fake.NewSimpleClientset()
	pod := deepseekPod("prefill-0", "1")
	pod.Spec.Containers[0].Image, pod.Spec.Containers[0].Command = "vllm", []string{"python3", "-m", "dynamo.vllm"}
	patches, err := m.Mutate(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	got := applyPatches(t, pod, patches)
	if got.Annotations[CRIURestoreAnnotation] != "p0__1" {
		t.Fatalf("not admitted as the restore placeholder: %v", got.Annotations)
	}
	if v := mainEnv(got, "DYN_NAMESPACE"); v != "nvcf-nvsnap-dsv41-flash" {
		t.Fatalf("placeholder DYN_NAMESPACE %q, want the shared nvcf-nvsnap-dsv41-flash", v)
	}
}

// Instances of one graph get different worker suffixes (the operator's hash):
// for a worker that registers through Kubernetes the suffix is left out of
// the process configuration, so another instance's capture restores; an etcd
// worker keeps it (a new suffix captures again, as before).
func TestProcessConfigWorkerSuffix(t *testing.T) {
	a := deepseekPod("prefill-0", "2", corev1.EnvVar{Name: "DYN_NAMESPACE_WORKER_SUFFIX", Value: "e53409be"})
	b := deepseekPod("prefill-0", "2", corev1.EnvVar{Name: "DYN_NAMESPACE_WORKER_SUFFIX", Value: "58ac05f4"})
	if ProcessConfig(a) != ProcessConfig(b) {
		t.Errorf("Kubernetes-discovery workers differing only in the suffix: %v", ProcessConfigChanges(ProcessConfig(a), ProcessConfig(b)))
	}
	ea, eb := a.DeepCopy(), b.DeepCopy()
	ea.Spec.Containers[0].Env[0].Value, eb.Spec.Containers[0].Env[0].Value = "etcd", "etcd"
	if ProcessConfig(ea) == ProcessConfig(eb) {
		t.Error("etcd workers differing in the suffix: same process configuration, want different")
	}
}

// One admission decides once: the restore check (asAdmitted) and the
// patches the pod gets agree even when a later read of the namespace fails.
func TestSharedNamespaceDecidedOncePerAdmission(t *testing.T) {
	m := sharedMutator()
	lists := 0
	m.Kube.(*fake.Clientset).PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		lists++
		if lists > 1 {
			return true, nil, errors.New("apiserver unavailable")
		}
		return false, nil, nil
	})
	pod := deepseekPod("prefill-0", "2")
	ctx := withSharedNamespaceDecision(context.Background(), pod)
	if v := mainEnv(m.asAdmitted(ctx, pod), "DYN_NAMESPACE"); v != "nvcf-nvsnap-dsv41-flash" {
		t.Fatalf("restore check saw DYN_NAMESPACE %q, want the shared name", v)
	}
	got := applyPatches(t, pod, m.sharedNamespacePatches(ctx, pod))
	if v := mainEnv(got, "DYN_NAMESPACE"); v != "nvcf-nvsnap-dsv41-flash" {
		t.Fatalf("pod admitted with DYN_NAMESPACE %q after the restore check accepted the shared name", v)
	}
	if lists != 1 {
		t.Errorf("namespace listed %d times in one admission, want 1", lists)
	}
	// Without the admission's decision a failed read keeps the original
	// name: the case the shared decision guards against.
	if ops := m.sharedNamespacePatches(context.Background(), pod); len(ops) != 0 {
		t.Errorf("a failed read still rewrote the namespace: %v", ops)
	}
}

// A pod restored by its restore-from annotation (not a group record) is
// admitted with the shared Dynamo namespace its restore check judged it
// with.
func TestSharedNamespaceOnExplicitRestorePlaceholder(t *testing.T) {
	man := criuCaptureRecord()
	man.SourcePodMeta[checkpointstore.CRIUScopeMetaKey] = CRIUNamespaceScope(deepseekNS)
	m := criuMutator(t, man)
	m.MainContainer = 0
	m.DynamoNamespace = "nvcf-nvsnap"
	m.Kube = fake.NewSimpleClientset()
	pod := deepseekPod("prefill-0", "1")
	pod.Annotations = map[string]string{RestoreFromAnnotation: "abc123"}
	pod.Spec.Containers[0].Image, pod.Spec.Containers[0].Command = "vllm", []string{"python3", "-m", "dynamo.vllm"}
	patches, err := m.Mutate(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	got := applyPatches(t, pod, patches)
	if got.Annotations[CRIURestoreAnnotation] == "" {
		t.Fatalf("not admitted as the restore placeholder: %v", got.Annotations)
	}
	if v := mainEnv(got, "DYN_NAMESPACE"); v != "nvcf-nvsnap-dsv41-flash" {
		t.Fatalf("placeholder DYN_NAMESPACE %q, want the shared nvcf-nvsnap-dsv41-flash", v)
	}
}

// A component that registers through Kubernetes gets its Dynamo namespace
// only from the shared decision: when that keeps the original name (here a
// running pod of the instance has it), the fixed name is not set either,
// even with instance-local etcd and NATS endpoints.
func TestSharedNamespaceKeptOverFixedName(t *testing.T) {
	running := deepseekPod("frontend-0", "")
	running.Status.Phase = corev1.PodRunning
	m := sharedMutator(running)
	m.CaptureCandidate = criuCandidates
	pod := deepseekPod("prefill-1", "2",
		corev1.EnvVar{Name: "ETCD_ENDPOINTS", Value: "etcd:2379"},
		corev1.EnvVar{Name: "NATS_SERVER", Value: "nats://nats:4222"})
	ctx := withSharedNamespaceDecision(context.Background(), pod)
	ops := append(m.dynamoNamespacePatches(ctx, pod), m.sharedNamespacePatches(ctx, pod)...)
	got := applyPatches(t, pod, ops)
	if v := mainEnv(got, "DYN_NAMESPACE"); v != deepseekNS+"-dsv41-flash" {
		t.Fatalf("DYN_NAMESPACE %q, want the original %s-dsv41-flash the running pod has", v, deepseekNS)
	}
}

// failPodListsAfter makes every list of pods after the first n fail.
func failPodListsAfter(m *Mutator, n int) *int {
	lists := 0
	m.Kube.(*fake.Clientset).PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		lists++
		if lists > n {
			return true, nil, errors.New("apiserver unavailable")
		}
		return false, nil, nil
	})
	return &lists
}

// Through Mutate: a Kubernetes-discovery pod with instance-local etcd and
// NATS endpoints, while a running pod of the instance has the original
// name, is admitted with the original name (not the fixed one).
func TestMutateKubeDiscoveryKeepsOriginalNameWhileInstanceRuns(t *testing.T) {
	running := deepseekPod("frontend-0", "")
	running.Status.Phase = corev1.PodRunning
	m := sharedMutator(running)
	m.Backend, m.CaptureCandidate = newBackend(t), criuCandidates
	pod := deepseekPod("prefill-1", "2",
		corev1.EnvVar{Name: "ETCD_ENDPOINTS", Value: "etcd:2379"},
		corev1.EnvVar{Name: "NATS_SERVER", Value: "nats://nats:4222"})
	patches, err := m.Mutate(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	if v := mainEnv(applyPatches(t, pod, patches), "DYN_NAMESPACE"); v != deepseekNS+"-dsv41-flash" {
		t.Fatalf("DYN_NAMESPACE %q, want the original %s-dsv41-flash the running pod has", v, deepseekNS)
	}
}

// Through Mutate: the namespace is read once per admission, so a pod whose
// restore check accepted the shared name is admitted as the placeholder
// with it even though every later read of the namespace fails.
func TestMutateSharedNamespaceDecidedOnce(t *testing.T) {
	man := criuCaptureRecord()
	man.SourcePodMeta[checkpointstore.CRIUScopeMetaKey] = CRIUNamespaceScope(deepseekNS)
	m := criuMutator(t, man)
	m.MainContainer = 0
	m.DynamoNamespace = "nvcf-nvsnap"
	m.Kube = fake.NewSimpleClientset()
	lists := failPodListsAfter(m, 1)
	pod := deepseekPod("prefill-0", "1")
	pod.Annotations = map[string]string{RestoreFromAnnotation: "abc123"}
	pod.Spec.Containers[0].Image, pod.Spec.Containers[0].Command = "vllm", []string{"python3", "-m", "dynamo.vllm"}
	patches, err := m.Mutate(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	got := applyPatches(t, pod, patches)
	if got.Annotations[CRIURestoreAnnotation] == "" {
		t.Fatalf("not admitted as the restore placeholder (a later read of the namespace decided again): %v", got.Annotations)
	}
	if v := mainEnv(got, "DYN_NAMESPACE"); v != "nvcf-nvsnap-dsv41-flash" {
		t.Fatalf("placeholder DYN_NAMESPACE %q, want the shared nvcf-nvsnap-dsv41-flash", v)
	}
	if *lists != 1 {
		t.Errorf("namespace pods listed %d times in one admission, want 1", *lists)
	}
}
