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
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/common"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/util/k8sutil"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance/readiness"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	nvcav2beta1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v2beta1"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

const haNS = "sr-ha"

func haMS() *v1alpha1.MiniService {
	return &v1alpha1.MiniService{
		ObjectMeta: metav1.ObjectMeta{Name: "sr-ha-miniservice", UID: "ms-uid", Generation: 2},
		Spec:       v1alpha1.MiniServiceSpec{Namespace: haNS},
	}
}

// preparedUtilsPod looks like the utils pod NVCA has prepared before creating it: an init container, the utils container
// with the concurrency env, an ESS-free volume set including the shared storage claims, resources, and a node name.
func preparedUtilsPod(concurrency string) *corev1.Pod {
	env := []corev1.EnvVar{{Name: "INSTANCE_ID", Value: "sr-ha-miniservice"}, {Name: "FUNCTION_ID", Value: "f1"}}
	if concurrency != "" {
		env = append(env, corev1.EnvVar{Name: maxRequestConcurrencyEnv, Value: concurrency})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: common.UtilsPodName, Namespace: haNS,
			Labels:      map[string]string{"function-id": "f1", "icms-request-id": "req"},
			Annotations: map[string]string{"nvca.nvcf.nvidia.io/storage-sharedstorage-secrets-rw-pvc-name": "nvcf-secrets-data-rw"},
		},
		Spec: corev1.PodSpec{
			NodeName:      "node-7",
			RestartPolicy: corev1.RestartPolicyAlways,
			InitContainers: []corev1.Container{{
				Name: "init", Image: "init:1",
				VolumeMounts: []corev1.VolumeMount{{Name: "model-data", MountPath: "/config/models"}},
			}},
			Containers: []corev1.Container{
				{Name: common.UtilsContainerName, Image: "utils:1", Env: env,
					Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}}},
				{Name: "sidecar", Image: "side:1", Env: []corev1.EnvVar{{Name: maxRequestConcurrencyEnv, Value: "99"}}},
			},
			Volumes: []corev1.Volume{
				{Name: "model-data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "secrets-data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "nvcf-secrets-data-rw"}}},
			},
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{
					{Key: "node.type", Operator: corev1.NodeSelectorOpIn, Values: []string{"gpu"}}}}}}}},
		},
	}
}

func TestSplitUtilsConcurrency(t *testing.T) {
	for name, tt := range map[string]struct {
		total, requested int32
		replicas, per    int32
		reason           string
	}{
		"even split":                {4, 2, 2, 2, ""},
		"never rounds up":           {5, 2, 2, 2, "slots stay unused"},
		"three replicas":            {7, 3, 3, 2, "slots stay unused"},
		"total equals replicas":     {3, 3, 3, 1, ""},
		"total below the request":   {1, 2, 1, 1, "below the requested 2"},
		"two slots, five requested": {2, 5, 2, 1, "below the requested 5"},
		"large even":                {1024, 4, 4, 256, ""},
		"one replica":               {4, 1, 1, 4, ""},
	} {
		t.Run(name, func(t *testing.T) {
			r, per, reason := splitUtilsConcurrency(tt.total, tt.requested)
			assert.Equal(t, tt.replicas, r)
			assert.Equal(t, tt.per, per)
			assert.LessOrEqual(t, r*per, tt.total, "the configured total is never exceeded")
			assert.GreaterOrEqual(t, per, int32(1), "no replica runs with a limit of zero")
			if tt.reason == "" {
				assert.Empty(t, reason)
			} else {
				assert.Contains(t, reason, tt.reason)
			}
		})
	}
}

func TestBuildUtilsDeployment(t *testing.T) {
	ms, pod := haMS(), preparedUtilsPod("4")
	orig := pod.DeepCopy()

	dep, plan, err := buildUtilsDeployment(ms, pod, 2)
	require.NoError(t, err)
	require.NotNil(t, dep)
	assert.Equal(t, utilsHAPlan{Replicas: 2, TotalConcurrency: 4, PerReplica: 2}, plan)
	assert.Equal(t, orig, pod, "the source pod must not be modified")

	assert.Equal(t, "utils", dep.Name)
	assert.Equal(t, haNS, dep.Namespace)
	assert.EqualValues(t, 2, *dep.Spec.Replicas)

	t.Run("selector and labels carry the reserved role label", func(t *testing.T) {
		want := map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils, nvcatypes.MiniserviceNameLabel: "sr-ha-miniservice"}
		assert.Equal(t, want, dep.Spec.Selector.MatchLabels)
		for k, v := range want {
			assert.Equal(t, v, dep.Spec.Template.Labels[k], "template label %s", k)
			assert.Equal(t, v, dep.Labels[k], "object label %s", k)
		}
		assert.Equal(t, "f1", dep.Spec.Template.Labels["function-id"], "the pod's own labels are kept")
	})

	t.Run("owned by the MiniService", func(t *testing.T) {
		require.Len(t, dep.OwnerReferences, 1)
		o := dep.OwnerReferences[0]
		assert.Equal(t, "MiniService", o.Kind)
		assert.Equal(t, v1alpha1.SchemeGroupVersion.String(), o.APIVersion)
		assert.Equal(t, types.UID("ms-uid"), o.UID)
		assert.True(t, *o.Controller)
	})

	t.Run("rolling update never surges", func(t *testing.T) {
		ru := dep.Spec.Strategy.RollingUpdate
		require.NotNil(t, ru)
		assert.EqualValues(t, 0, ru.MaxSurge.IntValue())
		assert.EqualValues(t, 1, ru.MaxUnavailable.IntValue())
	})

	t.Run("the template is the prepared pod, not a re-translation", func(t *testing.T) {
		spec := dep.Spec.Template.Spec
		assert.Equal(t, orig.Spec.InitContainers, spec.InitContainers, "init containers are kept")
		assert.Equal(t, orig.Spec.Volumes, spec.Volumes, "volumes, including the shared storage claims, are kept")
		assert.Equal(t, orig.Spec.RestartPolicy, spec.RestartPolicy)
		assert.Equal(t, orig.Annotations, dep.Spec.Template.Annotations)
		assert.Empty(t, spec.NodeName, "no node pinning from the source pod")
		assert.Equal(t, orig.Spec.Affinity.NodeAffinity, spec.Affinity.NodeAffinity, "existing node affinity is kept")
	})

	t.Run("concurrency is split on the utils container only", func(t *testing.T) {
		spec := dep.Spec.Template.Spec
		got := map[string]string{}
		for _, e := range spec.Containers[0].Env {
			got[e.Name] = e.Value
		}
		assert.Equal(t, "2", got[maxRequestConcurrencyEnv])
		assert.Equal(t, "sr-ha-miniservice", got["INSTANCE_ID"], "the instance id is shared by all replicas")
		assert.Equal(t, "99", spec.Containers[1].Env[0].Value, "another container's env is untouched")
		assert.Equal(t, orig.Spec.Containers[0].Resources, spec.Containers[0].Resources)
	})

	t.Run("soft spread across nodes", func(t *testing.T) {
		pa := dep.Spec.Template.Spec.Affinity.PodAntiAffinity
		require.NotNil(t, pa)
		assert.Empty(t, pa.RequiredDuringSchedulingIgnoredDuringExecution, "spreading is a preference, never a requirement")
		require.Len(t, pa.PreferredDuringSchedulingIgnoredDuringExecution, 1)
		term := pa.PreferredDuringSchedulingIgnoredDuringExecution[0]
		assert.EqualValues(t, 100, term.Weight)
		assert.Equal(t, corev1.LabelHostname, term.PodAffinityTerm.TopologyKey)
		assert.Equal(t, dep.Spec.Selector.MatchLabels, term.PodAffinityTerm.LabelSelector.MatchLabels)
	})
}

func TestBuildUtilsDeploymentConcurrencyCases(t *testing.T) {
	ms := haMS()
	t.Run("five over two replicas gives two each", func(t *testing.T) {
		dep, plan, err := buildUtilsDeployment(ms, preparedUtilsPod("5"), 2)
		require.NoError(t, err)
		assert.EqualValues(t, 2, plan.PerReplica)
		assert.Contains(t, plan.Reason, "unused")
		assert.EqualValues(t, 2, *dep.Spec.Replicas)
	})
	t.Run("a total of one cannot be split: no Deployment, the bare pod stays", func(t *testing.T) {
		dep, plan, err := buildUtilsDeployment(ms, preparedUtilsPod("1"), 2)
		require.NoError(t, err)
		assert.Nil(t, dep)
		assert.EqualValues(t, 1, plan.Replicas)
		assert.Contains(t, plan.Reason, "below the requested")
	})
	t.Run("replicas are reduced to the total", func(t *testing.T) {
		dep, plan, err := buildUtilsDeployment(ms, preparedUtilsPod("3"), 5)
		require.NoError(t, err)
		assert.EqualValues(t, 3, plan.Replicas)
		assert.EqualValues(t, 3, *dep.Spec.Replicas)
	})
	for name, pod := range map[string]*corev1.Pod{
		"no concurrency env": preparedUtilsPod(""),
		"invalid":            preparedUtilsPod("many"),
		"zero":               preparedUtilsPod("0"),
	} {
		t.Run("refused: "+name, func(t *testing.T) {
			dep, _, err := buildUtilsDeployment(ms, pod, 2)
			assert.ErrorIs(t, err, ErrUtilsHAConcurrency)
			assert.Nil(t, dep)
		})
	}
	t.Run("refused: no utils container", func(t *testing.T) {
		pod := preparedUtilsPod("4")
		pod.Spec.Containers = pod.Spec.Containers[1:]
		_, _, err := buildUtilsDeployment(ms, pod, 2)
		assert.ErrorIs(t, err, ErrUtilsHAConcurrency)
	})
}

type haLookup struct {
	ms *utilsprovenance.MiniServiceRef
}

func (haLookup) GetReplicaSet(context.Context, string, string) (*appsv1.ReplicaSet, error) {
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "replicasets"}, "x")
}
func (haLookup) GetDeployment(context.Context, string, string) (*appsv1.Deployment, error) {
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "deployments"}, "x")
}
func (l haLookup) GetMiniService(context.Context, string) (*utilsprovenance.MiniServiceRef, error) {
	return l.ms, nil
}
func (l haLookup) FindMiniServiceByNamespace(context.Context, string) (*utilsprovenance.MiniServiceRef, error) {
	return l.ms, nil
}

// TestBuiltDeploymentPassesTheProvenanceRules builds the Deployment and evaluates its creation with the real engine: it
// must be accepted from NVCA and refused from anyone else, which is the boundary the webhook enforces.
func TestBuiltDeploymentPassesTheProvenanceRules(t *testing.T) {
	ms := haMS()
	dep, _, err := buildUtilsDeployment(ms, preparedUtilsPod("4"), 2)
	require.NoError(t, err)

	engine := &utilsprovenance.Engine{
		Identities: utilsprovenance.Identities{NVCA: "system:serviceaccount:nvca-system:nvca"},
		Lookup:     haLookup{ms: &utilsprovenance.MiniServiceRef{UID: ms.UID, Namespace: haNS}},
	}
	req := func(user string) utilsprovenance.Request {
		return utilsprovenance.Request{
			Operation: utilsprovenance.OperationCreate, Kind: utilsprovenance.KindDeployment, Username: user,
			Namespace: haNS, Name: dep.Name, Object: dep,
		}
	}
	got := engine.Evaluate(context.Background(), req("system:serviceaccount:nvca-system:nvca"))
	assert.True(t, got.Allowed, got.Message)

	got = engine.Evaluate(context.Background(), req("system:serviceaccount:"+haNS+":miniservice-instance-permissions"))
	assert.False(t, got.Allowed, "a chart identity must not be able to create the same Deployment")
	assert.Equal(t, utilsprovenance.ReasonDenyWriter, got.Reason)
}

func haScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, appsv1.AddToScheme(s))
	require.NoError(t, v1alpha1.AddToScheme(s))
	return s
}

func ctrlRef(kind, name string, uid types.UID) metav1.OwnerReference {
	return metav1.OwnerReference{Kind: kind, Name: name, UID: uid, Controller: ptr.To(true)}
}

func TestListHAUtilsPodsFollowsTheOwnershipChain(t *testing.T) {
	ms := haMS()
	genuineDep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: UtilsDeploymentName, Namespace: haNS, UID: "dep-uid", OwnerReferences: []metav1.OwnerReference{ctrlRef("MiniService", ms.Name, ms.UID)}}}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Name: "utils-h1", Namespace: haNS, UID: "rs-uid", OwnerReferences: []metav1.OwnerReference{ctrlRef("Deployment", "utils", "dep-uid")}}}
	otherRS := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Name: "web-h1", Namespace: haNS, UID: "web-rs-uid", OwnerReferences: []metav1.OwnerReference{ctrlRef("Deployment", "web", "web-uid")}}}
	pod := func(name string, owner metav1.OwnerReference, labeled bool) *corev1.Pod {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: haNS, OwnerReferences: []metav1.OwnerReference{owner}}}
		if labeled {
			p.Labels = map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}
		}
		return p
	}
	list := func(objs ...client.Object) []string {
		c := clientfake.NewClientBuilder().WithScheme(haScheme(t)).WithObjects(objs...).Build()
		pods, err := listHAUtilsPods(context.Background(), c, ms)
		require.NoError(t, err)
		var names []string
		for _, p := range pods {
			names = append(names, p.Name)
		}
		return names
	}

	t.Run("genuine pods are found, forged ones are not", func(t *testing.T) {
		got := list(genuineDep, rs, otherRS,
			pod("utils-h1-a", ctrlRef("ReplicaSet", "utils-h1", "rs-uid"), true),
			pod("utils-h1-b", ctrlRef("ReplicaSet", "utils-h1", "rs-uid"), true),
			pod("forged-label", metav1.OwnerReference{}, true),
			pod("chart-pod", ctrlRef("ReplicaSet", "web-h1", "web-rs-uid"), false),
			pod("wrong-uid", ctrlRef("ReplicaSet", "utils-h1", "stolen-uid"), true),
			pod("not-controller", metav1.OwnerReference{Kind: "ReplicaSet", Name: "utils-h1", UID: "rs-uid"}, true),
		)
		assert.ElementsMatch(t, []string{"utils-h1-a", "utils-h1-b"}, got)
	})
	t.Run("a Deployment not owned by this MiniService yields nothing", func(t *testing.T) {
		foreign := genuineDep.DeepCopy()
		foreign.OwnerReferences = []metav1.OwnerReference{ctrlRef("MiniService", "other", "other-uid")}
		assert.Empty(t, list(foreign, rs, pod("utils-h1-a", ctrlRef("ReplicaSet", "utils-h1", "rs-uid"), true)))
	})
	t.Run("a chart Deployment named utils with no owner yields nothing", func(t *testing.T) {
		chart := genuineDep.DeepCopy()
		chart.OwnerReferences = nil
		assert.Empty(t, list(chart, rs, pod("utils-h1-a", ctrlRef("ReplicaSet", "utils-h1", "rs-uid"), true)))
	})
	t.Run("no Deployment is not an error", func(t *testing.T) {
		assert.Empty(t, list(pod("p", metav1.OwnerReference{}, false)))
	})
}

func TestUtilsInfraObjectIsTheBarePodUnlessEverythingAllowsHA(t *testing.T) {
	ms, log := haMS(), logr.Discard()
	open := readiness.Gate{Verifier: stubVerifier{verifiedS}}

	prev := utilsHAImplemented
	t.Cleanup(func() { utilsHAImplemented = prev })

	build := func(opts ControllerOptions, concurrency string) (client.Object, utilsHAPlan) {
		c, _ := newFakeClient(mgrScheme)
		ui, err := (&Reconciler{ControllerOptions: opts, Client: c}).utilsInfraObject(context.Background(), log, ms, preparedUtilsPod(concurrency))
		require.NoError(t, err)
		return ui.Apply, ui.Plan
	}
	asPod := func(t *testing.T, o client.Object) {
		t.Helper()
		_, ok := o.(*corev1.Pod)
		assert.True(t, ok, "expected the bare utils pod, got %T", o)
	}

	t.Run("disabled by default: the path is not implemented", func(t *testing.T) {
		utilsHAImplemented = false
		o, plan := build(ControllerOptions{UtilsHAEnabled: true, UtilsReplicas: 3, UtilsProvenanceGate: open}, "6")
		asPod(t, o)
		assert.EqualValues(t, 1, plan.Replicas)
	})

	utilsHAImplemented = true
	t.Run("not enabled by the option", func(t *testing.T) {
		o, _ := build(ControllerOptions{UtilsHAEnabled: false, UtilsReplicas: 3, UtilsProvenanceGate: open}, "6")
		asPod(t, o)
	})
	t.Run("default of one replica", func(t *testing.T) {
		o, _ := build(ControllerOptions{UtilsHAEnabled: true, UtilsProvenanceGate: open}, "6")
		asPod(t, o)
	})
	t.Run("the rollout gate is closed", func(t *testing.T) {
		for name, g := range map[string]readiness.Checkable{"nil": nil, "failed": gateFor(failedS), "stale": gateFor(staleS)} {
			o, _ := build(ControllerOptions{UtilsHAEnabled: true, UtilsReplicas: 3, UtilsProvenanceGate: g}, "6")
			asPod(t, o)
			_ = name
		}
	})
	t.Run("the gate is open: a Deployment", func(t *testing.T) {
		o, plan := build(ControllerOptions{UtilsHAEnabled: true, UtilsReplicas: 3, UtilsProvenanceGate: open}, "6")
		dep, ok := o.(*appsv1.Deployment)
		require.True(t, ok, "got %T", o)
		assert.EqualValues(t, 3, *dep.Spec.Replicas)
		assert.EqualValues(t, 3, plan.Replicas)
		assert.EqualValues(t, 2, plan.PerReplica)
	})
	t.Run("a pod without a concurrency limit falls back", func(t *testing.T) {
		o, plan := build(ControllerOptions{UtilsHAEnabled: true, UtilsReplicas: 3, UtilsProvenanceGate: open}, "")
		asPod(t, o)
		assert.EqualValues(t, 1, plan.Replicas)
	})
	t.Run("a total of one falls back", func(t *testing.T) {
		o, plan := build(ControllerOptions{UtilsHAEnabled: true, UtilsReplicas: 3, UtilsProvenanceGate: open}, "1")
		asPod(t, o)
		assert.EqualValues(t, 1, plan.Replicas)
	})
}

func TestHAUtilsInitState(t *testing.T) {
	ms := haMS()
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: UtilsDeploymentName, Namespace: haNS, UID: "dep-uid", OwnerReferences: []metav1.OwnerReference{ctrlRef("MiniService", ms.Name, ms.UID)}}}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Name: "utils-h1", Namespace: haNS, UID: "rs-uid", OwnerReferences: []metav1.OwnerReference{ctrlRef("Deployment", "utils", "dep-uid")}}}

	newPod := func(name string, mutate func(p *corev1.Pod)) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: haNS, OwnerReferences: []metav1.OwnerReference{ctrlRef("ReplicaSet", "utils-h1", "rs-uid")}},
			Spec:       corev1.PodSpec{InitContainers: []corev1.Container{{Name: "init"}}},
		}
		mutate(p)
		return p
	}
	initDone := func(p *corev1.Pod) {
		p.Status.Phase = corev1.PodRunning
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodInitialized, Status: corev1.ConditionTrue}}
		p.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "init", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
	}
	initRunning := func(p *corev1.Pod) {
		p.Status.Phase = corev1.PodRunning
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodInitialized, Status: corev1.ConditionFalse}}
	}
	failed := func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed }

	run := func(objs ...client.Object) (bool, reconcile.Result, error) {
		r := &Reconciler{
			Client:            clientfake.NewClientBuilder().WithScheme(haScheme(t)).WithObjects(objs...).Build(),
			ControllerOptions: ControllerOptions{K8sTimeConfig: (&k8sutil.TimeConfig{}).Complete()},
		}
		return r.haUtilsInitState(context.Background(), ms, &nvcav2beta1.ICMSRequest{})
	}

	t.Run("no replicas yet: wait", func(t *testing.T) {
		proceed, res, err := run(dep, rs)
		assert.False(t, proceed)
		assert.True(t, res.Requeue)
		assert.NoError(t, err)
	})
	t.Run("init is still running everywhere: wait without error", func(t *testing.T) {
		proceed, res, err := run(dep, rs, newPod("a", initRunning), newPod("b", initRunning))
		assert.False(t, proceed)
		assert.False(t, res.Requeue, "pod events trigger the next reconcile")
		assert.NoError(t, err)
	})
	t.Run("the first replica with init complete lets the install proceed", func(t *testing.T) {
		proceed, _, err := run(dep, rs, newPod("a", initRunning), newPod("b", initDone))
		assert.True(t, proceed)
		assert.NoError(t, err)
	})
	t.Run("a terminally bad replica does not block a healthy one", func(t *testing.T) {
		proceed, _, err := run(dep, rs, newPod("a", failed), newPod("b", initDone))
		assert.True(t, proceed)
		assert.NoError(t, err)
	})
	t.Run("every replica terminally bad is terminal", func(t *testing.T) {
		proceed, _, err := run(dep, rs, newPod("a", failed), newPod("b", failed))
		assert.False(t, proceed)
		assert.True(t, isTerminal(err))
	})
	t.Run("a forged pod with init complete is not counted", func(t *testing.T) {
		forged := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "forged", Namespace: haNS,
			Labels: map[string]string{nvcatypes.PodRoleLabelKey: nvcatypes.PodRoleValueUtils}},
			Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: "init"}}}}
		initDone(forged)
		proceed, _, err := run(dep, rs, newPod("a", initRunning), forged)
		assert.False(t, proceed, "only pods in the ownership chain count as replicas")
		assert.NoError(t, err)
	})
}

// TestUtilsInfraObjectNeverAddsABarePodToAnHAInstance is the regression for a gate lost between the creation of the HA
// Deployment and the end of the install: the next install reconciliation used to return the bare pod for the paused
// plan and create it next to the replicas, exceeding the instance concurrency and duplicating metering.
func TestUtilsInfraObjectNeverAddsABarePodToAnHAInstance(t *testing.T) {
	log := logr.Discard()
	open := readiness.Gate{Verifier: stubVerifier{verifiedS}}
	prev := utilsHAImplemented
	utilsHAImplemented = true
	t.Cleanup(func() { utilsHAImplemented = prev })

	ownedDeployment := func(ms *v1alpha1.MiniService, replicas int32) *appsv1.Deployment {
		return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: UtilsDeploymentName, Namespace: haNS, UID: "dep-uid",
			OwnerReferences: []metav1.OwnerReference{ctrlRef("MiniService", ms.Name, ms.UID)}},
			Spec: appsv1.DeploymentSpec{Replicas: &replicas}}
	}
	run := func(t *testing.T, ms *v1alpha1.MiniService, opts ControllerOptions, objs ...client.Object) utilsInfra {
		t.Helper()
		c, _ := newFakeClient(mgrScheme, objs...)
		ui, err := (&Reconciler{ControllerOptions: opts, Client: c}).utilsInfraObject(context.Background(), log, ms, preparedUtilsPod("6"))
		require.NoError(t, err)
		return ui
	}
	enabled := func(g readiness.Checkable) ControllerOptions {
		return ControllerOptions{UtilsHAEnabled: true, UtilsReplicas: 2, UtilsProvenanceGate: g}
	}
	recorded := func() *v1alpha1.MiniService {
		ms := haMS()
		recordUtilsHA(ms, utilsHARecord{Replicas: 2, DeploymentUID: "dep-uid"})
		return ms
	}
	noBarePod := func(t *testing.T, ui utilsInfra) {
		t.Helper()
		_, bare := ui.Apply.(*corev1.Pod)
		assert.False(t, bare, "a bare utils pod must never be created next to HA replicas")
	}

	t.Run("the gate closes between Deployment creation and init completion", func(t *testing.T) {
		ms := haMS()
		// Reconciliation 1: the gate is open, the Deployment is built.
		first := run(t, ms, enabled(open))
		dep, ok := first.Apply.(*appsv1.Deployment)
		require.True(t, ok, "got %T", first.Apply)
		recordUtilsHA(ms, newUtilsHARecord(first.Plan.Replicas, dep))
		created := dep.DeepCopy()
		created.UID = "dep-uid"
		created.OwnerReferences = []metav1.OwnerReference{ctrlRef("MiniService", ms.Name, ms.UID)}
		// Reconciliation 2: the init containers are not done, and the gate is now closed.
		for name, g := range map[string]readiness.Checkable{"failed": gateFor(failedS), "stale": gateFor(staleS), "nil": nil} {
			second := run(t, ms, enabled(g), created)
			noBarePod(t, second)
			assert.Nil(t, second.Apply, name)
			assert.NotNil(t, second.Deployment, name)
			assert.EqualValues(t, 2, second.Plan.Replicas, name)
		}
	})
	t.Run("an existing Deployment is left as it is while the gate is open too", func(t *testing.T) {
		ms := recorded()
		ui := run(t, ms, enabled(open), ownedDeployment(ms, 2))
		assert.Nil(t, ui.Apply, "no template change for an existing instance")
		assert.NotNil(t, ui.Deployment)
	})
	t.Run("NVCA restarted before the HA record was saved: the Deployment is found and recorded", func(t *testing.T) {
		ms := haMS()
		ui := run(t, ms, enabled(gateFor(failedS)), ownedDeployment(ms, 2))
		noBarePod(t, ui)
		require.NotNil(t, ui.Deployment)
		assert.EqualValues(t, 2, ui.Plan.Replicas)
	})
	t.Run("a rollback with the switch off does not add a bare pod either", func(t *testing.T) {
		ms := recorded()
		ui := run(t, ms, ControllerOptions{UtilsHAEnabled: false, UtilsProvenanceGate: open}, ownedDeployment(ms, 2))
		noBarePod(t, ui)
		assert.NotNil(t, ui.Deployment)
	})
	t.Run("a recorded Deployment that vanished: nothing is created while paused or switched off", func(t *testing.T) {
		ms := recorded()
		for name, opts := range map[string]ControllerOptions{
			"gate closed":  enabled(gateFor(failedS)),
			"switched off": {UtilsHAEnabled: false, UtilsProvenanceGate: open},
		} {
			ui := run(t, ms, opts)
			assert.Nil(t, ui.Apply, name)
			assert.EqualValues(t, 2, ui.Plan.Replicas, name)
		}
	})
	t.Run("a recorded Deployment that vanished is rebuilt while the path is allowed", func(t *testing.T) {
		ms := recorded()
		ui := run(t, ms, enabled(open))
		_, ok := ui.Apply.(*appsv1.Deployment)
		assert.True(t, ok, "got %T", ui.Apply)
	})
	t.Run("a recorded HA instance whose concurrency now leaves room for one replica gets no bare pod", func(t *testing.T) {
		ms := recorded()
		c, _ := newFakeClient(mgrScheme)
		ui, err := (&Reconciler{ControllerOptions: enabled(open), Client: c}).utilsInfraObject(context.Background(), log, ms, preparedUtilsPod("1"))
		require.NoError(t, err)
		assert.Nil(t, ui.Apply)
		assert.EqualValues(t, 2, ui.Plan.Replicas)
	})
	t.Run("the reverse transition: the bare pod exists and the gate opens, no Deployment is added", func(t *testing.T) {
		ms := haMS()
		bare := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: UtilsDeploymentName, Namespace: haNS}}
		// Reconciliation 1 had a closed gate and created the bare pod. Reconciliation 2 has an open gate.
		ui := run(t, ms, enabled(open), bare)
		_, isPod := ui.Apply.(*corev1.Pod)
		assert.True(t, isPod, "got %T", ui.Apply)
		assert.Nil(t, ui.Deployment)
		assert.EqualValues(t, 1, ui.Plan.Replicas)
		// And with the HA switch on and replicas requested, still the bare form.
		ui = run(t, ms, ControllerOptions{UtilsHAEnabled: true, UtilsReplicas: 3, UtilsProvenanceGate: open}, bare)
		_, isPod = ui.Apply.(*corev1.Pod)
		assert.True(t, isPod)
	})
	t.Run("an instance that never had HA keeps the bare pod behavior", func(t *testing.T) {
		ui := run(t, haMS(), enabled(gateFor(failedS)))
		_, bare := ui.Apply.(*corev1.Pod)
		assert.True(t, bare)
		assert.EqualValues(t, 1, ui.Plan.Replicas)
	})
}
