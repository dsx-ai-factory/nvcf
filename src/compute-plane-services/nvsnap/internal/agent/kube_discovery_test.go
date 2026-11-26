// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func kubeRank(ns, name, uid, ip, discovery string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid)},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Env: []corev1.EnvVar{
			{Name: "DYN_NAMESPACE", Value: "qwen"},
			{Name: "DYN_DISCOVERY_BACKEND", Value: discovery},
		}}}},
		Status: corev1.PodStatus{PodIP: ip},
	}
}

// workerSpec is a DynamoWorkerMetadata spec as Dynamo 1.5 writes it.
func workerSpec(ip string) map[string]any {
	return map[string]any{"data": map[string]any{
		"endpoints": map[string]any{"qwen/backend/generate/1c4c": map[string]any{
			"instance_id": int64(7965170713353809), "namespace": "qwen",
			"transport": map[string]any{"tcp": ip + ":44325/1c4c47d2f1de51/generate"},
		}},
		"event_channels": map[string]any{"kv": map[string]any{
			"transport": map[string]any{"kind": "Zmq", "config": map[string]any{"endpoint": "tcp://" + ip + ":34203"}},
		}},
		"model_cards": map[string]any{"qwen/backend/generate/1c4c": map[string]any{
			"card_json": map[string]any{
				"display_name": "Qwen 1.2.3.4",
				"extra_files":  []any{map[string]any{"path": "http://" + ip + ":9090/v1/metadata/qwen/tokenizer.json"}},
			},
		}},
	}}
}

func workerCR(ns, name, ownerName, ownerUID string, controller bool, spec map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "nvidia.com/v1alpha1", "kind": "DynamoWorkerMetadata",
		"metadata": map[string]any{"name": name, "namespace": ns},
		"spec":     spec,
	}}
	u.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: ownerName, UID: types.UID(ownerUID), Controller: &controller}})
	return u
}

func fakeDyn(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{dynamoWorkerMetadata: "DynamoWorkerMetadataList"}, objs...)
}

func TestReplaceIPv4WholeAddressOnly(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"10.0.0.1:80/x", "10.9.9.9:80/x"},
		{"tcp://10.0.0.1:5", "tcp://10.9.9.9:5"},
		{"10.0.0.12:80", "10.0.0.12:80"},  // a longer address
		{"110.0.0.1:80", "110.0.0.1:80"},  // a longer address
		{"10.0.0.1.5", "10.0.0.1.5"},      // not an address
		{"a10.0.0.1", "a10.9.9.9"},        // a letter bounds it
		{"10.0.0.1,10.0.0.1", "10.9.9.9,10.9.9.9"},
	} {
		if got := replaceIPv4(tc.in, "10.0.0.1", "10.9.9.9"); got != tc.want {
			t.Errorf("replaceIPv4(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestIPv4Tokens(t *testing.T) {
	got := ipv4Tokens("tcp://10.0.0.1:5/1.2.3/4.5.6.7.8 and 192.168.1.20")
	want := []string{"10.0.0.1", "192.168.1.20"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReplacePodIP(t *testing.T) {
	raw, _ := json.Marshal(workerSpec("10.0.0.1"))
	got, err := replacePodIP(raw, "10.0.0.1", "10.9.9.9")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(got)
	s := string(b)
	// The model card's files are fetched from the worker's system port by
	// a frontend that has not loaded the model.
	for _, want := range []string{`"tcp":"10.9.9.9:44325/1c4c47d2f1de51/generate"`, `"endpoint":"tcp://10.9.9.9:34203"`,
		`"path":"http://10.9.9.9:9090/v1/metadata/qwen/tokenizer.json"`, `"display_name":"Qwen 1.2.3.4"`} {
		if !strings.Contains(s, want) {
			t.Errorf("spec lacks %s: %s", want, s)
		}
	}
	// The instance ID survives as the integer it was, not a float.
	if !strings.Contains(s, `"instance_id":7965170713353809`) {
		t.Errorf("instance_id changed: %s", s)
	}
}

func TestRegistrationsOf(t *testing.T) {
	ctx := context.Background()
	pod := kubeRank("a", "worker-x", "u1", "10.0.0.1", "kubernetes")
	a := &Agent{dynClient: fakeDyn(
		workerCR("a", "worker-x", "worker-x", "u1", true, workerSpec("10.0.0.1")),
		workerCR("a", "worker-x-sidecar", "worker-x", "u1", false, workerSpec("10.0.0.1")),
		workerCR("a", "worker-y", "worker-y", "u2", true, workerSpec("10.0.0.2")), // another pod's
	)}
	got, err := a.registrationsOf(ctx, pod)
	if err != nil {
		t.Fatal(err)
	}
	if got.PodIP != "10.0.0.1" || len(got.Objects) != 2 {
		t.Fatalf("got %+v, want the pod's two objects at 10.0.0.1", got)
	}
	suffixes := map[string]bool{}
	for _, o := range got.Objects {
		suffixes[o.Suffix] = o.Controller
	}
	if c, ok := suffixes[""]; !ok || !c {
		t.Errorf("main container's object: %v", suffixes)
	}
	if c, ok := suffixes["-sidecar"]; !ok || c {
		t.Errorf("sidecar's object: %v", suffixes)
	}
}

func TestRegistrationsOfFailsClosed(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		a    *Agent
		pod  *corev1.Pod
		want string
	}{
		{"no client", &Agent{}, kubeRank("a", "w", "u1", "10.0.0.1", "kubernetes"), "no client"},
		{"not registered", &Agent{dynClient: fakeDyn()}, kubeRank("a", "w", "u1", "10.0.0.1", "kubernetes"), "has not registered"},
		{"no address", &Agent{dynClient: fakeDyn()}, kubeRank("a", "w", "u1", "", "kubernetes"), "no IPv4"},
		{"another address", &Agent{dynClient: fakeDyn(workerCR("a", "w", "w", "u1", true, workerSpec("10.0.0.7")))},
			kubeRank("a", "w", "u1", "10.0.0.1", "kubernetes"), "serves at 10.0.0.7"},
		{"a URL at another address", &Agent{dynClient: fakeDyn(workerCR("a", "w", "w", "u1", true, func() map[string]any {
			s := workerSpec("10.0.0.1")
			card := s["data"].(map[string]any)["model_cards"].(map[string]any)["qwen/backend/generate/1c4c"].(map[string]any)["card_json"].(map[string]any)
			card["extra_files"] = []any{map[string]any{"path": "http://10.0.0.8:9090/x"}}
			return s
		}()))}, kubeRank("a", "w", "u1", "10.0.0.1", "kubernetes"), "serves at 10.0.0.8"},
		{"misnamed", &Agent{dynClient: fakeDyn(workerCR("a", "other", "w", "u1", true, workerSpec("10.0.0.1")))},
			kubeRank("a", "w", "u1", "10.0.0.1", "kubernetes"), "not named after it"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.a.registrationsOf(ctx, tc.pod); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRecordRegistrationsSkipsOtherRanks(t *testing.T) {
	a := &Agent{dynClient: fakeDyn(workerCR("a", "w1", "w1", "u1", true, workerSpec("10.0.0.1")))}
	data, err := a.recordRegistrations(context.Background(), []*corev1.Pod{
		kubeRank("a", "w0", "u0", "10.0.0.0", "etcd"),
		kubeRank("a", "w1", "u1", "10.0.0.1", "kubernetes"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := data[fmt.Sprintf(kubeRegistrationKey, 0)]; ok || len(data) != 1 || data[fmt.Sprintf(kubeRegistrationKey, 1)] == "" {
		t.Errorf("got keys %v, want rank 1's only", data)
	}
}

func TestWriteRegistrations(t *testing.T) {
	ctx := context.Background()
	src := &Agent{dynClient: fakeDyn(workerCR("a", "w-old", "w-old", "u-old", true, workerSpec("10.0.0.1")))}
	record, err := src.recordRegistrations(ctx, []*corev1.Pod{kubeRank("a", "w-old", "u-old", "10.0.0.1", "kubernetes")})
	if err != nil {
		t.Fatal(err)
	}
	dyn := fakeDyn()
	dst := &Agent{dynClient: dyn}
	placeholder := kubeRank("b", "w-new", "u-new", "10.0.0.2", "kubernetes")
	if err := dst.writeRegistrations(ctx, record, []*corev1.Pod{placeholder}); err != nil {
		t.Fatal(err)
	}
	got, err := dyn.Resource(dynamoWorkerMetadata).Namespace("b").Get(ctx, "w-new", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	refs := got.GetOwnerReferences()
	if len(refs) != 1 || refs[0].UID != "u-new" || refs[0].Name != "w-new" || refs[0].Controller == nil || !*refs[0].Controller {
		t.Errorf("owner %+v, want the placeholder as controller", refs)
	}
	b, _ := json.Marshal(got.Object["spec"])
	if !strings.Contains(string(b), "10.0.0.2:44325/1c4c47d2f1de51/generate") || !strings.Contains(string(b), "http://10.0.0.2:9090/") || strings.Contains(string(b), "10.0.0.1") {
		t.Errorf("transports not moved to the placeholder's address: %s", b)
	}
	// Driven again: the placeholder's own object is overwritten.
	if err := dst.writeRegistrations(ctx, record, []*corev1.Pod{placeholder}); err != nil {
		t.Errorf("second write: %v", err)
	}
}

func TestWriteRegistrationsFailsClosed(t *testing.T) {
	ctx := context.Background()
	src := &Agent{dynClient: fakeDyn(workerCR("a", "w-old", "w-old", "u-old", true, workerSpec("10.0.0.1")))}
	record, err := src.recordRegistrations(ctx, []*corev1.Pod{kubeRank("a", "w-old", "u-old", "10.0.0.1", "kubernetes")})
	if err != nil {
		t.Fatal(err)
	}
	placeholder := kubeRank("b", "w-new", "u-new", "10.0.0.2", "kubernetes")
	// Another pod's object of that name.
	taken := &Agent{dynClient: fakeDyn(workerCR("b", "w-new", "w-new", "u-other", true, workerSpec("10.0.0.3")))}
	if err := taken.writeRegistrations(ctx, record, []*corev1.Pod{placeholder}); err == nil {
		t.Error("overwrote another pod's registration")
	}
	// A Kubernetes rank whose capture kept nothing.
	if err := (&Agent{dynClient: fakeDyn()}).writeRegistrations(ctx, map[string]string{}, []*corev1.Pod{placeholder}); err == nil {
		t.Error("restored a Kubernetes rank without its registration")
	}
	// An etcd rank needs nothing.
	if err := (&Agent{}).writeRegistrations(ctx, map[string]string{}, []*corev1.Pod{kubeRank("b", "e", "u", "10.0.0.4", "etcd")}); err != nil {
		t.Errorf("etcd rank: %v", err)
	}
	// No address yet.
	noIP := kubeRank("b", "w-new", "u-new", "", "kubernetes")
	if err := (&Agent{dynClient: fakeDyn()}).writeRegistrations(ctx, record, []*corev1.Pod{noIP}); err == nil {
		t.Error("wrote a registration without the placeholder's address")
	}
}
