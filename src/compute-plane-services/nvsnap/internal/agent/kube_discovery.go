// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Dynamo components that register through Kubernetes.
//
// With DYN_DISCOVERY_BACKEND=kubernetes a Dynamo worker has no etcd to
// capture with it. It writes its registration once, at startup, into a
// DynamoWorkerMetadata object in its namespace: named after its pod (and
// container), owned by its pod, listing its endpoints with the addresses
// it serves them at. A frontend routes to a worker when the worker's
// container is ready and an object of that name, owned by that pod,
// exists.
//
// A restored worker never registers again, and its replacement pod has
// another name, UID and IP. So a capture keeps each rank's registration
// objects and the rank's address (registrationsOf); the restore writes
// them again for the placeholder pod it restores into, named after it,
// owned by it, with the captured address replaced by the placeholder's
// wherever the objects name it (writeRegistrations): the endpoints'
// transports, and the URLs of the model card's files on the worker's
// system port, from which a frontend that has not loaded the model yet
// fetches its tokenizer. The worker's instance
// IDs in the objects stay those it was captured with: the restored
// process answers to them.
//
// Both ends fail closed. A rank whose registration cannot be read, or
// whose transports or URLs name an address other than its pod's, is not
// captured. A restore whose registrations cannot be written does not run:
// its pods start cold, rather than run ready with no route to them.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// dynamoWorkerMetadata is the resource a Dynamo component registers
// itself as under Kubernetes discovery.
var dynamoWorkerMetadata = schema.GroupVersionResource{Group: "nvidia.com", Version: "v1alpha1", Resource: "dynamoworkermetadatas"}

// kubeRegistrationKey in a group record holds rank i's registrations
// (rankRegistrations, JSON).
const kubeRegistrationKey = "kubeRegistrations.%d"

// kubeRegistrationBudget bounds the registrations one capture keeps in
// its group record, which shares a ConfigMap's 1 MiB with the rest of
// the record. A Dynamo worker's object is a few KiB (its model card).
const kubeRegistrationBudget = 256 << 10

// kubeDiscoveryRank reports a Dynamo component that registers as
// Kubernetes objects in its namespace.
func kubeDiscoveryRank(pod *corev1.Pod) bool {
	return webhook.DynamoComponent(pod) && webhook.DynamoDiscovery(pod) == "kubernetes"
}

// workerRegistration is one DynamoWorkerMetadata object of a rank.
type workerRegistration struct {
	// Suffix is the object's name after its pod's name: "" for the pod's
	// main container, "-<container>" for another.
	Suffix string `json:"suffix"`
	// Controller is whether the pod is the object's controlling owner.
	Controller bool            `json:"controller"`
	Spec       json.RawMessage `json:"spec"`
}

// rankRegistrations is what a capture keeps of a rank's registration.
type rankRegistrations struct {
	PodIP   string               `json:"podIP"`
	Objects []workerRegistration `json:"objects"`
}

// registrationsOf reads the DynamoWorkerMetadata objects pod owns, to
// keep with its capture.
func (a *Agent) registrationsOf(ctx context.Context, pod *corev1.Pod) (rankRegistrations, error) {
	var out rankRegistrations
	if a.dynClient == nil {
		return out, fmt.Errorf("no client for custom resources")
	}
	ip := pod.Status.PodIP
	if net.ParseIP(ip).To4() == nil {
		return out, fmt.Errorf("pod %s has no IPv4 address (%q)", pod.Name, ip)
	}
	list, err := a.dynClient.Resource(dynamoWorkerMetadata).Namespace(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return out, fmt.Errorf("list DynamoWorkerMetadata in %s: %w", pod.Namespace, err)
	}
	out.PodIP = ip
	for i := range list.Items {
		o := &list.Items[i]
		owner, controller := false, false
		for _, r := range o.GetOwnerReferences() {
			if r.UID == pod.UID {
				owner, controller = true, r.Controller != nil && *r.Controller
			}
		}
		if !owner {
			continue
		}
		if !strings.HasPrefix(o.GetName(), pod.Name) {
			return out, fmt.Errorf("DynamoWorkerMetadata %s is owned by pod %s but not named after it", o.GetName(), pod.Name)
		}
		spec, found, err := unstructured.NestedFieldNoCopy(o.Object, "spec")
		if err != nil || !found {
			return out, fmt.Errorf("DynamoWorkerMetadata %s has no spec", o.GetName())
		}
		raw, err := json.Marshal(spec)
		if err != nil {
			return out, fmt.Errorf("DynamoWorkerMetadata %s: %w", o.GetName(), err)
		}
		if other := foreignAddress(raw, ip); other != "" {
			// An address the restore would not know to replace.
			return out, fmt.Errorf("DynamoWorkerMetadata %s serves at %s, not at its pod's address %s", o.GetName(), other, ip)
		}
		out.Objects = append(out.Objects, workerRegistration{Suffix: strings.TrimPrefix(o.GetName(), pod.Name), Controller: controller, Spec: raw})
	}
	if len(out.Objects) == 0 {
		return out, fmt.Errorf("pod %s owns no DynamoWorkerMetadata: it has not registered", pod.Name)
	}
	return out, nil
}

// recordRegistrations reads every Kubernetes-discovery rank's
// registrations into data, a group record's capture outcome.
func (a *Agent) recordRegistrations(ctx context.Context, ranks []*corev1.Pod) (map[string]string, error) {
	data := map[string]string{}
	total := 0
	for i, r := range ranks {
		if !kubeDiscoveryRank(r) {
			continue
		}
		regs, err := a.registrationsOf(ctx, r)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(regs)
		if err != nil {
			return nil, err
		}
		if total += len(b); total > kubeRegistrationBudget {
			return nil, fmt.Errorf("the ranks' registrations take more than %d bytes", kubeRegistrationBudget)
		}
		data[fmt.Sprintf(kubeRegistrationKey, i)] = string(b)
	}
	return data, nil
}

// writeRegistrations writes, for every rank of a group restore, the
// registrations its capture kept (record, the group record's data), as
// the placeholder pod's.
func (a *Agent) writeRegistrations(ctx context.Context, record map[string]string, ranks []*corev1.Pod) error {
	for i, p := range ranks {
		v := record[fmt.Sprintf(kubeRegistrationKey, i)]
		if v == "" {
			if kubeDiscoveryRank(p) {
				return fmt.Errorf("rank %d (%s) registers through Kubernetes, and its capture kept no registration", i, p.Name)
			}
			continue
		}
		if a.dynClient == nil {
			return fmt.Errorf("no client for custom resources")
		}
		var regs rankRegistrations
		if err := json.Unmarshal([]byte(v), &regs); err != nil {
			return fmt.Errorf("rank %d: registrations: %w", i, err)
		}
		ip := p.Status.PodIP
		if net.ParseIP(ip).To4() == nil {
			return fmt.Errorf("placeholder %s has no IPv4 address (%q)", p.Name, ip)
		}
		for _, o := range regs.Objects {
			spec, err := replacePodIP(o.Spec, regs.PodIP, ip)
			if err != nil {
				return fmt.Errorf("rank %d: %w", i, err)
			}
			if err := a.applyRegistration(ctx, p, p.Name+o.Suffix, o.Controller, spec); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyRegistration creates DynamoWorkerMetadata name in pod's namespace,
// owned by pod, with spec; an object of that name pod already owns is
// overwritten (a restore driven again), any other is an error.
func (a *Agent) applyRegistration(ctx context.Context, pod *corev1.Pod, name string, controller bool, spec map[string]any) error {
	no := false
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": dynamoWorkerMetadata.GroupVersion().String(),
		"kind":       "DynamoWorkerMetadata",
		"metadata":   map[string]any{"name": name, "namespace": pod.Namespace},
		"spec":       spec,
	}}
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID,
		Controller: &controller, BlockOwnerDeletion: &no,
	}})
	api := a.dynClient.Resource(dynamoWorkerMetadata).Namespace(pod.Namespace)
	_, err := api.Create(ctx, obj, metav1.CreateOptions{})
	if !apierrors.IsAlreadyExists(err) {
		if err != nil {
			return fmt.Errorf("create DynamoWorkerMetadata %s/%s: %w", pod.Namespace, name, err)
		}
		return nil
	}
	cur, err := api.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get DynamoWorkerMetadata %s/%s: %w", pod.Namespace, name, err)
	}
	mine := false
	for _, r := range cur.GetOwnerReferences() {
		mine = mine || r.UID == pod.UID
	}
	if !mine {
		return fmt.Errorf("DynamoWorkerMetadata %s/%s exists and is not the placeholder's", pod.Namespace, name)
	}
	obj.SetResourceVersion(cur.GetResourceVersion())
	if _, err := api.Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update DynamoWorkerMetadata %s/%s: %w", pod.Namespace, name, err)
	}
	return nil
}

// replacePodIP returns spec (JSON) with the IPv4 address old replaced by
// new in every string, as a whole address only (10.0.0.1 is not part of
// 10.0.0.12).
func replacePodIP(spec json.RawMessage, old, new string) (map[string]any, error) {
	// Numbers decoded exactly: instance IDs are integers of up to 53 bits.
	d := json.NewDecoder(bytes.NewReader(spec))
	d.UseNumber()
	var v map[string]any
	if err := d.Decode(&v); err != nil {
		return nil, fmt.Errorf("registration spec: %w", err)
	}
	walkStrings(v, false, func(s string, _ bool) string { return replaceIPv4(s, old, new) })
	return v, nil
}

// foreignAddress returns an IPv4 address other than ip that spec names
// where a peer would connect to it: in a string under a "transport" key,
// or in a URL. "" when there is none.
func foreignAddress(spec json.RawMessage, ip string) string {
	var v any
	if err := json.Unmarshal(spec, &v); err != nil {
		return "unparseable spec"
	}
	other := ""
	walkStrings(v, false, func(s string, transport bool) string {
		if !transport && !strings.Contains(s, "://") {
			return s
		}
		for _, a := range ipv4Tokens(s) {
			if a != ip && other == "" {
				other = a
			}
		}
		return s
	})
	return other
}

// walkStrings replaces, in place, every string of v with f's result; f
// learns whether the string is under a "transport" key.
func walkStrings(v any, in bool, f func(s string, transport bool) string) any {
	switch t := v.(type) {
	case map[string]any:
		for k, e := range t {
			t[k] = walkStrings(e, in || k == "transport", f)
		}
		return t
	case []any:
		for i, e := range t {
			t[i] = walkStrings(e, in, f)
		}
		return t
	case string:
		return f(t, in)
	case json.Number:
		// As an unstructured object holds numbers.
		if i, err := t.Int64(); err == nil {
			return i
		}
		f, _ := t.Float64()
		return f
	default:
		return v
	}
}

// ipv4Tokens returns the dotted-quad IPv4 addresses in s that are not part
// of a longer run of digits and dots.
func ipv4Tokens(s string) []string {
	var out []string
	i := 0
	for i < len(s) {
		if !isDigitOrDot(s[i]) || (i > 0 && isDigitOrDot(s[i-1])) {
			i++
			continue
		}
		j := i
		for j < len(s) && isDigitOrDot(s[j]) {
			j++
		}
		if ip := net.ParseIP(s[i:j]); ip != nil && ip.To4() != nil && strings.Count(s[i:j], ".") == 3 {
			out = append(out, s[i:j])
		}
		i = j
	}
	return out
}

// replaceIPv4 replaces each whole occurrence of the address old in s.
func replaceIPv4(s, old, new string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if strings.HasPrefix(s[i:], old) && (i == 0 || !isDigitOrDot(s[i-1])) &&
			(i+len(old) == len(s) || !isDigitOrDot(s[i+len(old)])) {
			b.WriteString(new)
			i += len(old)
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

func isDigitOrDot(c byte) bool { return c == '.' || (c >= '0' && c <= '9') }

// writeGroupRegistrations is writeRegistrations for group record key.
func (c *criuAutoRestorer) writeGroupRegistrations(ctx context.Context, key string, ranks []*corev1.Pod) error {
	needed := false
	for _, p := range ranks {
		needed = needed || kubeDiscoveryRank(p)
	}
	cm, err := c.a.kubeClient.CoreV1().ConfigMaps(c.a.criuFailedNamespace()).Get(ctx, criuGroupConfigMapName(key), metav1.GetOptions{})
	if err != nil {
		if !needed && apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("group record: %w", err)
	}
	return c.a.writeRegistrations(ctx, cm.Data, ranks)
}
