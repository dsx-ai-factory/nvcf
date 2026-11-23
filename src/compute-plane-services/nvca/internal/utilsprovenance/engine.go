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

// Package utilsprovenance decides whether an admission request that creates or changes an object carrying the
// reserved utils metadata is allowed. Only NVCA may write the reserved keys. Pods created from NVCA's utils
// Deployment are accepted when the owner chain Pod, ReplicaSet, Deployment, MiniService verifies by UID. The package
// holds no state and does no admission I/O, so every rule is unit tested without a cluster.
package utilsprovenance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

const (
	// PodTemplateHashLabel is added by the Deployment controller to ReplicaSets and their pods.
	PodTemplateHashLabel = "pod-template-hash"
	// DeploymentAnnotationPrefix is the prefix of annotations the Deployment controller keeps on ReplicaSets.
	DeploymentAnnotationPrefix = "deployment.kubernetes.io/"

	// DefaultLookupTimeout bounds all lookups made for one decision.
	DefaultLookupTimeout = 2 * time.Second
)

// Operation is the admission operation.
type Operation string

// Operations the engine understands.
const (
	OperationCreate Operation = "CREATE"
	OperationUpdate Operation = "UPDATE"
)

// Kind is the kind of the object in the request.
type Kind string

// Kinds the engine understands.
const (
	KindPod        Kind = "Pod"
	KindReplicaSet Kind = "ReplicaSet"
	KindDeployment Kind = "Deployment"
	// KindScale is a deployments/scale request. Only Name and Namespace are meaningful, the Scale object does not
	// carry the Deployment template or reserved keys.
	KindScale Kind = "Scale"
)

// Reason is a fixed, bounded set of decision reasons, safe to use as a metric label.
type Reason string

// Decision reasons.
const (
	ReasonUnprotected        Reason = "unprotected"
	ReasonNVCAWriter         Reason = "nvca_writer"
	ReasonChainVerified      Reason = "chain_verified"
	ReasonControllerAllowed  Reason = "controller_allowed"
	ReasonLegacyUnchanged    Reason = "legacy_unchanged"
	ReasonDenyWriter         Reason = "deny_writer"
	ReasonDenyProtectedEdit  Reason = "deny_protected_edit"
	ReasonDenyNoOwner        Reason = "deny_no_owner"
	ReasonDenyUIDMismatch    Reason = "deny_uid_mismatch"
	ReasonDenyTemplate       Reason = "deny_template_mismatch"
	ReasonDenyMiniService    Reason = "deny_miniservice"
	ReasonDenyControllerEdit Reason = "deny_controller_edit"
	ReasonRetryLookup        Reason = "retry_lookup"
)

// AllReasons lists every Reason, for metric pre-initialization.
var AllReasons = []Reason{
	ReasonUnprotected, ReasonNVCAWriter, ReasonChainVerified, ReasonControllerAllowed, ReasonLegacyUnchanged,
	ReasonDenyWriter, ReasonDenyProtectedEdit, ReasonDenyNoOwner, ReasonDenyUIDMismatch, ReasonDenyTemplate,
	ReasonDenyMiniService, ReasonDenyControllerEdit, ReasonRetryLookup,
}

// WebhookName is the name of the validating webhook that applies these rules. The readiness check matches it in the
// refusal message, so a refusal from another webhook never counts as proof that enforcement is active.
const WebhookName = "validate-utils-provenance.nvca.nvcf.nvidia.io"

// denialPrefix starts every refusal message, followed by the reason in brackets.
const denialPrefix = "utils-provenance["

// FormatDenial returns the refusal message for a decision, carrying the reason in a stable, parseable form.
func FormatDenial(d Decision) string {
	return fmt.Sprintf("%s%s]: %s", denialPrefix, d.Reason, d.Message)
}

// ParseDenial extracts the reason from a refusal message made by FormatDenial. The message the API server returns
// wraps it, so it searches for the marker instead of requiring a prefix.
func ParseDenial(msg string) (Reason, bool) {
	i := strings.Index(msg, denialPrefix)
	if i < 0 {
		return "", false
	}
	rest := msg[i+len(denialPrefix):]
	j := strings.Index(rest, "]")
	if j <= 0 {
		return "", false
	}
	return Reason(rest[:j]), true
}

// Decision is the outcome of one evaluation. The caller applies the audit or enforce mode.
type Decision struct {
	Allowed bool
	// Retryable is true when the request was refused only because a lookup failed, so a retry can succeed.
	Retryable bool
	Reason    Reason
	Message   string
}

func allow(r Reason) Decision { return Decision{Allowed: true, Reason: r} }

func deny(r Reason, format string, a ...any) Decision {
	return Decision{Reason: r, Message: fmt.Sprintf(format, a...)}
}

func retry(format string, a ...any) Decision {
	return Decision{Reason: ReasonRetryLookup, Retryable: true, Message: fmt.Sprintf(format, a...)}
}

// Identities holds the usernames the engine trusts. An empty name never matches.
type Identities struct {
	// NVCA is the username of NVCA's own service account.
	NVCA string
	// ReplicaSetController and DeploymentController are the kube controller usernames.
	ReplicaSetController string
	DeploymentController string
}

// Default controller usernames, for clusters that run controllers with per-controller service accounts.
const (
	DefaultReplicaSetController = "system:serviceaccount:kube-system:replicaset-controller"
	DefaultDeploymentController = "system:serviceaccount:kube-system:deployment-controller"
)

func is(have, want string) bool { return want != "" && have == want }

// MiniServiceRef is what the engine needs to know about a MiniService.
type MiniServiceRef struct {
	UID types.UID
	// Namespace is the instance namespace the MiniService owns.
	Namespace string
}

// Lookup reads the objects of the owner chain. Implementations should serve from informers and fall back to a
// direct GET by name on a miss. Kubernetes cannot GET by UID, so the engine always compares UIDs itself.
type Lookup interface {
	GetReplicaSet(ctx context.Context, namespace, name string) (*appsv1.ReplicaSet, error)
	GetDeployment(ctx context.Context, namespace, name string) (*appsv1.Deployment, error)
	// GetMiniService reads the cluster scoped MiniService by name.
	GetMiniService(ctx context.Context, name string) (*MiniServiceRef, error)
	// FindMiniServiceByNamespace returns the MiniService that owns the instance namespace.
	FindMiniServiceByNamespace(ctx context.Context, namespace string) (*MiniServiceRef, error)
}

// Request is one admission request reduced to what the rules need.
type Request struct {
	Operation Operation
	Kind      Kind
	// Subresource is empty for the main resource, "ephemeralcontainers" for that pod subresource.
	Subresource string
	Username    string
	Namespace   string
	Name        string
	// Object and OldObject are *corev1.Pod, *appsv1.ReplicaSet or *appsv1.Deployment. OldObject is nil on create.
	// Both are nil for KindScale.
	Object    client.Object
	OldObject client.Object
}

// Engine evaluates requests.
type Engine struct {
	Identities    Identities
	Lookup        Lookup
	LookupTimeout time.Duration
}

// Evaluate returns the decision for r. It never mutates r.
func (e *Engine) Evaluate(ctx context.Context, r Request) Decision {
	timeout := e.LookupTimeout
	if timeout <= 0 {
		timeout = DefaultLookupTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	switch r.Kind {
	case KindPod:
		return e.evalPod(ctx, r)
	case KindReplicaSet:
		return e.evalReplicaSet(ctx, r)
	case KindDeployment:
		return e.evalDeployment(ctx, r)
	case KindScale:
		return e.evalScale(ctx, r)
	default:
		return allow(ReasonUnprotected)
	}
}

func (e *Engine) isNVCA(r Request) bool { return is(r.Username, e.Identities.NVCA) }

// ---- reserved key helpers ----

func hasRoleLabel(labels map[string]string) bool {
	return labels[nvcatypes.PodRoleLabelKey] == nvcatypes.PodRoleValueUtils
}

func hasInfraAnnotation(ann map[string]string) bool {
	return ann[nvcatypes.InfraObjectAnnotationKey] != ""
}

func hasMiniServiceOwner(refs []metav1.OwnerReference) bool {
	for _, o := range refs {
		if o.Kind == "MiniService" {
			return true
		}
	}
	return false
}

func controllerRef(refs []metav1.OwnerReference, kind string) *metav1.OwnerReference {
	for i := range refs {
		if refs[i].Kind == kind && refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	return nil
}

func withoutHash(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		if k != PodTemplateHashLabel {
			out[k] = v
		}
	}
	return out
}

func sameLabels(a, b map[string]string) bool {
	return apiequality.Semantic.DeepEqual(withoutHash(a), withoutHash(b))
}

// lookupFailure maps a lookup error to a retryable decision. Context expiry and not found both mean the object may
// simply not be visible yet.
func lookupFailure(what string, err error) Decision {
	switch {
	case apierrors.IsNotFound(err), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return retry("could not verify %s yet, retry: %v", what, err)
	default:
		return retry("could not read %s, retry: %v", what, err)
	}
}

// ---- Pod ----

func (e *Engine) evalPod(ctx context.Context, r Request) Decision {
	pod, _ := r.Object.(*corev1.Pod)
	old, _ := r.OldObject.(*corev1.Pod)
	if pod == nil {
		return allow(ReasonUnprotected)
	}
	switch r.Operation {
	case OperationCreate:
		return e.podCreate(ctx, r, pod)
	case OperationUpdate:
		return e.podUpdate(r, pod, old)
	}
	return allow(ReasonUnprotected)
}

func (e *Engine) podCreate(ctx context.Context, r Request, pod *corev1.Pod) Decision {
	claimsRole := hasRoleLabel(pod.Labels)
	claimsInfra := hasInfraAnnotation(pod.Annotations)
	if !claimsRole && !claimsInfra {
		return allow(ReasonUnprotected)
	}
	switch {
	case e.isNVCA(r):
		return e.nvcaBarePod(ctx, r, claimsRole)
	case is(r.Username, e.Identities.ReplicaSetController):
		return e.chainPod(ctx, r, pod)
	default:
		return deny(ReasonDenyWriter, "only NVCA may create objects with reserved utils metadata, got %q", r.Username)
	}
}

// nvcaBarePod accepts NVCA's own pods only in the namespace of a live MiniService.
func (e *Engine) nvcaBarePod(ctx context.Context, r Request, claimsRole bool) Decision {
	if !claimsRole {
		// The legacy utils pod carries only the infra annotation. NVCA writes it as before.
		return allow(ReasonNVCAWriter)
	}
	ms, err := e.Lookup.FindMiniServiceByNamespace(ctx, r.Namespace)
	if err != nil {
		return lookupFailure("the MiniService of the namespace", err)
	}
	if ms.Namespace != r.Namespace {
		return deny(ReasonDenyMiniService, "namespace %q is not owned by the MiniService", r.Namespace)
	}
	return allow(ReasonNVCAWriter)
}

// chainPod verifies Pod, ReplicaSet, Deployment, MiniService by UID. The pod is compared with the ReplicaSet that
// owns it, not with the current Deployment template, so pods of an older revision stay valid during a rollout.
func (e *Engine) chainPod(ctx context.Context, r Request, pod *corev1.Pod) Decision {
	rsRef := controllerRef(pod.OwnerReferences, "ReplicaSet")
	if rsRef == nil {
		return deny(ReasonDenyNoOwner, "pod with reserved utils metadata has no controller ReplicaSet owner")
	}
	rs, err := e.Lookup.GetReplicaSet(ctx, r.Namespace, rsRef.Name)
	if err != nil {
		return lookupFailure("the ReplicaSet "+rsRef.Name, err)
	}
	if rs.UID != rsRef.UID {
		return deny(ReasonDenyUIDMismatch, "owner UID of ReplicaSet %q does not match", rsRef.Name)
	}
	if !hasRoleLabel(rs.Spec.Template.Labels) || !sameLabels(pod.Labels, rs.Spec.Template.Labels) {
		return deny(ReasonDenyTemplate, "pod labels do not match the template of ReplicaSet %q", rsRef.Name)
	}
	if d := e.verifyReplicaSetOwner(ctx, r.Namespace, rs); !d.Allowed {
		return d
	}
	return allow(ReasonChainVerified)
}

// verifyReplicaSetOwner checks the ReplicaSet's Deployment and that Deployment's MiniService.
func (e *Engine) verifyReplicaSetOwner(ctx context.Context, namespace string, rs *appsv1.ReplicaSet) Decision {
	dRef := controllerRef(rs.OwnerReferences, "Deployment")
	if dRef == nil {
		return deny(ReasonDenyNoOwner, "ReplicaSet %q has no controller Deployment owner", rs.Name)
	}
	dep, err := e.Lookup.GetDeployment(ctx, namespace, dRef.Name)
	if err != nil {
		return lookupFailure("the Deployment "+dRef.Name, err)
	}
	if dep.UID != dRef.UID {
		return deny(ReasonDenyUIDMismatch, "owner UID of Deployment %q does not match", dRef.Name)
	}
	if !hasRoleLabel(dep.Spec.Template.Labels) {
		return deny(ReasonDenyTemplate, "Deployment %q template does not carry the reserved label", dep.Name)
	}
	return e.verifyDeploymentMiniService(ctx, namespace, dep)
}

func (e *Engine) verifyDeploymentMiniService(ctx context.Context, namespace string, dep *appsv1.Deployment) Decision {
	mRef := controllerRef(dep.OwnerReferences, "MiniService")
	if mRef == nil {
		return deny(ReasonDenyMiniService, "Deployment %q has no controller MiniService owner", dep.Name)
	}
	ms, err := e.Lookup.GetMiniService(ctx, mRef.Name)
	if err != nil {
		return lookupFailure("the MiniService "+mRef.Name, err)
	}
	if ms.UID != mRef.UID {
		return deny(ReasonDenyUIDMismatch, "owner UID of MiniService %q does not match", mRef.Name)
	}
	if ms.Namespace != namespace {
		return deny(ReasonDenyMiniService, "MiniService %q does not own namespace %q", mRef.Name, namespace)
	}
	return allow(ReasonChainVerified)
}

// Classifier decides whether a pod being admitted is a utils pod of an instance, by verified provenance. Webhooks that
// treat utils pods differently from chart pods (shared storage instead of metadata injection) must use it for pods that
// are not the legacy bare pod, instead of trusting the label.
type Classifier interface {
	ClassifyUtilsPod(ctx context.Context, username, namespace string, pod *corev1.Pod, create bool) (bool, Decision)
}

// ClassifyUtilsPod reports whether pod is a utils pod, and the decision that led to it.
//
// A pod without the reserved label is not claiming to be utils: (false, unprotected). A labeled pod is utils only when
// its provenance verifies. On create the caller must be NVCA, or the ReplicaSet controller with a verified owner chain
// (pod, ReplicaSet, Deployment, MiniService, by UID). On update the pod already exists, so the chain alone decides:
// who is updating it is judged by the update rules, not by classification.
//
// A labeled pod whose provenance cannot be verified because a lookup failed gets a retryable decision. The caller must
// not fall back to treating it as an ordinary chart pod: it would receive the wrong mutation and cannot be repaired.
// A labeled pod that fails verification for good is not utils (false, deny decision), and the validating webhook
// decides what happens to it.
func (e *Engine) ClassifyUtilsPod(ctx context.Context, username, namespace string, pod *corev1.Pod, create bool) (bool, Decision) {
	if !hasRoleLabel(pod.Labels) {
		return false, allow(ReasonUnprotected)
	}
	timeout := e.LookupTimeout
	if timeout <= 0 {
		timeout = DefaultLookupTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	r := Request{Operation: OperationCreate, Kind: KindPod, Username: username, Namespace: namespace, Name: pod.Name, Object: pod}
	var d Decision
	if create {
		d = e.podCreate(ctx, r, pod)
	} else {
		d = e.chainPod(ctx, r, pod)
	}
	return d.Allowed, d
}

func (e *Engine) podUpdate(r Request, pod, old *corev1.Pod) Decision {
	if old == nil {
		old = &corev1.Pod{}
	}
	if e.isNVCA(r) {
		return allow(ReasonNVCAWriter)
	}
	oldRole, newRole := hasRoleLabel(old.Labels), hasRoleLabel(pod.Labels)
	oldInfra, newInfra := old.Annotations[nvcatypes.InfraObjectAnnotationKey], pod.Annotations[nvcatypes.InfraObjectAnnotationKey]

	if !oldRole && !newRole {
		// Legacy and chart pods: only the infra annotation is guarded. Unchanged updates are not blocked, so
		// existing behavior holds (kubelet writes status through a subresource that is not in scope).
		if oldInfra != newInfra {
			return deny(ReasonDenyWriter, "only NVCA may add, change or remove the infra-object annotation")
		}
		if oldInfra != "" {
			return allow(ReasonLegacyUnchanged)
		}
		return allow(ReasonUnprotected)
	}
	if !oldRole && newRole {
		return deny(ReasonDenyWriter, "only NVCA may add the reserved utils label")
	}
	// The old pod is protected. Only the ReplicaSet controller may touch it, and only its own owner reference.
	if r.Subresource != "" || !is(r.Username, e.Identities.ReplicaSetController) {
		return deny(ReasonDenyProtectedEdit, "protected utils pods can only be changed by NVCA")
	}
	if !apiequality.Semantic.DeepEqual(old.Labels, pod.Labels) ||
		!apiequality.Semantic.DeepEqual(old.Annotations, pod.Annotations) ||
		!apiequality.Semantic.DeepEqual(old.Spec, pod.Spec) ||
		!onlyReplicaSetOwnerChanged(old.OwnerReferences, pod.OwnerReferences) {
		return deny(ReasonDenyControllerEdit, "the ReplicaSet controller may only change its own owner reference")
	}
	return allow(ReasonControllerAllowed)
}

// onlyReplicaSetOwnerChanged reports whether the two reference lists differ at most in ReplicaSet references.
func onlyReplicaSetOwnerChanged(oldRefs, newRefs []metav1.OwnerReference) bool {
	return apiequality.Semantic.DeepEqual(nonReplicaSet(oldRefs), nonReplicaSet(newRefs))
}

func nonReplicaSet(refs []metav1.OwnerReference) []metav1.OwnerReference {
	var out []metav1.OwnerReference
	for _, o := range refs {
		if o.Kind != "ReplicaSet" {
			out = append(out, o)
		}
	}
	return out
}

// ---- ReplicaSet ----

func rsProtected(rs *appsv1.ReplicaSet) bool {
	return rs != nil && (hasRoleLabel(rs.Spec.Template.Labels) || hasRoleLabel(rs.Labels) || hasInfraAnnotation(rs.Annotations))
}

func (e *Engine) evalReplicaSet(ctx context.Context, r Request) Decision {
	rs, _ := r.Object.(*appsv1.ReplicaSet)
	old, _ := r.OldObject.(*appsv1.ReplicaSet)
	if rs == nil {
		return allow(ReasonUnprotected)
	}
	if e.isNVCA(r) {
		return allow(ReasonNVCAWriter)
	}
	switch r.Operation {
	case OperationCreate:
		if !rsProtected(rs) {
			return allow(ReasonUnprotected)
		}
		if !is(r.Username, e.Identities.DeploymentController) {
			return deny(ReasonDenyWriter, "only NVCA or the Deployment controller may create a protected ReplicaSet")
		}
		return e.deploymentControllerCreatesRS(ctx, r.Namespace, rs)
	case OperationUpdate:
		if old == nil {
			old = &appsv1.ReplicaSet{}
		}
		if !rsProtected(old) && !rsProtected(rs) {
			return allow(ReasonUnprotected)
		}
		if !rsProtected(old) {
			return deny(ReasonDenyWriter, "only NVCA may add reserved utils metadata to a ReplicaSet")
		}
		if !is(r.Username, e.Identities.DeploymentController) {
			return deny(ReasonDenyProtectedEdit, "protected ReplicaSets can only be changed by NVCA or the Deployment controller")
		}
		if !onlyReplicasOrDeploymentAnnotations(old, rs) {
			return deny(ReasonDenyControllerEdit,
				"the Deployment controller may only change replicas and %s* annotations of a protected ReplicaSet",
				DeploymentAnnotationPrefix)
		}
		return allow(ReasonControllerAllowed)
	}
	return allow(ReasonUnprotected)
}

// deploymentControllerCreatesRS accepts a new ReplicaSet only when its owner Deployment verifies and the ReplicaSet
// template equals that Deployment's template, ignoring only the controller-added hash label. NVCA's mutating webhooks
// act on pods, not on Deployment or ReplicaSet templates, so the Deployment controller copies the template unchanged
// and a full comparison is sound. A mismatch can also be a race: the Deployment changed after the controller read it.
// So a mismatch is retryable, not a hard refusal, and the controller recreates the ReplicaSet from the newer template.
func (e *Engine) deploymentControllerCreatesRS(ctx context.Context, namespace string, rs *appsv1.ReplicaSet) Decision {
	dRef := controllerRef(rs.OwnerReferences, "Deployment")
	if dRef == nil {
		return deny(ReasonDenyNoOwner, "protected ReplicaSet has no controller Deployment owner")
	}
	dep, err := e.Lookup.GetDeployment(ctx, namespace, dRef.Name)
	if err != nil {
		return lookupFailure("the Deployment "+dRef.Name, err)
	}
	if dep.UID != dRef.UID {
		return deny(ReasonDenyUIDMismatch, "owner UID of Deployment %q does not match", dRef.Name)
	}
	if !hasRoleLabel(dep.Spec.Template.Labels) {
		return deny(ReasonDenyTemplate, "Deployment %q template does not carry the reserved label", dep.Name)
	}
	if !sameTemplate(&dep.Spec.Template, &rs.Spec.Template) {
		return Decision{
			Reason: ReasonRetryLookup, Retryable: true,
			Message: fmt.Sprintf("ReplicaSet template differs from Deployment %q, retry with the current template", dRef.Name),
		}
	}
	if d := e.verifyDeploymentMiniService(ctx, namespace, dep); !d.Allowed {
		return d
	}
	return allow(ReasonChainVerified)
}

// sameTemplate compares two pod templates, ignoring the controller-added hash label.
func sameTemplate(a, b *corev1.PodTemplateSpec) bool {
	x, y := a.DeepCopy(), b.DeepCopy()
	x.Labels, y.Labels = withoutHash(x.Labels), withoutHash(y.Labels)
	return apiequality.Semantic.DeepEqual(x, y)
}

// onlyReplicasOrDeploymentAnnotations is true when old and new differ at most in spec.replicas and annotations
// that start with DeploymentAnnotationPrefix.
func onlyReplicasOrDeploymentAnnotations(old, cur *appsv1.ReplicaSet) bool {
	o, c := old.DeepCopy(), cur.DeepCopy()
	o.Spec.Replicas, c.Spec.Replicas = nil, nil
	o.Annotations, c.Annotations = stripPrefixed(o.Annotations), stripPrefixed(c.Annotations)
	// Only compare what the controller must not change.
	return apiequality.Semantic.DeepEqual(o.Spec, c.Spec) &&
		apiequality.Semantic.DeepEqual(o.Labels, c.Labels) &&
		apiequality.Semantic.DeepEqual(o.Annotations, c.Annotations) &&
		apiequality.Semantic.DeepEqual(o.OwnerReferences, c.OwnerReferences)
}

func stripPrefixed(in map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		if !strings.HasPrefix(k, DeploymentAnnotationPrefix) {
			out[k] = v
		}
	}
	return out
}

// ---- Deployment ----

func depProtected(d *appsv1.Deployment) bool {
	return d != nil && (hasRoleLabel(d.Spec.Template.Labels) || hasRoleLabel(d.Labels) ||
		hasInfraAnnotation(d.Annotations) || hasMiniServiceOwner(d.OwnerReferences))
}

func (e *Engine) evalDeployment(ctx context.Context, r Request) Decision {
	dep, _ := r.Object.(*appsv1.Deployment)
	old, _ := r.OldObject.(*appsv1.Deployment)
	if dep == nil {
		return allow(ReasonUnprotected)
	}
	switch r.Operation {
	case OperationCreate:
		if !depProtected(dep) {
			return allow(ReasonUnprotected)
		}
		if !e.isNVCA(r) {
			return deny(ReasonDenyWriter, "only NVCA may create a Deployment with reserved utils metadata")
		}
		// NVCA's own object must name a live MiniService that owns this namespace.
		if d := e.verifyDeploymentMiniService(ctx, r.Namespace, dep); !d.Allowed {
			return d
		}
		if !hasRoleLabel(dep.Spec.Template.Labels) || !hasRoleLabel(dep.Spec.Selector.MatchLabels) {
			return deny(ReasonDenyTemplate, "the utils Deployment must carry the reserved label in its template and selector")
		}
		return allow(ReasonNVCAWriter)
	case OperationUpdate:
		if old == nil {
			old = &appsv1.Deployment{}
		}
		if !depProtected(old) && !depProtected(dep) {
			return allow(ReasonUnprotected)
		}
		if e.isNVCA(r) {
			return allow(ReasonNVCAWriter)
		}
		if !depProtected(old) {
			return deny(ReasonDenyWriter, "only NVCA may add reserved utils metadata to a Deployment")
		}
		return deny(ReasonDenyProtectedEdit, "protected utils Deployments can only be changed by NVCA")
	}
	return allow(ReasonUnprotected)
}

// ---- Scale ----

// evalScale decides a deployments/scale request. The request carries no template, so the target is read.
func (e *Engine) evalScale(ctx context.Context, r Request) Decision {
	if e.isNVCA(r) {
		return allow(ReasonNVCAWriter)
	}
	dep, err := e.Lookup.GetDeployment(ctx, r.Namespace, r.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return allow(ReasonUnprotected)
		}
		return lookupFailure("the scale target "+r.Name, err)
	}
	if !depProtected(dep) {
		return allow(ReasonUnprotected)
	}
	return deny(ReasonDenyProtectedEdit, "the replica count of a utils Deployment can only be changed by NVCA")
}
