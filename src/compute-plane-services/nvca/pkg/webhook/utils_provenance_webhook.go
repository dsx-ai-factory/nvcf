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

package webhook

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/core"
	"github.com/sirupsen/logrus"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
	nvcav1alpha1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	nvcaclientset "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/client/clientset/versioned"
	whmetrics "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/webhook/metrics"
)

// UtilsProvenanceMode selects what the utils provenance webhook does with a refusal.
type UtilsProvenanceMode string

const (
	// UtilsProvenanceAudit logs and counts what would be refused and allows every request.
	UtilsProvenanceAudit UtilsProvenanceMode = "audit"
	// UtilsProvenanceEnforce refuses requests the engine refuses.
	UtilsProvenanceEnforce UtilsProvenanceMode = "enforce"

	decisionAllow          = "allow"
	decisionDenyHard       = "deny_hard"
	decisionDenyRetry      = "deny_retry"
	decisionAuditWouldDeny = "audit_would_deny"
)

// ParseUtilsProvenanceMode parses a mode name. The empty string means audit.
func ParseUtilsProvenanceMode(s string) (UtilsProvenanceMode, error) {
	switch UtilsProvenanceMode(s) {
	case "", UtilsProvenanceAudit:
		return UtilsProvenanceAudit, nil
	case UtilsProvenanceEnforce:
		return UtilsProvenanceEnforce, nil
	}
	return "", fmt.Errorf("unknown utils provenance mode %q, want audit or enforce", s)
}

var utilsProvenanceKinds = []string{
	string(utilsprovenance.KindPod), string(utilsprovenance.KindReplicaSet),
	string(utilsprovenance.KindDeployment), string(utilsprovenance.KindScale),
}

var utilsProvenanceDecisions = []string{decisionAllow, decisionDenyHard, decisionDenyRetry, decisionAuditWouldDeny}

// defaultUtilsProvenanceRetryBackoffs are the waits between re-evaluations of a retryable decision. Kube controllers
// retry a refused create with a long exponential backoff (a minute or more was observed), so a short internal retry
// resolves cache lag and template races without delaying the replica. The total stays well below the webhook timeout.
var defaultUtilsProvenanceRetryBackoffs = []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}

// utilsProvenanceDecisionBudget bounds one admission decision including its internal retries.
const utilsProvenanceDecisionBudget = 5 * time.Second

// utilsProvenanceWebhook validates objects that carry or try to carry the reserved utils metadata.
type utilsProvenanceWebhook struct {
	engine  *utilsprovenance.Engine
	mode    UtilsProvenanceMode
	decoder runtime.Decoder
	// retryBackoffs are the waits between re-evaluations while the decision is retryable. Nil means the default.
	retryBackoffs []time.Duration
}

// NewUtilsProvenanceWebhook returns an HTTP handler for the utils provenance validating webhook.
func NewUtilsProvenanceWebhook(ctx context.Context, name string, engine *utilsprovenance.Engine,
	mode UtilsProvenanceMode) (http.Handler, error) {
	if engine == nil {
		return nil, fmt.Errorf("utils provenance engine is required")
	}
	if _, err := ParseUtilsProvenanceMode(string(mode)); err != nil {
		return nil, err
	}
	if m := whmetrics.FromContext(ctx); m != nil {
		reasons := make([]string, 0, len(utilsprovenance.AllReasons))
		for _, r := range utilsprovenance.AllReasons {
			reasons = append(reasons, string(r))
		}
		m.InitUtilsProvenance(utilsProvenanceKinds, utilsProvenanceDecisions, reasons)
	}
	sch := runtime.NewScheme()
	registerScheme(sch, corev1.SchemeGroupVersion, &corev1.Pod{})
	registerScheme(sch, appsv1.SchemeGroupVersion, &appsv1.Deployment{}, &appsv1.ReplicaSet{})
	return newStandaloneWebhook(ctx, name, &utilsProvenanceWebhook{
		engine:  engine,
		mode:    mode,
		decoder: serializer.NewCodecFactory(sch).UniversalDeserializer(),
	})
}

// Handle implements admission.Handler.
func (w *utilsProvenanceWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := core.GetLogger(ctx)

	ereq, err := w.toEngineRequest(req)
	if err != nil {
		// A request we cannot read is never blocked here. The scoped rules cannot judge it.
		log.WithError(err).Warn("utils provenance: unreadable admission request, allowing")
		return admission.Allowed("unreadable request")
	}
	if ereq == nil {
		return admission.Allowed("not in scope")
	}

	d := w.evaluate(ctx, *ereq)
	decision := w.decision(d)
	if m := whmetrics.FromContext(ctx); m != nil {
		m.UtilsProvenanceDecisions.WithLabelValues(string(ereq.Kind), decision, string(d.Reason)).Inc()
	}

	if d.Allowed {
		return admission.Allowed(string(d.Reason))
	}
	// Log refusals with identifiers only. Object bodies are never logged.
	fields := logrus.Fields{
		"kind": ereq.Kind, "operation": ereq.Operation, "namespace": ereq.Namespace, "name": ereq.Name,
		"user": ereq.Username, "reason": d.Reason, "decision": decision, "mode": w.mode,
	}
	if w.mode == UtilsProvenanceAudit {
		log.WithFields(fields).Warn("utils provenance: would refuse (audit mode, allowing)")
		return admission.Allowed("audit: " + string(d.Reason)).WithWarnings("utils provenance audit: " + d.Message)
	}
	log.WithFields(fields).Warn("utils provenance: refused: " + d.Message)
	if d.Retryable {
		return admission.Response{AdmissionResponse: admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Status: metav1.StatusFailure, Code: http.StatusServiceUnavailable, Reason: metav1.StatusReasonServiceUnavailable,
				Message: utilsprovenance.FormatDenial(d),
			},
		}}
	}
	return admission.Denied(utilsprovenance.FormatDenial(d))
}

// evaluate runs the engine and re-evaluates a retryable decision after short waits, so a lookup that only misses
// because the object is not visible yet usually resolves inside this one admission request.
func (w *utilsProvenanceWebhook) evaluate(ctx context.Context, req utilsprovenance.Request) utilsprovenance.Decision {
	backoffs := w.retryBackoffs
	if backoffs == nil {
		backoffs = defaultUtilsProvenanceRetryBackoffs
	}
	// Bound the whole decision, retries included, below the webhook timeout (10s unless configured).
	ctx, cancel := context.WithTimeout(ctx, utilsProvenanceDecisionBudget)
	defer cancel()
	d := w.engine.Evaluate(ctx, req)
	for _, wait := range backoffs {
		if !d.Retryable {
			return d
		}
		select {
		case <-ctx.Done():
			return d
		case <-time.After(wait):
		}
		d = w.engine.Evaluate(ctx, req)
	}
	return d
}

// decision maps an engine decision and the mode to the metric decision label.
func (w *utilsProvenanceWebhook) decision(d utilsprovenance.Decision) string {
	switch {
	case d.Allowed:
		return decisionAllow
	case w.mode == UtilsProvenanceAudit:
		return decisionAuditWouldDeny
	case d.Retryable:
		return decisionDenyRetry
	default:
		return decisionDenyHard
	}
}

func (w *utilsProvenanceWebhook) toEngineRequest(req admission.Request) (*utilsprovenance.Request, error) {
	var op utilsprovenance.Operation
	switch req.Operation {
	case admissionv1.Create:
		op = utilsprovenance.OperationCreate
	case admissionv1.Update:
		op = utilsprovenance.OperationUpdate
	default:
		return nil, nil
	}
	out := &utilsprovenance.Request{
		Operation:   op,
		Subresource: req.SubResource,
		Username:    req.UserInfo.Username,
		Namespace:   req.Namespace,
		Name:        req.Name,
	}
	switch {
	case req.Resource.Resource == "deployments" && req.SubResource == "scale":
		out.Kind = utilsprovenance.KindScale
		return out, nil
	case req.Kind.Kind == "Pod":
		out.Kind = utilsprovenance.KindPod
	case req.Kind.Kind == "ReplicaSet":
		out.Kind = utilsprovenance.KindReplicaSet
	case req.Kind.Kind == "Deployment":
		out.Kind = utilsprovenance.KindDeployment
	default:
		return nil, nil
	}
	obj, err := w.decode(req.Object.Raw, req.Kind)
	if err != nil {
		return nil, err
	}
	out.Object = obj
	if len(req.OldObject.Raw) > 0 {
		old, err := w.decode(req.OldObject.Raw, req.Kind)
		if err != nil {
			return nil, err
		}
		out.OldObject = old
	}
	return out, nil
}

func (w *utilsProvenanceWebhook) decode(raw []byte, kind metav1.GroupVersionKind) (client.Object, error) {
	obj, _, err := w.decoder.Decode(raw, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", kind.Kind, err)
	}
	co, ok := obj.(client.Object)
	if !ok {
		return nil, fmt.Errorf("decoded %s is not a client.Object", kind.Kind)
	}
	return co, nil
}

// kubeProvenanceLookup reads the owner chain from the API server. Each call is a direct GET by name, bounded by the
// engine's lookup timeout. Kubernetes cannot GET by UID, so the engine compares UIDs. Informer backed reads can be
// added later behind the same interface.
type kubeProvenanceLookup struct {
	kube kubernetes.Interface
	nvca nvcaclientset.Interface
}

// NewKubeProvenanceLookup returns a utilsprovenance.Lookup backed by the API server.
func NewKubeProvenanceLookup(kube kubernetes.Interface, nvca nvcaclientset.Interface) utilsprovenance.Lookup {
	return &kubeProvenanceLookup{kube: kube, nvca: nvca}
}

func (l *kubeProvenanceLookup) GetReplicaSet(ctx context.Context, namespace, name string) (*appsv1.ReplicaSet, error) {
	return l.kube.AppsV1().ReplicaSets(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (l *kubeProvenanceLookup) GetDeployment(ctx context.Context, namespace, name string) (*appsv1.Deployment, error) {
	return l.kube.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (l *kubeProvenanceLookup) GetMiniService(ctx context.Context, name string) (*utilsprovenance.MiniServiceRef, error) {
	ms, err := l.nvca.NvcaV1alpha1().MiniServices("").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return miniServiceRef(ms), nil
}

func (l *kubeProvenanceLookup) FindMiniServiceByNamespace(ctx context.Context, namespace string) (*utilsprovenance.MiniServiceRef, error) {
	list, err := l.nvca.NvcaV1alpha1().MiniServices("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i := range list.Items {
		if list.Items[i].Spec.Namespace == namespace {
			return miniServiceRef(&list.Items[i]), nil
		}
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Group: "nvca.nvcf.nvidia.io", Resource: "miniservices"}, namespace)
}

func miniServiceRef(ms *nvcav1alpha1.MiniService) *utilsprovenance.MiniServiceRef {
	return &utilsprovenance.MiniServiceRef{UID: ms.UID, Namespace: ms.Spec.Namespace}
}

// utilsProvenanceConfig is what the webhook server needs to register the utils provenance webhook.
type utilsProvenanceConfig struct {
	engine *utilsprovenance.Engine
	mode   UtilsProvenanceMode
}

// newNVCAClientset builds the NVCA CRD clientset from the kubeconfig path. Replaced in tests.
var newNVCAClientset = func(ctx context.Context, path string) (nvcaclientset.Interface, error) {
	configurator := core.NewPathKubeConfigurator().WithPath(path)
	cfg, ok := <-configurator.Start(ctx)
	if !ok || cfg == nil {
		return nil, fmt.Errorf("failed to load the kube config for the NVCA clientset")
	}
	return nvcaclientset.NewForConfig(cfg)
}

// selfUsername returns the username the API server sees for this process, so the engine knows NVCA's own identity.
// Replaced in tests.
var selfUsername = func(ctx context.Context, kube kubernetes.Interface) (string, error) {
	var lastErr error
	for i := 0; i < 5; i++ {
		rev, err := kube.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
		if err == nil && rev.Status.UserInfo.Username != "" {
			return rev.Status.UserInfo.Username, nil
		}
		if err == nil {
			err = fmt.Errorf("empty username in the self subject review")
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Duration(i+1) * time.Second):
		}
	}
	return "", fmt.Errorf("resolve own username: %w", lastErr)
}

func newUtilsProvenanceConfig(ctx context.Context, kubeconfigPath string, kube kubernetes.Interface,
	mode UtilsProvenanceMode, rsControllerUser, depControllerUser string) (*utilsProvenanceConfig, error) {
	nvca, err := newNVCAClientset(ctx, kubeconfigPath)
	if err != nil {
		return nil, err
	}
	self, err := selfUsername(ctx, kube)
	if err != nil {
		return nil, err
	}
	return &utilsProvenanceConfig{
		mode: mode,
		engine: &utilsprovenance.Engine{
			Identities: utilsprovenance.Identities{
				NVCA:                 self,
				ReplicaSetController: rsControllerUser,
				DeploymentController: depControllerUser,
			},
			Lookup: NewKubeProvenanceLookup(kube, nvca),
		},
	}, nil
}
