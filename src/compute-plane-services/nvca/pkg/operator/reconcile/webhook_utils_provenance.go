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

package operator

import (
	"fmt"
	"strings"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance"
	nvidiaiov1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvcf/v1"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

const (
	utilsProvenanceWebhookName      = utilsprovenance.WebhookName
	utilsProvenanceScaleWebhookName = "validate-utils-provenance-scale.nvca.nvcf.nvidia.io"
	utilsProvenanceWebhookPath      = "/validate-utils-provenance"
)

// utilsProvenanceMatchExpression is a CEL expression that is true only for requests whose new or old object carries
// reserved utils metadata: the pod-role label, the infra-object annotation, or a MiniService owner reference. For
// Deployments and ReplicaSets the pod template is checked too, because only the template reaches the pods. Objects
// that carry none of these never reach the webhook, so an outage cannot block them.
func utilsProvenanceMatchExpression() string {
	label := fmt.Sprintf("%q", nvcatypes.PodRoleLabelKey)
	annotation := fmt.Sprintf("%q", nvcatypes.InfraObjectAnnotationKey)

	reservedMeta := func(obj string) string {
		return fmt.Sprintf("(has(%[1]s.metadata.labels) && %[2]s in %[1]s.metadata.labels) || "+
			"(has(%[1]s.metadata.annotations) && %[3]s in %[1]s.metadata.annotations)", obj, label, annotation)
	}
	reservedTemplate := func(obj string) string {
		return fmt.Sprintf("(has(%[1]s.spec) && has(%[1]s.spec.template) && has(%[1]s.spec.template.metadata) && "+
			"((has(%[1]s.spec.template.metadata.labels) && %[2]s in %[1]s.spec.template.metadata.labels) || "+
			"(has(%[1]s.spec.template.metadata.annotations) && %[3]s in %[1]s.spec.template.metadata.annotations)))",
			obj, label, annotation)
	}
	miniServiceOwner := func(obj string) string {
		return fmt.Sprintf("(has(%[1]s.metadata.ownerReferences) && "+
			"%[1]s.metadata.ownerReferences.exists(o, o.kind == 'MiniService'))", obj)
	}
	reserved := func(obj string, workload bool) string {
		parts := []string{reservedMeta(obj)}
		if workload {
			parts = append(parts, reservedTemplate(obj), miniServiceOwner(obj))
		}
		return "(" + strings.Join(parts, " || ") + ")"
	}
	either := func(workload bool) string {
		return fmt.Sprintf("(%s || (oldObject != null && %s))", reserved("object", workload), reserved("oldObject", workload))
	}
	return fmt.Sprintf("(request.kind.kind == 'Pod' && %s) || "+
		"((request.kind.kind == 'Deployment' || request.kind.kind == 'ReplicaSet') && %s)", either(false), either(true))
}

// makeUtilsProvenanceValidatingWebhooks returns the utils provenance webhooks. The main one is selected by the match
// expression. The scale webhook is separate because a Scale request carries no template or reserved keys, so the
// expression cannot select it. It has its own failure policy and is always Ignore.
func makeUtilsProvenanceValidatingWebhooks(nb *nvidiaiov1.NVCFBackend, webhookCert WebhookCert,
	nsSelector *metav1.LabelSelector, settings UtilsProvenanceSettings) []admissionregistrationv1.ValidatingWebhook {
	namespaced := ptr.To(admissionregistrationv1.NamespacedScope)
	createUpdate := []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update}

	main := admissionregistrationv1.ValidatingWebhook{
		Name:                    utilsProvenanceWebhookName,
		AdmissionReviewVersions: []string{"v1"},
		FailurePolicy:           ptr.To(settings.failurePolicy()),
		SideEffects:             ptr.To(admissionregistrationv1.SideEffectClassNone),
		MatchPolicy:             ptr.To(admissionregistrationv1.Equivalent),
		NamespaceSelector:       nsSelector,
		ClientConfig:            makeWebhookClientConfig(nb, webhookCert, utilsProvenanceWebhookPath),
		MatchConditions: []admissionregistrationv1.MatchCondition{{
			Name:       "carries-reserved-utils-metadata",
			Expression: utilsProvenanceMatchExpression(),
		}},
		Rules: []admissionregistrationv1.RuleWithOperations{
			{
				Rule: admissionregistrationv1.Rule{
					APIGroups: []string{""}, APIVersions: []string{"v1"},
					Resources: []string{"pods", "pods/ephemeralcontainers"}, Scope: namespaced,
				},
				Operations: createUpdate,
			},
			{
				Rule: admissionregistrationv1.Rule{
					APIGroups: []string{"apps"}, APIVersions: []string{"v1"},
					Resources: []string{"deployments", "replicasets"}, Scope: namespaced,
				},
				Operations: createUpdate,
			},
		},
	}

	scale := admissionregistrationv1.ValidatingWebhook{
		Name:                    utilsProvenanceScaleWebhookName,
		AdmissionReviewVersions: []string{"v1"},
		FailurePolicy:           ptr.To(admissionregistrationv1.Ignore),
		SideEffects:             ptr.To(admissionregistrationv1.SideEffectClassNone),
		MatchPolicy:             ptr.To(admissionregistrationv1.Equivalent),
		NamespaceSelector:       nsSelector,
		ClientConfig:            makeWebhookClientConfig(nb, webhookCert, utilsProvenanceWebhookPath),
		Rules: []admissionregistrationv1.RuleWithOperations{{
			Rule: admissionregistrationv1.Rule{
				APIGroups: []string{"apps"}, APIVersions: []string{"v1"},
				Resources: []string{"deployments/scale"}, Scope: namespaced,
			},
			Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
		}},
	}
	return []admissionregistrationv1.ValidatingWebhook{main, scale}
}
