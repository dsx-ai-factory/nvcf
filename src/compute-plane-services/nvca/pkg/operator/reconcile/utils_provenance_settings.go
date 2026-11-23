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
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"

	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

// Modes of the utils provenance webhook. They mirror the webhook server's own flag values.
const (
	UtilsProvenanceModeAudit   = "audit"
	UtilsProvenanceModeEnforce = "enforce"
)

// UtilsProvenanceSettings is how the operator configures the utils provenance webhook in the NVCA pod and in the
// webhook configuration.
type UtilsProvenanceSettings struct {
	// Mode is audit (default) or enforce.
	Mode string
	// ReplicaSetControllerUser and DeploymentControllerUser override the kube controller usernames the webhook
	// trusts. Empty means the webhook's own defaults.
	ReplicaSetControllerUser string
	DeploymentControllerUser string
}

// NormalizeUtilsProvenanceMode returns the mode, treating the empty string as audit and rejecting unknown values.
func NormalizeUtilsProvenanceMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", UtilsProvenanceModeAudit:
		return UtilsProvenanceModeAudit, nil
	case UtilsProvenanceModeEnforce:
		return UtilsProvenanceModeEnforce, nil
	}
	return "", fmt.Errorf("invalid utils provenance mode %q, want %q or %q", mode, UtilsProvenanceModeAudit,
		UtilsProvenanceModeEnforce)
}

// webhookArgs returns the flags passed to the webhook server container. The defaults (audit, no username overrides)
// add nothing, so the webhook container args, and therefore the NVCA rollout, stay unchanged for existing installs.
func (s UtilsProvenanceSettings) webhookArgs() []string {
	var args []string
	if mode, err := NormalizeUtilsProvenanceMode(s.Mode); err == nil && mode != UtilsProvenanceModeAudit {
		args = append(args, "--utils-provenance-mode", mode)
	}
	if s.ReplicaSetControllerUser != "" {
		args = append(args, "--utils-provenance-replicaset-controller-user", s.ReplicaSetControllerUser)
	}
	if s.DeploymentControllerUser != "" {
		args = append(args, "--utils-provenance-deployment-controller-user", s.DeploymentControllerUser)
	}
	return args
}

// agentEnv returns the environment variables for the NVCA agent container. Audit adds nothing, which keeps the agent
// pod unchanged for existing installs.
func (s UtilsProvenanceSettings) agentEnv() []corev1.EnvVar {
	if mode, err := NormalizeUtilsProvenanceMode(s.Mode); err == nil && mode == UtilsProvenanceModeEnforce {
		return []corev1.EnvVar{{Name: nvcatypes.UtilsProvenanceModeEnv, Value: mode}}
	}
	return nil
}

// agentRules returns the extra ClusterRole rules the NVCA agent needs. The readiness check reviews the effective
// permissions of instance accounts with SubjectAccessReviews, and only runs in enforce mode, so audit installs keep the
// ClusterRole they have today.
func (s UtilsProvenanceSettings) agentRules() []rbacv1.PolicyRule {
	if mode, err := NormalizeUtilsProvenanceMode(s.Mode); err == nil && mode == UtilsProvenanceModeEnforce {
		return []rbacv1.PolicyRule{{
			APIGroups: []string{"authorization.k8s.io"},
			Resources: []string{"subjectaccessreviews"},
			Verbs:     []string{"create"},
		}}
	}
	return nil
}

// failurePolicy is Ignore while auditing, so a webhook outage cannot block NVCA updates to running instances, and
// Fail when enforcing. The match condition keeps the set of objects that can be blocked to those carrying reserved
// utils metadata.
func (s UtilsProvenanceSettings) failurePolicy() admissionregistrationv1.FailurePolicyType {
	if mode, _ := NormalizeUtilsProvenanceMode(s.Mode); mode == UtilsProvenanceModeEnforce {
		return admissionregistrationv1.Fail
	}
	return admissionregistrationv1.Ignore
}
