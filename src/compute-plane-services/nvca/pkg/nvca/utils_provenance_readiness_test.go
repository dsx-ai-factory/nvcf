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

package nvca

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	mscontroller "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/miniservice"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/utilsprovenance/rollout"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestUtilsProvenanceEnforcingFollowsTheOperatorEnv(t *testing.T) {
	for value, want := range map[string]bool{"": false, "audit": false, "enforce": true, "Enforce": true, "block": false} {
		t.Setenv(types.UtilsProvenanceModeEnv, value)
		assert.Equal(t, want, utilsProvenanceEnforcing(), "value %q", value)
	}
}

func TestUtilsReplicasFromEnv(t *testing.T) {
	log := logrus.New()
	t.Run("HA disabled defaults to one", func(t *testing.T) {
		for value, want := range map[string]int32{"": 1, "1": 1, "2": 2, "3": 3, "0": 1, "-2": 1, "abc": 1, "2.5": 1} {
			t.Setenv(types.UtilsReplicasEnv, value)
			assert.Equal(t, want, utilsReplicasFromEnv(log, false), "value %q", value)
		}
	})
	t.Run("HA enabled defaults to two", func(t *testing.T) {
		for value, want := range map[string]int32{"": 2, "1": 1, "2": 2, "3": 3, "0": 2, "-2": 2, "abc": 2, "2.5": 2} {
			t.Setenv(types.UtilsReplicasEnv, value)
			assert.Equal(t, want, utilsReplicasFromEnv(log, true), "value %q", value)
		}
	})
}

func TestMiniServiceTrustReadsTheRecordedDigestsOfAnInstanceNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	withRecord := func(name, ns, message string) *v1alpha1.MiniService {
		ms := &v1alpha1.MiniService{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.MiniServiceSpec{Namespace: ns}}
		ms.Status.Conditions = []metav1.Condition{{Type: mscontroller.ConditionUtilsHA, Status: metav1.ConditionTrue, Reason: "r",
			Message: message, LastTransitionTime: metav1.Now()}}
		return ms
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		withRecord("a", "sr-a", `{"replicas":2,"podSpecDigest":"pd","containersDigest":"cd","deploymentSpecDigest":"dd","deploymentUID":"dep-uid"}`),
		withRecord("b", "sr-b", `{"replicas":2}`),
		withRecord("c", "sr-c", `2`),
	).Build()
	trust := miniServiceTrust{reader: c}

	d, ok, err := trust.UtilsTrust(context.Background(), "sr-a")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, rollout.TrustedDigests{PodSpec: "pd", Containers: "cd", DeploymentSpec: "dd", Replicas: 2, DeploymentUID: "dep-uid"}, d)

	for _, ns := range []string{"sr-b", "sr-c", "sr-unknown"} {
		_, ok, err := trust.UtilsTrust(context.Background(), ns)
		require.NoError(t, err)
		assert.False(t, ok, "%s has no recorded digests", ns)
	}
}
