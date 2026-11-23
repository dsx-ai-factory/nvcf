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

package readiness

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func reviewerReturning(status authorizationv1.SubjectAccessReviewStatus) KubeReviewer {
	c := fake.NewSimpleClientset()
	c.PrependReactor("create", "subjectaccessreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authorizationv1.SubjectAccessReview{Status: status}, nil
	})
	return KubeReviewer{Client: c}
}

type oneAccount struct{}

func (oneAccount) InstanceAccounts(context.Context) ([]Account, error) {
	return []Account{{Namespace: "sr-1", Name: "default"}}, nil
}

func TestKubeReviewerTreatsAnEvaluationErrorAsInconclusive(t *testing.T) {
	right := Right{Group: "apps", Resource: "deployments", Verb: "update"}
	for name, tt := range map[string]struct {
		status  authorizationv1.SubjectAccessReviewStatus
		allowed bool
		wantErr bool
	}{
		"allowed":                         {authorizationv1.SubjectAccessReviewStatus{Allowed: true}, true, false},
		"plain denial":                    {authorizationv1.SubjectAccessReviewStatus{Allowed: false, Reason: "no RBAC policy matched"}, false, false},
		"explicit denial":                 {authorizationv1.SubjectAccessReviewStatus{Allowed: false, Denied: true}, false, false},
		"denial with an evaluation error": {authorizationv1.SubjectAccessReviewStatus{Allowed: false, EvaluationError: "webhook authorizer unavailable"}, false, true},
		"allowed despite an error":        {authorizationv1.SubjectAccessReviewStatus{Allowed: true, EvaluationError: "another authorizer failed"}, true, false},
	} {
		t.Run(name, func(t *testing.T) {
			ok, err := reviewerReturning(tt.status).Allowed(context.Background(), "system:serviceaccount:sr-1:default", nil, "sr-1", right, "")
			assert.Equal(t, tt.allowed, ok)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "could not be evaluated")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// An evaluation error must keep the RBAC check unverified, never count as a clean review.
func TestRBACStaysUnverifiedWhenAReviewCannotBeEvaluated(t *testing.T) {
	c := &Checker{
		Accounts: oneAccount{},
		Reviewer: reviewerReturning(authorizationv1.SubjectAccessReviewStatus{Allowed: false, EvaluationError: "authorizer failed"}),
	}
	st := c.CheckRBAC(context.Background())
	assert.Equal(t, OutcomeUnverified, st.LastRBAC)
	assert.False(t, st.RBACVerified)
	assert.Contains(t, st.Reason, "could not be evaluated")

	// The same reviewer answering a plain denial verifies.
	ok := &Checker{Accounts: oneAccount{}, Reviewer: reviewerReturning(authorizationv1.SubjectAccessReviewStatus{Allowed: false})}
	st = ok.CheckRBAC(context.Background())
	assert.Equal(t, OutcomeVerified, st.LastRBAC)
	assert.True(t, st.RBACVerified)
}
