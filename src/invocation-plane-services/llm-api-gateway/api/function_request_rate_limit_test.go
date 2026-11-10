/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

package api

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/config"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/nvcf"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/provider"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/ratelimit"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/requestctx"
)

// countingRateLimiter is an in-memory limiter with the same contract as the
// production one: a test-only check never writes, a must-consume always
// writes, and a plain consume writes only when the request fits. Budgets do
// not refill, which is enough to count requests inside one window.
type countingRateLimiter struct {
	mu   sync.Mutex
	used map[string]int64
}

func newCountingRateLimiter() *countingRateLimiter {
	return &countingRateLimiter{used: map[string]int64{}}
}

func (r *countingRateLimiter) CheckLimit(
	_ context.Context,
	key string,
	l ratelimit.RateLimit,
	tokensRequested int64,
	testOnly bool,
	_ string,
	mustConsume bool,
) (*ratelimit.RateLimitResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := &ratelimit.RateLimitResult{
		CurrentValue: l.Limit - r.used[key],
		Requested:    tokensRequested,
		RateLimit:    l,
	}
	if !testOnly && (mustConsume || result.Allowed()) {
		r.used[key] += tokensRequested
	}
	return result, nil
}

func (r *countingRateLimiter) Reset(_ context.Context, keys ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, key := range keys {
		delete(r.used, key)
	}
	return nil
}

func (r *countingRateLimiter) usedFor(suffix string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var total int64
	for key, used := range r.used {
		if strings.HasSuffix(key, suffix) {
			total += used
		}
	}
	return total
}

func TestNVCFAuthMiddlewarePropagatesFunctionRequestRateLimits(t *testing.T) {
	t.Parallel()

	authClient := &stubInvocationAuthClient{
		authResponse: &nvcf.InvocationAuthResponse{
			RoutingKey:                   "fn-chat",
			ClientAuthID:                 "subject-123",
			AuthContext:                  map[string]string{"ncaId": "nca-456"},
			RateLimitKey:                 "nca-456",
			FunctionRequestRateLimit:     "5-M",
			FunctionUserRequestRateLimit: "2-S",
			RequestLimitAccount:          "owner-nca",
		},
	}
	tc := priorityMiddlewarePaths[0]

	e := echo.New()
	e.Use(NewContextMiddleware(config.Default()))
	e.Use(NewNVCFAuthMiddleware(authClient))
	e.POST(tc.path, func(ec echo.Context) error {
		reqCtx := ec.(*GatewayContext).RequestContext()
		require.NotNil(t, reqCtx)
		require.Equal(t, "5-M", reqCtx.FunctionRequestRateLimit)
		require.Equal(t, "2-S", reqCtx.FunctionUserRequestRateLimit)
		require.Equal(t, "owner-nca", reqCtx.RequestLimitAccount)
		require.Equal(t, "nca-456", reqCtx.OrgID, "token limits keep the authorized account")
		return ec.NoContent(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	req.Header.Set(echo.HeaderAuthorization, "Bearer sk-live")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
}

func TestFunctionRequestLimitResolver(t *testing.T) {
	t.Parallel()

	accountKey := rateLimitSubjectKey("owner-nca", "", "fn-alpha")

	tests := []struct {
		name   string
		reqCtx requestctx.RequestContext
		want   []ratelimit.ResourceLimit
	}{
		{
			name:   "no request limit",
			reqCtx: requestctx.RequestContext{OrgID: "nca-1", RoutingKey: "fn-alpha"},
		},
		{
			name: "per-minute limit counts per account and function",
			reqCtx: requestctx.RequestContext{
				OrgID: "nca-1", RequestLimitAccount: "owner-nca", RoutingKey: "fn-alpha",
				FunctionRequestRateLimit: "5-M",
			},
			want: []ratelimit.ResourceLimit{{SubjectKey: accountKey, RequestsPerMinute: 5}},
		},
		{
			name: "several windows",
			reqCtx: requestctx.RequestContext{
				OrgID: "nca-1", RequestLimitAccount: "owner-nca", RoutingKey: "fn-alpha",
				FunctionRequestRateLimit: "5-S,300-H,1000-D",
			},
			want: []ratelimit.ResourceLimit{{SubjectKey: accountKey, RequestsPerSecond: 5, RequestsPerHour: 300, RequestsPerDay: 1000}},
		},
		{
			name: "account falls back to OrgID",
			reqCtx: requestctx.RequestContext{
				OrgID: "owner-nca", RoutingKey: "fn-alpha", FunctionRequestRateLimit: "5-M",
			},
			want: []ratelimit.ResourceLimit{{SubjectKey: accountKey, RequestsPerMinute: 5}},
		},
		{
			name: "project does not split the account budget",
			reqCtx: requestctx.RequestContext{
				OrgID: "owner-nca", ProjectID: "project-1", RoutingKey: "fn-alpha", FunctionRequestRateLimit: "5-M",
			},
			want: []ratelimit.ResourceLimit{{SubjectKey: accountKey, RequestsPerMinute: 5}},
		},
		{
			name: "user limit comes first and is keyed by caller",
			reqCtx: requestctx.RequestContext{
				OrgID: "nca-1", RequestLimitAccount: "owner-nca", RoutingKey: "fn-alpha", APIKeyID: "subject-a",
				FunctionRequestRateLimit: "5-M", FunctionUserRequestRateLimit: "2-M",
			},
			want: []ratelimit.ResourceLimit{
				{SubjectKey: "nvcf:owner-nca:user:subject-a:routing_key:fn-alpha", RequestsPerMinute: 2},
				{SubjectKey: accountKey, RequestsPerMinute: 5},
			},
		},
		{
			name: "no caller subject skips the user limit",
			reqCtx: requestctx.RequestContext{
				OrgID: "nca-1", RequestLimitAccount: "owner-nca", RoutingKey: "fn-alpha",
				FunctionRequestRateLimit: "5-M", FunctionUserRequestRateLimit: "2-M",
			},
			want: []ratelimit.ResourceLimit{{SubjectKey: accountKey, RequestsPerMinute: 5}},
		},
		{
			name: "user limit alone",
			reqCtx: requestctx.RequestContext{
				OrgID: "owner-nca", RoutingKey: "fn-alpha", APIKeyID: "subject-a", FunctionUserRequestRateLimit: "2-M",
			},
			want: []ratelimit.ResourceLimit{{SubjectKey: "nvcf:owner-nca:user:subject-a:routing_key:fn-alpha", RequestsPerMinute: 2}},
		},
		{
			name: "malformed entry is dropped and the rest applies",
			reqCtx: requestctx.RequestContext{
				OrgID: "owner-nca", RoutingKey: "fn-alpha", FunctionRequestRateLimit: "five-M,5-S",
			},
			want: []ratelimit.ResourceLimit{{SubjectKey: accountKey, RequestsPerSecond: 5}},
		},
		{
			name: "week window is dropped like a malformed entry",
			reqCtx: requestctx.RequestContext{
				OrgID: "owner-nca", RoutingKey: "fn-alpha", FunctionRequestRateLimit: "5-M,5-W",
			},
			want: []ratelimit.ResourceLimit{{SubjectKey: accountKey, RequestsPerMinute: 5}},
		},
		{
			name: "month window is dropped and the stricter duplicate wins in either order",
			reqCtx: requestctx.RequestContext{
				OrgID: "owner-nca", RoutingKey: "fn-alpha", FunctionRequestRateLimit: "5-MO,3-M,5-M",
			},
			want: []ratelimit.ResourceLimit{{SubjectKey: accountKey, RequestsPerMinute: 3}},
		},
		{
			name: "both limits without a valid entry apply no limit",
			reqCtx: requestctx.RequestContext{
				OrgID: "owner-nca", RoutingKey: "fn-alpha", APIKeyID: "subject-a",
				FunctionRequestRateLimit: "5-W", FunctionUserRequestRateLimit: "five-M",
			},
		},
		{
			name: "two entries for one window keep the stricter",
			reqCtx: requestctx.RequestContext{
				OrgID: "owner-nca", RoutingKey: "fn-alpha", FunctionRequestRateLimit: "5-M,3-M",
			},
			want: []ratelimit.ResourceLimit{{SubjectKey: accountKey, RequestsPerMinute: 3}},
		},
		{
			name: "a limit with no valid entry is skipped, not failed",
			reqCtx: requestctx.RequestContext{
				OrgID: "owner-nca", RoutingKey: "fn-alpha", APIKeyID: "subject-a",
				FunctionRequestRateLimit: "5-M", FunctionUserRequestRateLimit: "five-M",
			},
			want: []ratelimit.ResourceLimit{{SubjectKey: accountKey, RequestsPerMinute: 5}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reqCtx := tc.reqCtx
			limits, err := FunctionRequestLimitResolver{}.ResolveLimits(context.Background(), &reqCtx, "/v1/decisions")
			require.NoError(t, err)
			require.Len(t, limits, len(tc.want))
			for i, want := range tc.want {
				got := limits[i]
				require.Equal(t, want.SubjectKey, got.SubjectKey)
				require.Equal(t, want.RequestsPerSecond, got.RequestsPerSecond, "per second")
				require.Equal(t, want.RequestsPerMinute, got.RequestsPerMinute, "per minute")
				require.Equal(t, want.RequestsPerHour, got.RequestsPerHour, "per hour")
				require.Equal(t, want.RequestsPerDay, got.RequestsPerDay, "per day")
				require.False(t, got.AllowDebt, "request limits never run into debt")
				require.Zero(t, got.TokensPerMinute+got.InputTokensPerMinute+got.OutputTokensPerMinute, "request limits carry no token windows")
			}
		})
	}
}

// TestMalformedFunctionRequestRateWarnsOncePerPolicy shows a bad policy is
// logged once per function and policy, not on every request.
func TestMalformedFunctionRequestRateWarnsOncePerPolicy(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	ctx := zerolog.New(&logs).WithContext(context.Background())
	reqCtx := &requestctx.RequestContext{OrgID: "nca-1", RoutingKey: "fn-warn-once", FunctionRequestRateLimit: "five-M,5-S"}

	for range 3 {
		limits, err := FunctionRequestLimitResolver{}.ResolveLimits(ctx, reqCtx, "/v1/decisions")
		require.NoError(t, err)
		require.Len(t, limits, 1)
	}
	require.Equal(t, 1, strings.Count(logs.String(), "five-M"), logs.String())
	require.Contains(t, logs.String(), `"routingKey":"fn-warn-once"`)
}

func TestDefaultResolverAppliesFunctionRequestLimitsOnEveryRoute(t *testing.T) {
	t.Parallel()

	h := NewHandlers(config.Default(), nil, nil)
	for _, endpoint := range []string{
		chatCompletionsEndpointPath,
		"/v1/completions",
		"/v1/responses",
		messagesEndpointPath,
		"/v1/embeddings",
		"/v1/decisions",
		"/v1/systemone",
	} {
		reqCtx := &requestctx.RequestContext{OrgID: "nca-1", RoutingKey: "fn-alpha", FunctionRequestRateLimit: "5-M"}
		limits, err := h.limitResolver.ResolveLimits(context.Background(), reqCtx, endpoint)
		require.NoError(t, err, endpoint)
		var perMinute int64
		for _, limit := range limits {
			perMinute += limit.RequestsPerMinute
		}
		require.Equal(t, int64(5), perMinute, "%s must apply the function request limit", endpoint)
	}
}

func requestLimitTestAPI(t *testing.T, upstream string, auth *nvcf.InvocationAuthResponse, limiter ratelimit.RateLimiter) *echo.Echo {
	t.Helper()
	p, err := provider.NewStargateProvider(config.StargateConfig{URL: upstream})
	require.NoError(t, err)
	e := echo.New()
	e.Use(NewContextMiddleware(config.Default()))
	e.Use(NewNVCFAuthMiddleware(&stubInvocationAuthClient{authResponse: auth}))
	h := NewHandlers(config.Default(), p, limiter)
	RegisterRoutes(e, h)
	return e
}

// TestFunctionRequestRateLimitRefusesSixthDecisionRequest is the QA case: with
// a fresh 5-M budget, requests 1 to 5 reach the backend and request 6 gets the
// route's native 429 with Retry-After and never reaches the backend.
func TestFunctionRequestRateLimitRefusesSixthDecisionRequest(t *testing.T) {
	t.Parallel()

	for _, route := range decisionRoutes {
		t.Run(route.name, func(t *testing.T) {
			t.Parallel()

			var hits atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
				_, _ = w.Write([]byte(`{}`))
			}))
			defer upstream.Close()

			auth := &nvcf.InvocationAuthResponse{
				RoutingKey: "fn-alpha", RateLimitKey: "org-test", ClientAuthID: "subject-a",
				AuthContext:              map[string]string{"ncaId": "org-test"},
				ModelSpecs:               decisionSpecs(route.path),
				FunctionRequestRateLimit: "5-M",
				RequestLimitAccount:      "org-test",
			}
			e := requestLimitTestAPI(t, upstream.URL, auth, newCountingRateLimiter())

			for i := 1; i <= 5; i++ {
				rec := httptest.NewRecorder()
				e.ServeHTTP(rec, decisionRequest(route, route.body))
				require.Equal(t, http.StatusOK, rec.Code, "request %d: %s", i, rec.Body.String())
				require.Equal(t, "5", rec.Header().Get("X-Ratelimit-Limit-Requests"), "request %d", i)
			}

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, decisionRequest(route, route.body))
			requireNativeError(t, route, rec, http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded", "rate_limit_exceeded")
			require.NotEmpty(t, rec.Header().Get("Retry-After"))
			require.Equal(t, "0", rec.Header().Get("X-Ratelimit-Remaining-Requests"))
			require.Equal(t, int32(5), hits.Load(), "the refused request must not reach the backend")
		})
	}
}

func newRequestLimitPlan(t *testing.T, limiter ratelimit.RateLimiter, reqCtx *requestctx.RequestContext, request ratelimit.ResourceRequest) (*AdmissionPlan, *httptest.ResponseRecorder) {
	t.Helper()
	rec := httptest.NewRecorder()
	ectx := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/v1/decisions", nil), rec)
	resolver := CompositeLimitResolver{CallerLimitResolver{}, FunctionRequestLimitResolver{}}
	plan, err := NewAdmissionPlan(NewGatewayContext(ectx), reqCtx, "/v1/decisions", resolver, limiter, request, request)
	require.NoError(t, err)
	require.NotNil(t, plan)
	return plan, rec
}

// TestCheckRequestsTakesTheSlotAtAdmission shows two requests checked at the
// same time cannot both take the last slot: the request is counted when it is
// admitted, not later when tokens are committed.
func TestCheckRequestsTakesTheSlotAtAdmission(t *testing.T) {
	t.Parallel()

	limiter := newCountingRateLimiter()
	reqCtx := &requestctx.RequestContext{OrgID: "nca-1", RoutingKey: "fn-alpha", FunctionRequestRateLimit: "1-M"}
	request := ratelimit.ResourceRequest{Requests: 1}

	first, _ := newRequestLimitPlan(t, limiter, reqCtx, request)
	second, _ := newRequestLimitPlan(t, limiter, reqCtx, request)

	require.NoError(t, first.CheckRequests(context.Background()))
	err := second.CheckRequests(context.Background())
	require.Error(t, err, "the second request must not pass while the first holds the only slot")
	require.Equal(t, http.StatusTooManyRequests, err.(*echo.HTTPError).Code)

	_, err = first.CheckTokensAndFinalize(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), limiter.usedFor(":"+string(ratelimit.RequestsPerMinute)), "an admitted request counts once")
}

// TestCheckRequestsStopsAtTheFirstRefusingLimit shows a request refused by the
// per-user limit does not use the account's budget.
func TestCheckRequestsStopsAtTheFirstRefusingLimit(t *testing.T) {
	t.Parallel()

	limiter := newCountingRateLimiter()
	reqCtx := &requestctx.RequestContext{
		OrgID: "nca-1", RoutingKey: "fn-alpha", APIKeyID: "subject-a",
		FunctionRequestRateLimit: "5-M", FunctionUserRequestRateLimit: "1-M",
	}
	request := ratelimit.ResourceRequest{Requests: 1}

	first, _ := newRequestLimitPlan(t, limiter, reqCtx, request)
	require.NoError(t, first.CheckRequests(context.Background()))
	_, err := first.CheckTokensAndFinalize(context.Background())
	require.NoError(t, err)

	second, _ := newRequestLimitPlan(t, limiter, reqCtx, request)
	require.Error(t, second.CheckRequests(context.Background()))
	second.Close()

	accountKey := rateLimitSubjectKey("nca-1", "", "fn-alpha") + ":" + string(ratelimit.RequestsPerMinute)
	require.Equal(t, int64(1), limiter.usedFor(accountKey), "the refused request must not count against the account")
}

// admitOnceThenRefuse admits one request, then sends refused more that must be
// refused, so a test can read what the refused requests were charged.
func admitOnceThenRefuse(t *testing.T, limiter ratelimit.RateLimiter, reqCtx *requestctx.RequestContext, refused int) {
	t.Helper()
	request := ratelimit.ResourceRequest{Requests: 1}

	first, _ := newRequestLimitPlan(t, limiter, reqCtx, request)
	require.NoError(t, first.CheckRequests(context.Background()))
	_, err := first.CheckTokensAndFinalize(context.Background())
	require.NoError(t, err)

	for range refused {
		plan, _ := newRequestLimitPlan(t, limiter, reqCtx, request)
		require.Error(t, plan.CheckRequests(context.Background()))
		plan.Close()
	}
}

// TestRequestRefusedByOneWindowIsNotChargedToItsOtherWindows shows requests the
// second window refuses do not use up the hour window of the same limit.
func TestRequestRefusedByOneWindowIsNotChargedToItsOtherWindows(t *testing.T) {
	t.Parallel()

	limiter := newCountingRateLimiter()
	reqCtx := &requestctx.RequestContext{OrgID: "nca-1", RoutingKey: "fn-alpha", FunctionRequestRateLimit: "1-S,10-H"}

	admitOnceThenRefuse(t, limiter, reqCtx, 3)

	require.Equal(t, int64(1), limiter.usedFor(":"+string(ratelimit.RequestsPerSecond)))
	require.Equal(t, int64(1), limiter.usedFor(":"+string(ratelimit.RequestsPerHour)), "refused requests must not use the hour window")
}

// TestRequestRefusedByTheAccountIsNotChargedToTheUser shows requests the account
// limit refuses give their slot back to the per-user limit checked before it.
func TestRequestRefusedByTheAccountIsNotChargedToTheUser(t *testing.T) {
	t.Parallel()

	limiter := newCountingRateLimiter()
	reqCtx := &requestctx.RequestContext{
		OrgID: "nca-1", RoutingKey: "fn-alpha", APIKeyID: "subject-a",
		FunctionRequestRateLimit: "1-M", FunctionUserRequestRateLimit: "5-M",
	}

	admitOnceThenRefuse(t, limiter, reqCtx, 3)

	userKey := "nvcf:nca-1:user:subject-a:routing_key:fn-alpha:" + string(ratelimit.RequestsPerMinute)
	require.Equal(t, int64(1), limiter.usedFor(userKey), "refused requests must not use the user's budget")
	accountKey := rateLimitSubjectKey("nca-1", "", "fn-alpha") + ":" + string(ratelimit.RequestsPerMinute)
	require.Equal(t, int64(1), limiter.usedFor(accountKey))
}

// failingRateLimiter fails the charges fails picks, like a limiter whose store
// is down while RATE_LIMIT_FAIL_OPEN=false. Other calls and refunds go to the
// counting limiter.
type failingRateLimiter struct {
	*countingRateLimiter
	fails func(key string, mustConsume bool) bool
}

func (r failingRateLimiter) CheckLimit(
	ctx context.Context,
	key string,
	l ratelimit.RateLimit,
	tokensRequested int64,
	testOnly bool,
	requestID string,
	mustConsume bool,
) (*ratelimit.RateLimitResult, error) {
	if tokensRequested > 0 && r.fails(key, mustConsume) {
		return nil, errors.New("rate limit store unavailable")
	}
	return r.countingRateLimiter.CheckLimit(ctx, key, l, tokensRequested, testOnly, requestID, mustConsume)
}

// TestLimiterErrorGivesBackEarlierLimits shows a limiter error on the account
// limit does not leave the request charged to the per-user limit before it.
func TestLimiterErrorGivesBackEarlierLimits(t *testing.T) {
	t.Parallel()

	accountPrefix := rateLimitSubjectKey("nca-1", "", "fn-alpha") + ":"
	limiter := failingRateLimiter{
		countingRateLimiter: newCountingRateLimiter(),
		fails:               func(key string, _ bool) bool { return strings.HasPrefix(key, accountPrefix) },
	}
	reqCtx := &requestctx.RequestContext{
		OrgID: "nca-1", RoutingKey: "fn-alpha", APIKeyID: "subject-a",
		FunctionRequestRateLimit: "5-M", FunctionUserRequestRateLimit: "2-M",
	}

	plan, _ := newRequestLimitPlan(t, limiter, reqCtx, ratelimit.ResourceRequest{Requests: 1})
	require.Error(t, plan.CheckRequests(context.Background()))
	plan.Close()

	userKey := "nvcf:nca-1:user:subject-a:routing_key:fn-alpha:" + string(ratelimit.RequestsPerMinute)
	require.Zero(t, limiter.usedFor(userKey), "the per-user slot is given back")
}

// TestTokenRefusalGivesTheRequestBack shows a request admitted on count but
// refused on tokens, which never reaches the backend, does not count against
// the request limit, and the headers show the slot as free again.
func TestTokenRefusalGivesTheRequestBack(t *testing.T) {
	t.Parallel()

	limiter := newCountingRateLimiter()
	reqCtx := &requestctx.RequestContext{
		OrgID: "nca-1", RoutingKey: "fn-alpha", Model: "m", FunctionRequestRateLimit: "5-M",
		ModelSpecs: map[string]nvcf.ModelSpec{"m": {TokenRateLimit: "10-M"}},
	}

	plan, rec := newRequestLimitPlan(t, limiter, reqCtx, ratelimit.ResourceRequest{Requests: 1, InputTokens: 100})
	require.NoError(t, plan.CheckRequests(context.Background()))
	_, err := plan.CheckTokensAndFinalize(context.Background())
	require.Error(t, err, "100 tokens exceed the 10-M token limit")
	plan.Close()

	require.Zero(t, limiter.usedFor(":"+string(ratelimit.RequestsPerMinute)), "the request slot is given back")
	require.Equal(t, "5", rec.Header().Get("X-Ratelimit-Remaining-Requests"))
}

// TestTokenCommitFailureGivesTheRequestBack shows a request whose token commit
// fails (possible only with fail-open off) is not forwarded and gets its
// request slot back.
func TestTokenCommitFailureGivesTheRequestBack(t *testing.T) {
	t.Parallel()

	limiter := failingRateLimiter{
		countingRateLimiter: newCountingRateLimiter(),
		fails:               func(_ string, mustConsume bool) bool { return mustConsume },
	}
	reqCtx := &requestctx.RequestContext{
		OrgID: "nca-1", RoutingKey: "fn-alpha", Model: "m", FunctionRequestRateLimit: "5-M",
		ModelSpecs: map[string]nvcf.ModelSpec{"m": {TokenRateLimit: "1000-M"}},
	}

	plan, _ := newRequestLimitPlan(t, limiter, reqCtx, ratelimit.ResourceRequest{Requests: 1, InputTokens: 100})
	require.NoError(t, plan.CheckRequests(context.Background()))
	_, err := plan.CheckTokensAndFinalize(context.Background())
	require.Error(t, err, "the token commit fails")
	plan.Close()

	require.Zero(t, limiter.usedFor(":"+string(ratelimit.RequestsPerMinute)), "the request slot is given back")
}

// TestRequestHeadersReportTheBindingWindow shows the request headers describe
// the window with the least room left.
func TestRequestHeadersReportTheBindingWindow(t *testing.T) {
	t.Parallel()

	limiter := newCountingRateLimiter()
	reqCtx := &requestctx.RequestContext{OrgID: "nca-1", RoutingKey: "fn-alpha", FunctionRequestRateLimit: "5-S,3-H"}

	plan, rec := newRequestLimitPlan(t, limiter, reqCtx, ratelimit.ResourceRequest{Requests: 1})
	require.NoError(t, plan.CheckRequests(context.Background()))
	_, err := plan.CheckTokensAndFinalize(context.Background())
	require.NoError(t, err)

	require.Equal(t, "3", rec.Header().Get("X-Ratelimit-Limit-Requests"))
	require.Equal(t, "2", rec.Header().Get("X-Ratelimit-Remaining-Requests"))
}
