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
	"net/http"
	"strings"
	"testing"

	echo "github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/internal/ptr"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/models"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/nvcf"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/requestctx"
)

func TestNormalizeChatRequestRejectsRequestLargerThanTokenLimit(t *testing.T) {
	t.Parallel()

	// Every bucket is full, so only the request's size can refuse it.
	limiter := &recordingRateLimiter{
		currentValueFn: func(call rateLimitCall) int64 {
			return call.limit.Limit
		},
	}
	handlers := NewHandlers(rateLimitConfig(), nil, limiter)
	gc, rec := newRateLimitGatewayContext()
	gc.store.Set(contextKeyRequestContext, &requestctx.RequestContext{
		RequestID:  "req-oversized",
		OrgID:      "nca-456",
		RoutingKey: "fn-chat",
		Model:      "company-name/model-name",
		ModelSpecs: map[string]nvcf.ModelSpec{
			"company-name/model-name": {TokenRateLimit: "100-M"},
		},
	})

	request := &models.ChatCompletionRequest{
		Model: "fn-chat/company-name/model-name",
		Messages: &[]models.ChatMessage{
			{Role: models.ChatCompletionRoleUser, Content: models.SingleTextContent("hello world")},
		},
		MaxCompletionTokens: ptr.To(uint32(1000)),
	}

	_, err := handlers.normalizeChatRequest(gc, request, nil)
	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusRequestEntityTooLarge, httpErr.Code, err.Error())
	message, _ := httpErr.Message.(string)
	require.True(t, strings.HasPrefix(message, "request needs "), message)
	require.Contains(t, message, "more than the rate limit of 100 tokens per minute")
	require.Empty(t, rec.Header().Get("Retry-After"))
	require.Equal(t, "false", rec.Header().Get("X-Should-Retry"))
	for _, call := range limiter.Calls() {
		require.False(t, call.mustConsume, "unexpected must-consume call: %+v", call)
	}
}

func TestKeepRetryableLimitErrorPrefers429Over413(t *testing.T) {
	t.Parallel()

	tooMany := echo.NewHTTPError(http.StatusTooManyRequests, "rate limit exceeded")
	tooLarge := echo.NewHTTPError(http.StatusRequestEntityTooLarge, "request needs more")

	require.True(t, keepRetryableLimitError(nil, tooLarge))
	require.True(t, keepRetryableLimitError(tooLarge, tooMany))
	require.True(t, keepRetryableLimitError(tooMany, tooMany))
	require.False(t, keepRetryableLimitError(tooMany, tooLarge))
}
