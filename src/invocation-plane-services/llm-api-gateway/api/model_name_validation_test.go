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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	echo "github.com/labstack/echo/v4"

	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/config"
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/provider"
)

type routerModelSeen struct {
	header    string
	bodyModel string
}

// recordingRouter stands in for the LLM request router and records the
// X-Model header and body model of every request it receives.
type recordingRouter struct {
	mu   sync.Mutex
	seen []routerModelSeen
}

func (r *recordingRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	raw, _ := io.ReadAll(req.Body)
	var body struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(raw, &body)
	r.mu.Lock()
	r.seen = append(r.seen, routerModelSeen{header: req.Header.Get("X-Model"), bodyModel: body.Model})
	r.mu.Unlock()

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `data: {"id":"chatcmpl-name","object":"chat.completion.chunk","created":123,"model":"company-name/model-name","choices":[{"index":0,"delta":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}`+"\n\n")
	fmt.Fprint(w, "data: [DONE]\n\n")
}

func (r *recordingRouter) take() []routerModelSeen {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := r.seen
	r.seen = nil
	return seen
}

func newModelNameTestServer(t *testing.T) (*echo.Echo, *recordingRouter) {
	t.Helper()
	router := &recordingRouter{}
	upstream := httptest.NewServer(router)
	t.Cleanup(upstream.Close)

	stargateProvider, err := provider.NewStargateProvider(config.StargateConfig{URL: upstream.URL})
	if err != nil {
		t.Fatalf("new stargate provider: %v", err)
	}
	cfg := config.Default()
	e := echo.New()
	e.Use(NewContextMiddleware(cfg))
	e.Use(NewNVCFAuthMiddleware(echoingInvocationAuthClient{}))
	RegisterRoutes(e, NewHandlers(cfg, stargateProvider, nil))
	return e, router
}

func serveModelNameRequest(e *echo.Echo, path, bodyFormat, model string) *httptest.ResponseRecorder {
	encodedModel, _ := json.Marshal(model)
	body := fmt.Sprintf(bodyFormat, encodedModel)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	req.Header.Set(echo.HeaderAuthorization, "Bearer sk-live")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

// TestLLMRoutesRejectMalformedModelNamesBeforeRouting covers model names the
// router header cannot carry unchanged. Go's HTTP client refuses control bytes
// in X-Model (a 502), the header path drops surrounding whitespace while the
// body keeps it, and the router cannot read non-ASCII bytes.
func TestLLMRoutesRejectMalformedModelNamesBeforeRouting(t *testing.T) {
	t.Parallel()

	const modelPrefix = "3f2b1c4d-5e6a-4b7c-8d9e-0a1b2c3d4e5f/company-name/model-name"
	const (
		whitespaceMessage = "model has leading or trailing whitespace"
		charactersMessage = "model contains control or non-ASCII characters"
	)

	routes := []struct {
		name       string
		path       string
		bodyFormat string
	}{
		{"chat", "/v1/chat/completions", `{"model":%s,"messages":[{"role":"user","content":"hi"}]}`},
		{"chat stream", "/v1/chat/completions", `{"model":%s,"stream":true,"messages":[{"role":"user","content":"hi"}]}`},
		{"responses", "/v1/responses", `{"model":%s,"input":"hi"}`},
		{"messages", "/v1/messages", `{"model":%s,"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`},
		{"embeddings", "/v1/embeddings", `{"model":%s,"input":["hi"]}`},
		{"decisions", "/v1/decisions", `{"model":%s}`},
	}
	cases := []struct {
		name        string
		model       string
		wantMessage string
	}{
		{"trailing space", modelPrefix + " ", whitespaceMessage},
		{"trailing tab", modelPrefix + "\t", whitespaceMessage},
		{"leading space", strings.Replace(modelPrefix, "/", "/ ", 1), whitespaceMessage},
		{"newline", modelPrefix + "\n", charactersMessage},
		{"carriage return newline", modelPrefix + "\r\n", charactersMessage},
		{"nul", modelPrefix + "\x00", charactersMessage},
		{"interior tab", strings.Replace(modelPrefix, "-name", "\t-name", 1), charactersMessage},
		{"delete", modelPrefix + "\x7f", charactersMessage},
		{"non-ascii", modelPrefix + "\u00e9", charactersMessage},
		{"invalid utf-8", modelPrefix + "\xff", charactersMessage},
	}

	e, router := newModelNameTestServer(t)
	for _, route := range routes {
		for _, tc := range cases {
			t.Run(route.name+"/"+tc.name, func(t *testing.T) {
				rec := serveModelNameRequest(e, route.path, route.bodyFormat, tc.model)
				seen := router.take()

				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
				}
				if !strings.Contains(rec.Body.String(), tc.wantMessage) {
					t.Fatalf("body = %s, want message %q", rec.Body.String(), tc.wantMessage)
				}
				if len(seen) != 0 {
					t.Fatalf("router received %d requests %q, want none", len(seen), seen)
				}
			})
		}
	}
}

func TestChatCompletionsSendsSameModelInHeaderAndBody(t *testing.T) {
	t.Parallel()

	e, router := newModelNameTestServer(t)
	rec := serveModelNameRequest(
		e,
		"/v1/chat/completions",
		`{"model":%s,"messages":[{"role":"user","content":"hi"}]}`,
		"3f2b1c4d-5e6a-4b7c-8d9e-0a1b2c3d4e5f/company-name/model-name",
	)
	seen := router.take()

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	want := []routerModelSeen{{header: "company-name/model-name", bodyModel: "company-name/model-name"}}
	if len(seen) != 1 || seen[0] != want[0] {
		t.Fatalf("router saw %q, want %q", seen, want)
	}
}
