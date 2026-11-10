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

package gateway

import (
	config "ai-api-gateway-service/gateway_config"
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

const benchmarkBodySize = 10 << 20

// discardUpstream reads and discards each forwarded body, then signals done.
type discardUpstream struct {
	done chan struct{}
}

func (u *discardUpstream) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Body != nil {
		_, _ = io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
	}
	u.done <- struct{}{}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       http.NoBody,
		Request:    request,
	}, nil
}

func benchmarkChatBody(model string) []byte {
	prefix := `{"model":"` + model + `","messages":[{"role":"user","content":"`
	suffix := `"}]}`
	return []byte(prefix + strings.Repeat("a", benchmarkBodySize-len(prefix)-len(suffix)) + suffix)
}

func newBenchmarkDirector(b *testing.B, mappings *config.GatewayConfig, upstream http.RoundTripper) *ModelDirector {
	b.Helper()
	vanity, err := NewVanityDirector("http://nvcf.invalid", upstream)
	if err != nil {
		b.Fatal(err)
	}
	llm, err := NewLLMGatewayDirector("http://llm.invalid", upstream)
	if err != nil {
		b.Fatal(err)
	}
	director, err := NewModelDirector(mappings, regexp.MustCompile("^$"), vanity, llm, NewTrafficShadower(10, 30*time.Second))
	if err != nil {
		b.Fatal(err)
	}
	return director
}

// runBodyBenchmark sends body through ServeChatCompletions once per iteration
// and waits for upstreams forwarded requests, so shadow replays count too.
func runBodyBenchmark(b *testing.B, director *ModelDirector, upstream *discardUpstream, body []byte, upstreams int) {
	b.Helper()
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
		request.Header.Set(contentTypeHeader, contentTypeApplicationJSON)
		recorder := httptest.NewRecorder()
		director.ServeChatCompletions(recorder, request)
		if recorder.Code != http.StatusOK {
			b.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		for range upstreams {
			<-upstream.done
		}
	}
}

func BenchmarkLLMGatewayRequestBody(b *testing.B) {
	upstream := &discardUpstream{done: make(chan struct{}, 1)}
	director := newBenchmarkDirector(b, llmMappings(llmModelEntry()), upstream)
	runBodyBenchmark(b, director, upstream, benchmarkChatBody(publicModel), 1)
}

func BenchmarkShadowedRequestBody(b *testing.B) {
	const primaryModel = "microsoft/phi-2"
	const shadowModel = "microsoft/phi-2-shadow"

	mappings := &config.GatewayConfig{}
	mappings.OpenAI.Host = openAIHost
	mappings.OpenAI.ChatCompletions = map[string]config.ModelFunctionDetails{
		"primary": {
			ModelName:  primaryModel,
			FunctionID: "primary-func",
			Shadows:    []config.ShadowConfig{{ModelName: shadowModel}},
		},
		"shadow": {ModelName: shadowModel, FunctionID: "shadow-func"},
	}
	upstream := &discardUpstream{done: make(chan struct{}, 2)}
	director := newBenchmarkDirector(b, mappings, upstream)
	runBodyBenchmark(b, director, upstream, benchmarkChatBody(primaryModel), 2)
}
