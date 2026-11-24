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
	stdjson "encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/bytebufferpool"
)

// TestMain poisons every released request body buffer, so any test that reads
// a buffer after it returns to the pool sees corrupt bytes and fails.
func TestMain(m *testing.M) {
	poisonReleasedRequestBodies.Store(true)
	os.Exit(m.Run())
}

func newTestPooledBody(t *testing.T, content string) (*pooledRequestBody, *atomic.Int32) {
	t.Helper()
	buffer := bytebufferpool.Get()
	_, err := buffer.WriteString(content)
	require.NoError(t, err)
	var releases atomic.Int32
	body := newPooledRequestBody(buffer, func(buffer *bytebufferpool.ByteBuffer) {
		releases.Add(1)
		releaseRequestBodyBuffer(buffer)
	})
	return body, &releases
}

func TestTopLevelModelSpans(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "single model",
			body: `{"model":"a","messages":[]}`,
			want: `{"model":"X","messages":[]}`,
		},
		{
			name: "duplicate and mixed-case keys",
			body: `{"model":"a","Model":"b","MODEL":"c","x":1}`,
			want: `{"model":"X","Model":"X","MODEL":"X","x":1}`,
		},
		{
			name: "escaped key",
			body: `{"model":"a","\u006dodel":"b","MO\u0044EL":"c","mo\u0064el_x":"d"}`,
			want: `{"model":"X","\u006dodel":"X","MO\u0044EL":"X","mo\u0064el_x":"d"}`,
		},
		{
			name: "whitespace and non-string model",
			body: " {\n  \"messages\" : [{\"model\":\"nested\"}],\n  \"model\" : 42 ,\n  \"stream\": true\n}\n",
			want: " {\n  \"messages\" : [{\"model\":\"nested\"}],\n  \"model\" : \"X\" ,\n  \"stream\": true\n}\n",
		},
		{
			name: "strings with delimiters before model",
			body: `{"content":"}{][,\"model\":","nested":{"a":["]}"]},"model":null}`,
			want: `{"content":"}{][,\"model\":","nested":{"a":["]}"]},"model":"X"}`,
		},
		{
			name: "object and array model values",
			body: `{"model":{"a":[1,2]},"Model":[{"b":"}"}]}`,
			want: `{"model":"X","Model":"X"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := replaceTopLevelModel([]byte(tc.body), "X")
			require.NoError(t, err)
			assert.Equal(t, tc.want, string(got))
		})
	}
}

func TestTopLevelModelSpansRejectsInvalidBodies(t *testing.T) {
	for name, body := range map[string]string{
		"invalid JSON":    `{"model":"a"`,
		"trailing data":   `{"model":"a"} {}`,
		"not an object":   `["model"]`,
		"no model member": `{"messages":[{"model":"a"}]}`,
		"empty":           ``,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := topLevelModelSpans([]byte(body))
			assert.Error(t, err)
		})
	}
	// Strict JSON rejects these; the model extractor accepts them.
	for name, body := range lenientJSONBodies("m") {
		t.Run(name, func(t *testing.T) {
			_, err := topLevelModelSpans([]byte(body))
			assert.ErrorIs(t, err, errInvalidJSONBody)
		})
	}
}

func TestPooledRequestBodyReleasesBufferAfterLastReader(t *testing.T) {
	const original = `{"model":"primary","messages":[{"role":"user","content":"hi"}]}`
	primary, releases := newTestPooledBody(t, original)

	forwarded, err := primary.withTopLevelModel("fn/primary")
	require.NoError(t, err)
	shadow, err := primary.withTopLevelModel("shadow")
	require.NoError(t, err)

	wantForwarded := `{"model":"fn/primary","messages":[{"role":"user","content":"hi"}]}`
	wantShadow := `{"model":"shadow","messages":[{"role":"user","content":"hi"}]}`
	assert.Equal(t, int64(len(wantForwarded)), forwarded.Len())
	assert.Equal(t, int64(len(wantShadow)), shadow.Len())

	// Read part of the shadow body, then close the primary and forwarded
	// bodies. The shadow still holds a reference, so the buffer must stay out of
	// the pool and the rest of the shadow body must be intact.
	head := make([]byte, 5)
	_, err = io.ReadFull(shadow, head)
	require.NoError(t, err)
	require.NoError(t, primary.Close())
	forwardedBytes, err := io.ReadAll(forwarded)
	require.NoError(t, err)
	assert.Equal(t, wantForwarded, string(forwardedBytes))
	require.NoError(t, forwarded.Close())
	assert.Zero(t, releases.Load(), "buffer released while the shadow body still reads it")

	rest, err := io.ReadAll(shadow)
	require.NoError(t, err)
	assert.Equal(t, wantShadow, string(head)+string(rest))
	require.NoError(t, shadow.Close())
	assert.Equal(t, int32(1), releases.Load())

	require.NoError(t, shadow.Close(), "a second close is a no-op")
	assert.Equal(t, int32(1), releases.Load())
	_, err = shadow.Read(make([]byte, 1))
	assert.ErrorIs(t, err, http.ErrBodyReadAfterClose)
	_, err = primary.withTopLevelModel("late")
	assert.ErrorIs(t, err, http.ErrBodyReadAfterClose, "a closed body must not hand out new references")
}

// http.Transport may close a request body from another goroutine while it is
// still reading it. Close must wait for the read, and no read may observe the
// released buffer.
func TestPooledRequestBodyCloseDuringRead(t *testing.T) {
	original := `{"model":"m","content":"` + string(bytes.Repeat([]byte("a"), 64<<10)) + `"}`
	for range 50 {
		primary, releases := newTestPooledBody(t, original)
		forwarded, err := primary.withTopLevelModel("m")
		require.NoError(t, err)
		require.NoError(t, primary.Close())

		var read bytes.Buffer
		done := make(chan error, 1)
		go func() {
			chunk := make([]byte, 512)
			for {
				n, err := forwarded.Read(chunk)
				read.Write(chunk[:n])
				if err != nil {
					done <- err
					return
				}
			}
		}()
		require.NoError(t, forwarded.Close())
		err = <-done
		if err != io.EOF {
			assert.ErrorIs(t, err, http.ErrBodyReadAfterClose)
		}
		assert.Equal(t, original[:read.Len()], read.String(), "a read returned bytes from a released buffer")
		assert.Equal(t, int32(1), releases.Load())
	}
}

func TestTrafficShadowerClosesUnreadBody(t *testing.T) {
	body, releases := newTestPooledBody(t, `{"model":"m"}`)
	shadow, err := body.withTopLevelModel("shadow")
	require.NoError(t, err)
	require.NoError(t, body.Close())

	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	request.Body = shadow
	shadower := NewTrafficShadower(1, 30*time.Second)
	require.NoError(t, shadower.Shadow(request, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})))

	assert.Eventually(t, func() bool { return releases.Load() == 1 }, 5*time.Second, 10*time.Millisecond)
}

// gatedUpstream holds the primary LLM Gateway request until the shadow replay
// has been forwarded, which is after the replay finished reading the shadow
// body from the primary's pooled buffer and closed it. The primary body must
// still be intact when it is read afterwards.
type gatedUpstream struct {
	t            *testing.T
	shadowDone   chan struct{}
	shadowOnce   sync.Once
	mu           sync.Mutex
	bodiesByName map[string]string
}

func (u *gatedUpstream) RoundTrip(request *http.Request) (*http.Response, error) {
	isShadow := isShadowRequest(request)
	if !isShadow {
		select {
		case <-u.shadowDone:
		case <-time.After(5 * time.Second):
			u.t.Error("timed out waiting for the shadow request")
		}
	}
	body, err := io.ReadAll(request.Body)
	_ = request.Body.Close()
	if err != nil {
		return nil, err
	}
	var payload struct {
		Model string `json:"model"`
	}
	if err := stdjson.Unmarshal(body, &payload); err != nil {
		u.t.Errorf("forwarded body is not valid JSON: %v", err)
	}
	u.mu.Lock()
	u.bodiesByName[payload.Model] = string(body)
	u.mu.Unlock()
	if isShadow {
		u.shadowOnce.Do(func() { close(u.shadowDone) })
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: request}, nil
}

func TestLLMGatewayPrimaryBodySurvivesShadowRelease(t *testing.T) {
	const shadowModel = "meta/llama-3.3-70b-next"

	primary := llmModelEntry()
	primary.Shadows = []config.ShadowConfig{{ModelName: shadowModel}}
	shadow := llmModelEntry()
	shadow.ModelName = shadowModel
	mappings := &config.GatewayConfig{}
	mappings.OpenAI.Host = openAIHost
	mappings.OpenAI.ChatCompletions = map[string]config.ModelFunctionDetails{
		"llm":        primary,
		"llm-shadow": shadow,
	}

	upstream := &gatedUpstream{t: t, shadowDone: make(chan struct{}), bodiesByName: map[string]string{}}
	vanity, err := NewVanityDirector("http://nvcf.invalid", upstream)
	require.NoError(t, err)
	llm, err := NewLLMGatewayDirector("http://llm.invalid", upstream)
	require.NoError(t, err)
	director, err := NewModelDirector(mappings, regexp.MustCompile("^$"), vanity, llm, NewTrafficShadower(10, 30*time.Second))
	require.NoError(t, err)

	content := string(bytes.Repeat([]byte("b"), 1<<20))
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		bytes.NewBufferString(`{"messages":[{"role":"user","content":"`+content+`"}],"model":"`+publicModel+`","MODEL":"`+publicModel+`"}`))
	recorder := httptest.NewRecorder()
	director.ServeChatCompletions(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)

	wantBody := func(model string) string {
		target := `"` + llmFunctionID + "/" + model + `"`
		return `{"messages":[{"role":"user","content":"` + content + `"}],"model":` + target + `,"MODEL":` + target + `}`
	}
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	assert.Equal(t, wantBody(publicModel), upstream.bodiesByName[llmFunctionID+"/"+publicModel])
	assert.Equal(t, wantBody(shadowModel), upstream.bodiesByName[llmFunctionID+"/"+shadowModel])
}

// lenientJSONBodies returns bodies with model set that goccy/go-json decodes
// but json.Valid rejects.
func lenientJSONBodies(model string) map[string]string {
	prefix := `{"model":"` + model + `","x":`
	return map[string]string{
		"raw newline in string": prefix + "\"a\nb\"}",
		"raw tab in string":     prefix + "\"a\tb\"}",
		"invalid escape":        prefix + `"C:\path"}`,
		"leading zero number":   prefix + `010}`,
	}
}

// The LLM Gateway path must answer bodies that strict JSON rejects with 400
// invalid_json, as the decision routes do, instead of a 502.
func TestLLMGatewayRejectsLenientJSONBodiesWithBadRequest(t *testing.T) {
	llmRequests := make(chan capturedRequest, 1)
	llmBackend := captureServer(t, llmRequests, nil)
	mux := openAIMux(t, llmMappings(llmModelEntry()), "http://nvcf.invalid", llmBackend.URL)

	for name, body := range lenientJSONBodies(publicModel) {
		t.Run(name, func(t *testing.T) {
			_, err := extractOpenAIJSONBody(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(body)))
			require.NoError(t, err, "the model extractor must accept this body for the test to cover the rewrite path")

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, openAIRequest(t, "/v1/chat/completions", body))
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			var payload OpenAIError
			require.NoError(t, stdjson.Unmarshal(rec.Body.Bytes(), &payload))
			assert.Equal(t, "invalid_json", payload.Error.Code)
			assert.Empty(t, llmRequests, "an invalid body must not reach the LLM Gateway")
		})
	}
}
