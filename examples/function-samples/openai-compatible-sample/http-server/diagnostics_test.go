// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func captureRequestLogs(t *testing.T, enabled bool) *bytes.Buffer {
	t.Helper()
	previousEnabled, previousOutput := logRequests, log.Writer()
	t.Cleanup(func() {
		logRequests = previousEnabled
		log.SetOutput(previousOutput)
	})
	logRequests = enabled
	var output bytes.Buffer
	log.SetOutput(&output)
	return &output
}

func TestDecisionRequestAccessLogs(t *testing.T) {
	t.Setenv("NVCF_INSTANCE_ID", "test-pod")
	t.Setenv("NVCF_FUNCTION_ID", "test-function")
	t.Setenv("NVCF_FUNCTION_VERSION_ID", "test-version")
	for _, route := range []struct{ path, body string }{
		{decisionRoute, decisionsBody},
		{systemOneRoute, systemOneBody},
	} {
		for _, test := range []struct {
			name, method, query, body string
			status                    int
		}{
			{"success", http.MethodPost, "", route.body, http.StatusOK},
			{"echo", http.MethodPost, "", strings.TrimSuffix(route.body, "}") + `,"x_load_tester_echo":true,"image":"private-image-data"}`, http.StatusOK},
			{"invalid JSON", http.MethodPost, "", `{"private-body":`, http.StatusBadRequest},
			{"wrong method", http.MethodGet, "", "private-body", http.StatusMethodNotAllowed},
			{"mock error", http.MethodPost, "?mock_error=429&secret=private-query", route.body, http.StatusTooManyRequests},
		} {
			t.Run(route.path+"/"+test.name, func(t *testing.T) {
				var baselineBody string
				var baselineHeaders http.Header
				for _, enabled := range []bool{false, true} {
					t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
						output := captureRequestLogs(t, enabled)
						request := httptest.NewRequest(test.method, route.path+test.query, strings.NewReader(test.body))
						request.Header.Set("X-Request-Id", "caller-request-id")
						for _, name := range []string{"Authorization", "Proxy-Authorization", "X-Api-Key", "Cookie", "X-Other"} {
							request.Header.Set(name, "private-"+name)
						}
						recorder := httptest.NewRecorder()
						newRouter().ServeHTTP(recorder, request)
						if recorder.Code != test.status {
							t.Fatalf("status = %d, want %d: %s", recorder.Code, test.status, recorder.Body.String())
						}
						if !enabled {
							baselineBody, baselineHeaders = recorder.Body.String(), recorder.Header().Clone()
							if output.Len() != 0 {
								t.Fatalf("log while disabled: %s", output)
							}
							return
						}
						if recorder.Body.String() != baselineBody || !reflect.DeepEqual(recorder.Header(), baselineHeaders) {
							t.Fatal("logging changed response body or headers")
						}
						line := output.String()
						if strings.Count(line, "\n") != 1 {
							t.Fatalf("want one log line, got %q", line)
						}
						for _, field := range []string{`id="caller-request-id"`, `replica="test-pod"`, `method=` + strconv.Quote(test.method), `path=` + strconv.Quote(route.path), `function_id="test-function"`, `function_version_id="test-version"`} {
							if !strings.Contains(line, field) {
								t.Errorf("log missing %s: %s", field, line)
							}
						}
						if strings.Contains(line, "private-") || strings.Contains(line, "questions") {
							t.Fatalf("log leaked request data: %s", line)
						}
					})
				}
			})
		}
	}
}

func TestDecisionRequestLogEscapesFields(t *testing.T) {
	output := captureRequestLogs(t, true)
	request := httptest.NewRequest(http.MethodPost, decisionRoute, strings.NewReader(decisionsBody))
	request.Header.Set("X-Request-Id", "caller\nforged-log")
	newRouter().ServeHTTP(httptest.NewRecorder(), request)
	if line := output.String(); strings.Count(line, "\n") != 1 || !strings.Contains(line, `id="caller\nforged-log"`) {
		t.Fatalf("request id was not escaped: %q", line)
	}
}

func TestMessagesRequestLogUnchanged(t *testing.T) {
	t.Setenv("NVCF_INSTANCE_ID", "test-pod")
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			output := captureRequestLogs(t, enabled)
			recorder := postJSONWithHeaders(t, "/v1/messages", messagesBody+`}`, map[string]string{"X-Test": "private-value"})
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d", recorder.Code)
			}
			if !enabled {
				if output.Len() != 0 {
					t.Fatalf("log while disabled: %s", output)
				}
				return
			}
			want := "messages request id=" + recorder.Header().Get("request-id") + " replica=test-pod method=POST path=/v1/messages headers=content-type,x-test\n"
			if line := output.String(); !strings.HasSuffix(line, want) || strings.Count(line, "\n") != 1 {
				t.Fatalf("Messages log = %q, want suffix %q", line, want)
			}
		})
	}
}

func TestMessagesDiagnosticsOffByDefault(t *testing.T) {
	recorder := postJSONWithHeaders(t, "/v1/messages", messagesBody+`}`, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	if _, ok := responseMap(t, recorder)["x_backend"]; ok {
		t.Fatalf("x_backend present without the control: %s", recorder.Body.String())
	}
	if got := recorder.Header().Get(replicaHeader); got != "" {
		t.Fatalf("%s = %q without the control", replicaHeader, got)
	}
}

func TestMessagesDiagnosticsReportsReplicaAndHeaders(t *testing.T) {
	t.Setenv("NVCF_INSTANCE_ID", "0-sr-test-replica")
	t.Setenv("NVCF_FUNCTION_ID", "fn-id")
	t.Setenv("NVCF_FUNCTION_VERSION_ID", "ver-id")
	recorder := postJSONWithHeaders(t, "/v1/messages", messagesBody+`,"x_load_tester_echo_diagnostics":true}`, map[string]string{
		"X-Test-Header": "spoofed",
		"X-Api-Key":     "synthetic-secret",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get(replicaHeader); got != "0-sr-test-replica" {
		t.Fatalf("%s = %q, want 0-sr-test-replica", replicaHeader, got)
	}
	if got := recorder.Header().Get(functionVersionHeader); got != "ver-id" {
		t.Fatalf("%s = %q, want ver-id", functionVersionHeader, got)
	}
	body := responseMap(t, recorder)
	backend, ok := body["x_backend"].(map[string]any)
	if !ok {
		t.Fatalf("x_backend = %#v, want object", body["x_backend"])
	}
	if backend["replica"] != "0-sr-test-replica" || backend["function_id"] != "fn-id" || backend["path"] != "/v1/messages" || backend["method"] != "POST" {
		t.Fatalf("x_backend = %#v", backend)
	}
	headers, ok := backend["received_headers"].(map[string]any)
	if !ok {
		t.Fatalf("received_headers = %#v, want object", backend["received_headers"])
	}
	if got, ok := headers["x-test-header"].([]any); !ok || len(got) != 1 || got[0] != "spoofed" {
		t.Fatalf("received_headers[x-test-header] = %#v", headers["x-test-header"])
	}
	if got, ok := headers["x-api-key"].([]any); !ok || len(got) != 1 || got[0] != "<redacted>" {
		t.Fatalf("received_headers[x-api-key] = %#v, want [\"<redacted>\"]", headers["x-api-key"])
	}
}

func TestMessagesDiagnosticsFallsBackToHostname(t *testing.T) {
	t.Setenv("NVCF_INSTANCE_ID", "")
	host, err := os.Hostname()
	if err != nil {
		t.Skipf("os.Hostname: %v", err)
	}
	if got := replicaID(); got != host {
		t.Fatalf("replicaID() = %q, want hostname %q", got, host)
	}
}

func TestMessagesDiagnosticsStreamStart(t *testing.T) {
	t.Setenv("NVCF_INSTANCE_ID", "0-sr-test-replica")
	recorder := newFlushCountingRecorder()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/messages",
		strings.NewReader(messagesBody+`,"stream":true,"x_load_tester_echo_diagnostics":true}`))
	request.Header.Set("Content-Type", "application/json")
	newRouter().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get(replicaHeader); got != "0-sr-test-replica" {
		t.Fatalf("%s = %q on the stream", replicaHeader, got)
	}
	start := strings.SplitN(recorder.Body.String(), "event: content_block_start", 2)[0]
	if !strings.Contains(start, `"x_backend"`) || !strings.Contains(start, `"replica":"0-sr-test-replica"`) {
		t.Fatalf("message_start lacks x_backend: %s", start)
	}
}
