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
package proxy

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nvcf-grpc-proxy/proxy/worker"
)

// hijackableRecorder can be taken over like an HTTP/1 response, which is how
// ReverseProxy completes an upgrade.
type hijackableRecorder struct {
	*httptest.ResponseRecorder
}

func (r hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, peer := net.Pipe()
	_ = peer.Close()
	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
}

// The upstream handlers below write what ReverseProxy writes for each kind of
// response. The end-to-end tests check the same fields through a real one.
func TestServeReportedRpc(t *testing.T) {
	grpcRequest := http.Header{"Content-Type": {"application/grpc"}}
	tests := []struct {
		name       string
		header     http.Header
		upstream   http.HandlerFunc
		protocol   string
		status     any // nil when status_code must be absent
		incomplete bool
		bytes      []float64 // bytes_in and bytes_out, nil when they must be absent
		responded  bool
	}{
		{
			name:   "gRPC status from the trailers",
			header: grpcRequest,
			upstream: func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.ReadAll(r.Body)
				_, _ = w.Write([]byte("reply"))
				w.Header().Set(http.TrailerPrefix+"Grpc-Status", "0")
			},
			protocol: rpcProtocolGRPC, status: float64(0), bytes: []float64{5, 5}, responded: true,
		},
		{
			name:   "gRPC trailers-only response",
			header: grpcRequest,
			upstream: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Grpc-Status", "14")
				w.WriteHeader(http.StatusOK)
			},
			protocol: rpcProtocolGRPC, status: float64(14), bytes: []float64{0, 0}, responded: true,
		},
		{
			name:   "gRPC call that ends without a status",
			header: grpcRequest,
			upstream: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadGateway)
			},
			protocol: rpcProtocolGRPC, incomplete: true, bytes: []float64{0, 0}, responded: true,
		},
		{
			name:     "gRPC call with nothing sent back",
			header:   grpcRequest,
			upstream: func(http.ResponseWriter, *http.Request) {},
			protocol: rpcProtocolGRPC, incomplete: true, bytes: []float64{0, 0},
		},
		{
			name: "HTTP response",
			upstream: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("ok"))
			},
			protocol: rpcProtocolHTTP, status: float64(http.StatusOK), bytes: []float64{0, 2}, responded: true,
		},
		{
			// ReverseProxy aborts this way when the worker goes away mid-body.
			name: "HTTP response cut off",
			upstream: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("partial"))
				panic(http.ErrAbortHandler)
			},
			protocol: rpcProtocolHTTP, status: float64(http.StatusOK), incomplete: true, bytes: []float64{0, 7}, responded: true,
		},
		{
			name:   "WebSocket upgrade",
			header: http.Header{"Upgrade": {"websocket"}},
			upstream: func(w http.ResponseWriter, _ *http.Request) {
				conn, _, err := http.NewResponseController(w).Hijack()
				if err == nil {
					_ = conn.Close()
				}
			},
			protocol: rpcProtocolWebSocket, status: float64(http.StatusSwitchingProtocols), responded: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			publisher, received := newRecordingLedger(t)
			director := &StreamDirector{eventLedger: publisher}
			wc := worker.NewWorkerConnection(uuid.New(), testFunctionId, testFunctionVersionId, func() {}, func() {})
			wc.CallerNcaId = "nca-1"
			r := httptest.NewRequest(http.MethodPost, "/Echo/EchoMessage", strings.NewReader("hello"))
			for k, v := range tc.header {
				r.Header[k] = v
			}

			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						assert.Equal(t, http.ErrAbortHandler, recovered, "only an abort may escape")
					}
				}()
				director.serveReportedRpc(tc.upstream, wc, hijackableRecorder{httptest.NewRecorder()}, r)
			}()
			publisher.Close()

			events := received()
			started := single(t, events, eventTypeRpcStarted)
			finished := single(t, events, eventTypeRpcFinished)
			assert.Equal(t, "nca-1", finished["ncaid"])
			assert.Equal(t, wc.RequestId.String(), finished["resourceid"])
			startedData, data := eventData(started), eventData(finished)
			for _, d := range []map[string]any{startedData, data} {
				assert.Equal(t, wc.AttachmentId.String(), d["attachment_id"])
				assert.Equal(t, "/Echo/EchoMessage", d["method"])
				assert.Equal(t, tc.protocol, d["protocol"])
			}
			require.NotEmpty(t, startedData["rpc_id"])
			assert.Equal(t, startedData["rpc_id"], data["rpc_id"])

			if tc.status == nil {
				assert.NotContains(t, data, "status_code")
			} else {
				assert.Equal(t, tc.status, data["status_code"])
			}
			assert.Equal(t, tc.incomplete, data["incomplete"])
			if tc.bytes == nil {
				assert.NotContains(t, data, "bytes_in")
				assert.NotContains(t, data, "bytes_out")
			} else {
				assert.Equal(t, []any{tc.bytes[0], tc.bytes[1]}, []any{data["bytes_in"], data["bytes_out"]})
			}
			_, responded := data["time_to_first_response_ms"]
			assert.Equal(t, tc.responded, responded)
		})
	}
}
