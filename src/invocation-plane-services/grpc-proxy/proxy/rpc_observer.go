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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"nvcf-grpc-proxy/proxy/worker"
)

const (
	rpcProtocolGRPC      = "grpc"
	rpcProtocolWebSocket = "websocket"
	rpcProtocolHTTP      = "http"
)

// serveReportedRpc forwards r through upstream like upstream.ServeHTTP and
// reports the call to the event ledger: Rpc.Started before forwarding and
// Rpc.Finished once it ends, including when ReverseProxy aborts the copy.
func (s *StreamDirector) serveReportedRpc(upstream http.Handler, wc *worker.WorkerConnection, w http.ResponseWriter, r *http.Request) {
	rpc := &rpcObserver{
		ResponseWriter: w,
		id:             uuid.New(),
		method:         r.URL.Path,
		protocol:       rpcProtocol(r),
		startedAt:      time.Now(),
	}
	if r.Body != nil && r.Body != http.NoBody {
		// A shallow copy, so the server's own request keeps its body.
		r = r.WithContext(r.Context())
		rpc.body = &countingBody{ReadCloser: r.Body}
		r.Body = rpc.body
	}
	s.eventLedger.Publish(r.Context(), newRpcStarted(wc, rpc))
	completed := false
	defer func() {
		// Also runs while an aborted copy panics with http.ErrAbortHandler,
		// which then carries on unwinding.
		s.eventLedger.Publish(r.Context(), newRpcFinished(wc, rpc, !completed, time.Now()))
	}()
	upstream.ServeHTTP(rpc, r)
	completed = true
}

func rpcProtocol(r *http.Request) string {
	switch {
	case strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc"):
		return rpcProtocolGRPC
	case strings.EqualFold(r.Header.Get("Upgrade"), "websocket"):
		return rpcProtocolWebSocket
	}
	return rpcProtocolHTTP
}

// rpcObserver records what the client was sent. ReverseProxy writes to it
// from the handler goroutine only.
type rpcObserver struct {
	http.ResponseWriter
	id        uuid.UUID
	method    string
	protocol  string
	startedAt time.Time
	body      *countingBody

	status        int
	firstResponse time.Time
	bytesOut      int64
	hijacked      bool
}

func (o *rpcObserver) WriteHeader(code int) {
	o.responded()
	if o.status == 0 && code >= http.StatusOK {
		o.status = code
	}
	o.ResponseWriter.WriteHeader(code)
}

func (o *rpcObserver) Write(b []byte) (int, error) {
	o.responded()
	if o.status == 0 {
		o.status = http.StatusOK
	}
	n, err := o.ResponseWriter.Write(b)
	o.bytesOut += int64(n)
	return n, err
}

// Hijack is how ReverseProxy takes the connection over for an upgrade. The
// upgraded traffic is copied on the raw connection, so it is not counted.
func (o *rpcObserver) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(o.ResponseWriter).Hijack()
	if err == nil {
		o.responded()
		o.hijacked = true
		o.status = http.StatusSwitchingProtocols
	}
	return conn, rw, err
}

// Unwrap lets http.ResponseController reach the writer underneath, for Flush.
func (o *rpcObserver) Unwrap() http.ResponseWriter {
	return o.ResponseWriter
}

func (o *rpcObserver) responded() {
	if o.firstResponse.IsZero() {
		o.firstResponse = time.Now()
	}
}

// statusCode is the gRPC status of a gRPC call, otherwise the HTTP status.
func (o *rpcObserver) statusCode() (int, bool) {
	if o.protocol != rpcProtocolGRPC {
		return o.status, o.status != 0
	}
	// A trailers-only response carries the status in its headers. Otherwise
	// ReverseProxy adds the trailers to the header map once the body is done.
	header := o.Header()
	for _, key := range []string{"Grpc-Status", http.TrailerPrefix + "Grpc-Status"} {
		if values := header[key]; len(values) > 0 {
			if code, err := strconv.Atoi(values[0]); err == nil {
				return code, true
			}
		}
	}
	return 0, false
}

// countingBody counts the request bytes forwarded to the worker. The
// transport reads it on its own goroutine.
type countingBody struct {
	io.ReadCloser
	n atomic.Int64
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.n.Add(int64(n))
	return n, err
}
