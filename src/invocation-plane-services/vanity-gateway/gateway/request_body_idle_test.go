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
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const idleTestHost = "idle.test"

// idleTestGateway serves the production router, built by buildChiMux, behind
// the same h2c handler as the gateway server, with upstream as the NVCF API.
// The streamed body idle timeout is shortened to idle for the test.
func idleTestGateway(t *testing.T, idle time.Duration, upstream http.Handler) string {
	t.Helper()
	previous := streamedBodyIdleTimeout
	streamedBodyIdleTimeout = idle
	t.Cleanup(func() { streamedBodyIdleTimeout = previous })

	backend := httptest.NewServer(upstream)
	t.Cleanup(backend.Close)
	mappings := &config.GatewayConfig{}
	mappings.OpenAI.Host = idleTestHost
	mappings.OpenAI.ChatCompletions = map[string]config.ModelFunctionDetails{
		"direct": {ModelName: prefixTestModel, FunctionID: "direct-func"},
	}
	mux, err := buildChiMux(mappings, Config{NvcfApiEndpoint: backend.URL, PrivateModelNameRegexPattern: "^$"})
	require.NoError(t, err)
	gateway := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	t.Cleanup(gateway.Close)
	return gateway.URL
}

func idleTestClient(protocol string) *http.Client {
	if protocol == "h2c" {
		return &http.Client{Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, network, addr)
			},
		}}
	}
	return &http.Client{Transport: &http.Transport{}}
}

// idleTestUpload starts a streamed chat request whose body the test writes
// through the returned pipe, and delivers the client result.
func idleTestUpload(t *testing.T, protocol, url string) (*io.PipeWriter, <-chan clientResult) {
	t.Helper()
	pr, pw := io.Pipe()
	// Unblock the client and the upstream before the servers close.
	t.Cleanup(func() { _ = pw.CloseWithError(errors.New("test finished")) })
	req, err := http.NewRequest(http.MethodPost, url+"/v1/chat/completions", pr)
	require.NoError(t, err)
	req.Host = idleTestHost
	req.Header.Set(contentTypeHeader, contentTypeApplicationJSON)
	return pw, sendAsync(idleTestClient(protocol), req)
}

const idleTestHead = `{"model":"` + prefixTestModel + `","stream":false,"input":"`

// deadlineRecorder is a response writer that supports read deadlines. Like
// the HTTP/2 server's writer, it must not be used after its handler returns.
type deadlineRecorder struct {
	*httptest.ResponseRecorder
	t           *testing.T
	mu          sync.Mutex
	deadlines   []time.Time
	handlerDone bool
}

func (w *deadlineRecorder) SetReadDeadline(deadline time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handlerDone {
		w.t.Error("read deadline set after the handler returned")
	}
	w.deadlines = append(w.deadlines, deadline)
	return nil
}

func (w *deadlineRecorder) recorded() []time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.deadlines)
}

// heldBody blocks its first read until release is closed, then returns more
// data, as a client upload that stalls and later resumes.
type heldBody struct {
	reading chan struct{}
	release chan struct{}
	served  bool
}

func (b *heldBody) Read(p []byte) (int, error) {
	if b.served {
		return 0, io.EOF
	}
	close(b.reading)
	<-b.release
	b.served = true
	return copy(p, "more"), nil
}

func (b *heldBody) Close() error { return nil }

// TestStreamedBodyIdleLimit holds a read of the client body. A read that
// waits the idle limit gets one read deadline in the past, which ends it. Once
// the handler stops the limit, no deadline may be set, because the HTTP/2
// server's writer must not be used after its handler returns.
func TestStreamedBodyIdleLimit(t *testing.T) {
	const idle = 20 * time.Millisecond
	previous := streamedBodyIdleTimeout
	streamedBodyIdleTimeout = idle
	t.Cleanup(func() { streamedBodyIdleTimeout = previous })

	start := func(t *testing.T) (*heldBody, *deadlineRecorder, func(), <-chan []byte) {
		prefix := bytebufferpool.Get()
		_, _ = prefix.WriteString("head")
		rest := &heldBody{reading: make(chan struct{}), release: make(chan struct{})}
		body := newPrefixReplayBody(prefix, rest)
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		req.Body = body
		writer := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder(), t: t}
		stop := limitStreamedBodyIdle(writer, req)
		read := make(chan []byte, 1)
		go func() {
			got, _ := io.ReadAll(body)
			read <- got
		}()
		<-rest.reading
		return rest, writer, stop, read
	}

	t.Run("a read that waits the idle limit is ended once", func(t *testing.T) {
		rest, writer, stop, read := start(t)
		defer stop()
		require.Eventually(t, func() bool { return len(writer.recorded()) > 0 }, time.Second, idle/4)
		time.Sleep(3 * idle)
		deadlines := writer.recorded()
		require.Len(t, deadlines, 1)
		assert.True(t, deadlines[0].Before(time.Now()), "the deadline must end the waiting read now")
		close(rest.release)
		await(t, read, "the held read")
	})

	t.Run("no deadline after the handler stops the limit", func(t *testing.T) {
		rest, writer, stop, read := start(t)
		stop()
		// The limit may already have ended the read if this goroutine was
		// slow to stop it; only calls after stop are errors.
		writer.mu.Lock()
		writer.handlerDone = true
		setBeforeStop := len(writer.deadlines)
		writer.mu.Unlock()
		time.Sleep(5 * idle)
		close(rest.release)
		await(t, read, "the held read")
		assert.Len(t, writer.recorded(), setBeforeStop)
	})
}

// await returns the next value from ch, or fails the test after 10 seconds.
func await[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

type upstreamRead struct {
	size int
	err  error
}

// readingUpstream reads the whole request body, optionally after a delay,
// reports what it read, and answers 200.
func readingUpstream(delay time.Duration, reads chan<- upstreamRead) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		body, err := io.ReadAll(r.Body)
		reads <- upstreamRead{size: len(body), err: err}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	})
}

// TestStreamedRequestIdleUpload covers the idle timeout on a streamed body
// through the production router and server handler, over HTTP/1.1 and h2c.
// A stalled upload must end the upstream request. A slow but steady upload,
// an upstream that is slow to read, a long response after the upload, and an
// early upstream response must not be cut off.
func TestStreamedRequestIdleUpload(t *testing.T) {
	const idle = 400 * time.Millisecond
	for _, protocol := range []string{"http1", "h2c"} {
		t.Run(protocol+"/stalled upload ends the upstream request", func(t *testing.T) {
			reads := make(chan upstreamRead, 1)
			url := idleTestGateway(t, idle, readingUpstream(0, reads))
			pw, result := idleTestUpload(t, protocol, url)
			_, err := io.WriteString(pw, idleTestHead+filler(2*requestPrefixLimit))
			require.NoError(t, err)

			assert.Error(t, await(t, reads, "the upstream body").err, "the upstream must see an incomplete body")
			if got := await(t, result, "the client response"); got.err == nil {
				assert.Equal(t, statusClientClosedRequest, got.status, got.body)
			}
		})

		t.Run(protocol+"/slow steady upload completes", func(t *testing.T) {
			const chunks = 12
			wantSize := len(idleTestHead) + 2*requestPrefixLimit + chunks*1024 + 2
			reads := make(chan upstreamRead, 1)
			url := idleTestGateway(t, idle, readingUpstream(0, reads))
			pw, result := idleTestUpload(t, protocol, url)
			_, err := io.WriteString(pw, idleTestHead+filler(2*requestPrefixLimit))
			require.NoError(t, err)
			for range chunks {
				time.Sleep(idle / 4)
				_, err = io.WriteString(pw, filler(1024))
				require.NoError(t, err)
			}
			_, err = io.WriteString(pw, `"}`)
			require.NoError(t, err)
			require.NoError(t, pw.Close())

			read := await(t, reads, "the upstream body")
			require.NoError(t, read.err)
			assert.Equal(t, wantSize, read.size)
			got := await(t, result, "the client response")
			require.NoError(t, got.err)
			assert.Equal(t, http.StatusOK, got.status, got.body)
		})

		t.Run(protocol+"/upstream slow to read does not end the upload", func(t *testing.T) {
			// Larger than the socket and flow control buffers, so the
			// gateway waits on the upstream, not on the client.
			const size = 16 << 20
			reads := make(chan upstreamRead, 1)
			url := idleTestGateway(t, idle, readingUpstream(3*idle, reads))
			pw, result := idleTestUpload(t, protocol, url)
			go func() {
				if _, err := io.WriteString(pw, idleTestHead+filler(size)+`"}`); err == nil {
					_ = pw.Close()
				}
			}()

			read := await(t, reads, "the upstream body")
			require.NoError(t, read.err)
			assert.Equal(t, len(idleTestHead)+size+2, read.size)
			got := await(t, result, "the client response")
			require.NoError(t, got.err)
			assert.Equal(t, http.StatusOK, got.status, got.body)
		})

		t.Run(protocol+"/long response after the upload is not cut off", func(t *testing.T) {
			const parts = 10
			url := idleTestGateway(t, idle, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				for range parts {
					_, _ = io.WriteString(w, "data: part\n\n")
					w.(http.Flusher).Flush()
					time.Sleep(idle / 2)
				}
			}))
			pw, result := idleTestUpload(t, protocol, url)
			_, err := io.WriteString(pw, idleTestHead+filler(2*requestPrefixLimit)+`"}`)
			require.NoError(t, err)
			require.NoError(t, pw.Close())

			got := await(t, result, "the client response")
			require.NoError(t, got.err)
			assert.Equal(t, http.StatusOK, got.status)
			assert.Equal(t, parts, strings.Count(got.body, "data: part"), "the response ran past several idle windows and must arrive in full")
		})

		t.Run(protocol+"/early upstream response with a stalled upload", func(t *testing.T) {
			url := idleTestGateway(t, idle, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Answer without waiting for the body. Without full duplex
				// the test server first reads up to 256 KiB more of it.
				require.NoError(t, http.NewResponseController(w).EnableFullDuplex())
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = io.WriteString(w, `{"error":"early-reject"}`)
			}))
			pw, result := idleTestUpload(t, protocol, url)
			_, err := io.WriteString(pw, idleTestHead+filler(2*requestPrefixLimit))
			require.NoError(t, err)

			got := await(t, result, "the early upstream response")
			require.NoError(t, got.err)
			assert.Equal(t, http.StatusUnauthorized, got.status)
			// Let a read still waiting on the stalled upload reach the idle
			// limit after the handler returned.
			time.Sleep(3 * idle)
		})
	}
}
