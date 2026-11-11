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

// End-to-end checks of the events a session sends to the event ledger, with a
// real gRPC client and worker on either side of the proxy. They cover only
// what needs real traffic; the event fields themselves are unit tested.

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"nvcf-grpc-proxy/proxy/eventledger"
	"nvcf-grpc-proxy/proxy/internal/echo"
	"nvcf-grpc-proxy/proxy/metrics"
)

const e2eWait = 10 * time.Second

type e2eEnv struct {
	director  *StreamDirector
	publisher *eventledger.Publisher
	received  func() []map[string]any
	client    echo.EchoClient
	conn      *grpc.ClientConn
}

// newE2EEnv starts the proxy's HTTP/2 listener with a recording ledger and
// connects one client. Every RPC on that client shares one session.
func newE2EEnv(t *testing.T) *e2eEnv {
	t.Helper()
	publisher, received := newRecordingLedger(t)
	director := newEventDirector(t, publisher)
	listener, addr := bindEphemeral(t)
	health, err := healthManager("http://"+addr, nil)
	require.NoError(t, err)
	t.Cleanup(serveProxy(t, createHttp2Server(director, "", health), listener))
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &e2eEnv{director: director, publisher: publisher, received: received, client: echo.NewEchoClient(conn), conn: conn}
}

// attachWorker waits for the session's work to be dispatched, then attaches
// an echo worker as a worker CONNECT does. It returns the session's request
// ID and the proxy end of the tunnel.
func (e *e2eEnv) attachWorker(t *testing.T) (uuid.UUID, net.Conn) {
	t.Helper()
	e.waitFor(t, eventTypeWorkDispatched)
	info := e.director.workerAuth.Get(testWorkerToken).Value()
	listener, addr := bindEphemeral(t)
	server := grpc.NewServer()
	server.RegisterService(&echo.Echo_ServiceDesc, &echo.Server{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	tunnel, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, e.director.RegisterWorker(t.Context(), testWorkerToken, info, tunnel))
	return info.requestId, tunnel
}

// waitFor blocks until the ledger has accepted each listed event type.
func (e *e2eEnv) waitFor(t *testing.T, eventTypes ...string) {
	t.Helper()
	require.Eventually(t, func() bool {
		events := e.received()
		for _, eventType := range eventTypes {
			if len(ofType(events, eventType)) == 0 {
				return false
			}
		}
		return true
	}, e2eWait, 5*time.Millisecond, "waiting for %v", eventTypes)
}

// finish closes the client, waits for the session's last events and returns
// everything the ledger accepted.
func (e *e2eEnv) finish(t *testing.T) []map[string]any {
	t.Helper()
	require.NoError(t, e.conn.Close())
	e.waitFor(t, eventTypeAttachmentClosed, eventTypeRpcStarted, eventTypeRpcFinished)
	// Close delivers anything still in flight, so the counts below are final.
	e.publisher.Close()
	return e.received()
}

func eventData(event map[string]any) map[string]any {
	data, _ := event["data"].(map[string]any)
	return data
}

// single returns the only event of eventType, failing if there is not exactly
// one.
func single(t *testing.T, events []map[string]any, eventType string) map[string]any {
	t.Helper()
	matching := ofType(events, eventType)
	require.Len(t, matching, 1, "events of type %s in %v", eventType, eventTypes(events))
	return matching[0]
}

func clientCtx() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer client-token", "function-id", testFunctionId)
}

func unaryInBackground(client echo.EchoClient) <-chan error {
	errs := make(chan error, 1)
	go func() {
		_, err := client.EchoMessage(clientCtx(), &echo.EchoRequest{Message: "hello"})
		errs <- err
	}()
	return errs
}

func callResult(t *testing.T, errs <-chan error) error {
	t.Helper()
	select {
	case err := <-errs:
		return err
	case <-time.After(e2eWait):
		t.Fatal("the client call did not return")
		return nil
	}
}

func echoOnce(t *testing.T, stream echo.Echo_EchoMessageStreamingClient) {
	t.Helper()
	require.NoError(t, stream.Send(&echo.EchoRequest{Message: "hello"}))
	_, err := stream.Recv()
	require.NoError(t, err)
}

// assertSession checks the one session in events: a single Dispatched,
// Established and Closed, and a single reported RPC, all about requestId and
// observed in order. It returns Rpc.Finished's data.
func assertSession(t *testing.T, events []map[string]any, requestId uuid.UUID, cause, method string) map[string]any {
	t.Helper()
	ordered := []map[string]any{
		single(t, events, eventTypeWorkDispatched),
		single(t, events, eventTypeAttachmentEstablished),
		single(t, events, eventTypeRpcStarted),
		single(t, events, eventTypeRpcFinished),
	}
	closed := single(t, events, eventTypeAttachmentClosed)
	assert.Empty(t, ofType(events, eventTypeAttachmentSetupFailed))
	var previous time.Time
	for _, event := range append(ordered, closed) {
		assert.Equal(t, requestId.String(), event["resourceid"], "%s resourceid", event["type"])
		assert.Equal(t, "nca-1", event["ncaid"], "%s ncaid", event["type"])
		if id, ok := eventData(event)["attachment_id"]; ok {
			assert.Equal(t, eventData(ordered[1])["attachment_id"], id, "%s attachment_id", event["type"])
		}
	}
	// Closed is stamped when the tunnel stopped, which can precede Finished.
	for _, event := range ordered {
		at, err := time.Parse(time.RFC3339Nano, event["time"].(string))
		require.NoError(t, err)
		assert.False(t, at.Before(previous), "%s observed before the event preceding it", event["type"])
		previous = at
	}
	assert.Equal(t, cause, eventData(closed)["cause"])
	finished := eventData(ordered[3])
	assert.Equal(t, method, finished["method"])
	assert.Equal(t, eventData(ordered[2])["rpc_id"], finished["rpc_id"])
	return finished
}

func TestLedgerE2EUnaryCall(t *testing.T) {
	e := newE2EEnv(t)
	replies := unaryInBackground(e.client)
	requestId, _ := e.attachWorker(t)
	require.NoError(t, callResult(t, replies))

	finished := assertSession(t, e.finish(t), requestId, metrics.CloseReasonClientClosed, echo.Echo_EchoMessage_FullMethodName)
	assert.Equal(t, float64(codes.OK), finished["status_code"])
	assert.Equal(t, false, finished["incomplete"])
}

func TestLedgerE2EConcurrentCallsShareOneSession(t *testing.T) {
	e := newE2EEnv(t)
	replies := make([]<-chan error, 100)
	for i := range replies {
		replies[i] = unaryInBackground(e.client)
	}
	requestId, _ := e.attachWorker(t)
	for _, reply := range replies {
		require.NoError(t, callResult(t, reply))
	}

	// One dispatch, one attachment and one reported RPC for all 100 calls.
	assertSession(t, e.finish(t), requestId, metrics.CloseReasonClientClosed, echo.Echo_EchoMessage_FullMethodName)
}

func TestLedgerE2EClientCancelsStream(t *testing.T) {
	e := newE2EEnv(t)
	ctx, cancel := context.WithCancel(clientCtx())
	defer cancel()
	stream, err := e.client.EchoMessageStreaming(ctx)
	require.NoError(t, err)
	requestId, _ := e.attachWorker(t)
	echoOnce(t, stream)
	cancel()
	_, err = stream.Recv()
	require.Equal(t, codes.Canceled, status.Code(err), "%v", err)

	// Cancelling one RPC does not end the session: the next call reuses the
	// attachment without a new dispatch.
	require.NoError(t, callResult(t, unaryInBackground(e.client)))

	finished := assertSession(t, e.finish(t), requestId, metrics.CloseReasonClientClosed, echo.Echo_EchoMessageStreaming_FullMethodName)
	assert.Equal(t, true, finished["incomplete"])
}

// A client that is only receiving must still learn that its worker is gone.
// The transport fails a response only once its read of the request body
// returns, so this hangs unless the proxy closes the body when the tunnel goes.
func TestLedgerE2ETunnelLossMidStream(t *testing.T) {
	tests := []struct {
		name  string
		lose  func(t *testing.T, e *e2eEnv, tunnel net.Conn)
		cause string
	}{
		{
			name:  "worker connection drops",
			lose:  func(_ *testing.T, _ *e2eEnv, tunnel net.Conn) { _ = tunnel.Close() },
			cause: metrics.CloseReasonWorkerClosed,
		},
		{
			name:  "proxy shuts down",
			lose:  func(t *testing.T, e *e2eEnv, _ net.Conn) { require.NoError(t, e.director.Close()) },
			cause: metrics.CloseReasonShutdown,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newE2EEnv(t)
			stream, err := e.client.EchoMessageStreaming(clientCtx())
			require.NoError(t, err)
			requestId, tunnel := e.attachWorker(t)
			echoOnce(t, stream)

			tc.lose(t, e, tunnel)
			recvErrs := make(chan error, 1)
			go func() {
				_, err := stream.Recv()
				recvErrs <- err
			}()
			select {
			case err := <-recvErrs:
				assert.Contains(t, []codes.Code{codes.Internal, codes.Unavailable}, status.Code(err), "%v", err)
			case <-time.After(2 * time.Second):
				t.Fatal("the client stream is still waiting after its worker went away")
			}

			finished := assertSession(t, e.finish(t), requestId, tc.cause, echo.Echo_EchoMessageStreaming_FullMethodName)
			assert.Equal(t, true, finished["incomplete"])
		})
	}
}
