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

package nvcf

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	llmgatewaypb "github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/nvcf/pb"
)

func authorizeWithStub(t *testing.T, invocationService *stubInvocationService) *InvocationAuthResponse {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	llmgatewaypb.RegisterLlmGatewayServer(server, invocationService)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("create client conn: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	client := NewClientWithConn(conn, func() string { return "service-token" }, time.Second)
	authResponse, err := client.AuthorizeInvocation(context.Background(), "client-token", "fn-123")
	if err != nil {
		t.Fatalf("authorize invocation: %v", err)
	}
	return authResponse
}

func TestAuthorizeInvocationMapsFunctionRequestRateLimits(t *testing.T) {
	t.Parallel()

	authResponse := authorizeWithStub(t, &stubInvocationService{
		t:                            t,
		functionRequestRateLimit:     stringPtr("5-M"),
		functionUserRequestRateLimit: stringPtr("2-S,100-H"),
		ownerNCAID:                   "owner-nca",
	})

	if authResponse.FunctionRequestRateLimit != "5-M" {
		t.Fatalf("function request rate limit = %q, want 5-M", authResponse.FunctionRequestRateLimit)
	}
	if authResponse.FunctionUserRequestRateLimit != "2-S,100-H" {
		t.Fatalf("function user request rate limit = %q, want 2-S,100-H", authResponse.FunctionUserRequestRateLimit)
	}
	if authResponse.RequestLimitAccount != "owner-nca" {
		t.Fatalf("request limit account = %q, want the key owner's account owner-nca", authResponse.RequestLimitAccount)
	}
	if authResponse.RateLimitKey != "nca-456" {
		t.Fatalf("rate limit key = %q, want nca-456 (token buckets keep the authorized account)", authResponse.RateLimitKey)
	}
}

func TestAuthorizeInvocationRequestLimitAccountFallsBackToNCAID(t *testing.T) {
	t.Parallel()

	authResponse := authorizeWithStub(t, &stubInvocationService{t: t})

	if authResponse.FunctionRequestRateLimit != "" || authResponse.FunctionUserRequestRateLimit != "" {
		t.Fatalf("request limits = %q / %q, want empty when NVCF API sends none",
			authResponse.FunctionRequestRateLimit, authResponse.FunctionUserRequestRateLimit)
	}
	if authResponse.RequestLimitAccount != "nca-456" {
		t.Fatalf("request limit account = %q, want ncaId nca-456 when no owner is sent", authResponse.RequestLimitAccount)
	}
}
