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

package requestctx

import (
	"github.com/dsx-ai-factory/nvcf/src/invocation-plane-services/llm-gateway/nvcf"
)

// Session sources recorded in RequestContext.SessionSource.
const (
	SessionSourceHeader           = "header"
	SessionSourcePayload          = "payload"
	SessionSourcePromptCacheKey   = "prompt_cache_key"
	SessionSourceConversationID   = "conversation_id"
	SessionSourceClaudeCodeHeader = "claude_code_header"
)

type RequestContext struct {
	RequestID        string
	APIKeyID         string // client auth subject from NVCF API, set for API key and JWT callers; keys the per-user function request rate limit, so it must stay the subject, not a key ID
	OrgID            string // auth-derived rate limit key; currently used to scope rate limiting
	ProjectID        string // used to further scope rate limiting when auth provides a project ID
	BearerToken      string
	RoutingKey       string
	Model            string
	RoutingMethod    string
	ModelSpecs       map[string]nvcf.ModelSpec
	TargetRegion     string
	SessionID        string
	SessionSource    string
	CacheAffinityKey string
	// LastClusterLookupKey and LastClusterWriteKey are set only for payload
	// sessions when last-cluster hints are on. They are opaque prefix-chain
	// values; the lookup value is empty on the first turn.
	LastClusterLookupKey string
	LastClusterWriteKey  string
	// Priority is the caller priority resolved by NVCF API at auth time, or nil
	// when no priority config applies. Lower value is higher priority, 0 is
	// highest. Forwarded to Stargate as the X-Priority header when set.
	Priority *uint32
	// AccountInputTokenRateLimit and AccountOutputTokenRateLimit are the
	// account-scoped token rate limit resolved by NVCF API, keyed by OrgID
	// (ncaId) rather than any one function. Empty when none applies.
	AccountInputTokenRateLimit  string
	AccountOutputTokenRateLimit string
	// FunctionRequestRateLimit and FunctionUserRequestRateLimit are the
	// function's request rate limits for this caller, resolved by NVCF API.
	// RequestLimitAccount is the account they count against (the key owner's
	// account, else OrgID). Empty limits mean no request limit applies.
	FunctionRequestRateLimit     string
	FunctionUserRequestRateLimit string
	RequestLimitAccount          string
}
