// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package suppress

import (
	"slices"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/jsonvalue"
)

func strictRules(strict bool) Config {
	cfg := namedRules()
	cfg.Strict = strict
	return cfg
}

func TestStrictModeStripsARecordARuleCannotEvaluate(t *testing.T) {
	tests := []struct {
		name       string
		strict     bool
		line       string
		action     Action
		unresolved []string
	}{
		{
			name:       "no headers captured",
			strict:     true,
			line:       `{"event_type":"request_payload","payload":{"request_id":"r1","request":{"messages":["secret"]}}}`,
			action:     StripPayload,
			unresolved: []string{"tenants", "nca_id"},
		},
		{
			name:       "headers as a string",
			strict:     true,
			line:       `{"event_type":"request_payload","payload":{"request_id":"r1","http_request_headers":"nvcf-ncaid: acme","request":{"messages":["secret"]}}}`,
			action:     StripPayload,
			unresolved: []string{"tenants", "nca_id"},
		},
		{
			name:       "one rule's header missing",
			strict:     true,
			line:       `{"event_type":"request_payload","payload":{"http_request_headers":{"nvcf-ncaid":"other"},"request":{"messages":["secret"]}}}`,
			action:     StripPayload,
			unresolved: []string{"tenants"},
		},
		{
			name:   "every header present, no match",
			strict: true,
			line:   `{"event_type":"request_payload","payload":{"http_request_headers":{"x-tenant":"t2","nvcf-ncaid":"other"}}}`,
		},
		{
			name:   "header present and matching",
			strict: true,
			line:   `{"event_type":"request_payload","payload":{"http_request_headers":{"nvcf-ncaid":"acme"}}}`,
			action: StripPayload,
		},
		{
			name:   "a matching drop rule still drops",
			strict: true,
			line:   `{"event_type":"request_payload","payload":{"http_request_headers":{"x-tenant":"t1"}}}`,
			action: DropRecord,
		},
		{
			name: "strict off exports what no rule can evaluate",
			line: `{"event_type":"request_payload","payload":{"request_id":"r1","request":{"messages":["secret"]}}}`,
		},
		{
			name: "strict off with headers as a string",
			line: `{"event_type":"request_payload","payload":{"http_request_headers":"nvcf-ncaid: acme","request":{"messages":["secret"]}}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := mustEngine(t, strictRules(tt.strict))
			kept, outcome := engine.Apply(mustDecode(t, tt.line))
			if outcome.Action != tt.action {
				t.Fatalf("action = %q, want %q", outcome.Action, tt.action)
			}
			if !slices.Equal(outcome.Unresolved, tt.unresolved) {
				t.Fatalf("unresolved = %v, want %v", outcome.Unresolved, tt.unresolved)
			}
			if tt.action == StripPayload && strings.Contains(string(jsonvalue.Encode(kept)), "secret") {
				t.Fatalf("stripped record still holds the body: %s", jsonvalue.Encode(kept))
			}
		})
	}
}

// A record that strict mode strips gives a reason of its own and carries no
// identifier, because no rule matched it.
func TestStrictStubNamesTheUnresolvedHeaderAndNoIdentifier(t *testing.T) {
	engine := mustEngine(t, strictRules(true))
	kept, _ := engine.Apply(mustDecode(t, `{"event_type":"request_payload","payload":{"request_id":"r1","model":"m","request":{"messages":["secret"]}}}`))
	got := string(jsonvalue.Encode(kept))
	for _, want := range []string{`"request_id":"r1"`, `"payload_suppressed":true`, `"suppression_reason":"header_unresolved"`, `"http_request_headers":{}`} {
		if !strings.Contains(got, want) {
			t.Errorf("stub = %s, want it to hold %s", got, want)
		}
	}
	if strings.Contains(got, "nca_id") {
		t.Errorf("stub = %s, want no NCA id", got)
	}
}

// Rules evaluate any record that carries request headers, so a record type
// newer than Dynamo v1.4.0 that carries them is checked too. A record with
// no header fields is left to the headerless policy.
func TestRulesEvaluateAnyRecordThatCarriesRequestHeaders(t *testing.T) {
	tests := []struct {
		name      string
		strict    bool
		line      string
		evaluated bool
		action    Action
	}{
		{name: "new type with a matching header", line: `{"event_type":"response_payload","payload":{"http_request_headers":{"nvcf-ncaid":"acme"},"response":"secret"}}`, evaluated: true, action: StripPayload},
		{name: "new type with headers as a string, strict", strict: true, line: `{"event_type":"response_payload","payload":{"http_request_headers":"x","response":"secret"}}`, evaluated: true, action: StripPayload},
		{name: "top-level headers", line: `{"event_type":"response_payload","headers":{"x-tenant":"t1"}}`, evaluated: true, action: DropRecord},
		{name: "no header fields", strict: true, line: `{"event_type":"request_end","request":{"request_id":"r1"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := mustEngine(t, strictRules(tt.strict))
			kept, outcome := engine.Apply(mustDecode(t, tt.line))
			if outcome.Evaluated != tt.evaluated || outcome.Action != tt.action {
				t.Fatalf("evaluated = %v action = %q, want %v %q", outcome.Evaluated, outcome.Action, tt.evaluated, tt.action)
			}
			if tt.action == StripPayload && strings.Contains(string(jsonvalue.Encode(kept)), "secret") {
				t.Fatalf("stripped record still holds the body: %s", jsonvalue.Encode(kept))
			}
		})
	}
}

// A newer record type that carries a payload but no request header field
// cannot be checked by any rule. Strict mode strips it; otherwise the
// headerless policy decides, as for trace records.
func TestStrictModeStripsAPayloadThatNoRuleCanCheck(t *testing.T) {
	const payloadNoHeaders = `{"event_type":"response_payload","payload":{"request_id":"r1","response":"secret"}}`
	const traceRecord = `{"event_type":"request_end","request":{"request_id":"r1"}}`
	tests := []struct {
		name       string
		strict     bool
		headerless HeaderlessPolicy
		line       string
		action     Action
		unresolved []string
	}{
		{name: "strict strips it", strict: true, line: payloadNoHeaders, action: StripPayload, unresolved: []string{"tenants", "nca_id"}},
		{name: "the drop policy still drops it", strict: true, headerless: HeaderlessDrop, line: payloadNoHeaders, action: DropRecord},
		{name: "strict off exports it", line: payloadNoHeaders},
		{name: "strict off with the drop policy drops it", headerless: HeaderlessDrop, line: payloadNoHeaders, action: DropRecord},
		{name: "a trace record is left alone", strict: true, line: traceRecord},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := strictRules(tt.strict)
			cfg.Headerless = tt.headerless
			kept, outcome := mustEngine(t, cfg).Apply(mustDecode(t, tt.line))
			if outcome.Action != tt.action || !slices.Equal(outcome.Unresolved, tt.unresolved) {
				t.Fatalf("action = %q unresolved = %v, want %q %v", outcome.Action, outcome.Unresolved, tt.action, tt.unresolved)
			}
			if tt.action == StripPayload && strings.Contains(string(jsonvalue.Encode(kept)), "secret") {
				t.Fatalf("stripped record still holds the body: %s", jsonvalue.Encode(kept))
			}
		})
	}
}

// Under the drop headerless policy, a record of another type that has a
// header field but on which no rule's header resolved is treated as
// headerless and dropped, as it was before such records were checked.
func TestHeaderlessDropCoversARecordNoRuleCouldResolve(t *testing.T) {
	tests := []struct {
		name       string
		line       string
		action     Action
		headerless bool
	}{
		{name: "headers null", line: `{"event_type":"tool_end","headers":null,"tool":{"output":"secret"}}`, action: DropRecord, headerless: true},
		{name: "empty request headers", line: `{"event_type":"response_payload","payload":{"http_request_headers":{},"response":"secret"}}`, action: DropRecord, headerless: true},
		{name: "a resolved header that does not match is exported", line: `{"event_type":"response_payload","headers":{"x-tenant":"t2"}}`},
		{name: "a request payload without headers is unchanged", line: `{"event_type":"request_payload","payload":{"request_id":"r1"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := strictRules(false)
			cfg.Headerless = HeaderlessDrop
			_, outcome := mustEngine(t, cfg).Apply(mustDecode(t, tt.line))
			if outcome.Action != tt.action || outcome.Headerless != tt.headerless {
				t.Fatalf("action = %q headerless = %v, want %q %v", outcome.Action, outcome.Headerless, tt.action, tt.headerless)
			}
		})
	}
}

// Strict mode strips a record no rule can check whatever kind its payload
// is, not only an object.
func TestStrictModeStripsAPayloadOfAnyKind(t *testing.T) {
	for name, line := range map[string]string{
		"string": `{"event_type":"request_payload_v2","payload":"secret"}`,
		"array":  `{"event_type":"request_payload_v2","payload":["secret"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			kept, outcome := mustEngine(t, strictRules(true)).Apply(mustDecode(t, line))
			if outcome.Action != StripPayload {
				t.Fatalf("action = %q, want %q", outcome.Action, StripPayload)
			}
			if strings.Contains(string(jsonvalue.Encode(kept)), "secret") {
				t.Fatalf("stripped record still holds the body: %s", jsonvalue.Encode(kept))
			}
		})
	}
}
