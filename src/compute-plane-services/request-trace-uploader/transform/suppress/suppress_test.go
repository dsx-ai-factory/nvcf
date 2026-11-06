// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package suppress

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/compat"
)

// goldenCase holds what the earlier Python uploader produced for one line:
// the NCA id it resolved and, when suppressed, the stub it wrote.
type goldenCase struct {
	In    string  `json:"in"`
	IDs   string  `json:"ids"`
	NCAID string  `json:"nca_id"`
	Out   *string `json:"out"`
}

func loadGolden(t *testing.T) []goldenCase {
	t.Helper()
	data, err := os.ReadFile("testdata/python.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

func mustDecode(t *testing.T, line string) *compat.Value {
	t.Helper()
	value, err := compat.Decode([]byte(line))
	if err != nil {
		t.Fatalf("Decode(%s) error = %v", line, err)
	}
	return value
}

func mustEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return engine
}

// Where the earlier uploader leaked, this one differs on purpose. A record
// with a repeated key never reaches the rules: Decode refuses it, and the
// line is rejected. A blank header value does not hide a later key of the
// same name in another case, so that record is suppressed.
var (
	repeatedKeyCase = `{"schema":"dynamo.request.trace.v1","event_type":"request_payload","event_time_unix_ms":1777312801000,"event_source":"frontend","payload":{"request_id":"dup","request_id":"r20","http_request_headers":{"nvcf-ncaid":"acme"}}}`
	blankFirstCase  = `{"schema":"dynamo.request.trace.v1","event_type":"request_payload","event_time_unix_ms":1777312801000,"event_source":"frontend","payload":{"request_id":"r21","http_request_headers":{"Nvcf-NcaId":"","nvcf-ncaid":"acme"}}}`
)

// pythonCases returns the golden cases this uploader shares with the earlier
// one.
func pythonCases(t *testing.T) []goldenCase {
	t.Helper()
	var cases []goldenCase
	for _, tc := range loadGolden(t) {
		if tc.In != repeatedKeyCase && tc.In != blankFirstCase {
			cases = append(cases, tc)
		}
	}
	if len(cases) != len(loadGolden(t))-2 {
		t.Fatal("the golden set no longer holds both divergent cases")
	}
	return cases
}

func TestDivergencesFromThePythonUploader(t *testing.T) {
	if _, err := compat.Decode([]byte(repeatedKeyCase)); err == nil {
		t.Error("Decode(repeated key) error = nil, want it refused")
	}
	engine := mustEngine(t, FromIDs([]string{"acme"}))
	if _, outcome := engine.Apply(mustDecode(t, blankFirstCase)); outcome.Action != StripPayload || outcome.ID != "acme" {
		t.Errorf("outcome = %+v, want the record suppressed for acme", outcome)
	}
}

func TestStubMatchesPythonUploaderByteForByte(t *testing.T) {
	for _, tc := range pythonCases(t) {
		t.Run(tc.In, func(t *testing.T) {
			engine := mustEngine(t, FromIDs(strings.Split(tc.IDs, ",")))
			record := mustDecode(t, tc.In)
			before := string(compat.Encode(record))

			got, outcome := engine.Apply(record)
			if tc.Out == nil {
				if outcome.Action != "" || got != record {
					t.Fatalf("outcome = %+v, want the record passed through", outcome)
				}
				if after := string(compat.Encode(record)); after != before {
					t.Fatalf("record changed to %s", after)
				}
				return
			}
			if outcome.Action != StripPayload || outcome.ID != tc.NCAID {
				t.Fatalf("outcome = %+v, want stripPayload with id %q", outcome, tc.NCAID)
			}
			if encoded := string(compat.Encode(got)); encoded != *tc.Out {
				t.Errorf("stub = %s\nwant   %s", encoded, *tc.Out)
			}
		})
	}
}

func TestResolveIDMatchesPythonUploader(t *testing.T) {
	for _, tc := range pythonCases(t) {
		event := EventOf(mustDecode(t, tc.In))
		got := ""
		if CarriesPayload(event) {
			got = NormalizeID(ResolveHeader(HeaderSets(event), DefaultHeaders...))
		}
		if got != tc.NCAID {
			t.Errorf("%s: id = %q, want %q", tc.In, got, tc.NCAID)
		}
	}
}

func TestDropRecordRuleDropsTheRecord(t *testing.T) {
	engine := mustEngine(t, Config{Rules: []Rule{{Headers: DefaultHeaders, Values: []string{"acme"}, Action: DropRecord}}})
	record := mustDecode(t, `{"event_type":"request_payload","payload":{"http_request_headers":{"nvcf-ncaid":"nca-acme-nca"}}}`)

	got, outcome := engine.Apply(record)
	if got != nil || outcome.Action != DropRecord || outcome.ID != "acme" {
		t.Fatalf("got = %v outcome = %+v, want the record dropped for acme", got, outcome)
	}
}

func TestFirstMatchingRuleWins(t *testing.T) {
	engine := mustEngine(t, Config{Rules: []Rule{
		{Headers: []string{"x-tenant"}, Values: []string{"t1"}, Action: DropRecord},
		{Headers: DefaultHeaders, Values: []string{"acme"}, Action: StripPayload},
	}})
	record := mustDecode(t, `{"event_type":"request_payload","payload":{"http_request_headers":{"nvcf-ncaid":"acme","x-tenant":"t1"}}}`)

	if _, outcome := engine.Apply(record); outcome.Action != DropRecord || outcome.ID != "t1" {
		t.Fatalf("outcome = %+v, want the first rule", outcome)
	}
}

func TestOutcomeReportsEvaluationAndHeaderPresence(t *testing.T) {
	engine := mustEngine(t, FromIDs([]string{"acme"}))
	tests := []struct {
		name          string
		line          string
		evaluated     bool
		headerPresent bool
	}{
		{name: "payload with header", line: `{"event_type":"request_payload","payload":{"http_request_headers":{"nvcf-ncaid":"other"}}}`, evaluated: true, headerPresent: true},
		{name: "payload without header", line: `{"event_type":"request_payload","payload":{"http_request_headers":{"x":"y"}}}`, evaluated: true},
		{name: "trace record", line: `{"event_type":"request_end","headers":{"nvcf-ncaid":"acme"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, outcome := engine.Apply(mustDecode(t, tt.line))
			if outcome.Evaluated != tt.evaluated || outcome.HeaderPresent != tt.headerPresent || outcome.Action != "" {
				t.Fatalf("outcome = %+v, want evaluated %v header %v and no action", outcome, tt.evaluated, tt.headerPresent)
			}
		})
	}
}

func TestNoRulesPassesEverythingThrough(t *testing.T) {
	engine := mustEngine(t, FromIDs([]string{"", " "}))
	if engine.Active() {
		t.Fatal("Active() = true with only blank ids")
	}
	record := mustDecode(t, `{"event_type":"request_payload","payload":{"http_request_headers":{"nvcf-ncaid":""}}}`)
	if got, outcome := engine.Apply(record); got != record || outcome.Action != "" {
		t.Fatalf("outcome = %+v, want pass through", outcome)
	}
}

func TestNormalizeID(t *testing.T) {
	tests := map[string]string{
		"acme":           "acme",
		"nca-acme-nca":   "acme",
		" nca-acme-nca ": "acme",
		"nca-acme":       "nca-acme",
		"acme-nca":       "acme-nca",
		"nca--nca":       "",
		"":               "",
	}
	for in, want := range tests {
		if got := NormalizeID(in); got != want {
			t.Errorf("NormalizeID(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{name: "none", cfg: Config{}},
		{name: "from ids", cfg: FromIDs([]string{"a", "b"})},
		{name: "unknown action", cfg: Config{Rules: []Rule{{Headers: DefaultHeaders, Values: []string{"a"}, Action: "bashStub"}}}, wantErr: true},
		{name: "no headers", cfg: Config{Rules: []Rule{{Values: []string{"a"}, Action: StripPayload}}}, wantErr: true},
		{name: "blank header", cfg: Config{Rules: []Rule{{Headers: []string{""}, Values: []string{"a"}, Action: StripPayload}}}, wantErr: true},
		{name: "upper-case header", cfg: Config{Rules: []Rule{{Headers: []string{"NVCF-NCAID"}, Values: []string{"a"}, Action: StripPayload}}}, wantErr: true},
		{name: "no values", cfg: Config{Rules: []Rule{{Headers: DefaultHeaders, Values: []string{" "}, Action: StripPayload}}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if _, err := New(tt.cfg); (err != nil) != tt.wantErr {
				t.Fatalf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// A blank value under the first matching key does not hide a later key with
// a value.
func TestResolveHeaderSkipsABlankKey(t *testing.T) {
	engine := mustEngine(t, FromIDs([]string{"acme"}))
	record := mustDecode(t, `{"event_type":"request_payload","payload":{"http_request_headers":{"NVCF-NCAID":"","nvcf-ncaid":"acme"}}}`)
	if _, outcome := engine.Apply(record); outcome.Action != StripPayload {
		t.Fatalf("outcome = %+v, want the record suppressed", outcome)
	}
}

// Only an object under "event" wraps a record; a bare record that also has
// an "event" field is still evaluated.
func TestEventOfUnwrapsOnlyAnObject(t *testing.T) {
	engine := mustEngine(t, FromIDs([]string{"acme"}))
	record := mustDecode(t, `{"event_type":"request_payload","event":"x","payload":{"http_request_headers":{"nvcf-ncaid":"acme"}}}`)
	if _, outcome := engine.Apply(record); outcome.Action != StripPayload {
		t.Fatalf("outcome = %+v, want the bare record suppressed", outcome)
	}
}

func TestValidateRejectsAHeaderNameThatIsNotAToken(t *testing.T) {
	for _, name := range []string{"nvcf-ncaid ", " nvcf-ncaid", "nvcf ncaid", "nvcf-ncaid:", "x-tenant\t", "\u00e9"} {
		cfg := Config{Rules: []Rule{{Headers: []string{name}, Values: []string{"a"}, Action: DropRecord}}}
		if err := cfg.Validate(); err == nil {
			t.Errorf("Validate(header %q) error = nil, want an error", name)
		}
	}
}

// A configured id is normalized once, like the header it is compared with,
// so an id with nested affixes still matches the same header value.
func TestFromIDsNormalizesEachIDOnce(t *testing.T) {
	const id = "nca-nca-acme-nca-nca"
	engine := mustEngine(t, FromIDs([]string{" " + id + " "}))
	record := mustDecode(t, `{"schema":"dynamo.request.trace.v1","event_type":"request_payload","event_time_unix_ms":1,"payload":{"request_id":"r","http_request_headers":{"nvcf-ncaid":"`+id+`"}}}`)
	if _, outcome := engine.Apply(record); outcome.Action != StripPayload || outcome.ID != "nca-acme-nca" {
		t.Fatalf("outcome = %+v, want stripPayload with id nca-acme-nca", outcome)
	}
}

func TestFromIDsKeepsIDsTrimmedForNewToNormalize(t *testing.T) {
	cfg := FromIDs([]string{" nca-acme-nca ", "beta"})
	if len(cfg.Rules) != 1 || !slices.Equal(cfg.Rules[0].Values, []string{"nca-acme-nca", "beta"}) {
		t.Fatalf("rules = %+v, want one rule with the trimmed ids", cfg.Rules)
	}
}

func TestFromIDsIgnoresIDsThatNormalizeToNothing(t *testing.T) {
	if cfg := FromIDs([]string{"", " ", "nca--nca"}); len(cfg.Rules) != 0 {
		t.Fatalf("rules = %+v, want none", cfg.Rules)
	}
}
