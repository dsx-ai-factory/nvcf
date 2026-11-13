// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/suppress"
)

func requiredEnv(extra map[string]string) map[string]string {
	env := map[string]string{EnvSourceDir: "/records", EnvBackend: string(BackendDebug)}
	for name, value := range extra {
		env[name] = value
	}
	return env
}

func ncaRule(values ...string) suppress.Rule {
	return suppress.Rule{Name: NCAIDRuleName, Headers: suppress.DefaultHeaders, Values: values, Action: suppress.StripPayload, NCAID: true}
}

func TestLoadSuppressNCAIDs(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		rules []suppress.Rule
	}{
		{name: "unset is off"},
		{name: "empty is off", env: map[string]string{EnvSuppressNCAIDs: ""}},
		{name: "only separators is off", env: map[string]string{EnvSuppressNCAIDs: " , ,,"}},
		{name: "one id", env: map[string]string{EnvSuppressNCAIDs: "acme"}, rules: []suppress.Rule{ncaRule("acme")}},
		{
			name:  "split and trimmed, and normalized like the shell exporter when compiled",
			env:   map[string]string{EnvSuppressNCAIDs: " nca-acme-nca , beta,,nca-gamma-nca"},
			rules: []suppress.Rule{ncaRule("acme", "beta", "gamma")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, warnings, err := Load(mapLookup(requiredEnv(tt.env)))
			if err != nil || len(warnings) != 0 {
				t.Fatalf("Load() error = %v warnings = %v", err, warnings)
			}
			if got := compiledValues(cfg.Suppression.Rules); !reflect.DeepEqual(got, tt.rules) {
				t.Fatalf("Rules = %+v, want %+v", got, tt.rules)
			}
		})
	}
}

// compiledValues returns rules with each value as the engine compiles it:
// normalized once, blank values left out.
func compiledValues(rules []suppress.Rule) []suppress.Rule {
	var out []suppress.Rule
	for _, rule := range rules {
		var values []string
		for _, value := range rule.Values {
			if normalized := suppress.NormalizeID(value); normalized != "" {
				values = append(values, normalized)
			}
		}
		rule.Values = values
		out = append(out, rule)
	}
	return out
}

func TestLoadParsesExplicitSuppressionRules(t *testing.T) {
	cfg, _, err := Load(mapLookup(requiredEnv(map[string]string{
		EnvSuppressNCAIDs:   "acme",
		EnvSuppressionRules: `[{"name":"tenants","headers":[" X-Tenant "],"values":["t1","t2"],"action":"dropRecord"}]`,
	})))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []suppress.Rule{
		ncaRule("acme"),
		{Name: "tenants", Headers: []string{"x-tenant"}, Values: []string{"t1", "t2"}, Action: suppress.DropRecord},
	}
	if !reflect.DeepEqual(cfg.Suppression.Rules, want) {
		t.Fatalf("Rules = %+v, want %+v", cfg.Suppression.Rules, want)
	}
}

func TestLoadHeaderlessPolicy(t *testing.T) {
	cfg, _, err := Load(mapLookup(requiredEnv(nil)))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Suppression.Headerless != suppress.HeaderlessExport {
		t.Fatalf("default Headerless = %q, want %q", cfg.Suppression.Headerless, suppress.HeaderlessExport)
	}
	cfg, _, err = Load(mapLookup(requiredEnv(map[string]string{EnvSuppressHeaderless: "drop"})))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Suppression.Headerless != suppress.HeaderlessDrop {
		t.Fatalf("Headerless = %q, want %q", cfg.Suppression.Headerless, suppress.HeaderlessDrop)
	}
}

func TestLoadRejectsInvalidSuppression(t *testing.T) {
	rule := func(body string) map[string]string {
		return map[string]string{EnvSuppressionRules: "[" + body + "]"}
	}
	tests := []struct {
		name string
		env  map[string]string
	}{
		{name: "unparseable", env: map[string]string{EnvSuppressionRules: `[{"name":`}},
		{name: "not a list", env: map[string]string{EnvSuppressionRules: `{"name":"a"}`}},
		{name: "trailing data", env: map[string]string{EnvSuppressionRules: `[] []`}},
		{name: "unknown field", env: rule(`{"name":"a","header":"x-tenant","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}`)},
		{name: "unknown action", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"],"action":"bashStub"}`)},
		{name: "missing action", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"]}`)},
		{name: "empty value list", env: rule(`{"name":"a","headers":["x-tenant"],"values":[],"action":"dropRecord"}`)},
		{name: "only blank values", env: rule(`{"name":"a","headers":["x-tenant"],"values":[" ",""],"action":"dropRecord"}`)},
		{name: "no headers", env: rule(`{"name":"a","headers":[],"values":["t1"],"action":"dropRecord"}`)},
		{name: "blank header", env: rule(`{"name":"a","headers":[" "],"values":["t1"],"action":"dropRecord"}`)},
		{name: "missing name", env: rule(`{"headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}`)},
		{name: "name is not a short lowercase token", env: rule(`{"name":"Tenant Rule","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}`)},
		{name: "duplicate names", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"},{"name":"a","headers":["x-org"],"values":["o1"],"action":"dropRecord"}`)},
		{name: "name taken by the nca id rule", env: map[string]string{
			EnvSuppressNCAIDs:   "acme",
			EnvSuppressionRules: `[{"name":"nca_id","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}]`,
		}},
		{name: "unknown headerless policy", env: map[string]string{EnvSuppressHeaderless: "reject"}},
		{name: "quoted nca id", env: map[string]string{EnvSuppressNCAIDs: `"acme"`}},
		{name: "nca ids joined by a semicolon", env: map[string]string{EnvSuppressNCAIDs: "acme;o1"}},
		{name: "nca ids joined by a space", env: map[string]string{EnvSuppressNCAIDs: "acme o1"}},
		{name: "nca ids joined by a newline", env: map[string]string{EnvSuppressNCAIDs: "acme\no1"}},
		{name: "nca ids as a JSON list", env: map[string]string{EnvSuppressNCAIDs: `["acme"]`}},
		{name: "rule value that is not an id", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1 o1"],"action":"dropRecord"}`)},
		{name: "null instead of a list", env: map[string]string{EnvSuppressionRules: ` null `}},
		{name: "repeated field", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"],"values":["o1"],"action":"dropRecord"}`)},
		{name: "field repeated in another case", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"],"Values":["o1"],"action":"dropRecord"}`)},
		{name: "field repeated with a folded letter", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"],"value\u017f":["o1"],"action":"dropRecord"}`)},
		{name: "repeated action", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"],"action":"stripPayload","action":"dropRecord"}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Load(mapLookup(requiredEnv(tt.env)))
			if err == nil {
				t.Fatal("Load() error = nil, want a startup error")
			}
			// A configured value can be a customer identifier, so it must
			// not be echoed into the error.
			for _, value := range []string{"t1", "o1", "acme"} {
				if strings.Contains(err.Error(), value) {
					t.Fatalf("Load() error = %q, echoes a configured value", err)
				}
			}
		})
	}
}

// An id that is empty once its nca- and -nca affixes are removed, such as
// nca--nca, can never match; it stops startup instead of being skipped, which
// could turn suppression off without a word. The error names the setting and
// the entry, never the value.
func TestLoadRejectsAnIDThatReducesToNothing(t *testing.T) {
	tests := map[string]map[string]string{
		"only such an id":  {EnvSuppressNCAIDs: "nca--nca"},
		"one among others": {EnvSuppressNCAIDs: "acme,nca-nca"},
		"a rule value":     {EnvSuppressionRules: `[{"name":"a","headers":["x-tenant"],"values":["nca--nca","t1"],"action":"dropRecord"}]`},
	}
	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := Load(mapLookup(requiredEnv(env)))
			if err == nil {
				t.Fatal("Load() accepted an id that is empty once reduced")
			}
			if strings.Contains(err.Error(), "nca-nca") || strings.Contains(err.Error(), "nca--nca") {
				t.Fatalf("Load() error = %q, echoes the configured id", err)
			}
		})
	}
}
