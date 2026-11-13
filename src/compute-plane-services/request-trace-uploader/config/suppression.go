// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/suppress"
)

const (
	// EnvSuppressNCAIDs lists suppressed NCA ids, separated by commas. It
	// builds one stripPayload rule, named NCAIDRuleName, on the NCA id
	// headers. Unset or blank turns it off.
	EnvSuppressNCAIDs = "REQUEST_TRACE_UPLOADER_SUPPRESS_NCA_IDS"
	// EnvSuppressionRules holds further named rules as a JSON list of
	// {"name", "headers", "values", "action"} objects. They are evaluated
	// after the NCA id rule, in order.
	EnvSuppressionRules = "REQUEST_TRACE_UPLOADER_SUPPRESSION_RULES"
	// EnvSuppressHeaderless is the policy for records that carry no headers
	// while suppression is on: "export" (the default) or "drop".
	EnvSuppressHeaderless = "REQUEST_TRACE_UPLOADER_SUPPRESS_HEADERLESS"
	// NCAIDRuleName names the rule EnvSuppressNCAIDs builds.
	NCAIDRuleName = "nca_id"
)

// ruleNamePattern is the telemetry label value shape, so a rule name can
// label a counter.
var ruleNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)

// idPattern is the shape of a suppressed value: printable ASCII with no
// white space, quotes, brackets, commas or semicolons. A value outside it,
// such as two ids joined by a space or a quoted id, can never match a
// header, so it would turn suppression off without a sign.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9._~@:+/=!#$%&*^|-]{1,256}$`)

// checkValues reports the position of the first non-blank value that does
// not look like an id, or that is empty once its nca- and -nca affixes are
// removed and so could never match, without the value.
func checkValues(values []string) error {
	for i, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" && (!idPattern.MatchString(trimmed) || suppress.NormalizeID(trimmed) == "") {
			return fmt.Errorf("entry %d is not a single id", i)
		}
	}
	return nil
}

type ruleSpec struct {
	Name    string   `json:"name"`
	Headers []string `json:"headers"`
	Values  []string `json:"values"`
	Action  string   `json:"action"`
}

// suppressionValue builds the suppression rules. Every problem is an error,
// never a fallback: a rule that silently turned off would export payloads
// an operator asked to suppress. Errors never echo a configured value,
// because values can be customer identifiers.
func suppressionValue(lookup LookupFunc) (suppress.Config, error) {
	var cfg suppress.Config
	idList := strings.Split(valueOrDefault(lookup, EnvSuppressNCAIDs, ""), ",")
	if err := checkValues(idList); err != nil {
		return suppress.Config{}, fmt.Errorf("%s %w", EnvSuppressNCAIDs, err)
	}
	ids := suppress.FromIDs(idList)
	for _, rule := range ids.Rules {
		rule.Name = NCAIDRuleName
		cfg.Rules = append(cfg.Rules, rule)
	}

	explicit, err := explicitRules(valueOrDefault(lookup, EnvSuppressionRules, ""))
	if err != nil {
		return suppress.Config{}, err
	}
	cfg.Rules = append(cfg.Rules, explicit...)

	switch policy := suppress.HeaderlessPolicy(strings.TrimSpace(valueOrDefault(lookup, EnvSuppressHeaderless, string(suppress.HeaderlessExport)))); policy {
	case suppress.HeaderlessExport, suppress.HeaderlessDrop:
		cfg.Headerless = policy
	default:
		return suppress.Config{}, fmt.Errorf("%s must be %q or %q", EnvSuppressHeaderless, suppress.HeaderlessExport, suppress.HeaderlessDrop)
	}

	if err := cfg.Validate(); err != nil {
		return suppress.Config{}, fmt.Errorf("%s or %s: %w", EnvSuppressNCAIDs, EnvSuppressionRules, err)
	}
	return cfg, nil
}

// ruleFields holds the JSON names of ruleSpec's fields.
var ruleFields = func() []string {
	spec := reflect.TypeFor[ruleSpec]()
	names := make([]string, spec.NumField())
	for i := range names {
		names[i], _, _ = strings.Cut(spec.Field(i).Tag.Get("json"), ",")
	}
	return names
}()

func explicitRules(raw string) ([]suppress.Rule, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	// JSON null would decode to no rules and turn the setting off.
	if !strings.HasPrefix(trimmed, "[") {
		return nil, fmt.Errorf("%s is not a JSON list of rules", EnvSuppressionRules)
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	var items []json.RawMessage
	if err := decoder.Decode(&items); err != nil {
		return nil, fmt.Errorf("%s is not a JSON list of rules: %w", EnvSuppressionRules, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s has data after the rule list", EnvSuppressionRules)
	}
	specs := make([]ruleSpec, len(items))
	for i, item := range items {
		if err := checkRepeatedFields(item); err != nil {
			return nil, fmt.Errorf("%s rule %d: %w", EnvSuppressionRules, i, err)
		}
		decoder := json.NewDecoder(bytes.NewReader(item))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&specs[i]); err != nil {
			return nil, fmt.Errorf("%s is not a JSON list of rules: %w", EnvSuppressionRules, err)
		}
	}
	rules := make([]suppress.Rule, 0, len(specs))
	for i, spec := range specs {
		if !ruleNamePattern.MatchString(spec.Name) {
			return nil, fmt.Errorf("%s rule %d: name must match %s", EnvSuppressionRules, i, ruleNamePattern)
		}
		action := suppress.Action(spec.Action)
		if action != suppress.StripPayload && action != suppress.DropRecord {
			return nil, fmt.Errorf("%s rule %q: action must be %q or %q", EnvSuppressionRules, spec.Name, suppress.StripPayload, suppress.DropRecord)
		}
		if err := checkValues(spec.Values); err != nil {
			return nil, fmt.Errorf("%s rule %q value %w", EnvSuppressionRules, spec.Name, err)
		}
		headers := make([]string, len(spec.Headers))
		for j, header := range spec.Headers {
			// Header lookup ignores case, so the configured case does not
			// matter.
			headers[j] = strings.ToLower(strings.TrimSpace(header))
		}
		rules = append(rules, suppress.Rule{Name: spec.Name, Headers: headers, Values: spec.Values, Action: action})
	}
	return rules, nil
}

// checkRepeatedFields rejects a rule object that sets a field twice. The
// JSON decoder keeps the last value and matches field names ignoring case,
// so {"values":["a"],"Values":["b"]} would silently drop "a".
func checkRepeatedFields(item json.RawMessage) error {
	decoder := json.NewDecoder(bytes.NewReader(item))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		// Not an object: decoding the rule reports it.
		return nil
	}
	seen := make(map[string]bool, len(ruleFields))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, _ := token.(string)
		for _, field := range ruleFields {
			if strings.EqualFold(key, field) {
				if seen[field] {
					return fmt.Errorf("field %q is set more than once", field)
				}
				seen[field] = true
			}
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return err
		}
	}
	return nil
}
