// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/suppress"
)

const (
	payloadRecordType = "request_payload"
	// headerlessReason labels records_dropped for records the headerless
	// suppression policy removed.
	headerlessReason = "suppression_headerless"
)

var actionLabels = map[suppress.Action]string{
	suppress.StripPayload: "strip_payload",
	suppress.DropRecord:   "drop_record",
}

// suppressionRules maps each named rule to its action label. Counters carry
// only the operator-assigned rule name, never a header name or value.
type suppressionRules struct {
	order  []string
	action map[string]string
}

func newSuppressionRules(cfg suppress.Config) (suppressionRules, error) {
	rules := suppressionRules{action: map[string]string{}}
	for _, rule := range cfg.Rules {
		if rule.Name == "" {
			continue
		}
		if err := telemetry.ValidLabelValue(rule.Name); err != nil {
			return suppressionRules{}, fmt.Errorf("suppression rule name: %w", err)
		}
		label, ok := actionLabels[rule.Action]
		if !ok {
			return suppressionRules{}, fmt.Errorf("suppression rule %q: unknown action %q", rule.Name, rule.Action)
		}
		rules.order = append(rules.order, rule.Name)
		rules.action[rule.Name] = label
	}
	return rules, nil
}

func (r suppressionRules) names() []string { return append([]string(nil), r.order...) }

// preset records a zero for every rule's series, so a rule shows on the
// first export before it has seen a record.
func (r suppressionRules) preset(m telemetry.Metrics) {
	for _, name := range r.order {
		m.Add(telemetry.RecordsSuppressed, 0, telemetry.Labels{"rule": name, "action": r.action[name]})
		m.Add(telemetry.SuppressionHeaderPresent, 0, telemetry.Labels{"rule": name})
	}
}

func (u *Uploader) recordPrepared(result transform.Result) {
	payload := telemetry.Labels{"record_type": payloadRecordType}
	u.metrics.Add(telemetry.RecordsEvaluated, float64(result.Evaluated), payload)
	u.metrics.Add(telemetry.RecordsIncomplete, float64(result.Incomplete), payload)
	for name, count := range result.Rules {
		action, ok := u.rules.action[name]
		if !ok {
			continue
		}
		u.metrics.Add(telemetry.SuppressionHeaderPresent, float64(count.HeaderPresent), telemetry.Labels{"rule": name})
		u.metrics.Add(telemetry.RecordsSuppressed, float64(count.Matched), telemetry.Labels{"rule": name, "action": action})
	}
	for recordType, count := range result.HeaderlessDropped {
		u.metrics.Add(telemetry.RecordsDropped, float64(count), telemetry.Labels{"record_type": recordType, "reason": headerlessReason})
	}
	u.watch.observe(result)
}

// headerWatch warns once when suppression is on and, after a threshold of
// payload records, some rule's header has not been on any of them. That is
// the sign of a producer that does not capture the header, so that rule can
// suppress nothing. Each rule is judged on its own header. It only warns:
// records are never held or dropped for it.
type headerWatch struct {
	mu        sync.Mutex
	threshold int
	evaluated int
	// unseen holds the rules whose header no record has carried yet.
	unseen map[string]bool
}

func newHeaderWatch(active bool, threshold int, rules []string) *headerWatch {
	w := &headerWatch{threshold: threshold, unseen: map[string]bool{}}
	if active {
		for _, rule := range rules {
			w.unseen[rule] = true
		}
	}
	return w
}

func (w *headerWatch) observe(result transform.Result) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.unseen) == 0 {
		return
	}
	for name, count := range result.Rules {
		if count.HeaderPresent > 0 {
			delete(w.unseen, name)
		}
	}
	w.evaluated += result.Evaluated
	if w.evaluated < w.threshold || len(w.unseen) == 0 {
		return
	}
	rules := slices.Sorted(maps.Keys(w.unseen))
	w.unseen = nil
	slog.Warn("suppression is on but no payload record carried a suppression header of these rules; check that the producer captures the rule headers",
		"payload_records", w.evaluated,
		"rules", rules)
}
