// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

const (
	alertRulesFile     = "../../deploy/helm/nvsnap/files/prometheus-rules.yaml"
	alertRulesTestFile = "../../deploy/helm/nvsnap/files/prometheus-rules.test.yaml"
	runbookFile        = "../../docs/operate/RUNBOOK.md"
)

// TestAlertRulesAreTestedAndDocumented: every alert has promtool cases
// (files/test-rules.sh runs them; they need promtool or docker, which this
// test does not), a runbook section its runbook_url names, and every
// runbook alert section is an alert. The rule file is what the chart
// renders, so a renamed or added alert without its runbook or tests fails
// here, in the build.
func TestAlertRulesAreTestedAndDocumented(t *testing.T) {
	raw, err := os.ReadFile(alertRulesFile)
	if err != nil {
		t.Fatal(err)
	}
	var rules struct {
		Groups []struct {
			Rules []struct {
				Alert       string            `json:"alert"`
				Expr        string            `json:"expr"`
				Labels      map[string]string `json:"labels"`
				Annotations map[string]string `json:"annotations"`
			} `json:"rules"`
		} `json:"groups"`
	}
	if err := yaml.Unmarshal(raw, &rules); err != nil {
		t.Fatalf("parse %s: %v", alertRulesFile, err)
	}
	tests, err := os.ReadFile(alertRulesTestFile)
	if err != nil {
		t.Fatal(err)
	}
	runbook, err := os.ReadFile(runbookFile)
	if err != nil {
		t.Fatal(err)
	}
	sections := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^## (Nvsnap\w+)$`).FindAllStringSubmatch(string(runbook), -1) {
		sections[m[1]] = true
	}
	alerts := map[string]bool{}
	for _, g := range rules.Groups {
		for _, r := range g.Rules {
			if r.Alert == "" {
				continue
			}
			alerts[r.Alert] = true
			if r.Labels["severity"] == "" || r.Labels["service"] != "nvsnap" {
				t.Errorf("%s: labels %v, want a severity and service nvsnap", r.Alert, r.Labels)
			}
			if !strings.Contains(string(tests), "alertname: "+r.Alert+"\n") {
				t.Errorf("%s has no promtool case in %s", r.Alert, alertRulesTestFile)
			}
			if !sections[r.Alert] {
				t.Errorf("%s has no section in %s", r.Alert, runbookFile)
			}
			if want := "/docs/operate/RUNBOOK.md#" + strings.ToLower(r.Alert); !strings.HasSuffix(r.Annotations["runbook_url"], want) {
				t.Errorf("%s: runbook_url %q, want it to end %q", r.Alert, r.Annotations["runbook_url"], want)
			}
			if r.Annotations["summary"] == "" || r.Annotations["description"] == "" {
				t.Errorf("%s has no summary or description", r.Alert)
			}
		}
	}
	if len(alerts) == 0 {
		t.Fatalf("%s has no alerts", alertRulesFile)
	}
	for s := range sections {
		if !alerts[s] {
			t.Errorf("%s has a section for %s, which is no alert", runbookFile, s)
		}
	}
}

// TestAgentRBACCoversTheInventoryLease: the inventory's Lease calls go
// through client-go's resourcelock, which TestAgentRBACCoversItsClientCalls
// cannot see.
func TestAgentRBACCoversTheInventoryLease(t *testing.T) {
	allowed := agentRules(t)[[2]string{"coordination.k8s.io", "leases"}]
	for _, v := range []string{"get", "create", "update"} {
		if !allowed[v] {
			t.Errorf("the agent's ClusterRole does not allow %s on leases; the capture inventory cannot lead", v)
		}
	}
}
