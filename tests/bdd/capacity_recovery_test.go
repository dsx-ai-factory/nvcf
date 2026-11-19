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

package bdd_tmp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cucumber/godog"

	"nvcf-bdd/dsl"
	"nvcf-bdd/harness"
	"nvcf-bdd/steps"
)

func TestCapacityRecoveryScenarioUsesCandidateAndSaturatedProbe(t *testing.T) {
	t.Setenv("CAPACITY_NVCA_AGENT_VERSION", "3.0.0-review")
	t.Setenv("REPO_ROOT", "/repo-root-placeholder")
	t.Setenv("NVCF_CLI", "/usr/bin/nvcf-cli")
	t.Setenv("SAMPLE_NGC_ORG", "test-org")
	t.Setenv("SAMPLE_NGC_TEAM", "test-team")
	t.Setenv("HOME", t.TempDir())
	runner := newFakeRunner(nil)
	runner.respond = func(command string) (harness.Result, bool) {
		if strings.Contains(command, "scripts/check_capacity_recovery.py") {
			return harness.Result{Stdout: "capacity recovery passed"}, true
		}
		return harness.Result{}, false
	}
	suite := newWiringSuite(t, runner)
	seedComputePlaneLocalBDDFixture(t, suite.Config.RepoRoot)
	writeHelmfileRegisterValues(t, suite.Config.RepoRoot)
	environment := filepath.Join(suite.Config.RepoRoot, "deploy/stacks/nvcf-compute-plane/environments/local-bdd.yaml")
	if err := os.MkdirAll(filepath.Dir(environment), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(environment, []byte("global: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(mustResolveFeaturePath(t, "single-cluster-helmfile.feature"))
	if err != nil {
		t.Fatal(err)
	}
	_, scenario, found := strings.Cut(string(data), "    @capacity-recovery\n")
	scenario, _, ended := strings.Cut(scenario, "    @function-lifecycle @helm-function\n")
	if !found || !ended {
		t.Fatal("missing saturated capacity scenario")
	}
	sc := steps.NewScenarioContext(suite)
	var output strings.Builder
	status := godog.TestSuite{
		Name:                "capacity-recovery-wiring",
		ScenarioInitializer: func(ctx *godog.ScenarioContext) { steps.RegisterAll(ctx, sc) },
		Options: &godog.Options{Format: "pretty", Output: &output, Strict: true,
			FeatureContents: []godog.Feature{{Name: "capacity", Contents: []byte("Feature: Capacity recovery\n" + scenario)}}},
	}.Run()
	if status != 0 {
		t.Fatalf("capacity scenario failed: %s", output.String())
	}
	version, found, err := dsl.ReadYAMLKey(environment, "global.nvcaOperator.selfManaged.nvcaVersion")
	if err != nil || !found || version != "3.0.0-review" {
		t.Fatalf("candidate version override = %v, found=%v, err=%v", version, found, err)
	}
	registration := filepath.Join(suite.Config.RepoRoot,
		"deploy/stacks/nvcf-compute-plane/registration/ncp-local-register-values.yaml")
	capacity, found, err := dsl.ReadYAMLKey(registration, "worker.staticGPUCapacity")
	if err != nil || !found || capacity != "4" {
		t.Fatalf("static capacity in operator values = %v, found=%v, err=%v", capacity, found, err)
	}
	if !commandRanThatContainsAll(runner.runs, "check_capacity_recovery.py", "--agent-version 3.0.0-review",
		"--expected-capacity 4", "--publication-timeout 60", "--recovery-timeout 180", "--context k3d-ncp-local") {
		t.Fatal("capacity probe did not receive the candidate, saturation and timing inputs")
	}
}
