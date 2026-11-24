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
	"strings"
	"testing"

	"github.com/cucumber/godog"

	"nvcf-bdd/harness"
	"nvcf-bdd/steps"
)

// TestExistingClusterFakeKitSessions requires an installed local stack and
// explicit red/green images. It preserves the production restart-age threshold.
func TestExistingClusterFakeKitSessions(t *testing.T) {
	if testing.Short() {
		t.Skip("live run skipped under -short")
	}
	if os.Getenv("BDD_CLEANUP_MODE") != "" {
		t.Fatal("unset BDD_CLEANUP_MODE to preserve the installed cluster for this scenario")
	}
	for _, key := range []string{"FAKE_KIT_RED_AGENT_VERSION", "CAPACITY_NVCA_AGENT_VERSION",
		"FAKE_KIT_RUNTIME_IMAGE", "FAKE_KIT_FILLER_IMAGE"} {
		if os.Getenv(key) == "" {
			t.Fatalf("set %s before running the existing-cluster fake Kit scenario", key)
		}
	}
	runLiveFeature(t, "fake-kit-sessions.feature")
}

func TestFakeKitScenarioRetainsThresholdsAndUsesExplicitImages(t *testing.T) {
	t.Setenv("REPO_ROOT", "/repo-root-placeholder")
	t.Setenv("NVCF_CLI", "/usr/bin/nvcf-cli")
	t.Setenv("FAKE_KIT_RED_AGENT_VERSION", "red-control")
	t.Setenv("CAPACITY_NVCA_AGENT_VERSION", "green-candidate")
	t.Setenv("FAKE_KIT_RUNTIME_IMAGE", "example.test/python-runtime:fixture")
	t.Setenv("FAKE_KIT_FILLER_IMAGE", "example.test/echo:fixture")
	runner := newFakeRunner(nil)
	runner.respond = func(command string) (harness.Result, bool) {
		if strings.Contains(command, "scripts/check_fake_kit_sessions.py") {
			return harness.Result{Stdout: "fake Kit sessions passed"}, true
		}
		return harness.Result{}, false
	}
	suite := newWiringSuite(t, runner)
	sc := steps.NewScenarioContext(suite)
	var output strings.Builder
	status := godog.TestSuite{
		Name:                "fake-kit-wiring",
		ScenarioInitializer: func(ctx *godog.ScenarioContext) { steps.RegisterAll(ctx, sc) },
		Options: &godog.Options{Format: "pretty", Output: &output, Strict: true,
			Paths: []string{mustResolveFeaturePath(t, "fake-kit-sessions.feature")}},
	}.Run()
	if status != 0 {
		t.Fatalf("fake Kit wiring failed: %s", output.String())
	}
	if !commandRanThatContainsAll(runner.runs, "check_fake_kit_sessions.py", "--red-agent-version red-control",
		"--green-agent-version green-candidate", "--minimum-pod-age 600", "--observation-seconds 600",
		"--session-ends 4", "--function-active-timeout 90", "--capacity 4", "--context k3d-ncp-local") {
		t.Fatal("fake Kit scenario omitted explicit versions, capacity, context or real-time thresholds")
	}
}
