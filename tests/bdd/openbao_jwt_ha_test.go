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
	"strings"
	"testing"

	"github.com/cucumber/godog"

	"nvcf-bdd/harness"
	"nvcf-bdd/steps"
)

func TestOpenBaoJWTPluginHAFeatureFileWiresToSteps(t *testing.T) {
	t.Setenv("OPENBAO_RAFT_TEST_CONTAINER", "jwt-ha-wiring")
	runner := newFakeRunner(nil)
	runner.respond = func(command string) (harness.Result, bool) {
		if strings.HasSuffix(command, "jwt-plugin-lifecycle cleanup") {
			return harness.Result{ExitCode: 1}, true
		}
		return harness.Result{ExitCode: 0}, true
	}
	sc := steps.NewScenarioContext(newWiringSuite(t, runner))
	var out strings.Builder
	status := godog.TestSuite{
		Name:                "openbao-jwt-ha-wiring",
		ScenarioInitializer: func(ctx *godog.ScenarioContext) { steps.RegisterAll(ctx, sc) },
		Options:             &godog.Options{Format: "pretty", Paths: []string{mustResolveFeaturePath(t, "openbao-jwt-plugin-ha.feature")}, Strict: true, Output: &out},
	}.Run()
	if status != 0 {
		t.Fatalf("godog status = %d\n%s", status, out.String())
	}
	for _, action := range []string{"pin-old", "upgrade", "rollback", "operator step-down", "restart target-only"} {
		if !commandRanThatContains(runner.runs, action) {
			t.Fatalf("missing operator action %s", action)
		}
	}
}

// TestOpenBaoJWTPluginHA uses a fresh three-process Raft fixture and normal failover.
func TestOpenBaoJWTPluginHA(t *testing.T) {
	if testing.Short() {
		t.Skip("live HA lifecycle requires a prepared isolated container")
	}
	t.Setenv("BDD_CLEANUP_MODE", "")
	runLiveFeature(t, "openbao-jwt-plugin-ha.feature")
}
