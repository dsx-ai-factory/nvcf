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

	"nvcf-bdd/harness"
	"nvcf-bdd/steps"
)

// TestAPIKeysUpgradeBefore creates a key with a legacy service ID on the
// installed release. Run it before upgrading API Keys.
func TestAPIKeysUpgradeBefore(t *testing.T) {
	runLiveAPIKeysUpgrade(t, "@api-keys-before-upgrade")
}

// TestAPIKeysUpgradeAfter uses the key from TestAPIKeysUpgradeBefore after
// API Keys is upgraded and the account-index copy has completed.
func TestAPIKeysUpgradeAfter(t *testing.T) {
	runLiveAPIKeysUpgrade(t, "@api-keys-after-upgrade")
}

// TestAPIKeysUpgradeNewKey creates and uses a new key after the upgrade and the
// account-index copy, and checks that the account index holds canonical IDs.
func TestAPIKeysUpgradeNewKey(t *testing.T) {
	runLiveAPIKeysUpgrade(t, "@api-keys-new-key-after-upgrade")
}

func runLiveAPIKeysUpgrade(t *testing.T, tag string) {
	t.Helper()
	if testing.Short() {
		t.Skip("live run skipped under -short")
	}
	if os.Getenv("BDD_CLEANUP_MODE") != "" {
		t.Fatal("existing-install validation requires BDD_CLEANUP_MODE to be unset")
	}
	runLiveFeatureTags(t, "api-keys-upgrade.feature", tag)
}

func TestAPIKeysUpgradeFeatureFileWiresToSteps(t *testing.T) {
	for key, value := range map[string]string{
		"NVCF_CLI":           "/usr/bin/nvcf-cli",
		"UPGRADE_CLI_CONFIG": "/site/cli.yaml",
		"UPGRADE_KUBECONFIG": "/site/kubeconfig",
		"UPGRADE_CONTEXT":    "upgrade-context",
		"UPGRADE_KEY_FILE":   filepath.Join(t.TempDir(), "legacy-key.json"),
	} {
		t.Setenv(key, value)
	}
	runner := newFakeRunner(nil)
	runner.respond = func(command string) (harness.Result, bool) {
		switch {
		case strings.Contains(command, "api-keys-marker"):
			return harness.Result{Stdout: " record_key\n------------\n   completed\n\n(1 rows)\n"}, true
		case strings.Contains(command, "api-keys-index"):
			return harness.Result{Stdout: " issuer_service_id\n-------------------\n    cloud-functions\n\n(2 rows)\n"}, true
		case strings.Contains(command, "api-key list"):
			return harness.Result{Stdout: `{"keys":[{"description":"bdd-m0-legacy-key"},{"description":"bdd-m0-new-key"}]}`}, true
		case strings.Contains(command, "cluster list"):
			return harness.Result{Stdout: "Found 1 cluster(s)\n"}, true
		}
		return harness.Result{}, false
	}
	suite := newWiringSuite(t, runner)
	sc := steps.NewScenarioContext(suite)
	var output strings.Builder
	status := godog.TestSuite{
		Name:                "api-keys-upgrade-wiring",
		ScenarioInitializer: func(ctx *godog.ScenarioContext) { steps.RegisterAll(ctx, sc) },
		Options: &godog.Options{
			Format: "pretty",
			Paths:  []string{mustResolveFeaturePath(t, "api-keys-upgrade.feature")},
			Strict: true,
			Output: &output,
		},
	}.Run()
	if status != 0 {
		t.Fatalf("godog suite status = %d\n%s", status, output.String())
	}
	if !commandRanThatContains(runner.runs, "api-key generate --for function --description bdd-m0-legacy-key") {
		t.Fatal("legacy-ID function key was not generated")
	}
	if !commandRanThatContains(runner.runs, "api-key generate --for function --description bdd-m0-new-key") {
		t.Fatal("new function key was not generated")
	}
	if !commandRanThatContains(runner.runs, "keys_by_account_index_copy") {
		t.Fatal("account-index completion marker was not checked")
	}
}
