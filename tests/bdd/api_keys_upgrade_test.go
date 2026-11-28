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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cucumber/godog"

	"nvcf-bdd/harness"
	"nvcf-bdd/steps"
)

// TestAPIKeysUpgrade starts from an installation that runs the previous
// release, runs UPGRADE_APPLY_COMMAND, and checks that a key created with a
// legacy service ID before the upgrade works after it and that a new key
// carries the account ID. It changes the installation.
func TestAPIKeysUpgrade(t *testing.T) {
	if testing.Short() {
		t.Skip("live run skipped under -short")
	}
	if os.Getenv("BDD_CLEANUP_MODE") != "" {
		t.Fatal("the API Keys upgrade test requires BDD_CLEANUP_MODE to be unset")
	}
	defaultUpgradeApplyCommand(t)
	runLiveFeature(t, "api-keys-upgrade.feature")
}

// defaultUpgradeApplyCommand applies the checked-out control-plane stack when
// UPGRADE_APPLY_COMMAND is not set. make install takes a kubeconfig path but no
// context, so the feature first asserts that the kubeconfig's current context is
// UPGRADE_CONTEXT. It upgrades only if the checkout pins the versions under
// test; the feature fails fast when the API Keys image does not change.
func defaultUpgradeApplyCommand(t *testing.T) {
	t.Helper()
	if os.Getenv("UPGRADE_APPLY_COMMAND") != "" {
		return
	}
	env := os.Getenv("UPGRADE_ENV")
	kubeconfig := os.Getenv("UPGRADE_KUBECONFIG")
	if env == "" || kubeconfig == "" {
		t.Fatal("set UPGRADE_APPLY_COMMAND, or UPGRADE_ENV and UPGRADE_KUBECONFIG to upgrade with make install from this checkout")
	}
	root, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	t.Setenv("UPGRADE_APPLY_COMMAND", "make -C "+strings.TrimSpace(string(root))+
		"/deploy/stacks/self-managed install HELMFILE_ENV="+env+" KUBECONFIG_FILE="+kubeconfig)
}

// apiKeysUpgradeWiring is the outcome of running the feature against fakes.
type apiKeysUpgradeWiring struct {
	status int
	output string
	runner *fakeRunner
	http   *recordingHTTP
}

// runAPIKeysUpgradeWiring runs the feature against a fake runner that models an
// installation whose completion record appears only after the fake upgrade
// command runs. imageChanges says whether that command changes the API Keys
// image.
func runAPIKeysUpgradeWiring(t *testing.T, imageChanges bool) apiKeysUpgradeWiring {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.WriteFile(filepath.Join(home, ".nvcf-cli.nvcf-cli-local.state"),
		[]byte(`{"token":"admin-jwt","apiKey":"nvapi-wiring"}`), 0o600); err != nil {
		t.Fatalf("seed CLI state: %v", err)
	}
	for key, value := range map[string]string{
		"REPO_ROOT":             "/repo",
		"NVCF_CLI":              "/usr/bin/nvcf-cli",
		"UPGRADE_CLI_CONFIG":    "/site/nvcf-cli-local.yaml",
		"UPGRADE_KUBECONFIG":    "/site/kubeconfig",
		"UPGRADE_CONTEXT":       "upgrade-context",
		"UPGRADE_NCA_ID":        "account-a",
		"UPGRADE_APPLY_COMMAND": "/bin/true upgrade-applied",
	} {
		t.Setenv(key, value)
	}
	upgraded := false
	runner := newFakeRunner(nil)
	runner.respond = func(command string) (harness.Result, bool) {
		switch {
		case strings.Contains(command, "upgrade-applied"):
			upgraded = true
			return harness.Result{}, true
		case strings.Contains(command, "containers[0].image"):
			if upgraded && imageChanges {
				return harness.Result{Stdout: "registry/nvcf-api-keys-service:new"}, true
			}
			return harness.Result{Stdout: "registry/nvcf-api-keys-service:old"}, true
		case strings.Contains(command, "api-keys-image-changed"):
			fields := strings.Fields(command)
			before, after := fields[len(fields)-2], fields[len(fields)-1]
			if before == after {
				return harness.Result{ExitCode: 1}, true
			}
			return harness.Result{}, true
		case strings.Contains(command, "keys_by_account_index_copy"):
			if upgraded {
				return harness.Result{Stdout: " record_key\n------------\n   completed\n\n(1 rows)\n"}, true
			}
			return harness.Result{Stdout: " record_key\n------------\n\n(0 rows)\n"}, true
		case strings.Contains(command, "api-keys-key-id"):
			return harness.Result{Stdout: "key-1\n"}, true
		case strings.Contains(command, "SELECT issuer_service_id"):
			return harness.Result{Stdout: " issuer_service_id\n-------------------\n   cloud-functions\n\n(1 rows)\n"}, true
		case strings.Contains(command, "SELECT nca_id FROM api_keys_api.keys;"):
			return harness.Result{Stdout: " nca_id\n----------\n account-a\n account-a\n\n(2 rows)\n"}, true
		case strings.Contains(command, "api-keys-cql.sh"):
			return harness.Result{Stdout: " key_id\n--------\n key-1\n\n(1 rows)\n"}, true
		}
		return harness.Result{}, false
	}
	suite := newWiringSuite(t, runner)
	httpFake := &recordingHTTP{status: 200}
	suite.HTTP = httpFake
	sc := steps.NewScenarioContext(suite)
	var output strings.Builder
	status := godog.TestSuite{
		Name:                "api-keys-upgrade-wiring",
		ScenarioInitializer: func(ctx *godog.ScenarioContext) { steps.RegisterAll(ctx, sc) },
		Options: &godog.Options{
			Format:        "pretty",
			Paths:         []string{mustResolveFeaturePath(t, "api-keys-upgrade.feature")},
			Strict:        true,
			StopOnFailure: true,
			Output:        &output,
		},
	}.Run()
	return apiKeysUpgradeWiring{status: status, output: output.String(), runner: runner, http: httpFake}
}

func TestAPIKeysUpgradeFeatureFileWiresToSteps(t *testing.T) {
	run := runAPIKeysUpgradeWiring(t, true)
	if run.status != 0 {
		t.Fatalf("godog suite status = %d\n%s", run.status, run.output)
	}
	runs := run.runner.runs
	legacy := indexOfCommand(runs, "--description bdd-m0-legacy-key")
	upgrade := indexOfCommand(runs, "upgrade-applied")
	rollout := indexOfCommand(runs, "rollout status")
	if legacy < 0 || upgrade < 0 || rollout < 0 || legacy >= upgrade || upgrade >= rollout {
		t.Fatalf("legacy key, upgrade and rollout wait did not run in that order: %d %d %d", legacy, upgrade, rollout)
	}
	if indexOfCommand(runs, "--description bdd-m0-new-key") < upgrade {
		t.Fatal("the new key was not created after the upgrade")
	}
	if len(run.http.requests) != 3 {
		t.Fatalf("saved-key requests = %d, want 3: %v", len(run.http.requests), run.http.requests)
	}
	for i, headers := range run.http.headers {
		if headers["Authorization"] != "Bearer nvapi-wiring" {
			t.Fatalf("request %d did not use the saved function key", i)
		}
	}
}

// An upgrade command that leaves the API Keys image unchanged must fail the
// scenario before the completion-record wait and before the second key request.
func TestAPIKeysUpgradeFailsWhenTheImageDoesNotChange(t *testing.T) {
	run := runAPIKeysUpgradeWiring(t, false)
	if run.status == 0 {
		t.Fatal("scenario passed although the upgrade did not change the API Keys image")
	}
	completionChecks := 0
	for _, command := range run.runner.runs {
		if strings.Contains(command, "keys_by_account_index_copy") {
			completionChecks++
		}
	}
	if completionChecks != 1 {
		t.Fatalf("completion record checks = %d, want only the precondition", completionChecks)
	}
	if len(run.http.requests) != 1 {
		t.Fatalf("saved-key requests = %d, want only the one before the upgrade", len(run.http.requests))
	}
}

func indexOfCommand(runs []string, needle string) int {
	for i, run := range runs {
		if strings.Contains(run, needle) {
			return i
		}
	}
	return -1
}
