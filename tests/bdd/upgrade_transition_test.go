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
	"testing"

	"github.com/cucumber/godog"
	"nvcf-bdd/harness"
	"nvcf-bdd/steps"
)

// This invokes the real offline audit binary. It does not deploy a cluster.
func TestUpgradeTransitionAudit(t *testing.T) {
	if testing.Short() || os.Getenv("UPGRADE_LAB") == "" {
		t.Skip("set UPGRADE_LAB and TRANSITION_* to run the real artifact audit")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	suite := &harness.Suite{
		Config:    harness.Config{RepoRoot: root, OutDir: out},
		Runner:    harness.NewCommandRunner(root, filepath.Join(out, "logs")),
		Ledger:    harness.NewLedger(filepath.Join(out, "originals")),
		EnvLedger: harness.NewEnvLedger(),
		Cache:     harness.NewCommandCache(),
	}
	t.Cleanup(func() {
		if err := suite.Teardown(); err != nil {
			t.Error(err)
		}
	})
	sc := steps.NewScenarioContext(suite)
	status := godog.TestSuite{
		Name:                "upgrade-transition-audit",
		ScenarioInitializer: func(ctx *godog.ScenarioContext) { steps.RegisterAll(ctx, sc) },
		Options:             &godog.Options{Format: "pretty", Paths: []string{mustResolveFeaturePath(t, "upgrade-transition-audit.feature")}, Strict: true, StopOnFailure: true},
	}.Run()
	if status != 0 {
		t.Fatalf("artifact audit suite status = %d", status)
	}
}
