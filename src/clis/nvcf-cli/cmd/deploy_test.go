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

package cmd

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"nvcf-cli/internal/state"
)

func TestApplySavedDeployContextFillsMissingFunctionIDs(t *testing.T) {
	config := &DeployConfig{}
	currentState := &state.State{
		FunctionID: "func-from-state",
		VersionID:  "ver-from-state",
	}

	applySavedDeployContext(config, currentState)

	if config.FunctionID != "func-from-state" {
		t.Fatalf("FunctionID = %q, want func-from-state", config.FunctionID)
	}
	if config.VersionID != "ver-from-state" {
		t.Fatalf("VersionID = %q, want ver-from-state", config.VersionID)
	}
}

func TestApplySavedDeployContextKeepsExplicitFunctionIDs(t *testing.T) {
	config := &DeployConfig{
		FunctionID: "func-explicit",
		VersionID:  "ver-explicit",
	}
	currentState := &state.State{
		FunctionID: "func-from-state",
		VersionID:  "ver-from-state",
	}

	applySavedDeployContext(config, currentState)

	if config.FunctionID != "func-explicit" {
		t.Fatalf("FunctionID = %q, want func-explicit", config.FunctionID)
	}
	if config.VersionID != "ver-explicit" {
		t.Fatalf("VersionID = %q, want ver-explicit", config.VersionID)
	}
}

func TestSaveStateForCurrentCommandPersistsFunctionContextForConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	previousCfgFile := cfgFile
	previousManager := configStateManager
	previousManagerKey := configStateManagerKey
	t.Cleanup(func() {
		cfgFile = previousCfgFile
		configStateManager = previousManager
		configStateManagerKey = previousManagerKey
	})

	configPath := filepath.Join(t.TempDir(), "nvcf-cli-local.yaml")
	if err := os.WriteFile(configPath, []byte("base_http_url: http://api.localhost:8080\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfgFile = configPath
	configStateManager = nil
	configStateManagerKey = ""

	if err := LoadStateForCurrentCommand(); err != nil {
		t.Fatalf("load state: %v", err)
	}

	SetCurrentFunction("func-from-create", "ver-from-create", "bdd-function")
	SetCurrentTokens("admin-token", "", time.Now().Add(time.Hour), time.Time{})
	if err := SaveStateForCurrentCommand(); err != nil {
		t.Fatalf("save state: %v", err)
	}

	sm := state.GetStateManagerForConfig(configPath)
	if err := sm.Load(); err != nil {
		t.Fatalf("reload state: %v", err)
	}
	saved := sm.GetState()
	if saved.ConfigFile != configPath {
		t.Fatalf("ConfigFile = %q, want %q", saved.ConfigFile, configPath)
	}
	if saved.FunctionID != "func-from-create" {
		t.Fatalf("FunctionID = %q, want func-from-create", saved.FunctionID)
	}
	if saved.VersionID != "ver-from-create" {
		t.Fatalf("VersionID = %q, want ver-from-create", saved.VersionID)
	}
	if saved.Token != "admin-token" {
		t.Fatalf("Token = %q, want admin-token", saved.Token)
	}
}

func TestLoadDeployConfig(t *testing.T) {
	previousInputFile := deployFlags.inputFile
	previousGPU := deployFlags.gpu
	previousBackend := deployFlags.backend
	previousClusterName := deployFlags.clusterName
	t.Cleanup(func() {
		deployFlags.inputFile = previousInputFile
		deployFlags.gpu = previousGPU
		deployFlags.backend = previousBackend
		deployFlags.clusterName = previousClusterName
	})

	writeJSON := func(t *testing.T, contents string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "deploy.json")
		if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
			t.Fatalf("write JSON: %v", err)
		}
		return path
	}

	t.Run("nested configuration and CLI overrides", func(t *testing.T) {
		deployFlags.inputFile = writeJSON(t, `{"functionId":"file-function","versionId":"file-version","deploymentSpecifications":[{"gpu":"L40S","instanceType":"NCP.GPU.L40S_1x","minInstances":2,"maxInstances":4,"clusters":["cluster-a"],"regions":["region-a"],"cpuArch":"x86_64","configuration":{"key":"value"}}]}`)
		deployFlags.gpu = "B200"
		deployFlags.backend = ""
		deployFlags.clusterName = "legacy-cluster"

		cmd := &cobra.Command{}
		cmd.Flags().String("gpu", "", "")
		cmd.Flags().String("backend", "", "")
		cmd.Flags().String("cluster-name", "", "")
		for name, value := range map[string]string{"gpu": "B200", "backend": "", "cluster-name": "legacy-cluster"} {
			if err := cmd.Flags().Set(name, value); err != nil {
				t.Fatalf("set %s flag: %v", name, err)
			}
		}

		config, err := loadDeployConfig(cmd)
		if err != nil {
			t.Fatalf("load deploy config: %v", err)
		}
		if config.FunctionID != "file-function" || config.VersionID != "file-version" {
			t.Fatalf("IDs = (%q, %q), want file-function and file-version", config.FunctionID, config.VersionID)
		}
		if config.GPU != "B200" || config.InstanceType != "NCP.GPU.L40S_1x" {
			t.Fatalf("GPU and instance type = (%q, %q), want B200 and NCP.GPU.L40S_1x", config.GPU, config.InstanceType)
		}
		if config.MinInstances != 2 || config.MaxInstances != 4 || config.Timeout != 900 {
			t.Fatalf("instance counts and timeout = (%d, %d, %d), want (2, 4, 900)", config.MinInstances, config.MaxInstances, config.Timeout)
		}
		if config.Backend != "legacy-cluster" || len(config.Clusters) != 1 || config.Clusters[0] != "cluster-a" || len(config.Regions) != 1 || config.Regions[0] != "region-a" {
			t.Fatalf("backend, clusters, regions = (%q, %v, %v), want legacy-cluster, [cluster-a], [region-a]", config.Backend, config.Clusters, config.Regions)
		}
		if config.CPUArch != "x86_64" || config.Configuration["key"] != "value" {
			t.Fatalf("hardware/configuration = (%q, %v), want x86_64 and key=value", config.CPUArch, config.Configuration)
		}
	})

	t.Run("malformed JSON", func(t *testing.T) {
		deployFlags.inputFile = writeJSON(t, `{`)
		if _, err := loadDeployConfig(&cobra.Command{}); err == nil {
			t.Fatal("loadDeployConfig succeeded for malformed JSON")
		}
	})

	t.Run("empty deployment specifications", func(t *testing.T) {
		deployFlags.inputFile = writeJSON(t, `{"deploymentSpecifications":[]}`)
		if _, err := loadDeployConfig(&cobra.Command{}); err == nil {
			t.Fatal("loadDeployConfig succeeded with an empty deploymentSpecifications array")
		}
	})
}
