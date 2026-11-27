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
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"nvcf-cli/internal/client"
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

func TestRunDeployGetOutput(t *testing.T) {
	responseBody := `{"deployment":{"functionId":"function-123","functionVersionId":"version-456","deploymentId":"deployment-789","functionName":"sample-function","ncaId":"nca-012","functionStatus":"ACTIVE","createdAt":"2026-01-02T03:04:05Z","lastUpdatedAt":"2026-01-03T04:05:06Z","healthInfo":[{"status":"HEALTHY","message":"ready","reason":"checks passed"}],"deploymentSpecifications":[{"gpu":"H100","instanceType":"NCP.GPU.H100_1x","minInstances":1,"maxInstances":4,"backend":"backend-a","maxRequestConcurrency":16,"preferredOrder":2,"regions":["us-east"],"availabilityZones":["zone-a"],"clusters":["cluster-a"]}]}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v2/nvcf/deployments/functions/function-123/versions/version-456" {
			t.Errorf("request = %s %s, want deployment GET", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer deploy-test-token" {
			t.Errorf("Authorization = %q, want Bearer deploy-test-token", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, responseBody)
	}))
	defer server.Close()

	previousSettings := viper.GetViper().AllSettings()
	viper.Reset()
	previousFunctionID, previousVersionID := deployGetFlags.functionID, deployGetFlags.versionID
	previousJSONOutput := jsonOutput
	t.Cleanup(func() {
		viper.Reset()
		for key, value := range previousSettings {
			viper.Set(key, value)
		}
		deployGetFlags.functionID, deployGetFlags.versionID = previousFunctionID, previousVersionID
		jsonOutput = previousJSONOutput
	})
	viper.Set("base_http_url", server.URL)
	viper.Set("token", "deploy-test-token")
	deployGetFlags.functionID, deployGetFlags.versionID = "function-123", "version-456"

	cases := []struct {
		name       string
		jsonOutput bool
		want       []string
	}{
		{
			name: "formatted deployment details",
			want: []string{
				"Function ID:         function-123", "Version ID:          version-456",
				"Deployment ID:       deployment-789", "Function Name:       sample-function",
				"NCA ID:              nca-012", "Status:              ACTIVE",
				"Message: ready", "Reason: checks passed", "GPU:                   H100",
				"Backend:               backend-a", "Max Concurrency:       16",
				"Preferred Order:       2", "Regions:               us-east",
				"Availability Zones:    zone-a", "Clusters:              cluster-a",
			},
		},
		{name: "JSON response", jsonOutput: true, want: []string{`"deploymentId": "deployment-789"`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			jsonOutput = tc.jsonOutput
			readEnd, writeEnd, err := os.Pipe()
			if err != nil {
				t.Fatalf("create stdout pipe: %v", err)
			}
			previousStdout := os.Stdout
			os.Stdout = writeEnd
			err = runDeployGet(deployGetCmd, nil)
			os.Stdout = previousStdout
			if closeErr := writeEnd.Close(); closeErr != nil {
				t.Fatalf("close stdout pipe: %v", closeErr)
			}
			if err != nil {
				t.Fatalf("runDeployGet: %v", err)
			}
			output, err := io.ReadAll(readEnd)
			_ = readEnd.Close()
			if err != nil {
				t.Fatalf("read stdout: %v", err)
			}
			for _, expected := range tc.want {
				if !bytes.Contains(output, []byte(expected)) {
					t.Errorf("output missing %q:\n%s", expected, output)
				}
			}
		})
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

func TestLoadDeployConfigChangedFlags(t *testing.T) {
	previousFlags := deployFlags
	t.Cleanup(func() { deployFlags = previousFlags })
	deployFlags = struct {
		inputFile             string
		functionID            string
		versionID             string
		instanceType          string
		gpu                   string
		minInstances          int
		maxInstances          int
		backend               string
		clusters              []string
		regions               []string
		availabilityZones     []string
		attributes            []string
		maxRequestConcurrency int
		preferredOrder        int
		cpuArch               string
		os                    string
		driverVersion         string
		storage               string
		systemMemory          string
		gpuMemory             string
		helmConfig            map[string]string
		clusterName           string
		timeout               int
	}{
		functionID: "function-flag", versionID: "version-flag", instanceType: "NCP.GPU.B200_1x", gpu: "B200",
		minInstances: 2, maxInstances: 5, clusters: []string{"cluster-flag"}, regions: []string{"region-flag"},
		availabilityZones: []string{"zone-flag"}, attributes: []string{"attribute-flag"}, maxRequestConcurrency: 12,
		preferredOrder: 3, cpuArch: "arm64", os: "linux", driverVersion: "550.54", storage: "80G",
		systemMemory: "256G", gpuMemory: "180G", clusterName: "legacy-backend", timeout: 1200,
	}

	cmd := &cobra.Command{}
	flagValues := map[string]string{
		"function-id": "function-flag", "version-id": "version-flag", "instance-type": "NCP.GPU.B200_1x", "gpu": "B200",
		"min-instances": "2", "max-instances": "5", "clusters": "cluster-flag", "regions": "region-flag",
		"availability-zones": "zone-flag", "attributes": "attribute-flag", "max-request-concurrency": "12",
		"preferred-order": "3", "cpu-arch": "arm64", "os": "linux", "driver-version": "550.54",
		"storage": "80G", "system-memory": "256G", "gpu-memory": "180G", "timeout": "1200", "cluster-name": "legacy-backend",
	}
	for name, value := range flagValues {
		cmd.Flags().String(name, "", "")
		if err := cmd.Flags().Set(name, value); err != nil {
			t.Fatalf("set %s flag: %v", name, err)
		}
	}

	config, err := loadDeployConfig(cmd)
	if err != nil {
		t.Fatalf("load deploy config: %v", err)
	}
	if config.FunctionID != "function-flag" || config.VersionID != "version-flag" || config.InstanceType != "NCP.GPU.B200_1x" || config.GPU != "B200" {
		t.Fatalf("identity/spec fields = (%q, %q, %q, %q)", config.FunctionID, config.VersionID, config.InstanceType, config.GPU)
	}
	if config.MinInstances != 2 || config.MaxInstances != 5 || config.Timeout != 1200 {
		t.Fatalf("counts/timeout = (%d, %d, %d)", config.MinInstances, config.MaxInstances, config.Timeout)
	}
	if config.Backend != "legacy-backend" || len(config.Clusters) != 1 || config.Clusters[0] != "cluster-flag" || len(config.Regions) != 1 || config.Regions[0] != "region-flag" || len(config.AvailabilityZones) != 1 || config.AvailabilityZones[0] != "zone-flag" || len(config.Attributes) != 1 || config.Attributes[0] != "attribute-flag" {
		t.Fatalf("placement fields = backend %q, clusters %v, regions %v, zones %v, attributes %v", config.Backend, config.Clusters, config.Regions, config.AvailabilityZones, config.Attributes)
	}
	if config.MaxRequestConcurrency != 12 || config.PreferredOrder != 3 || config.CPUArch != "arm64" || config.OS != "linux" || config.DriverVersion != "550.54" || config.Storage != "80G" || config.SystemMemory != "256G" || config.GPUMemory != "180G" {
		t.Fatalf("optional fields were not populated: %+v", config)
	}
}

func TestDeployUpdateValidationAndGPUSpecResolution(t *testing.T) {
	min, max, negative, zero := 1, 3, -1, 0
	validConfig := func() *UpdateDeploymentConfig {
		return &UpdateDeploymentConfig{
			FunctionID: "function-id", VersionID: "version-id", GPU: "H100", InstanceType: "NCP.GPU.H100_1x",
			MinInstances: &min,
		}
	}
	tests := []struct {
		name    string
		config  *UpdateDeploymentConfig
		wantErr bool
	}{
		{name: "valid selector and settable field", config: validConfig()},
		{name: "missing function ID", config: func() *UpdateDeploymentConfig { c := validConfig(); c.FunctionID = ""; return c }(), wantErr: true},
		{name: "missing version ID", config: func() *UpdateDeploymentConfig { c := validConfig(); c.VersionID = ""; return c }(), wantErr: true},
		{name: "missing GPU", config: func() *UpdateDeploymentConfig { c := validConfig(); c.GPU = ""; return c }(), wantErr: true},
		{name: "missing instance type", config: func() *UpdateDeploymentConfig { c := validConfig(); c.InstanceType = ""; return c }(), wantErr: true},
		{name: "no settable field", config: func() *UpdateDeploymentConfig { c := validConfig(); c.MinInstances = nil; return c }(), wantErr: true},
		{name: "negative minimum", config: func() *UpdateDeploymentConfig { c := validConfig(); c.MinInstances = &negative; return c }(), wantErr: true},
		{name: "nonpositive maximum", config: func() *UpdateDeploymentConfig { c := validConfig(); c.MinInstances = nil; c.MaxInstances = &zero; return c }(), wantErr: true},
		{name: "minimum greater than maximum", config: func() *UpdateDeploymentConfig { c := validConfig(); c.MinInstances = &max; c.MaxInstances = &min; return c }(), wantErr: true},
		{name: "custom policy", config: func() *UpdateDeploymentConfig { c := validConfig(); c.MinInstances = nil; c.AutoscalingConfigurationPolicy = client.AutoscalingPolicyCustom; return c }()},
		{name: "platform policy", config: func() *UpdateDeploymentConfig { c := validConfig(); c.MinInstances = nil; c.AutoscalingConfigurationPolicy = client.AutoscalingPolicyPlatform; return c }()},
		{name: "invalid policy", config: func() *UpdateDeploymentConfig { c := validConfig(); c.AutoscalingConfigurationPolicy = "invalid"; return c }(), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDeployUpdateConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateDeployUpdateConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	config := validConfig()
	response := func(specs ...client.GPUSpecificationDto) *client.DeploymentResponse {
		return &client.DeploymentResponse{Deployment: client.FunctionDeploymentDto{
			DeploymentID: "deployment-id", DeploymentSpecifications: specs,
		}}
	}
	gpuSpec := func(id, gpu, instanceType string) client.GPUSpecificationDto {
		return client.GPUSpecificationDto{GpuSpecificationID: id, GPU: gpu, InstanceType: instanceType}
	}
	gpuTests := []struct {
		name       string
		deployment *client.DeploymentResponse
		wantID     string
		wantErr    bool
	}{
		{name: "unique match ignores nonmatching specs", deployment: response(gpuSpec("other-id", "A100", "other"), gpuSpec("gpu-spec-id", config.GPU, config.InstanceType)), wantID: "gpu-spec-id"},
		{name: "missing deployment ID", deployment: &client.DeploymentResponse{Deployment: client.FunctionDeploymentDto{DeploymentSpecifications: []client.GPUSpecificationDto{gpuSpec("id", config.GPU, config.InstanceType)}}}, wantErr: true},
		{name: "no match", deployment: response(gpuSpec("id", "A100", config.InstanceType)), wantErr: true},
		{name: "duplicate matches", deployment: response(gpuSpec("first", config.GPU, config.InstanceType), gpuSpec("second", config.GPU, config.InstanceType)), wantErr: true},
		{name: "matched spec without ID", deployment: response(gpuSpec("", config.GPU, config.InstanceType)), wantErr: true},
	}
	for _, tt := range gpuTests {
		t.Run("GPU spec: "+tt.name, func(t *testing.T) {
			gotID, err := findDeployUpdateGPUSpecID(tt.deployment, config)
			if (err != nil) != tt.wantErr {
				t.Fatalf("findDeployUpdateGPUSpecID() error = %v, wantErr %v", err, tt.wantErr)
			}
			if gotID != tt.wantID {
				t.Fatalf("findDeployUpdateGPUSpecID() ID = %q, want %q", gotID, tt.wantID)
			}
		})
	}
}

func TestLoadDeployUpdateConfig(t *testing.T) {
	previousFlags := deployUpdateFlags
	t.Cleanup(func() {
		deployUpdateFlags = previousFlags
	})

	inputFile := filepath.Join(t.TempDir(), "update.json")
	data := `{"functionId":"file-function","versionId":"file-version","gpu":"H100","instanceType":"NCP.GPU.H100_1x","minInstances":1,"maxInstances":4}`
	if err := os.WriteFile(inputFile, []byte(data), 0o600); err != nil {
		t.Fatalf("write update config: %v", err)
	}

	tests := []struct {
		name      string
		inputFile string
		fileData  string
		writeFile bool
		wantErr   bool
	}{
		{name: "merge file and changed flags", inputFile: inputFile},
		{name: "malformed JSON", inputFile: "malformed.json", fileData: `{`, writeFile: true, wantErr: true},
		{name: "missing input file", inputFile: filepath.Join(t.TempDir(), "missing.json"), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.writeFile {
				path := filepath.Join(t.TempDir(), tt.inputFile)
				if err := os.WriteFile(path, []byte(tt.fileData), 0o600); err != nil {
					t.Fatalf("write test input: %v", err)
				}
				tt.inputFile = path
			}
			deployUpdateFlags = struct {
				inputFile         string
				functionID        string
				versionID         string
				gpu               string
				instanceType      string
				minInstances      int
				maxInstances      int
				autoscalingPolicy string
			}{
				inputFile: tt.inputFile, functionID: "flag-function", minInstances: 2,
				maxInstances: 3, autoscalingPolicy: string(client.AutoscalingPolicyCustom),
			}

			cmd := &cobra.Command{Use: "update"}
			cmd.Flags().String("function-id", "", "")
			cmd.Flags().String("version-id", "", "")
			cmd.Flags().String("gpu", "", "")
			cmd.Flags().String("instance-type", "", "")
			cmd.Flags().Int("min-instances", 0, "")
			cmd.Flags().Int("max-instances", 0, "")
			cmd.Flags().String("autoscaling-policy", "", "")
			for name, value := range map[string]string{
				"function-id": "flag-function", "min-instances": "2", "max-instances": "3",
				"autoscaling-policy": string(client.AutoscalingPolicyCustom),
			} {
				if err := cmd.Flags().Set(name, value); err != nil {
					t.Fatalf("mark %s changed: %v", name, err)
				}
			}

			got, err := loadDeployUpdateConfig(cmd)
			if (err != nil) != tt.wantErr {
				t.Fatalf("loadDeployUpdateConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got.FunctionID != "flag-function" || got.VersionID != "file-version" {
				t.Errorf("IDs = (%q, %q), want flag-function and file-version", got.FunctionID, got.VersionID)
			}
			if got.GPU != "H100" || got.InstanceType != "NCP.GPU.H100_1x" {
				t.Errorf("GPU and instance type = (%q, %q), want file values", got.GPU, got.InstanceType)
			}
			if got.MinInstances == nil || *got.MinInstances != 2 {
				t.Errorf("MinInstances = %v, want pointer to 2", got.MinInstances)
			}
			if got.MaxInstances == nil || *got.MaxInstances != 3 {
				t.Errorf("MaxInstances = %v, want pointer to 3", got.MaxInstances)
			}
			if got.AutoscalingConfigurationPolicy != client.AutoscalingPolicyCustom {
				t.Errorf("AutoscalingConfigurationPolicy = %q, want %q", got.AutoscalingConfigurationPolicy, client.AutoscalingPolicyCustom)
			}
		})
	}
}

func TestRunDeployUpdateHTTPPaths(t *testing.T) {
	previousSettings := viper.GetViper().AllSettings()
	previousFlags := deployUpdateFlags
	t.Cleanup(func() {
		viper.Reset()
		for key, value := range previousSettings {
			viper.Set(key, value)
		}
		deployUpdateFlags = previousFlags
	})

	cases := []struct {
		name         string
		lookupStatus int
		lookupBody   string
		patchStatus  int
		patchBody    string
		wantErr      string
		wantPatch    bool
	}{
		{
			name:         "updates matching GPU specification",
			lookupStatus: http.StatusOK,
			lookupBody:   `{"deployment":{"deploymentId":"deployment-123","deploymentSpecifications":[{"gpuSpecificationId":"gpu-spec-456","gpu":"H100","instanceType":"NCP.GPU.H100_1x"}]}}`,
			patchStatus:  http.StatusOK,
			patchBody:    `{"gpuSpecification":{"gpu":"H100","instanceType":"NCP.GPU.H100_1x","minInstances":2}}`,
			wantPatch:    true,
		},
		{
			name:         "lookup failure",
			lookupStatus: http.StatusInternalServerError,
			lookupBody:   `lookup unavailable`,
			wantErr:      "failed to look up existing deployment",
		},
		{
			name:         "no matching GPU specification",
			lookupStatus: http.StatusOK,
			lookupBody:   `{"deployment":{"deploymentId":"deployment-123","deploymentSpecifications":[{"gpuSpecificationId":"gpu-spec-456","gpu":"A100","instanceType":"NCP.GPU.H100_1x"}]}}`,
			wantErr:      "no GPU spec matches",
		},
		{
			name:         "PATCH failure",
			lookupStatus: http.StatusOK,
			lookupBody:   `{"deployment":{"deploymentId":"deployment-123","deploymentSpecifications":[{"gpuSpecificationId":"gpu-spec-456","gpu":"H100","instanceType":"NCP.GPU.H100_1x"}]}}`,
			patchStatus:  http.StatusInternalServerError,
			patchBody:    `update unavailable`,
			wantErr:      "failed to update GPU specification",
			wantPatch:    true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			patchSeen := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					if r.URL.Path != "/v2/nvcf/deployments/functions/function-123/versions/version-456" {
						t.Errorf("lookup path = %q", r.URL.Path)
					}
					w.WriteHeader(tc.lookupStatus)
					_, _ = io.WriteString(w, tc.lookupBody)
					return
				}
				patchSeen = true
				if r.Method != http.MethodPatch || r.URL.Path != "/v2/nvcf/deployments/deployment-123/gpu-specifications/gpu-spec-456" {
					t.Errorf("PATCH request = %s %s", r.Method, r.URL.Path)
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read PATCH body: %v", err)
				} else if !bytes.Contains(body, []byte(`"minInstances":2`)) {
					t.Errorf("PATCH body %s does not contain minInstances 2", body)
				}
				w.WriteHeader(tc.patchStatus)
				_, _ = io.WriteString(w, tc.patchBody)
			}))
			defer server.Close()

			viper.Reset()
			viper.Set("base_http_url", server.URL)
			viper.Set("token", "deploy-test-token")
			inputFile := filepath.Join(t.TempDir(), "update.json")
			if err := os.WriteFile(inputFile, []byte(`{"functionId":"function-123","versionId":"version-456","gpu":"H100","instanceType":"NCP.GPU.H100_1x","minInstances":2}`), 0o600); err != nil {
				t.Fatalf("write update config: %v", err)
			}
			deployUpdateFlags = struct {
				inputFile         string
				functionID        string
				versionID         string
				gpu               string
				instanceType      string
				minInstances      int
				maxInstances      int
				autoscalingPolicy string
			}{inputFile: inputFile}

			cmd := &cobra.Command{Use: "update"}
			cmd.Flags().String("function-id", "", "")
			cmd.Flags().String("version-id", "", "")
			cmd.Flags().String("gpu", "", "")
			cmd.Flags().String("instance-type", "", "")
			cmd.Flags().Int("min-instances", 0, "")
			cmd.Flags().Int("max-instances", 0, "")
			cmd.Flags().String("autoscaling-policy", "", "")
			err := runDeployUpdate(cmd, nil)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("runDeployUpdate() error = %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("runDeployUpdate() error = %v, want context %q", err, tc.wantErr)
			}
			if patchSeen != tc.wantPatch {
				t.Errorf("PATCH seen = %t, want %t", patchSeen, tc.wantPatch)
			}
		})
	}
}
