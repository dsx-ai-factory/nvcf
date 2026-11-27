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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"nvcf-cli/internal/client"
	"nvcf-cli/internal/logging"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runFunctionCreateWithResponse(t *testing.T, response string, demo bool) (string, error, string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v2/nvcf/functions", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(response))
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	testDir := t.TempDir()
	t.Setenv("HOME", testDir)
	t.Chdir(testDir)
	inputFile := filepath.Join(testDir, "create-function.json")
	require.NoError(t, os.WriteFile(inputFile, []byte(`{
		"name": "function-task-test",
		"containerImage": "registry.example/function:test",
		"inferenceUrl": "/echo",
		"inferencePort": 8000,
		"healthProtocol": "HTTP",
		"healthUri": "/health",
		"healthPort": 8000,
		"healthTimeout": "PT30S",
		"healthExpectedStatus": 200
	}`), 0600))

	oldCreateFlags := createFlags
	oldCfgFile := cfgFile
	oldStateManager := configStateManager
	oldStateManagerKey := configStateManagerKey
	t.Cleanup(func() {
		createFlags = oldCreateFlags
		cfgFile = oldCfgFile
		configStateManager = oldStateManager
		configStateManagerKey = oldStateManagerKey
		jsonOutput = false
		logging.SetJSONOutput(false)
		viper.Reset()
	})

	viper.Reset()
	viper.Set("base_http_url", server.URL)
	viper.Set("base_grpc_url", "localhost:50051")
	viper.Set("token", "test-admin-token")
	viper.Set("demo", demo)
	cfgFile = filepath.Join(testDir, "workflow.yaml")
	configStateManager = nil
	configStateManagerKey = ""
	createFlags.inputFile = inputFile
	jsonOutput = true
	logging.SetJSONOutput(true)

	var runErr error
	output := captureStdout(t, func() {
		runErr = runCreate(createCmd, nil)
	})
	return output, runErr, testDir
}

func TestLoadDeleteConfig(t *testing.T) {
	oldDeleteFlags := deleteFlags
	t.Cleanup(func() { deleteFlags = oldDeleteFlags })

	testDir := t.TempDir()
	validPath := filepath.Join(testDir, "delete.json")
	malformedPath := filepath.Join(testDir, "malformed.json")
	require.NoError(t, os.WriteFile(validPath, []byte(`{"functionId":"json-function","versionId":"json-version","graceful":false,"deleteDeploymentOnly":true}`), 0600))
	require.NoError(t, os.WriteFile(malformedPath, []byte(`{"functionId":`), 0600))

	tests := []struct {
		name      string
		inputFile string
		setFlags  map[string]string
		want      *DeleteConfig
		wantErr   bool
	}{
		{
			name:      "JSON values are retained when flags are unchanged",
			inputFile: validPath,
			want:      &DeleteConfig{FunctionID: "json-function", VersionID: "json-version", DeleteDeploymentOnly: true},
		},
		{
			name:      "changed CLI flags override corresponding JSON values",
			inputFile: validPath,
			setFlags: map[string]string{
				"function-id":     "cli-function",
				"version-id":      "cli-version",
				"graceful":        "true",
				"deployment-only": "false",
			},
			want: &DeleteConfig{FunctionID: "cli-function", VersionID: "cli-version", Graceful: true},
		},
		{name: "unreadable file", inputFile: filepath.Join(testDir, "missing.json"), wantErr: true},
		{name: "malformed JSON", inputFile: malformedPath, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleteFlags = struct {
				inputFile            string
				functionID           string
				versionID            string
				graceful             bool
				deleteDeploymentOnly bool
			}{}
			deleteFlags.inputFile = tt.inputFile
			cmd := &cobra.Command{Use: "delete"}
			cmd.Flags().StringVar(&deleteFlags.functionID, "function-id", "", "")
			cmd.Flags().StringVar(&deleteFlags.versionID, "version-id", "", "")
			cmd.Flags().BoolVar(&deleteFlags.graceful, "graceful", false, "")
			cmd.Flags().BoolVar(&deleteFlags.deleteDeploymentOnly, "deployment-only", false, "")
			for name, value := range tt.setFlags {
				require.NoError(t, cmd.Flags().Set(name, value))
			}

			config, err := loadDeleteConfig(cmd)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.inputFile)
				assert.Nil(t, config)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, config)
		})
	}
}

func TestLoadCreateConfigAppliesAllFlagOverrides(t *testing.T) {
	originalFlags := createFlags
	t.Cleanup(func() { createFlags = originalFlags })
	createFlags = struct {
		inputFile            string
		name                 string
		containerImage       string
		inferenceURL         string
		inferencePort        int
		description          string
		tags                 []string
		healthURI            string
		healthProtocol       string
		healthPort           int
		healthTimeout        string
		healthExpectedStatus int
		functionType         string
		apiBodyFormat        string
		containerArgs        string
		containerEnvironment []string
		helmChart            string
		helmChartServiceName string
		secrets              []string
		models               []string
		llmModels            []string
		resources            []string
		rateLimit            string
		rateLimitExempted    []string
		rateLimitSync        bool
		logsTelemetryId      string
		metricsTelemetryId   string
		tracesTelemetryId    string
	}{}
	inputFile := filepath.Join(t.TempDir(), "create.json")
	require.NoError(t, os.WriteFile(inputFile, []byte(`{"name":"file-name","containerImage":"file-image","description":"file description","llmInvocationConfig":{"priority":{"defaultPriority":3,"perAccountPriority":{"nca-file":4}}}}`), 0600))
	createFlags.inputFile = inputFile

	cmd := &cobra.Command{Use: "create"}
	flags := cmd.Flags()
	flags.StringVar(&createFlags.name, "name", "", "")
	flags.StringVar(&createFlags.containerImage, "image", "", "")
	flags.StringVar(&createFlags.inferenceURL, "inference-url", "", "")
	flags.IntVar(&createFlags.inferencePort, "inference-port", 0, "")
	flags.StringVar(&createFlags.description, "description", "", "")
	flags.StringSliceVar(&createFlags.tags, "tags", nil, "")
	flags.StringVar(&createFlags.healthURI, "health-uri", "", "")
	flags.StringVar(&createFlags.healthProtocol, "health-protocol", "", "")
	flags.IntVar(&createFlags.healthPort, "health-port", 0, "")
	flags.StringVar(&createFlags.healthTimeout, "health-timeout", "", "")
	flags.IntVar(&createFlags.healthExpectedStatus, "health-expected-status", 200, "")
	flags.StringVar(&createFlags.functionType, "function-type", "", "")
	flags.StringVar(&createFlags.apiBodyFormat, "api-body-format", "", "")
	flags.StringVar(&createFlags.containerArgs, "container-args", "", "")
	flags.StringSliceVar(&createFlags.containerEnvironment, "container-env", nil, "")
	flags.StringVar(&createFlags.helmChart, "helm-chart", "", "")
	flags.StringVar(&createFlags.helmChartServiceName, "helm-chart-service", "", "")
	flags.StringSliceVar(&createFlags.secrets, "secrets", nil, "")
	flags.StringSliceVar(&createFlags.models, "models", nil, "")
	flags.StringArrayVar(&createFlags.llmModels, "llm-model", nil, "")
	flags.Uint32(llmDefaultPriorityFlag, 0, "")
	flags.StringArray(llmPerAccountPriorityFlag, nil, "")
	flags.StringSliceVar(&createFlags.resources, "resources", nil, "")
	flags.StringVar(&createFlags.rateLimit, "rate-limit", "", "")
	flags.StringSliceVar(&createFlags.rateLimitExempted, "rate-limit-exempted", nil, "")
	flags.BoolVar(&createFlags.rateLimitSync, "rate-limit-sync", false, "")
	flags.StringVar(&createFlags.logsTelemetryId, "logs-telemetry-id", "", "")
	flags.StringVar(&createFlags.metricsTelemetryId, "metrics-telemetry-id", "", "")
	flags.StringVar(&createFlags.tracesTelemetryId, "traces-telemetry-id", "", "")

	values := map[string]string{
		"name": "flag-name", "image": "registry.example/flag:1", "inference-url": "/flag", "inference-port": "8100",
		"description": "flag description", "tags": "alpha,beta", "health-uri": "/ready", "health-protocol": "HTTP",
		"health-port": "8101", "health-timeout": "PT9S", "health-expected-status": "204",
		"function-type": "STREAMING", "api-body-format": "CUSTOM", "container-args": "--flag",
		"container-env": "MODE=flag", "helm-chart": "chart:flag", "helm-chart-service": "flag-service",
		"secrets": "TOKEN=flag", "models": "model:v2:ngc://models/flag", "llm-model": "name=llm,uris=ngc://models/llm,routingMethod=priority",
		llmDefaultPriorityFlag: "11", llmPerAccountPriorityFlag: "nca-flag:17", "resources": "resource:v3:ngc://resources/flag",
		"rate-limit": "12-M", "rate-limit-exempted": "nca-exempt", "rate-limit-sync": "true",
		"logs-telemetry-id": "logs-flag", "metrics-telemetry-id": "metrics-flag", "traces-telemetry-id": "traces-flag",
	}
	for name, value := range values {
		require.NoError(t, flags.Set(name, value), "set %s", name)
		assert.True(t, flags.Changed(name), "%s should be marked changed", name)
	}

	config, err := loadCreateConfig(cmd)
	require.NoError(t, err)
	assert.Equal(t, "flag-name", config.Name)
	assert.Equal(t, "registry.example/flag:1", config.ContainerImage)
	assert.Equal(t, "/flag", config.InferenceURL)
	assert.Equal(t, 8100, config.InferencePort)
	assert.Equal(t, "flag description", config.Description)
	assert.Equal(t, []string{"alpha", "beta"}, config.Tags)
	assert.Equal(t, "/ready", config.HealthURI)
	assert.Equal(t, "HTTP", config.HealthProtocol)
	assert.Equal(t, 8101, config.HealthPort)
	assert.Equal(t, "PT9S", config.HealthTimeout)
	assert.Equal(t, 204, config.HealthExpectedStatus)
	assert.Equal(t, "STREAMING", config.FunctionType)
	assert.Equal(t, "CUSTOM", config.APIBodyFormat)
	assert.Equal(t, "--flag", config.ContainerArgs)
	assert.Equal(t, []ContainerEnvironmentEntry{{Key: "MODE", Value: "flag"}}, config.ContainerEnvironment)
	assert.Equal(t, "chart:flag", config.HelmChart)
	assert.Equal(t, "flag-service", config.HelmChartServiceName)
	assert.Equal(t, []string{"TOKEN=flag"}, config.Secrets)
	routingMethod := "priority"
	assert.Equal(t, []ArtifactConfig{{Name: "model", Version: "v2", URI: "ngc://models/flag"}, {Name: "llm", LLMConfig: &LLMConfigInput{URIs: []string{"ngc://models/llm"}, RoutingMethod: &routingMethod}}}, config.Models)
	assert.Equal(t, []ArtifactConfig{{Name: "resource", Version: "v3", URI: "ngc://resources/flag"}}, config.Resources)
	assert.Equal(t, "12-M", config.RateLimit)
	assert.Equal(t, []string{"nca-exempt"}, config.RateLimitExempted)
	assert.True(t, config.RateLimitSync)
	assert.Equal(t, "logs-flag", config.LogsTelemetryId)
	assert.Equal(t, "metrics-flag", config.MetricsTelemetryId)
	assert.Equal(t, "traces-flag", config.TracesTelemetryId)
	assertPriorityConfig(t, config.LLMInvocationConfig, 11, map[string]uint32{"nca-flag": 17})
}

func TestBuildCreateFunctionRequestConvertsSecrets(t *testing.T) {
	originalSecrets := createFlags.secrets
	t.Cleanup(func() { createFlags.secrets = originalSecrets })

	tests := []struct {
		name        string
		cliSecrets  []string
		jsonSecrets interface{}
		want        []client.SecretDto
		wantErr     bool
	}{
		{
			name:       "combines CLI and JSON secrets and skips unsupported values",
			cliSecrets: []string{"CLI_TOKEN=cli-value"},
			jsonSecrets: []interface{}{
				"JSON_TOKEN=json-value",
				map[string]interface{}{"name": "OBJECT_TOKEN", "value": "object-value"},
				42,
			},
			want: []client.SecretDto{
				{Name: "CLI_TOKEN", Value: "cli-value"},
				{Name: "JSON_TOKEN", Value: "json-value"},
				{Name: "OBJECT_TOKEN", Value: "object-value"},
			},
		},
		{
			name:        "malformed CLI pair returns no request",
			cliSecrets:  []string{"MALFORMED"},
			jsonSecrets: []interface{}{"JSON_TOKEN=json-value"},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			createFlags.secrets = tt.cliSecrets
			req, _, err := buildCreateFunctionRequest(&CreateConfig{Secrets: tt.jsonSecrets})
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, req)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, req)
			assert.Equal(t, tt.want, req.Secrets)
		})
	}
}

func runTaskCreateWithResponse(t *testing.T, response string) (string, error) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/nvct/tasks", r.URL.Path)
		assert.Equal(t, "Bearer test-task-api-key", r.Header.Get("Authorization"))
		var request client.CreateTaskRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		assert.Equal(t, "task-test", request.Name)
		require.NotNil(t, request.GpuSpecification)
		assert.Equal(t, "H100", request.GpuSpecification.GPU)
		assert.Equal(t, "NCP.GPU.H100_8x", request.GpuSpecification.InstanceType)
		assert.Equal(t, "ncp-local", request.GpuSpecification.Backend)
		assert.Equal(t, "registry.example/task:test", request.ContainerImage)
		assert.Equal(t, []client.ContainerEnvironmentEntry{{
			Key:   "WORKFLOW_REQUEST_BASE64",
			Value: "encoded-request",
		}, {
			Key:   "RESULTS_LOCATION",
			Value: "test-org/test-model",
		}}, request.ContainerEnvironment)
		assert.Equal(t, []client.ArtifactDto{{
			Name:    "model",
			Version: "test",
			URI:     "ngc://models/test",
		}}, request.Models)
		assert.Equal(t, []client.ArtifactDto{{
			Name:    "dataset",
			Version: "test",
			URI:     "ngc://datasets/test",
		}}, request.Resources)
		assert.Equal(t, "UPLOAD", request.ResultHandlingStrategy)
		assert.Equal(t, "test-org/test-model", request.ResultsLocation)
		assert.Equal(t, "Inventory model and dataset artifacts", request.Description)
		assert.Equal(t, "PT15M", request.MaxRuntimeDuration)
		assert.Equal(t, "PT15M", request.MaxQueuedDuration)
		assert.Equal(t, "PT1M", request.TerminationGracePeriodDuration)
		assert.Equal(t, []client.SecretDto{{
			Name:  "NGC_API_KEY",
			Value: "nvapi-test",
		}}, request.Secrets)
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(response))
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	testDir := t.TempDir()
	t.Setenv("HOME", testDir)
	inputFile := filepath.Join(testDir, "create-task.json")
	require.NoError(t, os.WriteFile(inputFile, []byte(`{
		"secrets": [{"name": "NGC_API_KEY", "value": "nvapi-test"}]
	}`), 0600))

	oldTaskCreateFlags := taskCreateFlags
	flagNames := []string{
		"name",
		"gpu",
		"instance-type",
		"backend",
		"image",
		"container-env",
		"models",
		"resources",
		"description",
		"max-runtime",
		"max-queued",
		"termination-grace",
		"result-strategy",
		"results-location",
	}
	oldChanged := make(map[string]bool, len(flagNames))
	for _, name := range flagNames {
		oldChanged[name] = taskCreateCmd.Flags().Lookup(name).Changed
	}
	oldCfgFile := cfgFile
	oldStateManager := configStateManager
	oldStateManagerKey := configStateManagerKey
	t.Cleanup(func() {
		taskCreateFlags = oldTaskCreateFlags
		cfgFile = oldCfgFile
		configStateManager = oldStateManager
		configStateManagerKey = oldStateManagerKey
		for name, changed := range oldChanged {
			taskCreateCmd.Flags().Lookup(name).Changed = changed
		}
		jsonOutput = false
		logging.SetJSONOutput(false)
		viper.Reset()
	})

	viper.Reset()
	viper.Set("base_nvct_url", server.URL)
	viper.Set("base_grpc_url", "localhost:50051")
	viper.Set("api_key", "test-function-api-key")
	viper.Set("nvct_api_key", "test-task-api-key")
	cfgFile = filepath.Join(testDir, "workflow.yaml")
	configStateManager = nil
	configStateManagerKey = ""
	taskCreateFlags.inputFile = inputFile
	taskCreateFlags.name = "task-test"
	taskCreateFlags.gpu = "H100"
	taskCreateFlags.instanceType = "NCP.GPU.H100_8x"
	taskCreateFlags.backend = "ncp-local"
	taskCreateFlags.containerImage = "registry.example/task:test"
	taskCreateFlags.containerEnvironment = []string{
		"WORKFLOW_REQUEST_BASE64=encoded-request",
		"RESULTS_LOCATION=test-org/test-model",
	}
	taskCreateFlags.description = "Inventory model and dataset artifacts"
	taskCreateFlags.maxRuntimeDuration = "PT15M"
	taskCreateFlags.maxQueuedDuration = "PT15M"
	taskCreateFlags.terminationGracePeriodDuration = "PT1M"
	taskCreateFlags.resultHandlingStrategy = "UPLOAD"
	taskCreateFlags.resultsLocation = "test-org/test-model"
	taskCreateFlags.models = []string{"model:test:ngc://models/test"}
	taskCreateFlags.resources = []string{"dataset:test:ngc://datasets/test"}
	for _, name := range flagNames {
		taskCreateCmd.Flags().Lookup(name).Changed = true
	}
	jsonOutput = true
	logging.SetJSONOutput(true)

	var runErr error
	output := captureStdout(t, func() {
		runErr = runTaskCreate(taskCreateCmd, nil)
	})
	return output, runErr
}

func TestGenerateDemoFolderWritesOperationFiles(t *testing.T) {
	t.Chdir(t.TempDir())

	const functionID = "function-test-id"
	const versionID = "version-test-id"
	const functionName = "demo-function"
	require.NoError(t, generateDemoFolder(functionID, versionID, &CreateConfig{Name: functionName}))

	demoDir := versionID + "_demo"
	readJSON := func(filename string) map[string]interface{} {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(demoDir, filename))
		require.NoError(t, err)
		var decoded map[string]interface{}
		require.NoError(t, json.Unmarshal(data, &decoded))
		return decoded
	}

	deploy := readJSON("deploy.json")
	assert.Equal(t, functionID, deploy["functionId"])
	assert.Equal(t, versionID, deploy["versionId"])
	specifications, ok := deploy["deploymentSpecifications"].([]interface{})
	require.True(t, ok)
	require.Len(t, specifications, 1)
	specification, ok := specifications[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "H100", specification["gpu"])
	assert.Equal(t, float64(1), specification["maxInstances"])
	assert.Equal(t, float64(1), specification["minInstances"])
	assert.Equal(t, "NCP.GPU.H100_1x", specification["instanceType"])

	invoke := readJSON("invoke.json")
	assert.Equal(t, functionID, invoke["functionId"])
	assert.Equal(t, versionID, invoke["versionId"])
	assert.Equal(t, float64(120), invoke["timeout"])
	assert.Equal(t, float64(10), invoke["pollDurationSeconds"])
	requestBody, ok := invoke["requestBody"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "Hello, World! This is a test request.", requestBody["input"])
	parameters, ok := requestBody["parameters"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, 0.7, parameters["temperature"])
	assert.Equal(t, float64(100), parameters["max_tokens"])
	assert.Equal(t, 0.9, parameters["top_p"])
	modelConfig, ok := requestBody["model_config"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, float64(1), modelConfig["batch_size"])
	assert.Equal(t, false, modelConfig["return_logprobs"])

	for _, tc := range []struct {
		filename             string
		deleteDeploymentOnly bool
	}{
		{filename: "delete-function.json", deleteDeploymentOnly: false},
		{filename: "delete-deployment.json", deleteDeploymentOnly: true},
	} {
		deletion := readJSON(tc.filename)
		assert.Equal(t, functionID, deletion["functionId"])
		assert.Equal(t, versionID, deletion["versionId"])
		assert.Equal(t, true, deletion["graceful"])
		assert.Equal(t, tc.deleteDeploymentOnly, deletion["deleteDeploymentOnly"])
	}

	readme, err := os.ReadFile(filepath.Join(demoDir, "README.md"))
	require.NoError(t, err)
	assert.Contains(t, string(readme), functionName)
	for _, filename := range []string{"deploy.json", "invoke.json", "delete-deployment.json", "delete-function.json"} {
		assert.Contains(t, string(readme), filename)
	}
}

func TestGenerateDemoFolderFilesystemErrors(t *testing.T) {
	t.Run("mkdir", func(t *testing.T) {
		t.Chdir(t.TempDir())
		const versionID = "version-test-id"
		demoDir := versionID + "_demo"
		require.NoError(t, os.WriteFile(demoDir, []byte("file blocks directory creation"), 0644))

		err := generateDemoFolder("function-test-id", versionID, &CreateConfig{Name: "demo-function"})
		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to create demo folder")
	})

	for _, tc := range []struct {
		filename string
	}{
		{filename: "deploy.json"},
		{filename: "invoke.json"},
		{filename: "delete-function.json"},
		{filename: "delete-deployment.json"},
		{filename: "README.md"},
	} {
		t.Run(tc.filename, func(t *testing.T) {
			t.Chdir(t.TempDir())
			const versionID = "version-test-id"
			demoDir := versionID + "_demo"
			require.NoError(t, os.Mkdir(demoDir, 0755))
			target := filepath.Join(demoDir, tc.filename)
			require.NoError(t, os.Mkdir(target, 0755))

			err := generateDemoFolder("function-test-id", versionID, &CreateConfig{Name: "demo-function"})
			require.Error(t, err)
			wantError := "failed to write file " + target
			if tc.filename == "README.md" {
				wantError = "failed to write README.md"
			}
			assert.ErrorContains(t, err, wantError)
			assert.ErrorContains(t, err, target)
		})
	}
}

func TestFunctionCreateJSONOutputFromInputFile(t *testing.T) {
	output, err, testDir := runFunctionCreateWithResponse(t, `{
		"function": {
			"id": "function-test",
			"versionId": "version-test",
			"name": "function-task-test",
			"status": "INACTIVE"
		}
	}`, true)
	require.NoError(t, err)

	var parsed client.CreateFunctionResponse
	require.NoError(t, json.Unmarshal([]byte(output), &parsed))
	assert.Equal(t, "function-test", parsed.Function.ID)
	assert.Equal(t, "version-test", parsed.Function.VersionID)
	assert.Equal(t, "function-task-test", parsed.Function.Name)
	assert.DirExists(t, filepath.Join(testDir, "version-test_demo"))
	assert.FileExists(t, filepath.Join(testDir, "version-test_demo", "deploy.json"))
}

func TestFunctionCreateRejectsEmptyIDs(t *testing.T) {
	tests := []struct {
		name       string
		functionID string
		versionID  string
	}{
		{name: "empty function ID", functionID: "", versionID: "version-test"},
		{name: "empty version ID", functionID: "function-test", versionID: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := json.Marshal(client.CreateFunctionResponse{
				Function: client.FunctionData{
					ID:        test.functionID,
					VersionID: test.versionID,
					Name:      "function-task-test",
				},
			})
			require.NoError(t, err)

			output, runErr, _ := runFunctionCreateWithResponse(t, string(response), false)
			require.Error(t, runErr)
			assert.Contains(t, runErr.Error(), "did not include a function ID and version ID")
			assert.Empty(t, output)
			assert.False(t, HasCurrentFunction())
		})
	}
}

func TestTaskCreateRejectsEmptyID(t *testing.T) {
	output, err := runTaskCreateWithResponse(t, `{
		"task": {
			"id": "",
			"name": "task-test",
			"status": "QUEUED"
		}
	}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not include a task ID")
	assert.Empty(t, output)
	assert.False(t, HasCurrentTask())
}

func TestTaskCreatePreservesWorkflowPayload(t *testing.T) {
	output, err := runTaskCreateWithResponse(t, `{
		"task": {
			"id": "task-test",
			"name": "task-test",
			"status": "QUEUED"
		}
	}`)
	require.NoError(t, err)

	var parsed client.TaskResponse
	require.NoError(t, json.Unmarshal([]byte(output), &parsed))
	assert.Equal(t, "task-test", parsed.Task.ID)
}

func TestParseContainerEnvironment(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantKey     string
		wantValue   string
		wantErrText string
	}{
		{name: "key and value", input: "MODE=production", wantKey: "MODE", wantValue: "production"},
		{name: "value contains equals signs", input: "TOKEN=a=b=c", wantKey: "TOKEN", wantValue: "a=b=c"},
		{name: "missing equals sign", input: "INVALID", wantErrText: "INVALID"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entries, err := parseContainerEnvironment([]string{test.input})
			if test.wantErrText != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.wantErrText)
				assert.Empty(t, entries)
				return
			}

			require.NoError(t, err)
			require.Len(t, entries, 1)
			assert.Equal(t, test.wantKey, entries[0].Key)
			assert.Equal(t, test.wantValue, entries[0].Value)
		})
	}
}
