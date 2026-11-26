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
		filename            string
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
