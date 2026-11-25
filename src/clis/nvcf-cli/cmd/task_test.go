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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Command Structure ------------------------------------------------------

func TestTaskCommandStructure(t *testing.T) {
	t.Run("top-level command is task", func(t *testing.T) {
		assert.Equal(t, "task", taskCmd.Use)
	})

	t.Run("registers all expected subcommands", func(t *testing.T) {
		expected := map[string]bool{
			"create":                  false,
			"list":                    false,
			"bulk":                    false,
			"get [taskId]":            false,
			"delete [taskId]":         false,
			"cancel [taskId]":         false,
			"events [taskId]":         false,
			"results [taskId]":        false,
			"update-secrets [taskId]": false,
		}
		for _, sub := range taskCmd.Commands() {
			if _, ok := expected[sub.Use]; ok {
				expected[sub.Use] = true
			}
		}
		for use, found := range expected {
			assert.Truef(t, found, "expected subcommand %q to be registered", use)
		}
	})

}

func TestTaskCreateCommandFlagSurface(t *testing.T) {
	mustHave := []string{
		"input-file", "name", "gpu", "instance-type", "backend", "clusters",
		"image", "container-args", "container-env",
		"tags", "description",
		"max-runtime", "max-queued", "termination-grace",
		"result-strategy", "results-location",
		"helm-chart",
		"logs-telemetry-id", "metrics-telemetry-id", "traces-telemetry-id",
		"models", "resources", "secrets",
	}
	for _, name := range mustHave {
		t.Run(name, func(t *testing.T) {
			assert.NotNilf(t, taskCreateCmd.Flags().Lookup(name), "task create should expose --%s", name)
		})
	}
}

func TestTaskGetCommandFlagSurface(t *testing.T) {
	assert.NotNil(t, taskGetCmd.Flags().Lookup("include-secrets"))
	assert.NotNil(t, taskGetCmd.Flags().Lookup("timeout"))
}

func TestTaskEventsCommandFlagSurface(t *testing.T) {
	assert.NotNil(t, taskEventsCmd.Flags().Lookup("limit"))
	assert.NotNil(t, taskEventsCmd.Flags().Lookup("cursor"))
	assert.NotNil(t, taskEventsCmd.Flags().Lookup("timeout"))
}

func TestTaskResultsCommandFlagSurface(t *testing.T) {
	assert.NotNil(t, taskResultsCmd.Flags().Lookup("limit"))
	assert.NotNil(t, taskResultsCmd.Flags().Lookup("cursor"))
	assert.NotNil(t, taskResultsCmd.Flags().Lookup("timeout"))
}

func TestNewTaskRequestContext(t *testing.T) {
	t.Run("rejects negative timeout", func(t *testing.T) {
		_, _, err := newTaskRequestContext(-1)
		require.Error(t, err)
	})

	t.Run("rejects timeout that overflows duration", func(t *testing.T) {
		timeoutSeconds := int64(maxTaskRequestTimeoutSeconds + 1)
		if int64(int(timeoutSeconds)) != timeoutSeconds {
			t.Skip("int cannot represent an overflowing time.Duration in seconds")
		}

		_, _, err := newTaskRequestContext(int(timeoutSeconds))
		require.Error(t, err)
	})

	t.Run("uses caller timeout", func(t *testing.T) {
		start := time.Now()
		ctx, cancel, err := newTaskRequestContext(1)
		require.NoError(t, err)
		defer cancel()

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.WithinDuration(t, start.Add(time.Second), deadline, 100*time.Millisecond)
	})

	t.Run("keeps default timeout", func(t *testing.T) {
		ctx, cancel, err := newTaskRequestContext(0)
		require.NoError(t, err)
		defer cancel()

		_, ok := ctx.Deadline()
		assert.False(t, ok)
		assert.NoError(t, ctx.Err())
		assert.NotEqual(t, context.Canceled, ctx.Err())
	})
}

func TestTaskEventsTimeoutCancelsHTTPRequest(t *testing.T) {
	cancelObserved := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/v1/nvct/tasks/task-test/events", r.URL.Path)
			select {
			case <-r.Context().Done():
				close(cancelObserved)
				return
			case <-time.After(5 * time.Second):
				t.Error("task events request was not canceled")
			}
		},
	))
	t.Cleanup(server.Close)

	oldCfgFile := cfgFile
	oldStateManager := configStateManager
	oldStateManagerKey := configStateManagerKey
	oldTaskEventsFlags := taskPaginationFlags
	timeoutFlag := taskEventsCmd.Flags().Lookup("timeout")
	require.NotNil(t, timeoutFlag)
	oldTimeoutValue := timeoutFlag.Value.String()
	oldTimeoutChanged := timeoutFlag.Changed
	t.Cleanup(func() {
		cfgFile = oldCfgFile
		configStateManager = oldStateManager
		configStateManagerKey = oldStateManagerKey
		taskPaginationFlags = oldTaskEventsFlags
		assert.NoError(t, taskEventsCmd.Flags().Set("timeout", oldTimeoutValue))
		timeoutFlag.Changed = oldTimeoutChanged
		viper.Reset()
	})

	viper.Reset()
	viper.Set("base_nvct_url", server.URL)
	viper.Set("base_grpc_url", "localhost:50051")
	viper.Set("api_key", "test-function-api-key")
	viper.Set("nvct_api_key", "test-task-api-key")
	cfgFile = filepath.Join(t.TempDir(), "workflow.yaml")
	configStateManager = nil
	configStateManagerKey = ""
	require.NoError(t, taskEventsCmd.Flags().Set("timeout", "1"))

	err := runTaskEvents(taskEventsCmd, []string{"task-test"})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-cancelObserved:
	case <-time.After(5 * time.Second):
		t.Fatal("task events request cancellation was not observed")
	}
}

func TestTaskResultsTimeoutCancelsHTTPRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/v1/nvct/tasks/task-test/results", r.URL.Path)
			select {
			case <-r.Context().Done():
				return
			case <-time.After(3 * time.Second):
				t.Error("task results request was not canceled")
			}
		},
	))
	t.Cleanup(server.Close)

	oldCfgFile := cfgFile
	oldStateManager := configStateManager
	oldStateManagerKey := configStateManagerKey
	oldTaskResultsFlags := taskResultsPaginationFlags
	t.Cleanup(func() {
		cfgFile = oldCfgFile
		configStateManager = oldStateManager
		configStateManagerKey = oldStateManagerKey
		taskResultsPaginationFlags = oldTaskResultsFlags
		viper.Reset()
	})

	viper.Reset()
	viper.Set("base_nvct_url", server.URL)
	viper.Set("base_grpc_url", "localhost:50051")
	viper.Set("api_key", "test-function-api-key")
	viper.Set("nvct_api_key", "test-task-api-key")
	cfgFile = filepath.Join(t.TempDir(), "workflow.yaml")
	configStateManager = nil
	configStateManagerKey = ""
	taskResultsPaginationFlags.timeoutSeconds = 1

	start := time.Now()
	err := runTaskResults(taskResultsCmd, []string{"task-test"})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 2*time.Second)
}

func TestTaskUpdateSecretsInputAndRequest(t *testing.T) {
	var requestMu sync.Mutex
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestMu.Lock()
		requestCount++
		requestMu.Unlock()
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/v1/nvct/secrets/tasks/task-test", r.URL.Path)
		var body struct {
			Secrets []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"secrets"`
		}
		if assert.NoError(t, json.NewDecoder(r.Body).Decode(&body)) {
			assert.Equal(t, []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			}{{Name: "NGC_API_KEY", Value: "nvapi-test"}}, body.Secrets)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	oldFlags := taskUpdateSecretsFlags
	oldCfgFile := cfgFile
	oldStateManager := configStateManager
	oldStateManagerKey := configStateManagerKey
	oldViperSettings := viper.GetViper().AllSettings()
	t.Cleanup(func() {
		taskUpdateSecretsFlags = oldFlags
		cfgFile = oldCfgFile
		configStateManager = oldStateManager
		configStateManagerKey = oldStateManagerKey
		viper.Reset()
		for key, value := range oldViperSettings {
			viper.Set(key, value)
		}
	})

	cfgFile = filepath.Join(t.TempDir(), "workflow.yaml")
	configStateManager = nil
	configStateManagerKey = ""

	tests := []struct {
		name       string
		inputFile  string
		inputJSON  string
		cliSecrets []string
		wantError  string
		wantCalls  int
	}{
		{name: "unreadable input file", inputFile: filepath.Join(t.TempDir(), "missing.json"), wantError: "failed to read input file", wantCalls: 0},
		{name: "malformed JSON", inputFile: filepath.Join(t.TempDir(), "malformed.json"), inputJSON: "{", wantError: "failed to parse JSON file", wantCalls: 0},
		{name: "empty secrets payload", inputFile: filepath.Join(t.TempDir(), "empty.json"), inputJSON: `{"secrets":[]}`, wantError: "at least one secret must be provided", wantCalls: 0},
		{name: "invalid CLI secret pair", cliSecrets: []string{"missing-value"}, wantError: `invalid secret format "missing-value": must be name=value`, wantCalls: 0},
		{name: "valid JSON secrets", inputFile: filepath.Join(t.TempDir(), "secrets.json"), inputJSON: `{"secrets":[{"name":"NGC_API_KEY","value":"nvapi-test"}]}`, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.inputJSON != "" {
				require.NoError(t, os.WriteFile(tt.inputFile, []byte(tt.inputJSON), 0600))
			}
			taskUpdateSecretsFlags = struct {
				inputFile string
				secrets   []string
			}{inputFile: tt.inputFile, secrets: tt.cliSecrets}
			viper.Reset()
			viper.Set("base_nvct_url", server.URL)
			viper.Set("base_grpc_url", "localhost:50051")
			viper.Set("api_key", "test-function-api-key")
			viper.Set("nvct_api_key", "test-task-api-key")

			requestMu.Lock()
			before := requestCount
			requestMu.Unlock()
			err := runTaskUpdateSecrets(taskUpdateSecretsCmd, []string{"task-test"})
			if tt.wantError != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantError)
			} else {
				require.NoError(t, err)
			}
			requestMu.Lock()
			gotCalls := requestCount - before
			requestMu.Unlock()
			assert.Equal(t, tt.wantCalls, gotCalls)
		})
	}
}

func TestTaskGetTimeoutCancelsHTTPRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/v1/nvct/tasks/task-test", r.URL.Path)
			select {
			case <-r.Context().Done():
				return
			case <-time.After(3 * time.Second):
				t.Error("task get request was not canceled")
			}
		},
	))
	t.Cleanup(server.Close)

	oldCfgFile := cfgFile
	oldStateManager := configStateManager
	oldStateManagerKey := configStateManagerKey
	oldTaskGetFlags := taskGetFlags
	t.Cleanup(func() {
		cfgFile = oldCfgFile
		configStateManager = oldStateManager
		configStateManagerKey = oldStateManagerKey
		taskGetFlags = oldTaskGetFlags
		viper.Reset()
	})

	viper.Reset()
	viper.Set("base_nvct_url", server.URL)
	viper.Set("base_grpc_url", "localhost:50051")
	viper.Set("api_key", "test-function-api-key")
	viper.Set("nvct_api_key", "test-task-api-key")
	cfgFile = filepath.Join(t.TempDir(), "workflow.yaml")
	configStateManager = nil
	configStateManagerKey = ""
	taskGetFlags.timeoutSeconds = 1

	start := time.Now()
	err := runTaskGet(taskGetCmd, []string{"task-test"})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 2*time.Second)
}

func TestRunTaskListFormatsTasksAndPagination(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/nvct/tasks", r.URL.Path)
		assert.Equal(t, "12", r.URL.Query().Get("limit"))
		assert.Equal(t, "RUNNING", r.URL.Query().Get("status"))
		assert.Equal(t, "previous-page", r.URL.Query().Get("cursor"))
		assert.Equal(t, "Bearer test-task-api-key", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tasks":[{"id":"task-with-details","name":"detailed-job","status":"RUNNING","gpuSpecification":{"gpu":"H100","instanceType":"GPU.H100_1x"},"createdAt":"2026-01-02T03:04:05Z","percentComplete":65},{"id":"task-without-details","name":"minimal-job","status":"QUEUED"}],"cursor":"next-page","limit":12}`))
	}))
	t.Cleanup(server.Close)

	oldCfgFile := cfgFile
	oldStateManager := configStateManager
	oldStateManagerKey := configStateManagerKey
	oldTaskListFlags := taskListFlags
	oldJSONOutput := jsonOutput
	oldStdout := os.Stdout
	t.Cleanup(func() {
		cfgFile = oldCfgFile
		configStateManager = oldStateManager
		configStateManagerKey = oldStateManagerKey
		taskListFlags = oldTaskListFlags
		jsonOutput = oldJSONOutput
		os.Stdout = oldStdout
		viper.Reset()
	})

	viper.Reset()
	viper.Set("base_nvct_url", server.URL)
	viper.Set("base_grpc_url", "localhost:50051")
	viper.Set("api_key", "test-function-api-key")
	viper.Set("nvct_api_key", "test-task-api-key")
	cfgFile = filepath.Join(t.TempDir(), "workflow.yaml")
	configStateManager = nil
	configStateManagerKey = ""
	taskListFlags = struct {
		limit  int
		status string
		cursor string
	}{limit: 12, status: "RUNNING", cursor: "previous-page"}
	jsonOutput = false

	readEnd, writeEnd, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writeEnd
	err = runTaskList(taskListCmd, nil)
	closeErr := writeEnd.Close()
	os.Stdout = oldStdout
	output, readErr := io.ReadAll(readEnd)
	readEnd.Close()
	require.NoError(t, err)
	require.NoError(t, closeErr)
	require.NoError(t, readErr)

	text := string(bytes.TrimSpace(output))
	assert.Contains(t, text, "Listing tasks...")
	assert.Contains(t, text, "Found 2 tasks:")
	assert.Contains(t, text, "ID: task-with-details")
	assert.Contains(t, text, "Name: detailed-job")
	assert.Contains(t, text, "Status: RUNNING")
	assert.Contains(t, text, "GPU: H100 (GPU.H100_1x)")
	assert.Contains(t, text, "Created: 2026-01-02T03:04:05Z")
	assert.Contains(t, text, "Progress: 65%")
	assert.Contains(t, text, "ID: task-without-details")
	assert.Contains(t, text, "Name: minimal-job")
	assert.Contains(t, text, "Status: QUEUED")
	secondTaskOutput := strings.SplitN(text, "ID: task-without-details", 2)
	require.Len(t, secondTaskOutput, 2)
	assert.NotContains(t, secondTaskOutput[1], "GPU: ")
	assert.NotContains(t, secondTaskOutput[1], "Created: ")
	assert.NotContains(t, secondTaskOutput[1], "Progress: ")
	assert.Contains(t, text, "Next cursor: next-page (limit=12)")
}

func TestRunTaskGetFormatsDetailedTask(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/nvct/tasks/task-get-details", r.URL.Path)
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "Bearer test-task-api-key", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"task":{"id":"task-get-details","ncaId":"nca-123","name":"detailed-task","status":"RUNNING","percentComplete":73,"createdAt":"2026-02-03T04:05:06Z","lastUpdatedAt":"2026-02-03T04:06:06Z","lastHeartbeatAt":"2026-02-03T04:07:06Z","description":"task description","gpuSpecification":{"gpu":"H100","instanceType":"GPU.H100_1x","backend":"kubernetes","clusters":["cluster-a","cluster-b"]},"containerImage":"registry.example/task:latest","containerArgs":"--serve","containerEnvironment":[{"key":"MODE","value":"production"}],"tags":["daily","priority"],"secrets":["API_TOKEN","DB_PASSWORD"],"resultHandlingStrategy":"UPLOAD","resultsLocation":"s3://results/task","telemetries":{"logsTelemetryId":"logs-123","metricsTelemetryId":"metrics-456","tracesTelemetryId":"traces-789"},"instances":[{"instanceId":"instance-123","instanceState":"READY","instanceType":"GPU.H100_1x","backend":"kubernetes","location":"us-central1"}]}}`))
	}))
	t.Cleanup(server.Close)

	oldCfgFile := cfgFile
	oldStateManager := configStateManager
	oldStateManagerKey := configStateManagerKey
	oldTaskGetFlags := taskGetFlags
	oldJSONOutput := jsonOutput
	oldStdout := os.Stdout
	t.Cleanup(func() {
		cfgFile = oldCfgFile
		configStateManager = oldStateManager
		configStateManagerKey = oldStateManagerKey
		taskGetFlags = oldTaskGetFlags
		jsonOutput = oldJSONOutput
		os.Stdout = oldStdout
		viper.Reset()
	})

	viper.Reset()
	viper.Set("base_nvct_url", server.URL)
	viper.Set("base_grpc_url", "localhost:50051")
	viper.Set("api_key", "test-function-api-key")
	viper.Set("nvct_api_key", "test-task-api-key")
	cfgFile = filepath.Join(t.TempDir(), "workflow.yaml")
	configStateManager = nil
	configStateManagerKey = ""
	taskGetFlags = struct {
		includeSecrets bool
		timeoutSeconds int
	}{includeSecrets: false}
	jsonOutput = false

	readEnd, writeEnd, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writeEnd
	err = runTaskGet(taskGetCmd, []string{"task-get-details"})
	closeErr := writeEnd.Close()
	os.Stdout = oldStdout
	output, readErr := io.ReadAll(readEnd)
	readEnd.Close()
	require.NoError(t, err)
	require.NoError(t, closeErr)
	require.NoError(t, readErr)

	text := string(bytes.TrimSpace(output))
	for _, expected := range []string{
		"Task Details:", "ID: task-get-details", "Name: detailed-task", "NCA ID: nca-123", "Status: RUNNING",
		"Progress: 73%", "Created: 2026-02-03T04:05:06Z", "Last Updated: 2026-02-03T04:06:06Z",
		"Last Heartbeat: 2026-02-03T04:07:06Z", "Description: task description", "GPU: H100",
		"Instance Type: GPU.H100_1x", "Backend: kubernetes", "Clusters: [cluster-a cluster-b]",
		"Image: registry.example/task:latest", "Args: --serve", "Env: MODE=production", "Tags: daily, priority",
		"Secret keys: API_TOKEN, DB_PASSWORD", "Result Strategy: UPLOAD", "Results Location: s3://results/task",
		"logs-123", "metrics-456", "traces-789", "Instances (1):", "instance-123 [READY] GPU.H100_1x @ kubernetes/us-central1",
	} {
		assert.Contains(t, text, expected)
	}
	assert.NotContains(t, text, "supersecret")
	assert.NotContains(t, text, "test-task-api-key")
	assert.NotContains(t, text, "test-function-api-key")
}

// --- Helpers ----------------------------------------------------------------

func TestParseSecretsListCLI(t *testing.T) {
	secrets, err := parseSecretsList(nil, []string{"FOO=bar", "QUX=quux"})
	require.NoError(t, err)
	require.Len(t, secrets, 2)
	assert.Equal(t, "FOO", secrets[0].Name)
	assert.Equal(t, "bar", secrets[0].Value)
	assert.Equal(t, "QUX", secrets[1].Name)
	assert.Equal(t, "quux", secrets[1].Value)
}

func TestParseSecretsListInvalid(t *testing.T) {
	_, err := parseSecretsList(nil, []string{"missing-equals"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be name=value")
}

func TestParseSecretsListJSON(t *testing.T) {
	// Mimic the shape we'd get from json.Unmarshal into interface{}.
	raw := []interface{}{
		"FOO=bar",
		map[string]interface{}{"name": "API_KEY", "value": "abc"},
		map[string]interface{}{"name": "STRUCT_VAL", "value": map[string]interface{}{"nested": true}},
	}
	secrets, err := parseSecretsList(raw, nil)
	require.NoError(t, err)
	require.Len(t, secrets, 3)
	assert.Equal(t, "FOO", secrets[0].Name)
	assert.Equal(t, "bar", secrets[0].Value)
	assert.Equal(t, "API_KEY", secrets[1].Name)
	assert.Equal(t, "abc", secrets[1].Value)
	assert.Equal(t, "STRUCT_VAL", secrets[2].Name)
	nested, ok := secrets[2].Value.(map[string]interface{})
	require.True(t, ok, "expected map secret value")
	assert.Equal(t, true, nested["nested"])
}

func TestParseSecretsListSecretConfigSlice(t *testing.T) {
	raw := []SecretConfig{
		{Name: "A", Value: "alpha"},
		{Name: "B", Value: 42},
	}
	secrets, err := parseSecretsList(raw, nil)
	require.NoError(t, err)
	require.Len(t, secrets, 2)
	assert.Equal(t, "A", secrets[0].Name)
	assert.Equal(t, "alpha", secrets[0].Value)
	assert.Equal(t, "B", secrets[1].Name)
	assert.Equal(t, 42, secrets[1].Value)
}

func TestParseArtifactsList(t *testing.T) {
	artifacts, err := parseArtifactsList(
		[]ArtifactConfig{{Name: "model-a", Version: "1.0", URI: "s3://bucket/a"}},
		[]string{"model-b:2.0:s3://bucket/b"},
	)
	require.NoError(t, err)
	require.Len(t, artifacts, 2)
	assert.Equal(t, "model-a", artifacts[0].Name)
	assert.Equal(t, "1.0", artifacts[0].Version)
	assert.Equal(t, "s3://bucket/a", artifacts[0].URI)
	assert.Equal(t, "model-b", artifacts[1].Name)
	assert.Equal(t, "2.0", artifacts[1].Version)
	assert.Equal(t, "s3://bucket/b", artifacts[1].URI)
}

func TestParseArtifactsListInvalid(t *testing.T) {
	_, err := parseArtifactsList(nil, []string{"too:few"})
	require.Error(t, err)
}

// --- resolveTaskID ---------------------------------------------------------

func TestResolveTaskIDFromArgs(t *testing.T) {
	got, err := resolveTaskID([]string{"explicit-id"})
	require.NoError(t, err)
	assert.Equal(t, "explicit-id", got)
}

func TestResolveTaskIDFromEmptyArgsNoState(t *testing.T) {
	// Save and restore default state so we don't affect other tests/users.
	original := *GetStateManagerForCurrentCommand().GetState()
	GetStateManagerForCurrentCommand().ClearTask()
	t.Cleanup(func() { *GetStateManagerForCurrentCommand().GetState() = original })

	_, err := resolveTaskID(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "task ID is required")
}

func TestResolveTaskIDFromState(t *testing.T) {
	sm := GetStateManagerForCurrentCommand()
	original := *sm.GetState()
	sm.SetTask("state-task", "saved-task")
	t.Cleanup(func() { *sm.GetState() = original })

	got, err := resolveTaskID(nil)
	require.NoError(t, err)
	assert.Equal(t, "state-task", got)
}

func TestLoadTaskCreateConfigCLIOverrides(t *testing.T) {
	originalFlags := taskCreateFlags
	t.Cleanup(func() { taskCreateFlags = originalFlags })

	newCommand := func() *cobra.Command {
		cmd := &cobra.Command{}
		cmd.Flags().StringVar(&taskCreateFlags.inputFile, "input-file", "", "")
		cmd.Flags().StringVar(&taskCreateFlags.gpu, "gpu", "", "")
		cmd.Flags().StringVar(&taskCreateFlags.logsTelemetryId, "logs-telemetry-id", "", "")
		cmd.Flags().StringSliceVar(&taskCreateFlags.containerEnvironment, "container-env", nil, "")
		t.Cleanup(func() {
			for _, name := range []string{"input-file", "gpu", "logs-telemetry-id", "container-env"} {
				cmd.Flags().Lookup(name).Changed = false
			}
		})
		return cmd
	}

	t.Run("CLI overrides file values and initializes telemetries", func(t *testing.T) {
		configPath := filepath.Join(t.TempDir(), "task.json")
		err := os.WriteFile(configPath, []byte(`{"gpuSpecification":{"gpu":"file-gpu","instanceType":"GPU.H100_1x"},"telemetries":{"metricsTelemetryId":"file-metrics"}}`), 0o600)
		require.NoError(t, err)

		cmd := newCommand()
		taskCreateFlags.inputFile = configPath
		taskCreateFlags.gpu = "cli-gpu"
		taskCreateFlags.logsTelemetryId = "cli-logs"
		for _, name := range []string{"input-file", "gpu", "logs-telemetry-id"} {
			cmd.Flags().Lookup(name).Changed = true
		}

		cfg, err := loadTaskCreateConfig(cmd)
		require.NoError(t, err)
		require.NotNil(t, cfg.GpuSpecification)
		assert.Equal(t, "cli-gpu", cfg.GpuSpecification.GPU)
		assert.Equal(t, "GPU.H100_1x", cfg.GpuSpecification.InstanceType)
		require.NotNil(t, cfg.Telemetries)
		assert.Equal(t, "cli-logs", cfg.Telemetries.LogsTelemetryId)
		assert.Equal(t, "file-metrics", cfg.Telemetries.MetricsTelemetryId)
	})

	t.Run("rejects malformed container environment", func(t *testing.T) {
		cmd := newCommand()
		taskCreateFlags.containerEnvironment = []string{"MALFORMED"}
		cmd.Flags().Lookup("container-env").Changed = true

		_, err := loadTaskCreateConfig(cmd)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected key=value")
	})
}

func TestLoadTaskCreateConfigOverridesAndErrors(t *testing.T) {
	originalFlags := taskCreateFlags
	t.Cleanup(func() { taskCreateFlags = originalFlags })

	tests := []struct {
		name      string
		input     string
		flags     map[string]string
		want      *TaskCreateConfig
		wantError string
	}{
		{
			name:  "overrides selected scalar and GPU fields",
			input: `{"name":"file-name","gpuSpecification":{"gpu":"file-gpu","instanceType":"file-instance","backend":"file-backend","clusters":["file-cluster"]},"containerImage":"file-image","containerArgs":"file-args","containerEnvironment":[{"key":"FILE","value":"yes"}],"tags":["file-tag"],"description":"file-description","maxRuntimeDuration":"PT1H","maxQueuedDuration":"PT2H","terminationGracePeriodDuration":"PT3M","resultHandlingStrategy":"UPLOAD","resultsLocation":"file/location","helmChart":"file-chart"}`,
			flags: map[string]string{"name": "cli-name", "instance-type": "cli-instance", "backend": "cli-backend", "clusters": "cli-a,cli-b", "image": "cli-image", "container-args": "cli-args", "container-env": "CLI=enabled", "tags": "cli-tag", "description": "cli-description", "max-runtime": "PT4H", "max-queued": "PT5H", "termination-grace": "PT6M", "result-strategy": "NONE", "results-location": "cli/location", "helm-chart": "cli-chart"},
			want:  &TaskCreateConfig{Name: "cli-name", GpuSpecification: &TaskGpuSpecificationInput{GPU: "file-gpu", InstanceType: "cli-instance", Backend: "cli-backend", Clusters: []string{"cli-a", "cli-b"}}, ContainerImage: "cli-image", ContainerArgs: "cli-args", ContainerEnvironment: []ContainerEnvironmentEntry{{Key: "CLI", Value: "enabled"}}, Tags: []string{"cli-tag"}, Description: "cli-description", MaxRuntimeDuration: "PT4H", MaxQueuedDuration: "PT5H", TerminationGracePeriodDuration: "PT6M", ResultHandlingStrategy: "NONE", ResultsLocation: "cli/location", HelmChart: "cli-chart"},
		},
		{
			name:  "overrides every telemetry ID and preserves other values",
			input: `{"name":"file-name","telemetries":{"logsTelemetryId":"old-logs","metricsTelemetryId":"old-metrics","tracesTelemetryId":"old-traces"}}`,
			flags: map[string]string{"logs-telemetry-id": "new-logs", "metrics-telemetry-id": "new-metrics", "traces-telemetry-id": "new-traces"},
			want:  &TaskCreateConfig{Name: "file-name", Telemetries: &TaskTelemetriesInput{LogsTelemetryId: "new-logs", MetricsTelemetryId: "new-metrics", TracesTelemetryId: "new-traces"}},
		},
		{
			name:  "parses model and resource overrides",
			input: `{"models":[{"name":"old-model","version":"1","uri":"s3://old"}],"resources":[{"name":"old-resource","version":"1","uri":"s3://old-resource"}]}`,
			flags: map[string]string{"models": "new-model:2:s3://model", "resources": "new-resource:3:s3://resource"},
			want:  &TaskCreateConfig{Models: []ArtifactConfig{{Name: "new-model", Version: "2", URI: "s3://model"}}, Resources: []ArtifactConfig{{Name: "new-resource", Version: "3", URI: "s3://resource"}}},
		},
		{name: "rejects malformed model", flags: map[string]string{"models": "bad"}, wantError: `invalid model "bad"`},
		{name: "rejects malformed resource", flags: map[string]string{"resources": "bad"}, wantError: `invalid resource "bad"`},
		{name: "reports missing input file", input: "missing", wantError: "failed to read input file"},
		{name: "reports malformed JSON input", input: "{", wantError: "failed to parse JSON file"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			taskCreateFlags = struct {
				inputFile                      string
				name                           string
				gpu                            string
				instanceType                   string
				backend                        string
				clusters                       []string
				containerImage                 string
				containerArgs                  string
				containerEnvironment           []string
				tags                           []string
				description                    string
				maxRuntimeDuration             string
				maxQueuedDuration              string
				terminationGracePeriodDuration string
				resultHandlingStrategy         string
				resultsLocation                string
				helmChart                      string
				logsTelemetryId                string
				metricsTelemetryId             string
				tracesTelemetryId              string
				models                         []string
				resources                      []string
				secrets                        []string
			}{}
			cmd := &cobra.Command{}
			cmd.Flags().StringVar(&taskCreateFlags.inputFile, "input-file", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.name, "name", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.instanceType, "instance-type", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.backend, "backend", "", "")
			cmd.Flags().StringSliceVar(&taskCreateFlags.clusters, "clusters", nil, "")
			cmd.Flags().StringVar(&taskCreateFlags.containerImage, "image", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.containerArgs, "container-args", "", "")
			cmd.Flags().StringSliceVar(&taskCreateFlags.containerEnvironment, "container-env", nil, "")
			cmd.Flags().StringSliceVar(&taskCreateFlags.tags, "tags", nil, "")
			cmd.Flags().StringVar(&taskCreateFlags.description, "description", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.maxRuntimeDuration, "max-runtime", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.maxQueuedDuration, "max-queued", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.terminationGracePeriodDuration, "termination-grace", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.resultHandlingStrategy, "result-strategy", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.resultsLocation, "results-location", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.helmChart, "helm-chart", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.logsTelemetryId, "logs-telemetry-id", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.metricsTelemetryId, "metrics-telemetry-id", "", "")
			cmd.Flags().StringVar(&taskCreateFlags.tracesTelemetryId, "traces-telemetry-id", "", "")
			cmd.Flags().StringSliceVar(&taskCreateFlags.models, "models", nil, "")
			cmd.Flags().StringSliceVar(&taskCreateFlags.resources, "resources", nil, "")

			inputPath := tt.input
			if tt.input == "missing" {
				inputPath = filepath.Join(t.TempDir(), "missing.json")
			} else if tt.input != "" {
				inputPath = filepath.Join(t.TempDir(), "task.json")
				require.NoError(t, os.WriteFile(inputPath, []byte(tt.input), 0o600))
			}
			if inputPath != "" {
				taskCreateFlags.inputFile = inputPath
				cmd.Flags().Lookup("input-file").Changed = true
			}
			for name, value := range tt.flags {
				require.NoError(t, cmd.Flags().Set(name, value))
			}

			got, err := loadTaskCreateConfig(cmd)
			if tt.wantError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantError)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRunTaskBulkInputValidation(t *testing.T) {
	oldFlags := taskBulkFlags
	t.Cleanup(func() { taskBulkFlags = oldFlags })

	tempDir := t.TempDir()
	emptyIDsFile := filepath.Join(tempDir, "empty-task-ids.json")
	require.NoError(t, os.WriteFile(emptyIDsFile, []byte(`{"taskIds":[]}`), 0o600))
	malformedFile := filepath.Join(tempDir, "malformed.json")
	require.NoError(t, os.WriteFile(malformedFile, []byte("{"), 0o600))

	tests := []struct {
		name      string
		inputFile string
		taskIDs   []string
		wantError string
	}{
		{name: "missing input file", inputFile: filepath.Join(tempDir, "missing.json"), wantError: "failed to read input file"},
		{name: "malformed input JSON", inputFile: malformedFile, wantError: "failed to parse JSON file"},
		{name: "no IDs supplied", wantError: "at least one task ID is required"},
		{name: "input file has no IDs", inputFile: emptyIDsFile, wantError: "at least one task ID is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			taskBulkFlags = struct {
				inputFile string
				taskIDs   []string
			}{inputFile: tt.inputFile, taskIDs: tt.taskIDs}

			err := runTaskBulk(taskBulkCmd, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantError)
		})
	}
}
