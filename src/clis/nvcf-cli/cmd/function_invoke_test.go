/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestInvokeConfigParsesVanityGatewayFromJSON(t *testing.T) {
	t.Parallel()

	rawJSON := `{
		"vanityHost": "vanity.localhost",
		"path": "/bdd/echo",
		"requestBody": {"message": "test"}
	}`

	var config InvokeConfig
	if err := json.Unmarshal([]byte(rawJSON), &config); err != nil {
		t.Fatalf("unmarshal invoke config: %v", err)
	}

	if config.VanityHost != "vanity.localhost" {
		t.Fatalf("vanityHost = %q, want %q", config.VanityHost, "vanity.localhost")
	}
	if config.Path != "/bdd/echo" {
		t.Fatalf("path = %q, want %q", config.Path, "/bdd/echo")
	}
}

func TestValidateInvokeConfigVanityGateway(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      *InvokeConfig
		useGRPC     bool
		wantErrText string
	}{
		{
			name: "valid vanity gateway config with path",
			config: &InvokeConfig{
				VanityHost:  "vanity.localhost",
				Path:        "/bdd/echo",
				RequestBody: map[string]interface{}{"message": "test"},
			},
			wantErrText: "",
		},
		{
			name: "vanity gateway with grpc fails",
			config: &InvokeConfig{
				VanityHost:  "vanity.localhost",
				Path:        "/bdd/echo",
				RequestBody: map[string]interface{}{"message": "test"},
			},
			useGRPC:     true,
			wantErrText: "--vanity-host is not supported with --grpc",
		},
		{
			name: "valid vanity gateway config with inference-url fallback",
			config: &InvokeConfig{
				VanityHost:   "llama.api.myorg.com",
				InferenceURL: "/v1/chat/completions",
				RequestBody:  map[string]interface{}{"model": "llama-3"},
			},
			wantErrText: "",
		},
		{
			name: "vanity gateway without path or inference-url fails",
			config: &InvokeConfig{
				VanityHost:  "vanity.localhost",
				RequestBody: map[string]interface{}{"message": "test"},
			},
			wantErrText: "path (or --inference-url) is required when using --vanity-host",
		},
		{
			name: "vanity gateway without request body fails",
			config: &InvokeConfig{
				VanityHost: "vanity.localhost",
				Path:       "/bdd/echo",
			},
			wantErrText: "request body is required",
		},
		{
			name: "path without vanity-host fails",
			config: &InvokeConfig{
				FunctionID:  "func-123",
				VersionID:   "ver-123",
				Path:        "/bdd/echo",
				RequestBody: map[string]interface{}{"message": "test"},
			},
			wantErrText: "--path is only supported with --vanity-host",
		},
		{
			name: "standard invoke requires function ID",
			config: &InvokeConfig{
				VersionID:   "ver-123",
				RequestBody: map[string]interface{}{"message": "test"},
			},
			wantErrText: "function ID is required",
		},
		{
			name: "standard invoke requires version ID",
			config: &InvokeConfig{
				FunctionID:  "func-123",
				RequestBody: map[string]interface{}{"message": "test"},
			},
			wantErrText: "version ID is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateInvokeConfig(tt.config, tt.useGRPC)
			if tt.wantErrText == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErrText) {
				t.Fatalf("validateInvokeConfig() error = %v, want containing %q", err, tt.wantErrText)
			}
		})
	}
}

func TestInvokeOptionsFromConfigVanityGateway(t *testing.T) {
	t.Parallel()

	config := &InvokeConfig{
		VanityHost:          "vanity.localhost",
		Path:                "/bdd/echo",
		PollDurationSeconds: 15,
	}

	opts := invokeOptionsFromConfig(config)
	if opts == nil {
		t.Fatal("expected invokeOptionsFromConfig to return non-nil options")
	}
	if opts.VanityHost != "vanity.localhost" {
		t.Fatalf("opts.VanityHost = %q, want %q", opts.VanityHost, "vanity.localhost")
	}
	if opts.Path != "/bdd/echo" {
		t.Fatalf("opts.Path = %q, want %q", opts.Path, "/bdd/echo")
	}
	// InferenceURL must stay scoped to config.InferenceURL: the client's
	// Vanity Gateway branch falls back from Path to InferenceURL on its own,
	// and letting --path leak into InferenceURL here would let a plain REST
	// invocation (no --vanity-host) silently route to --path instead of the
	// function's configured endpoint.
	if opts.InferenceURL != "" {
		t.Fatalf("opts.InferenceURL = %q, want empty (Path must not populate InferenceURL)", opts.InferenceURL)
	}
	if opts.PollDurationSeconds != 15 {
		t.Fatalf("opts.PollDurationSeconds = %d, want 15", opts.PollDurationSeconds)
	}
}

func TestInvokeOptionsFromConfigDoesNotLeakPathIntoInferenceURLForStandardInvoke(t *testing.T) {
	t.Parallel()

	// Regression test: --path must never override the function's inference
	// URL for a standard (non-Vanity-Gateway) invocation.
	config := &InvokeConfig{
		FunctionID:   "func-123",
		VersionID:    "ver-456",
		InferenceURL: "/v1/chat/completions",
	}

	opts := invokeOptionsFromConfig(config)
	if opts == nil {
		t.Fatal("expected invokeOptionsFromConfig to return non-nil options")
	}
	if opts.InferenceURL != "/v1/chat/completions" {
		t.Fatalf("opts.InferenceURL = %q, want %q", opts.InferenceURL, "/v1/chat/completions")
	}
}

func TestLoadInvokeConfigVanityGatewayFlags(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&invokeFlags.vanityHost, "vanity-host", "", "")
	cmd.Flags().StringVar(&invokeFlags.path, "path", "", "")
	cmd.Flags().StringVar(&invokeFlags.requestBody, "request-body", "", "")

	args := []string{"--vanity-host", "vanity.localhost", "--path", "/bdd/echo", "--request-body", `{"key":"val"}`}
	if err := cmd.ParseFlags(args); err != nil {
		t.Fatalf("ParseFlags failed: %v", err)
	}

	config, err := loadInvokeConfig(cmd)
	if err != nil {
		t.Fatalf("loadInvokeConfig failed: %v", err)
	}
	if config.VanityHost != "vanity.localhost" {
		t.Fatalf("config.VanityHost = %q, want %q", config.VanityHost, "vanity.localhost")
	}
	if config.Path != "/bdd/echo" {
		t.Fatalf("config.Path = %q, want %q", config.Path, "/bdd/echo")
	}
}

func TestLoadInvokeConfigFileFlagsAndErrors(t *testing.T) {
	previousFlags := invokeFlags
	t.Cleanup(func() { invokeFlags = previousFlags })

	dir := t.TempDir()
	validFile := dir + "/invoke.json"
	malformedFile := dir + "/malformed.json"
	if err := os.WriteFile(validFile, []byte(`{"functionId":"json-function","versionId":"json-version","inferenceUrl":"/json-url","path":"/json-path","vanityHost":"json-host","modelName":"json-model","requestBody":{"source":"json"},"timeout":17,"pollDurationSeconds":9,"grpcService":"json-service","grpcMethod":"JsonMethod","grpcPlaintext":true}`), 0o600); err != nil {
		t.Fatalf("write valid fixture: %v", err)
	}
	if err := os.WriteFile(malformedFile, []byte(`{"functionId":`), 0o600); err != nil {
		t.Fatalf("write malformed fixture: %v", err)
	}

	tests := []struct {
		name      string
		inputFile string
		changed   map[string]string
		setFlags  func()
		want      *InvokeConfig
		wantErr   string
	}{
		{
			name:      "retains JSON values and defaults when flags are unchanged",
			inputFile: validFile,
			want:      &InvokeConfig{FunctionID: "json-function", VersionID: "json-version", InferenceURL: "/json-url", Path: "/json-path", VanityHost: "json-host", ModelName: "json-model", RequestBody: map[string]interface{}{"source": "json"}, Timeout: 17, PollDurationSeconds: 9, GRPCService: "json-service", GRPCMethod: "JsonMethod", GRPCPlaintext: true},
		},
		{
			name:      "changed flags override all JSON values",
			inputFile: validFile,
			changed:   map[string]string{"function-id": "flag-function", "version-id": "flag-version", "inference-url": "/flag-url", "path": "/flag-path", "vanity-host": "flag-host", "model-name": "flag-model", "request-body": `{"source":"flag"}`, "timeout": "31", "poll-duration": "12", "grpc-service": "flag-service", "grpc-method": "FlagMethod", "grpc-plaintext": "false"},
			want:      &InvokeConfig{FunctionID: "flag-function", VersionID: "flag-version", InferenceURL: "/flag-url", Path: "/flag-path", VanityHost: "flag-host", ModelName: "flag-model", RequestBody: map[string]interface{}{"source": "flag"}, Timeout: 31, PollDurationSeconds: 12, GRPCService: "flag-service", GRPCMethod: "FlagMethod"},
		},
		{
			name: "defaults without input file",
			want: &InvokeConfig{Timeout: 60, PollDurationSeconds: 5},
		},
		{name: "missing input file", inputFile: dir + "/missing.json", wantErr: "failed to read input file"},
		{name: "malformed JSON file", inputFile: malformedFile, wantErr: "failed to parse JSON file"},
		{name: "invalid request body JSON", changed: map[string]string{"request-body": "{"}, wantErr: "invalid JSON in request-body"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			invokeFlags = struct {
				inputFile           string
				functionID          string
				versionID           string
				inferenceURL        string
				path                string
				vanityHost          string
				modelName           string
				requestBody         string
				timeout             int
				pollDurationSeconds int
				useGRPC             bool
				grpcService         string
				grpcMethod          string
				grpcPlaintext       bool
			}{}
			invokeFlags.inputFile = tt.inputFile
			cmd := &cobra.Command{}
			cmd.Flags().String("function-id", "", "")
			cmd.Flags().String("version-id", "", "")
			cmd.Flags().String("inference-url", "", "")
			cmd.Flags().String("path", "", "")
			cmd.Flags().String("vanity-host", "", "")
			cmd.Flags().String("model-name", "", "")
			cmd.Flags().String("request-body", "", "")
			cmd.Flags().Int("timeout", 60, "")
			cmd.Flags().Int("poll-duration", 5, "")
			cmd.Flags().String("grpc-service", "", "")
			cmd.Flags().String("grpc-method", "", "")
			cmd.Flags().Bool("grpc-plaintext", false, "")
			for name, value := range tt.changed {
				if err := cmd.Flags().Set(name, value); err != nil {
					t.Fatalf("set --%s: %v", name, err)
				}
				switch name {
				case "function-id":
					invokeFlags.functionID = value
				case "version-id":
					invokeFlags.versionID = value
				case "inference-url":
					invokeFlags.inferenceURL = value
				case "path":
					invokeFlags.path = value
				case "vanity-host":
					invokeFlags.vanityHost = value
				case "model-name":
					invokeFlags.modelName = value
				case "request-body":
					invokeFlags.requestBody = value
				case "timeout":
					invokeFlags.timeout = 31
				case "poll-duration":
					invokeFlags.pollDurationSeconds = 12
				case "grpc-service":
					invokeFlags.grpcService = value
				case "grpc-method":
					invokeFlags.grpcMethod = value
				case "grpc-plaintext":
					invokeFlags.grpcPlaintext = false
				}
			}
			config, err := loadInvokeConfig(cmd)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
				}
				if config != nil {
					t.Fatalf("config = %#v, want nil on error", config)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadInvokeConfig: %v", err)
			}
			if !reflect.DeepEqual(config, tt.want) {
				t.Fatalf("config = %#v, want %#v", config, tt.want)
			}
		})
	}
}
