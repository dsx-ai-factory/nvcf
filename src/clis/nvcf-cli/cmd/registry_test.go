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
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"nvcf-cli/internal/client"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Command Structure ---
// Guards against accidental rename/regression of the `registry-credential`
// command group and its subcommands (NVCF-10082 renamed `registry` ->
// `registry-credential`).

// TestRegistryCredentialCommandStructure verifies the registry-credential command structure, subcommands, and flags.
func TestRegistryCredentialCommandStructure(t *testing.T) {
	t.Run("top-level command uses registry-credential", func(t *testing.T) {
		assert.Equal(t, "registry-credential", registryCmd.Use)
	})

	t.Run("registers all expected subcommands", func(t *testing.T) {
		expected := map[string]bool{
			"list":                   false,
			"add":                    false,
			"get <credential-id>":    false,
			"update <credential-id>": false,
			"delete <credential-id>": false,
			"list-recognized":        false,
		}
		for _, sub := range registryCmd.Commands() {
			if _, ok := expected[sub.Use]; ok {
				expected[sub.Use] = true
			}
		}
		for use, found := range expected {
			assert.Truef(t, found, "expected subcommand %q to be registered", use)
		}
	})

	t.Run("add command marks hostname and artifact-type required", func(t *testing.T) {
		annotations := registryAddCmd.Flag("hostname").Annotations
		assert.Contains(t, annotations["cobra_annotation_bash_completion_one_required_flag"], "true")

		annotations = registryAddCmd.Flag("artifact-type").Annotations
		assert.Contains(t, annotations["cobra_annotation_bash_completion_one_required_flag"], "true")
	})

	t.Run("add command exposes secret/username/password flags", func(t *testing.T) {
		for _, name := range []string{"hostname", "username", "password", "secret", "secret-file", "artifact-type", "description", "tag"} {
			assert.NotNilf(t, registryAddCmd.Flag(name), "expected --%s flag on add command", name)
		}
	})

	t.Run("delete command exposes --force flag", func(t *testing.T) {
		assert.NotNil(t, registryDeleteCmd.Flag("force"))
	})

	t.Run("examples reference the new command name", func(t *testing.T) {
		assert.Contains(t, registryCmd.Long, "nvcf-cli registry-credential")
		assert.NotContains(t, registryCmd.Long, "nvcf-cli registry add")
		assert.NotContains(t, registryCmd.Long, "nvcf-cli registry list")
	})
}

// --- validateAndEncodeCredentials ---

// TestValidateAndEncodeCredentials tests credential flag combinations and base64 encoding.
func TestValidateAndEncodeCredentials(t *testing.T) {
	t.Run("returns secret as-is when only --secret provided", func(t *testing.T) {
		got, err := validateAndEncodeCredentials("preencoded==", "", "")
		require.NoError(t, err)
		assert.Equal(t, "preencoded==", got)
	})

	t.Run("base64-encodes username:password when both provided", func(t *testing.T) {
		got, err := validateAndEncodeCredentials("", "alice", "s3cret")
		require.NoError(t, err)

		decoded, decodeErr := base64.StdEncoding.DecodeString(got)
		require.NoError(t, decodeErr)
		assert.Equal(t, "alice:s3cret", string(decoded))
	})

	t.Run("rejects --secret combined with --username", func(t *testing.T) {
		_, err := validateAndEncodeCredentials("preencoded==", "alice", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot use --secret with --username/--password")
	})

	t.Run("rejects --secret combined with --password", func(t *testing.T) {
		_, err := validateAndEncodeCredentials("preencoded==", "", "s3cret")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot use --secret with --username/--password")
	})

	t.Run("rejects when no auth flags provided", func(t *testing.T) {
		_, err := validateAndEncodeCredentials("", "", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must provide either --secret, --secret-file, OR both --username and --password")
	})

	t.Run("rejects when only --username provided", func(t *testing.T) {
		_, err := validateAndEncodeCredentials("", "alice", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must provide either --secret, --secret-file, OR both --username and --password")
	})

	t.Run("rejects when only --password provided", func(t *testing.T) {
		_, err := validateAndEncodeCredentials("", "", "s3cret")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must provide either --secret, --secret-file, OR both --username and --password")
	})

	t.Run("encodes passwords containing colons correctly", func(t *testing.T) {
		// Passwords with `:` are common; the API receives the *first* `:` as the
		// separator. We only need to assert we encode the raw concatenation
		// (server-side decoding is not our concern).
		got, err := validateAndEncodeCredentials("", "alice", "p:a:s:s")
		require.NoError(t, err)

		decoded, decodeErr := base64.StdEncoding.DecodeString(got)
		require.NoError(t, decodeErr)
		assert.Equal(t, "alice:p:a:s:s", string(decoded))
	})

	t.Run("rejects invalid base64 in --secret", func(t *testing.T) {
		_, err := validateAndEncodeCredentials("not-valid-base64!@#$", "", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid base64 credential")
	})
}

// --- trimTrailingLineEnding ---

// TestTrimTrailingLineEnding verifies trailing line ending removal behavior for files and stdin.
func TestTrimTrailingLineEnding(t *testing.T) {
	t.Run("trims single LF line ending", func(t *testing.T) {
		assert.Equal(t, "secret", trimTrailingLineEnding("secret\n"))
	})

	t.Run("trims single CRLF line ending", func(t *testing.T) {
		assert.Equal(t, "secret", trimTrailingLineEnding("secret\r\n"))
	})

	t.Run("trims single CR line ending", func(t *testing.T) {
		assert.Equal(t, "secret", trimTrailingLineEnding("secret\r"))
	})

	t.Run("returns unchanged when no trailing line ending", func(t *testing.T) {
		assert.Equal(t, "secret", trimTrailingLineEnding("secret"))
	})

	t.Run("preserves leading whitespace", func(t *testing.T) {
		assert.Equal(t, "  secret", trimTrailingLineEnding("  secret\n"))
	})

	t.Run("preserves trailing whitespace before line ending", func(t *testing.T) {
		assert.Equal(t, "secret  ", trimTrailingLineEnding("secret  \n"))
	})

	t.Run("trims only the last line ending when multiple exist", func(t *testing.T) {
		assert.Equal(t, "secret\n", trimTrailingLineEnding("secret\n\n"))
	})

	t.Run("returns empty string unchanged", func(t *testing.T) {
		assert.Equal(t, "", trimTrailingLineEnding(""))
	})
}

// --- validateBase64Secret ---

// TestValidateBase64Secret verifies base64 encoding validation.
func TestValidateBase64Secret(t *testing.T) {
	t.Run("accepts standard base64", func(t *testing.T) {
		encoded := base64.StdEncoding.EncodeToString([]byte("alice:s3cret"))
		assert.NoError(t, validateBase64Secret(encoded))
	})

	t.Run("accepts raw unpadded base64", func(t *testing.T) {
		encoded := base64.RawStdEncoding.EncodeToString([]byte("alice:s3cret"))
		assert.NoError(t, validateBase64Secret(encoded))
	})

	t.Run("rejects empty string", func(t *testing.T) {
		err := validateBase64Secret("")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "secret cannot be empty")
	})

	t.Run("rejects non-base64 content", func(t *testing.T) {
		err := validateBase64Secret("this is not base64!")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid base64 credential")
	})
}

// --- resolveAndValidateCredentials ---

// TestResolveAndValidateCredentials tests credential resolution from inline flags, files, and stdin.
func TestResolveAndValidateCredentials(t *testing.T) {
	validSecret := base64.StdEncoding.EncodeToString([]byte("user:pass"))

	t.Run("resolves secret from file with trailing newline", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "secret.b64")
		require.NoError(t, os.WriteFile(filePath, []byte(validSecret+"\n"), 0600))

		got, err := resolveAndValidateCredentials("", filePath, "", "", nil)
		require.NoError(t, err)
		assert.Equal(t, validSecret, got)
	})

	t.Run("resolves secret from file with CRLF", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "secret.b64")
		require.NoError(t, os.WriteFile(filePath, []byte(validSecret+"\r\n"), 0600))

		got, err := resolveAndValidateCredentials("", filePath, "", "", nil)
		require.NoError(t, err)
		assert.Equal(t, validSecret, got)
	})

	t.Run("resolves secret from stdin using hyphen", func(t *testing.T) {
		stdin := strings.NewReader(validSecret + "\n")
		got, err := resolveAndValidateCredentials("", "-", "", "", stdin)
		require.NoError(t, err)
		assert.Equal(t, validSecret, got)
	})

	t.Run("rejects empty secret file", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "empty.b64")
		require.NoError(t, os.WriteFile(filePath, []byte(""), 0600))

		_, err := resolveAndValidateCredentials("", filePath, "", "", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "secret cannot be empty")
	})

	t.Run("rejects empty stdin", func(t *testing.T) {
		stdin := strings.NewReader("")
		_, err := resolveAndValidateCredentials("", "-", "", "", stdin)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "secret cannot be empty")
	})

	t.Run("rejects stdin with only newline", func(t *testing.T) {
		stdin := strings.NewReader("\n")
		_, err := resolveAndValidateCredentials("", "-", "", "", stdin)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "secret cannot be empty")
	})

	t.Run("rejects non-existent file", func(t *testing.T) {
		_, err := resolveAndValidateCredentials("", "/path/to/nonexistent/secret.b64", "", "", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read secret file")
	})

	t.Run("rejects invalid base64 in file", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "invalid.b64")
		require.NoError(t, os.WriteFile(filePath, []byte("plain-text-credentials\n"), 0600))

		_, err := resolveAndValidateCredentials("", filePath, "", "", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid secret from file")
		assert.Contains(t, err.Error(), "invalid base64 credential")
	})

	t.Run("rejects invalid base64 in stdin", func(t *testing.T) {
		stdin := strings.NewReader("plain-text-credentials\n")
		_, err := resolveAndValidateCredentials("", "-", "", "", stdin)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid secret from stdin")
		assert.Contains(t, err.Error(), "invalid base64 credential")
	})

	t.Run("rejects secret-file with leading space that breaks base64", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "space.b64")
		require.NoError(t, os.WriteFile(filePath, []byte(" "+validSecret+"\n"), 0600))

		_, err := resolveAndValidateCredentials("", filePath, "", "", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid secret from file")
	})

	t.Run("rejects --secret combined with --secret-file", func(t *testing.T) {
		_, err := resolveAndValidateCredentials(validSecret, "/path/to/file", "", "", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot use both --secret and --secret-file")
	})

	t.Run("rejects --secret-file combined with --username", func(t *testing.T) {
		_, err := resolveAndValidateCredentials("", "/path/to/file", "alice", "", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot use --secret-file with --username/--password")
	})

	t.Run("rejects --secret-file combined with --password", func(t *testing.T) {
		_, err := resolveAndValidateCredentials("", "/path/to/file", "", "s3cret", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot use --secret-file with --username/--password")
	})

	t.Run("rejects --secret-file combined with both --username and --password", func(t *testing.T) {
		_, err := resolveAndValidateCredentials("", "/path/to/file", "alice", "s3cret", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot use --secret-file with --username/--password")
	})

	t.Run("resolves valid base64 --secret", func(t *testing.T) {
		got, err := resolveAndValidateCredentials(validSecret, "", "", "", nil)
		require.NoError(t, err)
		assert.Equal(t, validSecret, got)
	})

	t.Run("resolves username and password", func(t *testing.T) {
		got, err := resolveAndValidateCredentials("", "", "alice", "s3cret", nil)
		require.NoError(t, err)
		decoded, decodeErr := base64.StdEncoding.DecodeString(got)
		require.NoError(t, decodeErr)
		assert.Equal(t, "alice:s3cret", string(decoded))
	})
}

// --- parseAndValidateArtifactTypes ---

func TestParseAndValidateArtifactTypes(t *testing.T) {
	t.Run("accepts all valid types", func(t *testing.T) {
		got, err := parseAndValidateArtifactTypes([]string{"CONTAINER", "HELM", "MODEL", "RESOURCE"})
		require.NoError(t, err)
		assert.Equal(t, []client.ArtifactType{
			client.ArtifactType("CONTAINER"),
			client.ArtifactType("HELM"),
			client.ArtifactType("MODEL"),
			client.ArtifactType("RESOURCE"),
		}, got)
	})

	t.Run("normalizes input to uppercase", func(t *testing.T) {
		got, err := parseAndValidateArtifactTypes([]string{"container", "Helm", "MoDeL"})
		require.NoError(t, err)
		assert.Equal(t, []client.ArtifactType{
			client.ArtifactType("CONTAINER"),
			client.ArtifactType("HELM"),
			client.ArtifactType("MODEL"),
		}, got)
	})

	t.Run("rejects unknown type with helpful message", func(t *testing.T) {
		_, err := parseAndValidateArtifactTypes([]string{"CONTAINER", "BOGUS"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid artifact type 'BOGUS'")
		assert.Contains(t, err.Error(), "CONTAINER, HELM, MODEL, RESOURCE")
	})

	t.Run("preserves original case in error message", func(t *testing.T) {
		_, err := parseAndValidateArtifactTypes([]string{"sneaky"})
		require.Error(t, err)
		// Original (lowercase) input is echoed back so users see what they typed.
		assert.Contains(t, err.Error(), "'sneaky'")
	})

	t.Run("returns nil for empty input (caller enforces non-empty)", func(t *testing.T) {
		got, err := parseAndValidateArtifactTypes(nil)
		require.NoError(t, err)
		assert.Nil(t, got)
	})
}

// --- formatArtifactTypes ---

func TestFormatArtifactTypes(t *testing.T) {
	t.Run("joins multiple types with comma+space", func(t *testing.T) {
		got := formatArtifactTypes([]client.ArtifactType{
			client.ArtifactType("CONTAINER"),
			client.ArtifactType("HELM"),
		})
		assert.Equal(t, "CONTAINER, HELM", got)
	})

	t.Run("returns empty string for empty slice", func(t *testing.T) {
		assert.Equal(t, "", formatArtifactTypes(nil))
		assert.Equal(t, "", formatArtifactTypes([]client.ArtifactType{}))
	})

	t.Run("handles single element", func(t *testing.T) {
		got := formatArtifactTypes([]client.ArtifactType{client.ArtifactType("MODEL")})
		assert.Equal(t, "MODEL", got)
	})
}

// --- formatTimestamp ---

func TestFormatTimestamp(t *testing.T) {
	t.Run("reformats valid RFC3339 timestamp", func(t *testing.T) {
		got := formatTimestamp("2025-01-15T10:30:45Z")
		// Expect the output to use the human-friendly "YYYY-MM-DD HH:MM:SS"
		// layout (not the RFC3339 "T...Z" layout). We don't assert the trailing
		// timezone abbreviation since it depends on the host locale.
		assert.Contains(t, got, "2025-01-15")
		assert.Contains(t, got, "10:30:45")
		assert.NotEqual(t, "2025-01-15T10:30:45Z", got, "timestamp should be reformatted, not echoed")
		// The date/time portion should be space-separated, not "T"-separated.
		assert.Regexp(t, `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`, got)
	})

	t.Run("returns input unchanged when not RFC3339", func(t *testing.T) {
		assert.Equal(t, "not-a-timestamp", formatTimestamp("not-a-timestamp"))
	})

	t.Run("returns empty input unchanged", func(t *testing.T) {
		assert.Equal(t, "", formatTimestamp(""))
	})

	t.Run("handles RFC3339 with timezone offset", func(t *testing.T) {
		got := formatTimestamp("2025-01-15T10:30:45-07:00")
		assert.Contains(t, got, "2025-01-15")
		// Either reformatted or returned as-is; both are acceptable, but it
		// must not blow up.
		assert.NotEmpty(t, got)
	})
}

// --- Sanity check: top-level Use constant matches help text ---

// TestRegistryCredentialUseMatchesHelp verifies subcommand and top-level long help reference the new command name.
func TestRegistryCredentialUseMatchesHelp(t *testing.T) {
	// Quick sanity that all subcommand long-help references use the new
	// command name. This catches drift if anyone re-introduces "registry "
	// as the top-level group name.
	for _, sub := range registryCmd.Commands() {
		assert.NotContains(t, sub.Long, "nvcf-cli registry add",
			"subcommand %q long help still references old `registry add` form", sub.Use)
		assert.NotContains(t, sub.Long, "nvcf-cli registry list",
			"subcommand %q long help still references old `registry list` form", sub.Use)
		assert.NotContains(t, sub.Long, "nvcf-cli registry get",
			"subcommand %q long help still references old `registry get` form", sub.Use)
	}

	// And the top-level Long should reference the new name.
	assert.True(t, strings.Contains(registryCmd.Long, "nvcf-cli registry-credential"),
		"top-level command long help missing `nvcf-cli registry-credential` reference")
}

// TestRegistryCredentialAddHelp verifies --secret-file documentation and examples in help text.
func TestRegistryCredentialAddHelp(t *testing.T) {
	assert.Contains(t, registryAddCmd.Long, "--secret-file",
		"registry add command help should describe --secret-file")
	assert.Contains(t, registryAddCmd.Long, "--secret-file /path/to/secret.b64",
		"registry add command help should provide file example")
	assert.Contains(t, registryAddCmd.Long, "--secret-file -",
		"registry add command help should provide stdin example")
}

func TestRunUpdateRegistryCredentialValidation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "request rejected", http.StatusInternalServerError)
	}))
	defer server.Close()

	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("NVCF_API_KEY", "test-api-key")
	t.Setenv("NVCF_BASE_HTTP_URL", server.URL)
	t.Setenv("NVCF_BASE_GRPC_URL", "127.0.0.1:50051")
	previousCfgFile := cfgFile
	t.Cleanup(func() {
		cfgFile = previousCfgFile
		viper.Reset()
	})
	cfgFile = ""
	viper.Reset()
	viper.SetEnvPrefix("NVCF")
	viper.AutomaticEnv()

	tests := []struct {
		name    string
		flags   []string
		wantErr string
	}{
		{name: "no update flags", wantErr: "no updates specified. Use --username/--password or --artifact-type flags"},
		{name: "only username", flags: []string{"--username", "alice"}, wantErr: "both username and password must be provided together"},
		{name: "only password", flags: []string{"--password", "secret"}, wantErr: "both username and password must be provided together"},
		{name: "invalid artifact type", flags: []string{"--artifact-type", "UNKNOWN"}, wantErr: "invalid artifact type 'UNKNOWN'"},
		{name: "valid updates reach API", flags: []string{"--username", "alice", "--password", "secret", "--artifact-type", "CONTAINER", "--artifact-type", "MODEL"}, wantErr: "failed to update registry credential:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{Use: "update"}
			cmd.Flags().String("username", "", "")
			cmd.Flags().String("password", "", "")
			cmd.Flags().StringSlice("artifact-type", nil, "")
			require.NoError(t, cmd.Flags().Parse(tt.flags))

			err := runUpdateRegistryCredential(cmd, []string{"credential-id"})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func setupRegistryCommandEnvironment(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("NVCF_API_KEY", "test-api-key")
	t.Setenv("NVCF_BASE_HTTP_URL", server.URL)
	t.Setenv("NVCF_BASE_GRPC_URL", "127.0.0.1:50051")
	previousCfgFile := cfgFile
	t.Cleanup(func() {
		cfgFile = previousCfgFile
		viper.Reset()
	})
	cfgFile = ""
	viper.Reset()
	viper.SetEnvPrefix("NVCF")
	viper.AutomaticEnv()
	return server
}

func TestRunListRegistryCredentials(t *testing.T) {
	tests := []struct {
		name       string
		response   string
		flags      []string
		artifact   []string
		provision  []string
	}{
		{name: "empty response", response: `{"registryCredentials":[]}`},
		{name: "formatted populated response", response: `{"registryCredentials":[{"registryCredentialId":"id","registryName":"abcdefghijklmnopqrstuvwxyz0123456789","registryHostname":"registry.example.com/with/a/long/path","artifactTypes":["CONTAINER","HELM","MODEL"],"provisionedBy":"USER"}]}`},
		{name: "filters are uppercased", response: `{"registryCredentials":[]}`, flags: []string{"--artifact-type", "container", "--provisioned-by", "user"}, artifact: []string{"CONTAINER"}, provision: []string{"USER"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := setupRegistryCommandEnvironment(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, "/v2/nvcf/registry-credentials", r.URL.Path)
				assert.Equal(t, tt.artifact, r.URL.Query()["artifactType"])
				assert.Equal(t, tt.provision, r.URL.Query()["provisionedBy"])
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tt.response))
			})
			_ = server
			cmd := &cobra.Command{Use: "list"}
			cmd.Flags().StringSlice("artifact-type", nil, "")
			cmd.Flags().StringSlice("provisioned-by", nil, "")
			require.NoError(t, cmd.Flags().Parse(tt.flags))
			require.NoError(t, runListRegistryCredentials(cmd, nil))
		})
	}
}

func TestRunAddRegistryCredential(t *testing.T) {
	for _, tt := range []struct {
		name       string
		flags      []string
		wantErr    string
		wantCalled bool
	}{
		{name: "missing artifact types", flags: []string{"--hostname", "registry.example.com", "--username", "alice", "--password", "secret"}, wantErr: "at least one artifact type must be specified"},
		{name: "invalid artifact type", flags: []string{"--hostname", "registry.example.com", "--username", "alice", "--password", "secret", "--artifact-type", "UNKNOWN"}, wantErr: "invalid artifact type 'UNKNOWN'"},
		{name: "valid request", flags: []string{"--hostname", "registry.example.com", "--username", "alice", "--password", "secret", "--artifact-type", "container", "--artifact-type", "model", "--description", "private registry", "--tag", "team:ml", "--tag", "env:test"}, wantCalled: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			setupRegistryCommandEnvironment(t, func(w http.ResponseWriter, r *http.Request) {
				called = true
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/v2/nvcf/registry-credentials", r.URL.Path)
				var body struct {
					RegistryHostname string `json:"registryHostname"`
					Secret struct { Name string `json:"name"`; Value string `json:"value"` } `json:"secret"`
					ArtifactTypes []string `json:"artifactTypes"`
					Description string `json:"description"`
					Tags []string `json:"tags"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				assert.Equal(t, "registry.example.com", body.RegistryHostname)
				assert.Equal(t, "credentials", body.Secret.Name)
				assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("alice:secret")), body.Secret.Value)
				assert.Equal(t, []string{"CONTAINER", "MODEL"}, body.ArtifactTypes)
				assert.Equal(t, "private registry", body.Description)
				assert.Equal(t, []string{"team:ml", "env:test"}, body.Tags)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"registryCredential":{"registryCredentialId":"id","registryName":"registry","registryHostname":"registry.example.com","artifactTypes":["CONTAINER"],"provisionedBy":"USER","createdAt":"2025-01-01T00:00:00Z"}}`))
			})
			cmd := &cobra.Command{Use: "add"}
			cmd.Flags().String("hostname", "", "")
			cmd.Flags().String("username", "", "")
			cmd.Flags().String("password", "", "")
			cmd.Flags().String("secret", "", "")
			cmd.Flags().String("secret-file", "", "")
			cmd.Flags().StringSlice("artifact-type", nil, "")
			cmd.Flags().String("description", "", "")
			cmd.Flags().StringSlice("tag", nil, "")
			require.NoError(t, cmd.Flags().Parse(tt.flags))
			err := runAddRegistryCredential(cmd, nil)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantCalled, called)
		})
	}
}

func TestRegistryCredentialCommandOutputAndDeletePaths(t *testing.T) {
	credential := `{"registryCredential":{"registryCredentialId":"cred-123","ncaId":"nca-1","registryCredentialName":"private","registryName":"Example Registry","registryHostname":"registry.example.com","artifactTypes":["CONTAINER","HELM"],"provisionedBy":"USER","createdAt":"2025-01-02T03:04:05Z","lastUpdatedAt":"2025-02-03T04:05:06Z","description":"private images","tags":["team:ml","env:test"]}}`
	tests := []struct {
		name       string
		json       bool
		command    string
		response   string
		wantMethod string
		wantPath   string
		flags      []string
		wantBody   bool
	}{
		{name: "get formatted", command: "get", response: credential, wantMethod: http.MethodGet, wantPath: "/v2/nvcf/registry-credentials/cred-123"},
		{name: "get JSON", command: "get", json: true, response: credential, wantMethod: http.MethodGet, wantPath: "/v2/nvcf/registry-credentials/cred-123"},
		{name: "update formatted", command: "update", response: credential, wantMethod: http.MethodPatch, wantPath: "/v2/nvcf/registry-credentials/cred-123", flags: []string{"--username", "alice", "--password", "secret", "--artifact-type", "MODEL"}, wantBody: true},
		{name: "update JSON", command: "update", json: true, response: credential, wantMethod: http.MethodPatch, wantPath: "/v2/nvcf/registry-credentials/cred-123", flags: []string{"--username", "alice", "--password", "secret", "--artifact-type", "MODEL"}, wantBody: true},
		{name: "delete formatted", command: "delete", wantMethod: http.MethodDelete, wantPath: "/v2/nvcf/registry-credentials/cred-123", flags: []string{"--force"}},
		{name: "delete JSON", command: "delete", json: true, wantMethod: http.MethodDelete, wantPath: "/v2/nvcf/registry-credentials/cred-123", flags: []string{"--force"}},
		{name: "recognized empty", command: "recognized", response: `{"recognizedRegistries":{}}`, wantMethod: http.MethodGet, wantPath: "/v2/nvcf/recognized-registries"},
		{name: "recognized populated", command: "recognized", response: `{"recognizedRegistries":{"Docker Hub":[{"hostname":"docker.io","type":"CONTAINER"}],"NVIDIA NGC":[{"hostname":"nvcr.io","type":"CONTAINER"},{"hostname":"helm.ngc.nvidia.com","type":"HELM"}]}}`, wantMethod: http.MethodGet, wantPath: "/v2/nvcf/recognized-registries"},
		{name: "recognized populated JSON", command: "recognized", json: true, response: `{"recognizedRegistries":{"Docker Hub":[{"hostname":"docker.io"}],"NVIDIA NGC":[{"hostname":"nvcr.io"}]}}`, wantMethod: http.MethodGet, wantPath: "/v2/nvcf/recognized-registries"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			previousJSON := jsonOutput
			jsonOutput = tt.json
			t.Cleanup(func() { jsonOutput = previousJSON })
			setupRegistryCommandEnvironment(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, tt.wantMethod, r.Method)
				assert.Equal(t, tt.wantPath, r.URL.Path)
				if tt.wantBody {
					var body struct {
						Secret struct {
							Name  string `json:"name"`
							Value string `json:"value"`
						} `json:"secret"`
						ArtifactTypes []string `json:"artifactTypeEnums"`
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
					assert.Equal(t, "credentials", body.Secret.Name)
					assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("alice:secret")), body.Secret.Value)
					assert.Equal(t, []string{"MODEL"}, body.ArtifactTypes)
				}
				if tt.command == "delete" {
					assert.Equal(t, "/v2/nvcf/registry-credentials/cred-123", r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				if tt.command == "delete" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				_, _ = w.Write([]byte(tt.response))
			})
			var cmd *cobra.Command
			var run func() error
			switch tt.command {
			case "get":
				cmd = &cobra.Command{Use: "get"}
				run = func() error { return runGetRegistryCredential(cmd, []string{"cred-123"}) }
			case "update":
				cmd = &cobra.Command{Use: "update"}
				cmd.Flags().String("username", "", "")
				cmd.Flags().String("password", "", "")
				cmd.Flags().StringSlice("artifact-type", nil, "")
				require.NoError(t, cmd.Flags().Parse(tt.flags))
				run = func() error { return runUpdateRegistryCredential(cmd, []string{"cred-123"}) }
			case "delete":
				cmd = &cobra.Command{Use: "delete"}
				cmd.Flags().Bool("force", false, "")
				require.NoError(t, cmd.Flags().Parse(tt.flags))
				run = func() error { return runDeleteRegistryCredential(cmd, []string{"cred-123"}) }
			default:
				cmd = &cobra.Command{Use: "list-recognized"}
				run = func() error { return runListRecognizedRegistries(cmd, nil) }
			}
			if tt.json {
				oldStdout := os.Stdout
				reader, writer, err := os.Pipe()
				require.NoError(t, err)
				os.Stdout = writer
				err = run()
				_ = writer.Close()
				os.Stdout = oldStdout
				output, readErr := io.ReadAll(reader)
				_ = reader.Close()
				require.NoError(t, err)
				require.NoError(t, readErr)
				if tt.command == "recognized" {
					assert.Contains(t, string(output), "recognizedRegistries")
					if tt.name == "recognized populated JSON" {
						assert.Contains(t, string(output), "docker.io")
						assert.Contains(t, string(output), "nvcr.io")
					}
				} else if tt.command == "delete" {
					assert.Contains(t, string(output), `"status": "deleted"`)
					assert.Contains(t, string(output), "cred-123")
				} else {
					assert.Contains(t, string(output), "registryCredentialId")
					assert.Contains(t, string(output), "cred-123")
				}
			} else {
				require.NoError(t, run())
			}
		})
	}
}
