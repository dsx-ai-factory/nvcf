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

package openbao

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const openBaoTestCertPEM = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"

func TestRootCAPEMFromOpenBaoResponseAcceptsJSONCertificate(t *testing.T) {
	body, err := json.Marshal(map[string]any{
		"data": map[string]string{"certificate": openBaoTestCertPEM},
	})
	require.NoError(t, err)

	got, err := rootCAPEMFromOpenBaoResponse(string(body))
	require.NoError(t, err)
	assert.Equal(t, openBaoTestCertPEM, got)
}

func TestRootCAPEMFromOpenBaoResponseAcceptsRawPEM(t *testing.T) {
	got, err := rootCAPEMFromOpenBaoResponse(openBaoTestCertPEM)
	require.NoError(t, err)
	assert.Equal(t, openBaoTestCertPEM, got)
}

func TestRootCAPEMFromOpenBaoResponseMapsMissingPKIToSentinel(t *testing.T) {
	_, err := rootCAPEMFromOpenBaoResponse(`{"errors":["no handler for route \"services/all/pki/root/cert/ca\""]}`)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPKICertificateNotFound))
}

func TestKubectlBaseArgsIncludesContext(t *testing.T) {
	c := NewClient(&Config{KubeconfigPath: "/tmp/kubeconfig", KubeContext: "cp-context"}, nil)
	assert.Equal(t, []string{"kubectl", "--kubeconfig", "/tmp/kubeconfig", "--context", "cp-context"}, c.kubectlBaseArgs())
}

func TestFilterKubectlOutputPreservesPEMBlockWithKubectlNoise(t *testing.T) {
	c := NewClient(&Config{}, nil)
	got := c.filterKubectlOutput("pod \"openbao-pki-root-ca\" deleted\n" + openBaoTestCertPEM + "pod \"openbao-pki-root-ca\" deleted\n")
	assert.Equal(t, openBaoTestCertPEM[:len(openBaoTestCertPEM)-1], got)
}

func TestFilterKubectlOutputDropsLowercaseAttachFallbackAfterResponse(t *testing.T) {
	c := NewClient(&Config{}, nil)
	response := `{"data":{"certificate":"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"}}`
	tests := map[string]string{
		"normal attach": response + "\npod \"openbao-pki-root-ca\" deleted\n",
		"log fallback": response +
			"\nwarning: couldn't attach to pod/openbao-pki-root-ca, falling back to streaming logs\n" +
			"pod \"openbao-pki-root-ca\" deleted\n",
	}

	for name, output := range tests {
		t.Run(name, func(t *testing.T) {
			filtered := c.filterKubectlOutput(output)
			got, err := rootCAPEMFromOpenBaoResponse(filtered)
			require.NoError(t, err)
			assert.Equal(t, openBaoTestCertPEM, got)
		})
	}
}

func TestRootCAPEMFromOpenBaoResponsePreservesCertificateErrors(t *testing.T) {
	tests := map[string]string{
		"malformed response":  "not-json",
		"OpenBao error":       `{"errors":["permission denied"]}`,
		"missing certificate": `{"data":{}}`,
	}

	for name, response := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := rootCAPEMFromOpenBaoResponse(response)
			require.Error(t, err)
		})
	}
}

func TestReadPKICertificatePEMRetriesMalformedResponse(t *testing.T) {
	responses := []pkiCertificateHTTPResponse{
		{StatusCode: http.StatusOK, ContentType: "text/plain", Body: "not-json"},
		{StatusCode: http.StatusOK, ContentType: "application/json", Body: `{"data":{"certificate":"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"}}`},
	}
	attempt := 0

	got, err := readPKICertificatePEM(context.Background(), len(responses), 0, func(context.Context) (pkiCertificateHTTPResponse, error) {
		response := responses[attempt]
		attempt++
		return response, nil
	})

	require.NoError(t, err)
	assert.Equal(t, openBaoTestCertPEM, got)
	assert.Equal(t, len(responses), attempt)
}

func TestReadPKICertificatePEMRetriesEmptyResponse(t *testing.T) {
	responses := []pkiCertificateHTTPResponse{
		{StatusCode: http.StatusOK},
		{StatusCode: http.StatusOK, ContentType: "application/json", Body: `{"data":{"certificate":"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"}}`},
	}
	attempt := 0

	got, err := readPKICertificatePEM(context.Background(), len(responses), 0, func(context.Context) (pkiCertificateHTTPResponse, error) {
		response := responses[attempt]
		attempt++
		return response, nil
	})

	require.NoError(t, err)
	assert.Equal(t, openBaoTestCertPEM, got)
	assert.Equal(t, len(responses), attempt)
}

func TestReadPKICertificatePEMRetriesMissingHTTPStatus(t *testing.T) {
	c := NewClient(&Config{}, nil)
	outputs := []string{
		`pod "openbao-pki-root-ca" deleted`,
		`{"data":{"certificate":"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"}}` + "\n" +
			curlHTTPStatusMarker + "200\n" +
			curlHTTPContentTypeMarker + "application/json\n",
	}
	attempt := 0

	got, err := readPKICertificatePEM(context.Background(), len(outputs), 0, func(context.Context) (pkiCertificateHTTPResponse, error) {
		output := outputs[attempt]
		attempt++
		return pkiCertificateHTTPResponseFromOutput(c.filterKubectlOutput(output))
	})

	require.NoError(t, err)
	assert.Equal(t, openBaoTestCertPEM, got)
	assert.Equal(t, len(outputs), attempt)
}

func TestReadPKICertificatePEMReportsMissingHTTPStatusAfterRetries(t *testing.T) {
	attempt := 0

	_, err := readPKICertificatePEM(context.Background(), 3, 0, func(context.Context) (pkiCertificateHTTPResponse, error) {
		attempt++
		return pkiCertificateHTTPResponseFromOutput("")
	})

	require.Error(t, err)
	assert.Equal(t, 3, attempt)
	assert.ErrorContains(t, err, "OpenBao PKI response missing HTTP status")
}

func TestReadPKICertificatePEMDoesNotRetryOpenBaoError(t *testing.T) {
	attempt := 0

	_, err := readPKICertificatePEM(context.Background(), 3, 0, func(context.Context) (pkiCertificateHTTPResponse, error) {
		attempt++
		return pkiCertificateHTTPResponse{
			StatusCode:  http.StatusOK,
			ContentType: "application/json",
			Body:        `{"errors":["permission denied"]}`,
		}, nil
	})

	require.Error(t, err)
	assert.Equal(t, 1, attempt)
}

func TestReadPKICertificatePEMRetriesServerError(t *testing.T) {
	responses := []pkiCertificateHTTPResponse{
		{StatusCode: http.StatusServiceUnavailable, ContentType: "text/plain", Body: "Internal Server Error"},
		{StatusCode: http.StatusOK, ContentType: "application/json", Body: `{"data":{"certificate":"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"}}`},
	}
	attempt := 0

	got, err := readPKICertificatePEM(context.Background(), len(responses), 0, func(context.Context) (pkiCertificateHTTPResponse, error) {
		response := responses[attempt]
		attempt++
		return response, nil
	})

	require.NoError(t, err)
	assert.Equal(t, openBaoTestCertPEM, got)
	assert.Equal(t, len(responses), attempt)
}

func TestReadPKICertificatePEMReportsServerErrorAfterRetries(t *testing.T) {
	attempt := 0

	_, err := readPKICertificatePEM(context.Background(), 3, 0, func(context.Context) (pkiCertificateHTTPResponse, error) {
		attempt++
		return pkiCertificateHTTPResponse{
			StatusCode:  http.StatusServiceUnavailable,
			ContentType: "text/plain; charset=utf-8",
			Body:        "Internal Server Error",
		}, nil
	})

	require.Error(t, err)
	assert.Equal(t, 3, attempt)
	assert.ErrorContains(t, err, "HTTP 503")
	assert.ErrorContains(t, err, `content type "text/plain; charset=utf-8"`)
	assert.ErrorContains(t, err, "Internal Server Error")
	assert.NotContains(t, err.Error(), "invalid character")
}

func TestReadPKICertificatePEMDoesNotRetryClientError(t *testing.T) {
	attempt := 0

	_, err := readPKICertificatePEM(context.Background(), 3, 0, func(context.Context) (pkiCertificateHTTPResponse, error) {
		attempt++
		return pkiCertificateHTTPResponse{
			StatusCode:  http.StatusForbidden,
			ContentType: "application/json",
			Body:        `{"errors":["permission denied"]}`,
		}, nil
	})

	require.Error(t, err)
	assert.Equal(t, 1, attempt)
	assert.ErrorContains(t, err, "HTTP 403")
	assert.ErrorContains(t, err, "permission denied")
}

func TestReadPKICertificatePEMReportsUnexpectedSuccessStatus(t *testing.T) {
	attempt := 0

	_, err := readPKICertificatePEM(context.Background(), 3, 0, func(context.Context) (pkiCertificateHTTPResponse, error) {
		attempt++
		return pkiCertificateHTTPResponse{StatusCode: http.StatusNoContent}, nil
	})

	require.Error(t, err)
	assert.Equal(t, 1, attempt)
	assert.ErrorContains(t, err, "HTTP 204")
}

func TestReadPKICertificatePEMPreservesMissingPKIError(t *testing.T) {
	attempt := 0

	_, err := readPKICertificatePEM(context.Background(), 3, 0, func(context.Context) (pkiCertificateHTTPResponse, error) {
		attempt++
		return pkiCertificateHTTPResponse{
			StatusCode:  http.StatusNotFound,
			ContentType: "application/json",
			Body:        `{"errors":["no handler for route services/all/pki/root/cert/ca"]}`,
		}, nil
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPKICertificateNotFound)
	assert.Equal(t, 1, attempt)
}

func TestPKICertificateHTTPResponseFromKubectlOutput(t *testing.T) {
	c := NewClient(&Config{}, nil)
	output := "Internal Server Error\nupstream unavailable\n" +
		curlHTTPStatusMarker + "503\n" +
		curlHTTPContentTypeMarker + "text/plain\n" +
		`pod "openbao-pki-root-ca" deleted` + "\n"

	response, err := pkiCertificateHTTPResponseFromOutput(c.filterKubectlOutput(output))

	require.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	assert.Equal(t, "text/plain", response.ContentType)
	assert.Equal(t, "Internal Server Error\nupstream unavailable", response.Body)
}

func TestPKICertificateHTTPErrorBoundsResponseBody(t *testing.T) {
	err := pkiCertificateHTTPError(pkiCertificateHTTPResponse{
		StatusCode: http.StatusBadGateway,
		Body:       strings.Repeat("x", maxOpenBaoHTTPErrorBody+100),
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), strings.Repeat("x", maxOpenBaoHTTPErrorBody))
	assert.NotContains(t, err.Error(), strings.Repeat("x", maxOpenBaoHTTPErrorBody+1))
	assert.True(t, strings.HasSuffix(err.Error(), "..."))
}

func TestPKICertificateHTTPErrorOmitsCertificateBody(t *testing.T) {
	err := pkiCertificateHTTPError(pkiCertificateHTTPResponse{
		StatusCode: http.StatusBadGateway,
		Body:       openBaoTestCertPEM,
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "<certificate response omitted>")
	assert.NotContains(t, err.Error(), "BEGIN CERTIFICATE")
}

func TestKubectlOutputMetadataDoesNotExposeCertificate(t *testing.T) {
	metadata := kubectlOutputMetadata(openBaoTestCertPEM)

	assert.Equal(t, "59 bytes", metadata)
	assert.NotContains(t, metadata, "CERTIFICATE")
}

func TestExecuteKubectlRunPreservesCommandError(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	c := NewClient(&Config{}, nil)
	_, err := c.executeKubectlRun(context.Background(), "test", nil)
	require.Error(t, err)

	var execErr *exec.Error
	require.ErrorAs(t, err, &execErr)
}

func TestReadPKICertificatePEMUsesPublicEndpointWithoutRootToken(t *testing.T) {
	testDir := t.TempDir()
	commandLog := filepath.Join(testDir, "kubectl.log")
	kubectlPath := filepath.Join(testDir, "kubectl")
	kubectlScript := `#!/bin/sh
printf '%s\n' "$*" >> "$KUBECTL_COMMAND_LOG"
case " $* " in
  *" get secret "*) exit 91 ;;
  *" X-Vault-Token: "*) exit 92 ;;
esac
printf '%s\n' '{"data":{"certificate":"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"}}'
printf '%s\n' '__NVCF_HTTP_STATUS__:200'
printf '%s\n' '__NVCF_HTTP_CONTENT_TYPE__:application/json'
`
	require.NoError(t, os.WriteFile(kubectlPath, []byte(kubectlScript), 0o755))
	t.Setenv("PATH", testDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("KUBECTL_COMMAND_LOG", commandLog)

	client := NewClient(&Config{
		OpenBaoURL:        "http://openbao-openbao.nvcf.svc.cluster.local:8200",
		OpenBaoNamespace:  "openbao",
		OpenBaoSecretName: "openbao-root-token",
		ClusterNamespace:  "nvcf",
		UtilityImage:      "curlimages/curl:latest",
	}, nil)

	got, err := client.ReadPKICertificatePEM(context.Background(), "services/all/pki/root")
	require.NoError(t, err)
	assert.Equal(t, openBaoTestCertPEM, got)

	logBody, err := os.ReadFile(commandLog)
	require.NoError(t, err)
	commands := string(logBody)
	assert.NotContains(t, commands, " get secret ")
	assert.NotContains(t, commands, "X-Vault-Token")
	assert.Contains(t, commands, "/v1/services/all/pki/root/cert/ca")
	assert.Contains(t, commands, "--write-out")
}

func TestGenerateJWTTokenHandlesSigningResponses(t *testing.T) {
	tests := []struct {
		name        string
		output      string
		wantToken   string
		wantErrText string
	}{
		{
			name:      "valid token",
			output:    `{"data":{"token":"signed-jwt"}}`,
			wantToken: "signed-jwt",
		},
		{
			name:        "empty response",
			output:      " \n\t",
			wantErrText: "empty response from JWT signing",
		},
		{
			name:        "malformed JSON",
			output:      "not-json",
			wantErrText: "failed to unmarshal JWT response",
		},
		{
			name:        "missing token",
			output:      `{"data":{}}`,
			wantErrText: "no token in JWT signing response",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDir := t.TempDir()
			kubectlPath := filepath.Join(testDir, "kubectl")
			kubectlScript := "#!/bin/sh\nprintf '%s' '" + tt.output + "'\n"
			require.NoError(t, os.WriteFile(kubectlPath, []byte(kubectlScript), 0o755))
			t.Setenv("PATH", testDir+string(os.PathListSeparator)+os.Getenv("PATH"))

			client := NewClient(&Config{
				OpenBaoURL:            "http://openbao.nvcf.svc.cluster.local:8200",
				OpenBaoJWTSigningRole: "nvcf-admin",
				OpenBaoNamespace:      "openbao",
				UtilityImage:          "curlimages/curl:latest",
				Debug:                 true,
			}, nil)

			token, err := client.generateJWTToken(context.Background(), "vault-token")
			if tt.wantErrText == "" {
				require.NoError(t, err)
				assert.Equal(t, tt.wantToken, token)
				return
			}

			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErrText)
			assert.Empty(t, token)
		})
	}
}

func TestGenerateAdminTokenWithRootSecret(t *testing.T) {
	rootToken := "s.test-root-token"
	tests := []struct {
		name        string
		signingJSON string
		wantToken   string
		wantErr     string
	}{
		{name: "valid signing response", signingJSON: `{"data":{"token":"admin-jwt"}}`, wantToken: "admin-jwt"},
		{name: "missing token", signingJSON: `{"data":{}}`, wantErr: "failed to generate JWT token: no token in JWT signing response"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDir := t.TempDir()
			kubectlPath := filepath.Join(testDir, "kubectl")
			commandLog := filepath.Join(testDir, "kubectl.log")
			encodedRoot := base64.StdEncoding.EncodeToString([]byte(rootToken))
			kubectlScript := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$KUBECTL_COMMAND_LOG\"\ncase \" $* \" in\n  *\" get secret \"*) printf '%s\\n' '" + encodedRoot + "'; exit 0 ;;\nesac\nprintf '%s\\n' '" + tt.signingJSON + "'\n"
			require.NoError(t, os.WriteFile(kubectlPath, []byte(kubectlScript), 0o755))
			t.Setenv("PATH", testDir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("KUBECTL_COMMAND_LOG", commandLog)

			client := NewClient(&Config{
				OpenBaoURL:            "http://openbao.nvcf.svc.cluster.local:8200",
				OpenBaoJWTSigningRole: "nvcf-admin",
				OpenBaoNamespace:      "openbao",
				OpenBaoSecretName:     "openbao-root-token",
				ClusterNamespace:      "nvcf",
				UtilityImage:          "curlimages/curl:latest",
				Debug:                 true,
			}, nil)

			token, expiration, err := client.GenerateAdminToken(context.Background())
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr)
				assert.Empty(t, token)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantToken, token)
				assert.WithinDuration(t, time.Now().Add(24*time.Hour), expiration, 5*time.Second)
			}

			logBody, readErr := os.ReadFile(commandLog)
			require.NoError(t, readErr)
			commands := string(logBody)
			assert.Contains(t, commands, "get secret openbao-root-token -n openbao")
			assert.Contains(t, commands, "X-Vault-Token: "+rootToken)
			assert.NotContains(t, commands, "auth/jwt/login")
		})
	}
}

func TestExecuteKubectlRunOutputAndErrorPaths(t *testing.T) {
	tests := []struct {
		name       string
		output     string
		exitCode   int
		wantOutput string
		wantErr    bool
	}{
		{
			name:       "JSON output",
			output:     `{"data":{"value":"ok"}}`,
			wantOutput: `{"data":{"value":"ok"}}`,
		},
		{
			name:       "PEM output",
			output:     openBaoTestCertPEM,
			wantOutput: strings.TrimSuffix(openBaoTestCertPEM, "\n"),
		},
		{
			name:       "plain text output",
			output:     "command completed",
			wantOutput: "command completed",
		},
		{
			name:     "command failure",
			output:   "kubectl diagnostic output",
			exitCode: 7,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDir := t.TempDir()
			kubectlPath := filepath.Join(testDir, "kubectl")
			kubectlScript := "#!/bin/sh\nprintf '%s\\n' 'pod \"utility\" deleted'\nprintf '%s\\n' '" + tt.output + "'\n"
			if tt.exitCode != 0 {
				kubectlScript += "exit " + strconv.Itoa(tt.exitCode) + "\n"
			}
			require.NoError(t, os.WriteFile(kubectlPath, []byte(kubectlScript), 0o755))
			t.Setenv("PATH", testDir+string(os.PathListSeparator)+os.Getenv("PATH"))

			client := NewClient(&Config{
				UtilityImage:     "curlimages/curl:latest",
				ClusterNamespace: "nvcf",
				KubeconfigPath:   "/tmp/kubeconfig",
				KubeContext:      "test-context",
				Debug:            true,
			}, nil)

			got, err := client.executeKubectlRun(context.Background(), "test-command", []string{"echo", "hello"})
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorContains(t, err, "kubectl run failed: exit status 7")
				assert.ErrorContains(t, err, "kubectl diagnostic output")
				assert.Empty(t, got)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantOutput, got)
		})
	}
}

func TestAuthenticateWithOpenBaoHandlesResponses(t *testing.T) {
	tests := []struct {
		name        string
		output      string
		exitCode    int
		wantToken   string
		wantErrText string
	}{
		{
			name:      "valid token",
			output:    `{"auth":{"client_token":"openbao-token"}}`,
			wantToken: "openbao-token",
		},
		{
			name:        "empty response",
			output:      " \n\t",
			wantErrText: "empty response from OpenBao authentication",
		},
		{
			name:        "malformed JSON",
			output:      "not-json",
			wantErrText: "failed to unmarshal auth response",
		},
		{
			name:        "missing token",
			output:      `{"auth":{}}`,
			wantErrText: "no client token in OpenBao response",
		},
		{
			name:        "kubectl failure",
			exitCode:    7,
			wantErrText: "failed to authenticate with OpenBao via kubectl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDir := t.TempDir()
			kubectlPath := filepath.Join(testDir, "kubectl")
			kubectlScript := "#!/bin/sh\nprintf '%s' '" + tt.output + "'\n"
			if tt.exitCode != 0 {
				kubectlScript += "exit " + strconv.Itoa(tt.exitCode) + "\n"
			}
			require.NoError(t, os.WriteFile(kubectlPath, []byte(kubectlScript), 0o755))
			t.Setenv("PATH", testDir+string(os.PathListSeparator)+os.Getenv("PATH"))

			client := NewClient(&Config{
				OpenBaoURL:         "http://openbao.nvcf.svc.cluster.local:8200",
				OpenBaoJWTAuthRole: "nvcf-admin",
				UtilityImage:       "curlimages/curl:latest",
				ClusterNamespace:   "nvcf",
				Debug:              true,
			}, nil)

			token, err := client.authenticateWithOpenBao(context.Background(), "sample-service-account-token")
			if tt.wantErrText == "" {
				require.NoError(t, err)
				assert.Equal(t, tt.wantToken, token)
				return
			}

			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErrText)
			assert.Empty(t, token)
		})
	}
}

func TestGenerateUserJWTTokenWithSubjectHandlesResponses(t *testing.T) {
	tests := []struct {
		name        string
		output      string
		exitCode    int
		wantToken   string
		wantErrText string
	}{
		{name: "valid token", output: `{"data":{"token":"user-jwt"}}`, wantToken: "user-jwt"},
		{name: "empty response", output: " \n\t", wantErrText: "empty response from user JWT signing"},
		{name: "malformed JSON", output: "not-json", wantErrText: "failed to unmarshal user JWT response"},
		{name: "missing token", output: `{"data":{}}`, wantErrText: "no token in user JWT signing response"},
		{name: "kubectl failure", exitCode: 7, wantErrText: "failed to sign user JWT token via kubectl"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDir := t.TempDir()
			kubectlPath := filepath.Join(testDir, "kubectl")
			argsLog := filepath.Join(testDir, "args")
			kubectlScript := "#!/bin/sh\nprintf '%s\\n' \"$@\" > " + strconv.Quote(argsLog) + "\nprintf '%s' '" + tt.output + "'\n"
			if tt.exitCode != 0 {
				kubectlScript += "exit " + strconv.Itoa(tt.exitCode) + "\n"
			}
			require.NoError(t, os.WriteFile(kubectlPath, []byte(kubectlScript), 0o755))
			t.Setenv("PATH", testDir+string(os.PathListSeparator)+os.Getenv("PATH"))

			client := NewClient(&Config{
				OpenBaoURL:            "http://openbao.nvcf.svc.cluster.local:8200",
				OpenBaoJWTSigningRole: "nvcf-user",
				UtilityImage:          "curlimages/curl:latest",
				ClusterNamespace:      "nvcf",
				Debug:                 true,
			}, nil)

			subject := "user@example.com"
			token, err := client.generateUserJWTTokenWithSubject(context.Background(), "sample-vault-token", subject)
			if tt.wantErrText != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErrText)
				assert.Empty(t, token)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantToken, token)
			args, readErr := os.ReadFile(argsLog)
			require.NoError(t, readErr)
			argList := strings.Split(strings.TrimSpace(string(args)), "\n")
			assert.Contains(t, argList, "openbao-user-jwt-sign")
			assert.Contains(t, argList, "http://openbao.nvcf.svc.cluster.local:8200/v1/services/nvcf-api/jwt/sign/nvcf-user")
			assert.Contains(t, argList, "X-Vault-Token: sample-vault-token")
			payloadIndex := -1
			for i, arg := range argList {
				if arg == "-d" && i+1 < len(argList) {
					payloadIndex = i + 1
					break
				}
			}
			require.NotEqual(t, -1, payloadIndex)
			var payload map[string]string
			require.NoError(t, json.Unmarshal([]byte(argList[payloadIndex]), &payload))
			assert.Equal(t, subject, payload["sub"])
		})
	}
}
