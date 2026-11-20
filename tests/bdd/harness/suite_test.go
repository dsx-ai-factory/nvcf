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

package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSignalSafeStepsPreserveNormalContinuation(t *testing.T) {
	suite := &Suite{}
	stopSignalCleanup := suite.InstallSignalCleanup()
	defer stopSignalCleanup()

	type contextKey string
	parent := context.WithValue(context.Background(), contextKey("scenario"), "retained")
	first, err := suite.BeginSignalSafeStep(parent)
	if err != nil {
		t.Fatalf("begin first signal-safe step: %v", err)
	}

	continuation := suite.EndSignalSafeStep(first)
	if err := continuation.Err(); err != nil {
		t.Fatalf("normal continuation was canceled: %v", err)
	}
	if got := continuation.Value(contextKey("scenario")); got != "retained" {
		t.Fatalf("normal continuation lost scenario value: got %v", got)
	}
	if repeated := suite.EndSignalSafeStep(first); repeated != continuation {
		t.Fatal("repeated end did not return the same continuation context")
	}

	second, err := suite.BeginSignalSafeStep(continuation)
	if err != nil {
		t.Fatalf("begin second signal-safe step: %v", err)
	}
	defer suite.EndSignalSafeStep(second)
	if err := second.Err(); err != nil {
		t.Fatalf("second step inherited canceled context: %v", err)
	}
}

func TestSuiteRestoresGeneratedSecretOnSignals(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		signal    os.Signal
		exitCode  int
		failWrite bool
	}{
		{name: "SIGINT", signal: os.Interrupt, exitCode: 130},
		{name: "SIGTERM", signal: syscall.SIGTERM, exitCode: 143},
		{name: "SIGTERM-fixture-write-failure", signal: syscall.SIGTERM, exitCode: 143, failWrite: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			testDir := t.TempDir()
			secretPath := filepath.Join(testDir, "local-bdd-pki-secrets.yaml")
			readyPath := filepath.Join(testDir, "ready")
			var output bytes.Buffer
			cmd := exec.Command(os.Args[0], "-test.run=^TestSuiteSignalCleanupHelper$")
			cmd.Env = append(os.Environ(),
				"NVCF_BDD_SIGNAL_HELPER=1",
				"NVCF_BDD_SIGNAL_SECRET="+secretPath,
				"NVCF_BDD_SIGNAL_READY="+readyPath,
				fmt.Sprintf("NVCF_BDD_SIGNAL_FAIL_WRITE=%t", testCase.failWrite),
			)
			cmd.Stdout = &output
			cmd.Stderr = &output
			if err := cmd.Start(); err != nil {
				t.Fatalf("start signal cleanup helper: %v", err)
			}

			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, err := os.Stat(readyPath); err == nil {
					break
				} else if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("stat helper readiness file: %v", err)
				}
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					t.Fatalf("signal cleanup helper did not become ready:\n%s", output.String())
				}
				time.Sleep(10 * time.Millisecond)
			}

			if err := cmd.Process.Signal(testCase.signal); err != nil {
				t.Fatalf("send %s: %v", testCase.name, err)
			}
			waitDone := make(chan error, 1)
			go func() {
				waitDone <- cmd.Wait()
			}()
			var err error
			select {
			case err = <-waitDone:
			case <-time.After(5 * time.Second):
				_ = cmd.Process.Kill()
				err = <-waitDone
				t.Fatalf("signal cleanup helper did not exit after %s (wait error: %v):\n%s", testCase.name, err, output.String())
			}
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("signal cleanup helper error = %v, want exit %d\n%s", err, testCase.exitCode, output.String())
			}
			if got := exitErr.ExitCode(); got != testCase.exitCode {
				t.Fatalf("signal cleanup helper exit code = %d, want %d\n%s", got, testCase.exitCode, output.String())
			}
			workspacePath, readErr := os.ReadFile(readyPath)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if _, err := os.Stat(string(workspacePath)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("subprocess workspace remained after signal: %v", err)
			}
			if _, err := os.Stat(secretPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("generated registry credential remained after %s cleanup: %v", testCase.name, err)
			}
		})
	}
}

func TestSuiteSignalCleanupHelper(t *testing.T) {
	if os.Getenv("NVCF_BDD_SIGNAL_HELPER") != "1" {
		return
	}
	secretPath := os.Getenv("NVCF_BDD_SIGNAL_SECRET")
	readyPath := os.Getenv("NVCF_BDD_SIGNAL_READY")
	suite := &Suite{
		Ledger:    NewLedger(filepath.Join(filepath.Dir(secretPath), "originals")),
		EnvLedger: NewEnvLedger(),
	}
	if err := suite.Ledger.Snapshot(secretPath); err != nil {
		t.Fatalf("snapshot generated secret: %v", err)
	}
	if err := suite.createTempWorkspace(); err != nil {
		t.Fatal(err)
	}
	stopSignalCleanup := suite.InstallSignalCleanup()
	defer stopSignalCleanup()

	stepContext, err := suite.BeginSignalSafeStep(context.Background())
	if err != nil {
		t.Fatalf("begin signal-safe step: %v", err)
	}
	defer suite.EndSignalSafeStep(stepContext)
	if err := os.WriteFile(secretPath, []byte("registryCredential: generated\n"), 0o600); err != nil {
		t.Fatalf("write generated secret: %v", err)
	}
	// The child is killed as a process group and cannot run an EXIT trap.
	// Suite teardown must still remove its nested secret workspace afterward.
	child := exec.CommandContext(stepContext, "/bin/sh", "-ec",
		`work=$(mktemp -d "$TMPDIR/child.XXXXXX"); trap 'rm -rf "$work"' EXIT; printf fixture > "$work/secret"; printf '%s' "$2" > "$1.tmp"; mv "$1.tmp" "$1"; sleep 30 & wait`,
		"signal-helper", readyPath, suite.tempWorkspace)
	configureCommandCancellation(child)
	if err := child.Run(); err == nil || stepContext.Err() == nil {
		t.Fatalf("child was not canceled: %v", err)
	}
	// Simulate a file-writing step completing after cancellation. Signal
	// cleanup must wait for this active step, then restore the absent snapshot.
	if err := os.WriteFile(secretPath, []byte("registryCredential: recreated-after-cancel\n"), 0o600); err != nil {
		t.Fatalf("rewrite generated secret after cancellation: %v", err)
	}
	if os.Getenv("NVCF_BDD_SIGNAL_FAIL_WRITE") == "true" {
		if err := os.RemoveAll(suite.tempWorkspace); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(suite.tempWorkspace, "copied-secret"), []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	suite.EndSignalSafeStep(stepContext)
	select {}
}

func TestCLIStateTrackingCanBeDisabled(t *testing.T) {
	for _, skip := range []bool{false, true} {
		for _, existed := range []bool{false, true} {
			t.Run(fmt.Sprintf("skip=%t/existed=%t", skip, existed), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "cli.state")
				if existed {
					if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				suite := &Suite{Ledger: NewLedger(t.TempDir()), EnvLedger: NewEnvLedger()}
				if skip {
					WithoutCLI()(suite)
				}
				if err := suite.snapshotCLIStatePath(path); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("concurrent-login"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := suite.Teardown(); err != nil {
					t.Fatal(err)
				}
				body, err := os.ReadFile(path)
				if !skip && !existed {
					if !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("CLI state was not removed: %v", err)
					}
				} else {
					expected := "original"
					if skip {
						expected = "concurrent-login"
					}
					if err != nil || string(body) != expected {
						t.Fatalf("state=%q err=%v", body, err)
					}
				}
			})
		}
	}
}

func TestNewSuiteWithoutCLIUsesOwnedWorkspace(t *testing.T) {
	t.Setenv("BDD_CLEANUP_MODE", "")
	root := t.TempDir()
	cfg := Config{RepoRoot: root, OutDir: root, CLIPath: filepath.Join(root, "bin", "cli"), LedgerDir: filepath.Join(root, "originals"), CommandLogDir: filepath.Join(root, "logs"), DiagnosticsDir: filepath.Join(root, "diagnostics")}
	previous := os.Getenv("TMPDIR")
	suite, err := newSuite(t, cfg, WithoutCLI(), WithTempWorkspace())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := suite.Teardown(); err != nil {
			t.Errorf("fallback teardown: %v", err)
		}
	})
	workspace := os.Getenv("TMPDIR")
	if workspace == previous {
		t.Fatal("workspace was not isolated")
	}
	if _, err := os.Stat(cfg.CLIPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("CLI was built: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "secret"), []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := suite.Teardown(); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("TMPDIR") != previous {
		t.Fatal("TMPDIR was not restored")
	}
	if _, err := os.Stat(workspace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace remained: %v", err)
	}
}

func TestTempWorkspaceResolvesRelativeBase(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base, err = filepath.Abs(base)
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(cwd, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("initial-absolute-base", testTempWorkspaceResolvesRelativeBase)
	t.Run("initial-relative-base", func(t *testing.T) {
		t.Setenv("TMPDIR", relative)
		testTempWorkspaceResolvesRelativeBase(t)
	})
}

func testTempWorkspaceResolvesRelativeBase(t *testing.T) {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(resolved)
	if err != nil {
		t.Fatal(err)
	}
	childDir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(cwd, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", relative)
	suite := &Suite{Ledger: NewLedger(root), EnvLedger: NewEnvLedger()}
	if err := suite.createTempWorkspace(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := suite.Teardown(); err != nil {
			t.Errorf("fallback teardown: %v", err)
		}
	})
	workspace := os.Getenv("TMPDIR")
	if !filepath.IsAbs(workspace) {
		t.Fatalf("relative workspace exported: %q", workspace)
	}
	command := exec.Command("/bin/sh", "-ec", `mktemp -d "$TMPDIR/child.XXXXXX"`)
	command.Dir = childDir
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("child mktemp: %v: %s", err, output)
	}
	childPath := strings.TrimSpace(string(output))
	inside, err := filepath.Rel(workspace, childPath)
	if err != nil || !filepath.IsLocal(inside) {
		t.Fatalf("child workspace=%q err=%v", childPath, err)
	}
	if err := suite.Teardown(); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("TMPDIR") != relative {
		t.Fatal("original relative TMPDIR was not restored")
	}
	if _, err := os.Stat(workspace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace remained: %v", err)
	}
}

func TestTempWorkspacePreservesSymlinkTraversal(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	physical := filepath.Join(root, "physical")
	if err := os.MkdirAll(filepath.Join(physical, "child"), 0o700); err != nil {
		t.Fatal(err)
	}
	expected := filepath.Join(physical, "tmp")
	if err := os.Mkdir(expected, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(physical, "child"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	// Keep link/.. uncleaned: the filesystem traverses the link before its parent.
	selected := root + "/link/../tmp"
	t.Setenv("TMPDIR", selected)
	suite := &Suite{Ledger: NewLedger(root), EnvLedger: NewEnvLedger()}
	if err := suite.createTempWorkspace(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := suite.Teardown(); err != nil {
			t.Errorf("fallback teardown: %v", err)
		}
	})
	got, err := os.Stat(filepath.Dir(suite.tempWorkspace))
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.Stat(expected)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(got, want) {
		t.Fatalf("TMPDIR selected the wrong parent: %s", suite.tempWorkspace)
	}
	if err := suite.Teardown(); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("TMPDIR") != selected {
		t.Fatal("original TMPDIR spelling was not restored")
	}
}
