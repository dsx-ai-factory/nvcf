// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeRCFixture(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
}

func newGuardFixture(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	workspace, home := filepath.Join(root, "workspace"), filepath.Join(root, "home")
	writeRCFixture(t, filepath.Join(workspace, "MODULE.bazel"), "")
	if err := os.Mkdir(home, 0700); err != nil {
		t.Fatal(err)
	}
	return workspace, home
}

func TestGuardRejectsFixedExternalValues(t *testing.T) {
	for _, flag := range []string{"--action_env", "--host_action_env", "--test_env"} {
		for _, scope := range []string{"common", "build", "test", "build:fixture"} {
			for _, separator := range []string{"=", " "} {
				t.Run(scope+flag+separator, func(t *testing.T) {
					workspace, home := newGuardFixture(t)
					path := filepath.Join(workspace, ".bazelrc")
					writeRCFixture(t, path, scope+" "+flag+separator+`"JAVA_TOOL_OPTIONS=-Dsecret=do-not-log"`+"\n")
					err := checkRCs("", workspace, home, nil, "", "")
					if err == nil || !strings.Contains(err.Error(), path+":1:") || !strings.Contains(err.Error(), flag) {
						t.Fatalf("fixed external option not diagnosed: %v", err)
					}
					if strings.Contains(err.Error(), "do-not-log") {
						t.Fatal("option content leaked")
					}
				})
			}
		}
	}
}

func TestGuardTraversesImportsWithBazelPathSemantics(t *testing.T) {
	workspace, home := newGuardFixture(t)
	writeRCFixture(t, filepath.Join(workspace, ".bazelrc"), "try-import %workspace%/absent.rc\nimport %workspace%/config/first.rc\n")
	// Relative imports are resolved from Bazel cwd, not config/first.rc.
	writeRCFixture(t, filepath.Join(workspace, "config", "first.rc"), "import second.rc\n")
	writeRCFixture(t, filepath.Join(workspace, "config", "second.rc"), "# unused file\n")
	writeRCFixture(t, filepath.Join(workspace, "second.rc"), "try-import %user.home%/extra.rc\n")
	path := filepath.Join(home, "extra.rc")
	writeRCFixture(t, path, "common --action_env=JAVA_TOOL_OPTIONS=-Dfoo=bar\n")
	err := checkRCs("", workspace, home, nil, "", "")
	if err == nil || !strings.Contains(err.Error(), path+":1:") {
		t.Fatalf("nested import not checked: %v", err)
	}
}

func TestGuardInlineHomeValuesAndInheritedReferences(t *testing.T) {
	workspace, home := newGuardFixture(t)
	writeRCFixture(t, filepath.Join(workspace, ".bazelrc"), "common --action_env=JAVA_TOOL_OPTIONS\ncommon --test_env=UNRELATED=fixed\n")
	inline := "common --action_env=JAVA_TOOL_OPTIONS=-Dfixture=preserved\n"
	if err := checkRCs(inline, workspace, home, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	writeRCFixture(t, filepath.Join(home, "imported.rc"), "common --host_action_env=JAVA_TOOL_OPTIONS=\n")
	if err := checkRCs(inline+"import %user.home%/imported.rc\n", workspace, home, nil, "", ""); err == nil {
		t.Fatal("home imported fixed value was accepted")
	}
}

func TestGuardChecksEverySelectedWorkspace(t *testing.T) {
	workspace, home := newGuardFixture(t)
	child := filepath.Join(workspace, "child")
	writeRCFixture(t, filepath.Join(child, "MODULE.bazel"), "")
	path := filepath.Join(child, ".bazelrc")
	writeRCFixture(t, path, "common --test_env=JAVA_TOOL_OPTIONS=-Dchild=kept\n")
	if err := checkRCs("", workspace, home, []string{"."}, "", ""); err != nil {
		t.Fatal(err)
	}
	err := checkRCs("", workspace, home, []string{".", "child"}, "", "")
	if err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("child workspace missed: %v", err)
	}
	subdir := filepath.Join(workspace, "inside")
	if err := os.Mkdir(subdir, 0700); err != nil {
		t.Fatal(err)
	}
	if got, err := workspaceRoot(subdir); err != nil || got != workspace {
		t.Fatalf("workspace ancestor: %q %v", got, err)
	}
}

func TestGuardAdditionalAndSystemRC(t *testing.T) {
	workspace, home := newGuardFixture(t)
	writeRCFixture(t, filepath.Join(workspace, "safe.rc"), "# inherited\n")
	writeRCFixture(t, filepath.Join(workspace, "fixed.rc"), "common --action_env=JAVA_TOOL_OPTIONS=-Dfoo=bar\n")
	for _, args := range [][2]string{{"safe.rc,fixed.rc", ""}, {"", filepath.Join(workspace, "fixed.rc")}} {
		err := checkRCs("", workspace, home, nil, args[0], args[1])
		if err == nil || !strings.Contains(err.Error(), "fixed JAVA_TOOL_OPTIONS") {
			t.Fatalf("extra rc missed: %v", err)
		}
	}
	if err := checkRCs("", workspace, home, nil, "", filepath.Join(workspace, "missing-system.rc")); err != nil {
		t.Fatal(err)
	}
}

func TestGuardRejectsUnverifiableImports(t *testing.T) {
	for _, input := range []string{
		"import missing.rc\n",
		"try-import %unknown%/optional.rc\n",
		"try-import-if-bazel-version >=9.0.0 optional.rc\n",
		"import a b\n",
		"import .bazelrc\n",
	} {
		t.Run(strings.TrimSpace(input), func(t *testing.T) {
			workspace, home := newGuardFixture(t)
			writeRCFixture(t, filepath.Join(workspace, ".bazelrc"), input)
			if err := checkRCs("", workspace, home, nil, "", ""); err == nil {
				t.Fatal("unverifiable import accepted")
			}
		})
	}
}

func TestGuardRejectsSymlinkImportCycles(t *testing.T) {
	workspace, home := newGuardFixture(t)
	path := filepath.Join(workspace, ".bazelrc")
	writeRCFixture(t, path, "import alias.rc\n")
	if err := os.Symlink(path, filepath.Join(workspace, "alias.rc")); err != nil {
		t.Skip(err)
	}
	err := checkRCs("", workspace, home, nil, "", "")
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cycle not detected: %v", err)
	}
}

func TestGuardPhysicalLineNumbersAndNoOutputOnFailure(t *testing.T) {
	workspace, home := newGuardFixture(t)
	path := filepath.Join(workspace, ".bazelrc")
	writeRCFixture(t, path, "# heading\r\nbuild --jvmopt=-Dfirst=one \\\r\n--jvmopt=-Dsecond=two\r\ncommon --action_env=JAVA_TOOL_OPTIONS=-Dsecret=hidden\r\n")
	var output bytes.Buffer
	err := configure(strings.NewReader("build --color=no\n"), &output, func(input string) error {
		return checkRCs(input, workspace, home, nil, "", "")
	})
	if err == nil || !strings.Contains(err.Error(), path+":4:") || strings.Contains(err.Error(), "hidden") || output.Len() != 0 {
		t.Fatalf("incorrect source location or output on failure: %v, %q", err, output.String())
	}
}

func TestGuardMissingWorkspaceAndUnreadableFiles(t *testing.T) {
	workspace, home := newGuardFixture(t)
	if err := checkRCs("", workspace, home, []string{"absent"}, "", ""); err == nil {
		t.Fatal("missing directory accepted")
	}
	if err := checkRCs("", home, home, nil, "", ""); err == nil {
		t.Fatal("missing workspace marker accepted")
	}
	writeRCFixture(t, filepath.Join(workspace, ".bazelrc"), "try-import directory.rc\n")
	if err := os.Mkdir(filepath.Join(workspace, "directory.rc"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := checkRCs("", workspace, home, nil, "", ""); err == nil {
		t.Fatal("unreadable optional file accepted")
	}
}
