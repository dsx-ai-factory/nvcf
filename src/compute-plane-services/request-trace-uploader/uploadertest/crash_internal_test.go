// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package uploadertest

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const envInertDir = "UPLOADERTEST_INERT_DIR"

// TestCrashChildOutsideTestBinary hits a crash point the way a non-test
// binary would. A Go build tool is not available under Bazel, so the child is
// still a test binary and the seam stands in for testing.Testing.
func TestCrashChildOutsideTestBinary(t *testing.T) {
	if !CrashChild() {
		t.Skip("runs only as a crash child")
	}
	crashAt("armed", false)
	if err := os.WriteFile(filepath.Join(os.Getenv(envInertDir), "after"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCrashPointIsInertOutsideATestBinary(t *testing.T) {
	dir := t.TempDir()
	result := RunCrash(t, "TestCrashChildOutsideTestBinary", "armed", envInertDir+"="+dir)

	if result.Killed {
		t.Fatalf("a non-test binary was killed at an armed crash point\n%s", result.Output)
	}
	if _, err := os.Stat(filepath.Join(dir, "after")); err != nil {
		t.Errorf("work after the crash point did not run: %v", err)
	}
}

const envTempReport = "UPLOADERTEST_TEMP_REPORT"

// TestCrashChildTempDir is a crash child that makes a temporary directory
// and then dies, so its own cleanup never runs.
func TestCrashChildTempDir(t *testing.T) {
	if !CrashChild() {
		t.Skip("runs only as a crash child")
	}
	if err := os.WriteFile(os.Getenv(envTempReport), []byte(t.TempDir()), 0o600); err != nil {
		t.Fatal(err)
	}
	CrashPoint("made-temp")
}

func TestCrashChildTempDirIsInsideTheParentsTempDir(t *testing.T) {
	var childTemp string
	report := filepath.Join(t.TempDir(), "report")
	t.Run("run", func(t *testing.T) {
		result := RunCrash(t, "TestCrashChildTempDir", "made-temp", envTempReport+"="+report)
		if !result.Killed {
			t.Fatalf("child was not killed\n%s", result.Output)
		}
		data, err := os.ReadFile(report)
		if err != nil {
			t.Fatal(err)
		}
		childTemp = string(data)
		if _, err := os.Stat(childTemp); err != nil {
			t.Fatalf("child temp dir: %v", err)
		}
	})
	// The subtest's cleanup removed its temporary directories, which must
	// have held the child's.
	if _, err := os.Stat(childTemp); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("child temp dir %s after the parent cleanup: %v, want it removed", childTemp, err)
	}
}

// fatalRecorder stands in for a test so a Fatalf from RunCrash can be read.
type fatalRecorder struct {
	testing.TB
	message string
}

func (f *fatalRecorder) Helper() {}

func (f *fatalRecorder) Fatalf(format string, args ...any) {
	f.message = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func TestRunCrashReportsAChildThatCannotStart(t *testing.T) {
	original := childBinary
	t.Cleanup(func() { childBinary = original })
	childBinary = filepath.Join(t.TempDir(), "missing-binary")

	recorder := &fatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunCrash(recorder, "TestCrashChildTempDir", "made-temp")
	}()
	<-done
	if !strings.Contains(recorder.message, "start crash child") {
		t.Fatalf("RunCrash failure = %q, want the start error", recorder.message)
	}
}

const (
	envHolderReport = "UPLOADERTEST_HOLDER_REPORT"
	envHold         = "UPLOADERTEST_HOLD"
)

// TestCrashChildOutputHolder stands in for a process a crash child leaves
// behind: it holds the child's output open for a minute.
func TestCrashChildOutputHolder(t *testing.T) {
	if os.Getenv(envHold) == "" {
		t.Skip("runs only as a crash child's descendant")
	}
	time.Sleep(time.Minute)
}

// TestCrashChildLeavesOutputHolder is a crash child that starts a process
// sharing its output, then reaches the crash point.
func TestCrashChildLeavesOutputHolder(t *testing.T) {
	if !CrashChild() {
		t.Skip("runs only as a crash child")
	}
	holder := exec.Command(os.Args[0], "-test.run=^TestCrashChildOutputHolder$")
	holder.Env = append(os.Environ(), envHold+"=1")
	holder.Stdout, holder.Stderr = os.Stdout, os.Stderr
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv(envHolderReport), []byte(strconv.Itoa(holder.Process.Pid)), 0o600); err != nil {
		t.Fatal(err)
	}
	CrashPoint("holding")
}

// A descendant that keeps the child's output open does not hold RunCrash
// past crashWaitDelay.
func TestRunCrashDoesNotWaitOnADescendantHoldingTheOutput(t *testing.T) {
	cases := []struct {
		name        string
		point       string
		wantKilled  bool
		wantFailure string
	}{
		{name: "child killed", point: "holding", wantKilled: true},
		{name: "child passed", point: "not-a-point", wantFailure: "left a process holding its output"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			report := filepath.Join(t.TempDir(), "holder")
			t.Cleanup(func() { killHolder(t, report) })

			recorder := &fatalRecorder{TB: t}
			done := make(chan CrashResult, 1)
			go func() {
				defer close(done)
				done <- RunCrash(recorder, "TestCrashChildLeavesOutputHolder", tc.point, envHolderReport+"="+report)
			}()
			var result CrashResult
			select {
			case result = <-done:
			case <-time.After(crashWaitDelay + 15*time.Second):
				t.Fatal("RunCrash still waiting on the output a descendant holds")
			}
			if result.Killed != tc.wantKilled || !strings.Contains(recorder.message, tc.wantFailure) {
				t.Fatalf("killed = %v, failure = %q; want killed %v and failure %q", result.Killed, recorder.message, tc.wantKilled, tc.wantFailure)
			}
		})
	}
}

func killHolder(t *testing.T, report string) {
	data, err := os.ReadFile(report)
	if err != nil {
		return
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		t.Errorf("holder pid %q: %v", data, err)
		return
	}
	if process, err := os.FindProcess(pid); err == nil {
		_ = process.Kill()
	}
}

// TestCrashChildKilledBeforeItsPoint is a crash child that dies by SIGKILL
// without reaching any crash point, as an outside kill would.
func TestCrashChildKilledBeforeItsPoint(t *testing.T) {
	if !CrashChild() {
		t.Skip("runs only as a crash child")
	}
	process, err := os.FindProcess(os.Getpid())
	if err == nil {
		err = process.Kill()
	}
	t.Fatalf("kill: %v", err)
}

// A child killed before it reported the requested crash point did not crash
// where the test meant, so the harness fails instead of returning Killed.
func TestRunCrashFailsWhenTheChildDiesBeforeItsPoint(t *testing.T) {
	recorder := &fatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunCrash(recorder, "TestCrashChildKilledBeforeItsPoint", "never-reached")
	}()
	<-done
	if !strings.Contains(recorder.message, "without reaching crash point") {
		t.Fatalf("RunCrash message = %q, want a failure for a child killed before its crash point", recorder.message)
	}
}
