// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
)

// writeStat writes a /proc/<pid>/stat under procBase for a process named
// comm that started at start.
func writeStat(t *testing.T, procBase string, pid int, comm string, start uint64) {
	t.Helper()
	dir := filepath.Join(procBase, strconv.Itoa(pid))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Fields 3 to 21, then the start time (22), then more.
	stat := fmt.Sprintf("%d (%s) S 1 %d %d 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 %d 1000 10 18446744073709551615\n", pid, comm, pid, pid, start)
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(stat), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestProcStartTime(t *testing.T) {
	proc := t.TempDir()
	writeStat(t, proc, 42, "python3", 123456)
	writeStat(t, proc, 43, "a (weird) name", 777)
	for pid, want := range map[int]uint64{42: 123456, 43: 777} {
		got, err := procStartTime(proc, pid)
		if err != nil || got != want {
			t.Errorf("procStartTime(%d) = %d, %v; want %d", pid, got, err, want)
		}
	}
	if _, err := procStartTime(proc, 44); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a missing process: %v, want not-exist", err)
	}
	if err := os.WriteFile(filepath.Join(proc, "42", "stat"), []byte("42 (short) S 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := procStartTime(proc, 42); err == nil {
		t.Error("a truncated stat read as a start time")
	}
}

// A dump that failed after the workload's process ended says so, and its
// reason is the workload's; one whose process still runs, or whose start
// was never read, keeps its error as it is.
func TestWorkloadExitedErr(t *testing.T) {
	proc := t.TempDir()
	dumpErr := errors.New("criu-v2 pid-namespace dump: exit status 1 (dump.log tail: Error (criu/seize.c:1073): The root task 7 does not exist)")
	writeStat(t, proc, 7, "server", 100)
	writeStat(t, proc, 8, "server", 200)

	for _, tc := range []struct {
		name   string
		pid    int
		start  uint64
		exited bool
	}{
		{"still running", 7, 100, false},
		{"pid taken by a later process", 8, 150, true},
		{"gone", 9, 100, true},
		{"start never read", 9, 0, false},
	} {
		err := workloadExitedErr(dumpErr, proc, tc.pid, tc.start)
		if got := errors.Is(err, errWorkloadExited); got != tc.exited {
			t.Errorf("%s: exited = %v, want %v (%v)", tc.name, got, tc.exited, err)
		}
		if !errors.Is(err, dumpErr) {
			t.Errorf("%s: the dump's error is lost: %v", tc.name, err)
		}
		want := "criu"
		if tc.exited {
			want = "workload_exited"
		}
		if got := metrics.Reason("CRIU dump failed (criu-v2): " + err.Error()); got != want {
			t.Errorf("%s: reason %q, want %q", tc.name, got, want)
		}
	}
	if workloadExitedErr(nil, proc, 9, 100) != nil {
		t.Error("no error became one")
	}
	twice := workloadExitedErr(workloadExitedErr(dumpErr, proc, 9, 100), proc, 9, 100)
	if got := twice.Error(); got != fmt.Sprintf("%s: %s", errWorkloadExited, dumpErr) {
		t.Errorf("marked twice: %q", got)
	}
}

// The agent's error for a multi-node pod with nothing on the GPU is the one
// the metrics class as no_gpu_process.
func TestErrNoGPUProcess_Reason(t *testing.T) {
	if got := metrics.Reason("CRIU dump failed (criu-v2): " + errNoGPUProcess("s1").Error()); got != "no_gpu_process" {
		t.Fatalf("reason %q, want no_gpu_process", got)
	}
}

// The node's own /proc reads the same way: this process's start time,
// twice the same.
func TestProcStartTime_Self(t *testing.T) {
	a, err := procStartTime("/proc", os.Getpid())
	if err != nil {
		t.Skipf("no /proc here: %v", err)
	}
	b, err := procStartTime("/proc", os.Getpid())
	if err != nil || a == 0 || a != b || processEnded("/proc", os.Getpid(), a) {
		t.Fatalf("start %d then %d (%v)", a, b, err)
	}
}

// Once CRIU has dumped the container, the dump itself stopped its process:
// a failure after that is not the workload's, gone process or not.
func TestDumpFailure_AfterTheDump(t *testing.T) {
	proc := t.TempDir() // pid 9 is gone
	err := errors.New("dump snapshot: not taken")
	if got := dumpFailure(err, true, proc, 9, 100, false); errors.Is(got, errWorkloadExited) || got.Error() != err.Error() {
		t.Errorf("after the dump: %v, want the error as it is", got)
	}
	if got := dumpFailure(err, false, proc, 9, 100, false); !errors.Is(got, errWorkloadExited) {
		t.Errorf("before the dump, the process gone: %v, want it marked", got)
	}
	if dumpFailure(nil, false, proc, 9, 100, false) != nil {
		t.Error("a dump that succeeded failed")
	}
}

// A workload already gone when its dump starts is told apart from a start
// time that could not be read: the first is the workload's exit, the
// second tells nothing.
func TestDumpFailure_GoneBeforeTheDump(t *testing.T) {
	proc := t.TempDir() // pid 9 is gone
	start, gone := initialStart(proc, 9)
	if start != 0 || !gone {
		t.Fatalf("a missing process: start %d, gone %v; want 0, true", start, gone)
	}
	err := errors.New("bundle tools: no such process")
	if got := dumpFailure(err, false, proc, 9, start, gone); !errors.Is(got, errWorkloadExited) {
		t.Errorf("gone before the dump: %v, want it marked", got)
	}
	if got := dumpFailure(err, true, proc, 9, start, gone); errors.Is(got, errWorkloadExited) {
		t.Errorf("gone, but CRIU dumped it: %v, want the error as it is", got)
	}

	// Another read error (a stat without a start time) tells nothing.
	writeStat(t, proc, 10, "server", 100)
	if err := os.WriteFile(filepath.Join(proc, "10", "stat"), []byte("10 (short) S 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	start, gone = initialStart(proc, 10)
	if start != 0 || gone {
		t.Fatalf("an unreadable start: start %d, gone %v; want 0, false", start, gone)
	}
	if got := dumpFailure(err, false, proc, 10, start, gone); errors.Is(got, errWorkloadExited) {
		t.Errorf("an unreadable start: %v, want the error as it is", got)
	}

	// A process read at the start keeps the existing check.
	writeStat(t, proc, 11, "server", 100)
	start, gone = initialStart(proc, 11)
	if start != 100 || gone {
		t.Fatalf("a running process: start %d, gone %v; want 100, false", start, gone)
	}
}

// CRIU completed, but the marker could not be written: the dump is
// reported done, so its failure is nvsnap's, never the workload's, even
// with the process gone since.
func TestPIDNSDumpDone_MarkerFails(t *testing.T) {
	proc := t.TempDir() // pid 9 is gone (the dump stopped it)
	dumped, err := pidNSDumpDone(filepath.Join(t.TempDir(), "missing", "dir"), pidNSCapture{})
	if !dumped || err == nil {
		t.Fatalf("marker write into a missing directory: dumped %v, err %v; want true and an error", dumped, err)
	}
	if got := dumpFailure(err, dumped, proc, 9, 100, false); errors.Is(got, errWorkloadExited) {
		t.Errorf("a marker failure after CRIU dumped: %v, want it not marked workload_exited", got)
	}
	dir := t.TempDir()
	if dumped, err := pidNSDumpDone(dir, pidNSCapture{Subtree: true}); !dumped || err != nil {
		t.Fatalf("marker write: dumped %v, err %v", dumped, err)
	}
	if c, ok, err := readPIDNSMarker(dir); err != nil || !ok || !c.Subtree {
		t.Errorf("marker read back: %+v %v %v", c, ok, err)
	}
}
