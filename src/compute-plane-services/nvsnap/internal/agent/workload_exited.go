// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// A capture can fail because the workload ended under it: the container's
// process exits between the agent finding it and CRIU seizing it, and CRIU
// reports a missing task ("The root task ... does not exist") or cgroup.
// That is the workload's doing, not a fault of nvsnap's: the dump's error
// says so (errWorkloadExited), which its outcome's reason
// (metrics.Reason: workload_exited) and the configuration's record carry.
// It is told from the process itself, never from CRIU's words: the
// container's process is gone, or another process took its pid. Only a
// dump that failed before CRIU dumped the container is checked (dumpV2):
// a dump that completed stopped the process itself, so a failure after it
// is not the workload's.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
)

// errWorkloadExited marks a dump that failed because the workload's
// process had ended.
var errWorkloadExited = errors.New(metrics.WorkloadExitedText)

// errNoGPUProcess is the error of a multi-node instance's pod that has no
// process on the GPU under the gpushare library to capture.
func errNoGPUProcess(session string) error {
	return fmt.Errorf("gpushare: fabric session %s, but no process of this pod runs on the GPU under %s", session, gpushareLibName)
}

// procStartTime returns the start time of process pid under procBase, in
// clock ticks since boot (field 22 of /proc/<pid>/stat), which tells the
// process from a later one given the same pid.
func procStartTime(procBase string, pid int) (uint64, error) {
	b, err := os.ReadFile(filepath.Join(procBase, strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, err
	}
	// The command name, in parentheses, may hold spaces and parentheses:
	// the fields that follow start after its last ')'.
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, fmt.Errorf("%s/%d/stat: no command name", procBase, pid)
	}
	f := strings.Fields(s[i+1:])
	const startTimeField = 22 - 3 // fields 1 and 2 are the pid and the name
	if len(f) <= startTimeField {
		return 0, fmt.Errorf("%s/%d/stat: %d fields after the name", procBase, pid, len(f))
	}
	return strconv.ParseUint(f[startTimeField], 10, 64)
}

// processEnded reports whether process pid, which started at start, has
// ended: its pid is free, or held by a later process. A start of 0 (not
// read) or a stat that cannot be read for another reason tells nothing.
func processEnded(procBase string, pid int, start uint64) bool {
	if start == 0 {
		return false
	}
	now, err := procStartTime(procBase, pid)
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	return err == nil && now != start
}

// workloadExitedErr is err, marked errWorkloadExited when process pid
// (which started at start) ended by the time its dump failed.
func workloadExitedErr(err error, procBase string, pid int, start uint64) error {
	if err == nil || errors.Is(err, errWorkloadExited) || !processEnded(procBase, pid, start) {
		return err
	}
	return fmt.Errorf("%w: %w", errWorkloadExited, err)
}

// initialStart reads process pid's start time when its dump begins. gone
// reports the process already ended (its stat does not exist); any other
// read error gives start 0 and gone false, which tells nothing.
func initialStart(procBase string, pid int) (start uint64, gone bool) {
	start, err := procStartTime(procBase, pid)
	if err != nil {
		return 0, errors.Is(err, os.ErrNotExist)
	}
	return start, false
}

// dumpFailure is the error of a dump that failed with err: marked
// errWorkloadExited when CRIU had not dumped the container (dumped false)
// and its process, pid (started at start), had ended, or was already gone
// when the dump began (gone).
func dumpFailure(err error, dumped bool, procBase string, pid int, start uint64, gone bool) error {
	if dumped {
		return err
	}
	if gone && err != nil && !errors.Is(err, errWorkloadExited) {
		return fmt.Errorf("%w: %w", errWorkloadExited, err)
	}
	return workloadExitedErr(err, procBase, pid, start)
}
