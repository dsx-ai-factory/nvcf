// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Checkpoint disk. A capture writes its pods' memory to the node's
// checkpoint filesystem: about 570 GB per rank of a large model. On a
// node whose checkpoint directory shares the kubelet's image filesystem,
// a capture that fills it past the kubelet's eviction threshold makes the
// kubelet evict every pod on the node, the instance's ranks and the agent
// among them, and keep doing so: what fills the disk is not a pod's, so
// evicting pods frees nothing.
//
// So the capture preflight (capture_preflight.go) checks, on each
// member's node, that the member's dump, with the dumps of the other
// members this node's preflight passed in the last diskReserveTTL, leaves
// CaptureDiskKeepPercent of the filesystem free. A dump is its processes'
// anonymous memory, the GPU memory they use and the deleted files it saves
// whole; when the GPU memory cannot be read, the capture is blocked. A node short of it first retires the checkpoints
// no capture will restore again without waiting checkpointRetireAge
// (pressureRetireAge); if it is still short, the capture is blocked before
// any member suspends, and the instance keeps serving. The node reaper
// also retires them early while the filesystem is low.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// pressureRetireAge is how old a checkpoint no record names must be to
// be retired while the checkpoint filesystem is low: past any capture and
// L2 promote still running.
const pressureRetireAge = 15 * time.Minute

// diskReserveTTL is how long a dump the preflight passed counts against
// the node's free space: past the group's freeze wait (groupFreezeWait),
// by when the dump is on disk or the capture is over.
const diskReserveTTL = groupFreezeWait

// diskReservation is a dump the preflight passed on this node, not yet on
// its disk.
type diskReservation struct {
	bytes int64
	at    time.Time
}

// diskReservations are this node's passed dumps, by pod.
var (
	diskReserveMu    sync.Mutex
	diskReservations = map[string]diskReservation{}
)

// gpuUsedBytes returns the GPU memory pids use, from nvidia-smi; a
// variable for tests.
var gpuUsedBytes = func(pids []int) (int64, error) {
	out, err := hostNvidiaSMI("--query-compute-apps=pid,used_memory", "--format=csv,noheader,nounits")
	if err != nil {
		return 0, err
	}
	return sumGPUUsed(string(out), pids), nil
}

// sumGPUUsed sums the used_memory (MiB) of nvidia-smi's "pid, used_memory"
// lines whose pid is in pids.
func sumGPUUsed(out string, pids []int) int64 {
	want := make(map[int]bool, len(pids))
	for _, p := range pids {
		want[p] = true
	}
	var mib int64
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		parts := strings.SplitN(line, ",", 2)
		if len(parts) != 2 {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil || !want[pid] {
			continue
		}
		if n, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil {
			mib += n
		}
	}
	return mib << 20
}

// captureBytes estimates the dump of a container whose processes are
// pids: their anonymous memory, the GPU memory they use and the deleted
// files the dump saves whole (each once, up to the ghost limit; a larger
// one blocks the capture on its own). An error when the GPU memory cannot
// be read on a node that has GPUs.
func (a *Agent) captureBytes(procBase string, pids []int) (int64, error) {
	var n int64
	ghosts := map[string]ghostFile{}
	for _, pid := range pids {
		anon, err := anonymousBytesOf(procBase, pid)
		if err != nil {
			return 0, err
		}
		n += anon
		deletedFiles(procBase, pid, ghosts)
	}
	limit := a.ghostLimit()
	for _, g := range ghosts {
		if g.allocated <= limit {
			n += g.allocated
		}
	}
	gpu, err := gpuUsedBytes(pids)
	if errors.Is(err, errNoNvidiaSMI) {
		return n, nil
	}
	if err != nil {
		return 0, fmt.Errorf("cannot read the GPU memory the dump saves: %w", err)
	}
	return n + gpu, nil
}

// anonymousBytesOf is anonymousBytes that fails when the measure cannot
// be read; a process gone since it was listed has nothing to dump.
func anonymousBytesOf(procBase string, pid int) (int64, error) {
	f, err := os.Open(filepath.Join(procBase, strconv.Itoa(pid), "smaps_rollup"))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("cannot read the memory of pid %d: %w", pid, err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(sc.Text(), "Anonymous:"); ok {
			kb, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(rest), " kB"), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("pid %d: unreadable Anonymous line %q", pid, rest)
			}
			return kb << 10, nil
		}
	}
	if err := sc.Err(); err != nil {
		return 0, fmt.Errorf("cannot read the memory of pid %d: %w", pid, err)
	}
	return 0, fmt.Errorf("pid %d: no Anonymous line in smaps_rollup", pid)
}

// reservedBytes is the sum of the live reservations of pods other than
// key, dropping expired ones.
func reservedBytes(key string, now time.Time) int64 {
	diskReserveMu.Lock()
	defer diskReserveMu.Unlock()
	var n int64
	for k, r := range diskReservations {
		if now.Sub(r.at) > diskReserveTTL {
			delete(diskReservations, k)
			continue
		}
		if k != key {
			n += r.bytes
		}
	}
	return n
}

// reserveDisk reserves bytes for key's dump. A live reservation of key that
// is larger stays as it is: a re-check (the on-demand preflight report)
// while the dump is still being written must not lower what it needs.
func reserveDisk(key string, bytes int64, now time.Time) {
	diskReserveMu.Lock()
	defer diskReserveMu.Unlock()
	if r, ok := diskReservations[key]; ok && now.Sub(r.at) <= diskReserveTTL && r.bytes >= bytes {
		return
	}
	diskReservations[key] = diskReservation{bytes: bytes, at: now}
}

// checkpointRoom is how many bytes the checkpoint filesystem takes before
// it has less than CaptureDiskKeepPercent free; on is false with the
// check off. The capture's own writes after the dump (the rootfs diff, the
// replay mounts) check it before they write.
func (a *Agent) checkpointRoom() (room int64, on bool, err error) {
	keep := a.config.CaptureDiskKeepPercent
	if keep <= 0 || keep >= 100 {
		return 0, false, nil
	}
	avail, total, err := diskSpace(a.config.CheckpointDir)
	if err != nil {
		return 0, true, err
	}
	reserve := total / 100 * uint64(keep) //nolint:gosec // keep checked to be 1..99
	if avail <= reserve {
		return 0, true, nil
	}
	return int64(avail - reserve), true, nil //nolint:gosec // bounded by the filesystem size
}

// diskFindings is the checkpoint-disk check of a container whose
// processes are pids: a block when its dump would leave less than
// CaptureDiskKeepPercent of the checkpoint filesystem free, after
// retiring what this node can.
func (a *Agent) diskFindings(ctx context.Context, procBase string, pids []int, pod, container string) []PreflightFinding {
	keep := a.config.CaptureDiskKeepPercent
	if keep <= 0 || keep >= 100 {
		return nil
	}
	block := func(detail string) []PreflightFinding {
		return []PreflightFinding{{Pod: pod, Container: container, Severity: preflightBlock, Check: "disk", Detail: detail}}
	}
	own, err := a.captureBytes(procBase, pids)
	if err != nil {
		return block(err.Error() + "; the capture is skipped rather than risk filling the checkpoint disk")
	}
	// The diskFindings of one node's members run concurrently: serialise
	// them so each counts the others' passed dumps.
	diskCheckMu.Lock()
	defer diskCheckMu.Unlock()
	key := pod + "/" + container
	size := own + reservedBytes(key, time.Now())
	err = checkFreeSpace(a.config.CheckpointDir, size, keep)
	if err == nil {
		reserveDisk(key, own, time.Now())
		return nil
	}
	a.log.WithError(err).WithField("pod", pod).Warn("capture preflight: the checkpoint filesystem is short of room; retiring the checkpoints no capture restores again")
	a.retireForSpace(ctx)
	if err = checkFreeSpace(a.config.CheckpointDir, size, keep); err == nil {
		reserveDisk(key, own, time.Now())
		return nil
	}
	return block(fmt.Sprintf("the dump (about %d GiB, %d GiB with the other members' on this node) does not fit: %v; the kubelet evicts every pod on a node whose disk runs this low", own>>30, size>>30, err))
}

// diskCheckMu serialises this node's disk checks.
var diskCheckMu sync.Mutex

// checkpointDiskLow reports whether the checkpoint filesystem has less
// than CaptureDiskKeepPercent plus 10 points free: the reaper then retires
// early (pressureRetireAge).
func (a *Agent) checkpointDiskLow() bool {
	keep := a.config.CaptureDiskKeepPercent
	if keep <= 0 || keep >= 100 {
		return false
	}
	avail, total, err := diskSpace(a.config.CheckpointDir)
	if err != nil || total == 0 {
		return false
	}
	return avail < total/100*uint64(min(keep+10, 99)) //nolint:gosec // keep checked to be 1..99
}

// retireForSpace retires, now, this node's checkpoints no capture will
// restore again that are older than pressureRetireAge.
func (a *Agent) retireForSpace(ctx context.Context) {
	if a.kubeClient == nil || a.config.NodeName == "" {
		return
	}
	pods, err := a.kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + a.config.NodeName})
	if err != nil {
		a.log.WithError(err).Warn("capture preflight: cannot list this node's pods; retiring nothing")
		return
	}
	a.reapCheckpointsOlderThan(ctx, pods.Items, time.Now(), pressureRetireAge)
}
