// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// The pid-namespace dump settles its source the same way.
func TestSettleSource(t *testing.T) {
	for _, c := range []struct {
		suspended, leave, failed bool
		want                     int
	}{
		{true, true, false, 1}, {true, false, false, 0}, {true, false, true, 1}, {true, true, true, 1},
		{false, true, true, 0},
	} {
		n := 0
		if err := settleSource(c.suspended, c.leave, c.failed, func() error { n++; return nil }); err != nil || n != c.want {
			t.Errorf("%+v: %d resumes, err %v; want %d", c, n, err, c.want)
		}
	}
	if err := settleSource(true, true, false, func() error { return errors.New("x") }); !errors.Is(err, errSourceLeftSuspended) {
		t.Errorf("a failed resume reports the source suspended: %v", err)
	}
}

// A capture that fails after the suspend and before its dump (the snapshot
// hook directory cannot be made, the action script cannot be written)
// brings the source back, and tells a group's driver the member failed.
// Once the dump has run its own settlement owns the source, and the guard
// does nothing.
func TestSuspendGuard_SettlesAFailureBeforeTheDump(t *testing.T) {
	prepErr, resumeFailed := syscall.ENOSPC, errors.New("resume failed")
	run := func(arm, handOff bool, resume, prep error) (resumes, aborts int, err error) {
		capture := func() (retErr error) {
			var g suspendGuard
			if arm {
				g.arm(func() error { resumes++; return resume }, func() { aborts++ })
				defer func() { retErr = g.settle(retErr) }()
			}
			if handOff {
				g.handOff()
			}
			return prep
		}
		return resumes, aborts, capture()
	}

	resumes, aborts, err := run(true, false, nil, prepErr)
	if resumes != 1 || aborts != 1 || !errors.Is(err, prepErr) || errors.Is(err, errSourceLeftSuspended) {
		t.Errorf("preparation fails: resumes=%d aborts=%d err=%v; want the source resumed, the group told and the preparation error", resumes, aborts, err)
	}
	resumes, _, err = run(true, false, resumeFailed, prepErr)
	if resumes != 1 || !errors.Is(err, prepErr) || !errors.Is(err, errSourceLeftSuspended) || !errors.Is(err, resumeFailed) {
		t.Errorf("preparation and resume fail: resumes=%d err=%v; want both reported, the source as left suspended", resumes, err)
	}
	if resumes, aborts, err = run(true, true, nil, prepErr); resumes != 0 || aborts != 0 || !errors.Is(err, prepErr) {
		t.Errorf("after the dump: resumes=%d aborts=%d err=%v; the dump's settlement owns the source", resumes, aborts, err)
	}
	if resumes, _, err = run(false, false, nil, prepErr); resumes != 0 || !errors.Is(err, prepErr) {
		t.Errorf("nothing suspended: resumes=%d err=%v", resumes, err)
	}
	// Settled once even if settle runs again.
	var g suspendGuard
	n := 0
	g.arm(func() error { n++; return nil }, nil)
	e := g.settle(g.settle(nil))
	if n != 1 || e != nil {
		t.Errorf("settle twice: %d resumes, err %v; want 1, nil", n, e)
	}
}

// A resume whose GPU launches were let through before every node of the
// instance resumed (the tool's --sync-open gave up waiting) leaves the
// workload running: the capture fails as stalled, and the source is not
// reported left suspended, which would replace the instance.
func TestSettleSource_UnsyncedOpenIsNotLeftSuspended(t *testing.T) {
	unsynced := fmt.Errorf("%w: sync-open: the driver aborted; %s", errResumeStalled, openUnsyncedText)
	err := settleSource(true, true, false, func() error { return unsynced })
	if !errors.Is(err, errResumeStalled) || errors.Is(err, errSourceLeftSuspended) {
		t.Errorf("settleSource = %v; want stalled, not left suspended", err)
	}
	if sourceMustBeReplacedText(err.Error()) {
		t.Errorf("an unsynced open replaces the instance: %v", err)
	}
	var g suspendGuard
	g.arm(func() error { return unsynced }, nil)
	err = g.settle(syscall.ENOSPC)
	if !errors.Is(err, errResumeStalled) || errors.Is(err, errSourceLeftSuspended) || !errors.Is(err, syscall.ENOSPC) {
		t.Errorf("guard.settle = %v; want the capture's error and stalled, not left suspended", err)
	}
}

// A suspend holds the in-place resume's GPU launches (--sync-open) only in
// place, across nodes, when the driver runs the open phase, with a tool that
// has the option, and then declares it to the driver before the suspend.
// Rolling upgrade: an older driver omits the request field, so a newer
// member resumes at once; an older member ignores the field and never
// declares a hold, so a newer driver does not wait for it (fabricOpener).
func TestSyncOpenFor(t *testing.T) {
	log := logrus.NewEntry(logrus.New())
	tool := "--sync-open MS: (suspend, with --fabric-map) ..."
	for _, c := range []struct {
		name         string
		leaveRunning bool
		driverOpens  bool
		fabric       bool
		tool         string
		want         time.Duration
	}{
		{"in place across nodes, new driver and tool", true, true, true, tool, gpushareSyncOpenWait},
		{"older driver, new tool", true, false, true, tool, 0},
		{"new driver, older tool", true, true, true, "--hold-gate --fabric-map DIR", 0},
		{"older driver and tool", true, false, true, "--hold-gate --fabric-map DIR", 0},
		{"dump for a move", false, true, true, tool, 0},
		{"one node", true, true, false, tool, 0},
	} {
		var f *fabricSessionDir
		if c.fabric {
			f = &fabricSessionDir{}
		}
		if got := syncOpenFor(c.leaveRunning, c.driverOpens, f, c.tool, "v1", log); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
		if f != nil && f.holdsOpen.Load() != (c.want > 0) {
			t.Errorf("%s: holdsOpen = %v", c.name, f.holdsOpen.Load())
		}
	}
}
