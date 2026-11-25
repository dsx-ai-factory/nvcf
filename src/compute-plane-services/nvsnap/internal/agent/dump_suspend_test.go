// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// dumpSteps records, in order, every step dumpAroundSuspend takes on the
// workload: the tool's verbs (through gpushareMX and gpushareNIXL), the
// suspend, the dump, the GPU resume and the store's harvest.
type dumpSteps struct {
	calls []string
	fail  map[string]bool
}

func (s *dumpSteps) step(name string) error {
	s.calls = append(s.calls, name)
	if s.fail[name] {
		return fmt.Errorf("%s: exit status 1", name)
	}
	return nil
}

// fakeDumpSource is a session (init 5000) whose engine (5100) is under the
// gpushare library, loads through ModelExpress and opted in to nixl-pause.
func fakeDumpSource(t *testing.T) (base string, pids []int) {
	t.Helper()
	base = t.TempDir()
	fakeGPUProc(t, base, 5000, 1, 5000, []string{testShim}, nil)
	fakeGPUProc(t, base, 5100, 2, 5000, []string{testShim, "/dev/nvidia0"}, nil)
	fakeMXProc(t, base, 5000, nil, []string{"/bin/sh"})
	fakeMXProc(t, base, 5100, []string{"NVSNAP_NIXL_PAUSE=1", "MODEL_EXPRESS_URL=http://mx:8001"}, []string{"python3", "-m", "vllm"})
	pids, _, ok, err := gpushareTargets(base, 5000)
	if err != nil || !ok {
		t.Fatalf("gpushareTargets = %v, %v, %v", pids, ok, err)
	}
	return base, pids
}

func (s *dumpSteps) install(t *testing.T) {
	t.Helper()
	prevMX, prevNIXL := gpushareMX, gpushareNIXL
	gpushareMX = func(_ context.Context, _ int, verb, _ string, _ []int) (string, error) {
		return "pid=2 mx: ok\n", s.step(verb)
	}
	gpushareNIXL = func(_ context.Context, _ int, verb, _ string, pids []int) (string, error) {
		var out strings.Builder
		for _, p := range pids {
			fmt.Fprintf(&out, "pid=%d nixl %s: ok engine paused, NIXL connector released on 1 worker(s)\n", p, strings.TrimPrefix(verb, "nixl-"))
		}
		return out.String(), s.step(verb)
	}
	t.Cleanup(func() { gpushareMX, gpushareNIXL = prevMX, prevNIXL })
}

func (s *dumpSteps) source(base string, pids []int, leaveRunning bool, dumpRuns bool) sourceDump {
	return sourceDump{
		on: true, leaveRunning: leaveRunning, procBase: base, hostPID: 5000, targetHostPID: 5000, pids: pids,
		toolText: nixlToolText, checkpointDir: base,
		suspend: func(context.Context, int, []int, string, *logrus.Entry) (string, bool, error) {
			if err := s.step("suspend"); err != nil {
				return "", false, err
			}
			return "GPU-0", true, nil
		},
		resume: func(context.Context, int, []int, string, time.Duration, *logrus.Entry) error {
			return s.step("gpu-resume")
		},
		watchFaults: func(*logrus.Entry) *gpuFaultWatch { return nil },
		closeFabric: func() {},
		collect:     func(string) error { return s.step("collect") },
		dump: func() (bool, bool, func(), error) {
			err := s.step("dump")
			return dumpRuns, err == nil && dumpRuns, func() {}, err
		},
	}
}

// The source's steps around its dump, in order, on every path: the pauses
// before the suspend (ModelExpress, then the NIXL KV connector), and, on a
// source that runs on, the GPU resume first, the connector's resume (checked)
// next, ModelExpress last. A source whose GPU resume failed is replaced and
// gets neither.
func TestDumpAroundSuspend_StepsInOrder(t *testing.T) {
	for _, tc := range []struct {
		name         string
		leaveRunning bool
		dumpRuns     bool // false: a preparation fails before CRIU
		fail         []string
		want         []string
		wantErr      bool
		wantReplaced bool
	}{
		{name: "capture", dumpRuns: true,
			want: []string{"mx-pause", "nixl-pause", "suspend", "dump", "collect"}},
		{name: "capture, dump fails", dumpRuns: true, fail: []string{"dump"}, wantErr: true,
			want: []string{"mx-pause", "nixl-pause", "suspend", "dump", "gpu-resume", "nixl-resume", "mx-resume"}},
		{name: "capture, fails before CRIU", dumpRuns: false, fail: []string{"dump"}, wantErr: true,
			want: []string{"mx-pause", "nixl-pause", "suspend", "dump", "gpu-resume", "nixl-resume", "mx-resume"}},
		{name: "suspend fails", dumpRuns: true, fail: []string{"suspend"}, wantErr: true,
			want: []string{"mx-pause", "nixl-pause", "suspend", "nixl-resume", "mx-resume"}},
		{name: "nixl-pause refused", dumpRuns: true, fail: []string{"nixl-pause"}, wantErr: true,
			want: []string{"mx-pause", "nixl-pause", "nixl-resume", "mx-resume"}},
		{name: "GPU resume fails", dumpRuns: true, fail: []string{"dump", "gpu-resume"}, wantErr: true, wantReplaced: true,
			want: []string{"mx-pause", "nixl-pause", "suspend", "dump", "gpu-resume"}},
		{name: "leave running", leaveRunning: true, dumpRuns: true,
			want: []string{"mx-pause", "nixl-pause", "suspend", "dump", "gpu-resume", "nixl-resume", "collect", "mx-resume"}},
		{name: "leave running, connector does not resume", leaveRunning: true, dumpRuns: true, fail: []string{"nixl-resume"},
			wantErr: true, wantReplaced: true,
			want: []string{"mx-pause", "nixl-pause", "suspend", "dump", "gpu-resume", "nixl-resume"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, pids := fakeDumpSource(t)
			s := &dumpSteps{fail: map[string]bool{}}
			for _, f := range tc.fail {
				s.fail[f] = true
			}
			s.install(t)
			a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true}}}
			info, err := a.dumpAroundSuspend(context.Background(), s.source(base, pids, tc.leaveRunning, tc.dumpRuns), testLog())
			if (err != nil) != tc.wantErr || sourceMustBeReplaced(err) != tc.wantReplaced {
				t.Errorf("err = %v; want err %v, replaced %v", err, tc.wantErr, tc.wantReplaced)
			}
			if !slices.Equal(s.calls, tc.want) {
				t.Errorf("steps = %v\n want %v", s.calls, tc.want)
			}
			if err == nil && (info == nil || !info.NIXL || !info.ModelExpress) {
				t.Errorf("info = %+v, want the pauses recorded", info)
			}
		})
	}
}

// With the agent's switch off, a pod's own marker pauses nothing, and the
// checkpoint records no paused connector.
func TestDumpAroundSuspend_SwitchOffPausesNoConnector(t *testing.T) {
	base, pids := fakeDumpSource(t)
	s := &dumpSteps{}
	s.install(t)
	a := &Agent{}
	info, err := a.dumpAroundSuspend(context.Background(), s.source(base, pids, false, true), testLog())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"mx-pause", "suspend", "dump", "collect"}; !slices.Equal(s.calls, want) {
		t.Errorf("steps = %v, want %v", s.calls, want)
	}
	if info.NIXL {
		t.Error("the checkpoint records a paused connector")
	}
}

// The capture refuses nixl-pause for an engine that spans nodes itself,
// not only in the preflight (a capture through POST /v1/checkpoints runs
// none): a capture that joins a multi-node session, or a container one of
// whose processes says so. Nothing is paused or suspended; ModelExpress,
// paused first, is resumed.
func TestDumpAroundSuspend_RefusesNIXLPauseAcrossNodes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		session string
		initEnv []string
	}{
		{"multi-node session", "s1", nil},
		{"fabric marker on the init", "", []string{"NVSNAP_GPUSHARE_FABRIC=1"}},
		{"group of two", "", []string{"LWS_GROUP_SIZE=2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, pids := fakeDumpSource(t)
			fakeMXProc(t, base, 5000, tc.initEnv, []string{"/bin/sh"})
			s := &dumpSteps{}
			s.install(t)
			a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true}}}
			d := s.source(base, pids, false, true)
			d.fabricSession = tc.session
			_, err := a.dumpAroundSuspend(context.Background(), d, testLog())
			if err == nil || !strings.Contains(err.Error(), "spans nodes") || sourceMustBeReplaced(err) {
				t.Errorf("err = %v, want a refusal naming the limit, source kept", err)
			}
			if want := []string{"mx-pause", "mx-resume"}; !slices.Equal(s.calls, want) {
				t.Errorf("steps = %v, want %v", s.calls, want)
			}
		})
	}
}
