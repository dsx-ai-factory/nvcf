// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

const nixlToolText = mxToolText + "  nixl-pause   (before suspend: ...)\n  nixl-resume  (after open: ...)\n"

// fakeNIXL records the tool calls in order, answers each with out[verb]
// (default: one engine that released its connector), and fails the verbs
// in fail.
func fakeNIXL(t *testing.T, out map[string]string, fail map[string]bool) *[]string {
	t.Helper()
	var calls []string
	prev := gpushareNIXL
	gpushareNIXL = func(_ context.Context, hostPID int, verb, host string, pids []int) (string, error) {
		c := fmt.Sprintf("%s@%d %v", verb, hostPID, pids)
		if host != "" {
			c = "--host " + host + " " + c
		}
		calls = append(calls, c)
		o, ok := out[verb]
		if !ok {
			o = "pid=7 nixl pause: ok engine paused, NIXL connector released on 1 worker(s), UCX 1.22.0\n"
		}
		if fail[verb] {
			o = "pid=7 nixl pause: err 2 RDMA device file(s) still open or mapped\nnixl-pause failed: resuming every pid\n"
			return o, &toolError{args: append([]string{verb}, intsToStrings(pids)...), err: errors.New("exit status 1"), out: o}
		}
		return o, nil
	}
	t.Cleanup(func() { gpushareNIXL = prev })
	return &calls
}

func TestGPUShareNIXLSupported(t *testing.T) {
	for text, want := range map[string]bool{
		nixlToolText:        true,
		mxToolText:          false,
		"  nixl-pause only": false,
		"":                  false,
	} {
		if got := gpushareNIXLSupported(text); got != want {
			t.Errorf("gpushareNIXLSupported(%q) = %v, want %v", text, got, want)
		}
	}
}

// Only NVSNAP_NIXL_PAUSE=1 opts in; another value, its absence or a
// process that cannot be read does not.
func TestEnvOptsIntoNIXLPause(t *testing.T) {
	base := t.TempDir()
	fakeMXProc(t, base, 10, []string{"PATH=/bin", "NVSNAP_NIXL_PAUSE=1"}, []string{"python3"})
	fakeMXProc(t, base, 11, []string{"PATH=/bin", "NVSNAP_NIXL_PAUSE=0"}, []string{"python3"})
	fakeMXProc(t, base, 12, []string{"PATH=/bin"}, []string{"python3"})
	for _, tc := range []struct {
		pids []int
		want bool
	}{{[]int{10}, true}, {[]int{11}, false}, {[]int{12}, false}, {[]int{99}, false}, {[]int{12, 10}, true}} {
		if got := envOptsIntoNIXLPause(base, tc.pids); got != tc.want {
			t.Errorf("envOptsIntoNIXLPause(%v) = %v, want %v", tc.pids, got, tc.want)
		}
	}
}

// A connector was released when any process answered ok with something
// other than "ok no ...".
func TestNIXLConnectorFound(t *testing.T) {
	for out, want := range map[string]bool{
		"pid=7 nixl pause: ok engine paused, NIXL connector released on 2 worker(s), UCX 1.22.0\npid=8 nixl pause: ok TP worker: NIXL connector released, UCX 1.22.0\n": true,
		"pid=7 nixl pause: err UCX 1.19.0 is not 1.22: restored into another pod, it would keep the old pod's routes, and NIXL would reach no peer\n":                   false,
		"pid=7 nixl pause: ok no NIXL KV connector\npid=8 nixl pause: ok no Python\n":                                                                                   false,
		"pid=7 nixl pause: ok no NIXL KV connector\npid=8 nixl pause: ok TP worker: NIXL connector released":                                                            true,
		"":                        false,
		"warning: ok something\n": false, // not a reply line
	} {
		if got := nixlConnectorFound(out); got != want {
			t.Errorf("nixlConnectorFound(%q) = %v, want %v", out, got, want)
		}
	}
}

func TestPauseNIXL(t *testing.T) {
	noConn := "pid=7 nixl pause: ok no NIXL KV connector\n"
	for _, tc := range []struct {
		name       string
		optedIn    bool
		text       string
		out        string
		fail       bool
		wantPaused bool
		wantErr    bool
		wantCalls  []string
	}{
		{"not opted in", false, nixlToolText, "", false, false, false, nil},
		{"tool without nixl", true, mxToolText, "", false, false, false, nil},
		{"opted in, connector released", true, nixlToolText, "", false, true, false, []string{"nixl-pause@42 [7 8]"}},
		{"opted in, no connector", true, nixlToolText, noConn, false, false, false, []string{"nixl-pause@42 [7 8]"}},
		// The tool resumes every pid itself, but may be stopped before it
		// does: the agent resumes them too.
		{"refused", true, nixlToolText, "", true, false, true, []string{"nixl-pause@42 [7 8]", "nixl-resume@42 [7 8]"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := map[string]string{}
			if tc.out != "" {
				out["nixl-pause"] = tc.out
			}
			calls := fakeNIXL(t, out, map[string]bool{"nixl-pause": tc.fail})
			paused, err := pauseNIXL(context.Background(), tc.optedIn, tc.text, 42, []int{7, 8}, testLog())
			if paused != tc.wantPaused || (err != nil) != tc.wantErr {
				t.Errorf("pauseNIXL = %v, %v; want paused %v, err %v", paused, err, tc.wantPaused, tc.wantErr)
			}
			if tc.wantErr && (!errors.Is(err, errNIXLPauseRefused) || !strings.Contains(err.Error(), "RDMA device file") || sourceMustBeReplaced(err)) {
				t.Errorf("err = %v, want errNIXLPauseRefused with the tool's reason, source kept", err)
			}
			if !slices.Equal(*calls, tc.wantCalls) {
				t.Errorf("calls = %v, want %v", *calls, tc.wantCalls)
			}
		})
	}
}

// A refused pause whose resume also fails leaves an engine that takes no
// requests: the source is replaced.
func TestPauseNIXL_RefusedAndNotResumed(t *testing.T) {
	calls := fakeNIXL(t, nil, map[string]bool{"nixl-pause": true, "nixl-resume": true})
	paused, err := pauseNIXL(context.Background(), true, nixlToolText, 42, []int{7}, testLog())
	if paused || !errors.Is(err, errNIXLPauseRefused) || !sourceMustBeReplaced(err) {
		t.Errorf("pauseNIXL = %v, %v; want errNIXLPauseRefused and the source replaced", paused, err)
	}
	if !slices.Equal(*calls, []string{"nixl-pause@42 [7]", "nixl-resume@42 [7]"}) {
		t.Errorf("calls = %v", *calls)
	}
}

// A source that runs on gets its connector back; when that fails it is
// reported as left suspended, so the caller replaces it, and the capture's
// own error is kept.
func TestResumeNIXLSource(t *testing.T) {
	captureErr := errors.New("dump failed")
	calls := fakeNIXL(t, nil, nil)
	if err := resumeNIXLSource(context.Background(), 42, []int{7}, captureErr, testLog()); !errors.Is(err, captureErr) || errors.Is(err, errSourceLeftSuspended) {
		t.Errorf("resume ok: err = %v, want the capture's own", err)
	}
	if !slices.Equal(*calls, []string{"nixl-resume@42 [7]"}) {
		t.Errorf("calls = %v (no --host on the source)", *calls)
	}

	fakeNIXL(t, nil, map[string]bool{"nixl-resume": true})
	err := resumeNIXLSource(context.Background(), 42, []int{7}, captureErr, testLog())
	if !errors.Is(err, errSourceLeftSuspended) || !errors.Is(err, captureErr) || !sourceMustBeReplaced(err) {
		t.Errorf("resume failed: err = %v, want errSourceLeftSuspended joined with the capture's error", err)
	}
	if err := resumeNIXLSource(context.Background(), 42, []int{7}, nil, testLog()); !errors.Is(err, errSourceLeftSuspended) {
		t.Errorf("resume failed after a good leave-running dump: err = %v, want errSourceLeftSuspended", err)
	}
}

// A restore resumes the connector only when its capture paused it, at the
// new pod's address, and fails when it cannot: the engine would take no
// requests.
func TestResumeRestoredNIXL(t *testing.T) {
	for _, tc := range []struct {
		name    string
		gs      *GPUShareInfo
		text    string
		host    string
		fail    bool
		wantErr bool
		want    []string
	}{
		{"no gpushare", nil, nixlToolText, "", false, false, nil},
		{"capture did not pause", &GPUShareInfo{PIDs: []int{7}, ModelExpress: true}, nixlToolText, "", false, false, nil},
		{"paused", &GPUShareInfo{PIDs: []int{7, 8}, NIXL: true}, nixlToolText, "", false, false, []string{"nixl-resume@99 [7 8]"}},
		{"paused, new pod address", &GPUShareInfo{PIDs: []int{7}, NIXL: true}, nixlToolText, "10.1.2.3", false, false, []string{"--host 10.1.2.3 nixl-resume@99 [7]"}},
		{"paused, resume fails", &GPUShareInfo{PIDs: []int{7}, NIXL: true}, nixlToolText, "", true, true, []string{"nixl-resume@99 [7]"}},
		{"paused, tool without nixl", &GPUShareInfo{PIDs: []int{7}, NIXL: true}, mxToolText, "", false, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := fakeNIXL(t, nil, map[string]bool{"nixl-resume": tc.fail})
			err := resumeRestoredNIXL(context.Background(), tc.gs, tc.text, 99, tc.host, testLog())
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, want err %v", err, tc.wantErr)
			}
			if !slices.Equal(*calls, tc.want) {
				t.Errorf("calls = %v, want %v", *calls, tc.want)
			}
		})
	}
}

// The restored engine listens at its pod's new address, keeps the
// captured one only when the pod kept it, and is not resumed when the
// pod's address is unknown: the restore fails and starts cold.
func TestRestoredNIXLHost(t *testing.T) {
	for _, tc := range []struct {
		pod, captured, want string
		wantErr             bool
	}{
		{"10.1.253.45", "10.1.245.116", "10.1.253.45", false},
		{"10.1.245.116", "10.1.245.116", "", false},
		{"10.1.253.45", "", "10.1.253.45", false},
		{"", "10.1.245.116", "", true},
	} {
		got, err := restoredNIXLHost(tc.pod, tc.captured)
		if got != tc.want || (err != nil) != tc.wantErr {
			t.Errorf("restoredNIXLHost(%q, %q) = %q, %v; want %q, err %v", tc.pod, tc.captured, got, err, tc.want, tc.wantErr)
		}
	}
}

// Without the capture's pause there is nothing to resume, address or not;
// with it, an unknown address fails the restore before any resume.
func TestResumeRestoredNIXLFor(t *testing.T) {
	a := &Agent{}
	calls := fakeNIXL(t, nil, nil)
	if err := a.resumeRestoredNIXLFor(context.Background(), &GPUShareInfo{PIDs: []int{7}}, nixlToolText, 99, "10.1.245.116", restoreV2Group{}, testLog()); err != nil {
		t.Errorf("not paused: err = %v", err)
	}
	gs := &GPUShareInfo{PIDs: []int{7}, NIXL: true}
	if err := a.resumeRestoredNIXLFor(context.Background(), gs, nixlToolText, 99, "10.1.245.116", restoreV2Group{}, testLog()); err == nil {
		t.Error("paused, pod address unknown: err = nil, want the restore failed")
	}
	g := restoreV2Group{InetAddrMap: "10.1.245.116=10.1.253.45"}
	if err := a.resumeRestoredNIXLFor(context.Background(), gs, nixlToolText, 99, "10.1.245.116", g, testLog()); err != nil {
		t.Errorf("paused, moved: err = %v", err)
	}
	if !slices.Equal(*calls, []string{"--host 10.1.253.45 nixl-resume@99 [7]"}) {
		t.Errorf("calls = %v, want one resume at the new address", *calls)
	}
}

// Only the RDMA findings of opted-in processes stop blocking, and only
// when nixl-pause handles the pod: with ModelExpress too, or an engine that
// spans nodes, the pod is refused with the limit named.
func TestNIXLPauseFindings(t *testing.T) {
	in := []PreflightFinding{
		{Pod: "w", Check: "rdma", Severity: preflightBlock, PID: 7, Detail: "process 7 holds RDMA devices /dev/infiniband/uverbs0"},
		{Pod: "w", Check: "rdma", Severity: preflightBlock, PID: 9, Detail: "process 9 holds RDMA devices /dev/infiniband/uverbs0"},
		{Pod: "w", Check: "socket", Severity: preflightBlock, Detail: "mptcp"},
	}
	base := t.TempDir()
	fakeMXProc(t, base, 7, []string{"NVSNAP_NIXL_PAUSE=1"}, []string{"python3"})

	out := nixlPauseFindings(in, map[int]bool{7: true}, base, "w", "engine")
	if out[0].Severity != preflightInfo || !strings.Contains(out[0].Detail, "nixl-pause") {
		t.Errorf("rdma finding of a pausable process = %+v, want info naming nixl-pause", out[0])
	}
	if out[1] != in[1] || out[2] != in[2] || len(out) != 3 {
		t.Errorf("other findings changed: %+v", out[1:])
	}
	if in[0].Severity != preflightBlock {
		t.Error("the input was changed")
	}
	if got := nixlPauseFindings(in, nil, base, "w", "engine"); !slices.Equal(got, in) {
		t.Errorf("no pausable process: findings changed: %+v", got)
	}

	for name, env := range map[string][]string{
		"ModelExpress": {"NVSNAP_NIXL_PAUSE=1", "MODEL_EXPRESS_URL=http://mx:8001"},
		"spans nodes":  {"NVSNAP_NIXL_PAUSE=1", "NVSNAP_GPUSHARE_FABRIC=1"},
	} {
		base := t.TempDir()
		fakeMXProc(t, base, 7, env, []string{"python3"})
		out := nixlPauseFindings(in, map[int]bool{7: true}, base, "w", "engine")
		if out[0].Severity != preflightBlock {
			t.Errorf("%s: the rdma finding was downgraded: %+v", name, out[0])
		}
		last := out[len(out)-1]
		if len(out) != len(in)+1 || last.Check != "nixl-pause" || last.Severity != preflightBlock || !strings.Contains(last.Detail, "not supported") {
			t.Errorf("%s: findings = %+v, want one nixl-pause block naming the limit", name, out)
		}
	}
}

// nixl-pause addresses the session's GPU processes under the library whose
// pod opted in.
func TestNIXLPausable(t *testing.T) {
	base := t.TempDir()
	optIn := []string{"NVSNAP_NIXL_PAUSE=1"}
	fakeGPUProc(t, base, 5000, 1, 5000, []string{testShim}, nil)                 // API server: no GPU state
	fakeGPUProc(t, base, 5100, 2, 5000, []string{testShim, "/dev/nvidia0"}, nil) // engine, opted in
	fakeGPUProc(t, base, 5200, 3, 5000, []string{"/dev/nvidia1"}, nil)           // GPU helper outside the library
	fakeGPUProc(t, base, 5300, 4, 5000, []string{testShim, "/dev/nvidia2"}, nil) // worker without the opt-in
	fakeGPUProc(t, base, 7100, 2, 7000, []string{testShim, "/dev/nvidia3"}, nil) // another session
	for _, pid := range []int{5000, 5100, 5200, 7100} {
		fakeMXProc(t, base, pid, optIn, []string{"python3"})
	}
	fakeMXProc(t, base, 5300, nil, []string{"python3"})
	got := nixlPausableByEnv(base, 5000)
	if len(got) != 1 || !got[5100] {
		t.Fatalf("nixlPausable = %v, want only 5100", got)
	}
}

// The capture records that it paused the connector, for its restore.
func TestGPUShareInfo_NIXLRoundTrip(t *testing.T) {
	for _, gs := range []GPUShareInfo{{PIDs: []int{7}}, {PIDs: []int{7}, NIXL: true}} {
		raw, err := json.Marshal(gs)
		if err != nil {
			t.Fatal(err)
		}
		var b GPUShareInfo
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Fatal(err)
		}
		if !gs.NIXL && strings.Contains(string(raw), "nixl") {
			t.Errorf("%s: an unpaused capture's record names nixl", raw)
		}
		if b.NIXL != gs.NIXL {
			t.Errorf("NIXL %v came back %v", gs.NIXL, b.NIXL)
		}
	}
}

// The agent pauses only with its own switch on (--webhook-nixl-pause): a
// pod may set NVSNAP_NIXL_PAUSE=1 itself, and with the switch off that
// neither turns its RDMA findings into information (the capture is
// refused as before) nor runs nixl-pause. With the switch on, the same
// process is paused.
func TestNIXLPause_NeedsTheAgentSwitch(t *testing.T) {
	base := t.TempDir()
	fakeGPUProc(t, base, 5000, 1, 5000, []string{testShim}, nil)
	fakeGPUProc(t, base, 5100, 2, 5000, []string{testShim, "/dev/nvidia0"}, nil)
	fakeMXProc(t, base, 5000, nil, []string{"python3"})
	fakeMXProc(t, base, 5100, []string{"NVSNAP_NIXL_PAUSE=1"}, []string{"python3"})
	rdma := []PreflightFinding{{Pod: "w", Check: "rdma", Severity: preflightBlock, PID: 5100, Detail: "process 5100 holds RDMA devices /dev/infiniband/uverbs0"}}

	for _, tc := range []struct {
		name       string
		on         bool
		wantSev    string
		wantPaused bool
		wantCalls  []string
	}{
		{"switch off, pod sets the env", false, preflightBlock, false, nil},
		{"switch on, opted in", true, preflightInfo, true, []string{"nixl-pause@42 [2]"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: tc.on}}}
			fs := nixlPauseFindings(rdma, a.nixlPausable(base, 5000), base, "w", "engine")
			if fs[0].Severity != tc.wantSev {
				t.Errorf("rdma finding severity %q, want %q", fs[0].Severity, tc.wantSev)
			}
			calls := fakeNIXL(t, nil, nil)
			paused, err := a.pauseNIXLFor(context.Background(), base, 5000, nixlToolText, 42, []int{2}, testLog())
			if err != nil || paused != tc.wantPaused {
				t.Errorf("pauseNIXLFor = %v, %v; want %v, nil", paused, err, tc.wantPaused)
			}
			if !slices.Equal(*calls, tc.wantCalls) {
				t.Errorf("calls = %v, want %v", *calls, tc.wantCalls)
			}
		})
	}
}
