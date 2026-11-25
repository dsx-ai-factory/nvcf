// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

const nixlToolText = mxToolText + "  nixl-pause   (before suspend: ...)\n  nixl-resume  (after open: ...)\n"

// fakeNIXL records the tool calls in order, answers each with out[verb]
// (default: one engine that released its connector), and fails the verbs
// in fail.
func fakeNIXL(t *testing.T, out map[string]string, fail map[string]bool) *[]string {
	t.Helper()
	fakeControlSockets(t, nil) // every pid has a control socket
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
			// One engine that released, or brought back, its connector.
			var b strings.Builder
			for _, p := range pids {
				if verb == "nixl-pause" {
					fmt.Fprintf(&b, "pid=%d nixl pause: ok engine paused, NIXL connector released on 1 worker(s), UCX 1.22.0\n", p)
				} else {
					fmt.Fprintf(&b, "pid=%d nixl resume: ok resumed as engine e2, side channel 10.0.0.9:5600, 0 request(s) prefilled here\n", p)
				}
			}
			o = b.String()
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
// The pause released nothing only when every pid says so ("ok no ...").
func TestNIXLNothingPaused(t *testing.T) {
	for out, want := range map[string]bool{
		"pid=7 nixl pause: ok engine paused, NIXL connector released on 2 worker(s), UCX 1.22.0\npid=8 nixl pause: ok TP worker: NIXL connector released, UCX 1.22.0\n": false,
		"pid=7 nixl pause: ok no NIXL KV connector\npid=8 nixl pause: ok no Python\n":                                                                                   true,
		"pid=7 nixl pause: ok no NIXL KV connector\npid=8 nixl pause: ok TP worker: NIXL connector released":                                                            false,
		"pid=7 nixl pause: ok no NIXL KV connector\n":                                                                                                                   false, // pid 8 did not answer
		"": false,
		"warning: ok no thing\npid=8 nixl pause: ok no Python\n": false, // not a reply line for pid 7
	} {
		if got := nixlNothingPaused(out, []int{7, 8}); got != want {
			t.Errorf("nixlNothingPaused(%q) = %v, want %v", out, got, want)
		}
	}
}

func TestPauseNIXL(t *testing.T) {
	noConn := "pid=7 nixl pause: ok no NIXL KV connector\npid=8 nixl pause: ok no NIXL KV connector\n"
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
// Before CRIU runs, a restore whose capture paused the connector checks
// it can bring it back: a GPU tool with nixl-resume, and the restored pod's
// address. Either missing is a restore that did not run here
// (errCheckpointUnavailable), which says nothing about the checkpoint.
func TestNIXLRestorePrecheck(t *testing.T) {
	a := &Agent{}
	gs := &GPUShareInfo{PIDs: []int{7}, NIXL: true}
	moved := restoreV2Group{InetAddrMap: "10.1.245.116=10.1.253.45"}
	for _, tc := range []struct {
		name     string
		gs       *GPUShareInfo
		tool     string
		group    restoreV2Group
		wantHost string
		notRun   bool
	}{
		{"not paused", &GPUShareInfo{PIDs: []int{7}}, mxToolText, restoreV2Group{}, "", false},
		{"paused, moved", gs, nixlToolText, moved, "10.1.253.45", false},
		{"paused, tool without nixl-resume", gs, mxToolText, moved, "", true},
		{"paused, pod address unknown", gs, nixlToolText, restoreV2Group{}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host, err := a.nixlRestorePrecheck(context.Background(), tc.gs, tc.tool, "10.1.245.116", tc.group)
			if host != tc.wantHost || (err != nil) != tc.notRun {
				t.Errorf("precheck = %q, %v; want %q, err %v", host, err, tc.wantHost, tc.notRun)
			}
			if tc.notRun && (!errors.Is(err, errCheckpointUnavailable) || !errors.Is(err, errNIXLResumeUnavailable)) {
				t.Errorf("err = %v, want a restore that did not run (errCheckpointUnavailable, errNIXLResumeUnavailable)", err)
			}
		})
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

	out := nixlPauseFindings(in, map[int]bool{7: true}, nixlPauseLimit(base, []int{7}, ""), "w", "engine")
	if out[0].Severity != preflightInfo || !strings.Contains(out[0].Detail, "nixl-pause") {
		t.Errorf("rdma finding of a pausable process = %+v, want info naming nixl-pause", out[0])
	}
	if out[1] != in[1] || out[2] != in[2] || len(out) != 3 {
		t.Errorf("other findings changed: %+v", out[1:])
	}
	if in[0].Severity != preflightBlock {
		t.Error("the input was changed")
	}
	if got := nixlPauseFindings(in, nil, "", "w", "engine"); !slices.Equal(got, in) {
		t.Errorf("no pausable process: findings changed: %+v", got)
	}

	for name, env := range map[string][]string{
		"ModelExpress": {"NVSNAP_NIXL_PAUSE=1", "MODEL_EXPRESS_URL=http://mx:8001"},
		"spans nodes":  {"NVSNAP_NIXL_PAUSE=1", "NVSNAP_GPUSHARE_FABRIC=1"},
	} {
		base := t.TempDir()
		fakeMXProc(t, base, 7, env, []string{"python3"})
		out := nixlPauseFindings(in, map[int]bool{7: true}, nixlPauseLimit(base, []int{7}, ""), "w", "engine")
		if out[0].Severity != preflightBlock {
			t.Errorf("%s: the rdma finding was downgraded: %+v", name, out[0])
		}
		last := out[len(out)-1]
		if len(out) != len(in)+1 || last.Check != "nixl-pause" || last.Severity != preflightBlock || !strings.Contains(last.Detail, "not supported") {
			t.Errorf("%s: findings = %+v, want one nixl-pause block naming the limit", name, out)
		}
	}
}

// nixl-pause addresses the session's GPU processes under the library, when
// any process of the container carries the opt-in: the container opted in,
// whatever a process's own environ shows now.
func TestNIXLPausable(t *testing.T) {
	base := t.TempDir()
	optIn := []string{"NVSNAP_NIXL_PAUSE=1"}
	fakeGPUProc(t, base, 5000, 1, 5000, []string{testShim}, nil)                 // API server: no GPU state
	fakeGPUProc(t, base, 5100, 2, 5000, []string{testShim, "/dev/nvidia0"}, nil) // engine, opted in
	fakeGPUProc(t, base, 5200, 3, 5000, []string{"/dev/nvidia1"}, nil)           // GPU helper outside the library
	fakeGPUProc(t, base, 5300, 4, 5000, []string{testShim, "/dev/nvidia2"}, nil) // worker whose environ shows no marker
	fakeGPUProc(t, base, 7100, 2, 7000, []string{testShim, "/dev/nvidia3"}, nil) // another session
	fakeGPUProc(t, base, 7200, 3, 7000, []string{testShim, "/dev/nvidia4"}, nil) // in a session that did not opt in
	for _, pid := range []int{5000, 5100, 5200, 7100} {
		fakeMXProc(t, base, pid, optIn, []string{"python3"})
	}
	fakeMXProc(t, base, 5300, nil, []string{"python3"})
	fakeMXProc(t, base, 7200, nil, []string{"python3"})
	if got := nixlPausableByEnv(base, 5000); len(got) != 2 || !got[5100] || !got[5300] {
		t.Errorf("nixlPausableByEnv(5000) = %v, want 5100 and 5300", got)
	}
	base2 := t.TempDir()
	fakeGPUProc(t, base2, 7000, 1, 7000, []string{testShim}, nil)
	fakeGPUProc(t, base2, 7200, 3, 7000, []string{testShim, "/dev/nvidia4"}, nil)
	fakeMXProc(t, base2, 7000, nil, []string{"python3"})
	fakeMXProc(t, base2, 7200, nil, []string{"python3"})
	if got := nixlPausableByEnv(base2, 7000); len(got) != 0 {
		t.Errorf("a container without the marker: %v, want none", got)
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
			fs := nixlPauseFindings(rdma, a.nixlPausable(base, 5000), "", "w", "engine")
			if fs[0].Severity != tc.wantSev {
				t.Errorf("rdma finding severity %q, want %q", fs[0].Severity, tc.wantSev)
			}
			calls := fakeNIXL(t, nil, nil)
			paused, err := a.pauseNIXLFor(context.Background(), base, 5000, "", nixlToolText, 42, []int{2}, testLog())
			if err != nil || paused != tc.wantPaused {
				t.Errorf("pauseNIXLFor = %v, %v; want %v, nil", paused, err, tc.wantPaused)
			}
			if !slices.Equal(*calls, tc.wantCalls) {
				t.Errorf("calls = %v, want %v", *calls, tc.wantCalls)
			}
		})
	}
}

// fakeRetitledProc gives process pid of base the environ a process has
// after setproctitle (vLLM names its engine and workers that way): the
// area is overwritten in place, mostly NULs with what is left of a few
// variables, and the webhook's markers are gone. Its cmdline is the title.
func fakeRetitledProc(t *testing.T, base string, pid int, title string) {
	t.Helper()
	dir := filepath.Join(base, fmt.Sprint(pid))
	environ := strings.Repeat("\x00", 4755) + "PATH=/usr/bin\x00UCX_TLS=rc\x00LD_LIBRARY_PATH=/usr/lib\x00"
	if err := os.WriteFile(filepath.Join(dir, "environ"), []byte(environ), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cmdline"), []byte(title+strings.Repeat(" ", 100)), 0o600); err != nil {
		t.Fatal(err)
	}
}

// retitledSession is a container (init 5000, the API server, environ
// intact, no GPU state) whose engine (5100, under the library, holding RDMA) was renamed
// with setproctitle, which wiped its environ. env is the container's
// environment, as the init keeps it.
func retitledSession(t *testing.T, env []string) (base string, rdma []PreflightFinding) {
	t.Helper()
	base = t.TempDir()
	fakeGPUProc(t, base, 5000, 1, 5000, []string{testShim}, nil) // the API server: no GPU state
	fakeGPUProc(t, base, 5100, 2, 5000, []string{testShim, "/dev/nvidia1"}, nil)
	fakeMXProc(t, base, 5000, env, []string{"/usr/bin/python3", "/usr/local/bin/vllm", "serve", "m"})
	fakeMXProc(t, base, 5100, nil, nil)
	fakeRetitledProc(t, base, 5100, "VLLM::EngineCore")
	return base, []PreflightFinding{{Pod: "w", Check: "rdma", Severity: preflightBlock, PID: 5100,
		Detail: "process 5100 (VLLM::EngineCor) holds RDMA devices /dev/infiniband/uverbs4"}}
}

// The opt-in and the limits are the container's, not each process's: a
// vLLM engine renamed with setproctitle has lost the webhook's markers
// from its environ, while its container's other processes keep them. Its
// RDMA is released by nixl-pause when the container opted in, and an
// engine that spans nodes is refused even when only the init still shows
// the fabric marker.
func TestReleaseFindings_RetitledEngine(t *testing.T) {
	a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: true}}}

	base, rdma := retitledSession(t, []string{"PATH=/bin", "NVSNAP_NIXL_PAUSE=1"})
	fs := a.releaseFindings(rdma, nixlToolText, base, 5000, "w", "engine")
	if len(fs) != 1 || fs[0].Severity != preflightInfo {
		t.Errorf("opted-in container, renamed engine: findings %+v, want its RDMA released (info)", fs)
	}

	base, rdma = retitledSession(t, []string{"PATH=/bin", "NVSNAP_NIXL_PAUSE=1", "NVSNAP_GPUSHARE_FABRIC=1"})
	fs = a.releaseFindings(rdma, nixlToolText, base, 5000, "w", "engine")
	if !preflightBlocked(fs, "nixl-pause") || fs[0].Severity != preflightBlock {
		t.Errorf("engine spanning nodes, renamed: findings %+v, want the RDMA blocked and a nixl-pause block", fs)
	}

	base, rdma = retitledSession(t, []string{"PATH=/bin"})
	fs = a.releaseFindings(rdma, nixlToolText, base, 5000, "w", "engine")
	if len(fs) != 1 || fs[0].Severity != preflightBlock {
		t.Errorf("container that did not opt in: findings %+v, want the RDMA blocked", fs)
	}
}

// preflightBlocked reports whether fs has a blocking finding of check.
func preflightBlocked(fs []PreflightFinding, check string) bool {
	for _, f := range fs {
		if f.Check == check && f.Severity == preflightBlock {
			return true
		}
	}
	return false
}

// Any process of the container says the engine spans nodes: the fabric
// marker, a group size above one, a multi-node engine flag, or a capture
// that joins a multi-node session. A single-pod group, or an engine
// started without such a flag, does not.
func TestNIXLSpansNodes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     []string
		cmdline []string
		session string
		spans   bool
	}{
		{"single node", []string{"PATH=/bin"}, []string{"vllm", "serve", "m"}, "", false},
		{"fabric marker", []string{"NVSNAP_GPUSHARE_FABRIC=1"}, nil, "", true},
		{"leader-worker group", []string{"LWS_GROUP_SIZE=2"}, nil, "", true},
		{"group of one", []string{"LWS_GROUP_SIZE=1", "GROVE_PCSG_TEMPLATE_NUM_PODS=1"}, nil, "", false},
		{"scaling group", []string{"GROVE_PCSG_TEMPLATE_NUM_PODS=3"}, nil, "", true},
		{"headless engine", nil, []string{"vllm", "serve", "m", "--headless"}, "", true},
		{"flag in a shell line", nil, []string{"/bin/sh", "-c", "vllm serve m --data-parallel-address=10.0.0.1"}, "", true},
		{"multi-node session", nil, nil, "s1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			fakeGPUProc(t, base, 5000, 1, 5000, []string{testShim}, nil)
			fakeGPUProc(t, base, 5100, 2, 5000, []string{testShim, "/dev/nvidia0"}, nil)
			fakeMXProc(t, base, 5000, tc.env, tc.cmdline)
			fakeMXProc(t, base, 5100, nil, nil)
			fakeRetitledProc(t, base, 5100, "VLLM::EngineCore")
			why := nixlSpansNodes(base, sessionPIDs(base, 5000), tc.session)
			if (why != "") != tc.spans {
				t.Errorf("nixlSpansNodes = %q, want spans %v", why, tc.spans)
			}
		})
	}
}

// A rank opted in to nixl-pause whose engine spans nodes by its spec is
// refused by the preflight even when its processes do not show it: a
// LeaderWorkerSet pod whose gpushare fabric is annotated "false" has no
// fabric marker in any process.
func TestPodPreflight_NIXLPauseRefusesAnEngineSpanningNodes(t *testing.T) {
	pod := func(ann map[string]string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "w", Namespace: "ns", Annotations: ann},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "engine"}}}}
	}
	spans := map[string]string{webhook.NIXLPauseAnnotation: "true", webhook.GPUShareFabricAnnotation: "false",
		"leaderworkerset.sigs.k8s.io/size": "2"}
	for _, tc := range []struct {
		name string
		on   bool
		ann  map[string]string
		want bool
	}{
		{"opted in, spans nodes", true, spans, true},
		{"switch off", false, spans, false},
		{"opted in, one node", true, map[string]string{webhook.NIXLPauseAnnotation: "true"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &Agent{config: Config{Webhook: WebhookConfig{NIXLPause: tc.on}}}
			if got := preflightBlocked(a.podPreflight(pod(tc.ann), "engine", true), "nixl-pause"); got != tc.want {
				t.Errorf("nixl-pause block = %v, want %v", got, tc.want)
			}
		})
	}
}

// The pause's outcome fails closed: a pod is taken as paused unless every
// pid answered "ok no ..." (no connector, no Python). Output the agent
// cannot read, or a pid that did not answer (the tool prints nothing for
// a pid without a control socket), counts as paused, so the source is
// resumed when it runs on and the checkpoint records the pause.
func TestPauseNIXL_UnclearOutputCountsAsPaused(t *testing.T) {
	for _, tc := range []struct {
		name string
		out  string
		want bool
	}{
		{"every pid without a connector", "pid=7 nixl pause: ok no NIXL KV connector\npid=8 nixl pause: ok no Python\n", false},
		{"one pid paused", "pid=7 nixl pause: ok no NIXL KV connector\npid=8 nixl pause: ok TP worker: NIXL connector released\n", true},
		{"no output", "", true},
		{"a pid did not answer", "pid=7 nixl pause: ok no NIXL KV connector\n", true},
		{"another format", "7: paused\n8: paused\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeNIXL(t, map[string]string{"nixl-pause": tc.out}, nil)
			paused, err := pauseNIXL(context.Background(), true, nixlToolText, 42, []int{7, 8}, testLog())
			if err != nil || paused != tc.want {
				t.Errorf("pauseNIXL = %v, %v; want %v, nil", paused, err, tc.want)
			}
		})
	}
}

// A resume succeeds only when every pid with a control socket answered ok:
// such a pid with no answer was not resumed, and an engine that is not
// resumed serves nothing. (A pid without a socket is not asked:
// TestNIXL_ProcessWithoutAControlSocket.)
func TestResumeNIXL_EveryPidAnswersOK(t *testing.T) {
	for _, tc := range []struct {
		name    string
		out     string
		wantErr bool
	}{
		{"every pid resumed", "pid=7 nixl resume: ok resumed as engine e2\npid=8 nixl resume: ok TP worker: NIXL connector up\n", false},
		{"not paused", "pid=7 nixl resume: ok not paused\npid=8 nixl resume 10.0.0.9: ok TP worker: NIXL connector up\n", false},
		{"a pid with a control socket did not answer", "pid=7 nixl resume: ok resumed as engine e2\n", true},
		{"no output", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeNIXL(t, map[string]string{"nixl-resume": tc.out}, nil)
			err := resumeNIXL(context.Background(), 42, "", []int{7, 8}, testLog())
			if (err != nil) != tc.wantErr {
				t.Errorf("resumeNIXL = %v, want err %v", err, tc.wantErr)
			}
		})
	}
}

// fakeNIXLTool answers like the GPU tool: one line per pid it reached,
// nothing (and exit 0) for a pid without a control socket (socketless).
// It records the pids it was given.
func fakeNIXLTool(t *testing.T, socketless map[int]bool) *[][]int {
	t.Helper()
	var given [][]int
	prev := gpushareNIXL
	gpushareNIXL = func(_ context.Context, _ int, verb, _ string, pids []int) (string, error) {
		given = append(given, append([]int(nil), pids...))
		var b strings.Builder
		for _, p := range pids {
			if socketless[p] {
				continue
			}
			if verb == "nixl-pause" {
				fmt.Fprintf(&b, "pid=%d nixl pause: ok engine paused, NIXL connector released on 1 worker(s)\n", p)
			} else {
				fmt.Fprintf(&b, "pid=%d nixl resume: ok resumed as engine e2\n", p)
			}
		}
		return b.String(), nil
	}
	t.Cleanup(func() { gpushareNIXL = prev })
	return &given
}

// fakeControlSockets makes pids in socketless have no control socket.
func fakeControlSockets(t *testing.T, socketless map[int]bool) {
	t.Helper()
	prev := controlSocketPIDs
	controlSocketPIDs = func(_ int, pids []int) []int {
		var out []int
		for _, p := range pids {
			if !socketless[p] {
				out = append(out, p)
			}
		}
		return out
	}
	t.Cleanup(func() { controlSocketPIDs = prev })
}

// A process under the library that never used CUDA (a resource tracker,
// a shell wrapper, an API server) has no control socket, and the tool
// prints nothing for it. The pause and the resume address only the
// processes with one: such a process neither fails the resume nor makes a
// pod without a connector count as paused.
func TestNIXL_ProcessWithoutAControlSocket(t *testing.T) {
	socketless := map[int]bool{9: true}
	fakeControlSockets(t, socketless)
	given := fakeNIXLTool(t, socketless)
	if err := resumeNIXL(context.Background(), 42, "", []int{7, 8, 9}, testLog()); err != nil {
		t.Errorf("resume with a socketless pid: %v", err)
	}
	paused, err := pauseNIXL(context.Background(), true, nixlToolText, 42, []int{7, 8, 9}, testLog())
	if err != nil || !paused {
		t.Errorf("pause = %v, %v; want paused", paused, err)
	}
	for _, g := range *given {
		if slices.Contains(g, 9) {
			t.Errorf("the tool was given the socketless pid: %v", g)
		}
	}
	// No process with a socket: nothing to pause; a restore that must
	// resume one cannot.
	fakeControlSockets(t, map[int]bool{7: true, 8: true, 9: true})
	if paused, err := pauseNIXL(context.Background(), true, nixlToolText, 42, []int{7, 8, 9}, testLog()); err != nil || paused {
		t.Errorf("pause with no control socket = %v, %v; want not paused", paused, err)
	}
	if err := resumeNIXL(context.Background(), 42, "", []int{7, 8, 9}, testLog()); err == nil {
		t.Error("resume with no control socket succeeded")
	}
}

// controlSocketPIDsIn reads the network namespace's unix sockets: a pid
// has a control socket when its library listens on @nvsnap-gpushare.<pid>.
func TestControlSocketPIDsIn(t *testing.T) {
	base := t.TempDir()
	if err := os.MkdirAll(filepath.Join(base, "42", "net"), 0o755); err != nil {
		t.Fatal(err)
	}
	unix := "Num       RefCount Protocol Flags    Type St Inode Path\n" +
		"0000000000000000: 00000002 00000000 00010000 0005 01 11111 @nvsnap-gpushare.7\n" +
		"0000000000000000: 00000002 00000000 00010000 0005 01 11112 @nvsnap-gpushare.80\n" +
		"0000000000000000: 00000002 00000000 00010000 0001 01 11113 /tmp/nvsnap-gpushare.8\n"
	if err := os.WriteFile(filepath.Join(base, "42", "net", "unix"), []byte(unix), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := controlSocketPIDsIn(base, 42, []int{7, 8, 9}); !slices.Equal(got, []int{7}) {
		t.Errorf("controlSocketPIDsIn = %v, want [7]", got)
	}
	// Unreadable: every pid, so a missing answer fails closed.
	if got := controlSocketPIDsIn(base, 43, []int{7, 8}); !slices.Equal(got, []int{7, 8}) {
		t.Errorf("unreadable namespace: %v, want every pid", got)
	}
}
