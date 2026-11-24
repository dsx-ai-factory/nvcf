// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

func freezeServer(t *testing.T) (*Agent, string) {
	t.Helper()
	a := &Agent{}
	r := mux.NewRouter()
	r.HandleFunc("/v1/group-freeze/{session}/{namespace}/{pod}", a.freezeHandler).Methods("GET", "POST")
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return a, srv.URL
}

// Members ready at different times are released together, only once the
// last one waits.
func TestGroupFreeze_ReleasesTogether(t *testing.T) {
	log := logrus.NewEntry(logrus.New())
	a1, b1 := freezeServer(t)
	a2, b2 := freezeServer(t)
	ctx := context.Background()
	var released [2]atomic.Int64
	errs := make(chan error, 2)
	go func() {
		err := a1.awaitGroupFreeze(ctx, "s1", "ns", "worker-0", log)
		released[0].Store(time.Now().UnixNano())
		errs <- err
	}()
	go func() {
		time.Sleep(300 * time.Millisecond) // a slower member, e.g. a GPU rank's suspend
		err := a2.awaitGroupFreeze(ctx, "s1", "ns", "etcd-0", log)
		released[1].Store(time.Now().UnixNano())
		errs <- err
	}()
	ok := driveGroupFreeze(ctx, http.DefaultClient, "s1", "", []freezeMember{
		{Base: b1, Namespace: "ns", Pod: "worker-0"}, {Base: b2, Namespace: "ns", Pod: "etcd-0"},
	}, 20*time.Millisecond, log, nil, nil, nil)
	if !ok {
		t.Fatal("driver did not release the group")
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("member: %v", err)
		}
	}
	if d := time.Duration(released[0].Load() - released[1].Load()); d > 100*time.Millisecond || d < -100*time.Millisecond {
		t.Errorf("members released %s apart", d)
	}
}

// Every member at the freeze point is released only once the gate opens:
// the instance's pods are held.
func TestGroupFreeze_WaitsForTheGate(t *testing.T) {
	log := logrus.NewEntry(logrus.New())
	a1, b1 := freezeServer(t)
	ctx := context.Background()
	errs := make(chan error, 1)
	go func() { errs <- a1.awaitGroupFreeze(ctx, "s5", "ns", "worker-0", log) }()
	gate := make(chan struct{})
	opened := time.Now().Add(300 * time.Millisecond)
	time.AfterFunc(300*time.Millisecond, func() { close(gate) })
	ok := driveGroupFreeze(ctx, http.DefaultClient, "s5", "", []freezeMember{{Base: b1, Namespace: "ns", Pod: "worker-0"}}, 20*time.Millisecond, log, nil, gate, nil)
	if !ok || time.Now().Before(opened) {
		t.Fatalf("released before the gate opened (ok=%v)", ok)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

// A member whose capture ends before the freeze point aborts the others.
func TestGroupFreeze_AbortsWhenAMemberEnds(t *testing.T) {
	log := logrus.NewEntry(logrus.New())
	a1, b1 := freezeServer(t)
	_, b2 := freezeServer(t)
	ctx := context.Background()
	errs := make(chan error, 1)
	go func() { errs <- a1.awaitGroupFreeze(ctx, "s2", "ns", "worker-0", log) }()
	ok := driveGroupFreeze(ctx, http.DefaultClient, "s2", "", []freezeMember{
		{Base: b1, Namespace: "ns", Pod: "worker-0"},
		{Base: b2, Namespace: "ns", Pod: "etcd-0", Done: func() bool { return true }},
	}, 20*time.Millisecond, log, nil, nil, nil)
	if ok {
		t.Fatal("driver released a group with a failed member")
	}
	select {
	case err := <-errs:
		if err == nil {
			t.Error("the waiting member was not aborted")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting member was never released")
	}
}

// A release that reaches a member's agent before the member does is kept.
func TestGroupFreeze_EarlyReleaseIsKept(t *testing.T) {
	log := logrus.NewEntry(logrus.New())
	a, base := freezeServer(t)
	resp, err := http.Post(base+"/v1/group-freeze/s3/ns/p?go=true", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.awaitGroupFreeze(ctx, "s3", "ns", "p", log); err != nil {
		t.Fatalf("awaitGroupFreeze = %v", err)
	}
}

// The post-dump hook passes other actions, reports the member dumped at
// post-dump and waits: exit 0 on go, non-zero on abort (criu then rolls
// the dump back).
func TestPostDumpScript(t *testing.T) {
	run := func(action string, release func(dir string)) (bool, error) {
		dir := t.TempDir()
		script, err := writePostDumpScript(dir)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(script)
		cmd.Env = append(os.Environ(), "CRTOOLS_SCRIPT_ACTION="+action)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		reached := false
		if release != nil {
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
				if _, err := os.Stat(filepath.Join(dir, "reached")); err == nil {
					reached = true
					break
				}
			}
			release(dir)
		}
		return reached, cmd.Wait()
	}
	if _, err := run("pre-dump", nil); err != nil {
		t.Errorf("pre-dump: %v, want a pass", err)
	}
	reached, err := run("post-dump", func(d string) { _ = os.WriteFile(filepath.Join(d, "go"), nil, 0o644) })
	if !reached || err != nil {
		t.Errorf("post-dump released: reached=%v err=%v, want reached and exit 0", reached, err)
	}
	reached, err = run("post-dump", func(d string) { _ = os.WriteFile(filepath.Join(d, "abort"), nil, 0o644) })
	if !reached || err == nil {
		t.Errorf("post-dump aborted: reached=%v err=%v, want reached and a failure", reached, err)
	}
}

// The post-dump phase: a member that reports itself past its dump and
// returns (no hook) does not abort the group.
func TestGroupFreeze_PostPhaseAcceptsAMemberWithoutHook(t *testing.T) {
	log := logrus.NewEntry(logrus.New())
	a, base := freezeServer(t)
	a.config.CheckpointDir = t.TempDir()
	markPostDumpReached(a.postDumpDir("s4", "ns", "etcd-0"), log)
	ok := driveGroupFreeze(context.Background(), http.DefaultClient, "s4", "post", []freezeMember{
		{Base: base, Namespace: "ns", Pod: "etcd-0", Done: func() bool { return true }},
	}, 20*time.Millisecond, log, nil, nil, nil)
	if !ok {
		t.Fatal("a member past its dump aborted the group")
	}
	if _, err := os.Stat(filepath.Join(a.postDumpDir("s4", "ns", "etcd-0"), "go")); err != nil {
		t.Errorf("member not released: %v", err)
	}
}

// The post-dump phase: a member whose dump failed aborts the others at
// once (seen 2026-10-18: they waited out the whole barrier, stopped, and
// the failed member's fabric resume waited on them).
func TestGroupFreeze_PostPhaseAbortsOnAFailedDump(t *testing.T) {
	log := logrus.NewEntry(logrus.New())
	a1, b1 := freezeServer(t)
	a2, b2 := freezeServer(t)
	a1.config.CheckpointDir, a2.config.CheckpointDir = t.TempDir(), t.TempDir()
	markPostDumpReached(a1.postDumpDir("s6", "ns", "worker-0"), log)
	markPostDumpFailed(a2.postDumpDir("s6", "ns", "worker-1"), log)
	t0 := time.Now()
	ok := driveGroupFreeze(context.Background(), http.DefaultClient, "s6", "post", []freezeMember{
		{Base: b1, Namespace: "ns", Pod: "worker-0"},
		{Base: b2, Namespace: "ns", Pod: "worker-1"},
	}, 20*time.Millisecond, log, nil, nil, nil)
	if ok {
		t.Fatal("a failed dump released the group")
	}
	if d := time.Since(t0); d > 5*time.Second {
		t.Errorf("aborted after %s; want at once", d)
	}
	if _, err := os.Stat(filepath.Join(a1.postDumpDir("s6", "ns", "worker-0"), "abort")); err != nil {
		t.Errorf("the dumped member was not aborted: %v", err)
	}
}

// A member whose agent does not take the release (restarting, a dropped
// connection) ends the capture at once: the others are aborted at the freeze
// point and at the post-dump barrier, instead of dumping and then waiting
// out postDumpWait for it (review).
func TestGroupFreeze_AbortsWhenAReleaseIsNotTaken(t *testing.T) {
	defer func(d time.Duration) { releaseRetry = d }(releaseRetry)
	releaseRetry = 10 * time.Millisecond
	log := logrus.NewEntry(logrus.New())
	a1, b1 := freezeServer(t)
	a1.config.CheckpointDir = t.TempDir()
	var gone []string
	var mu sync.Mutex
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"reached":true,"failed":false}`))
			return
		}
		mu.Lock()
		gone = append(gone, r.URL.Query().Get("phase")+":"+r.URL.Query().Get("go"))
		mu.Unlock()
		http.Error(w, "agent restarting", http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)
	ctx := context.Background()
	errs := make(chan error, 1)
	go func() { errs <- a1.awaitGroupFreeze(ctx, "s7", "ns", "worker-0", log) }()
	t0 := time.Now()
	ok := driveGroupFreeze(ctx, http.DefaultClient, "s7", "", []freezeMember{
		{Base: b1, Namespace: "ns", Pod: "worker-0"}, {Base: down.URL, Namespace: "ns", Pod: "etcd-0"},
	}, 20*time.Millisecond, log, nil, nil, nil)
	if ok {
		t.Fatal("driver reported the group released though a member never got its release")
	}
	if d := time.Since(t0); d > 5*time.Second {
		t.Errorf("took %s to give up", d)
	}
	// The healthy member got true first, then the abort is queued for it:
	// it either dumps and stops at the barrier, or is aborted here.
	if err := <-errs; err != nil {
		t.Logf("healthy member: %v", err)
	}
	if _, err := os.Stat(filepath.Join(a1.postDumpDir("s7", "ns", "worker-0"), "abort")); err != nil {
		t.Errorf("no post-dump abort recorded for the released member: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{":true", ":true", ":true", ":false", ":false", ":false", "post:false", "post:false", "post:false"}; strings.Join(gone, ",") != strings.Join(want, ",") {
		t.Errorf("the unreachable member was asked %v, want %v", gone, want)
	}
}

// holdRecorder serves a member's freeze point and records the hold requests
// it gets, with when they came, instead of freezing anything.
type holdRecorder struct {
	mu    sync.Mutex
	holds map[string]time.Time // pod -> when its hold was requested
}

func (h *holdRecorder) server(t *testing.T) (a *Agent, base string) {
	t.Helper()
	a = &Agent{}
	r := mux.NewRouter()
	r.HandleFunc("/v1/group-freeze/{session}/{namespace}/{pod}", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Query().Get("phase") == "hold" {
			h.mu.Lock()
			h.holds[mux.Vars(req)["pod"]] = time.Now()
			h.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		a.freezeHandler(w, req)
	}).Methods("GET", "POST")
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return a, srv.URL
}

func (h *holdRecorder) at(pod string) (time.Time, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	at, ok := h.holds[pod]
	return at, ok
}

// A GPU rank's pod is held when the vote goes; a companion's (etcd) only
// once the rank, its GPU memory saved, waits at the freeze point: while the
// rank saves, its engine runs and must still reach etcd. The members are
// released only after both holds landed.
func TestInstanceHold_CompanionsHeldOnceRanksWait(t *testing.T) {
	log := logrus.NewEntry(logrus.New())
	rec := &holdRecorder{holds: map[string]time.Time{}}
	aRank, bRank := rec.server(t)
	aEtcd, bEtcd := rec.server(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	members := []CheckpointRequest{
		{Namespace: "ns", PodName: "worker-0", ContainerName: "main"},
		{Namespace: "ns", PodName: "etcd-0", ContainerName: "etcd", Companion: true},
	}
	hold := newInstanceHold(ctx, http.DefaultClient, "s8", members, []string{bRank, bEtcd}, log)

	hold.holdRanks() // the vote went
	var rankWaits, released atomic.Int64
	errs := make(chan error, 2)
	saved := make(chan struct{})                                                   // the rank has saved its GPU memory
	go func() { errs <- aEtcd.awaitGroupFreeze(ctx, "s8", "ns", "etcd-0", log) }() // a companion is ready at once
	go func() {
		select {
		case <-saved:
		case <-ctx.Done():
			errs <- ctx.Err()
			return
		}
		rankWaits.Store(time.Now().UnixNano())
		err := aRank.awaitGroupFreeze(ctx, "s8", "ns", "worker-0", log)
		released.Store(time.Now().UnixNano())
		errs <- err
	}()
	select {
	case <-hold.ranksDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the GPU rank's hold")
	}
	if _, ok := rec.at("worker-0"); !ok {
		t.Error("the GPU rank's pod was not held when the vote went")
	}
	time.Sleep(100 * time.Millisecond) // the companion waits at the freeze point; the rank still saves
	if _, ok := rec.at("etcd-0"); ok {
		t.Fatal("the companion's pod was held while the GPU rank still saved its GPU memory")
	}
	close(saved)
	ok := driveGroupFreeze(ctx, http.DefaultClient, "s8", "", []freezeMember{
		{Base: bRank, Namespace: "ns", Pod: "worker-0"}, {Base: bEtcd, Namespace: "ns", Pod: "etcd-0"},
	}, 20*time.Millisecond, log, hold.holdCompanions, hold.companions(), nil)
	if !ok {
		t.Fatal("driver did not release the group")
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("member: %v", err)
		}
	}
	hold.wait()
	etcdHeld, held := rec.at("etcd-0")
	switch {
	case !held:
		t.Fatal("the companion's pod was never held")
	case etcdHeld.UnixNano() < rankWaits.Load():
		t.Errorf("the companion was held %s before the GPU rank reached the freeze point", time.Duration(rankWaits.Load()-etcdHeld.UnixNano()))
	case etcdHeld.UnixNano() > released.Load():
		t.Errorf("the members were released %s before the companion was held", time.Duration(etcdHeld.UnixNano()-released.Load()))
	}
}

// Every member can wait at the freeze point before the driver has seen the
// vote go (the members read the go a poll earlier). The GPU ranks' pods
// are still held, before the companions', and before the release.
func TestInstanceHold_FreezePointBeforeTheDriverSeesTheGo(t *testing.T) {
	log := logrus.NewEntry(logrus.New())
	rec := &holdRecorder{holds: map[string]time.Time{}}
	aRank, bRank := rec.server(t)
	aEtcd, bEtcd := rec.server(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	members := []CheckpointRequest{
		{Namespace: "ns", PodName: "worker-0", ContainerName: "main"},
		{Namespace: "ns", PodName: "etcd-0", ContainerName: "etcd", Companion: true},
	}
	hold := newInstanceHold(ctx, http.DefaultClient, "s10", members, []string{bRank, bEtcd}, log)
	var released atomic.Int64
	errs := make(chan error, 2)
	go func() { errs <- aEtcd.awaitGroupFreeze(ctx, "s10", "ns", "etcd-0", log) }()
	go func() {
		err := aRank.awaitGroupFreeze(ctx, "s10", "ns", "worker-0", log)
		released.Store(time.Now().UnixNano())
		errs <- err
	}()
	// No holdRanks: the driver has not seen the go yet.
	ok := driveGroupFreeze(ctx, http.DefaultClient, "s10", "", []freezeMember{
		{Base: bRank, Namespace: "ns", Pod: "worker-0"}, {Base: bEtcd, Namespace: "ns", Pod: "etcd-0"},
	}, 20*time.Millisecond, log, hold.holdCompanions, hold.companions(), nil)
	if !ok {
		t.Fatal("driver did not release the group")
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("member: %v", err)
		}
	}
	hold.holdRanks() // the driver sees the go late: nothing more to do
	hold.wait()
	rankHeld, rok := rec.at("worker-0")
	etcdHeld, eok := rec.at("etcd-0")
	switch {
	case !rok:
		t.Fatal("the GPU rank's pod was never held")
	case !eok:
		t.Fatal("the companion's pod was never held")
	case rankHeld.After(etcdHeld):
		t.Errorf("the companion was held %s before the GPU rank", rankHeld.Sub(etcdHeld))
	case rankHeld.UnixNano() > released.Load():
		t.Errorf("the members were released %s before the GPU rank was held", time.Duration(rankHeld.UnixNano()-released.Load()))
	}
}

// wait returns at once when no step began, and after each step begun.
func TestInstanceHold_WaitCoversTheStepsBegun(t *testing.T) {
	log := logrus.NewEntry(logrus.New())
	hold := newInstanceHold(context.Background(), http.DefaultClient, "s9", nil, nil, log)
	done := make(chan struct{})
	go func() { hold.wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wait blocked with no step begun")
	}
	hold.holdRanks()
	hold.holdCompanions()
	hold.holdCompanions() // once only
	hold.wait()
	select {
	case <-hold.companions():
	default:
		t.Error("the companions' step did not end")
	}
}

// The steps the group capture driver wires in (gpushare_fabric.go) hold
// only the GPU ranks at the vote and the companions at the freeze point:
// wiring the companions' hold to the vote again would fail here.
func TestInstanceHold_StepsHoldRanksAtTheVoteAndCompanionsAtTheFreezePoint(t *testing.T) {
	log := logrus.NewEntry(logrus.New())
	rec := &holdRecorder{holds: map[string]time.Time{}}
	_, bRank := rec.server(t)
	_, bEtcd := rec.server(t)
	members := []CheckpointRequest{
		{Namespace: "ns", PodName: "worker-0", ContainerName: "main"},
		{Namespace: "ns", PodName: "etcd-0", ContainerName: "etcd", Companion: true},
	}
	hold := newInstanceHold(context.Background(), http.DefaultClient, "s11", members, []string{bRank, bEtcd}, log)
	steps := hold.steps()
	steps.onGo()
	waited := make(chan struct{})
	go func() { hold.wait(); close(waited) }() // every hold the vote began
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("the vote's step did not end")
	}
	if _, ok := rec.at("worker-0"); !ok {
		t.Error("the vote's step did not hold the GPU rank")
	}
	if _, ok := rec.at("etcd-0"); ok {
		t.Fatal("the vote's step held a companion")
	}
	steps.atFreezePoint()
	select {
	case <-steps.gate:
	case <-time.After(5 * time.Second):
		t.Fatal("the freeze point's gate did not open")
	}
	if _, ok := rec.at("etcd-0"); !ok {
		t.Error("the freeze point's step did not hold the companion")
	}
}

// Holds still on their way when the gate's wait ends abort the capture:
// the members are told to stop, not to dump with some pods held and
// others running.
func TestDriveGroupFreeze_HoldsNotInTimeAbort(t *testing.T) {
	saved := holdGateWait
	holdGateWait = 100 * time.Millisecond
	t.Cleanup(func() { holdGateWait = saved })
	log := logrus.NewEntry(logrus.New())
	a1, b1 := freezeServer(t)
	a2, b2 := freezeServer(t)
	ctx := context.Background()
	errs := make(chan error, 2)
	go func() { errs <- a1.awaitGroupFreeze(ctx, "s12", "ns", "worker-0", log) }()
	go func() { errs <- a2.awaitGroupFreeze(ctx, "s12", "ns", "etcd-0", log) }()
	never := make(chan struct{}) // the companions' hold never lands
	ok := driveGroupFreeze(ctx, http.DefaultClient, "s12", "", []freezeMember{
		{Base: b1, Namespace: "ns", Pod: "worker-0"}, {Base: b2, Namespace: "ns", Pod: "etcd-0"},
	}, 20*time.Millisecond, log, nil, never, nil)
	if ok {
		t.Fatal("the members were released to dump with the instance's holds still pending")
	}
	for range 2 {
		if err := <-errs; err == nil {
			t.Error("a member was released to dump; want it aborted")
		}
	}
}

// A companion's hold that stalls (its agent accepts the request and never
// answers) makes the gate's wait abort the capture; the abort also ends
// the stalled request, so the driver's wait for the holds, and with it
// the capture's failure handling, does not last until the capture's own
// deadline (an hour).
func TestDriveInstanceFreeze_GateAbortEndsStalledHolds(t *testing.T) {
	saved := holdGateWait
	// The gate's wait starts when the members reach the freeze point; the
	// companion's hold, sent then to a local server, must reach it first.
	holdGateWait = 500 * time.Millisecond
	t.Cleanup(func() { holdGateWait = saved })
	log := logrus.NewEntry(logrus.New())
	rec := &holdRecorder{holds: map[string]time.Time{}}
	aRank, bRank := rec.server(t)

	// The companion's agent: its hold never answers.
	aEtcd := &Agent{}
	unblock := make(chan struct{})
	holdStalls := make(chan struct{}, 1)
	r := mux.NewRouter()
	r.HandleFunc("/v1/group-freeze/{session}/{namespace}/{pod}", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Query().Get("phase") == "hold" {
			holdStalls <- struct{}{}
			select {
			case <-req.Context().Done():
			case <-unblock:
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		aEtcd.freezeHandler(w, req)
	}).Methods("GET", "POST")
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(unblock) }) // runs before srv.Close
	bEtcd := srv.URL

	ctx, cancel := context.WithCancel(context.Background()) // the capture's: ends long after the test
	t.Cleanup(cancel)
	members := []CheckpointRequest{
		{Namespace: "ns", PodName: "worker-0", ContainerName: "main"},
		{Namespace: "ns", PodName: "etcd-0", ContainerName: "etcd", Companion: true},
	}
	hold := newInstanceHold(ctx, http.DefaultClient, "s13", members, []string{bRank, bEtcd}, log)
	hold.steps().onGo() // the vote went
	select {            // landed, so the companions' hold is sent at the freeze point
	case <-hold.ranksDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the GPU rank's hold did not land")
	}
	errs := make(chan error, 2)
	go func() { errs <- aRank.awaitGroupFreeze(ctx, "s13", "ns", "worker-0", log) }()
	go func() { errs <- aEtcd.awaitGroupFreeze(ctx, "s13", "ns", "etcd-0", log) }()
	driveInstanceFreeze(ctx, http.DefaultClient, "s13", []freezeMember{
		{Base: bRank, Namespace: "ns", Pod: "worker-0"}, {Base: bEtcd, Namespace: "ns", Pod: "etcd-0"},
	}, hold, log)
	// The companion's hold reached its agent and stalled there, so the
	// abort below ended a request in flight, not one never sent.
	select {
	case <-holdStalls:
	default:
		t.Fatal("the companion's hold had not reached its agent when the gate aborted")
	}
	for range 2 {
		if err := <-errs; err == nil {
			t.Error("a member was released to dump with the companion's hold still pending; want it aborted")
		}
	}
	waited := make(chan struct{})
	go func() { hold.wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("the driver still waits for the stalled hold after the gate aborted the capture")
	}
}

// The gate's abort cancels the holds still on their way before it tells
// the members to stop: an abort that a member's agent is slow to take
// does not keep a companion's hold coming, to freeze its pod while the
// capture is being abandoned.
func TestDriveInstanceFreeze_GateAbortCancelsHoldsBeforeTheAbortIsTaken(t *testing.T) {
	saved := holdGateWait
	// The gate's wait starts when the members reach the freeze point; the
	// companion's hold, sent then to a local server, must reach it first.
	holdGateWait = 500 * time.Millisecond
	t.Cleanup(func() { holdGateWait = saved })
	log := logrus.NewEntry(logrus.New())
	rec := &holdRecorder{holds: map[string]time.Time{}}
	aRank, bRank := rec.server(t)

	// The companion's agent: its hold never answers, and it takes the
	// abort only once the test ends.
	aEtcd := &Agent{}
	unblock := make(chan struct{})
	holdStalls := make(chan struct{}, 1)
	holdCancelled := make(chan struct{}, 1)
	r := mux.NewRouter()
	r.HandleFunc("/v1/group-freeze/{session}/{namespace}/{pod}", func(w http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		switch {
		case q.Get("phase") == "hold":
			holdStalls <- struct{}{}
			select {
			case <-req.Context().Done():
				holdCancelled <- struct{}{}
			case <-unblock:
			}
			w.WriteHeader(http.StatusNoContent)
			return
		case req.Method == http.MethodPost && q.Get("go") == "false":
			<-unblock
		}
		aEtcd.freezeHandler(w, req)
	}).Methods("GET", "POST")
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(context.Background()) // the capture's
	t.Cleanup(func() { close(unblock); cancel() })          // runs before srv.Close
	bEtcd := srv.URL

	members := []CheckpointRequest{
		{Namespace: "ns", PodName: "worker-0", ContainerName: "main"},
		{Namespace: "ns", PodName: "etcd-0", ContainerName: "etcd", Companion: true},
	}
	hold := newInstanceHold(ctx, http.DefaultClient, "s14", members, []string{bRank, bEtcd}, log)
	hold.steps().onGo() // the vote went
	select {            // landed, so the companions' hold is sent at the freeze point
	case <-hold.ranksDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the GPU rank's hold did not land")
	}
	go func() { _ = aRank.awaitGroupFreeze(ctx, "s14", "ns", "worker-0", log) }()
	go func() { _ = aEtcd.awaitGroupFreeze(ctx, "s14", "ns", "etcd-0", log) }()
	driven := make(chan struct{})
	go func() {
		defer close(driven)
		driveInstanceFreeze(ctx, http.DefaultClient, "s14", []freezeMember{
			{Base: bRank, Namespace: "ns", Pod: "worker-0"}, {Base: bEtcd, Namespace: "ns", Pod: "etcd-0"},
		}, hold, log)
	}()
	select {
	case <-holdStalls:
	case <-time.After(5 * time.Second):
		t.Fatal("the companion's hold never reached its agent")
	}
	select {
	case <-holdCancelled:
	case <-driven:
		t.Fatal("the driver returned although the companion's agent has not taken the abort")
	case <-time.After(5 * time.Second):
		t.Fatal("the companion's hold still runs while the driver waits for its agent to take the abort")
	}
}
