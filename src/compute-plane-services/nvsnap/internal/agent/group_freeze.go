// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// The group freeze point. The members of a group capture are dumped by
// their own nodes' agents, each after its own preparation (a GPU rank's
// suspend takes minutes for a large model, a companion's none). Dumped as
// each was ready, they stopped seconds apart, and an instance that keeps
// time between its pods saw that gap as real: an etcd that kept counting
// while its client had already stopped expired the client's lease, and the
// restored client found its lease gone. So every member waits at a freeze
// point once ready, and the group's driver releases them together: their
// dumps start at the same moment.
//
// Starting together is not stopping together for long enough: a member
// dumped first resumed while another was still being dumped and talked to it
// (a NATS ping and its pong), so the two images held the same TCP connection
// at different points and the restored connection never resynchronised. So
// each member's criu also waits in its post-dump hook, its tree still
// stopped and its network still locked, until every member is dumped; then
// they all go on together: they resume when the capture leaves the instance
// running, and end when it stops it for good, as a group capture does
// (checkpointInstance).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// groupFreezeWait bounds how long a member waits at the freeze point, and
// the driver for every member to reach it: the GPU ranks' suspend comes
// first.
const groupFreezeWait = 30 * time.Minute

// freezePoint is one member's place at its group's freeze point.
type freezePoint struct {
	reached chan struct{} // closed when the member waits at the point
	release chan bool     // true: dump now; false: the capture is aborted
	once    sync.Once
}

type freezePoints struct {
	mu sync.Mutex
	m  map[string]*freezePoint
}

func freezeKey(session, ns, pod string) string { return session + "/" + ns + "/" + pod }

func (f *freezePoints) get(key string) *freezePoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.m == nil {
		f.m = map[string]*freezePoint{}
	}
	p := f.m[key]
	if p == nil {
		p = &freezePoint{reached: make(chan struct{}), release: make(chan bool, 1)}
		f.m[key] = p
	}
	return p
}

func (f *freezePoints) drop(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
}

// awaitGroupFreeze waits at the freeze point of group session until the
// driver releases the member.
func (a *Agent) awaitGroupFreeze(ctx context.Context, session, ns, pod string, log *logrus.Entry) error {
	key := freezeKey(session, ns, pod)
	p := a.freezes.get(key)
	defer a.freezes.drop(key)
	p.once.Do(func() { close(p.reached) })
	log.Info("group capture: at the freeze point; waiting for every member")
	t0 := time.Now()
	select {
	case ok := <-p.release:
		if !ok {
			return fmt.Errorf("group capture %s: aborted at the freeze point", session)
		}
		log.WithField("waited", time.Since(t0).Round(time.Millisecond).String()).Info("group capture: released; dumping")
		return nil
	case <-time.After(groupFreezeWait):
		return fmt.Errorf("group capture %s: not released from the freeze point within %s", session, groupFreezeWait)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// postDumpWait bounds how long a dumped member waits, stopped, for the
// others: the slowest member's dump.
const postDumpWait = 10 * time.Minute

// postDumpDir is the post-dump barrier of a member of group session: its
// hook writes "reached" there and waits for "go" or "abort".
func (a *Agent) postDumpDir(session, ns, pod string) string {
	return filepath.Join(a.config.CheckpointDir, ".group-freeze", session, ns+"_"+pod)
}

// writePostDumpScript writes the member's criu action script into dir: at
// post-dump (the tree dumped, still stopped, the network still locked) it
// reports the member dumped and waits for the driver. Any other action
// passes. A non-zero exit makes criu roll the dump back and resume the tree.
func writePostDumpScript(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("post-dump barrier: %w", err)
	}
	path := filepath.Join(dir, "post-dump.sh")
	if err := writeDumpActionScript(path, "", dir); err != nil {
		return "", fmt.Errorf("post-dump barrier: %w", err)
	}
	return path, nil
}

// postDumpBarrier is the action script's wait at a group's post-dump
// barrier in dir.
func postDumpBarrier(dir string) string {
	return fmt.Sprintf(`D=%q
: > "$D/reached"
i=0
while [ ! -e "$D/go" ]; do
	[ -e "$D/abort" ] && exit 1
	i=$((i+1)); [ "$i" -gt %d ] && exit 1
	sleep 0.05
done
`, dir, int(postDumpWait/(50*time.Millisecond)))
}

// markPostDumpReached reports a member that has no post-dump hook as past
// its dump.
func markPostDumpReached(dir string, log *logrus.Entry) {
	if err := os.MkdirAll(dir, 0o755); err == nil {
		err = os.WriteFile(filepath.Join(dir, "reached"), nil, 0o644)
		if err == nil {
			return
		}
	}
	log.Warn("group capture: cannot report the member dumped; its group's driver will abort the capture")
}

// markPostDumpFailed reports a member whose dump failed: its group's
// driver aborts the others at once instead of waiting out the barrier.
func markPostDumpFailed(dir string, log *logrus.Entry) {
	if err := os.MkdirAll(dir, 0o755); err == nil {
		err = os.WriteFile(filepath.Join(dir, "failed"), nil, 0o644)
		if err == nil {
			return
		}
	}
	log.Warn("group capture: cannot report the member's dump failed; its group's driver aborts the capture when the barrier times out")
}

// freezeHandler serves a member's freeze point to its group's driver:
// GET reports whether the member waits there, POST ?go=true|false releases
// or aborts it. ?phase=post is the member's post-dump barrier.
func (a *Agent) freezeHandler(w http.ResponseWriter, r *http.Request) {
	v := mux.Vars(r)
	switch r.URL.Query().Get("phase") {
	case "hold":
		// POST ?container=&keep=: freeze the member's pod (pod_hold.go).
		if r.Method != http.MethodPost {
			http.Error(w, "hold is POST", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		if err := a.holdMember(r.Context(), v["session"], v["namespace"], v["pod"], q.Get("container"), q.Get("keep") == "true"); err != nil {
			code := http.StatusInternalServerError
			if errors.Is(err, errHoldReleased) {
				code = http.StatusConflict
			}
			http.Error(w, err.Error(), code)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	case "release":
		a.releaseHold(holdKey(v["session"], v["namespace"], v["pod"]), a.log.WithField("pod", v["namespace"]+"/"+v["pod"]))
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.URL.Query().Get("phase") == "post" {
		dir := a.postDumpDir(v["session"], v["namespace"], v["pod"])
		if r.Method == http.MethodPost {
			name := "abort"
			if r.URL.Query().Get("go") == "true" {
				name = "go"
			}
			if err := os.MkdirAll(dir, 0o755); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, err := os.Stat(filepath.Join(dir, "reached"))
		_, ferr := os.Stat(filepath.Join(dir, "failed"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"reached": err == nil, "failed": ferr == nil})
		return
	}
	key := freezeKey(v["session"], v["namespace"], v["pod"])
	if r.Method == http.MethodPost {
		p := a.freezes.get(key)
		select {
		case p.release <- r.URL.Query().Get("go") == "true":
		default: // already released
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	reached := false
	a.freezes.mu.Lock()
	if p := a.freezes.m[key]; p != nil {
		select {
		case <-p.reached:
			reached = true
		default:
		}
	}
	a.freezes.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"reached": reached})
}

// freezeMember is one member of a group capture as its driver sees it.
type freezeMember struct {
	Base, Namespace, Pod string
	// Done reports that the member's capture ended (it failed before the
	// freeze point).
	Done func() bool
}

// driveGroupFreeze releases every member from the freeze point of phase
// ("" before the dump, "post" after it) once all wait there, or aborts them
// all when one ends before it, or none of that happens in time. An abort
// calls aborted, if set, before it tells the members.
func driveGroupFreeze(ctx context.Context, client *http.Client, session, phase string, members []freezeMember, poll time.Duration, log *logrus.Entry, reached func(), gate <-chan struct{}, aborted func()) bool {
	wait := groupFreezeWait
	if phase == "post" {
		wait = postDumpWait
	}
	deadline := time.Now().Add(wait)
	// release posts the decision of phase to every member and reports
	// whether each one acknowledged it. A member that did not (its agent
	// restarting, a dropped connection) is asked again briefly.
	release := func(ok bool, phase string) bool {
		var wg sync.WaitGroup
		var missed atomic.Int32
		for _, m := range members {
			wg.Add(1)
			go func(m freezeMember) {
				defer wg.Done()
				u := fmt.Sprintf("%s/v1/group-freeze/%s/%s/%s?go=%t&phase=%s", m.Base, url.PathEscape(session), url.PathEscape(m.Namespace), url.PathEscape(m.Pod), ok, url.QueryEscape(phase))
				var err error
				for attempt := 0; attempt < releaseAttempts; attempt++ {
					if attempt > 0 {
						time.Sleep(releaseRetry)
					}
					if err = postFreeze(context.WithoutCancel(ctx), client, u); err == nil {
						return
					}
				}
				missed.Add(1)
				log.WithError(err).WithField("pod", m.Namespace+"/"+m.Pod).Warn("group capture: a member did not get its freeze point's decision")
			}(m)
		}
		wg.Wait()
		return missed.Load() == 0
	}
	// abort tells every member to stop, once aborted has ended what the
	// capture no longer needs: a member's agent can be slow to take it.
	abort := func() {
		if aborted != nil {
			aborted()
		}
		release(false, phase)
	}
	for {
		all := true
		for _, m := range members {
			// Read the end before the point: a member without a post-dump
			// hook reports it and returns at once.
			done := m.Done != nil && m.Done()
			reached, failed := freezeState(ctx, client, m.Base, session, phase, m.Namespace, m.Pod)
			if reached && !failed {
				continue
			}
			if done || failed {
				log.WithFields(logrus.Fields{"pod": m.Namespace + "/" + m.Pod, "phase": phaseName(phase)}).Warn("group capture: a member ended before the freeze point; aborting the others")
				abort()
				return false
			}
			all = false
		}
		if all {
			if reached != nil {
				reached()
			}
			if gate != nil {
				// The rest of the instance is being held. A hold still
				// on its way when the members dump could land in the
				// middle of the dumps, with some companions stopped and
				// others running: the capture is aborted instead, and
				// the instance starts again to be captured later. (A
				// hold that failed outright has ended: its pod runs, as
				// in a capture without holds.)
				held := false
				select {
				case <-gate:
					held = true
				case <-time.After(holdGateWait):
					log.WithField("wait", holdGateWait).Warn("group capture: the instance's pods are not all held in time; aborting the capture rather than dump it partly held")
				case <-ctx.Done():
				}
				if !held {
					abort()
					return false
				}
			}
			if !release(true, phase) {
				// The member that missed it stays at its freeze point
				// while the rest dump and then wait for it at the
				// post-dump barrier: end the capture now instead. The
				// post-dump abort is recorded ahead of the members that
				// already dump, and they stop on reaching the barrier.
				log.WithField("phase", phaseName(phase)).Warn("group capture: a member was not released; aborting the others")
				abort()
				if phase == "" {
					release(false, "post")
				}
				return false
			}
			log.WithFields(logrus.Fields{"members": len(members), "phase": phaseName(phase)}).Info("group capture: every member at the freeze point; released together")
			return true
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			log.Warn("group capture: not every member reached the freeze point; aborting")
			abort()
			return false
		}
		time.Sleep(poll)
	}
}

// releaseAttempts and releaseRetry bound how long the driver keeps asking a
// member that did not acknowledge its freeze point's decision.
const releaseAttempts = 3

var releaseRetry = 500 * time.Millisecond

// postFreeze posts to a member's freeze point; an error unless the member's
// agent accepted it.
func postFreeze(ctx context.Context, client *http.Client, u string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("status %s", resp.Status)
	}
	return nil
}

func phaseName(phase string) string {
	if phase == "" {
		return "before the dump"
	}
	return strings.TrimSpace(phase) + "-dump"
}

// freezeState reports whether a member waits at the freeze point of
// phase, and whether it failed before it (post-dump: its dump failed).
func freezeState(ctx context.Context, client *http.Client, base, session, phase, ns, pod string) (reached, failed bool) {
	u := fmt.Sprintf("%s/v1/group-freeze/%s/%s/%s?phase=%s", base, url.PathEscape(session), url.PathEscape(ns), url.PathEscape(pod), url.QueryEscape(phase))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false, false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, false
	}
	defer resp.Body.Close()
	var out map[string]bool
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&out) != nil {
		return false, false
	}
	return out["reached"], out["failed"]
}

// holdGateWait bounds the wait at the freeze point for the members' pods
// to be held.
var holdGateWait = 30 * time.Second

// instanceHold holds the member pods of a group capture (pod_hold.go) in
// two steps, so that nothing the GPU ranks talk to stops before they do:
//
//   - When the ranks' quiesce vote goes, the GPU ranks' pods are held, each
//     but for the container it saves: a sidecar such as a gang-health
//     watchdog stops before it can count the suspending engine as failed.
//   - Once every member waits at the freeze point, the GPU ranks are
//     suspended and the companions' pods are held. Holding a companion
//     earlier stopped it for the whole of the ranks' GPU save (a minute
//     for a large model) while their engines still ran: a Dynamo engine
//     lost its lease to the stopped etcd and began to shut down, the dump
//     saved it shutting down, and its restore finished the shutdown.
//
// Each step runs at most once; wait returns when the steps begun have
// landed, or failed. cancel ends the holds still on their way: a capture
// that aborts has no use for them, and its driver must not wait for a
// stalled one until the capture's own deadline.
type instanceHold struct {
	ctx     context.Context
	cancel  context.CancelFunc
	client  *http.Client
	session string
	members []CheckpointRequest
	bases   []string
	log     *logrus.Entry

	ranksOnce, companionsOnce   sync.Once
	ranksBegun, companionsBegun atomic.Bool
	ranksDone, companionsDone   chan struct{}
}

func newInstanceHold(ctx context.Context, client *http.Client, session string, members []CheckpointRequest, bases []string, log *logrus.Entry) *instanceHold {
	ctx, cancel := context.WithCancel(ctx)
	return &instanceHold{ctx: ctx, cancel: cancel, client: client, session: session, members: members, bases: bases, log: log,
		ranksDone: make(chan struct{}), companionsDone: make(chan struct{})}
}

// holdRanks holds the GPU ranks' pods, in the background.
func (h *instanceHold) holdRanks() {
	h.ranksOnce.Do(func() {
		h.ranksBegun.Store(true)
		go func() {
			defer close(h.ranksDone)
			holdMembers(h.ctx, h.client, h.session, h.members, h.bases, false, h.log)
		}()
	})
}

// holdCompanions holds the companions' pods, in the background, once the
// ranks' holds have landed; companions() is closed when it is done.
//
// Every member waits at the freeze point only after the vote went, so the
// ranks' hold is due by then even if the driver has not yet seen the go
// (the members can read it a poll before the driver does): it is begun
// here, at most once, and waited for, so no rank's sidecar still runs
// once the gate opens.
func (h *instanceHold) holdCompanions() {
	h.companionsOnce.Do(func() {
		h.companionsBegun.Store(true)
		h.holdRanks()
		go func() {
			defer close(h.companionsDone)
			<-h.ranksDone
			holdMembers(h.ctx, h.client, h.session, h.members, h.bases, true, h.log)
		}()
	})
}

// holdSteps are when a group capture holds its instance's pods: onGo
// when the quiesce vote goes, atFreezePoint once every member waits at
// the freeze point, whose release waits for gate.
type holdSteps struct {
	onGo, atFreezePoint func()
	gate                <-chan struct{}
}

// steps is the order the group capture driver holds h's pods in: the GPU
// ranks at the vote, the companions only at the freeze point.
func (h *instanceHold) steps() holdSteps {
	return holdSteps{onGo: h.holdRanks, atFreezePoint: h.holdCompanions, gate: h.companions()}
}

// companions is closed once the companions are held: the freeze point's gate.
func (h *instanceHold) companions() <-chan struct{} { return h.companionsDone }

// wait returns when every step begun has landed or failed: a hold still on
// its way would land after the members' release and keep its pod frozen.
func (h *instanceHold) wait() {
	if h.ranksBegun.Load() {
		<-h.ranksDone
	}
	if h.companionsBegun.Load() {
		<-h.companionsDone
	}
}

// driveInstanceFreeze drives a group capture's freeze points, before the
// dump and after it, with hold's steps at the first. A capture aborted
// before the dump (a member ended, the holds did not land in time) cancels
// the holds still on their way, before it tells the members to stop: the
// members' hold releases end any that landed (an agent refuses a hold that
// comes after its release).
func driveInstanceFreeze(ctx context.Context, client *http.Client, session string, members []freezeMember, hold *instanceHold, log *logrus.Entry) {
	steps := hold.steps()
	if !driveGroupFreeze(ctx, client, session, "", members, 200*time.Millisecond, log, steps.atFreezePoint, steps.gate, hold.cancel) {
		return
	}
	// Dumped, the members wait stopped until all are: none resumes to
	// talk to one not dumped yet.
	driveGroupFreeze(ctx, client, session, "post", members, 100*time.Millisecond, log, nil, nil, nil)
}

// holdMembers holds the member pods of capture session that are
// companions, or GPU ranks (pod_hold.go): a companion's whole pod, a GPU
// rank's but for the container it saves.
func holdMembers(ctx context.Context, client *http.Client, session string, members []CheckpointRequest, bases []string, companions bool, log *logrus.Entry) {
	var wg sync.WaitGroup
	n := 0
	for i := range members {
		if members[i].Companion != companions {
			continue
		}
		n++
		wg.Add(1)
		go func(base string, m *CheckpointRequest) {
			defer wg.Done()
			u := fmt.Sprintf("%s/v1/group-freeze/%s/%s/%s?phase=hold&container=%s&keep=%t", base, url.PathEscape(session),
				url.PathEscape(m.Namespace), url.PathEscape(m.PodName), url.QueryEscape(m.ContainerName), !m.Companion)
			if err := postNoContent(ctx, client, u); err != nil {
				log.WithError(err).WithField("pod", m.Namespace+"/"+m.PodName).Warn("group capture: cannot hold a member's pod; it keeps running during the capture")
			}
		}(bases[i], &members[i])
	}
	wg.Wait()
	what := "the GPU ranks' pods are held, each but for its saving container"
	if companions {
		what = "the companions' pods are held"
	}
	log.WithField("members", n).Info("group capture: " + what)
}

// releaseMembers ends every member's hold; a member ends its own with its
// capture, so this only covers one that did not.
func releaseMembers(ctx context.Context, client *http.Client, session string, members []CheckpointRequest, bases []string, log *logrus.Entry) {
	for i := range members {
		m := &members[i]
		u := fmt.Sprintf("%s/v1/group-freeze/%s/%s/%s?phase=release", bases[i], url.PathEscape(session), url.PathEscape(m.Namespace), url.PathEscape(m.PodName))
		if err := postNoContent(context.WithoutCancel(ctx), client, u); err != nil {
			log.WithError(err).WithField("pod", m.Namespace+"/"+m.PodName).Warn("group capture: cannot release a member's hold; its agent thaws it on its own")
		}
	}
}

func postNoContent(ctx context.Context, client *http.Client, u string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}
