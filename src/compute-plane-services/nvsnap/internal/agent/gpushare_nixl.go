// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// NIXL KV transfer around a capture (disaggregated vLLM, phase 1). A vLLM
// engine whose KV connector is NIXL over RDMA holds device files and
// mappings a checkpoint cannot save (checkpoint-restore/criu#267), and vLLM
// has no call to drop them. The GPU tool's nixl-pause has the engine stop
// taking requests, wait for KV transfers in flight, and shut its connector
// down on every worker; it fails while any RDMA device is still held (NCCL
// over RDMA, say). nixl-resume creates the connectors again on the same KV
// memory under a new engine id and, with --host, at the restored pod's
// address. Only pods that opt in (nvsnap.io/nixl-pause, which the webhook
// turns into NVSNAP_NIXL_PAUSE=1 in the process) are paused, and only by
// an agent whose own switch is on (--webhook-nixl-pause): anything else
// with RDMA is refused by the preflight as before.
//
// Unlike ModelExpress, a paused engine serves nothing. So every resume is
// checked: a source that runs on and does not resume is replaced
// (errSourceLeftSuspended), and a restore whose resume fails fails, so its
// replica starts cold. See docs/design/NIXL-PAUSE.md.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/types"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/containerd"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

const (
	// nixlPauseEnv marks a process whose pod opted in to nixl-pause
	// (webhook.NIXLPauseAnnotation, with the cluster's switch on).
	nixlPauseEnv = "NVSNAP_NIXL_PAUSE"
	// gpushareNIXLTimeout bounds a nixl-pause or nixl-resume: the engine
	// waits up to 60 s for transfers in flight, its TP workers up to 90 s
	// for the engine, and the tool up to 120 s for each reply.
	gpushareNIXLTimeout = 3 * time.Minute
)

// errNIXLPauseRefused is a nixl-pause that failed: the engine did not go
// idle in time, its connector is not handled, or a process still holds
// RDMA after the pause. The tool has resumed every pid.
var errNIXLPauseRefused = errors.New("the NIXL KV connector could not release the engine's RDMA state")

// errNIXLPauseUnsupported is a pod opted in to nixl-pause that it does not
// handle yet: nothing was paused.
var errNIXLPauseUnsupported = errors.New("nixl-pause: not supported for this workload")

// gpushareNIXLSupported reports whether the GPU tool (its binary's text,
// gpuToolText) has nixl-pause and nixl-resume. "" is false.
func gpushareNIXLSupported(toolText string) bool {
	return strings.Contains(toolText, "nixl-pause") && strings.Contains(toolText, "nixl-resume")
}

// procEnvSet reports whether any of the processes pids (host numbering
// under procBase) has key=value in its environment. A process that cannot
// be read counts as not.
func procEnvSet(procBase string, pids []int, key, value string) bool {
	for _, pid := range pids {
		env, err := os.ReadFile(filepath.Join(procBase, strconv.Itoa(pid), "environ"))
		if err != nil {
			continue
		}
		for _, kv := range bytes.Split(env, []byte{0}) {
			if k, v, ok := bytes.Cut(kv, []byte("=")); ok && string(k) == key && string(v) == value {
				return true
			}
		}
	}
	return false
}

// envOptsIntoNIXLPause reports whether the processes pids carry the
// nixl-pause marker in their environment. The marker alone does not opt a
// pod in: the agent's switch must be on too (Agent.nixlPauseOn).
func envOptsIntoNIXLPause(procBase string, pids []int) bool {
	return procEnvSet(procBase, pids, nixlPauseEnv, "1")
}

// gpushareNIXL runs nixl-pause or nixl-resume on pids (namespace pids)
// through the tool in the container whose init is hostPID; nixl-resume
// with host passes it as the address decode engines reach this one at. A
// variable for tests.
var gpushareNIXL = func(ctx context.Context, hostPID int, verb, host string, pids []int) (string, error) {
	args := make([]string, 0, len(pids)+3)
	if host != "" {
		args = append(args, "--host", host)
	}
	args = append(append(args, verb), intsToStrings(pids)...)
	return gpushareTool(ctx, hostPID, gpushareNIXLTimeout, args...)
}

// nixlReplies are the replies in the tool's output, by pid: one
// "pid=N nixl <verb>[ HOST]: <reply>" line per process it reached. A pid
// without a control socket gets no line.
func nixlReplies(out string) map[int]string {
	reps := map[int]string{}
	for _, line := range strings.Split(out, "\n") {
		head, rep, ok := strings.Cut(line, ": ")
		if !ok || !strings.HasPrefix(head, "pid=") {
			continue
		}
		f := strings.Fields(strings.TrimPrefix(head, "pid="))
		if len(f) < 2 || f[1] != "nixl" {
			continue
		}
		if pid, err := strconv.Atoi(f[0]); err == nil {
			reps[pid] = rep
		}
	}
	return reps
}

// nixlNothingPaused reports whether the pause's output shows that no
// process of pids released a connector: every pid answered "ok no ..."
// ("ok no NIXL KV connector", "ok no Python"). Anything else (a pid
// without an answer, output the agent cannot read) counts as paused: the
// source is then resumed whenever it runs on, and the checkpoint records
// the pause, which a resume of an engine not paused answers "not paused".
func nixlNothingPaused(out string, pids []int) bool {
	reps := nixlReplies(out)
	for _, pid := range pids {
		if !strings.HasPrefix(reps[pid], "ok no ") {
			return false
		}
	}
	return true
}

// nixlAllOK reports whether every pid answered the tool's verb with ok.
// The tool exits 0 for a pid without a control socket, which it did not
// reach.
func nixlAllOK(out string, pids []int) bool {
	reps := nixlReplies(out)
	for _, pid := range pids {
		if !strings.HasPrefix(reps[pid], "ok") {
			return false
		}
	}
	return true
}

// pauseNIXL has the NIXL KV connector in pids release its RDMA state
// before a capture's suspend, when the pod opted in (optedIn) and the GPU
// tool can (toolText). paused: a connector was released, and the source
// must get resumeNIXL whenever it runs on. A pod that did not opt in, or a
// tool without nixl-pause, is not paused and captures as before (the
// preflight refuses RDMA it cannot release). A pod that opted in but has
// no connector is not paused either. A failed pause returns
// errNIXLPauseRefused with the tool's reason, and the source has not been
// stopped. The tool resumes every pid when the pause fails, but a timeout
// or a cancel may stop it before it does, so the agent resumes them too
// (a resume of an engine that is not paused answers "not paused"); if
// that fails, the error also carries errSourceLeftSuspended and the
// source is replaced.
func pauseNIXL(ctx context.Context, optedIn bool, toolText string, hostPID int, pids []int, log *logrus.Entry) (paused bool, err error) {
	if !optedIn || len(pids) == 0 {
		return false, nil
	}
	if !gpushareNIXLSupported(toolText) {
		log.Info("gpushare: the pod opted in to nixl-pause, but the GPU tool has no nixl-pause; capturing it as it is")
		return false, nil
	}
	t0 := time.Now()
	out, err := gpushareNIXL(ctx, hostPID, "nixl-pause", "", pids)
	if err != nil {
		return false, resumeNIXLSource(ctx, hostPID, pids, fmt.Errorf("%w: %w", errNIXLPauseRefused, err), log)
	}
	l := log.WithFields(logrus.Fields{"pids": pids, "duration": time.Since(t0).Round(time.Millisecond).String()})
	if nixlNothingPaused(out, pids) {
		l.Info("gpushare: the pod opted in to nixl-pause, but no process has a NIXL KV connector")
		return false, nil
	}
	l.Info("gpushare: the NIXL KV connector released its RDMA state for the capture: " + lastLines(out, 1))
	return true, nil
}

// resumeNIXL has the NIXL KV connector in pids come back under a new
// engine id, at host when set. It runs to its end under its own deadline.
// An error means the engine takes no requests.
func resumeNIXL(ctx context.Context, hostPID int, host string, pids []int, log *logrus.Entry) error {
	t0 := time.Now()
	out, err := gpushareNIXL(context.WithoutCancel(ctx), hostPID, "nixl-resume", host, pids)
	l := log.WithFields(logrus.Fields{"pids": pids, "host": host, "duration": time.Since(t0).Round(time.Millisecond).String()})
	if err == nil && !nixlAllOK(out, pids) {
		err = fmt.Errorf("not every process answered ok: %s", lastLines(out, 4))
	}
	if err != nil {
		l.WithError(err).Warn("gpushare: the NIXL KV connector did not resume; the engine takes no requests")
		return fmt.Errorf("nixl-resume: %w", err)
	}
	l.Info("gpushare: the NIXL KV connector resumed: " + lastLines(out, 1))
	return nil
}

// resumeNIXLSource resumes a source whose capture paused its connector
// and that runs on. A failure leaves an engine that takes no requests in a
// pod that still reads Ready: reported as errSourceLeftSuspended, joined
// with the capture's err, so the caller replaces it.
func resumeNIXLSource(ctx context.Context, hostPID int, pids []int, err error, log *logrus.Entry) error {
	if rerr := resumeNIXL(ctx, hostPID, "", pids, log); rerr != nil {
		return errors.Join(err, fmt.Errorf("%w: %w", errSourceLeftSuspended, rerr))
	}
	return err
}

// resumeRestoredNIXL resumes the connector in a restore whose capture
// paused it (gs.NIXL), once the restored processes' GPU launches run and
// their network is unlocked, at host (the restored pod's address; "" keeps
// the captured one). A node bundle whose tool has no nixl-resume cannot
// bring the engine back: the restore fails, and its replica starts cold.
func resumeRestoredNIXL(ctx context.Context, gs *GPUShareInfo, toolText string, toolPID int, host string, log *logrus.Entry) error {
	if gs == nil || !gs.NIXL {
		return nil
	}
	if !gpushareNIXLSupported(toolText) {
		return errors.New("gpushare: the capture paused the NIXL KV connector, but this node's GPU tool has no nixl-resume")
	}
	return resumeNIXL(ctx, toolPID, host, gs.PIDs, log)
}

// restoredNIXLHost is the address a restored engine's side channel
// listens and is advertised at: its pod's own IP, podIP, when the restore
// moved it off the captured address; "" when it kept that address. An
// unknown podIP is an error: resumed on the captured address, the engine
// would advertise one its pod may not own, and decode engines could not
// reach it, so the restore fails and its replica starts cold.
func restoredNIXLHost(podIP, capturedIP string) (string, error) {
	if podIP == "" {
		return "", errors.New("gpushare: the restored pod's address is unknown, so the NIXL KV connector cannot be resumed at it")
	}
	if podIP == capturedIP {
		return "", nil
	}
	return podIP, nil
}

// restoredNIXLHostFor is restoredNIXLHost for the placeholder of group
// (its address from the restore's address map, else from the API), read
// only when the capture paused the connector.
func (a *Agent) restoredNIXLHostFor(ctx context.Context, gs *GPUShareInfo, capturedIP string, group restoreV2Group) (string, error) {
	if gs == nil || !gs.NIXL {
		return "", nil
	}
	return restoredNIXLHost(a.placeholderPodIP(ctx, group.Namespace, group.Pod, group.InetAddrMap, capturedIP), capturedIP)
}

// resumeRestoredNIXLFor resumes the connector of a restore of group at
// the restored pod's address (resumeRestoredNIXL, restoredNIXLHostFor).
func (a *Agent) resumeRestoredNIXLFor(ctx context.Context, gs *GPUShareInfo, toolText string, toolPID int, capturedIP string, group restoreV2Group, log *logrus.Entry) error {
	if gs == nil || !gs.NIXL {
		return nil
	}
	host, err := a.restoredNIXLHostFor(ctx, gs, capturedIP, group)
	if err != nil {
		return err
	}
	return resumeRestoredNIXL(ctx, gs, toolText, toolPID, host, log)
}

// nixlPausableByEnv are the processes of the container whose init is
// sessionHostPID (host numbering) that a capture's nixl-pause addresses,
// when the container opted in: the GPU processes under the library, the
// ones the capture suspends (gpushareTargets). nixl-pause checks the RDMA
// of these only. The opt-in is the container's: any of its processes
// carrying the marker. A process's own environ does not decide: vLLM
// renames its engine and workers with setproctitle, which overwrites the
// environ in place, so the engine that holds the RDMA no longer shows the
// marker the webhook set for its container.
func nixlPausableByEnv(procBase string, sessionHostPID int) map[int]bool {
	out := map[int]bool{}
	pids := sessionPIDs(procBase, sessionHostPID)
	if !envOptsIntoNIXLPause(procBase, pids) {
		return out
	}
	for _, pid := range pids {
		dir := filepath.Join(procBase, strconv.Itoa(pid))
		shim, gpu, _ := scanMaps(filepath.Join(dir, "maps"))
		if !gpu {
			gpu = holdsNvidiaFD(filepath.Join(dir, "fd"))
		}
		if shim && gpu {
			out[pid] = true
		}
	}
	return out
}

// groupSizeEnvs are variables a pod's controller sets to the number of
// pods its engine spans (LeaderWorkerSet, Grove).
var groupSizeEnvs = []string{"LWS_GROUP_SIZE", "GROVE_PCSG_TEMPLATE_NUM_PODS"}

// nixlSpansNodes says why the engine of the container whose processes are
// pids (host numbering) spans nodes, or "" when nothing says it does:
// the capture joins a multi-node session (fabricSession), or any of its
// processes carries the fabric marker, a group size above one, or a
// multi-node engine flag on its command line. Any process counts, since a
// process renamed with setproctitle has lost its environ and its command
// line; a container whose processes all lost theirs never opted in.
func nixlSpansNodes(procBase string, pids []int, fabricSession string) string {
	if fabricSession != "" {
		return "the capture joins the multi-node session " + fabricSession
	}
	if procEnvSet(procBase, pids, fabricEnv, "1") {
		return "the pod shares its GPU memory across nodes (" + fabricEnv + "=1)"
	}
	for _, pid := range pids {
		env, _ := os.ReadFile(filepath.Join(procBase, strconv.Itoa(pid), "environ"))
		for _, kv := range bytes.Split(env, []byte{0}) {
			k, v, ok := bytes.Cut(kv, []byte("="))
			if !ok || !slices.Contains(groupSizeEnvs, string(k)) {
				continue
			}
			if n, err := strconv.Atoi(string(v)); err == nil && n > 1 {
				return fmt.Sprintf("its pod is one of a group of %d (%s)", n, k)
			}
		}
		cmd, _ := os.ReadFile(filepath.Join(procBase, strconv.Itoa(pid), "cmdline"))
		if f := webhook.MultiNodeEngineArg(strings.Split(string(cmd), "\x00")); f != "" {
			return "its engine is started with " + f
		}
	}
	return ""
}

// nixlPauseLimit is why nixl-pause cannot handle the container whose
// processes are pids, or "": ModelExpress in the same pod (each pause
// fails while the other still holds RDMA), or an engine that spans nodes
// (nixlSpansNodes; untested, and its remote workers are released through
// the engine's own RPC).
func nixlPauseLimit(procBase string, pids []int, fabricSession string) string {
	if usesModelExpress(procBase, pids) {
		return "the pod opted in to nixl-pause and also loads through ModelExpress; NIXL KV and ModelExpress in one pod are not supported yet"
	}
	if why := nixlSpansNodes(procBase, pids, fabricSession); why != "" {
		return "the pod opted in to nixl-pause, but its engine spans nodes (" + why + "); nixl-pause is not supported for an engine that spans nodes yet"
	}
	return ""
}

// nixlPauseFindings applies nixl-pause to a container's preflight
// findings fs. The rdma findings of the processes in pausable
// (nixlPausable) become information: the capture has their connector
// release its RDMA right before the suspend, and fails there, before the
// instance is stopped, if any is still held. A container nixl-pause does
// not handle (limit, from nixlPauseLimit) is blocked instead, once. RDMA of
// any other process still blocks: nixl-pause does not check it, and the
// dump would fail on it.
func nixlPauseFindings(fs []PreflightFinding, pausable map[int]bool, limit, pod, container string) []PreflightFinding {
	if len(pausable) == 0 {
		return fs
	}
	out := make([]PreflightFinding, 0, len(fs)+1)
	for _, f := range fs {
		if limit == "" && f.Check == "rdma" && f.Severity == preflightBlock && pausable[f.PID] {
			f.Severity = preflightInfo
			f.Detail += "; the pod opted in to nixl-pause: its NIXL KV connector releases it right before the suspend, " +
				"and the capture fails there, before the instance is stopped, if any RDMA device is still held"
		}
		out = append(out, f)
	}
	if limit != "" {
		out = append(out, PreflightFinding{Pod: pod, Container: container, Severity: preflightBlock, Check: "nixl-pause", Detail: limit})
	}
	return out
}

// restoredNotServingError is a restore that failed after the restored
// processes ran in the placeholder, leaving an engine that takes no
// requests in a pod that may read Ready: its NIXL KV connector did not
// resume. Namespace, Pod, UID and Container name the placeholder, for
// whoever disposes of it (disposeFailedPlaceholder).
type restoredNotServingError struct {
	Namespace, Pod, Container string
	UID                       types.UID
	err                       error
}

func (e *restoredNotServingError) Error() string { return e.err.Error() }
func (e *restoredNotServingError) Unwrap() error { return e.err }

// restoredNotServing marks err, a failure after the restored processes
// ran in the placeholder info, as restoredNotServingError.
func restoredNotServing(info *containerd.ContainerInfo, err error) error {
	e := &restoredNotServingError{err: err}
	if info != nil {
		e.Namespace = info.Labels["io.kubernetes.pod.namespace"]
		e.Pod = info.Labels["io.kubernetes.pod.name"]
		e.Container = info.Labels["io.kubernetes.container.name"]
		e.UID = types.UID(info.Labels["io.kubernetes.pod.uid"])
	}
	return e
}

// nixlPauseOn reports whether this agent pauses NIXL KV connectors: its
// own switch, the cluster's (--webhook-nixl-pause, chart
// webhook.nixlPause.enabled). The process environment alone does not
// decide: a pod may set NVSNAP_NIXL_PAUSE=1 itself, which the webhook
// cannot always see (envFrom). Off, such a pod is captured as if it had
// not opted in, so its RDMA refuses the capture as before.
func (a *Agent) nixlPauseOn() bool { return a.config.Webhook.NIXLPause }

// nixlPausable is nixlPausableByEnv with the agent's switch on, and none
// with it off.
func (a *Agent) nixlPausable(procBase string, sessionHostPID int) map[int]bool {
	if !a.nixlPauseOn() {
		return map[int]bool{}
	}
	return nixlPausableByEnv(procBase, sessionHostPID)
}

// pauseNIXLFor is pauseNIXL for the capture of the container whose init is
// sessionHostPID, opted in when any of its processes carries the marker
// and the agent's switch is on. A marker with the switch off is logged and
// ignored. An engine that spans nodes (nixlSpansNodes: the capture joins
// fabricSession, or a process of the container says so) is refused here,
// before anything is paused, whether or not a preflight ran: a capture
// requested through POST /v1/checkpoints runs none.
func (a *Agent) pauseNIXLFor(ctx context.Context, procBase string, sessionHostPID int, fabricSession, toolText string, hostPID int, pids []int, log *logrus.Entry) (bool, error) {
	session := sessionPIDs(procBase, sessionHostPID)
	optedIn := envOptsIntoNIXLPause(procBase, session)
	if optedIn && !a.nixlPauseOn() {
		log.Warn("gpushare: the workload sets " + nixlPauseEnv + "=1, but this cluster does not enable nixl-pause (webhook.nixlPause.enabled); not pausing it")
		optedIn = false
	}
	if optedIn && gpushareNIXLSupported(toolText) {
		if why := nixlSpansNodes(procBase, session, fabricSession); why != "" {
			return false, fmt.Errorf("%w: the pod opted in to nixl-pause, but its engine spans nodes (%s); nixl-pause is not supported for an engine that spans nodes yet",
				errNIXLPauseUnsupported, why)
		}
	}
	return pauseNIXL(ctx, optedIn, toolText, hostPID, pids, log)
}
