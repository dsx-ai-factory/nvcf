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
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/types"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/containerd"
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

// nixlConnectorFound reports whether the tool's output (one
// "pid=N nixl pause: <reply>" line per process) shows a process that
// released a NIXL KV connector: an "ok" reply other than "ok no ..."
// ("ok no NIXL KV connector", "ok no Python").
func nixlConnectorFound(out string) bool {
	for _, line := range strings.Split(out, "\n") {
		_, rep, ok := strings.Cut(line, ": ")
		if !ok || !strings.HasPrefix(line, "pid=") {
			continue
		}
		if strings.HasPrefix(rep, "ok ") && !strings.HasPrefix(rep, "ok no ") {
			return true
		}
	}
	return false
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
	if !nixlConnectorFound(out) {
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
// sessionHostPID (host numbering) that a capture's nixl-pause addresses
// and whose pod opted in: the GPU processes under the library, the ones
// the capture suspends (gpushareTargets). nixl-pause checks the RDMA of
// these only.
func nixlPausableByEnv(procBase string, sessionHostPID int) map[int]bool {
	out := map[int]bool{}
	for _, pid := range sessionPIDs(procBase, sessionHostPID) {
		dir := filepath.Join(procBase, strconv.Itoa(pid))
		shim, gpu, _ := scanMaps(filepath.Join(dir, "maps"))
		if !gpu {
			gpu = holdsNvidiaFD(filepath.Join(dir, "fd"))
		}
		if shim && gpu && envOptsIntoNIXLPause(procBase, []int{pid}) {
			out[pid] = true
		}
	}
	return out
}

// nixlPauseFindings applies nixl-pause to a container's preflight
// findings fs. The rdma findings of the processes in pausable
// (nixlPausable) become information: the capture has their connector
// release its RDMA right before the suspend, and fails there, before the
// instance is stopped, if any is still held. Two cases nixl-pause does
// not handle yet block instead, once per container: a pod that also loads
// through ModelExpress (each pause fails while the other still holds
// RDMA), and an engine that spans nodes (untested; its remote workers are
// released through the engine's own RPC). RDMA of any other process still
// blocks: nixl-pause does not check it, and the dump would fail on it.
func nixlPauseFindings(fs []PreflightFinding, pausable map[int]bool, procBase, pod, container string) []PreflightFinding {
	if len(pausable) == 0 {
		return fs
	}
	pids := make([]int, 0, len(pausable))
	for pid := range pausable {
		pids = append(pids, pid)
	}
	var limit string
	switch {
	case usesModelExpress(procBase, pids):
		limit = "the pod opted in to nixl-pause and also loads through ModelExpress; NIXL KV and ModelExpress in one pod are not supported yet"
	case procEnvSet(procBase, pids, fabricEnv, "1"):
		limit = "the pod opted in to nixl-pause, but its engine spans nodes; nixl-pause is not supported for an engine that spans nodes yet"
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
// sessionHostPID, opted in when its processes carry the marker and the
// agent's switch is on. A marker with the switch off is logged and
// ignored.
func (a *Agent) pauseNIXLFor(ctx context.Context, procBase string, sessionHostPID int, toolText string, hostPID int, pids []int, log *logrus.Entry) (bool, error) {
	optedIn := envOptsIntoNIXLPause(procBase, sessionPIDs(procBase, sessionHostPID))
	if optedIn && !a.nixlPauseOn() {
		log.Warn("gpushare: the workload sets " + nixlPauseEnv + "=1, but this cluster does not enable nixl-pause (webhook.nixlPause.enabled); not pausing it")
		optedIn = false
	}
	return pauseNIXL(ctx, optedIn, toolText, hostPID, pids, log)
}
