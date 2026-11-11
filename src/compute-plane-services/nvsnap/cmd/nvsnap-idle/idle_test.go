// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package idle tests cmd/nvsnap-idle, the C program that holds CRIU restore
// placeholders.
package idle

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The placeholder contract: one thread, always one live child, exit 0 on
// SIGTERM. A restore needs every task but init above the reserved pid floor
// soon after the agent raises it, which only holds if nothing else lives
// here for long.
func TestIdle(t *testing.T) {
	cc, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("no C compiler")
	}
	bin := filepath.Join(t.TempDir(), "nvsnap-idle")
	if out, err := exec.Command(cc, "-static", "-Os", "-Wall", "-Wextra", "-Werror", "-o", bin, "c/nvsnap-idle.c").CombinedOutput(); err != nil {
		if strings.Contains(string(out), "cannot find -lc") {
			t.Skip("no static libc")
		}
		t.Fatalf("compile: %v\n%s", err, out)
	}
	cmd := exec.Command(bin)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	time.Sleep(200 * time.Millisecond)

	tasks, _ := os.ReadDir(fmt.Sprintf("/proc/%d/task", pid))
	if len(tasks) != 1 {
		t.Errorf("want a single thread, got %d", len(tasks))
	}
	children := func() []string {
		b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", pid, pid))
		return strings.Fields(string(b))
	}
	first := children()
	if len(first) != 1 {
		t.Fatalf("want one live child, got %v", first)
	}
	// The child is replaced as it exits, one at a time.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c := children()
		if len(c) > 1 {
			t.Fatalf("more than one child at once: %v", c)
		}
		if len(c) == 1 && c[0] != first[0] {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if c := children(); len(c) != 1 || c[0] == first[0] {
		t.Errorf("the child was not replaced after a tick: %v -> %v", first, c)
	}

	start := time.Now()
	_ = cmd.Process.Signal(syscall.SIGTERM)
	if err := cmd.Wait(); err != nil {
		t.Errorf("want exit 0 on SIGTERM, got %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("took %s to stop", d)
	}
}

// buildIdle compiles the program the way the agent image does.
func buildIdle(t *testing.T) string {
	t.Helper()
	cc, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("no C compiler")
	}
	bin := filepath.Join(t.TempDir(), "nvsnap-idle")
	if out, err := exec.CommandContext(t.Context(), cc, "-static", "-Os", "-Wall", "-Wextra", "-Werror", "-o", bin, "c/nvsnap-idle.c").CombinedOutput(); err != nil { //nolint:gosec // runs the program under test
		if strings.Contains(string(out), "cannot find -lc") {
			t.Skip("no static libc")
		}
		t.Fatalf("compile: %v\n%s", err, out)
	}
	return bin
}

// A placeholder given a cold start idles until its marker exists; the agent
// creates the marker and kills it, and the restarted container execs the
// workload as its init, with its arguments intact, and again on every later
// restart.
func TestIdleColdStart(t *testing.T) {
	bin := buildIdle(t)
	dir := t.TempDir()
	marker, out := filepath.Join(dir, "cold-start"), filepath.Join(dir, "out")
	workload := []string{"/bin/sh", "-c", `echo "$0 $1" > ` + out + `; exec sleep 30`, "a b", "c"}
	args := append([]string{"--cold-start", marker, "--"}, workload...)

	idle := exec.CommandContext(t.Context(), bin, args...) //nolint:gosec // runs the program under test
	if err := idle.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := os.Stat(out); err == nil {
		t.Fatal("the workload started without the marker")
	}
	if b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/comm", idle.Process.Pid)); strings.TrimSpace(string(b)) != "nvsnap-idle" {
		t.Errorf("without the marker it runs %q, want the idle program", b)
	}
	// What the agent does: the marker, then a kill of the container's init.
	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_ = idle.Process.Kill()
	_ = idle.Wait()

	for restart := range 2 {
		_ = os.Remove(out)
		c := exec.CommandContext(t.Context(), bin, args...) //nolint:gosec // runs the program under test
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if b, _ := os.ReadFile(out); strings.TrimSpace(string(b)) == "a b c" {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if b, _ := os.ReadFile(out); strings.TrimSpace(string(b)) != "a b c" {
			t.Fatalf("restart %d: the workload did not start with its arguments intact: %q", restart, b)
		}
		// Exec, not a child: the workload is the container's init.
		if b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/comm", c.Process.Pid)); strings.TrimSpace(string(b)) != "sleep" {
			t.Errorf("restart %d: pid %d runs %q, want the workload in place", restart, c.Process.Pid, b)
		}
		_ = c.Process.Kill()
		_ = c.Wait()
	}
}

// While it idles, a placeholder given a cold start shows only its own
// arguments, so a tool that finds the workload by its command line (a gang
// health sidecar matching "-m dynamo.vllm", pkill -f) does not take it, or
// its ticking children, for the workload. The agent still finds the marker.
func TestIdleColdStartHidesWorkload(t *testing.T) {
	bin := buildIdle(t)
	marker := filepath.Join(t.TempDir(), "cold-start")
	cmd := exec.CommandContext(t.Context(), bin, "--cold-start", marker, "--", "python3", "-m", "dynamo.vllm", "--model", "/m") //nolint:gosec // runs the program under test
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	time.Sleep(300 * time.Millisecond)
	args := func(pid int) []string {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			return nil
		}
		var out []string
		for _, a := range strings.Split(string(b), "\x00") {
			if a != "" {
				out = append(out, a)
			}
		}
		return out
	}
	want := []string{bin, "--cold-start", marker, "--"}
	if got := args(cmd.Process.Pid); !slices.Equal(got, want) {
		t.Errorf("idle command line %q, want %q", got, want)
	}
	b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", cmd.Process.Pid, cmd.Process.Pid))
	kids := strings.Fields(string(b))
	if len(kids) != 1 {
		t.Fatalf("want one live child, got %v", kids)
	}
	kid, _ := strconv.Atoi(kids[0])
	if got := args(kid); got != nil && !slices.Equal(got, want) {
		t.Errorf("child command line %q, want %q", got, want)
	}
}

// A workload that cannot be started is reported, not idled over.
func TestIdleColdStartExecFailure(t *testing.T) {
	bin := buildIdle(t)
	marker := filepath.Join(t.TempDir(), "cold-start")
	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	err := exec.CommandContext(t.Context(), bin, "--cold-start", marker, "--", "/nonexistent/workload").Run() //nolint:gosec // runs the program under test
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 127 {
		t.Errorf("want exit 127, got %v", err)
	}
}

// SIGTERM stops a placeholder that may cold start, without starting it.
func TestIdleColdStartStopsOnTerm(t *testing.T) {
	bin := buildIdle(t)
	dir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), bin, "--cold-start", filepath.Join(dir, "cold-start"), "--", "/bin/sh", "-c", "touch "+filepath.Join(dir, "ran")) //nolint:gosec // runs the program under test
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	_ = cmd.Process.Signal(syscall.SIGTERM)
	if err := cmd.Wait(); err != nil {
		t.Errorf("want exit 0 on SIGTERM, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ran")); err == nil {
		t.Error("the workload ran on SIGTERM")
	}
}

func TestIdleUsage(t *testing.T) {
	bin := buildIdle(t)
	for _, args := range [][]string{
		{"--cold-start"},
		{"--cold-start", "/m", "--"},
		{"--cold-start", "", "--", "/bin/true"},
		{"--cold-start", "/m", "x", "/bin/true"},
		{"--other", "/m", "--", "/bin/true"},
	} {
		err := exec.CommandContext(t.Context(), bin, args...).Run() //nolint:gosec // runs the program under test
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != 2 {
			t.Errorf("%q: want exit 2, got %v", args, err)
		}
	}
}

// startWorkload starts a stand-in for a restored workload beside the
// placeholder (not its child): on SIGTERM it notes the signal, takes a
// moment to shut down, and notes that it is done. The channel is closed
// once it has exited (and been reaped: a zombie still answers signal 0).
func startWorkload(t *testing.T, dir string) (*exec.Cmd, <-chan struct{}) {
	t.Helper()
	got, done := filepath.Join(dir, "got-term"), filepath.Join(dir, "done")
	w := exec.CommandContext(t.Context(), "/bin/sh", "-c", //nolint:gosec // a test fixture
		"trap 'touch "+got+"; sleep 1; touch "+done+"; exit 0' TERM; while :; do sleep 0.1; done")
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() { _ = w.Wait(); close(exited) }()
	t.Cleanup(func() { _ = w.Process.Kill(); <-exited })
	return w, exited
}

// On SIGTERM the placeholder passes the signal to the rest of its container
// (here, the pids NVSNAP_IDLE_PROCS lists) and exits only once they have
// exited: a restored workload shuts down as it would have without nvsnap.
func TestIdleForwardsTermToTheContainer(t *testing.T) {
	bin := buildIdle(t)
	dir := t.TempDir()
	w, waited := startWorkload(t, dir)
	procs := filepath.Join(dir, "cgroup.procs")
	if err := os.WriteFile(procs, []byte(strconv.Itoa(w.Process.Pid)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	idle := exec.CommandContext(t.Context(), bin, "--cold-start", filepath.Join(dir, "cold-start"), "--", "/bin/true") //nolint:gosec // runs the program under test
	idle.Env = append(os.Environ(), "NVSNAP_IDLE_PROCS="+procs)
	if err := idle.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := idle.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- idle.Wait() }()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("placeholder: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the placeholder did not exit after the workload did")
	}
	if _, err := os.Stat(filepath.Join(dir, "got-term")); err != nil {
		t.Fatal("the workload never got SIGTERM")
	}
	if _, err := os.Stat(filepath.Join(dir, "done")); err != nil {
		t.Error("the placeholder exited before the workload finished shutting down")
	}
	<-waited
}

// Outside a Kubernetes pod the placeholder's cgroup may hold unrelated
// processes: nothing is passed the signal.
func TestIdleForwardsNothingOutsideAPod(t *testing.T) {
	bin := buildIdle(t)
	dir := t.TempDir()
	_, exited := startWorkload(t, dir)
	idle := exec.CommandContext(t.Context(), bin) //nolint:gosec // runs the program under test
	env := make([]string, 0, len(os.Environ()))
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "KUBERNETES_SERVICE_HOST=") && !strings.HasPrefix(e, "NVSNAP_IDLE_PROCS=") {
			env = append(env, e)
		}
	}
	idle.Env = env
	if err := idle.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	_ = idle.Process.Signal(syscall.SIGTERM)
	if err := idle.Wait(); err != nil {
		t.Fatalf("placeholder: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(dir, "got-term")); err == nil {
		t.Error("a process outside a pod's container got SIGTERM")
	}
	select {
	case <-exited:
		t.Error("the unrelated process exited")
	default:
	}
}

// fakeSelf writes stand-ins for /proc/self/cgroup and /proc/self/mountinfo
// (NVSNAP_IDLE_SELF) naming this container's cgroup as cg, with a cgroup2
// mount of root at a directory of the test; it returns that directory.
func fakeSelf(t *testing.T, dir, cg, root string) string {
	t.Helper()
	mnt := filepath.Join(dir, "cgroup")
	if err := os.MkdirAll(mnt, 0o700); err != nil {
		t.Fatal(err)
	}
	self := filepath.Join(dir, "self")
	if err := os.MkdirAll(self, 0o700); err != nil {
		t.Fatal(err)
	}
	mountinfo := "25 1 0:22 / / rw - ext4 /dev/root rw\n" +
		"30 25 0:26 " + root + " " + mnt + " rw,nosuid - cgroup2 cgroup2 rw\n"
	for name, body := range map[string]string{"cgroup": "0::" + cg + "\n", "mountinfo": mountinfo} {
		if err := os.WriteFile(filepath.Join(self, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return mnt
}

// runIdleInPod starts the placeholder as in a pod (KUBERNETES_SERVICE_HOST
// set, no NVSNAP_IDLE_PROCS), with NVSNAP_IDLE_SELF at dir/self, sends it
// SIGTERM and returns its stderr once it has exited.
func runIdleInPod(t *testing.T, bin, dir string) string {
	t.Helper()
	idle := exec.CommandContext(t.Context(), bin) //nolint:gosec // runs the program under test
	env := []string{"KUBERNETES_SERVICE_HOST=10.0.0.1", "NVSNAP_IDLE_SELF=" + filepath.Join(dir, "self")}
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "KUBERNETES_SERVICE_HOST=") && !strings.HasPrefix(e, "NVSNAP_IDLE_") {
			env = append(env, e)
		}
	}
	idle.Env = env
	var stderr strings.Builder
	idle.Stderr = &stderr
	if err := idle.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	_ = idle.Process.Signal(syscall.SIGTERM)
	exited := make(chan error, 1)
	go func() { exited <- idle.Wait() }()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("placeholder: %v", err)
		}
	case <-time.After(10 * time.Second):
		_ = idle.Process.Kill()
		t.Fatal("the placeholder did not exit")
	}
	return stderr.String()
}

// The container's cgroup is found through the cgroup2 mount that shows it:
// a mount rooted at the container's own cgroup (as a runtime mounts it)
// lists the container's processes at its top, whatever the cgroup's path.
func TestIdleFindsTheContainerThroughItsCgroupMount(t *testing.T) {
	bin := buildIdle(t)
	dir := t.TempDir()
	w, waited := startWorkload(t, dir)
	cg := "/kubepods.slice/kubepods-pod1.slice/cri-containerd-c1.scope"
	mnt := fakeSelf(t, dir, cg, cg)
	if err := os.WriteFile(filepath.Join(mnt, "cgroup.procs"), []byte(strconv.Itoa(w.Process.Pid)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runIdleInPod(t, bin, dir)
	if _, err := os.Stat(filepath.Join(dir, "got-term")); err != nil {
		t.Fatal("the workload never got SIGTERM")
	}
	if _, err := os.Stat(filepath.Join(dir, "done")); err != nil {
		t.Error("the placeholder exited before the workload finished shutting down")
	}
	<-waited
}

// A cgroup namespace's "/" is the container's cgroup only if the cgroup2
// mount shows that namespace's root. A mount of a cgroup above it (the
// host's root bind-mounted into the container) lists other processes:
// nothing is passed the signal.
func TestIdleRefusesACgroupMountAboveTheContainer(t *testing.T) {
	bin := buildIdle(t)
	dir := t.TempDir()
	w, exited := startWorkload(t, dir)
	mnt := fakeSelf(t, dir, "/", "/../../..")
	if err := os.WriteFile(filepath.Join(mnt, "cgroup.procs"), []byte(strconv.Itoa(w.Process.Pid)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runIdleInPod(t, bin, dir)
	time.Sleep(300 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(dir, "got-term")); err == nil {
		t.Error("a process listed by a cgroup mount above the container got SIGTERM")
	}
	select {
	case <-exited:
		t.Error("the unrelated process exited")
	default:
	}
}

// A process of the container the placeholder may not signal (EPERM) is
// reported, and not waited for: it would only end at the kubelet's SIGKILL.
// The others are still passed SIGTERM and waited for.
func TestIdleReportsAProcessItCannotSignal(t *testing.T) {
	// pid 1 is the stand-in only if this test may not signal it: under the
	// same user (a container's init) SIGTERM would reach it.
	if err := syscall.Kill(1, 0); !errors.Is(err, syscall.EPERM) {
		t.Skipf("pid 1 can be signaled here (%v)", err)
	}
	bin := buildIdle(t)
	dir := t.TempDir()
	w, waited := startWorkload(t, dir)
	procs := filepath.Join(dir, "cgroup.procs")
	if err := os.WriteFile(procs, []byte("1\n"+strconv.Itoa(w.Process.Pid)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	idle := exec.CommandContext(t.Context(), bin) //nolint:gosec // runs the program under test
	idle.Env = append(os.Environ(), "NVSNAP_IDLE_PROCS="+procs)
	var stderr strings.Builder
	idle.Stderr = &stderr
	if err := idle.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	_ = idle.Process.Signal(syscall.SIGTERM)
	exited := make(chan error, 1)
	go func() { exited <- idle.Wait() }()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("placeholder: %v", err)
		}
	case <-time.After(10 * time.Second):
		_ = idle.Process.Kill()
		t.Fatal("the placeholder waited for a process it could not signal")
	}
	if _, err := os.Stat(filepath.Join(dir, "done")); err != nil {
		t.Error("the placeholder exited before the workload it could signal finished shutting down")
	}
	if !strings.Contains(stderr.String(), "cannot pass SIGTERM to pid 1") {
		t.Errorf("the failed send was not reported; stderr: %q", stderr.String())
	}
	<-waited
}
