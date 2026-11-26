// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
)

func diagSocket(src, dst string, sport, dport uint16, state uint8, inode uint32) *netlink.Socket {
	s := &netlink.Socket{State: state, INode: inode}
	s.ID.Source, s.ID.Destination = net.ParseIP(src), net.ParseIP(dst)
	s.ID.SourcePort, s.ID.DestinationPort = sport, dport
	return s
}

func TestOutsidePeers(t *testing.T) {
	own := func(i uint32) bool { return i < 100 }
	sockets := []*netlink.Socket{
		diagSocket("10.0.0.1", "10.96.7.224", 51286, 4222, tcpStateEstablished, 1), // NATS: reset
		diagSocket("10.0.0.1", "10.0.0.9", 44000, 2379, tcpStateEstablished, 2),    // etcd: reset
		diagSocket("10.0.0.1", "10.0.0.7", 9090, 51000, tcpStateEstablished, 3),    // a frontend's connection in: reset
		diagSocket("10.0.0.1", "10.0.0.2", 40000, 29500, tcpStateEstablished, 4),   // a member of the group: kept
		diagSocket("127.0.0.1", "127.0.0.1", 40001, 6100, tcpStateEstablished, 5),  // loopback: kept
		diagSocket("10.0.0.1", "10.0.0.9", 44001, 2379, tcpStateEstablished, 500),  // a sidecar's: not the container's
		diagSocket("10.0.0.1", "10.0.0.9", 44002, 2379, tcpStateListen, 6),         // not established
		diagSocket("::ffff:10.0.0.1", "::ffff:10.0.0.10", 44003, 2379, tcpStateEstablished, 7),
		diagSocket("fd00::1", "fd00::2", 44004, 29500, tcpStateEstablished, 8), // a member's secondary address: kept
	}
	members := []*corev1.Pod{{Status: corev1.PodStatus{PodIP: "10.0.0.1"}}, {Status: corev1.PodStatus{PodIP: "10.0.0.2", PodIPs: []corev1.PodIP{{IP: "10.0.0.2"}, {IP: "fd00::2"}}}}}
	got := outsidePeers(sockets, own, memberAddresses(members))
	s := make([]string, 0, len(got))
	for _, c := range got {
		s = append(s, c.Local.String()+">"+c.Remote.String())
	}
	want := "10.0.0.1:51286>10.96.7.224:4222 10.0.0.1:44000>10.0.0.9:2379 10.0.0.1:9090>10.0.0.7:51000 10.0.0.1:44003>10.0.0.10:2379"
	if strings.Join(s, " ") != want {
		t.Fatalf("got  %s\nwant %s", strings.Join(s, " "), want)
	}
}

func TestPeerResetContext(t *testing.T) {
	if peerResetFrom(context.Background()) != nil {
		t.Fatal("reset asked of a plain capture")
	}
	if got := peerResetFrom(withPeerReset(context.Background(), []string{"10.0.0.1"})); len(got) != 1 {
		t.Fatal("lost")
	}
	if a := (&Agent{}); true {
		if r, err := a.beginPeerReset(dumpV2Opts{}, 1, idLog()); r != nil || err != nil {
			t.Fatal("a dump not asked to reset began one")
		}
		if r, err := a.beginPeerReset(dumpV2Opts{ResetPeersOutside: []string{"10.0.0.1"}, LeaveRunning: true}, 1, idLog()); r != nil || err != nil {
			t.Fatal("a leave-running dump began a reset")
		}
		// Flag off: a direct request asking for the reset is not obeyed.
		if r, err := a.beginPeerReset(dumpV2Opts{ResetPeersOutside: []string{"10.0.0.1"}}, os.Getpid(), idLog()); r != nil || err != nil {
			t.Fatal("a reset began with identity restore off")
		}
	}
}

func TestEndPeerReset(t *testing.T) {
	saved := runNetUnlock
	t.Cleanup(func() { runNetUnlock = saved })
	unlocks := 0
	runNetUnlock = func(context.Context, netns.NsHandle, string, string) error {
		unlocks++
		return errors.New("criu net-unlock: no such command")
	}
	a := &Agent{}
	ctx := context.Background()
	dumpErr := errors.New("dump failed")
	if err := a.endPeerReset(ctx, nil, false, false, "/x"); err != nil {
		t.Fatal("no reset reported a failure")
	}
	conns := []tcpTuple{{Local: netip.MustParseAddrPort("10.0.0.1:1"), Remote: netip.MustParseAddrPort("10.0.0.9:2379")}}
	r := &sourcePeerReset{ns: netns.None(), conns: conns, log: idLog()}
	if err := a.endPeerReset(ctx, r, false, false, "/x"); err != nil || unlocks != 0 {
		t.Fatal("a dump CRIU did not complete was unlocked")
	}
	r = &sourcePeerReset{ns: netns.None(), conns: conns, log: idLog()}
	if err := a.endPeerReset(ctx, r, true, true, "/x"); err != nil || unlocks != 0 {
		t.Fatal("a leave-running dump was unlocked")
	}
	// Fail closed: the source's sessions cannot be reset, so the capture
	// fails.
	before := decisions(t, identityPeersResetFailed)
	r = &sourcePeerReset{ns: netns.None(), conns: conns, log: idLog()}
	if err := a.endPeerReset(ctx, r, true, false, "/x"); err == nil || unlocks != 1 {
		t.Fatalf("a failed unlock passed: %v", err)
	}
	if decisions(t, identityPeersResetFailed) != before+1 {
		t.Error("failure not counted")
	}
	// The failure reaches the capture as one whose source must be
	// replaced, never a dump to roll back; a capture that failed anyway
	// keeps its own error.
	o := &peerResetOutcome{}
	o.set(errors.New("unlock failed"))
	if err := o.fail(nil); !errors.Is(err, errSourceLeftSuspended) || !sourceMustBeReplaced(err) {
		t.Fatalf("reset failure: %v", err)
	}
	// With a later failure too (a store that cannot be collected), both
	// are kept, and the source is still replaced.
	if err := o.fail(dumpErr); !errors.Is(err, dumpErr) || !errors.Is(err, errSourceLeftSuspended) || !sourceMustBeReplaced(err) {
		t.Fatalf("combined failure: %v", err)
	}
	var none *peerResetOutcome
	if none.fail(nil) != nil {
		t.Fatal("no outcome failed a capture")
	}
	// Nothing to reset: nothing run.
	r = &sourcePeerReset{ns: netns.None(), log: idLog()}
	if err := a.endPeerReset(ctx, r, true, false, "/x"); err != nil || unlocks != 1 {
		t.Fatal("a source without outside connections was unlocked")
	}
	if args := netUnlockCmd(ctx, "/criu-bundle/criu", "/ck").Args; strings.Join(args, " ") !=
		"nsenter --net=/proc/self/fd/3 -- /criu-bundle/criu net-unlock -D /ck -o net-unlock-source.log" {
		t.Errorf("net-unlock argv %v", args)
	}
}

// needNetAdmin skips a test that needs CAP_NET_ADMIN (TCP repair mode,
// setns). It runs in a privileged container: see the pull request for the
// command.
func needNetAdmin(t *testing.T) {
	t.Helper()
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Skip(err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.SetsockoptInt(fd, unix.SOL_TCP, unix.TCP_REPAIR, 1); err != nil {
		t.Skipf("needs CAP_NET_ADMIN (TCP repair mode): %v", err)
	}
}

// silentlyClosed opens a connection to a listener, then closes the
// client side the way a dump that kills its source does: in repair mode,
// so no FIN and no RST reach the server. It returns the server's side,
// which still holds the connection, and the client's address pair.
func silentlyClosed(t *testing.T) (net.Conn, tcpTuple) {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	client, err := (&net.Dialer{}).DialContext(context.Background(), "tcp4", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	tup := tcpTuple{Local: netip.MustParseAddrPort(client.LocalAddr().String()), Remote: netip.MustParseAddrPort(client.RemoteAddr().String())}
	raw, err := client.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var serr error
	if err := raw.Control(func(fd uintptr) { serr = unix.SetsockoptInt(int(fd), unix.SOL_TCP, unix.TCP_REPAIR, 1) }); err != nil || serr != nil {
		t.Fatalf("repair mode: %v %v", err, serr)
	}
	_ = client.Close()
	return server, tup
}

// readErr reads from c until it fails or within passes.
func readErr(c net.Conn, within time.Duration) error {
	_ = c.SetReadDeadline(time.Now().Add(within))
	_, err := c.Read(make([]byte, 1))
	return err
}

// The mechanism, on a real kernel: a peer whose connection was closed in
// repair mode keeps it (the leftover session), and the probe on the
// connection's own address pair makes the peer drop it at once.
func TestProbeReset_PeerDropsSilentlyClosedConnection(t *testing.T) {
	needNetAdmin(t)
	server, _ := silentlyClosed(t)
	var ne net.Error
	if err := readErr(server, 500*time.Millisecond); !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("without the probe the peer saw %v; want it still holding the connection", err)
	}
	server, tup := silentlyClosed(t)
	if err := probeReset(context.Background(), tup); err != nil {
		t.Fatal(err)
	}
	if err := readErr(server, 2*time.Second); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("after the probe the peer saw %v; want a reset", err)
	}
}

// afterDump end to end in the test's own network namespace: the dump's
// lock is removed (stubbed here: no CRIU), then every listed connection
// is reset at its peer.
func TestSourcePeerReset_AfterDump(t *testing.T) {
	needNetAdmin(t)
	h, err := netns.Get()
	if err != nil {
		t.Skip(err)
	}
	if err := netns.Set(h); err != nil {
		t.Skipf("setns: %v", err)
	}
	saved := runNetUnlock
	t.Cleanup(func() { runNetUnlock = saved })
	unlocked := false
	runNetUnlock = func(context.Context, netns.NsHandle, string, string) error { unlocked = true; return nil }
	s1, t1 := silentlyClosed(t)
	s2, t2 := silentlyClosed(t)
	r := &sourcePeerReset{ns: h, conns: []tcpTuple{t1, t2}, log: idLog()}
	if err := (&Agent{}).endPeerReset(context.Background(), r, true, false, os.TempDir()); err != nil {
		t.Fatal(err)
	}
	if !unlocked {
		t.Error("the dump's lock was not removed first")
	}
	for i, s := range []net.Conn{s1, s2} {
		if err := readErr(s, 2*time.Second); !errors.Is(err, syscall.ECONNRESET) {
			t.Errorf("peer %d saw %v; want a reset", i, err)
		}
	}
}

// silentListener is a loopback address whose listener drops every new
// SYN, without privileges: its accept queue (backlog 0) is full with one
// connection never accepted, and Linux drops a SYN to a full queue.
func silentListener(t *testing.T) netip.AddrPort {
	t.Helper()
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	if err := unix.Bind(fd, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if err := unix.Listen(fd, 0); err != nil {
		t.Fatal(err)
	}
	sa, err := unix.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	addr := netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), uint16(sa.(*unix.SockaddrInet4).Port)) //nolint:gosec // a port fits
	filler, err := net.DialTimeout("tcp4", addr.String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = filler.Close() })
	return addr
}

// A probe nobody answers (a peer that drops the SYN) is not counted as a
// reset: the capture would otherwise leave a session open.
func TestProbeReset_UnansweredIsNotConfirmed(t *testing.T) {
	saved := peerProbeWait
	peerProbeWait = 300 * time.Millisecond
	t.Cleanup(func() { peerProbeWait = saved })
	err := probeReset(context.Background(), tcpTuple{Local: netip.MustParseAddrPort("127.0.0.1:0"), Remote: silentListener(t)})
	if !errors.Is(err, errProbeUnconfirmed) {
		t.Fatalf("an unanswered probe: %v; want it unconfirmed", err)
	}
}

// A probe stops when the capture's time is up, and does not report
// success then.
func TestProbeReset_RespectsContext(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = probeReset(ctx, tcpTuple{Local: netip.MustParseAddrPort("127.0.0.1:0"), Remote: netip.MustParseAddrPort(ln.Addr().String())})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("probe with its time up: %v", err)
	}
	// A deadline shorter than the probe's own wait bounds it: the probe
	// waits on a peer that never answers until the deadline, not its own
	// wait, and ends unconfirmed or with the deadline.
	saved := peerProbeWait
	peerProbeWait = 5 * time.Second
	t.Cleanup(func() { peerProbeWait = saved })
	silent := silentListener(t) // set up before the deadline starts
	dctx, dcancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer dcancel()
	t0 := time.Now()
	err = probeReset(dctx, tcpTuple{Local: netip.MustParseAddrPort("127.0.0.1:0"), Remote: silent})
	took := time.Since(t0)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, errProbeUnconfirmed) {
		t.Fatalf("probe past its deadline: %v", err)
	}
	if took < 250*time.Millisecond || took > 2*time.Second {
		t.Fatalf("probe took %s; want it to wait until its 300ms deadline", took)
	}
}

// The inventory fails when a process of the container cannot be inspected:
// a connection it missed would go unreset while the capture reports
// success.
func TestSourcePeerReset_InventoryFailsOnAnUninspectableProcess(t *testing.T) {
	proc := t.TempDir()
	mk := func(pid string) string {
		d := proc + "/" + pid
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(d+"/cgroup", []byte("0::/kubepods/pod1/main\n"), 0o644); err != nil { //nolint:gosec // a test file
			t.Fatal(err)
		}
		return d
	}
	if err := os.MkdirAll(mk("100")+"/fd", 0o755); err != nil {
		t.Fatal(err)
	}
	// Its fd directory cannot be read (not a directory here).
	if err := os.WriteFile(mk("101")+"/fd", nil, 0o644); err != nil { //nolint:gosec // a test file
		t.Fatal(err)
	}
	r := &sourcePeerReset{log: idLog()}
	if err := r.inventoryHook(proc, 100)(); err == nil || !strings.Contains(err.Error(), "list the container's sockets") {
		t.Fatalf("inventory with a process it cannot inspect: %v; want it to fail", err)
	}
	// One that went away is skipped.
	if err := os.RemoveAll(proc + "/101"); err != nil {
		t.Fatal(err)
	}
	if _, err := containerSocketInodesStrict(proc, 100); err != nil {
		t.Fatalf("a process gone: %v", err)
	}
}

// The reset the kernel sends in answer to the peer's challenge ACK may be
// lost: the probe then sees the challenge ACK but never the peer accept
// its retransmitted SYN, and must not report success. Needs iptables in
// the test's network namespace (NVSNAP_TEST_IPTABLES=1).
func TestProbeReset_LostResetIsNotConfirmed(t *testing.T) {
	needNetAdmin(t)
	if os.Getenv("NVSNAP_TEST_IPTABLES") == "" {
		t.Skip("NVSNAP_TEST_IPTABLES not set")
	}
	server, tup := silentlyClosed(t)
	rule := []string{"OUTPUT", "-p", "tcp", "--tcp-flags", "RST", "RST", "--sport", strconv.Itoa(int(tup.Local.Port())), "-j", "DROP"}
	add := exec.CommandContext(context.Background(), "iptables", append([]string{"-A"}, rule...)...) //nolint:gosec // a test rule it builds
	if out, err := add.CombinedOutput(); err != nil {
		t.Skipf("iptables: %v %s", err, out)
	}
	del := exec.CommandContext(context.Background(), "iptables", append([]string{"-D"}, rule...)...) //nolint:gosec // a test rule it builds
	t.Cleanup(func() { _ = del.Run() })
	saved := peerProbeWait
	peerProbeWait = 1500 * time.Millisecond
	t.Cleanup(func() { peerProbeWait = saved })
	if err := probeReset(context.Background(), tup); !errors.Is(err, errProbeUnconfirmed) {
		t.Fatalf("a probe whose reset was lost passed: %v", err)
	}
	var ne net.Error
	if err := readErr(server, 300*time.Millisecond); !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("the peer saw %v; want it still holding the connection", err)
	}
}

// runNetUnlock runs criu with a duplicate of the namespace handle: the
// handle the probes use stays open and valid after it, garbage collection
// included. Needs a container with nsenter (CAP_SYS_ADMIN).
func TestRunNetUnlock_KeepsTheNamespaceHandle(t *testing.T) {
	needNetAdmin(t)
	if _, err := exec.LookPath("nsenter"); err != nil {
		t.Skip("no nsenter")
	}
	h, err := netns.Get()
	if err != nil {
		t.Skip(err)
	}
	defer func() { _ = h.Close() }()
	dir := t.TempDir()
	criu := dir + "/criu"
	if err := os.WriteFile(criu, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil { //nolint:gosec // a test script
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := runNetUnlock(context.Background(), h, criu, dir); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		runtime.GC()
	}
	if err := inNetNSHandle(h, func() error { return nil }); err != nil {
		t.Fatalf("the namespace handle is no longer usable: %v", err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(h), &st); err != nil {
		t.Fatalf("the namespace handle was closed: %v", err)
	}
}

// The reset of a dumped source's connections ends within its budget,
// however many peers never answer, and fails the capture then: every
// connection must be confirmed. Probes of one local port never overlap.
func TestSourcePeerReset_BoundedAndFailsUnconfirmed(t *testing.T) {
	savedUnlock, savedProbe, savedBudget := runNetUnlock, probeInNS, peerResetBudget
	t.Cleanup(func() { runNetUnlock, probeInNS, peerResetBudget = savedUnlock, savedProbe, savedBudget })
	runNetUnlock = func(context.Context, netns.NsHandle, string, string) error { return nil }
	peerResetBudget = 300 * time.Millisecond
	var mu sync.Mutex
	inFlight := map[netip.AddrPort]bool{}
	overlap := false
	probeInNS = func(ctx context.Context, _ netns.NsHandle, c tcpTuple) error {
		mu.Lock()
		overlap = overlap || inFlight[c.Local]
		inFlight[c.Local] = true
		mu.Unlock()
		defer func() { mu.Lock(); delete(inFlight, c.Local); mu.Unlock() }()
		if c.Remote.Port() == 1 {
			return nil // confirmed
		}
		<-ctx.Done() // a peer that never answers
		return errProbeUnconfirmed
	}
	var conns []tcpTuple
	for i := range 40 {
		conns = append(conns, tcpTuple{Local: netip.MustParseAddrPort("10.0.0.1:9090"), Remote: netip.AddrPortFrom(netip.MustParseAddr("10.0.0.7"), uint16(i+1))})       //nolint:gosec // small
		conns = append(conns, tcpTuple{Local: netip.AddrPortFrom(netip.MustParseAddr("10.0.0.1"), uint16(40000+i)), Remote: netip.MustParseAddrPort("10.96.0.10:2379")}) //nolint:gosec // small
	}
	r := &sourcePeerReset{ns: netns.None(), conns: conns, log: idLog()}
	t0 := time.Now()
	err := r.afterDump(context.Background(), "criu", "/x")
	if err == nil || !errors.Is(err, errProbeUnconfirmed) {
		t.Fatalf("unconfirmed peers passed: %v", err)
	}
	if took := time.Since(t0); took > 2*time.Second {
		t.Fatalf("the reset took %s; want it within its budget", took)
	}
	if overlap {
		t.Error("two probes of one local port overlapped")
	}
}
