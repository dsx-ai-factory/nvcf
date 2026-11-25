// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
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
	}
	got := outsidePeers(sockets, own, []string{"10.0.0.1", "10.0.0.2"})
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
		if r, err := a.beginPeerReset(dumpV2Opts{}, "/proc", 1, idLog()); r != nil || err != nil {
			t.Fatal("a dump not asked to reset began one")
		}
		if r, err := a.beginPeerReset(dumpV2Opts{ResetPeersOutside: []string{"10.0.0.1"}, LeaveRunning: true}, "/proc", 1, idLog()); r != nil || err != nil {
			t.Fatal("a leave-running dump began a reset")
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
	if err := a.endPeerReset(ctx, nil, dumpErr, false, "/x"); !errors.Is(err, dumpErr) {
		t.Fatal("no reset changed the dump's outcome")
	}
	conns := []tcpTuple{{Local: netip.MustParseAddrPort("10.0.0.1:1"), Remote: netip.MustParseAddrPort("10.0.0.9:2379")}}
	r := &sourcePeerReset{ns: netns.None(), conns: conns, log: idLog()}
	if err := a.endPeerReset(ctx, r, dumpErr, false, "/x"); !errors.Is(err, dumpErr) || unlocks != 0 {
		t.Fatal("a failed dump was unlocked")
	}
	r = &sourcePeerReset{ns: netns.None(), conns: conns, log: idLog()}
	if err := a.endPeerReset(ctx, r, nil, true, "/x"); err != nil || unlocks != 0 {
		t.Fatal("a leave-running dump was unlocked")
	}
	// Fail closed: the source's sessions cannot be reset, so the capture
	// fails.
	before := decisions(t, identityPeersResetFailed)
	r = &sourcePeerReset{ns: netns.None(), conns: conns, log: idLog()}
	if err := a.endPeerReset(ctx, r, nil, false, "/x"); err == nil || unlocks != 1 {
		t.Fatalf("a failed unlock passed: %v", err)
	}
	if decisions(t, identityPeersResetFailed) != before+1 {
		t.Error("failure not counted")
	}
	// Nothing to reset: nothing run.
	r = &sourcePeerReset{ns: netns.None(), log: idLog()}
	if err := a.endPeerReset(ctx, r, nil, false, "/x"); err != nil || unlocks != 1 {
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
	if err := probeReset(tup); err != nil {
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
	if err := (&Agent{}).endPeerReset(context.Background(), r, nil, false, os.TempDir()); err != nil {
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
