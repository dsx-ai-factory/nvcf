// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Resetting a dumped source's connections to peers outside the captured
// group (identity restore, docs/design/IDENTITY-RESTORE.md).
//
// CRIU dumps established TCP connections in repair mode and, on a dump
// that kills the tree, closes them silently: no FIN, no RST. It also
// leaves its network lock in the pod's network namespace (an nftables
// table that drops every packet of the dumped connections), so the
// kernel cannot answer a peer's next packet with a reset either. A peer
// outside the captured group (a shared NATS, etcd, a frontend) keeps the
// dead source's connection until its own keepalive gives up: minutes, in
// which a shared NATS hands the source's subscription half the requests
// meant for the restored worker.
//
// After such a dump, while the pod's network namespace still exists:
//
//  1. `criu net-unlock -D <images>` in that namespace removes the dump's
//     lock (its table is named by the dump's run id in the images).
//  2. For each connection the source had to a peer outside the group, a
//     socket bound to the connection's own local address and port
//     connects to the peer. The peer, which still holds the connection,
//     answers the SYN with an ACK of its own sequence (RFC 5961 challenge
//     ACK); the probe, in SYN-SENT, answers that ACK with a reset whose
//     sequence number is the ACK's, exactly what the peer expects
//     (RFC 793), and the peer drops the connection at once. The probe is
//     then closed.
//
// This needs no sequence numbers from the images, no raw sockets and no
// change to the dump: only the agent's privileges in the pod's network
// namespace (CAP_NET_ADMIN for the unlock, a plain bind and connect for
// the probe). A connection the peer already closed answers the SYN with a
// SYN-ACK; the probe then closes it, which the peer sees as a client that
// left.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
)

// peerProbeWait bounds each probe's connect: the peer's challenge ACK and
// the reset that answers it come within a round trip; a SYN retransmit
// (after a second) is not wanted.
var peerProbeWait = 500 * time.Millisecond

// peerResetKey carries, in a capture's context, the addresses of the
// captured group's members: the connections between them are kept.
type peerResetKey struct{}

// withPeerReset asks the capture of ctx to reset its sources' connections
// to peers outside members (by address) after the dump.
func withPeerReset(ctx context.Context, members []string) context.Context {
	return context.WithValue(ctx, peerResetKey{}, members)
}

// peerResetFrom returns withPeerReset's addresses, or nil.
func peerResetFrom(ctx context.Context) []string {
	v, _ := ctx.Value(peerResetKey{}).([]string)
	return v
}

// memberAddresses returns the pod addresses of members.
func memberAddresses(members []*corev1.Pod) []string {
	out := make([]string, 0, len(members))
	for _, p := range members {
		if p.Status.PodIP != "" {
			out = append(out, p.Status.PodIP)
		}
	}
	return out
}

// tcpTuple is one established connection, from the namespace's side.
type tcpTuple struct {
	Local, Remote netip.AddrPort
}

// outsidePeers selects, among sockets, the established connections of
// the inodes own whose peer is neither loopback nor one of keep.
func outsidePeers(sockets []*netlink.Socket, own func(uint32) bool, keep []string) []tcpTuple {
	kept := map[string]bool{}
	for _, k := range keep {
		if ip := net.ParseIP(k); ip != nil {
			kept[ip.String()] = true
		}
	}
	var out []tcpTuple
	for _, s := range sockets {
		if s.State != tcpStateEstablished || !own(s.INode) || s.ID.Destination.IsLoopback() || s.ID.Source.IsLoopback() || kept[s.ID.Destination.String()] {
			continue
		}
		l, ok1 := netip.AddrFromSlice(s.ID.Source)
		r, ok2 := netip.AddrFromSlice(s.ID.Destination)
		if !ok1 || !ok2 {
			continue
		}
		out = append(out, tcpTuple{
			Local:  netip.AddrPortFrom(l.Unmap(), s.ID.SourcePort),
			Remote: netip.AddrPortFrom(r.Unmap(), s.ID.DestinationPort),
		})
	}
	return out
}

// sourcePeerReset is the reset of one dumped container's outside
// connections: listed before the dump, reset after it.
type sourcePeerReset struct {
	ns    netns.NsHandle
	conns []tcpTuple
	log   *logrus.Entry
}

// beginSourcePeerReset lists, before the dump, the container's (host pid
// hostPID) connections to peers outside keep, and holds its pod's network
// namespace for after the dump.
func beginSourcePeerReset(procBase string, hostPID int, keep []string, log *logrus.Entry) (*sourcePeerReset, error) {
	inodes, err := containerSocketInodes(procBase, hostPID)
	if err != nil {
		return nil, fmt.Errorf("list the container's sockets: %w", err)
	}
	h, err := netns.GetFromPid(hostPID)
	if err != nil {
		return nil, fmt.Errorf("open the pod's network namespace: %w", err)
	}
	r := &sourcePeerReset{ns: h, log: log}
	err = inNetNSHandle(h, func() error {
		r.conns = outsidePeers(allTCP(log), func(i uint32) bool { return inodes[i] }, keep)
		return nil
	})
	if err != nil {
		_ = h.Close()
		return nil, err
	}
	return r, nil
}

// close releases the namespace. Nil-safe.
func (r *sourcePeerReset) close() {
	if r != nil {
		_ = r.ns.Close()
	}
}

// netUnlockCmd builds `criu net-unlock` for a dump's images, run in the
// namespace passed as the command's fd 3.
func netUnlockCmd(ctx context.Context, criu, imagesDir string) *exec.Cmd {
	return exec.CommandContext(ctx, "nsenter", "--net=/proc/self/fd/3", "--", //nolint:gosec // the agent's own CRIU, a path it built
		criu, "net-unlock", "-D", imagesDir, "-o", "net-unlock-source.log")
}

// runNetUnlock is how afterDump removes the dump's lock; tests stub it.
var runNetUnlock = func(ctx context.Context, ns netns.NsHandle, criu, imagesDir string) error {
	f := os.NewFile(uintptr(ns), "netns")
	cctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	cmd := netUnlockCmd(cctx, criu, imagesDir)
	cmd.ExtraFiles = []*os.File{f}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("criu net-unlock: %w (output: %s; log: %s)", err, strings.TrimSpace(string(out)),
			tailOfFile(filepath.Join(imagesDir, "net-unlock-source.log"), 6))
	}
	return nil
}

// afterDump resets the listed connections once the dump has killed the
// source: removes the dump's network lock, then probes each connection.
// Any failure is returned: the capture fails rather than leave a dead
// identity's sessions open beside its restore.
func (r *sourcePeerReset) afterDump(ctx context.Context, criu, imagesDir string) error {
	defer r.close()
	if len(r.conns) == 0 {
		return nil
	}
	if err := runNetUnlock(ctx, r.ns, criu, imagesDir); err != nil {
		return err
	}
	var errs []error
	err := inNetNSHandle(r.ns, func() error {
		for _, c := range r.conns {
			if perr := probeReset(c); perr != nil {
				errs = append(errs, fmt.Errorf("%s -> %s: %w", c.Local, c.Remote, perr))
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("reset the source's connections to peers outside the group: %w", err)
	}
	r.log.WithField("connections", len(r.conns)).Info("identity capture: the dumped source's connections to peers outside the group were reset")
	return nil
}

// probeReset sends, from the calling thread's network namespace, a SYN
// on connection c's own address pair (see the file comment), waits up to
// peerProbeWait for the exchange, and closes the probe.
func probeReset(c tcpTuple) error {
	family := unix.AF_INET
	if c.Local.Addr().Is6() {
		family = unix.AF_INET6
	}
	fd, err := unix.Socket(family, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("socket: %w", err)
	}
	defer func() { _ = unix.Close(fd) }()
	if serr := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); serr != nil {
		return fmt.Errorf("reuseaddr: %w", serr)
	}
	if berr := unix.Bind(fd, sockaddrOf(c.Local)); berr != nil {
		return fmt.Errorf("bind the connection's local address: %w", berr)
	}
	err = unix.Connect(fd, sockaddrOf(c.Remote))
	if err != nil && !errors.Is(err, unix.EINPROGRESS) {
		if errors.Is(err, unix.ECONNREFUSED) || errors.Is(err, unix.ECONNRESET) {
			return nil // the peer answered with a reset: its side is gone
		}
		return fmt.Errorf("connect: %w", err)
	}
	pfd := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}} //nolint:gosec // a file descriptor fits in 32 bits
	_, _ = unix.Poll(pfd, int(peerProbeWait/time.Millisecond))
	return nil
}

func sockaddrOf(ap netip.AddrPort) unix.Sockaddr {
	if ap.Addr().Is4() {
		return &unix.SockaddrInet4{Port: int(ap.Port()), Addr: ap.Addr().As4()}
	}
	return &unix.SockaddrInet6{Port: int(ap.Port()), Addr: ap.Addr().As16()}
}

// inNetNSHandle runs f on a thread in network namespace h. A thread that
// cannot be put back in the agent's namespace is never reused: it stays
// locked and exits with the goroutine.
func inNetNSHandle(h netns.NsHandle, f func() error) error {
	errc := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		orig, err := netns.Get()
		if err != nil {
			runtime.UnlockOSThread()
			errc <- fmt.Errorf("get current netns: %w", err)
			return
		}
		defer func() { _ = orig.Close() }()
		if err := netns.Set(h); err != nil {
			runtime.UnlockOSThread()
			errc <- fmt.Errorf("enter the pod's network namespace: %w", err)
			return
		}
		ferr := f()
		if err := netns.Set(orig); err != nil {
			errc <- errors.Join(ferr, fmt.Errorf("return to the agent's network namespace: %w", err))
			return // the thread stays locked and is discarded
		}
		runtime.UnlockOSThread()
		errc <- ferr
	}()
	return <-errc
}

// podNetnsDialer returns a dialer whose connections are opened in the
// network namespace of pod's container (on this node), and a func that
// releases the namespace. Names are resolved by the agent first.
func (a *Agent) podNetnsDialer(ctx context.Context, pod *corev1.Pod, container string) (dial func(context.Context, string, string) (net.Conn, error), release func(), err error) {
	if a.runtime == nil {
		return nil, nil, errors.New("no container runtime client")
	}
	if container == "" {
		container = gpuContainer(pod)
	}
	info, err := a.runtime.FindContainerByPod(ctx, pod.Namespace, pod.Name, container)
	if err != nil {
		return nil, nil, fmt.Errorf("find the pod's container: %w", err)
	}
	if info == nil || info.PID == 0 {
		return nil, nil, errors.New("the pod's container has no running process")
	}
	h, err := netns.GetFromPid(int(info.PID))
	if err != nil {
		return nil, nil, fmt.Errorf("open the pod's network namespace: %w", err)
	}
	dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, serr := net.SplitHostPort(addr)
		if serr != nil {
			return nil, serr
		}
		ips := []string{host}
		if net.ParseIP(host) == nil {
			addrs, rerr := net.DefaultResolver.LookupHost(ctx, host)
			if rerr != nil {
				return nil, rerr
			}
			ips = addrs
		}
		var errs []error
		for _, ip := range ips {
			var conn net.Conn
			derr := inNetNSHandle(h, func() error {
				var e error
				conn, e = (&net.Dialer{Timeout: etcdCallTimeout}).DialContext(ctx, network, net.JoinHostPort(ip, port))
				return e
			})
			if derr == nil {
				return conn, nil
			}
			errs = append(errs, derr)
		}
		return nil, errors.Join(errs...)
	}
	return dial, func() { _ = h.Close() }, nil
}

// beginPeerReset starts, for a dump asked to (o.ResetPeersOutside), the
// reset of its source's outside connections; nil when not asked.
func (a *Agent) beginPeerReset(o dumpV2Opts, procBase string, hostPID int, log *logrus.Entry) (*sourcePeerReset, error) {
	if len(o.ResetPeersOutside) == 0 || o.LeaveRunning {
		return nil, nil
	}
	r, err := beginSourcePeerReset(procBase, hostPID, o.ResetPeersOutside, log)
	if err != nil {
		observeIdentity(identityPeersResetFailed)
		return nil, fmt.Errorf("identity capture: %w", err)
	}
	return r, nil
}

// endPeerReset finishes r after the dump (dumpErr): a dump that killed
// its source has the source's outside connections reset; any other
// outcome only releases r. A reset that fails fails the capture.
func (a *Agent) endPeerReset(ctx context.Context, r *sourcePeerReset, dumpErr error, leaveRunning bool, imagesDir string) error {
	if r == nil {
		return dumpErr
	}
	if dumpErr != nil || leaveRunning {
		r.close()
		return dumpErr
	}
	if err := r.afterDump(ctx, a.config.CRIUPath, imagesDir); err != nil {
		observeIdentity(identityPeersResetFailed)
		return fmt.Errorf("identity capture: %w", err)
	}
	observeIdentity(identityPeersReset)
	return nil
}
