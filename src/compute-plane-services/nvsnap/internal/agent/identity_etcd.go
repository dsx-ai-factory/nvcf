// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// Identity restore, etcd discovery (docs/design/IDENTITY-RESTORE.md).
// Behind the agent flag --identity-restore (chart value
// agent.identityRestore.enabled), off by default.
//
// A Dynamo worker that registers in an etcd shared with other workers (a
// namespace of several workers behind one frontend, or a registry in
// another namespace) is refused by the group capture today: its registry
// cannot be captured with it, and a restored copy would carry a
// registration that is either a duplicate (its source alive) or gone (its
// lease expired). With the flag on, such a worker is captured alone, its
// companions excluded, and restored as its own captured identity:
//
//   - At capture, before the freeze, the agent reads the worker's lease
//     (Dynamo's instance id), its granted TTL, every key under it, and the
//     pod address, and keeps them in the group record (never in logs).
//   - After the dump, the source's connections to peers outside the
//     captured group are reset (identity_peer_reset.go), so a shared NATS
//     drops the dead source's subscription at once.
//   - At restore, before the restored process runs, the agent refuses (the
//     pod starts cold) when the lease is still held, else grants the lease
//     again with the same ID and TTL, writes the saved keys with the old pod
//     address replaced by the new one, and keeps the lease alive until the
//     process's own keep-alive is seen. etcd refuses a grant of a live ID,
//     so two restores of one identity cannot both hold it.
//
// Kubernetes discovery stays refused. A record with an identity is never
// restored without its regrant: with the flag off, such a pod starts cold.
// With the flag on, a placeholder restores only from the record of its own
// capture (identityBeforeRestore); a record gone, or replaced since, is
// refused, the pod starting cold.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

// Group record keys of an identity capture.
const (
	identityBackendKey   = "identity.backend"   // identityBackendEtcd
	identityLeaseKey     = "identity.lease"     // lease ID, decimal
	identityTTLKey       = "identity.ttl"       // granted TTL, seconds
	identityPodIPKey     = "identity.podIP"     // the lease holder's pod address at capture
	identityMemberKey    = "identity.member"    // slot of the rank that holds the lease
	identityEndpointsKey = "identity.endpoints" // the rank's ETCD_ENDPOINTS
	identityKeysKey      = "identity.keys"      // JSON: every key under the lease, with its value
	// identityNamespaceKey is the namespace the identity was captured in,
	// the only one it restores into (identityBlocksCapture).
	identityNamespaceKey = "identity.namespace"
	// identityHolderKey names the restore that holds the identity
	// ("namespace/pod/uid"): one live restore per identity.
	identityHolderKey = "identity.holder"
	// identityGrantedKey names the holder whose restore had the lease
	// granted: only it may adopt a live lease after an agent restart.
	identityGrantedKey = "identity.granted"
	// identityHandoffKey names the holder whose process was seen keeping
	// the lease: no agent keeps it for that pod any more.
	identityHandoffKey = "identity.handoff"
)

const (
	dynamoEtcdEndpointsEnv = "ETCD_ENDPOINTS"
	// dynamoKeyRoot is the root of every key Dynamo registers in etcd.
	dynamoKeyRoot = "v1/"
	// dynamoInstancesRoot holds a worker's endpoint registrations, named
	// by its instance id, which is its lease ID in hex.
	dynamoInstancesRoot = "v1/instances/"
)

// Timing of the restore side; tests shorten them.
var (
	// identityPoll is how often the keeper reads the lease.
	identityPoll = time.Second
	// identityExpirySlack is how long past the recorded TTL a restore waits
	// for the captured lease to expire before it refuses.
	identityExpirySlack = 5 * time.Second
	// identityHandoffFloor bounds from below the wait, after the process
	// resumed, for its own keep-alive: max(3 TTL, this).
	identityHandoffFloor = 60 * time.Second
	// identityTolerance is how far a lease's TTL may run ahead of what
	// the agent's own last keep-alive explains before it counts as
	// another's keep-alive: etcd reports whole seconds.
	identityTolerance = 2 * time.Second
)

// Identity decisions, the label values of
// metrics.IdentityRestoreDecisions.
const (
	identityCaptured           = "captured"
	identityCaptureRefused     = "capture_refused"
	identityPeersReset         = "peers_reset"
	identityPeersResetFailed   = "peers_reset_failed"
	identityRefusedLiveLease   = "refused_live_lease"
	identityRefusedHeld        = "refused_held"
	identityRefusedError       = "refused_error"
	identityRefusedDisabled    = "refused_disabled"
	identityRegranted          = "regranted"
	identityHandoffSeen        = "keepalive_handoff"
	identityHandoffTimeout     = "keepalive_handoff_timeout"
	identityHandoffLost        = "keepalive_lease_lost"
	identityRestoreAborted     = "restore_aborted"
	identityKeeperAgentStopped = "keeper_agent_stopped"
	identityKeysFailed         = "keys_write_failed"
)

func observeIdentity(decision string) {
	metrics.IdentityRestoreDecisions.WithLabelValues(decision).Inc()
}

// identityKey is one key a captured worker held under its lease.
type identityKey struct {
	Key   []byte `json:"k"`
	Value []byte `json:"v"`
}

// capturedIdentity is a worker's etcd identity as a capture saved it.
type capturedIdentity struct {
	Lease     int64
	TTL       int64
	PodIP     string
	Member    int
	Endpoints string
	Keys      []identityKey
}

// write stores the identity in group record data d.
func (i *capturedIdentity) write(d map[string]string) error {
	b, err := json.Marshal(i.Keys)
	if err != nil {
		return err
	}
	d[identityBackendKey] = identityBackendEtcd
	d[identityLeaseKey] = strconv.FormatInt(i.Lease, 10)
	d[identityTTLKey] = strconv.FormatInt(i.TTL, 10)
	d[identityPodIPKey] = i.PodIP
	d[identityMemberKey] = strconv.Itoa(i.Member)
	d[identityEndpointsKey] = i.Endpoints
	d[identityKeysKey] = string(b)
	delete(d, identityHolderKey)
	delete(d, identityGrantedKey)
	delete(d, identityHandoffKey)
	return nil
}

// hasIdentity reports group record data that holds an identity, whole or
// not.
func hasIdentity(d map[string]string) bool {
	for k := range d {
		if strings.HasPrefix(k, "identity.") && k != identityHolderKey && k != identityGrantedKey && k != identityHandoffKey {
			return true
		}
	}
	return false
}

// identityFromRecord reads the identity of group record data d; nil
// without one. A partial or malformed identity is an error: the record is
// not restored without it.
func identityFromRecord(d map[string]string) (*capturedIdentity, error) {
	if !hasIdentity(d) {
		return nil, nil
	}
	if b := d[identityBackendKey]; b != identityBackendEtcd {
		return nil, fmt.Errorf("identity record: backend %q is not supported", b)
	}
	var i capturedIdentity
	var err error
	if i.Lease, err = strconv.ParseInt(d[identityLeaseKey], 10, 64); err != nil || i.Lease == 0 {
		return nil, fmt.Errorf("identity record: bad lease %q", d[identityLeaseKey])
	}
	if i.TTL, err = strconv.ParseInt(d[identityTTLKey], 10, 64); err != nil || i.TTL <= 0 {
		return nil, fmt.Errorf("identity record: bad TTL %q", d[identityTTLKey])
	}
	if i.Member, err = strconv.Atoi(d[identityMemberKey]); err != nil || i.Member < 0 {
		return nil, fmt.Errorf("identity record: bad member %q", d[identityMemberKey])
	}
	i.PodIP, i.Endpoints = d[identityPodIPKey], d[identityEndpointsKey]
	if net.ParseIP(i.PodIP) == nil || i.Endpoints == "" {
		return nil, errors.New("identity record: no pod address or etcd endpoint")
	}
	if err := json.Unmarshal([]byte(d[identityKeysKey]), &i.Keys); err != nil {
		return nil, fmt.Errorf("identity record: keys: %w", err)
	}
	if len(i.Keys) == 0 {
		return nil, errors.New("identity record: no keys")
	}
	return &i, nil
}

// identityEnabled reports the flag.
func (a *Agent) identityEnabled() bool { return a.config.IdentityRestore }

// dynamoEtcdEndpoints returns the literal ETCD_ENDPOINTS of the pod's
// container that names a Dynamo namespace, or "".
func dynamoEtcdEndpoints(pod *corev1.Pod) string {
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		named := false
		ep := ""
		for _, e := range c.Env {
			switch {
			case e.Name == "DYN_NAMESPACE" && e.ValueFrom == nil && e.Value != "":
				named = true
			case e.Name == dynamoEtcdEndpointsEnv && e.ValueFrom == nil:
				ep = e.Value
			}
		}
		if named {
			return ep
		}
	}
	return ""
}

// Identity backends: how a worker's registration is made valid again at
// restore. Only etcd is built; Kubernetes discovery (DynamoWorkerMetadata)
// is refused until its backend lands.
const (
	identityBackendEtcd = "etcd"
)

// keepsDynamoIdentity reports a pod the identity restore keeps as itself:
// the flag on, and a worker it can take (identityRestoreCandidate). Its
// Dynamo namespace is left as the pod sets it (webhook
// dynamoNamespacePatches).
func (a *Agent) keepsDynamoIdentity(pod *corev1.Pod) bool {
	if !a.identityEnabled() {
		return false
	}
	b, _ := identityRestoreCandidate(pod)
	return b != ""
}

// identityRestoreCandidate decides whether rank pod may be captured alone
// with its registry identity, its companions excluded, and with which
// backend; why says why not. This is the one decision the backends share:
// capture alone in a shared namespace, keep the frontend and other workers
// out of the companions, reset the source's outside connections after the
// dump. What each backend saves and re-creates is its own.
func identityRestoreCandidate(pod *corev1.Pod) (backend, why string) {
	if !webhook.DynamoComponent(pod) {
		return "", "not a Dynamo component"
	}
	if pod.Spec.HostNetwork {
		return "", "host network: its address is the node's, shared with other workers"
	}
	switch d := webhook.DynamoDiscovery(pod); d {
	case "etcd", "":
		// Unset: Dynamo registers in etcd when it names one; the capture
		// then proves it by finding the registration.
		ep := dynamoEtcdEndpoints(pod)
		if ep == "" {
			return "", "no literal " + dynamoEtcdEndpointsEnv
		}
		if _, err := etcdGatewayBases(ep, pod.Namespace); err != nil {
			return "", err.Error()
		}
		return identityBackendEtcd, ""
	default:
		return "", fmt.Sprintf("discovery backend %q has no identity restore yet", d)
	}
}

// identityRanksCandidate is identityRestoreCandidate for an instance: its
// Dynamo ranks must all pass, with one backend and one registry.
func identityRanksCandidate(ranks []*corev1.Pod) (backend, why string) {
	ep := ""
	for _, r := range ranks {
		if !webhook.DynamoComponent(r) {
			continue
		}
		b, why := identityRestoreCandidate(r)
		if b == "" {
			return "", r.Name + ": " + why
		}
		if backend != "" && b != backend {
			return "", "the ranks register with different backends"
		}
		backend = b
		if e := dynamoEtcdEndpoints(r); ep == "" {
			ep = e
		} else if e != ep {
			return "", "the ranks name different registries"
		}
	}
	if backend == "" {
		return "", "no Dynamo rank"
	}
	return backend, ""
}

// newIdentityEtcd opens the registry a worker reaches at endpoints, from
// the network namespace of via's container on this node, so the calls
// take the network path (and policy) the worker's own do. close releases
// the namespace. Tests replace it.
var newIdentityEtcd = func(ctx context.Context, a *Agent, endpoints, ns string, via *corev1.Pod, container string) (identityEtcd, func(), error) {
	bases, err := etcdGatewayBases(endpoints, ns)
	if err != nil {
		return nil, nil, err
	}
	dial, closeNS, err := a.podNetnsDialer(ctx, via, container)
	if err != nil {
		return nil, nil, err
	}
	tr := &http.Transport{DialContext: dial, MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second}
	return &etcdGateway{bases: bases, client: &http.Client{Transport: tr}}, func() { tr.CloseIdleConnections(); closeNS() }, nil
}

// ipToken matches ip as a whole address inside a value: not part of a
// longer address.
func ipToken(ip string) *regexp.Regexp {
	q := regexp.QuoteMeta(ip)
	if strings.Contains(ip, ":") {
		return regexp.MustCompile(`(^|[^0-9A-Fa-f:])` + q + `($|[^0-9A-Fa-f:])`)
	}
	return regexp.MustCompile(`(^|[^0-9.])` + q + `($|[^0-9.]|\.[^0-9]|\.$)`)
}

// requestPlaneTCP opens the address of an endpoint registration on
// Dynamo's TCP request plane, its default: the value of an instance
// registration is {"transport":{"tcp":"<ip>:<port>/<path>"}}, the address
// a SocketAddr ("[ip]" for IPv6), serialized by serde_json without spaces.
const requestPlaneTCP = `"tcp":"`

// hostForms are the forms in which a value names ip as a host, the only
// ones a restore rewrites: a URL host ("//ip:", "//ip/"; a model card's
// URL, a ZMQ event channel's "tcp://ip:port"), and the address of a TCP
// request-plane registration (`"tcp":"ip:`). An address in any other form
// (a bare "ip" or "ip:port" string) is not rewritten.
func hostForms(ip string) []string {
	h := bracketed(ip)
	return []string{"//" + h + ":", "//" + h + "/", requestPlaneForm(ip)}
}

// requestPlaneForm is the form in which a TCP request-plane registration
// names ip: `"tcp":"ip:` (`"tcp":"[ip]:` for IPv6).
func requestPlaneForm(ip string) string { return requestPlaneTCP + bracketed(ip) + ":" }

// bracketed is ip as the host of an address: in brackets for IPv6.
func bracketed(ip string) string {
	if strings.Contains(ip, ":") {
		return "[" + ip + "]"
	}
	return ip
}

// rewriteHost replaces every host form of old in v with new.
func rewriteHost(v []byte, old, nw string) []byte {
	s := string(v)
	for i, f := range hostForms(old) {
		s = strings.ReplaceAll(s, f, hostForms(nw)[i])
	}
	return []byte(s)
}

// strayAddress reports whether v names any of ips other than in a host
// form of the holder's, which a restore could not rewrite.
func strayAddress(v []byte, holder string, ips []string) bool {
	s := string(v)
	if holder != "" {
		for i, f := range hostForms(holder) {
			s = strings.ReplaceAll(s, f, hostForms("")[i])
		}
	}
	for _, ip := range ips {
		if ip != "" && ipToken(ip).MatchString(s) {
			return true
		}
	}
	return false
}

// mentions reports whether v names ip in a host form.
func mentions(v []byte, ip string) bool {
	for _, f := range hostForms(ip) {
		if strings.Contains(string(v), f) {
			return true
		}
	}
	return false
}

// requestPlaneAddrs returns the TCP request-plane addresses ("ip:port")
// at which v registers an endpoint at ip.
func requestPlaneAddrs(v []byte, ip string) []string {
	f := requestPlaneForm(ip)
	var out []string
	for s := string(v); ; {
		i := strings.Index(s, f)
		if i < 0 {
			return out
		}
		s = s[i+len(f):]
		n := 0
		for n < len(s) && s[n] >= '0' && s[n] <= '9' {
			n++
		}
		if n > 0 {
			out = append(out, net.JoinHostPort(ip, s[:n]))
		}
	}
}

// Reaching a restored worker's TCP request plane; tests replace them.
var (
	// requestPlaneWait bounds the wait for a restored worker's request
	// plane to accept a connection at its new address; a quarter of the
	// lease's TTL bounds it too, since the keeper does not renew the
	// lease meanwhile.
	requestPlaneWait = 5 * time.Second
	// dialRequestPlane connects to addr from the agent's network (the
	// node's by default: a pod on the host network reaches every pod)
	// and closes the connection.
	dialRequestPlane = func(ctx context.Context, addr string) error {
		c, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", addr)
		if err != nil {
			return err
		}
		return c.Close()
	}
)

// awaitRequestPlane waits for every address of addrs to accept a
// connection, at most min(requestPlaneWait, ttl/4) in all. A restored
// worker registered at an address that does not reach it takes requests
// it never answers: a listener restored on the captured address with
// nothing that leads the new one to it (restore_address.go
// restoreSourceAddress: the loopback alias alone).
func awaitRequestPlane(ctx context.Context, addrs []string, ttl int64) error {
	deadline := time.Now().Add(min(requestPlaneWait, time.Duration(ttl)*time.Second/4))
	for _, addr := range addrs {
		for {
			err := dialRequestPlane(ctx, addr)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if time.Now().After(deadline) {
				// The dial's error names the address: not logged.
				why := "no answer"
				if errors.Is(err, syscall.ECONNREFUSED) {
					why = "refused"
				}
				return fmt.Errorf("the restored worker's TCP request plane does not accept connections at its new address (%s)", why)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return nil
}

// identityLeaseOf finds, among the Dynamo keys kvs, the one lease that a
// worker at one of ips holds: a lease with an instance registration named
// by its own ID and a key whose value names one of ips as a URL host. It
// returns the lease and the index in ips of the address it names. None, or
// several, is an error: the worker's identity cannot be told.
func identityLeaseOf(kvs []etcdKV, ips []string) (lease int64, member int, err error) {
	registered := map[int64]bool{}
	for _, kv := range kvs {
		if kv.Lease == 0 || !strings.HasPrefix(string(kv.Key), dynamoInstancesRoot) {
			continue
		}
		parts := strings.Split(string(kv.Key), "/")
		if id, perr := strconv.ParseUint(parts[len(parts)-1], 16, 64); perr == nil && strconv.FormatUint(id, 16) == strconv.FormatUint(uint64(kv.Lease), 16) { //nolint:gosec // a lease ID's bits, compared as etcd names it in hex
			registered[kv.Lease] = true
		}
	}
	found := map[int64]int{}
	for _, kv := range kvs {
		if !registered[kv.Lease] {
			continue
		}
		for i, ip := range ips {
			if ip != "" && mentions(kv.Value, ip) {
				if prev, ok := found[kv.Lease]; ok && prev != i {
					return 0, 0, fmt.Errorf("lease %x names two ranks' addresses", kv.Lease)
				}
				found[kv.Lease] = i
			}
		}
	}
	switch len(found) {
	case 0:
		return 0, 0, errors.New("no Dynamo registration in etcd names the worker's address")
	case 1:
		for l, i := range found {
			return l, i, nil
		}
	}
	return 0, 0, fmt.Errorf("%d Dynamo registrations name the worker's addresses; its identity cannot be told", len(found))
}

// readIdentity reads, from registry e, the identity of the worker whose
// ranks have addresses ips (by slot).
func readIdentity(ctx context.Context, e identityEtcd, ips []string, endpoints string) (*capturedIdentity, error) {
	kvs, err := e.RangePrefix(ctx, []byte(dynamoKeyRoot))
	if err != nil {
		return nil, fmt.Errorf("read the registry: %w", err)
	}
	lease, member, err := identityLeaseOf(kvs, ips)
	if err != nil {
		return nil, err
	}
	info, err := e.TimeToLive(ctx, lease, true)
	if err != nil {
		return nil, fmt.Errorf("read the worker's lease: %w", err)
	}
	if info.TTL <= 0 || info.GrantedTTL <= 0 || len(info.Keys) == 0 {
		return nil, errors.New("the worker's lease is not live")
	}
	byKey := map[string]etcdKV{}
	for _, kv := range kvs {
		byKey[string(kv.Key)] = kv
	}
	id := &capturedIdentity{Lease: lease, TTL: info.GrantedTTL, PodIP: ips[member], Member: member, Endpoints: endpoints}
	for _, k := range info.Keys {
		kv, ok := byKey[string(k)]
		if !ok {
			got, gerr := e.Get(ctx, k)
			if gerr != nil {
				return nil, fmt.Errorf("read a key of the worker's lease: %w", gerr)
			}
			if got == nil {
				return nil, errors.New("a key of the worker's lease went away while it was read")
			}
			kv = *got
		}
		if kv.Lease != lease {
			return nil, errors.New("a key of the worker's lease moved to another lease while it was read")
		}
		if strayAddress(kv.Key, "", ips) || strayAddress(kv.Value, ips[member], ips) {
			// A form the restore would not rewrite: restored, it would
			// point at the captured pod.
			return nil, errors.New("a key of the worker's lease names a rank's address in a form a restore cannot rewrite")
		}
		id.Keys = append(id.Keys, identityKey{Key: kv.Key, Value: kv.Value})
	}
	if err := identityStillHeld(ctx, e, id, ips[member]); err != nil {
		return nil, err
	}
	return id, nil
}

// identityStillHeld reads the lease's whole key set again: the same keys,
// each with the same value under the same lease, among them an instance
// registration named by the lease and a key naming the holder's address.
// Anything else is a registration that changed while it was read.
func identityStillHeld(ctx context.Context, e identityEtcd, id *capturedIdentity, holderIP string) error {
	changed := errors.New("the worker's registration changed while it was read")
	info, err := e.TimeToLive(ctx, id.Lease, true)
	if err != nil {
		return fmt.Errorf("read the worker's lease: %w", err)
	}
	if info.TTL <= 0 || len(info.Keys) != len(id.Keys) {
		return changed
	}
	saved := map[string][]byte{}
	for _, k := range id.Keys {
		saved[string(k.Key)] = k.Value
	}
	registered, named := false, false
	hex := strconv.FormatUint(uint64(id.Lease), 16) //nolint:gosec // a lease ID's bits, as etcd names it in hex
	for _, k := range info.Keys {
		v, ok := saved[string(k)]
		if !ok {
			return changed
		}
		cur, gerr := e.Get(ctx, k)
		if gerr != nil {
			return fmt.Errorf("read a key of the worker's lease: %w", gerr)
		}
		if cur == nil || cur.Lease != id.Lease || !bytes.Equal(cur.Value, v) {
			return changed
		}
		registered = registered || (strings.HasPrefix(string(k), dynamoInstancesRoot) && strings.HasSuffix(string(k), "/"+hex))
		named = named || mentions(v, holderIP)
	}
	if !registered || !named {
		return errors.New("the worker's saved keys no longer hold its instance registration and address")
	}
	return nil
}

// captureIdentity reads the identity of leader's instance (ranks) for its
// capture. why says why it cannot be captured with one.
func (c *criuAutoRestorer) captureIdentity(ctx context.Context, leader *corev1.Pod, ranks []*corev1.Pod) (id *capturedIdentity, why string) {
	backend, why := identityRanksCandidate(ranks)
	if backend == "" {
		return nil, why
	}
	if backend != identityBackendEtcd {
		return nil, "no identity backend " + backend
	}
	ips := make([]string, len(ranks))
	endpoints := ""
	for i, r := range ranks {
		ips[i] = r.Status.PodIP
		if net.ParseIP(ips[i]) == nil {
			return nil, r.Name + " has no pod address"
		}
		if endpoints == "" {
			endpoints = dynamoEtcdEndpoints(r)
		}
	}
	e, closeFn, err := newIdentityEtcd(ctx, c.a, endpoints, leader.Namespace, leader, gpuContainer(leader))
	if err != nil {
		return nil, "cannot reach the worker's etcd: " + err.Error()
	}
	defer closeFn()
	id, err = readIdentity(ctx, e, ips, endpoints)
	if err != nil {
		id, err = readIdentity(ctx, e, ips, endpoints) // once more: a registration may have been changing
	}
	if err != nil {
		return nil, err.Error()
	}
	if !webhook.DynamoComponent(ranks[id.Member]) {
		return nil, "the lease is held by a rank that is not a Dynamo component"
	}
	// Checked before the freeze: the record of a capture that killed its
	// source must not fail to hold its identity.
	if werr := id.write(map[string]string{}); werr != nil {
		return nil, "cannot record the identity: " + werr.Error()
	}
	return id, ""
}

// identityCheckKey carries, in a capture's context, the identity its
// holder's dump checks under the freeze.
type identityCheckKey struct{}

// withIdentityCheck asks the capture of ctx to check id again once the
// rank that holds it is frozen (frozenIdentityCheck).
func withIdentityCheck(ctx context.Context, id *capturedIdentity) context.Context {
	return context.WithValue(ctx, identityCheckKey{}, id)
}

// identityCheckFrom returns withIdentityCheck's identity, or nil.
func identityCheckFrom(ctx context.Context) *capturedIdentity {
	id, _ := ctx.Value(identityCheckKey{}).(*capturedIdentity)
	return id
}

// identityUnchecked fails a capture whose identity id the holder's dump
// did not confirm under the freeze (results by slot): an agent that
// predates the check ignores it, and its identity may be stale. nil when
// there is no identity.
func identityUnchecked(id *capturedIdentity, results []*CheckpointResult) error {
	if id == nil {
		return nil
	}
	if id.Member >= len(results) || results[id.Member] == nil || !results[id.Member].IdentityChecked {
		return errors.New("identity capture: the agent that dumped the lease holder did not confirm its etcd identity under the freeze")
	}
	return nil
}

// identityFreezeCheckTimeout bounds the check of an identity under the
// freeze; the worker is stopped meanwhile.
var identityFreezeCheckTimeout = 10 * time.Second

// frozenIdentityCheck returns the dump's check, once req's container is
// frozen and before CRIU runs, that the worker's lease holds exactly the
// keys and values the capture read before the freeze (req.IdentityCheck;
// identityStillHeld): a key added, removed or changed in between would be
// missing from the record, and a restore would write a stale set. Frozen,
// the worker changes nothing more before the dump ends it. A mismatch or
// an unreadable registry fails the dump before it runs, and the source
// resumes. checked is set once the check passed. nil when not asked.
func (a *Agent) frozenIdentityCheck(ctx context.Context, req CheckpointRequest, checked *bool) func() error {
	id := req.IdentityCheck
	if id == nil {
		return nil
	}
	return func() error {
		cctx, cancel := context.WithTimeout(ctx, identityFreezeCheckTimeout)
		defer cancel()
		via := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: req.Namespace, Name: req.PodName}}
		e, closeFn, err := newIdentityEtcd(cctx, a, id.Endpoints, req.Namespace, via, req.ContainerName)
		if err != nil {
			return fmt.Errorf("identity capture: reach the worker's etcd under the freeze: %w", err)
		}
		defer closeFn()
		if err := identityStillHeld(cctx, e, id, id.PodIP); err != nil {
			return fmt.Errorf("identity capture: under the freeze: %w", err)
		}
		*checked = true
		return nil
	}
}

// frozenHooks runs each of hooks that is set, in order, as one
// pidnsDumpHooks.Frozen; nil when none is.
func frozenHooks(hooks ...func() error) func() error {
	var set []func() error
	for _, h := range hooks {
		if h != nil {
			set = append(set, h)
		}
	}
	if len(set) == 0 {
		return nil
	}
	return func() error {
		for _, h := range set {
			if err := h(); err != nil {
				return err
			}
		}
		return nil
	}
}

// identityAlone decides whether leader's instance, which the group capture
// would refuse (why), is captured alone with its etcd identity instead.
// Only with the flag on, and only for an instance whose ranks pass
// identityRanksCandidate.
func (c *criuAutoRestorer) identityAlone(ranks []*corev1.Pod, why string, log *logrus.Entry) bool {
	if !c.a.identityEnabled() {
		return false
	}
	if b, not := identityRanksCandidate(ranks); b == "" {
		log.WithFields(logrus.Fields{"reason": why, "identity": not}).Info("CRIU group capture: the instance cannot be captured with its etcd identity")
		return false
	}
	log.WithField("reason", why).Info("CRIU group capture: capturing the worker alone, with its etcd identity, its companions excluded")
	return true
}

// sharedBlocksCapture is sharedCompanionsBlockCapture, except that with
// the flag on a Dynamo worker that registers in etcd is not refused for
// the companions it shares: it is captured alone (alone), with its
// identity.
func (c *criuAutoRestorer) sharedBlocksCapture(ctx context.Context, leader *corev1.Pod, pods []corev1.Pod, log *logrus.Entry) (again, skip, alone bool) {
	if !c.a.identityEnabled() {
		again, skip = c.sharedCompanionsBlockCapture(ctx, leader, pods, log)
		return again, skip, false
	}
	why, err := c.a.sharedCompanionsReason(ctx, leader, pods)
	if err != nil {
		log.WithError(err).Info("CRIU group capture: waiting to read the namespace's scaling groups")
		return true, true, false
	}
	if why == "" {
		return false, false, false
	}
	if b, _ := identityRestoreCandidate(leader); b != "" {
		log.WithField("reason", why).Info("CRIU group capture: the namespace's companions are shared; the worker is captured alone, with its etcd identity")
		return false, false, true
	}
	log.WithField("reason", why).Info("CRIU group capture: skipped; the instance starts from the caches")
	return false, true, false
}

// identityRecord reads group record key, following an alias. nil data
// without one.
func (a *Agent) identityRecord(ctx context.Context, key string) (data map[string]string, target string, err error) {
	cms := a.kubeClient.CoreV1().ConfigMaps(a.criuFailedNamespace())
	cm, err := cms.Get(ctx, criuGroupConfigMapName(key), metav1.GetOptions{})
	if err != nil {
		return nil, key, err
	}
	if cm.Data["state"] == criuGroupStateAlias && cm.Data["alias"] != "" {
		key = cm.Data["alias"]
		if cm, err = cms.Get(ctx, criuGroupConfigMapName(key), metav1.GetOptions{}); err != nil {
			return nil, key, err
		}
	}
	return cm.Data, key, nil
}

// identityPlaceholder reports whether pod is a placeholder of a group
// restore whose record holds an identity, with the flag on: its instance
// was captured alone, so what its namespace shares is not restored with
// it. An unreadable record is an error (fail closed).
func (a *Agent) identityPlaceholder(ctx context.Context, pod *corev1.Pod) (bool, error) {
	if pod.Annotations[webhook.CRIURestoreAnnotation] == "" {
		return false, nil
	}
	return a.identityRecordAlone(ctx, pod.Annotations[webhook.CRIUGroupAnnotation])
}

// errIdentityRefused marks a restore the identity restore refused before
// anything was restored: the pod starts cold, and the checkpoint is not
// held against.
var errIdentityRefused = errors.New("identity restore refused")

// identityRefusal is one refusal, with its decision.
type identityRefusal struct {
	decision string
	err      error
}

func (e *identityRefusal) Error() string { return "identity restore refused: " + e.err.Error() }
func (e *identityRefusal) Unwrap() []error {
	return []error{errIdentityRefused, e.err}
}

func refuseIdentity(decision string, err error) error {
	return &identityRefusal{decision: decision, err: err}
}

// identityDecision returns the decision of a refusal, "" for another error.
func identityDecision(err error) string {
	var r *identityRefusal
	if errors.As(err, &r) {
		return r.decision
	}
	return ""
}

// holderOf names pod as an identity holder.
func holderOf(pod *corev1.Pod) string {
	return pod.Namespace + "/" + pod.Name + "/" + string(pod.UID)
}

// holderAlive reports whether the pod holder names exists, with that UID,
// being deleted or not.
func (a *Agent) holderAlive(ctx context.Context, holder string) (bool, error) {
	parts := strings.SplitN(holder, "/", 3)
	if len(parts) != 3 {
		return false, nil
	}
	p, err := a.kubeClient.CoreV1().Pods(parts[0]).Get(ctx, parts[1], metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// A pod being deleted still runs until it is gone: a worker that
	// drains keeps its subscriptions meanwhile.
	return p.UID == types.UID(parts[2]), nil
}

// claimIdentity makes pod the holder of record key's identity. A holder
// that is another pod, alive, keeps it (refused); one gone is replaced.
// The update is conditional on the record's version, so of two agents
// claiming at once one wins.
func (c *criuAutoRestorer) claimIdentity(ctx context.Context, key string, pod *corev1.Pod) error {
	me := holderOf(pod)
	var refused error
	err := c.updateGroupRecord(ctx, key, func(d map[string]string) bool {
		refused = nil
		if identityClaimReadHook != nil {
			identityClaimReadHook()
		}
		if !hasIdentity(d) {
			refused = refuseIdentity(identityRefusedError, errors.New("the record no longer holds an identity"))
			return false
		}
		switch h := d[identityHolderKey]; h {
		case me:
			return false
		case "":
		default:
			alive, err := c.a.holderAlive(ctx, h)
			if err != nil {
				refused = refuseIdentity(identityRefusedError, fmt.Errorf("cannot tell whether the identity's holder lives: %w", err))
				return false
			}
			if alive {
				refused = refuseIdentity(identityRefusedHeld, errors.New("another restore holds the identity"))
				return false
			}
		}
		d[identityHolderKey] = me
		delete(d, identityGrantedKey)
		delete(d, identityHandoffKey)
		return true
	})
	if refused != nil {
		return refused
	}
	if err != nil {
		return refuseIdentity(identityRefusedError, fmt.Errorf("claim the identity: %w", err))
	}
	return nil
}

// identityClaimReadHook, when set, runs in a claim between its read of the
// record and its write; for tests.
var identityClaimReadHook func()

// markGranted records that pod's restore had record key's lease granted.
func (c *criuAutoRestorer) markGranted(ctx context.Context, key string, pod *corev1.Pod) error {
	me := holderOf(pod)
	var lost bool
	err := c.updateGroupRecord(ctx, key, func(d map[string]string) bool {
		lost = d[identityHolderKey] != me
		if lost || d[identityGrantedKey] == me {
			return false
		}
		d[identityGrantedKey] = me
		return true
	})
	if err == nil && lost {
		err = errors.New("the claim was lost")
	}
	return err
}

// releaseIdentity clears pod's claim on record key's identity.
func (c *criuAutoRestorer) releaseIdentity(ctx context.Context, key string, pod *corev1.Pod, log *logrus.Entry) {
	me := holderOf(pod)
	if err := c.updateGroupRecord(ctx, key, func(d map[string]string) bool {
		if d[identityHolderKey] != me {
			return false
		}
		delete(d, identityHolderKey)
		delete(d, identityGrantedKey)
		delete(d, identityHandoffKey)
		return true
	}); err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).Warn("identity restore: cannot release the claim on the identity; it is released once the pod is gone")
	}
}

// awaitLeaseFree waits until etcd holds no live lease id, for at most the
// recorded TTL and identityExpirySlack: a lease the dead source held
// expires within its TTL. A lease whose TTL is renewed meanwhile, or that
// outlives the wait, is held by a live process (refused).
func awaitLeaseFree(ctx context.Context, e identityEtcd, id, ttl int64) error {
	deadline := time.Now().Add(time.Duration(ttl)*time.Second + identityExpirySlack)
	last := int64(-1)
	for {
		info, err := e.TimeToLive(ctx, id, false)
		if err != nil {
			return refuseIdentity(identityRefusedError, fmt.Errorf("read the captured lease: %w", err))
		}
		if info.TTL < 0 {
			return nil
		}
		if last >= 0 && info.TTL > last {
			return refuseIdentity(identityRefusedLiveLease, errors.New("the captured lease is being kept alive by a live process"))
		}
		last = info.TTL
		if time.Now().After(deadline) {
			return refuseIdentity(identityRefusedLiveLease, errors.New("the captured lease is still live"))
		}
		select {
		case <-ctx.Done():
			return refuseIdentity(identityRefusedError, ctx.Err())
		case <-time.After(identityPoll):
		}
	}
}

// grantLease grants lease id of identity id again with its recorded TTL.
// etcd refuses a live ID: another process holds the identity.
func grantLease(ctx context.Context, e identityEtcd, id *capturedIdentity) error {
	if err := e.Grant(ctx, id.Lease, id.TTL); err != nil {
		if errors.Is(err, errLeaseExists) {
			return refuseIdentity(identityRefusedLiveLease, errors.New("etcd refused the grant: the lease is live"))
		}
		return refuseIdentity(identityRefusedError, fmt.Errorf("grant the lease: %w", err))
	}
	return nil
}

// writeIdentityKeys writes the identity's keys under its lease, with the
// captured address replaced by newIP. A key another lease holds now is
// left as it is and fails the write: another worker owns it.
func writeIdentityKeys(ctx context.Context, e identityEtcd, id *capturedIdentity, newIP string) error {
	var writes []identityKey
	var addrs []string
	for _, k := range id.Keys {
		// A key already under the lease is the process's own, maybe
		// newer than the capture's (an agent that adopts after a
		// restart): left as it is.
		cur, err := e.Get(ctx, k.Key)
		if err != nil {
			return fmt.Errorf("write the identity's keys: %w", err)
		}
		if cur != nil && cur.Lease == id.Lease {
			continue
		}
		v := rewriteHost(k.Value, id.PodIP, newIP)
		writes = append(writes, identityKey{Key: k.Key, Value: v})
		addrs = append(addrs, requestPlaneAddrs(v, newIP)...)
	}
	// Registered on the TCP request plane, the worker is routed to at the
	// address its keys name: only once it answers there.
	if err := awaitRequestPlane(ctx, slices.Compact(slices.Sorted(slices.Values(addrs))), id.TTL); err != nil {
		return fmt.Errorf("write the identity's keys: %w", err)
	}
	for _, k := range writes {
		owned, err := e.PutOwned(ctx, k.Key, k.Value, id.Lease)
		if err != nil {
			return fmt.Errorf("write the identity's keys: %w", err)
		}
		if !owned {
			return errors.New("write the identity's keys: a key is held by another lease")
		}
	}
	return nil
}

// regrant grants the lease and writes the keys at once. Any failure after
// the grant revokes it, so no half-written identity stays.
func regrant(ctx context.Context, e identityEtcd, id *capturedIdentity, newIP string) error { //nolint:unparam // tests restore to one address
	if err := grantLease(ctx, e, id); err != nil {
		return err
	}
	if err := writeIdentityKeys(ctx, e, id, newIP); err != nil {
		_ = e.Revoke(context.WithoutCancel(ctx), id.Lease)
		return refuseIdentity(identityRefusedError, err)
	}
	return nil
}

// leaseKeeper keeps a regranted lease alive from the agent until the
// restored process's own keep-alive is seen.
type leaseKeeper struct {
	e       identityEtcd
	id, ttl int64
	log     *logrus.Entry
	// release closes the registry; writeKeys registers the identity once
	// the process runs; onAborted releases the claim of a restore that
	// failed; onZombie ends a restored pod whose identity was lost;
	// onHandoff records that the process keeps its lease.
	release, onAborted, onZombie, onHandoff func()
	writeKeys                               func(context.Context) error

	once    sync.Once
	resumed chan struct{}
	aborted chan struct{}
	done    chan struct{}
	outcome string
}

func newLeaseKeeper(e identityEtcd, id, ttl int64, log *logrus.Entry) *leaseKeeper {
	return &leaseKeeper{e: e, id: id, ttl: ttl, log: log, release: func() {},
		resumed: make(chan struct{}), aborted: make(chan struct{}), done: make(chan struct{})}
}

// restored tells the keeper how the restore ended: resumed (err nil, or
// the pod already runs its restored engine), the identity is registered
// and the wait for the process's keep-alive starts; failed, the lease is
// revoked. Nil-safe; only the first call counts.
func (k *leaseKeeper) restored(err error) {
	if k == nil {
		return
	}
	k.once.Do(func() {
		if err == nil || errors.Is(err, errNotPlaceholder) {
			close(k.resumed)
		} else {
			close(k.aborted)
		}
	})
}

// run keeps the lease alive. Before the process resumes it renews the
// lease whenever half its TTL is left: a gateway call may take seconds. Once it resumed, the keeper
// writes the identity's keys (the frontend routes to the worker only then),
// and watches the lease's TTL against what its own last renewal explains.
// Held above that for a whole TTL, the lease is renewed by someone else
// all along: the process's keep-alive has taken over, and it stops. One
// renewal not its own (etcd renews every lease on a leader change) keeps
// it above for under half a TTL, after which the keeper renews it again;
// samples need not rise between polls. Before it hands off, the keeper
// renews the lease once more: a late sample may tell of a lease that has
// expired since. It fails closed: a process whose keep-alive is not
// seen within max(3 TTL, identityHandoffFloor), or whose keys cannot be
// written, has its lease revoked and its pod ended, so the frontend stops
// routing to a worker that does not keep its registration and no second
// restore takes the identity while this one lives.
func (k *leaseKeeper) run(ctx context.Context) {
	defer close(k.done)
	defer k.release()
	ttl := time.Duration(k.ttl) * time.Second
	handoffWait := max(3*ttl, identityHandoffFloor)
	tolerance := min(identityTolerance, ttl/2)
	own, ownTTL := time.Now(), ttl
	var deadline time.Time
	resumed := k.resumed
	// above is when the lease's TTL first ran ahead of what the agent's
	// own last renewal explains, zero while it does not.
	var above time.Time
	end := func(outcome string, revoke bool, then func()) {
		k.outcome = outcome
		observeIdentity(outcome)
		if revoke {
			if err := k.e.Revoke(context.WithoutCancel(ctx), k.id); err != nil {
				k.log.WithError(err).Warn("identity restore: cannot revoke the lease; it expires on its own")
			}
		}
		if then != nil {
			then()
		}
	}
	for {
		select {
		case <-ctx.Done():
			// The claim stays: the restored pod may hold the identity on
			// its own.
			k.log.Warn("identity restore: the agent stops; the lease is no longer kept alive by it")
			end(identityKeeperAgentStopped, false, nil)
			return
		case <-k.aborted:
			k.log.Warn("identity restore: the restore failed; revoking the regranted lease")
			end(identityRestoreAborted, true, k.onAborted)
			return
		case <-resumed:
			resumed = nil
			if k.writeKeys != nil {
				if err := k.writeKeys(ctx); err != nil {
					if ctx.Err() != nil {
						// The agent stops: the claim stays, and the next
						// agent adopts the identity and registers it.
						end(identityKeeperAgentStopped, false, nil)
						return
					}
					k.log.WithError(err).Error("identity restore: cannot register the restored worker; revoking its lease and ending its pod")
					end(identityKeysFailed, true, k.onZombie)
					return
				}
				k.log.Info("identity restore: the restored worker is registered at its new address")
			}
			deadline = time.Now().Add(handoffWait)
		case <-time.After(identityPoll):
		}
		// etcd reads the TTL somewhere between the request (at) and the
		// reply. Each bound is taken the way that cannot hand off early:
		// the TTL is compared with what the agent's own renewal explains
		// at the request, a lease seen ahead counts as ahead from the
		// reply, and how long it stays ahead is measured to the request.
		at := time.Now()
		info, err := k.e.TimeToLive(ctx, k.id, false)
		switch {
		case err != nil:
			k.log.WithError(err).Warn("identity restore: cannot read the lease; keeping it alive")
		case info.TTL < 0:
			k.log.Error("identity restore: the lease expired before the restored process kept it alive; ending its pod")
			end(identityHandoffLost, false, k.onZombie)
			return
		default:
			if !deadline.IsZero() && time.Duration(info.TTL)*time.Second > ownTTL-at.Sub(own)+tolerance {
				if above.IsZero() {
					above = time.Now()
				}
			} else {
				above = time.Time{}
			}
			if !above.IsZero() && at.Sub(above) >= ttl {
				// A late reply may tell of a lease that expired since:
				// renewed once more, it is shown live now, or lost.
				sent := time.Now()
				got, kerr := k.e.KeepAlive(ctx, k.id)
				switch {
				case kerr != nil:
					k.log.WithError(kerr).Warn("identity restore: cannot confirm the lease before handing it off; trying again")
					own, ownTTL, above = time.Now(), ttl, time.Time{}
				case got > 0 && time.Since(sent) >= time.Duration(got)*time.Second:
					// The renewal's reply came as late as the TTL it
					// granted: the lease may have expired since. Not a
					// handoff; the next sample tells.
					k.log.Warn("identity restore: the lease's renewal answered too late to confirm it; checking it again")
					own, ownTTL, above = sent, time.Duration(got)*time.Second, time.Time{}
				case got <= 0:
					k.log.Error("identity restore: the lease expired before the handoff; ending the restored pod")
					end(identityHandoffLost, false, k.onZombie)
					return
				default:
					k.log.WithField("waited", time.Since(deadline.Add(-handoffWait)).Round(time.Second).String()).
						Info("identity restore: the restored process keeps its lease alive; the agent stops keeping it")
					end(identityHandoffSeen, false, k.onHandoff)
					return
				}
			}
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			k.log.WithField("waited", handoffWait.String()).Error("identity restore: the restored process never kept its lease alive; revoking it and ending its pod")
			end(identityHandoffTimeout, true, k.onZombie)
			return
		}
		// etcd reports whole seconds, rounded down: renewed once at most
		// half the TTL can be left.
		if err != nil || time.Duration(info.TTL+1)*time.Second <= max(ttl/2, time.Second) {
			got, kerr := k.e.KeepAlive(ctx, k.id)
			switch {
			case kerr != nil:
				k.log.WithError(kerr).Warn("identity restore: keep-alive failed; trying again")
			case got <= 0:
				k.log.Error("identity restore: etcd no longer holds the lease; ending the restored pod")
				end(identityHandoffLost, false, k.onZombie)
				return
			default:
				own, ownTTL, above = time.Now(), time.Duration(got)*time.Second, time.Time{}
			}
		}
	}
}

// identityBeforeRestore makes the identity of record key valid for its
// restore into slots (placeholders by slot), before anything is restored:
// claims it, waits for the captured lease to be free and grants it again.
// Its keys are written once the process resumed (leaseKeeper). via is a
// placeholder on this node the registry is reached from; keep is the
// agent's lifetime, which the keeper runs in. It returns a running keeper
// to tell the restore's outcome (restored), or nil for a record without an
// identity. A refusal (errIdentityRefused) restored nothing and holds
// nothing.
//
// A record whose identity this pod already holds (an agent that stopped
// after its claim, the restore done or not) is adopted: a live lease is
// kept alive, not refused, and a restore that finds the engine already
// running hands the lease over to it.
//
// With the flag off, only a record that holds an identity is acted on: it
// is refused, since nothing would grant its lease (a placeholder admitted
// while the flag was on).
func (c *criuAutoRestorer) identityBeforeRestore(ctx, keep context.Context, key string, slots []*corev1.Pod, via *corev1.Pod, log *logrus.Entry) (*leaseKeeper, error) {
	if key == "" {
		return nil, nil
	}
	d, key, err := c.a.identityRecord(ctx, key)
	if err != nil {
		if !c.a.identityEnabled() {
			return nil, nil // the restore decides as it always has
		}
		// With the flag on, a checkpoint whose record cannot be read may
		// hold an identity nothing would grant: refused (fail closed).
		return nil, refuseIdentity(identityRefusedError, fmt.Errorf("read the group record: %w", err))
	}
	if c.a.identityEnabled() {
		// The identity a record holds is its capture's: a record replaced
		// since the placeholder was admitted (recaptured, or capturing
		// now) says nothing of this checkpoint's identity.
		if !recordUsable(d) {
			return nil, refuseIdentity(identityRefusedError, fmt.Errorf("the group record is %q, not a capture to restore", d["state"]))
		}
		for _, s := range slots {
			if s != nil && !recordOf(d, s.Annotations[webhook.CRIURestoreAnnotation]) {
				return nil, refuseIdentity(identityRefusedError, errors.New("the group record no longer names the placeholder's checkpoint"))
			}
		}
	}
	if !hasIdentity(d) {
		return nil, nil
	}
	if !c.a.identityEnabled() {
		return nil, refuseIdentity(identityRefusedDisabled, errors.New("the capture holds an etcd identity and identity restore is off"))
	}
	id, err := identityFromRecord(d)
	if err != nil {
		return nil, refuseIdentity(identityRefusedError, err)
	}
	// An identity restores only into the namespace it was captured in
	// when its worker names that namespace in its configuration
	// (identityBlocksCapture): restored elsewhere, it would join the
	// source namespace's registry.
	for _, s := range slots {
		if s == nil {
			continue
		}
		if webhook.NamesOwnInstance(s) != "" && s.Namespace != d[identityNamespaceKey] {
			return nil, refuseIdentity(identityRefusedError, errors.New("the identity was captured in another namespace, which the worker names in its configuration"))
		}
	}
	if id.Member >= len(slots) || slots[id.Member] == nil {
		return nil, refuseIdentity(identityRefusedError, fmt.Errorf("no placeholder for the lease holder's slot %d", id.Member))
	}
	holder := slots[id.Member]
	newIP := holder.Status.PodIP
	if net.ParseIP(newIP) == nil {
		return nil, refuseIdentity(identityRefusedError, errors.New("the placeholder has no pod address"))
	}
	log = log.WithField("identityKeys", len(id.Keys))
	// Adopted only once this pod's restore had the lease granted: a claim
	// alone proves nothing about the lease.
	adopt := d[identityHolderKey] == holderOf(holder) && d[identityGrantedKey] == holderOf(holder)
	if !adopt {
		if cerr := c.claimIdentity(ctx, key, holder); cerr != nil {
			return nil, cerr
		}
	}
	release := func() {
		if !adopt {
			c.releaseIdentity(context.WithoutCancel(ctx), key, holder, log)
		}
	}
	e, closeFn, err := newIdentityEtcd(ctx, c.a, id.Endpoints, via.Namespace, via, via.Annotations[webhook.CRIURestoreContainerAnnotation])
	if err != nil {
		release()
		return nil, refuseIdentity(identityRefusedError, fmt.Errorf("reach the registry: %w", err))
	}
	granted := false
	if adopt {
		info, terr := e.TimeToLive(ctx, id.Lease, false)
		if terr != nil {
			// Fail closed: whether the lease is still this restore's
			// grant cannot be told. The claim stays this pod's.
			closeFn()
			return nil, refuseIdentity(identityRefusedError, fmt.Errorf("read the adopted lease: %w", terr))
		}
		granted = info.TTL >= 0 // held already: keep it, not refuse it
	}
	if !granted {
		if err := awaitLeaseFree(ctx, e, id.Lease, id.TTL); err != nil {
			closeFn()
			release()
			return nil, err
		}
		if err := grantLease(ctx, e, id); err != nil {
			closeFn()
			release()
			return nil, err
		}
	}
	k := c.identityKeeper(keep, key, id, e, closeFn, holder, newIP, log)
	// The keeper renews the lease from the grant on, while the grant is
	// recorded.
	go k.run(keep)
	if granted {
		log.Info("identity restore: this pod already holds the identity; keeping its lease alive")
		return k, nil
	}
	if merr := c.markGranted(ctx, key, holder); merr != nil {
		rerr := refuseIdentity(identityRefusedError, fmt.Errorf("record the grant: %w", merr))
		k.restored(rerr) // revokes the lease and releases the claim
		<-k.done
		return nil, rerr
	}
	observeIdentity(identityRegranted)
	log.Info("identity restore: the captured lease is granted again; restoring")
	c.clearRefusal(ctx, key, refusalID(slots), log)
	return k, nil
}

// clearRefusal drops refusal rid's counted stall entry from record key:
// its restore runs now, so a later refusal of these pods is a new one.
func (c *criuAutoRestorer) clearRefusal(ctx context.Context, key, rid string, log *logrus.Entry) {
	if err := c.updateGroupRecord(ctx, key, func(d map[string]string) bool {
		r := listed(d[identityRefusedPodsKey])
		n := len(r)
		r = slices.DeleteFunc(r, func(x string) bool { return x == rid })
		if len(r) == n {
			return false
		}
		setListed(d, identityRefusedPodsKey, r)
		return true
	}); err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).Warn("identity restore: cannot clear an earlier refusal from the group record")
	}
}

// identityKeeper is the keeper of identity id of record key for holder,
// restored at newIP, over registry e (closed by closeFn).
func (c *criuAutoRestorer) identityKeeper(keep context.Context, key string, id *capturedIdentity, e identityEtcd, closeFn func(), holder *corev1.Pod, newIP string, log *logrus.Entry) *leaseKeeper {
	k := newLeaseKeeper(e, id.Lease, id.TTL, log)
	// One keeper per identity in this agent: an agent that restarted finds
	// none and adopts the restored pod's lease (considerRestoredIdentity).
	c.attempted.Store("identity-keeper/"+key, struct{}{})
	k.release = func() {
		closeFn()
		c.attempted.Delete("identity-keeper/" + key)
	}
	k.writeKeys = func(ctx context.Context) error { return writeIdentityKeys(ctx, e, id, newIP) }
	k.onAborted = func() { c.releaseIdentity(context.WithoutCancel(keep), key, holder, log) }
	k.onHandoff = func() {
		if err := c.updateGroupRecord(context.WithoutCancel(keep), key, func(d map[string]string) bool {
			if d[identityHolderKey] != holderOf(holder) || d[identityHandoffKey] == holderOf(holder) {
				return false
			}
			d[identityHandoffKey] = holderOf(holder)
			return true
		}); err != nil {
			log.WithError(err).Warn("identity restore: cannot record the handoff; an agent that restarts checks the lease again")
		}
	}
	k.onZombie = func() {
		// A restore whose process does not keep its registration counts
		// against its capture, as any failed restore (recordFailure), so
		// replacements that fail the same way end; then the pod is ended.
		// The claim stays until the pod is gone (holderAlive).
		cause := fmt.Errorf("identity restore: the restored worker lost its registration (%s)", k.outcome)
		ck := holder.Annotations[webhook.CRIURestoreAnnotation]
		if uerr := c.updateGroupRecord(context.WithoutCancel(keep), key, func(d map[string]string) bool { return recordFailure(d, ck, true, cause) }); uerr != nil && !apierrors.IsNotFound(uerr) {
			log.WithError(uerr).Error("identity restore: cannot count the lost registration against the capture")
		}
		if derr := c.a.deletePodRetrying(context.WithoutCancel(keep), holder.Namespace, holder.Name, holder.UID); derr != nil {
			log.WithError(derr).Error("identity restore: cannot delete the pod that lost its identity")
		}
	}
	return k
}

// considerRestoredIdentity adopts, after an agent restart, the identity of
// a restored pod that no agent keeps: the restore's agent stopped after
// the restore and before the restored process was seen keeping its lease
// (the pod is marked restored, so no restore is tried in it again, and
// its keys may never have been written). The agent of the pod that drove
// the restore (a lone pod, or a group's leader) runs a keeper for it, as
// the restore's would have from the process's resume on: it registers
// the keys the lease does not hold yet and waits for the handoff; a lease
// lost, or never kept, ends the holder's pod (onZombie).
func (c *criuAutoRestorer) considerRestoredIdentity(ctx context.Context, pod *corev1.Pod) {
	key := pod.Annotations[webhook.CRIUGroupAnnotation]
	if !c.a.identityEnabled() || key == "" || pod.Annotations[webhook.CRIURestoredAnnotation] == "" ||
		pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return
	}
	if o, _, ok := podRank(pod, webhook.CRIUGroupOrdinalAnnotation, webhook.CRIUGroupSizeAnnotation); isGroupPlaceholder(pod) && (!ok || o != 0) {
		return // a group's identity is kept from its leader's node
	}
	d, key, err := c.a.identityRecord(ctx, key)
	if err != nil || !hasIdentity(d) {
		return
	}
	h := d[identityHolderKey]
	if h == "" || d[identityGrantedKey] != h || d[identityHandoffKey] == h {
		return
	}
	if _, running := c.attempted.Load("identity-keeper/" + key); running {
		return
	}
	if _, busy := c.attempted.LoadOrStore("identity-adopt/"+key, struct{}{}); busy {
		return
	}
	go func() {
		defer c.attempted.Delete("identity-adopt/" + key)
		log := c.a.log.WithFields(logrus.Fields{"pod": pod.Namespace + "/" + pod.Name, "criuGroup": key})
		id, err := identityFromRecord(d)
		if err != nil {
			log.WithError(err).Warn("identity restore: the restored pod's identity record cannot be read")
			return
		}
		parts := strings.SplitN(h, "/", 3)
		if len(parts) != 3 {
			return
		}
		holder, err := c.a.kubeClient.CoreV1().Pods(parts[0]).Get(ctx, parts[1], metav1.GetOptions{})
		if err != nil || string(holder.UID) != parts[2] || holder.Annotations[webhook.CRIURestoredAnnotation] == "" ||
			(holder.UID != pod.UID && holder.Annotations[webhook.CRIUGroupAnnotation] != pod.Annotations[webhook.CRIUGroupAnnotation]) {
			return // not this restore's pod, or not restored: its restore adopts it
		}
		if net.ParseIP(holder.Status.PodIP) == nil {
			return
		}
		e, closeFn, err := newIdentityEtcd(ctx, c.a, id.Endpoints, pod.Namespace, pod, pod.Annotations[webhook.CRIURestoreContainerAnnotation])
		if err != nil {
			log.WithError(err).Warn("identity restore: cannot reach the registry to adopt the restored pod's lease; trying again on its next event")
			return
		}
		if _, running := c.attempted.Load("identity-keeper/" + key); running {
			closeFn()
			return
		}
		log.Info("identity restore: adopting the lease of a restored pod no agent keeps")
		k := c.identityKeeper(ctx, key, id, e, closeFn, holder, holder.Status.PodIP, log)
		go k.run(ctx)
		k.restored(nil) // the process runs: register it and wait for its keep-alive
	}()
}

// identityRefused handles the pods (by slot) of a restore the identity
// restore refused (settleRefused).
//
// The refusal is recorded on record key first (identityRefusingKey, by pod
// UID), before any pod is acted on, so that a pod left unfinished is
// finished even by another agent, or after this one restarts: the pod's
// own agent finishes it on its next event (considerRefusing). When the
// refusal cannot be recorded, nothing is done and again hands it to the
// restore's next event.
//
// When a pod's refusal is left unfinished, or a pod kept for the workload
// it runs was refused for an error that may pass (an adopted lease that
// could not be read), and nothing was done to any pod, again hands the
// refusal to the restore's next event, which decides it anew. Once a pod
// has started cold or been deleted the restore cannot run again (its
// leader may be gone), so this agent also finishes the pods left
// unfinished itself (finishRefused).
func (c *criuAutoRestorer) identityRefused(ctx context.Context, key, checkpoint string, pods []*corev1.Pod, err error, again func(), log *logrus.Entry) {
	decision := identityDecision(err)
	observeIdentity(decision)
	log = log.WithError(err).WithField("decision", decision)
	rid := refusalID(pods)
	if merr := c.markRefusing(ctx, key, rid, pods); merr != nil {
		log.WithError(merr).Warn("identity restore: cannot record the refusal on the group record; nothing done, it is tried again")
		if again != nil {
			again()
		}
		return
	}
	pending, running, remote, acted := c.settleRefused(ctx, key, checkpoint, rid, pods, err, log)
	retry := len(pending) > 0 || (len(running) > 0 && decision == identityRefusedError)
	if retry && !acted && again != nil {
		c.unmarkRefusing(ctx, key, pods, false, log)
		again()
		return
	}
	// Pods on other nodes stay recorded: their own agents settle them.
	c.unmarkRefusing(ctx, key, settledOf(pods, append(pending, remote...)), true, log)
	if len(pending) > 0 {
		go c.finishRefused(ctx, key, checkpoint, rid, pending, err, log)
	}
}

// refusalID names the refusal of pods (by slot) on the group record: its
// first pod (a group's leader, or the lone pod), as holderOf names it, so
// that a refusal whose pod is gone can be told. A refusal decided again by
// the restore's next event keeps it.
func refusalID(pods []*corev1.Pod) string {
	if len(pods) == 0 {
		return ""
	}
	return holderOf(pods[0])
}

// settledOf is pods without pending, by UID.
func settledOf(pods, pending []*corev1.Pod) []*corev1.Pod {
	var out []*corev1.Pod
	for _, p := range pods {
		if !slices.ContainsFunc(pending, func(q *corev1.Pod) bool { return q.UID == p.UID }) {
			out = append(out, p)
		}
	}
	return out
}

// identityFinishRetry paces finishRefused, as recoverRetry paces what an
// earlier run left. Tests shorten it (identityFinishRetryMu).
var (
	identityFinishRetry   = struct{ first, max, total time.Duration }{time.Second, 30 * time.Second, 24 * time.Hour}
	identityFinishRetryMu sync.RWMutex
)

// finishRefused tries pods' unfinished refusal again until every pod is
// settled, for at most identityFinishRetry's total or until ctx ends. Past
// that, each pod's own agent finishes it (considerRefusing).
func (c *criuAutoRestorer) finishRefused(ctx context.Context, key, checkpoint, rid string, pods []*corev1.Pod, err error, log *logrus.Entry) {
	identityFinishRetryMu.RLock()
	b := identityFinishRetry
	identityFinishRetryMu.RUnlock()
	time.Sleep(b.first)
	if rerr := retryUntil(ctx, b.first, b.max, b.total, func() (bool, error) {
		pending, _, remote, _ := c.settleRefused(ctx, key, checkpoint, rid, pods, err, log)
		c.unmarkRefusing(ctx, key, settledOf(pods, append(pending, remote...)), true, log)
		pods = pending
		return len(pods) == 0, nil
	}); len(pods) > 0 {
		log.WithError(rerr).WithField("pods", len(pods)).Error("identity restore: a refusal could not be finished here; each pod's own agent finishes it")
	}
}

// Record keys of refusals in progress. identityRefusingKey lists the pods
// refusals act on, as "uid=refusal" (refusalID), each until it is
// settled; identityRefusedPodsKey lists the refusals a stall was counted
// for (chargeRefusal), each until its pods are settled or its restore runs.
//
// Neither list drops an entry to make room. A refusal that would take
// identityRefusingKey past identityRefusalListMax is not recorded, and so
// not acted on, until entries settle (markRefusing); one that would take
// identityRefusedPodsKey past it is not counted, and its pods not
// deleted, until entries go: settled, restored (clearRefusal), or dead
// (deadRefusals).
const (
	identityRefusingKey    = "refusingPods"
	identityRefusedPodsKey = "refusedPods"
	identityRefusalListMax = 512
)

// errRefusalListFull is a refusal that cannot be recorded: too many are in
// progress on the record.
var errRefusalListFull = errors.New("too many refusals in progress on the group record")

// listed returns record list v.
func listed(v string) []string {
	if v == "" {
		return nil
	}
	return strings.Split(v, ",")
}

// setListed stores list l as record key k of d; an empty list is removed.
func setListed(d map[string]string, k string, l []string) {
	if len(l) == 0 {
		delete(d, k)
		return
	}
	d[k] = strings.Join(l, ",")
}

// refusing returns record data d's refusals in progress, by pod UID.
func refusing(d map[string]string) map[string]string {
	out := map[string]string{}
	for _, e := range listed(d[identityRefusingKey]) {
		if uid, rid, ok := strings.Cut(e, "="); ok {
			out[uid] = rid
		}
	}
	return out
}

// markRefusing records refusal rid of pods on record key, before any pod
// is acted on. A record gone is no error with identity restore on: every
// restore of its placeholders is refused, and each pod's own agent
// finishes it (considerRefusing). With the flag off it is an error: the
// restore's next event decides again, from no identity.
func (c *criuAutoRestorer) markRefusing(ctx context.Context, key, rid string, pods []*corev1.Pod) error {
	if key == "" || len(pods) == 0 {
		return nil
	}
	err := c.updateGroupRecord(ctx, key, func(d map[string]string) bool {
		l, in := listed(d[identityRefusingKey]), refusing(d)
		n := len(l)
		for _, p := range pods {
			if _, ok := in[string(p.UID)]; !ok {
				l = append(l, string(p.UID)+"="+rid)
			}
		}
		if len(l) == n || len(l) > identityRefusalListMax {
			return false
		}
		setListed(d, identityRefusingKey, l)
		return true
	})
	if err == nil {
		// Read back: a list that was full was left as it is.
		d, _, rerr := c.a.identityRecord(ctx, key)
		if rerr != nil {
			err = rerr
		} else {
			in := refusing(d)
			for _, p := range pods {
				if _, ok := in[string(p.UID)]; !ok {
					return errRefusalListFull
				}
			}
		}
	}
	if apierrors.IsNotFound(err) && c.a.identityEnabled() {
		return nil
	}
	return err
}

// unmarkRefusing drops pods from record key's refusals in progress. Once
// they are settled (not acted on again), a refusal with no pod left in
// progress also drops its counted stall's entry; pods handed back to their
// restore's next event keep it, so that refusal is not counted again. A
// failure leaves them listed, which only has their agents find them
// settled.
func (c *criuAutoRestorer) unmarkRefusing(ctx context.Context, key string, pods []*corev1.Pod, settled bool, log *logrus.Entry) {
	if key == "" || len(pods) == 0 {
		return
	}
	if err := c.updateGroupRecord(ctx, key, func(d map[string]string) bool {
		rids := map[string]bool{}
		l := listed(d[identityRefusingKey])
		n := len(l)
		l = slices.DeleteFunc(l, func(e string) bool {
			uid, rid, _ := strings.Cut(e, "=")
			drop := slices.ContainsFunc(pods, func(p *corev1.Pod) bool { return string(p.UID) == uid })
			if drop {
				rids[rid] = true
			}
			return drop
		})
		changed := len(l) != n
		setListed(d, identityRefusingKey, l)
		if settled {
			left := map[string]bool{}
			for _, e := range l {
				_, rid, _ := strings.Cut(e, "=")
				left[rid] = true
			}
			r := listed(d[identityRefusedPodsKey])
			m := len(r)
			r = slices.DeleteFunc(r, func(rid string) bool { return rids[rid] && !left[rid] })
			if len(r) != m {
				setListed(d, identityRefusedPodsKey, r)
				changed = true
			}
		}
		return changed
	}); err != nil && !apierrors.IsNotFound(err) {
		log.WithError(err).Warn("identity restore: cannot clear settled pods from the group record's refusal")
	}
}

// orphanedRank reports whether pod, a non-leader placeholder of a group
// restore, has no leader left to restore it: no pod of its instance with
// its group key is a placeholder that may still drive the restore (one
// gone, started cold, restored, or being deleted cannot). It cannot be
// told when the pods cannot be listed: then it is not.
func (c *criuAutoRestorer) orphanedRank(ctx context.Context, pod *corev1.Pod) bool {
	if !isGroupPlaceholder(pod) {
		return false
	}
	if o, _, ok := podRank(pod, webhook.CRIUGroupOrdinalAnnotation, webhook.CRIUGroupSizeAnnotation); !ok || o == 0 {
		return false
	}
	pods, err := c.a.kubeClient.CoreV1().Pods(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		o, _, ok := podRank(p, webhook.CRIUGroupOrdinalAnnotation, webhook.CRIUGroupSizeAnnotation)
		if ok && o == 0 && p.Annotations[webhook.CRIUGroupAnnotation] == pod.Annotations[webhook.CRIUGroupAnnotation] &&
			p.Annotations[webhook.CRIURestoreAnnotation] != "" && p.Annotations[webhook.CRIURestoredAnnotation] == "" &&
			!coldStarted(p) && p.DeletionTimestamp == nil && groupMember(p, pod) {
			return false
		}
	}
	return true
}

// considerRefusing finishes, on a pod's own event and on its own node, a
// refusal recorded for it (markRefusing) that the agent which refused it
// left unfinished: that agent may have restarted (with identity restore
// on or off), or the pod may be on another node. With the flag on, a pod
// whose group record is gone is refused all the same: any restore of it
// would be (identityBeforeRestore); with it off, a group's rank whose
// record is gone is when no leader is left to restore it (orphanedRank).
// It reports whether the pod is under such a refusal, which then is the
// only thing done for it.
func (c *criuAutoRestorer) considerRefusing(ctx context.Context, pod *corev1.Pod) bool {
	key := pod.Annotations[webhook.CRIUGroupAnnotation]
	ck, _, ready := criuRestoreTarget(pod)
	if key == "" || !ready {
		return false
	}
	d, _, err := c.a.identityRecord(ctx, key)
	var rid string
	switch {
	case apierrors.IsNotFound(err) && (c.a.identityEnabled() || c.orphanedRank(ctx, pod)):
		rid = holderOf(pod)
	case err != nil:
		return false
	default:
		var ok bool
		if rid, ok = refusing(d)[string(pod.UID)]; !ok {
			return false
		}
	}
	if _, busy := c.attempted.LoadOrStore("refusing/"+string(pod.UID), struct{}{}); busy {
		return true
	}
	go func() {
		defer c.attempted.Delete("refusing/" + string(pod.UID))
		log := c.a.log.WithFields(logrus.Fields{"pod": pod.Namespace + "/" + pod.Name, "criuGroup": key})
		cause := refuseIdentity(identityRefusedError, errors.New("a refusal recorded on the group record, or a group record gone, finished by the pod's own agent"))
		pending, _, remote, _ := c.settleRefused(context.WithoutCancel(ctx), key, ck, rid, []*corev1.Pod{pod}, cause, log)
		c.unmarkRefusing(context.WithoutCancel(ctx), key, settledOf([]*corev1.Pod{pod}, append(pending, remote...)), true, log)
	}()
	return true
}

// podRunsWorkload reports whether pod's container runs a GPU workload,
// as a restore would refuse it (errNotPlaceholder): a restored engine an
// earlier agent left. Only a pod on this node can be inspected (local); one
// on another node is reported as running none, and is decided with the
// instance's pods on this node (settleRefused). Without a runtime client
// nothing can be inspected, nor started cold, so it reports none. Tests
// replace it.
var podRunsWorkload = func(ctx context.Context, a *Agent, pod *corev1.Pod, container string) (runs, local bool, err error) {
	if a.runtime == nil {
		return false, true, nil
	}
	if pod.Spec.NodeName != a.config.NodeName {
		return false, false, nil
	}
	info, err := a.runtime.FindContainerByPod(ctx, pod.Namespace, pod.Name, container)
	if err != nil {
		return false, true, err
	}
	if info == nil || info.PID == 0 {
		return false, true, nil
	}
	pid, err := a.gpuProcessInSamePidNS(ctx, hostProcBase(), int(info.PID))
	return pid != 0, true, err
}

// settleRefused acts on pods (as refused, by UID and restore annotation)
// for a refused restore, nothing counted against the capture:
//
//   - a pod gone, replaced, no longer naming its refused checkpoint, or
//     started cold already is left as it is;
//   - when any pod runs a workload already (podRunsWorkload), the restore
//     ran before (an agent that stopped before it marked the pods): every
//     pod is kept as it is (running), none started cold or deleted;
//   - any other pod starts its workload cold in place. One that cannot is
//     deleted, for its controller to replace, only once record key counts
//     a stall for the refusal (chargeRefusal), so the replacements,
//     admitted at once and refused the same way, end at
//     criuGroupMaxStalls;
//   - a pod of which it cannot be told whether it runs a workload is never
//     deleted: it starts cold, or is kept and tried again;
//   - a pod on another node, which cannot be inspected here, is not acted
//     on (remote): its refusal stays recorded for its own agent, which can
//     inspect it and start it cold in place (considerRefusing).
//
// A pod started cold or deleted gives up its claim on the identity. A pod
// that cannot be read, or deleted, or whose stall cannot be counted, or
// that another refusal of this agent is acting on, is returned unfinished
// (pending), and nothing is deleted then. acted reports whether a pod
// started cold or was deleted.
func (c *criuAutoRestorer) settleRefused(ctx context.Context, key, checkpoint, rid string, pods []*corev1.Pod, err error, log *logrus.Entry) (pending, running, remote []*corev1.Pod, acted bool) {
	var live, unsure []*corev1.Pod
	runs := false
	for _, p := range pods {
		pl := log.WithField("pod", p.Namespace+"/"+p.Name)
		if _, busy := c.attempted.LoadOrStore("settling/"+string(p.UID), struct{}{}); busy {
			pending = append(pending, p)
			continue
		}
		defer c.attempted.Delete("settling/" + string(p.UID))
		var pod *corev1.Pod
		gerr := retry.OnError(retry.DefaultBackoff, func(e error) bool { return !apierrors.IsNotFound(e) }, func() error {
			var e error
			pod, e = c.a.kubeClient.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
			return e
		})
		if apierrors.IsNotFound(gerr) || (gerr == nil && (pod.UID != p.UID ||
			pod.Annotations[webhook.CRIURestoreAnnotation] != p.Annotations[webhook.CRIURestoreAnnotation] || coldStarted(pod))) {
			pl.Warn("identity restore refused; the pod is gone, replaced, started cold, or no longer names the refused checkpoint: left as it is")
			continue
		}
		if gerr != nil {
			pl.WithError(gerr).Warn("identity restore refused, and the pod cannot be read; trying again")
			pending = append(pending, p)
			continue
		}
		r, local, rerr := podRunsWorkload(ctx, c.a, pod, pod.Annotations[webhook.CRIURestoreContainerAnnotation])
		switch {
		case !local:
			// Its workload cannot be inspected from here, nor told from
			// its instance's pods here (its restore may have run on its
			// own): left, recorded, to its own agent (considerRefusing).
			pl.Info("identity restore refused; the pod is on another node, left to its own agent")
			remote = append(remote, p)
			continue
		case rerr != nil:
			pl.WithError(rerr).Warn("identity restore refused, and whether the pod runs a workload cannot be told; it is not deleted")
			unsure = append(unsure, pod)
		case r:
			runs = true
		}
		live = append(live, pod)
	}
	if runs {
		log.Warn("identity restore refused, and the instance already runs its workload: its pods are kept as they are")
		return pending, live, remote, false
	}
	var cold []*corev1.Pod
	for _, pod := range live {
		pl := log.WithField("pod", pod.Namespace+"/"+pod.Name)
		cerr := c.a.coldStartInPlace(ctx, pod, pod.Annotations[webhook.CRIURestoreContainerAnnotation], pl)
		switch {
		case cerr == nil:
			pl.Warn("identity restore refused; the pod starts cold in place")
			c.releaseIdentity(ctx, key, pod, pl)
			acted = true
		case slices.Contains(unsure, pod):
			pl.WithField("coldStart", cerr.Error()).Warn("identity restore refused; the pod cannot start cold in place, and may run a workload: kept, trying again")
			pending = append(pending, pod)
		default:
			pl.WithField("coldStart", cerr.Error()).Warn("identity restore refused, and the pod cannot start cold in place")
			cold = append(cold, pod)
		}
	}
	if len(cold) == 0 {
		return pending, nil, remote, acted
	}
	var uerr error
	if key != "" {
		dead := c.deadRefusals(ctx, key)
		full := false
		uerr = c.updateGroupRecord(ctx, key, func(d map[string]string) bool {
			var charged bool
			charged, full = chargeRefusal(d, checkpoint, rid, dead, err)
			return charged
		})
		if uerr == nil && full {
			uerr = errRefusalListFull
		}
	}
	if uerr != nil && !apierrors.IsNotFound(uerr) {
		log.WithError(uerr).Warn("identity restore: cannot count the refusal on the group record; its pods are kept and the refusal tried again")
		return append(pending, cold...), nil, remote, acted
	}
	for _, pod := range cold {
		if derr := c.a.deletePodRetrying(ctx, pod.Namespace, pod.Name, pod.UID); derr != nil {
			log.WithError(derr).WithField("pod", pod.Namespace+"/"+pod.Name).Error("identity restore: cannot delete the refused pod")
			pending = append(pending, pod)
			continue
		}
		c.releaseIdentity(ctx, key, pod, log)
		acted = true
	}
	return pending, nil, remote, acted
}

// deadRefusals returns, when record key's counted refusals reach their
// bound, those of them whose first pod is gone (or replaced) and none of
// whose pods are in progress: nothing will try them again, so their
// entries can go. Nil below the bound, or when it cannot be told.
func (c *criuAutoRestorer) deadRefusals(ctx context.Context, key string) map[string]bool {
	d, _, err := c.a.identityRecord(ctx, key)
	if err != nil || len(listed(d[identityRefusedPodsKey])) < identityRefusalListMax {
		return nil
	}
	live := map[string]bool{}
	for _, r := range refusing(d) {
		live[r] = true
	}
	dead := map[string]bool{}
	for _, rid := range listed(d[identityRefusedPodsKey]) {
		if live[rid] {
			continue
		}
		parts := strings.SplitN(rid, "/", 3)
		if len(parts) != 3 {
			dead[rid] = true // not a pod's name: nothing can name it again
			continue
		}
		p, gerr := c.a.kubeClient.CoreV1().Pods(parts[0]).Get(ctx, parts[1], metav1.GetOptions{})
		if apierrors.IsNotFound(gerr) || (gerr == nil && string(p.UID) != parts[2]) {
			dead[rid] = true
		}
	}
	return dead
}

// chargeRefusal counts on group record d one stall for refusal rid, whose
// pods that cannot start cold are deleted next (recordFailure). A refusal
// is counted once, whichever agent counts it first: the refusing agent,
// a retry of it, or a pod's own agent finishing it (considerRefusing),
// all name it by rid. A counted refusal's entry is never dropped to make
// room, only when it is dead (deadRefusals): past
// identityRefusalListMax entries, a new refusal is not counted (full),
// and its pods are not deleted, until entries go.
func chargeRefusal(d map[string]string, checkpoint, rid string, dead map[string]bool, cause error) (charged, full bool) {
	l := listed(d[identityRefusedPodsKey])
	l = slices.DeleteFunc(l, func(r string) bool { return dead[r] && r != rid })
	switch {
	case slices.Contains(l, rid):
		return false, false
	case len(l) >= identityRefusalListMax:
		return false, true
	case !recordFailure(d, checkpoint, false, cause):
		return false, false
	}
	setListed(d, identityRefusedPodsKey, append(l, rid))
	return true, false
}

// identityRecordVisible reports whether admission may restore from group
// record data d: a record with an identity only with the flag on, since
// only then is its identity granted again before it runs.
func (a *Agent) identityRecordVisible(key string, d map[string]string) bool {
	if a.identityEnabled() || !hasIdentity(d) {
		return true
	}
	// Admission asks for every pod: one decision per capture, counted
	// and logged once.
	if _, seen := a.identityDisabledSeen.LoadOrStore(key+"\x00"+d["checkpoint.0"], struct{}{}); !seen {
		observeIdentity(identityRefusedDisabled)
		a.log.WithField("criuGroup", key).Info("CRIU group restore: the capture holds an etcd identity and identity restore is off; cold start")
	}
	return false
}

// Call sites the identity restore wraps; tests stub them.
var (
	checkpointMember = (*Agent).Checkpoint
	restoreInstance  = (*Agent).groupRestore
)

// loneIdentityBeforeRestore is identityBeforeRestore for the lone restore
// of checkpoint id into placeholder ns/name. It returns the placeholder it
// read, for a refusal.
func (c *criuAutoRestorer) loneIdentityBeforeRestore(ctx, keep context.Context, key, ns, name string, uid types.UID, id string, log *logrus.Entry) (*leaseKeeper, *corev1.Pod, error) {
	if key == "" {
		return nil, nil, nil
	}
	if !c.a.identityEnabled() {
		// Only a record that holds an identity is refused; the slots are
		// not read for it.
		k, err := c.identityBeforeRestore(ctx, keep, key, nil, nil, log)
		return k, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}, err
	}
	pod, err := c.samePlaceholder(ctx, ns, name, uid, id)
	if err != nil {
		// Not this attempt's pod: nothing is claimed, and the refusal
		// finds the pod replaced and leaves it.
		return nil, nil, err
	}
	k, err := c.identityBeforeRestore(ctx, keep, key, []*corev1.Pod{pod}, pod, log)
	return k, pod, err
}

// samePlaceholder reads placeholder ns/name, and refuses unless it is
// still pod uid naming checkpoint id: the restore finds its target by name
// alone, and the identity was set up for that pod.
func (c *criuAutoRestorer) samePlaceholder(ctx context.Context, ns, name string, uid types.UID, id string) (*corev1.Pod, error) {
	pod, err := c.a.kubeClient.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, refuseIdentity(identityRefusedError, fmt.Errorf("read the placeholder: %w", err))
	}
	if pod.UID != uid || pod.Annotations[webhook.CRIURestoreAnnotation] != id {
		return nil, refuseIdentity(identityRefusedError, errors.New("the placeholder was replaced or no longer names this checkpoint"))
	}
	return pod, nil
}

// sharedRecheck is the shared-companions check again just before the
// hold: a capture that took its companions must not go on once another
// worker shares them, even though that worker could be captured alone.
func (c *criuAutoRestorer) sharedRecheck(ctx context.Context, leader *corev1.Pod, alone bool, log *logrus.Entry) (again, skip bool) {
	again, skip, nowAlone := c.sharedBlocksCapture(ctx, leader, nil, log)
	if !skip && nowAlone && !alone {
		log.Info("CRIU group capture: skipped; another worker now shares the companions taken with the instance")
		return false, true
	}
	return again, skip
}

// identityRecordAlone reports whether group record key holds an identity
// captured alone (no companions), with the flag on: what its namespace
// shares is not restored with it. An unreadable record is an error.
func (a *Agent) identityRecordAlone(ctx context.Context, key string) (bool, error) {
	if !a.identityEnabled() || key == "" {
		return false, nil
	}
	d, _, err := a.identityRecord(ctx, key)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read the group record: %w", err)
	}
	if !hasIdentity(d) {
		return false, nil
	}
	for k, v := range d {
		if strings.HasPrefix(k, "companion.") && v == "true" {
			return false, nil
		}
	}
	return true, nil
}
