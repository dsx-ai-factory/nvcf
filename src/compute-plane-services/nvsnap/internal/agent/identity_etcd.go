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
	// identityHolderKey names the restore that holds the identity
	// ("namespace/pod/uid"): one live restore per identity.
	identityHolderKey = "identity.holder"
	// identityGrantedKey names the holder whose restore had the lease
	// granted: only it may adopt a live lease after an agent restart.
	identityGrantedKey = "identity.granted"
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
	return nil
}

// hasIdentity reports group record data that holds an identity, whole or
// not.
func hasIdentity(d map[string]string) bool {
	for k := range d {
		if strings.HasPrefix(k, "identity.") && k != identityHolderKey && k != identityGrantedKey {
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

// urlHostForms are the forms in which a value names ip as a URL host, the
// only ones a restore rewrites.
func urlHostForms(ip string) []string {
	if strings.Contains(ip, ":") {
		return []string{"//[" + ip + "]:", "//[" + ip + "]/"}
	}
	return []string{"//" + ip + ":", "//" + ip + "/"}
}

// rewriteHost replaces every URL-host form of old in v with new.
func rewriteHost(v []byte, old, nw string) []byte {
	s := string(v)
	for i, f := range urlHostForms(old) {
		s = strings.ReplaceAll(s, f, urlHostForms(nw)[i])
	}
	return []byte(s)
}

// strayAddress reports whether v names any of ips other than as the
// holder's URL host, which a restore could not rewrite.
func strayAddress(v []byte, holder string, ips []string) bool {
	s := string(v)
	if holder != "" {
		for _, f := range urlHostForms(holder) {
			s = strings.ReplaceAll(s, f, "//")
		}
	}
	for _, ip := range ips {
		if ip != "" && ipToken(ip).MatchString(s) {
			return true
		}
	}
	return false
}

// mentions reports whether v names ip as a URL host.
func mentions(v []byte, ip string) bool {
	for _, f := range urlHostForms(ip) {
		if strings.Contains(string(v), f) {
			return true
		}
	}
	return false
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
// and is not being deleted.
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
	return p.UID == types.UID(parts[2]) && p.DeletionTimestamp == nil, nil
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
	for _, k := range id.Keys {
		owned, err := e.PutOwned(ctx, k.Key, rewriteHost(k.Value, id.PodIP, newIP), id.Lease)
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
	// failed; onZombie ends a restored pod whose identity was lost.
	release, onAborted, onZombie func()
	writeKeys                    func(context.Context) error

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
// lease whenever a quarter of its TTL is left. Once it resumed, the keeper
// writes the identity's keys (the frontend routes to the worker only then),
// and watches the lease's TTL against what its own last renewal explains.
// Held above that for a whole TTL, the lease is renewed by someone else
// all along: the process's keep-alive has taken over, and it stops. One
// renewal not its own (etcd renews every lease on a leader change) keeps
// it above for under three quarters of a TTL, after which the keeper
// renews it again; samples need not rise between polls. It fails closed: a process whose keep-alive is not
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
		// A sample is taken as of its request: a late reply then tells of
		// a lease that may have expired since, never of one renewed later.
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
					above = at
				}
			} else {
				above = time.Time{}
			}
			if !above.IsZero() && at.Sub(above) >= ttl {
				k.log.WithField("waited", time.Since(deadline.Add(-handoffWait)).Round(time.Second).String()).
					Info("identity restore: the restored process keeps its lease alive; the agent stops keeping it")
				end(identityHandoffSeen, false, nil)
				return
			}
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			k.log.WithField("waited", handoffWait.String()).Error("identity restore: the restored process never kept its lease alive; revoking it and ending its pod")
			end(identityHandoffTimeout, true, k.onZombie)
			return
		}
		if err != nil || time.Duration(info.TTL)*time.Second <= ttl/4 {
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
		if apierrors.IsNotFound(err) || !c.a.identityEnabled() {
			return nil, nil // the restore decides as it always has
		}
		return nil, refuseIdentity(identityRefusedError, fmt.Errorf("read the group record: %w", err))
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
		if merr := c.markGranted(ctx, key, holder); merr != nil {
			_ = e.Revoke(context.WithoutCancel(ctx), id.Lease)
			closeFn()
			release()
			return nil, refuseIdentity(identityRefusedError, fmt.Errorf("record the grant: %w", merr))
		}
		observeIdentity(identityRegranted)
		log.Info("identity restore: the captured lease is granted again; restoring")
	} else {
		log.Info("identity restore: this pod already holds the identity; keeping its lease alive")
	}
	k := newLeaseKeeper(e, id.Lease, id.TTL, log)
	k.release = closeFn
	k.writeKeys = func(ctx context.Context) error { return writeIdentityKeys(ctx, e, id, newIP) }
	k.onAborted = func() { c.releaseIdentity(context.WithoutCancel(ctx), key, holder, log) }
	k.onZombie = func() {
		// The claim stays until the pod is gone (holderAlive).
		if derr := c.a.deletePodRetrying(context.WithoutCancel(keep), holder.Namespace, holder.Name, holder.UID); derr != nil {
			log.WithError(derr).Error("identity restore: cannot delete the pod that lost its identity")
		}
	}
	go k.run(keep)
	return k, nil
}

// identityRefused handles the pods (by slot) of a restore the identity
// restore refused (settleRefused). When a pod's refusal is left unfinished,
// or a pod kept for the workload it runs was refused for an error that may
// pass (an adopted lease that could not be read), and nothing was done to
// any pod, again hands the refusal to the restore's next event, which
// decides it anew. Once a pod has started cold or been deleted the restore
// cannot run again (its leader may be gone), so each pod left unfinished is
// finished on its own next event instead (considerIdentityRefused).
func (c *criuAutoRestorer) identityRefused(ctx context.Context, key, checkpoint string, pods []*corev1.Pod, err error, again func(), log *logrus.Entry) {
	decision := identityDecision(err)
	observeIdentity(decision)
	log = log.WithError(err).WithField("decision", decision)
	pending, running, acted := c.settleRefused(ctx, key, checkpoint, pods, err, log)
	if len(pending) == 0 && (len(running) == 0 || decision != identityRefusedError) {
		return
	}
	if !acted && again != nil {
		again()
		return
	}
	for _, p := range pending {
		c.attempted.Store(identityRetryKey(p.UID), &identityRefusalRetry{key: key, checkpoint: checkpoint, pod: p, err: err, log: log})
	}
}

// identityRetryKey is the attempted key under which a pod's unfinished
// refusal waits for the pod's next event.
func identityRetryKey(uid types.UID) string { return "identity-refused/" + string(uid) }

// identityRefusalRetry is a pod's unfinished refusal.
type identityRefusalRetry struct {
	key, checkpoint string
	pod             *corev1.Pod // as refused: its UID and restore annotation
	err             error
	log             *logrus.Entry
}

// considerIdentityRefused finishes, on pod's event, a refusal
// identityRefused left unfinished for it; it waits again for the next
// event while it stays unfinished.
func (c *criuAutoRestorer) considerIdentityRefused(ctx context.Context, pod *corev1.Pod) {
	v, ok := c.attempted.LoadAndDelete(identityRetryKey(pod.UID))
	if !ok {
		return
	}
	r := v.(*identityRefusalRetry)
	go func() {
		if pending, _, _ := c.settleRefused(context.WithoutCancel(ctx), r.key, r.checkpoint, []*corev1.Pod{r.pod}, r.err, r.log); len(pending) > 0 {
			c.attempted.Store(identityRetryKey(r.pod.UID), r)
		}
	}()
}

// podRunsWorkload reports whether pod's container runs a GPU workload,
// as a restore would refuse it (errNotPlaceholder): a restored engine an
// earlier agent left. Without a runtime client nothing on the node can be
// inspected, nor started cold, so it reports none. Tests replace it.
var podRunsWorkload = func(ctx context.Context, a *Agent, pod *corev1.Pod, container string) (bool, error) {
	if a.runtime == nil {
		return false, nil
	}
	info, err := a.runtime.FindContainerByPod(ctx, pod.Namespace, pod.Name, container)
	if err != nil {
		return false, err
	}
	if info == nil || info.PID == 0 {
		return false, nil
	}
	pid, err := a.gpuProcessInSamePidNS(ctx, hostProcBase(), int(info.PID))
	return pid != 0, err
}

// settleRefused acts on each of pods (as refused, by UID and restore
// annotation) for a refused restore, nothing counted against the
// capture:
//
//   - a pod gone, replaced, or that no longer names its refused
//     checkpoint is left as it is;
//   - a pod that runs a workload already (podRunsWorkload) is kept as it
//     is (running): the refusal neither replaces nor deletes it. When that
//     cannot be told, the pod is handled as one that does not (it starts
//     cold);
//   - any other pod starts its workload cold in place. One that cannot is
//     deleted, for its controller to replace, only once record key counts
//     a stall for the refusal (chargeRefusal), so the replacements,
//     admitted at once and refused the same way, end at
//     criuGroupMaxStalls.
//
// A pod that cannot be read, or deleted, or whose stall cannot be counted,
// is returned unfinished (pending), and nothing is deleted then. acted
// reports whether a pod started cold or was deleted.
func (c *criuAutoRestorer) settleRefused(ctx context.Context, key, checkpoint string, pods []*corev1.Pod, err error, log *logrus.Entry) (pending, running []*corev1.Pod, acted bool) {
	var cold []*corev1.Pod
	for _, p := range pods {
		pl := log.WithField("pod", p.Namespace+"/"+p.Name)
		var pod *corev1.Pod
		gerr := retry.OnError(retry.DefaultBackoff, func(e error) bool { return !apierrors.IsNotFound(e) }, func() error {
			var e error
			pod, e = c.a.kubeClient.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
			return e
		})
		if apierrors.IsNotFound(gerr) || (gerr == nil && (pod.UID != p.UID ||
			pod.Annotations[webhook.CRIURestoreAnnotation] != p.Annotations[webhook.CRIURestoreAnnotation])) {
			pl.Warn("identity restore refused; the pod is gone, replaced, or no longer names the refused checkpoint: left as it is")
			continue
		}
		if gerr != nil {
			pl.WithError(gerr).Warn("identity restore refused, and the pod cannot be read; trying again on its next event")
			pending = append(pending, p)
			continue
		}
		container := pod.Annotations[webhook.CRIURestoreContainerAnnotation]
		runs, rerr := podRunsWorkload(ctx, c.a, pod, container)
		if rerr != nil {
			pl.WithError(rerr).Warn("identity restore refused, and whether the pod runs a workload cannot be told; it starts cold")
		}
		if runs && rerr == nil {
			pl.Warn("identity restore refused, and the pod already runs its workload: kept as it is")
			running = append(running, p)
			continue
		}
		cerr := c.a.coldStartInPlace(ctx, pod, container, pl)
		if cerr == nil {
			pl.Warn("identity restore refused; the pod starts cold in place")
			acted = true
			continue
		}
		pl.WithField("coldStart", cerr.Error()).Warn("identity restore refused, and the pod cannot start cold in place")
		cold = append(cold, pod)
	}
	if len(cold) == 0 {
		return pending, running, acted
	}
	var uerr error
	if key != "" {
		uerr = c.updateGroupRecord(ctx, key, func(d map[string]string) bool { return chargeRefusal(d, checkpoint, cold, pods, err) })
	}
	if uerr != nil && !apierrors.IsNotFound(uerr) {
		log.WithError(uerr).Warn("identity restore: cannot count the refusal on the group record; its pods are kept and the refusal tried again")
		return append(pending, cold...), running, acted
	}
	for _, pod := range cold {
		if derr := c.a.deletePodRetrying(ctx, pod.Namespace, pod.Name, pod.UID); derr != nil {
			log.WithError(derr).WithField("pod", pod.Namespace+"/"+pod.Name).Error("identity restore: cannot delete the refused pod")
			pending = append(pending, pod)
			continue
		}
		acted = true
	}
	return pending, running, acted
}

// identityRefusedPodsKey lists, on a group record, the UIDs of the last
// pods refusals counted a stall for (at most identityRefusedPodsMax).
const (
	identityRefusedPodsKey = "refusedPods"
	identityRefusedPodsMax = 32
)

// chargeRefusal counts on group record d one stall for the refusal of all,
// whose pods cold are deleted next (recordFailure). A refusal is counted
// once: when every pod of cold is listed already (the refusal tried again
// after a delete failed, or a rank that could not be read before), nothing
// is counted again.
func chargeRefusal(d map[string]string, checkpoint string, cold, all []*corev1.Pod, cause error) bool {
	var charged []string
	if v := d[identityRefusedPodsKey]; v != "" {
		charged = strings.Split(v, ",")
	}
	fresh := false
	for _, p := range cold {
		fresh = fresh || !slices.Contains(charged, string(p.UID))
	}
	if !fresh || !recordFailure(d, checkpoint, false, cause) {
		return false
	}
	for _, p := range all {
		if !slices.Contains(charged, string(p.UID)) {
			charged = append(charged, string(p.UID))
		}
	}
	if n := len(charged); n > identityRefusedPodsMax {
		charged = charged[n-identityRefusedPodsMax:]
	}
	d[identityRefusedPodsKey] = strings.Join(charged, ",")
	return true
}

// identityRecordVisible reports whether admission may restore from group
// record data d: a record with an identity only with the flag on, since
// only then is its identity granted again before it runs.
func (a *Agent) identityRecordVisible(key string, d map[string]string) bool {
	if a.identityEnabled() || !hasIdentity(d) {
		return true
	}
	observeIdentity(identityRefusedDisabled)
	a.log.WithField("criuGroup", key).Info("CRIU group restore: the capture holds an etcd identity and identity restore is off; cold start")
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
