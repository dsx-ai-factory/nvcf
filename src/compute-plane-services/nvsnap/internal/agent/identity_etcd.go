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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

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
	return nil
}

// hasIdentity reports group record data that holds an identity, whole or
// not.
func hasIdentity(d map[string]string) bool {
	for k := range d {
		if strings.HasPrefix(k, "identity.") && k != identityHolderKey {
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
	return id, nil
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
		return nil, err.Error()
	}
	if !webhook.DynamoComponent(ranks[id.Member]) {
		return nil, "the lease is held by a rank that is not a Dynamo component"
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
	key := pod.Annotations[webhook.CRIUGroupAnnotation]
	if !a.identityEnabled() || key == "" || pod.Annotations[webhook.CRIURestoreAnnotation] == "" {
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

// releaseIdentity clears pod's claim on record key's identity.
func (c *criuAutoRestorer) releaseIdentity(ctx context.Context, key string, pod *corev1.Pod, log *logrus.Entry) {
	me := holderOf(pod)
	if err := c.updateGroupRecord(ctx, key, func(d map[string]string) bool {
		if d[identityHolderKey] != me {
			return false
		}
		delete(d, identityHolderKey)
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

// regrant grants lease id again with the recorded TTL and writes the
// identity's keys with the captured address replaced by newIP. Any failure
// after the grant revokes it, so no half-written identity stays.
func regrant(ctx context.Context, e identityEtcd, id *capturedIdentity, newIP string) error {
	if err := e.Grant(ctx, id.Lease, id.TTL); err != nil {
		if errors.Is(err, errLeaseExists) {
			return refuseIdentity(identityRefusedLiveLease, errors.New("etcd refused the grant: the lease is live"))
		}
		return refuseIdentity(identityRefusedError, fmt.Errorf("grant the lease: %w", err))
	}
	for _, k := range id.Keys {
		if err := e.Put(ctx, k.Key, rewriteHost(k.Value, id.PodIP, newIP), id.Lease); err != nil {
			_ = e.Revoke(context.WithoutCancel(ctx), id.Lease)
			return refuseIdentity(identityRefusedError, fmt.Errorf("write the identity's keys: %w", err))
		}
	}
	return nil
}

// leaseKeeper keeps a regranted lease alive from the agent until the
// restored process's own keep-alive is seen.
type leaseKeeper struct {
	e        identityEtcd
	id, ttl  int64
	log      *logrus.Entry
	release  func()
	onFailed func()

	once    sync.Once
	resumed chan struct{}
	aborted chan struct{}
	done    chan struct{}
	outcome string
}

func newLeaseKeeper(e identityEtcd, id, ttl int64, release, onFailed func(), log *logrus.Entry) *leaseKeeper {
	return &leaseKeeper{e: e, id: id, ttl: ttl, log: log, release: release, onFailed: onFailed,
		resumed: make(chan struct{}), aborted: make(chan struct{}), done: make(chan struct{})}
}

// restored tells the keeper how the restore ended: resumed (err nil), the
// wait for the process's keep-alive starts; failed, the lease is revoked.
// Nil-safe; only the first call counts.
func (k *leaseKeeper) restored(err error) {
	if k == nil {
		return
	}
	k.once.Do(func() {
		if err == nil {
			close(k.resumed)
		} else {
			close(k.aborted)
		}
	})
}

// run keeps the lease alive. Before the process resumes it renews the
// lease whenever a quarter of its TTL is left. After, it watches for a
// renewal it did not make (the lease's TTL above what its own last renewal
// explains): the process's keep-alive has taken over, and it stops. It
// fails closed: a process whose keep-alive is not seen within
// max(3 TTL, identityHandoffFloor) has its lease revoked, so the frontend
// stops routing to a worker that does not keep its registration.
func (k *leaseKeeper) run(ctx context.Context) {
	defer close(k.done)
	defer k.release()
	ttl := time.Duration(k.ttl) * time.Second
	handoffWait := max(3*ttl, identityHandoffFloor)
	own, ownTTL := time.Now(), ttl
	var deadline time.Time
	resumed := k.resumed
	end := func(outcome string, revoke bool) {
		k.outcome = outcome
		observeIdentity(outcome)
		if revoke {
			if err := k.e.Revoke(context.WithoutCancel(ctx), k.id); err != nil {
				k.log.WithError(err).Warn("identity restore: cannot revoke the lease; it expires on its own")
			}
		}
		if outcome != identityHandoffSeen && k.onFailed != nil {
			k.onFailed()
		}
	}
	for {
		select {
		case <-ctx.Done():
			k.log.Warn("identity restore: the agent stops; the lease is no longer kept alive by it")
			end(identityKeeperAgentStopped, false)
			return
		case <-k.aborted:
			k.log.Warn("identity restore: the restore failed; revoking the regranted lease")
			end(identityRestoreAborted, true)
			return
		case <-resumed:
			resumed = nil
			deadline = time.Now().Add(handoffWait)
		case <-time.After(identityPoll):
		}
		info, err := k.e.TimeToLive(ctx, k.id, false)
		switch {
		case err != nil:
			k.log.WithError(err).Warn("identity restore: cannot read the lease; keeping it alive")
		case info.TTL < 0:
			k.log.Error("identity restore: the lease expired before the restored process kept it alive")
			end(identityHandoffLost, false)
			return
		case !deadline.IsZero() && time.Duration(info.TTL)*time.Second > ownTTL-time.Since(own)+identityTolerance:
			k.log.WithField("waited", time.Since(deadline.Add(-handoffWait)).Round(time.Second).String()).
				Info("identity restore: the restored process keeps its lease alive; the agent stops keeping it")
			end(identityHandoffSeen, false)
			return
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			k.log.WithField("waited", handoffWait.String()).Error("identity restore: the restored process never kept its lease alive; revoking it")
			end(identityHandoffTimeout, true)
			return
		}
		if err != nil || time.Duration(info.TTL)*time.Second <= ttl/4 {
			got, kerr := k.e.KeepAlive(ctx, k.id)
			switch {
			case kerr != nil:
				k.log.WithError(kerr).Warn("identity restore: keep-alive failed; trying again")
			case got <= 0:
				k.log.Error("identity restore: etcd no longer holds the lease")
				end(identityHandoffLost, false)
				return
			default:
				own, ownTTL = time.Now(), time.Duration(got)*time.Second
			}
		}
	}
}

// identityBeforeRestore makes the identity of record key valid for its
// restore into slots (placeholders by slot), before anything is restored:
// claims it, waits for the captured lease to be free, grants it again and
// writes its keys with the new address of the lease holder's slot. via is
// a placeholder on this node the registry is reached from. It returns a
// running keeper to tell the restore's outcome (restored), or nil for a
// record without an identity. A refusal (errIdentityRefused) restored
// nothing and holds nothing.
func (c *criuAutoRestorer) identityBeforeRestore(ctx context.Context, key string, slots []*corev1.Pod, via *corev1.Pod, log *logrus.Entry) (*leaseKeeper, error) {
	if !c.a.identityEnabled() || key == "" {
		// With the flag off nothing here runs: admission never makes a
		// placeholder of a record with an identity then (lookupCRIUGroup).
		return nil, nil
	}
	d, key, err := c.a.identityRecord(ctx, key)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil // no record: the restore decides as it always has
		}
		return nil, refuseIdentity(identityRefusedError, fmt.Errorf("read the group record: %w", err))
	}
	if !hasIdentity(d) {
		return nil, nil
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
	if cerr := c.claimIdentity(ctx, key, holder); cerr != nil {
		return nil, cerr
	}
	release := func() { c.releaseIdentity(context.WithoutCancel(ctx), key, holder, log) }
	e, closeFn, err := newIdentityEtcd(ctx, c.a, id.Endpoints, via.Namespace, via, via.Annotations[webhook.CRIURestoreContainerAnnotation])
	if err != nil {
		release()
		return nil, refuseIdentity(identityRefusedError, fmt.Errorf("reach the registry: %w", err))
	}
	if err := awaitLeaseFree(ctx, e, id.Lease, id.TTL); err != nil {
		closeFn()
		release()
		return nil, err
	}
	if err := regrant(ctx, e, id, newIP); err != nil {
		closeFn()
		release()
		return nil, err
	}
	observeIdentity(identityRegranted)
	log.Info("identity restore: the captured lease is granted again and its keys written with the new address; restoring")
	k := newLeaseKeeper(e, id.Lease, id.TTL, closeFn, release, log)
	go k.run(context.WithoutCancel(ctx))
	return k, nil
}

// identityRefused handles a lone restore the identity restore refused: the
// pod starts its workload cold in place, nothing counted against the
// capture; a pod that cannot is deleted, for its controller to replace.
func (c *criuAutoRestorer) identityRefused(ctx context.Context, ns, name string, uid types.UID, container string, err error, log *logrus.Entry) {
	decision := identityDecision(err)
	observeIdentity(decision)
	log = log.WithError(err).WithField("decision", decision)
	pod, gerr := c.a.kubeClient.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if gerr != nil || pod.UID != uid {
		log.Warn("identity restore refused; the pod is gone or replaced")
		return
	}
	cerr := c.a.coldStartInPlace(ctx, pod, container, log)
	if cerr == nil {
		log.Warn("identity restore refused; the pod starts cold in place")
		return
	}
	log.WithField("coldStart", cerr.Error()).Warn("identity restore refused, and the pod cannot start cold in place; deleting it")
	if derr := c.a.deletePodRetrying(ctx, ns, name, uid); derr != nil {
		log.WithError(derr).Error("identity restore: cannot delete the refused pod")
	}
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
	restoreLone      = (*Agent).restoreProbed
	restoreInstance  = (*Agent).groupRestore
)

// loneIdentityBeforeRestore is identityBeforeRestore for the lone restore
// of checkpoint id into placeholder ns/name.
func (c *criuAutoRestorer) loneIdentityBeforeRestore(ctx context.Context, key, ns, name, id string, log *logrus.Entry) (*leaseKeeper, error) {
	if !c.a.identityEnabled() || key == "" {
		return nil, nil
	}
	pod, err := c.a.kubeClient.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, refuseIdentity(identityRefusedError, fmt.Errorf("read the placeholder: %w", err))
	}
	if pod.Annotations[webhook.CRIURestoreAnnotation] != id {
		return nil, refuseIdentity(identityRefusedError, errors.New("the placeholder no longer names this checkpoint"))
	}
	return c.identityBeforeRestore(ctx, key, []*corev1.Pod{pod}, pod, log)
}
