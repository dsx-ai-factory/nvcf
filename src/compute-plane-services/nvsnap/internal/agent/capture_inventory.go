// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// The capture-record inventory: what the cluster has cached for restores.
// One agent of the cluster, the holder of Lease captureInventoryLease in
// the records' namespace, scans the capture records every
// captureInventoryInterval and exports how many there are in each state
// and from which agent bundle, the oldest in each state, and the verified
// ones no restore has used. A capture being verified whose namespace went
// first counts as state "abandoned". One agent exports it, so the numbers are not
// multiplied by the node count. docs/operate/METRICS.md lists the series.

import (
	"context"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
)

const (
	captureInventoryLease    = "nvsnap-capture-inventory"
	captureInventoryInterval = time.Minute
	// The Lease timings: a new leader takes over within about a minute of
	// the old one going without releasing the Lease.
	captureInventoryLeaseDuration = 60 * time.Second
	captureInventoryRenewDeadline = 40 * time.Second
	captureInventoryRetryPeriod   = 10 * time.Second
	// captureRecordAbandoned is the inventory state of a capture being
	// verified whose namespace went first; its record stays "verifying".
	captureRecordAbandoned = "abandoned"
)

// startCaptureInventory runs for the inventory Lease until ctx ends, and
// exports the inventory while it holds it.
func (a *Agent) startCaptureInventory(ctx context.Context) {
	if a.kubeClient == nil || a.config.NodeName == "" {
		return
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: captureInventoryLease, Namespace: a.criuFailedNamespace()},
		Client:     a.kubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: a.config.NodeName},
	}
	inv := &captureInventory{}
	go func() {
		for ctx.Err() == nil {
			elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
				Lock:            lock,
				Name:            captureInventoryLease,
				LeaseDuration:   captureInventoryLeaseDuration,
				RenewDeadline:   captureInventoryRenewDeadline,
				RetryPeriod:     captureInventoryRetryPeriod,
				ReleaseOnCancel: true,
				Callbacks: leaderelection.LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) {
						a.log.Info("capture inventory: this agent exports the capture-record inventory")
						inv.lead()
						a.runCaptureInventory(ctx, inv)
					},
					// client-go runs this on its own goroutine, possibly while
					// a scan is still running: clear stops that scan's export.
					OnStoppedLeading: inv.clear,
				},
			})
			if err != nil {
				a.log.WithError(err).Error("capture inventory: cannot run for its Lease; no inventory from this agent")
				return
			}
			elector.Run(ctx) // returns when leadership is lost or ctx ends
			select {
			case <-ctx.Done():
			case <-time.After(captureInventoryRetryPeriod):
			}
		}
	}()
}

// runCaptureInventory scans the records until ctx ends.
func (a *Agent) runCaptureInventory(ctx context.Context, inv *captureInventory) {
	t := time.NewTicker(captureInventoryInterval)
	defer t.Stop()
	for {
		a.scanCaptureRecords(ctx, inv, time.Now())
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// scanCaptureRecords exports the inventory of the records as they are at
// now. A failed list keeps the last export; its scan time says how old it
// is.
func (a *Agent) scanCaptureRecords(ctx context.Context, inv *captureInventory, now time.Time) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cms, err := a.kubeClient.CoreV1().ConfigMaps(a.criuFailedNamespace()).List(ctx, metav1.ListOptions{ResourceVersion: "0"})
	if err != nil {
		a.log.WithError(err).Warn("capture inventory: cannot list the capture records; the last inventory stays")
		return
	}
	inv.export(tallyCaptureRecords(cms.Items, now), now)
}

// recordTally is the inventory of a set of capture records.
type recordTally struct {
	records    map[[2]string]int        // {state, bundle} -> records
	oldest     map[string]time.Duration // state -> age of its oldest record
	unrestored int                      // complete, and not restored since verified
}

// tallyCaptureRecords counts the capture records among cms at now.
func tallyCaptureRecords(cms []corev1.ConfigMap, now time.Time) recordTally {
	t := recordTally{records: map[[2]string]int{}, oldest: map[string]time.Duration{}}
	for i := range cms {
		cm := &cms[i]
		if !strings.HasPrefix(cm.Name, criuGroupConfigMapPrefix) {
			continue
		}
		d := cm.Data
		state := d["state"]
		switch {
		case state == "":
			state = "unknown"
		case state == criuGroupStateVerifying && d[groupAbandonedKey] != "":
			// Its namespace went before a restore of it served
			// (verify_abandoned.go): nothing will verify it.
			state = captureRecordAbandoned
		}
		bundle := d[groupAgentBundleKey]
		if bundle == "" {
			bundle = "unknown"
		}
		if state == criuGroupStateAlias {
			bundle = "" // an alias names a record; it has no capture of its own
		}
		t.records[[2]string{state, bundle}]++
		started := cm.CreationTimestamp.Time
		if s, err := time.Parse(time.RFC3339, d["startedAt"]); err == nil {
			started = s
		}
		if age := now.Sub(started); age > t.oldest[state] {
			t.oldest[state] = age
		}
		if state == criuGroupStateComplete && !restoredSinceVerified(d) {
			t.unrestored++
		}
	}
	return t
}

// restoredSinceVerified reports whether a complete record's restore
// history has a restore after it was verified. A record without a
// verification time (from an agent before it was kept) counts as
// restored: it cannot be told apart.
func restoredSinceVerified(d map[string]string) bool {
	verified, err := time.Parse(time.RFC3339, d["verifiedAt"])
	if err != nil {
		return true
	}
	for _, e := range strings.Fields(d[groupRestoresKey]) {
		at, _, _ := strings.Cut(e, "=")
		if t, err := time.Parse(time.RFC3339, at); err == nil && t.After(verified) {
			return true
		}
	}
	return false
}

// captureInventory is the exported inventory: it removes the series of
// states and bundles no longer present instead of resetting the gauges,
// so a scrape never sees the inventory empty mid-update. It exports only
// while this agent leads: a scan that ends after leadership was lost
// exports nothing.
type captureInventory struct {
	mu      sync.Mutex
	leading bool
	records map[[2]string]bool
	states  map[string]bool
}

// lead starts this agent's export.
func (inv *captureInventory) lead() {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	inv.leading = true
	metrics.InventoryLeader.Set(1)
}

// export publishes tally t, scanned at now, while this agent leads.
func (inv *captureInventory) export(t recordTally, now time.Time) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if !inv.leading {
		return
	}
	records, states := map[[2]string]bool{}, map[string]bool{}
	for k, n := range t.records {
		metrics.CaptureRecords.WithLabelValues(k[0], k[1]).Set(float64(n))
		records[k] = true
	}
	for s, age := range t.oldest {
		metrics.CaptureRecordOldestSeconds.WithLabelValues(s).Set(age.Seconds())
		states[s] = true
	}
	for k := range inv.records {
		if !records[k] {
			metrics.CaptureRecords.DeleteLabelValues(k[0], k[1])
		}
	}
	for s := range inv.states {
		if !states[s] {
			metrics.CaptureRecordOldestSeconds.DeleteLabelValues(s)
		}
	}
	metrics.CaptureRecordsUnrestored.Set(float64(t.unrestored))
	metrics.InventoryScanTimestamp.Set(float64(now.Unix()))
	inv.records, inv.states = records, states
}

// clear stops this agent's export and drops what it exported: another
// agent exports the inventory now.
func (inv *captureInventory) clear() {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	inv.leading = false
	metrics.InventoryLeader.Set(0)
	metrics.CaptureRecords.Reset()
	metrics.CaptureRecordOldestSeconds.Reset()
	metrics.CaptureRecordsUnrestored.Set(0)
	metrics.InventoryScanTimestamp.Set(0)
	inv.records, inv.states = nil, nil
}
