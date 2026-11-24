// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// A capture being verified is verified only by a restore of it, in its
// own namespace, whose pods serve (restored_verify.go). When the namespace
// goes first (the function undeployed, before any restore of it or before
// one had served for restoredServedFor), nothing can verify it any more,
// and nothing said so: the record stayed "verifying", with no outcome
// counted and no summary line. The node reaper (reapNode) now marks such a
// record abandoned: once, by whichever agent updates it first, which
// counts the outcome (metrics.CaptureAbandoned) and writes the summary
// line. The record keeps its state, so admission and the next deployment's
// capture (groupRecaptures) treat it as before; nvsnap-server shows it as
// abandoned (fnstatus).

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/fnstatus"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/metrics"
)

// groupAbandonedKey is when a capture being verified was found with its
// namespace gone (fnstatus.AbandonedKey).
const groupAbandonedKey = fnstatus.AbandonedKey

// resolveAbandonedVerifies marks abandoned every capture being verified
// whose namespace is gone.
func (c *criuAutoRestorer) resolveAbandonedVerifies(ctx context.Context, now time.Time) {
	records, err := c.a.kubeClient.CoreV1().ConfigMaps(c.a.criuFailedNamespace()).List(ctx,
		metav1.ListOptions{LabelSelector: "app.kubernetes.io/managed-by=nvsnap"})
	if err != nil {
		c.a.log.WithError(err).Warn("CRIU capture: cannot list the group records to find abandoned verifications; trying on the next sweep")
		return
	}
	for i := range records.Items {
		r := &records.Items[i]
		key, ok := strings.CutPrefix(r.Name, criuGroupConfigMapPrefix)
		ns := r.Data["sourceNamespace"]
		if !ok || r.Data["state"] != criuGroupStateVerifying || ns == "" || r.Data[groupAbandonedKey] != "" {
			continue
		}
		if !c.namespaceGone(ctx, ns) {
			continue
		}
		c.abandonVerify(ctx, key, ns, now)
	}
}

// abandonVerify marks the capture of record key, being verified in
// namespace ns, abandoned, and records the outcome when this agent is the
// one that marked it.
func (c *criuAutoRestorer) abandonVerify(ctx context.Context, key, ns string, now time.Time) {
	var (
		marked bool
		data   map[string]string
	)
	reason := fmt.Sprintf("abandoned: namespace %s was removed before a restore of this capture served; "+
		"the configuration's next deployment captures it again", ns)
	if err := c.updateGroupRecord(ctx, key, func(d map[string]string) bool {
		marked = false
		if d["state"] != criuGroupStateVerifying || d["sourceNamespace"] != ns || d[groupAbandonedKey] != "" {
			return false // verified, captured again, or marked by another agent
		}
		d[groupAbandonedKey], d["reason"] = now.UTC().Format(time.RFC3339), reason
		marked = true
		data = make(map[string]string, len(d))
		for k, v := range d {
			data[k] = v
		}
		return true
	}); err != nil {
		c.a.log.WithError(err).WithField("criuGroup", key).Warn("CRIU capture: cannot mark an abandoned verification; trying on the next sweep")
		return
	}
	if marked {
		c.observeAbandoned(ctx, key, ns, data, now)
	}
}

// observeAbandoned records a capture abandoned before its verification:
// the outcome, and one summary line with what its record says of it.
func (c *criuAutoRestorer) observeAbandoned(ctx context.Context, key, ns string, d map[string]string, now time.Time) {
	gpu := c.a.nodeGPU(ctx, d["node.0"])
	ranks := recordRanks(d)
	metrics.ObserveCapture(metrics.MethodCRIU, metrics.CaptureAbandoned, "", gpu, ranks)
	fields := logrus.Fields{"criuGroup": key, "method": metrics.MethodCRIU, "outcome": metrics.CaptureAbandoned,
		"namespace": ns, "ranks": ranks, "gpu": gpu, "restores": len(strings.Fields(d["restores"])), "bundle": c.a.currentBundle()}
	if t, err := time.Parse(time.RFC3339, d["startedAt"]); err == nil {
		fields["seconds"] = int(now.Sub(t).Seconds())
	}
	c.a.log.WithFields(fields).Info(captureSummary)
}
