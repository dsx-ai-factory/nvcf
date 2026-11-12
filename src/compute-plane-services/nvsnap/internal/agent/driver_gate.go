// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

// The CRIU driver gate (docs/design/CRIU-CAPTURE-FALLBACK.md §2.8).
//
// A driver older than CRIUMinDriverMajor cannot restore every workload:
// driver 580 cannot restore multicast (NVLS) objects, which tensor-parallel
// engines on NVSwitch systems use (docs/design/GPUSHARE.md, Requirements
// and limits). And a capture is restored only under the driver major it
// was taken with.
//
// Each agent reads its node's driver when it starts and stamps the major
// on its own pod (DriverMajorAnnotation). Admission does not know which
// node a pod will run on, so it goes by every GPU node's: CRIU serves a
// configuration only when every agent that reports a driver reports the
// same major, and that major is at least the minimum. Otherwise a GPU pod
// falls back to the cachedir (reason "driver"), a companion starts cold,
// and nothing is captured with CRIU. The major is part of the group record
// key (criuGroupKeyDriver) and is kept in the record, so a capture is
// never restored under another driver. With no agent reporting a driver
// (an agent build before this one), nothing changes.

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// DriverMajorAnnotation is the NVIDIA driver major of the agent's
	// node, on the agent's own pod.
	DriverMajorAnnotation = "nvsnap.io/driver-major"

	// DefaultCRIUMinDriverMajor is the oldest driver major CRIU serves:
	// 610 restores multicast objects, 580 does not.
	DefaultCRIUMinDriverMajor = 610

	// recordDriverKey is the driver major a group record was captured
	// under.
	recordDriverKey = "driver"

	driverViewTTL = time.Minute
)

// Variables for tests.
var (
	nvidiaVersionFile = "/proc/driver/nvidia/version"
	serviceAccountNS  = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	driverReportRetry = 30 * time.Second
)

// "NVRM version: NVIDIA UNIX Open Kernel Module for aarch64  610.57.04  Release Build ..."
var nvrmVersion = regexp.MustCompile(`^NVRM version:.*?\s(\d{3,})\.\d+(?:\.\d+)?\s`)

// parseDriverMajor is the driver major in the first line of
// /proc/driver/nvidia/version; 0 when it has none.
func parseDriverMajor(text string) int {
	line, _, _ := strings.Cut(text, "\n")
	m := nvrmVersion.FindStringSubmatch(line + " ")
	if m == nil {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

// nodeDriverMajor is the driver major of the agent's node: from the
// kernel module, else from the GPU feature labels on its node; 0 when it
// has none. gpuNode reports a node with GPUs (capacity or the GPU feature
// label), whose driver may not be loaded yet. err is a failed node read.
func (a *Agent) nodeDriverMajor(ctx context.Context) (major int, gpuNode bool, err error) {
	if b, rerr := os.ReadFile(nvidiaVersionFile); rerr == nil {
		if n := parseDriverMajor(string(b)); n > 0 {
			a.ownDriver.Store(int64(n))
			return n, true, nil
		}
	}
	if a.kubeClient == nil || a.config.NodeName == "" {
		return 0, false, nil
	}
	node, err := a.kubeClient.CoreV1().Nodes().Get(ctx, a.config.NodeName, metav1.GetOptions{})
	if err != nil {
		return 0, false, err
	}
	gpus := node.Status.Capacity[corev1.ResourceName("nvidia.com/gpu")]
	// The labels the chart's agent affinity selects GPU nodes by.
	gpuNode = !gpus.IsZero() || node.Labels["nvidia.com/gpu.present"] == "true" ||
		node.Labels["cloud.google.com/gke-gpu"] == "true" || node.Labels["feature.node.kubernetes.io/pci-10de.present"] == "true"
	n, _ := strconv.Atoi(node.Labels["nvidia.com/cuda.driver.major"])
	if n > 0 {
		a.ownDriver.Store(int64(n))
		gpuNode = true
	}
	return n, gpuNode, nil
}

// agentNamespace is the namespace the agents run in.
func (a *Agent) agentNamespace() string {
	if b, err := os.ReadFile(serviceAccountNS); err == nil {
		if ns := strings.TrimSpace(string(b)); ns != "" {
			return ns
		}
	}
	return a.criuFailedNamespace()
}

// startDriverReport stamps the node's driver major on the agent's own pod,
// trying again until it has.
func (a *Agent) startDriverReport(ctx context.Context) {
	if a.kubeClient == nil || a.config.NodeName == "" {
		return
	}
	go func() {
		for {
			done, err := a.reportDriver(ctx)
			if done {
				return
			}
			a.log.WithError(err).Warn("driver report: cannot stamp the node's driver on this agent's pod; trying again")
			select {
			case <-ctx.Done():
				return
			case <-time.After(driverReportRetry):
			}
		}
	}()
}

// reportDriver stamps the node's driver major on the agent's pod. done is
// true once it has, or when there is nothing to stamp.
func (a *Agent) reportDriver(ctx context.Context) (done bool, err error) {
	major, gpuNode, err := a.nodeDriverMajor(ctx)
	switch {
	case err != nil:
		return false, err
	case major == 0 && gpuNode:
		// Its driver is not loaded yet (the agent can start before the
		// GPU operator): a GPU node must report, or the gate misses it.
		return false, fmt.Errorf("node %s has GPUs but no driver loaded yet", a.config.NodeName)
	case major == 0:
		a.log.Info("driver report: no GPUs on this node; this agent reports none")
		return true, nil
	}
	pods := a.kubeClient.CoreV1().Pods(a.agentNamespace())
	list, err := pods.List(ctx, metav1.ListOptions{LabelSelector: agentLabelSelector, FieldSelector: "spec.nodeName=" + a.config.NodeName})
	if err != nil {
		return false, err
	}
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, DriverMajorAnnotation, strconv.Itoa(major))
	stamped := 0
	for i := range list.Items {
		p := &list.Items[i]
		if p.DeletionTimestamp != nil || p.Spec.NodeName != a.config.NodeName {
			continue
		}
		if p.Annotations[DriverMajorAnnotation] != strconv.Itoa(major) {
			if _, err := pods.Patch(ctx, p.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
				return false, err
			}
		}
		stamped++
	}
	if stamped == 0 {
		return false, fmt.Errorf("no agent pod on node %s", a.config.NodeName)
	}
	a.log.WithField("driverMajor", major).Info("driver report: stamped this node's driver on the agent's pod")
	return true, nil
}

// clusterDriver is the drivers the agents report.
type clusterDriver struct {
	// Majors are the distinct majors reported, ascending.
	Majors []int
}

// Major is the one major every reporting agent has; 0 when none report
// or they differ. It keys the group records.
func (d clusterDriver) Major() int {
	if len(d.Majors) != 1 {
		return 0
	}
	return d.Majors[0]
}

// Gate is why the drivers keep CRIU from serving, "" when they do not:
// several majors, or one older than min. No reports keep nothing.
func (d clusterDriver) Gate(min int) string {
	switch {
	case len(d.Majors) == 0:
		return ""
	case len(d.Majors) > 1:
		s := make([]string, len(d.Majors))
		for i, m := range d.Majors {
			s[i] = strconv.Itoa(m)
		}
		return "the GPU nodes run driver majors " + strings.Join(s, ", ") + "; a CRIU capture restores only under the driver it was taken with"
	case d.Majors[0] < min:
		return fmt.Sprintf("the GPU nodes run driver %d; CRIU needs %d or later", d.Majors[0], min)
	}
	return ""
}

// driverView caches the cluster's drivers for driverViewTTL.
type driverView struct {
	mu       sync.Mutex
	at       time.Time
	failedAt time.Time
	view     clusterDriver
	ok       bool
}

// clusterDriver returns the drivers the agents report, cached for
// driverViewTTL. On a failed read it returns the last view read, else
// none (nothing gated, keys as before).
func (a *Agent) clusterDriver(ctx context.Context) clusterDriver {
	v := &a.drivers
	v.mu.Lock()
	if (v.ok || !v.failedAt.IsZero()) && time.Since(maxTime(v.at, v.failedAt)) < driverViewTTL {
		view := v.view
		v.mu.Unlock()
		return a.withOwnDriver(view)
	}
	v.mu.Unlock()
	if a.kubeClient == nil {
		return a.withOwnDriver(clusterDriver{})
	}
	// Not under the lock, and bounded: admissions wait on it.
	lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	list, err := a.kubeClient.CoreV1().Pods(a.agentNamespace()).List(lctx, metav1.ListOptions{LabelSelector: agentLabelSelector})
	if err != nil {
		v.mu.Lock()
		v.failedAt = time.Now() // tried again after the TTL, not per admission
		view := v.view
		v.mu.Unlock()
		a.log.WithError(err).Warn("driver gate: cannot list the agents to read their drivers; using the last view and this node's driver")
		return a.withOwnDriver(view)
	}
	seen := map[int]bool{}
	for i := range list.Items {
		p := &list.Items[i]
		if p.DeletionTimestamp != nil {
			continue
		}
		if n, err := strconv.Atoi(p.Annotations[DriverMajorAnnotation]); err == nil && n > 0 {
			seen[n] = true
		}
	}
	view := clusterDriver{}
	for n := range seen {
		view.Majors = append(view.Majors, n)
	}
	sort.Ints(view.Majors)
	v.mu.Lock()
	if v.ok && fmt.Sprint(view.Majors) != fmt.Sprint(v.view.Majors) {
		a.log.WithFields(logrus.Fields{"was": v.view.Majors, "now": view.Majors}).Info("driver gate: the GPU nodes' drivers changed")
	}
	v.view, v.at, v.ok, v.failedAt = view, time.Now(), true, time.Time{}
	v.mu.Unlock()
	return a.withOwnDriver(view)
}

// withOwnDriver adds this node's driver to view: known before its stamp
// is, and when the agents cannot be read.
func (a *Agent) withOwnDriver(view clusterDriver) clusterDriver {
	own := int(a.ownDriver.Load())
	if own <= 0 {
		return view
	}
	for _, m := range view.Majors {
		if m == own {
			return view
		}
	}
	out := clusterDriver{Majors: append(append([]int(nil), view.Majors...), own)}
	sort.Ints(out.Majors)
	return out
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// seedDrivers sets the cluster's drivers as read now: for tests.
func (a *Agent) seedDrivers(majors ...int) {
	a.drivers.mu.Lock()
	defer a.drivers.mu.Unlock()
	a.drivers.view, a.drivers.at, a.drivers.ok = clusterDriver{Majors: majors}, time.Now(), true
}

// criuMinDriverMajor is the configured minimum, or the default.
func (a *Agent) criuMinDriverMajor() int {
	if a.config.CRIUMinDriverMajor > 0 {
		return a.config.CRIUMinDriverMajor
	}
	return DefaultCRIUMinDriverMajor
}

// driverGate is why the cluster's drivers keep CRIU from serving, "" when
// they do not.
func (a *Agent) driverGate(ctx context.Context) string {
	return a.clusterDriver(ctx).Gate(a.criuMinDriverMajor())
}

// keyDriver is the driver major group record keys are made with now.
func (a *Agent) keyDriver(ctx context.Context) int {
	return a.clusterDriver(ctx).Major()
}

// criuGroupKeyDriver is the group record key of a configuration in a
// tenant scope under driver major driver; 0 is the key without a driver
// (no agent reports one, or they differ).
func criuGroupKeyDriver(cacheURI, scope string, driver int) string {
	if driver <= 0 {
		return criuGroupKey(cacheURI, scope)
	}
	return criuGroupKey(cacheURI, scope+"\x00driver="+strconv.Itoa(driver))
}

// recordDriver is the driver major group record data d was captured
// under; 0 for none recorded.
func recordDriver(d map[string]string) int {
	n, _ := strconv.Atoi(d[recordDriverKey])
	return n
}

// driverMismatch is why this node may not restore from group record key:
// its driver major is not the one the capture was taken under (a pod
// admitted before a node of another driver joined, or before the agents'
// view caught up). "" when it may, or when either is unknown.
func (c *criuAutoRestorer) driverMismatch(ctx context.Context, key string) string {
	if key == "" {
		return ""
	}
	d, err := c.a.groupRecordData(ctx, key)
	if err != nil {
		return ""
	}
	want := recordDriver(d)
	if want == 0 {
		return ""
	}
	own := int(c.a.ownDriver.Load())
	if own == 0 {
		own, _, _ = c.a.nodeDriverMajor(ctx)
	}
	if own == 0 || own == want {
		return ""
	}
	return fmt.Sprintf("this node runs driver %d; the capture was taken under %d", own, want)
}
