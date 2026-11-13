// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/fnstatus"
)

// functionGVR is NvsnapFunction: one object per function version, what
// nvsnap holds for it (docs/design/FUNCTION-CACHE-STATUS.md). Read-only for
// everyone but this server.
var functionGVR = schema.GroupVersionResource{Group: "nvsnap.io", Version: "v1alpha1", Resource: "nvsnapfunctions"}

const (
	functionKind = "NvsnapFunction"
	// functionManagedBy marks the objects this server keeps: only those
	// are deleted when their records are gone.
	functionManagedBy = "nvsnap-server"
)

// FunctionStatus keeps one NvsnapFunction per function version the capture
// records name, from the records in Namespace. The records are the source
// of truth: an object is rewritten when its records change, created for a
// version that gets its first record, and deleted when its last record is
// gone. Nothing nvsnap decides reads the objects.
type FunctionStatus struct {
	KubeClient kubernetes.Interface
	DynClient  dynamic.Interface
	Namespace  string        // where the records live
	Interval   time.Duration // default 30s
	Log        logrus.FieldLogger
	// Restores follows the restores' pods for each version's history
	// (restore_history.go); nil keeps none.
	Restores *restoreTracker
	// Now is the clock the restores are aged by; nil is time.Now.
	Now func() time.Time
}

func (f *FunctionStatus) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

// Run keeps the objects every Interval until ctx is cancelled.
func (f *FunctionStatus) Run(ctx context.Context) {
	if f.Interval == 0 {
		f.Interval = 30 * time.Second
	}
	if f.Log == nil {
		f.Log = logrus.NewEntry(logrus.New()).WithField("subsys", "server.function-status")
	}
	if f.Restores != nil {
		f.Restores.watch(ctx, f.KubeClient)
	}
	missing := false
	t := time.NewTicker(f.Interval)
	defer t.Stop()
	for {
		err := f.reconcileOnce(ctx)
		switch {
		case apierrors.IsNotFound(err) && !missing:
			missing = true
			f.Log.Info("function status: the NvsnapFunction CRD is not installed; not kept")
		case err != nil && !apierrors.IsNotFound(err):
			f.Log.WithError(err).Warn("function status: cannot keep the NvsnapFunction objects")
		case err == nil:
			missing = false
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// reconcileOnce makes the objects match the records once.
func (f *FunctionStatus) reconcileOnce(ctx context.Context) error {
	records, err := f.KubeClient.CoreV1().ConfigMaps(f.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/managed-by=nvsnap"})
	if err != nil {
		// Never delete on a list that failed: an empty view is a claim
		// that nothing is cached. The restores followed still age out.
		if f.Restores != nil {
			f.Restores.forget(nil, f.now())
		}
		return err
	}
	want := fnstatus.Project(records.Items)
	restores, over := f.restoresByObject(records.Items)
	// written: the objects whose status now holds what was merged into it
	// (written, or already equal); only their finished restores are
	// forgotten.
	written := map[string]bool{}
	objs := f.DynClient.Resource(functionGVR)
	have, err := objs.List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/managed-by=" + functionManagedBy})
	if err != nil {
		// No object is written (the CRD may not be installed, a steady
		// state): the restores followed are still aged out, or they would
		// grow with every labelled pod until the server restarts.
		if f.Restores != nil {
			f.Restores.forget(nil, f.now())
		}
		return err
	}
	existing := make(map[string]*unstructured.Unstructured, len(have.Items))
	for i := range have.Items {
		existing[have.Items[i].GetName()] = &have.Items[i]
	}
	var firstErr error
	keep := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for name := range want {
		s := want[name]
		// The history is the server's own: carried from the object, with
		// the restores followed since merged in.
		if obj, ok := existing[name]; ok {
			s.History = historyOf(obj)
		}
		s.History = s.History.Merge(restores[name])
		status, err := toMap(s)
		if err != nil {
			keep(err)
			continue
		}
		obj, ok := existing[name]
		if !ok {
			_, err = objs.Create(ctx, functionObject(name, s, status), metav1.CreateOptions{})
			written[name] = err == nil
			if apierrors.IsAlreadyExists(err) {
				err = nil // not ours, or made since the list; the next pass sees it
			}
			keep(err)
			continue
		}
		delete(existing, name)
		cur, _, _ := unstructured.NestedMap(obj.Object, "status")
		if sameJSON(cur, status) && maps.Equal(obj.GetLabels(), functionLabels(s)) {
			written[name] = true
			continue
		}
		next := functionObject(name, s, status)
		next.SetResourceVersion(obj.GetResourceVersion())
		_, err = objs.Update(ctx, next, metav1.UpdateOptions{})
		written[name] = err == nil
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			err = nil // the next pass sees it
		}
		keep(err)
	}
	for name := range existing {
		err := objs.Delete(ctx, name, metav1.DeleteOptions{})
		if apierrors.IsNotFound(err) {
			err = nil
		}
		keep(err)
	}
	if f.Restores != nil {
		// The restores whose final entries are written: the objects keep
		// them now. One whose object was not written is merged again next
		// pass.
		var done []string
		for name, ids := range over {
			if written[name] {
				done = append(done, ids...)
			}
		}
		f.Restores.forget(done, f.now())
	}
	return firstErr
}

// restoreTimeTolerance bounds how far a record's stamp that names no
// restore may be from the restore's last rank's restore: the agent stamps
// it when the restore ends.
const restoreTimeTolerance = 2 * time.Minute

// restoresByObject is the restores followed, by the object of their
// configuration, with the record's time and capture bundle; and, by
// object, the ids of those whose pods are all gone. Nothing before the
// watch has listed the pods.
func (f *FunctionStatus) restoresByObject(records []corev1.ConfigMap) (map[string][]fnstatus.Restore, map[string][]string) {
	if f.Restores == nil || !f.Restores.ready() {
		return nil, nil
	}
	type rec struct {
		object string
		data   map[string]string
	}
	byKey := map[string]rec{}
	for i := range records {
		r := &records[i]
		key, ok := strings.CutPrefix(r.Name, fnstatus.GroupRecordPrefix)
		if !ok || r.Data["state"] == fnstatus.StateAlias {
			continue
		}
		if name := fnstatus.ObjectName(r.Labels); name != "" {
			byKey[key] = rec{name, r.Data}
		}
	}
	byKeyEntries, overByKey := f.Restores.restores(func(key string) int { return fnstatus.GPURanks(byKey[key].data) })
	out, over := map[string][]fnstatus.Restore{}, map[string][]string{}
	for key, rs := range byKeyEntries {
		r, ok := byKey[key]
		if !ok {
			continue // its record is gone, and its object with it
		}
		for _, e := range rs {
			if at, err := time.Parse(time.RFC3339, e.RestoredAt); err == nil {
				e.RestoreSeconds = fnstatus.RecordRestoreSeconds(r.data, e.Leader, at, restoreTimeTolerance)
			}
			e.CapturedBy = r.data["agentBundle"]
			out[r.object] = append(out[r.object], e)
		}
		over[r.object] = append(over[r.object], overByKey[key]...)
	}
	return out, over
}

// historyOf is the history an object's status holds.
func historyOf(obj *unstructured.Unstructured) fnstatus.History {
	var h fnstatus.History
	if st, ok, _ := unstructured.NestedMap(obj.Object, "status"); ok {
		if b, err := json.Marshal(st); err == nil {
			_ = json.Unmarshal(b, &h)
		}
	}
	return h
}

// functionLabels select a function's versions: kubectl get nvsnapfunctions
// -l nvsnap.io/function-id=<id>.
func functionLabels(s fnstatus.Status) map[string]string {
	l := map[string]string{"app.kubernetes.io/managed-by": functionManagedBy}
	for k, v := range map[string]string{fnstatus.LabelFunction: s.FunctionID, fnstatus.LabelTenant: s.Tenant, fnstatus.LabelNamespace: s.Namespace} {
		if v != "" {
			l[k] = v
		}
	}
	return l
}

func functionObject(name string, s fnstatus.Status, status map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": functionGVR.GroupVersion().String(),
		"kind":       functionKind,
		"status":     status,
	}}
	u.SetName(name)
	u.SetLabels(functionLabels(s))
	return u
}

// sameJSON reports whether a and b encode the same: a status read back
// holds its numbers as int64, the one written as float64.
func sameJSON(a, b map[string]any) bool {
	ja, err := json.Marshal(a)
	if err != nil {
		return false
	}
	jb, err := json.Marshal(b)
	return err == nil && bytes.Equal(ja, jb)
}

// toMap is s as an unstructured object holds it.
func toMap(s fnstatus.Status) (map[string]any, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	err = json.Unmarshal(b, &m)
	return m, err
}
