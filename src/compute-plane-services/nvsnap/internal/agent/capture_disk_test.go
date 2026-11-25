// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const tib = uint64(1) << 40

// stubDisk makes diskSpace report avail/total from next, called once per
// statfs, and gpuUsedBytes report gpu; both restored after the test.
func stubDisk(t *testing.T, gpu int64, next func() (avail, total uint64)) {
	t.Helper()
	ds, gu := diskSpace, gpuUsedBytes
	t.Cleanup(func() { diskSpace, gpuUsedBytes = ds, gu })
	diskSpace = func(string) (uint64, uint64, error) { a, tot := next(); return a, tot, nil }
	gpuUsedBytes = func([]int) (int64, error) { return gpu, nil }
}

func writeCheckpoint(t *testing.T, dir, id string, created time.Time) {
	t.Helper()
	b, _ := json.Marshal(CheckpointMetadata{ID: id, CreatedAt: created, CatalogInfo: &CatalogInfo{InstanceCapture: true}})
	if err := os.MkdirAll(filepath.Join(dir, id), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, id, "metadata.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func diskTestRecord(key string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: criuGroupConfigMapName(key), Namespace: "nvsnap-system",
		Labels: map[string]string{"app.kubernetes.io/managed-by": "nvsnap"}}, Data: data}
}

func exists(dir, id string) bool {
	_, err := os.Stat(filepath.Join(dir, id))
	return err == nil
}

func TestSumGPUUsed(t *testing.T) {
	out := "112, 140000\n519, 139000\n777, 5000\nbad line\n521, x\n"
	if got, want := sumGPUUsed(out, []int{112, 519, 521}), int64(279000)<<20; got != want {
		t.Fatalf("sumGPUUsed = %d, want %d", got, want)
	}
}

// The failure this fixes: a capture of a Kimi rank (about 570 GB) on a
// node whose checkpoint filesystem had 584 GB of 4 TB free filled it past
// the kubelet's eviction threshold; the kubelet evicted a rank mid-capture
// and the agent with it. The preflight now blocks such a capture before
// any member suspends.
func TestDiskFindingsBlockADumpThatDoesNotFit(t *testing.T) {
	stubDisk(t, 560<<30, func() (uint64, uint64) { return 584 << 30, 4 * tib })
	a := &Agent{log: logrus.New(), config: Config{CheckpointDir: t.TempDir(), CaptureDiskKeepPercent: 20}}
	fs := a.diskFindings(context.Background(), t.TempDir(), nil, "rank-0", "main")
	if len(fs) != 1 || fs[0].Severity != preflightBlock || fs[0].Check != "disk" {
		t.Fatalf("findings = %+v, want one disk block", fs)
	}
}

func TestDiskFindingsPassWhenItFits(t *testing.T) {
	stubDisk(t, 560<<30, func() (uint64, uint64) { return 3 * tib, 4 * tib })
	a := &Agent{log: logrus.New(), config: Config{CheckpointDir: t.TempDir(), CaptureDiskKeepPercent: 20}}
	if fs := a.diskFindings(context.Background(), t.TempDir(), nil, "rank-0", "main"); len(fs) != 0 {
		t.Fatalf("findings = %+v, want none", fs)
	}
}

func TestDiskFindingsOffAtZero(t *testing.T) {
	stubDisk(t, 560<<30, func() (uint64, uint64) { return 1 << 30, 4 * tib })
	a := &Agent{log: logrus.New(), config: Config{CheckpointDir: t.TempDir()}}
	if fs := a.diskFindings(context.Background(), t.TempDir(), nil, "rank-0", "main"); len(fs) != 0 {
		t.Fatalf("findings = %+v, want none with the check off", fs)
	}
}

// A node short of room retires the checkpoints no record names, without
// waiting checkpointRetireAge, before it blocks; a named one stays.
func TestDiskFindingsRetireUnnamedCheckpointsFirst(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	writeCheckpoint(t, dir, "orphan", now.Add(-time.Hour))
	writeCheckpoint(t, dir, "recorded", now.Add(-time.Hour))
	writeCheckpoint(t, dir, "writing", now.Add(-time.Minute))
	kc := fake.NewSimpleClientset(diskTestRecord("k", map[string]string{"state": "complete", "size": "1", "checkpoint.0": "recorded"}))
	stubDisk(t, 560<<30, func() (uint64, uint64) {
		if exists(dir, "orphan") {
			return 584 << 30, 4 * tib
		}
		return 2 * tib, 4 * tib
	})
	a := &Agent{kubeClient: kc, log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir, CaptureDiskKeepPercent: 20}}
	if fs := a.diskFindings(context.Background(), t.TempDir(), nil, "rank-0", "main"); len(fs) != 0 {
		t.Fatalf("findings = %+v, want none once the orphan is retired", fs)
	}
	if exists(dir, "orphan") || !exists(dir, "recorded") || !exists(dir, "writing") {
		t.Fatalf("orphan %v recorded %v writing %v; want only the orphan gone",
			exists(dir, "orphan"), exists(dir, "recorded"), exists(dir, "writing"))
	}
}

// A low filesystem makes the reaper retire unnamed checkpoints at
// pressureRetireAge; with room it waits checkpointRetireAge.
func TestReaperRetiresEarlyWhileTheDiskIsLow(t *testing.T) {
	for _, low := range []bool{false, true} {
		dir := t.TempDir()
		now := time.Now()
		writeCheckpoint(t, dir, "orphan", now.Add(-time.Hour))
		stubDisk(t, 0, func() (uint64, uint64) {
			if low {
				return 600 << 30, 4 * tib
			}
			return 3 * tib, 4 * tib
		})
		a := &Agent{kubeClient: fake.NewSimpleClientset(), log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir, CaptureDiskKeepPercent: 20}}
		a.reapCheckpoints(context.Background(), nil, now)
		if exists(dir, "orphan") == low {
			t.Errorf("low=%v: orphan kept=%v", low, exists(dir, "orphan"))
		}
	}
}

// The prd21 failure: records left "verifying" after their namespaces
// went, marked abandoned, kept 2.5 TB of checkpoints per node for 11
// days. An abandoned record names nothing; one still being verified does.
func TestAbandonedVerifyKeepsNoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	writeCheckpoint(t, dir, "abandoned", now.Add(-5*time.Hour))
	writeCheckpoint(t, dir, "verifying", now.Add(-5*time.Hour))
	kc := fake.NewSimpleClientset(
		diskTestRecord("a", map[string]string{"state": criuGroupStateVerifying, "sourceNamespace": "gone", groupAbandonedKey: now.Format(time.RFC3339), "size": "1", "checkpoint.0": "abandoned"}),
		diskTestRecord("v", map[string]string{"state": criuGroupStateVerifying, "sourceNamespace": "live", "size": "1", "checkpoint.0": "verifying"}),
	)
	a := &Agent{kubeClient: kc, log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir}}
	dead, err := a.retirableCheckpoints(context.Background(), nil, now, checkpointRetireAge)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := dead["abandoned"]; !ok || len(dead) != 1 {
		t.Fatalf("retirable = %v, want only the abandoned record's checkpoint", dead)
	}
}

// A retired checkpoint's catalog row is deleted through nvsnap-server.
func TestReaperDropsTheCatalogRow(t *testing.T) {
	var mu sync.Mutex
	var deleted []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodDelete {
			deleted = append(deleted, r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	dir := t.TempDir()
	now := time.Now()
	writeCheckpoint(t, dir, "orphan", now.Add(-5*time.Hour))
	a := &Agent{kubeClient: fake.NewSimpleClientset(), log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir, CatalogURL: srv.URL}}
	a.reapCheckpointsOlderThan(context.Background(), nil, now, checkpointRetireAge)
	mu.Lock()
	defer mu.Unlock()
	if len(deleted) != 1 || deleted[0] != "/api/v1/checkpoints/orphan" {
		t.Fatalf("catalog deletes = %v, want /api/v1/checkpoints/orphan", deleted)
	}
}
