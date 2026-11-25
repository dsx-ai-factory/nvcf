// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/webhook"
)

const tib = uint64(1) << 40

// stubDisk makes diskSpace report avail/total from next, called once per
// statfs, and gpuUsedBytes report gpu; both restored after the test.
func stubDisk(t *testing.T, gpu int64, next func() (avail, total uint64)) {
	t.Helper()
	ds, gu := diskSpace, gpuUsedBytes
	resetReservations := func() {
		diskReserveMu.Lock()
		diskReservations = map[string]diskReservation{}
		diskReserveMu.Unlock()
	}
	resetReservations()
	t.Cleanup(func() { diskSpace, gpuUsedBytes = ds, gu; resetReservations() })
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

// GPU memory that cannot be read on a node with GPUs blocks: the dump
// could be far larger than its host memory.
func TestDiskFindingsBlockWhenGPUMemoryIsUnknown(t *testing.T) {
	stubDisk(t, 0, func() (uint64, uint64) { return 3 * tib, 4 * tib })
	gpuUsedBytes = func([]int) (int64, error) { return 0, errors.New("nvidia-smi: driver not loaded") }
	a := &Agent{log: logrus.New(), config: Config{CheckpointDir: t.TempDir(), CaptureDiskKeepPercent: 20}}
	fs := a.diskFindings(context.Background(), t.TempDir(), nil, "rank-0", "main")
	if len(fs) != 1 || fs[0].Severity != preflightBlock {
		t.Fatalf("findings = %+v, want one disk block", fs)
	}
	// A node with no nvidia-smi has no GPU memory to save.
	gpuUsedBytes = func([]int) (int64, error) { return 0, errNoNvidiaSMI }
	if fs := a.diskFindings(context.Background(), t.TempDir(), nil, "etcd-0", "etcd"); len(fs) != 0 {
		t.Fatalf("findings = %+v, want none on a node without GPUs", fs)
	}
}

// Two members on one node: each fits alone, not together. The second is
// blocked.
func TestDiskFindingsCountTheOtherMembersOnTheNode(t *testing.T) {
	stubDisk(t, 400<<30, func() (uint64, uint64) { return 1000 << 30, 2000 << 30 })
	a := &Agent{kubeClient: fake.NewSimpleClientset(), log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: t.TempDir(), CaptureDiskKeepPercent: 20}}
	if fs := a.diskFindings(context.Background(), t.TempDir(), nil, "rank-0", "main"); len(fs) != 0 {
		t.Fatalf("first member: %+v, want none", fs)
	}
	if fs := a.diskFindings(context.Background(), t.TempDir(), nil, "rank-1", "main"); len(fs) != 1 || fs[0].Severity != preflightBlock {
		t.Fatalf("second member: %+v, want one disk block", fs)
	}
	// The first member checked again does not count against itself.
	if fs := a.diskFindings(context.Background(), t.TempDir(), nil, "rank-0", "main"); len(fs) != 0 {
		t.Fatalf("first member again: %+v, want none", fs)
	}
}

// A deleted file the dump saves whole counts in the estimate.
func TestCaptureBytesCountDeletedFiles(t *testing.T) {
	stubDisk(t, 0, func() (uint64, uint64) { return 3 * tib, 4 * tib })
	a := &Agent{log: logrus.New()}
	before, err := a.captureBytes("/proc", []int{os.Getpid()})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "held"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(make([]byte, 64<<20)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		t.Fatal(err)
	}
	after, err := a.captureBytes("/proc", []int{os.Getpid()})
	if err != nil {
		t.Fatal(err)
	}
	if after-before < 48<<20 {
		t.Fatalf("estimate grew by %d bytes with a 64 MiB deleted file held, want about 64 MiB", after-before)
	}
}

// A delete cascaded from another node's reaper keeps a copy a pod on this
// node restores from.
func TestDeleteKeepsACheckpointAPodRestoresFrom(t *testing.T) {
	dir := t.TempDir()
	writeCheckpoint(t, dir, "busy", time.Now())
	writeCheckpoint(t, dir, "idle", time.Now())
	kc := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns",
		Annotations: map[string]string{webhook.CRIURestoreAnnotation: "busy"}}, Spec: corev1.PodSpec{NodeName: "n1"}})
	a := &Agent{kubeClient: kc, log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir}}
	for id, want := range map[string]int{"busy": http.StatusConflict, "idle": http.StatusNoContent} {
		req := mux.SetURLVars(httptest.NewRequest(http.MethodDelete, "/v1/checkpoints/"+id, nil), map[string]string{"id": id})
		rec := httptest.NewRecorder()
		a.deleteCheckpointHandler(rec, req)
		if rec.Code != want || exists(dir, id) != (want == http.StatusConflict) {
			t.Errorf("%s: status %d (want %d), still there %v", id, rec.Code, want, exists(dir, id))
		}
	}
}

// An unreadable memory measure blocks; a process gone since it was listed
// counts 0.
func TestCaptureBytesFailClosedOnUnreadableMemory(t *testing.T) {
	stubDisk(t, 0, func() (uint64, uint64) { return 3 * tib, 4 * tib })
	proc := t.TempDir()
	if err := os.MkdirAll(filepath.Join(proc, "41"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proc, "41", "smaps_rollup"), []byte("Rss: 10 kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	a := &Agent{log: logrus.New(), config: Config{CheckpointDir: t.TempDir(), CaptureDiskKeepPercent: 20}}
	if _, err := a.captureBytes(proc, []int{41}); err == nil {
		t.Fatal("no Anonymous line: want an error")
	}
	if fs := a.diskFindings(context.Background(), proc, []int{41}, "rank-0", "main"); len(fs) != 1 || fs[0].Severity != preflightBlock {
		t.Fatalf("findings = %+v, want one disk block", fs)
	}
	if n, err := a.captureBytes(proc, []int{42}); err != nil || n != 0 {
		t.Fatalf("gone process: %d %v, want 0 and no error", n, err)
	}
}

// A re-check of the same container (the on-demand preflight report) does
// not lower the reservation of a dump still being written.
func TestReserveDiskNeverLowersALiveReservation(t *testing.T) {
	stubDisk(t, 0, func() (uint64, uint64) { return 3 * tib, 4 * tib })
	now := time.Now()
	reserveDisk("p/main", 500<<30, now)
	reserveDisk("p/main", 100<<30, now.Add(time.Minute))
	if got := reservedBytes("other/main", now.Add(2*time.Minute)); got != 500<<30 {
		t.Fatalf("reserved = %d GiB, want 500", got>>30)
	}
	if got := reservedBytes("other/main", now.Add(diskReserveTTL+time.Minute)); got != 0 {
		t.Fatalf("reserved after the TTL = %d, want 0", got)
	}
}

// The rootfs diff is not archived when it would take the room the kept
// free space needs.
func TestRootfsDiffChecksTheCheckpointRoom(t *testing.T) {
	upper := t.TempDir()
	if err := os.WriteFile(filepath.Join(upper, "db"), make([]byte, 2<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		avail uint64
		fits  bool
	}{{avail: 20<<20 + 1<<20, fits: false}, {avail: 20<<20 + 64<<20, fits: true}} {
		stubDisk(t, 0, func() (uint64, uint64) { return tc.avail, 100 << 20 })
		ckpt := t.TempDir()
		a := &Agent{log: logrus.New(), config: Config{CheckpointDir: ckpt, CaptureDiskKeepPercent: 20}}
		err := a.mirrorRootfsDiff(context.Background(), "p/main", ckpt, upper, nil, logrus.NewEntry(logrus.New()))
		if (err == nil) != tc.fits {
			t.Errorf("avail %d MiB: err = %v, want fits=%v", tc.avail>>20, err, tc.fits)
		}
	}
}

// A catalog delete that fails is retried from its tombstone until it
// succeeds.
func TestCatalogDeleteIsRetriedFromItsTombstone(t *testing.T) {
	var mu sync.Mutex
	status, calls := http.StatusInternalServerError, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		w.WriteHeader(status)
	}))
	defer srv.Close()
	dir := t.TempDir()
	now := time.Now()
	writeCheckpoint(t, dir, "orphan", now.Add(-5*time.Hour))
	a := &Agent{kubeClient: fake.NewSimpleClientset(), log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir, CatalogURL: srv.URL}}
	a.reapCheckpointsOlderThan(context.Background(), nil, now, checkpointRetireAge)
	tomb := filepath.Join(dir, catalogTombstoneDir, "orphan")
	if exists(dir, "orphan") || !exists(filepath.Dir(tomb), "orphan") {
		t.Fatal("want the checkpoint gone and its tombstone kept after a failed catalog delete")
	}
	mu.Lock()
	status = http.StatusNoContent
	mu.Unlock()
	a.reapCheckpoints(context.Background(), nil, now) // each sweep retries first
	if exists(filepath.Dir(tomb), "orphan") {
		t.Fatal("tombstone kept after the catalog delete succeeded")
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("catalog deletes = %d, want 2", calls)
	}
}

// Another member's dump that the preflight passed and that is not yet on
// disk takes its room from the write-time check; the writer's own does not.
func TestArchiveRoomCountsOtherMembersPendingDumps(t *testing.T) {
	upper := t.TempDir()
	if err := os.WriteFile(filepath.Join(upper, "f"), make([]byte, 2<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	stubDisk(t, 0, func() (uint64, uint64) { return 20<<20 + 16<<20, 100 << 20 })
	a := &Agent{log: logrus.New(), config: Config{CheckpointDir: t.TempDir(), CaptureDiskKeepPercent: 20}}
	write := func() error { return nil }
	reserveDisk("p/main", 15<<20, time.Now()) // own: does not count
	if err := a.withCheckpointRoom("p/main", os.DirFS(upper), nil, write); err != nil {
		t.Fatalf("own reservation counted: %v", err)
	}
	reserveDisk("q/main", 15<<20, time.Now()) // another member's pending dump
	if err := a.withCheckpointRoom("p/main", os.DirFS(upper), nil, write); !errors.Is(err, errNoCheckpointRoom) {
		t.Fatalf("err = %v, want errNoCheckpointRoom with another member's dump pending", err)
	}
	releaseDisk("q/main") // its dump is on disk
	if err := a.withCheckpointRoom("p/main", os.DirFS(upper), nil, write); err != nil {
		t.Fatalf("after release: %v", err)
	}
}

// Tar headers and padding count: many small files are far larger as an
// archive than their payload.
func TestTarBytesCountHeadersAndPadding(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 2000; i++ {
		if err := os.WriteFile(filepath.Join(dir, "f"+strconv.Itoa(i)), []byte{1}, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	over, n, err := tarBytesOver(os.DirFS(dir), 1<<20, nil)
	if err != nil || !over {
		t.Fatalf("2000 one-byte files: over=%v n=%d err=%v, want over 1 MiB", over, n, err)
	}
	if _, payload, _ := treeBytesOver(os.DirFS(dir), 1<<20); payload != 2000 {
		t.Fatalf("payload = %d, want 2000", payload)
	}
}

// An unreachable nvsnap-server does not hold up local reclamation: the
// sweep retires local checkpoints first and bounds its catalog retries.
func TestCatalogRetriesDoNotDelayReclamation(t *testing.T) {
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-stop:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(stop)
	b, rt := catalogRetryBudget, catalogRequestTimeout
	catalogRetryBudget, catalogRequestTimeout = 300*time.Millisecond, 200*time.Millisecond
	defer func() { catalogRetryBudget, catalogRequestTimeout = b, rt }()
	dir := t.TempDir()
	now := time.Now()
	if err := os.MkdirAll(filepath.Join(dir, catalogTombstoneDir), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := os.WriteFile(filepath.Join(dir, catalogTombstoneDir, "old"+strconv.Itoa(i)), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeCheckpoint(t, dir, "orphan", now.Add(-5*time.Hour))
	a := &Agent{kubeClient: fake.NewSimpleClientset(), log: logrus.New(), config: Config{NodeName: "n1", CheckpointDir: dir, CatalogURL: srv.URL}}
	start := time.Now()
	a.reapCheckpoints(context.Background(), nil, now)
	if exists(dir, "orphan") {
		t.Fatal("the local checkpoint was not retired")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("sweep took %v with an unreachable server, want it bounded", d)
	}
	ents, _ := os.ReadDir(filepath.Join(dir, catalogTombstoneDir))
	if len(ents) != 21 {
		t.Fatalf("tombstones = %d, want all 21 kept for later sweeps", len(ents))
	}
}

// Sizing leaves out what the archive leaves out: a model cache the rootfs
// diff excludes must not block an archive whose included files fit.
func TestTarBytesLeaveOutTheWritersExclusions(t *testing.T) {
	dir := t.TempDir()
	hub := filepath.Join(dir, "root", ".cache", "huggingface", "hub")
	if err := os.MkdirAll(hub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hub, "weights"), make([]byte, 4<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "proc-mount"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "proc-mount", "x"), make([]byte, 4<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "app.log"), make([]byte, 64<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	exclude := append(tarPatternsForMountpoints([]string{"/proc-mount"}), rootfsDiffExcludeGlobs()...)
	if over, n, err := tarBytesOver(os.DirFS(dir), 1<<20, exclude); err != nil || over {
		t.Fatalf("with the exclusions: over=%v n=%d err=%v, want it to fit in 1 MiB", over, n, err)
	}
	if over, _, err := tarBytesOver(os.DirFS(dir), 1<<20, nil); err != nil || !over {
		t.Fatalf("without the exclusions: over=%v err=%v, want over 1 MiB", over, err)
	}
}

func TestTarExcluded(t *testing.T) {
	for _, tc := range []struct {
		name string
		pats []string
		want bool
	}{
		{"./root/.cache/huggingface/hub", []string{"*/huggingface/hub"}, true},
		{"./root/.cache/huggingface/hub/x/y", []string{"*/huggingface/hub/*"}, true},
		{"./root/.cache/huggingface", []string{"*/huggingface/hub"}, false},
		{"./usr/bin/nvidia-smi", tarPatternsForMountpoints([]string{"/usr/bin/nvidia-smi"}), true},
		{"./usr/bin/nvidia-smi-x", tarPatternsForMountpoints([]string{"/usr/bin/nvidia-smi"}), false},
		{"./sub/mnt/f", tarMountPatterns([]string{"sub/mnt"}), false},
		{"./sub/mnt", tarMountPatterns([]string{"sub/mnt"}), true},
	} {
		if got := tarExcluded(tc.name, tc.pats); got != tc.want {
			t.Errorf("tarExcluded(%q, %q) = %v, want %v", tc.name, tc.pats, got, tc.want)
		}
	}
}
