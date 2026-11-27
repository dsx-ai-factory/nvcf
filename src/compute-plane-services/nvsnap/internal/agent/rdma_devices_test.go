// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// rdmaFixture is a placeholder (pid 1, root placeholder) and a restored
// tree (pid 2, root tree) under a fake /proc.
func rdmaFixture(t *testing.T) (proc, placeholder, tree string) {
	t.Helper()
	proc, placeholder, tree = t.TempDir(), t.TempDir(), t.TempDir()
	for pid, root := range map[string]string{"1": placeholder, "2": tree} {
		if err := os.MkdirAll(filepath.Join(proc, pid), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(root, filepath.Join(proc, pid, "root")); err != nil {
			t.Fatal(err)
		}
	}
	return proc, placeholder, tree
}

// A pod without RDMA devices leaves the restored /dev as it is.
func TestInstallPlaceholderRDMANoDevices(t *testing.T) {
	proc, _, tree := rdmaFixture(t)
	if err := os.MkdirAll(filepath.Join(tree, rdmaDevDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tree, rdmaDevDir, "keep"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	made, err := installPlaceholderRDMA(proc, 1, 2)
	if err != nil || len(made) != 0 {
		t.Fatalf("made %v, %v; want nothing", made, err)
	}
	if _, err := os.Stat(filepath.Join(tree, rdmaDevDir, "keep")); err != nil {
		t.Errorf("the restored /dev was changed: %v", err)
	}
}

// The restored tree comes back with its source pod's RDMA nodes (uverbs5,
// uverbs6); its pod has uverbs4 and uverbs7. Afterwards the tree has
// exactly the pod's nodes, with the pod's device numbers. Root only
// (mknod).
func TestInstallPlaceholderRDMAReplacesTheSourcePods(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to create device nodes")
	}
	proc, placeholder, tree := rdmaFixture(t)
	old := syscall.Umask(0o022)
	defer syscall.Umask(old)
	mk := func(root, name string, minor int) {
		p := filepath.Join(root, rdmaDevDir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mknod(p, syscall.S_IFCHR|0o666, 231<<8|minor); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, 0o666); err != nil {
			t.Fatal(err)
		}
	}
	mk(placeholder, "uverbs4", 196)
	mk(placeholder, "uverbs7", 199)
	mk(placeholder, "rdma_cm", 64)
	mk(tree, "uverbs5", 197)
	mk(tree, "uverbs6", 198)
	mk(tree, "rdma_cm", 64)
	made, err := installPlaceholderRDMA(proc, 1, 2)
	// rdma_cm is already the pod's (same device and mode): left in place.
	if err != nil || len(made) != 2 {
		t.Fatalf("made %v, %v; want the pod's 2 missing nodes", made, err)
	}
	ents, _ := os.ReadDir(filepath.Join(tree, rdmaDevDir))
	var got []string
	for _, e := range ents {
		got = append(got, e.Name())
	}
	if len(got) != 3 || got[0] != "rdma_cm" || got[1] != "uverbs4" || got[2] != "uverbs7" {
		t.Errorf("restored /%s = %v, want [rdma_cm uverbs4 uverbs7]", rdmaDevDir, got)
	}
	var st syscall.Stat_t
	if err := syscall.Stat(filepath.Join(tree, rdmaDevDir, "uverbs7"), &st); err != nil || st.Rdev != 231<<8|199 {
		t.Errorf("uverbs7 rdev %x, %v; want the pod's", st.Rdev, err)
	}
	// The pod's permissions, whatever the agent's umask: a workload that is
	// not root must be able to open its NICs.
	if st.Mode&0o777 != 0o666 {
		t.Errorf("uverbs7 mode %o, want 666", st.Mode&0o777)
	}
}

// A symlink put where the node was made is not followed: the file it
// points at keeps its mode. Root only.
func TestChmodNodeAtDoesNotFollowALink(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root")
	}
	tree, outside := t.TempDir(), t.TempDir()
	target := filepath.Join(outside, "victim")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tree, rdmaDevDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(tree, rdmaDevDir, "uverbs4")); err != nil {
		t.Fatal(err)
	}
	d, err := openConfinedDir(tree)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if err := setNodeAttrsAt(d, filepath.Join(rdmaDevDir, "uverbs4"), 231<<8|196, 0o666, 0, 0); err == nil {
		t.Error("a symlink in the node's place was changed")
	}
	if st, _ := os.Stat(target); st.Mode().Perm() != 0o600 {
		t.Errorf("the link's target became %o", st.Mode().Perm())
	}
}

// A node bind-mounted into the restored /dev that is already the pod's is
// left in place; a mount point holding another device fails with a clear
// error, not a bare EBUSY. Root only (mount).
func TestInstallPlaceholderRDMAMountPoints(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root")
	}
	proc, placeholder, tree := rdmaFixture(t)
	mk := func(p string, minor int) {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mknod(p, syscall.S_IFCHR|0o666, 231<<8|minor); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, 0o666); err != nil {
			t.Fatal(err)
		}
	}
	bind := func(src, dst string) {
		if err := os.WriteFile(dst, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mount(src, dst, "", syscall.MS_BIND, ""); err != nil {
			t.Skipf("bind mount: %v", err)
		}
		t.Cleanup(func() { _ = syscall.Unmount(dst, syscall.MNT_DETACH) })
	}
	mk(filepath.Join(placeholder, rdmaDevDir, "uverbs4"), 196)
	src := t.TempDir()
	mk(filepath.Join(src, "uverbs4"), 196)
	if err := os.MkdirAll(filepath.Join(tree, rdmaDevDir), 0o755); err != nil {
		t.Fatal(err)
	}
	bind(filepath.Join(src, "uverbs4"), filepath.Join(tree, rdmaDevDir, "uverbs4"))
	made, err := installPlaceholderRDMA(proc, 1, 2)
	if err != nil || len(made) != 0 {
		t.Fatalf("the pod's own node, bind-mounted: made %v, %v; want it left in place", made, err)
	}
	mk(filepath.Join(src, "uverbs9"), 201)
	bind(filepath.Join(src, "uverbs9"), filepath.Join(tree, rdmaDevDir, "uverbs9"))
	if _, err := installPlaceholderRDMA(proc, 1, 2); err == nil || !strings.Contains(err.Error(), "mount point") {
		t.Errorf("another device mounted in the restored /dev: %v; want a mount-point error", err)
	}
}

// A group-restricted device (0660, group rdma) keeps its owner and group in
// the restored tree, so a workload in that group can still open it. Root
// only.
func TestInstallPlaceholderRDMAKeepsOwnership(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root")
	}
	proc, placeholder, tree := rdmaFixture(t)
	old := syscall.Umask(0o022)
	defer syscall.Umask(old)
	p := filepath.Join(placeholder, rdmaDevDir, "uverbs4")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mknod(p, syscall.S_IFCHR|0o660, 231<<8|196); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(p, 0, 4242); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0o660); err != nil {
		t.Fatal(err)
	}
	if made, err := installPlaceholderRDMA(proc, 1, 2); err != nil || len(made) != 1 {
		t.Fatalf("made %v, %v", made, err)
	}
	var st syscall.Stat_t
	if err := syscall.Stat(filepath.Join(tree, rdmaDevDir, "uverbs4"), &st); err != nil {
		t.Fatal(err)
	}
	if st.Mode&0o777 != 0o660 || st.Uid != 0 || st.Gid != 4242 {
		t.Errorf("restored uverbs4: mode %o owner %d:%d, want 660 0:4242", st.Mode&0o777, st.Uid, st.Gid)
	}
	// Already the pod's, owner included: left in place on a second pass.
	if made, err := installPlaceholderRDMA(proc, 1, 2); err != nil || len(made) != 0 {
		t.Errorf("second pass: made %v, %v; want nothing", made, err)
	}
}
