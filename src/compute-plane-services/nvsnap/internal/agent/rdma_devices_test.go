// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"os"
	"path/filepath"
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
	mk := func(root, name string, minor int) {
		p := filepath.Join(root, rdmaDevDir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mknod(p, syscall.S_IFCHR|0o666, 231<<8|minor); err != nil {
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
	if err != nil || len(made) != 3 {
		t.Fatalf("made %v, %v; want the pod's 3 nodes", made, err)
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
}
