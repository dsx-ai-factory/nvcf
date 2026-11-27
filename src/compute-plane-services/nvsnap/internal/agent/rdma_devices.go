// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"syscall"
)

// rdmaDevDir is where a container's RDMA device nodes are.
const rdmaDevDir = "dev/infiniband"

// installPlaceholderRDMA gives the restored tree (nestedPID, host
// numbering) the RDMA device nodes of its pod (placeholderPID): the
// placeholder's /dev/infiniband, which the runtime made for the NICs the
// pod was given. The dump saves the container's /dev, a tmpfs, so the tree
// comes back with its source pod's nodes: on a node whose pods each get
// their own NICs those are other pods' devices, which the pod's device
// cgroup refuses, and its own are missing, so UCX (NIXL) and NCCL's verbs
// find no RDMA device to come back on. Nodes the pod does not have are
// removed and the pod's are made, all inside the restored /dev only. It
// returns the nodes made. A pod without RDMA devices is left as is.
func installPlaceholderRDMA(procBase string, placeholderPID, nestedPID int) ([]string, error) {
	src := filepath.Join(procBase, strconv.Itoa(placeholderPID), "root", rdmaDevDir)
	ents, err := os.ReadDir(src)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the placeholder's /%s: %w", rdmaDevDir, err)
	}
	want := map[string]syscall.Stat_t{}
	for _, e := range ents {
		var st syscall.Stat_t
		if err := syscall.Lstat(filepath.Join(src, e.Name()), &st); err != nil {
			return nil, fmt.Errorf("stat the placeholder's /%s/%s: %w", rdmaDevDir, e.Name(), err)
		}
		if st.Mode&syscall.S_IFMT == syscall.S_IFCHR {
			want[e.Name()] = st
		}
	}
	dst, err := openWorkloadDir(procBase, nestedPID, "/")
	if err != nil {
		return nil, fmt.Errorf("open the restored tree's root: %w", err)
	}
	defer func() { _ = dst.Close() }()
	if err := dst.root.MkdirAll(rdmaDevDir, 0o755); err != nil {
		return nil, err
	}
	d, err := dst.root.Open(rdmaDevDir)
	if err != nil {
		return nil, err
	}
	have, err := d.ReadDir(-1)
	_ = d.Close()
	if err != nil {
		return nil, err
	}
	for _, e := range have {
		if _, ok := want[e.Name()]; ok || e.Type()&fs.ModeCharDevice == 0 {
			continue
		}
		if err := dst.root.Remove(filepath.Join(rdmaDevDir, e.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("remove the source pod's /%s/%s: %w", rdmaDevDir, e.Name(), err)
		}
	}
	names := make([]string, 0, len(want))
	for n := range want {
		names = append(names, n)
	}
	sort.Strings(names)
	made := make([]string, 0, len(names))
	for _, n := range names {
		st := want[n]
		if err := mknodIn(dst, filepath.Join(rdmaDevDir, n), st.Mode, st.Rdev); err != nil {
			return made, fmt.Errorf("create /%s/%s: %w", rdmaDevDir, n, err)
		}
		made = append(made, "/"+rdmaDevDir+"/"+n)
	}
	return made, nil
}
