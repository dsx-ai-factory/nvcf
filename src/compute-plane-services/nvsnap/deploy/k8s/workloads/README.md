<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# deploy/k8s/workloads

Pairs of pod manifests, `<id>.yaml` (the source pod to capture) and
`<id>-restore.yaml` (the pod that restores it), one pair per test workload.
Source pods carry `nvsnap.io/*` annotations (display name, model, port, GPU
count, capture path) that the scripts and the server's demo page read.

## Used by

- [`scripts/test-e2e.sh`](../../../scripts/test-e2e.sh), which picks a pair by
  workload id.
- nvsnap-server's demo workload catalog: [`docker/Dockerfile.server`](../../../docker/Dockerfile.server)
  copies this directory to `/etc/nvsnap/workloads`
  ([`internal/server/manifests.go`](../../../internal/server/manifests.go)).
- [`internal/manifests`](../../../internal/manifests/): its conformance
  tests read these files (`BUILD.bazel` exposes them as `workload_manifests`),
  and `internal/manifests/gen` writes the criu-v2 `-restore.yaml`
  placeholders from their sources.

## Key files

- `vllm-small.yaml` / `vllm-small-restore.yaml`: the single-GPU CRIU case.
- `vllm-small-gpushare.yaml`, `vllm-tp4-gpushare.yaml`: sources annotated
  `nvsnap.io/gpushare: "true"`, with their restore manifests.
- `vllm-small-rootfs-restore.yaml`: a rootfs restore of `vllm-small`.
- `BUILD.bazel`: the `workload_manifests` filegroup.

Adding a workload is described in [`scripts/README.md`](../../../scripts/README.md).
