<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# deploy/k8s/benchmarks

Source and restore pod manifests for the benchmark workloads, one pair per
model (for example `gpt-oss-120b.yaml` and `gpt-oss-120b-restore.yaml`).

## Used by

- [`scripts/test-bench.sh`](../../../scripts/test-bench.sh), which selects a
  pair by workload name and records its results in
  [docs/results/PDF-BENCH-RESULTS.md](../../../docs/results/PDF-BENCH-RESULTS.md).

## Key files

Each `<name>.yaml` is a source pod and `<name>-restore.yaml` its restore
pod, except Whisper, which `test-bench.sh` restores from
`whisper-large-v3-rootfs-restore.yaml`; `whisper-large-v3-restore.yaml` is
referenced nowhere. The `case` arms of `test-bench.sh` name the pair each
workload uses.
