<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# docker/agent/gpushare

C sources of gpushare, built into the agent base image and shipped in the
node bundle:

- `libnvsnap_gpushare.so`: an `LD_PRELOAD` library that releases GPU memory
  shared between processes before a CUDA checkpoint and maps it again after.
- `nvsnap-gpu-suspend`: drives that library and the CUDA checkpoint API.
- `libnvsnap_ibv.so`: NCCL's verbs library (`NCCL_IBVERBS_LIB`), with RDMA
  state destroyed before a checkpoint and created again after it.

## Used by

- [`../Dockerfile.base`](../) (`gpushare-builder` stage: `make`).
- [`scripts/check-gpushare-build.sh`](../../../scripts/check-gpushare-build.sh),
  run by the `nvsnap gpushare` GitHub workflow, compiles it with warnings as
  errors; the workflow then runs the host tests in
  [`tests/gpushare/host`](../../../tests/gpushare/host/).
- The webhook preloads the library into the GPU containers of pods annotated
  `nvsnap.io/gpushare: "true"`. Where CRIU is the capture method it does so by
  default for the GPU containers of every pod that does not run to
  completion; a pod opts out with `nvsnap.io/gpushare: "false"`. A container
  whose `LD_PRELOAD` comes from a ConfigMap or Secret reference gets no
  library, and so cannot be checkpointed
  ([`internal/webhook/gpushare.go`](../../../internal/webhook/gpushare.go)).
- `webhook.gpushareIBV` (default `false`) points NCCL at `libnvsnap_ibv.so`;
  a pod's `nvsnap.io/gpushare-ibv` annotation (`"true"` or `"false"`)
  overrides it. The webhook never replaces a `NCCL_IBVERBS_LIB` the container
  sets in `env`, and skips a container whose `envFrom` may set it unless the
  annotation is `"true"`.

## Key files

- `gpushare.h`, `gpushare.c`: the preload library.
- `nvsnap-gpu-suspend.c`: the driver program.
- `nvsnap_ibv.c`, `nvsnap_ibv.map`: the verbs library and its symbol versions.
- `Makefile`: builds all three.

See [docs/design/GPUSHARE.md](../../../docs/design/GPUSHARE.md).
