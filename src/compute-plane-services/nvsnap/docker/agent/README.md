<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# docker/agent

The two-stage agent image. `Dockerfile.base` builds the parts that change
rarely: CRIU from the pinned fork, the gpushare libraries, and
`nvsnap-cuda-checkpoint`. `Dockerfile.app` adds the Go binaries and
`nvsnap-idle` on top of that base. Everything nvsnap places in a pod's node
bundle lands under `/criu-bundle`.

## Used by

- [`ci/build-image.sh`](../../ci/build-image.sh) (`base` with this directory
  as context, `agent` with the nvsnap root as context) and
  [`scripts/build-agent.sh`](../../scripts/build-agent.sh).
- [`scripts/check-gpushare-build.sh`](../../scripts/check-gpushare-build.sh)
  and the `nvsnap gpushare` GitHub workflow read the builder and CUDA header
  images from `Dockerfile.base`'s stages.

## Key files

- `Dockerfile.base`: CRIU, [`gpushare/`](gpushare/), `cuda-checkpoint`
  build stages.
- `Dockerfile.app`: builds `nvsnap-agent`, `nvsnap-mount-prep`,
  `nvsnap-rootfs-restore`, `nvsnap-helper` and `nvsnap-idle` into
  `/criu-bundle`.
- `nvsnap-cuda-checkpoint.c`: a `cuda-checkpoint` replacement on the CUDA
  driver checkpoint API, with the same command line.
- `cuda-checkpoint-wrapper.sh`: installed as `/criu-bundle/cuda-checkpoint`;
  sets library paths and clears `LD_PRELOAD` before running the real binary.
- `Dockerfile.app.criuv2`: an app image from public inputs only; nothing
  builds it.
