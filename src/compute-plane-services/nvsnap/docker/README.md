<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# docker/

Dockerfiles for the nvsnap images. [`ci/build-image.sh`](../ci/build-image.sh)
builds them by component (`base`, `agent`, `server`, `l2-wait`; its
`blobstore` component names `docker/nvsnap-blobstore/Dockerfile`, which does
not exist), and
[`scripts/build-agent.sh`](../scripts/build-agent.sh) builds the agent's base
and app images; tags come from [`scripts/versions.sh`](../scripts/versions.sh).

## Image map

| Dockerfile | Image | Built by |
|---|---|---|
| [`agent/Dockerfile.base`](agent/) | `nvsnap-agent-base`: CRIU, gpushare, `cuda-checkpoint` | `build-image.sh base`, `build-agent.sh` |
| [`agent/Dockerfile.app`](agent/) | `nvsnap-agent`: the Go and C binaries on the base | `build-image.sh agent`, `build-agent.sh`, `make docker-build` |
| [`Dockerfile.server`](Dockerfile.server) | `nvsnap-server` with the web UI and demo workloads | `build-image.sh server`, `scripts/build-nvsnap-images.sh` |
| [`nvsnap-l2-wait/Dockerfile`](nvsnap-l2-wait/Dockerfile) | `nvsnap-l2-wait` ([`cmd/nvsnap-l2-wait`](../cmd/nvsnap-l2-wait/)) | `build-image.sh l2-wait` |

## Stale or unreferenced

- `Dockerfile.agent`: copies `docker/phase2/criu-src/` and builds
  `cmd/restore-entrypoint`, neither of which exists; nothing builds it.
- `agent/Dockerfile.app.criuv2`: no script, workflow or Makefile names it.
- `phase2/`: built only by `scripts/build-container.sh`, which nothing calls
  and which copies a `bin/restore-entrypoint` whose source no longer exists.
- `criu-builder/`: nothing references its two Dockerfiles.

## Rules

- Every image has a checked-in Dockerfile; never build from an ad-hoc inline
  Dockerfile.
- Bump the image tag in `versions.sh` on every rebuild; nodes cache by tag.

See [docs/design/THIRD-PARTY-FORKS.md](../docs/design/THIRD-PARTY-FORKS.md)
for the CRIU fork the base image builds.
