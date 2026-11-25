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

Separately, [`BUILD.bazel`](../BUILD.bazel) defines two Bazel images with no
Dockerfile, `nvsnap_agent_image` and `nvsnap_server_image`: the Go binary
alone on distroless/static, so the agent has no CRIU or gpushare and the
server no UI (the comments beside them). The `image-push-manual` GitHub
workflow (manual dispatch, or a PR labelled `deploy-to-stg`) finds them by
their `_index` targets and pushes them to the dev registry.

## Stale or unreferenced

- `Dockerfile.agent`: copies `docker/phase2/criu-src/` and builds
  `cmd/restore-entrypoint`, neither of which exists; nothing builds it.
- `agent/Dockerfile.app.criuv2`: no script, workflow or Makefile names it.
- `phase2/`: built by `scripts/build-container.sh`, which nothing calls and
  which copies a `bin/restore-entrypoint` whose source no longer exists.
  `scripts/build-agent-base.sh`, which nothing calls either, copies
  `phase2/criu-src/`, which does not exist.
- `criu-builder/`: nothing references its two Dockerfiles.

## Rules

- Every Docker-built image has a checked-in Dockerfile; never build from an
  ad-hoc inline Dockerfile.
- Bump the image tag in `versions.sh` on every rebuild; nodes cache by tag.

See [docs/design/THIRD-PARTY-FORKS.md](../docs/design/THIRD-PARTY-FORKS.md)
for the CRIU fork the base image builds.
