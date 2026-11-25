<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# ci/

Image build helpers.

## Key files

- `build-image.sh <component>`: builds and pushes one nvsnap image
  (`base`, `agent`, `server`, `blobstore`, `l2-wait`) with `buildah` (or
  `docker` with `BUILDER=docker`), tagged from
  [`scripts/versions.sh`](../scripts/versions.sh). It skips the build when the
  tag already exists in the registry (`NO_SKIP=1` forces it) and pushes
  only when `PUSH` is `1` (the default). The Dockerfiles are in [`docker/`](../docker/).
- `Dockerfile.bazel`: a Bazel CI toolchain image on Ubuntu 22.04.

`build-image.sh` is the Dockerfile path. The Bazel images in
[`BUILD.bazel`](../BUILD.bazel) are built and pushed separately, by the
`image-push-manual` GitHub workflow; see [`docker/README.md`](../docker/README.md).

## Used by

- `build-image.sh`: run by hand, as
  [docs/operate/DEVELOPING.md](../docs/operate/DEVELOPING.md) and
  [`docker/README.md`](../docker/README.md) describe. No workflow in this
  repository calls it. Its `blobstore` component points at
  `docker/nvsnap-blobstore/Dockerfile`, which does not exist.
- `Dockerfile.bazel`: its header says its version moves with a
  `.gitlab-ci.yml` variable; this repository has no `.gitlab-ci.yml` and
  nothing else references the file.
