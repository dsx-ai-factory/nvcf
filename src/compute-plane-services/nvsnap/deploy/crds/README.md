<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# deploy/crds

CustomResourceDefinitions for the `nvsnap.io` group, for `kubectl apply`
outside Helm.

## Used by

- `make install-crds` and `make undeploy` (the
  [Makefile](../../Makefile)) apply and delete every file here.
- [`scripts/test-helm-chart.sh`](../../scripts/test-helm-chart.sh) checks that
  the chart's copy in `deploy/helm/nvsnap/crds/` matches
  `nvsnap.io_gpucheckpoints.yaml`.
- nvsnap-server reads and writes `gpucheckpoints` and `gpurestores` objects;
  it creates restores, lists and gets them, and updates their status
  ([`internal/server/server.go`](../../internal/server/server.go)).

## Key files

- `nvsnap.io_gpucheckpoints.yaml`: the `gpucheckpoints` and `gpurestores`
  CRDs; the source of the chart's copy.
- `gpucheckpoint.yaml`, `gpurestore.yaml`: older single-CRD files for the
  same two resources. Nothing references them by name; `make install-crds`
  applies them with the rest of the directory.
