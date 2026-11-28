<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# deploy/k8s/charts/vllm-workers

A test Helm chart: a stock vLLM Deployment of N identical GPU workers with
no nvsnap markers. It exercises the webhook's one-downloader election
([`internal/election`](../../../../internal/election/)).

## Used by

- [`scripts/test-election-e2e.sh`](../../../../scripts/test-election-e2e.sh),
  which installs it with N replicas and expects one leader.
- Listed as a test fixture in the repository's
  `tools/docs-version-sync/chart-image-exclusions.yaml`.

## Key files

- `values.yaml`: `replicas`, `image`, `model`, `tensorParallel` (also the
  GPU request per worker), `imagePullSecret`, `env`, `excludeNodes`.
- `templates/deployment.yaml`: the Deployment.

See [docs/ARCHITECTURE.md](../../../../docs/ARCHITECTURE.md).
