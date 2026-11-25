<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# deploy/k8s

Plain Kubernetes manifests: an agent and server install without Helm,
sample workloads for the test scripts, and cluster-preparation manifests.
The supported install is the Helm chart in [`../helm/nvsnap`](../helm/nvsnap/).

## Used by

- `make deploy` and `make undeploy` apply and delete `agent-daemonset.yaml`.
- [`scripts/build-agent.sh`](../../scripts/build-agent.sh) rewrites image tags
  in `agent-daemonset*.yaml`, `nvsnap-server.yaml`, `nvsnap-blobstore.yaml`
  and `workloads/*.yaml`.
- [`scripts/sync-versions.sh`](../../scripts/sync-versions.sh) rewrites the
  nvsnap image references in every `*.yaml` here to the versions in
  `scripts/versions.sh`. `scripts/test-e2e.sh` runs it on startup, so a test
  run can leave manifest edits behind.
- [`scripts/bringup-cluster.sh`](../../scripts/bringup-cluster.sh) applies
  `namespace.yaml` and `nvcf-cluster-prep/storage-classes.yaml`.

## Key files

- `agent-daemonset.yaml`, `agent-daemonset-crio.yaml`: the agent for
  containerd and for CRI-O nodes.
- `nvsnap-server.yaml`: the server Deployment, Service, PVC and RBAC.
- `webhook.yaml`: cert-manager Issuer and Certificate, the Service and the
  MutatingWebhookConfiguration for the webhook the agent serves.
- `namespace.yaml`: the `nvsnap-test` and `nvsnap-system` namespaces.
- `nvcf-cluster-prep/storage-classes.yaml`: StorageClasses for GKE.
- `tcpxo-installer.yaml`: a DaemonSet named `nccl-tcpxo-installer`;
  nothing here references it.
- `nvsnap-blobstore.yaml`: Deployment, Service and PVC for `nvsnap-blobstore`, whose
  header names `internal/blobstore/`, which does not exist.

## Subdirectories

- [`workloads/`](workloads/): source and restore pods for `scripts/test-e2e.sh`.
- [`benchmarks/`](benchmarks/): source and restore pods for `scripts/test-bench.sh`.
- [`charts/vllm-workers/`](charts/vllm-workers/): test chart for the model
  download election.
