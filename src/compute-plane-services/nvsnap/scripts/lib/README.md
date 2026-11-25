<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# scripts/lib

Shell helpers sourced by the test scripts in [`scripts/`](../), with
fixture tests that need no cluster.

## Used by

- [`scripts/test-e2e.sh`](../test-e2e.sh) sources `agent-auth.sh`,
  `wait-progress.sh` and `restore-guard.sh`.
- [`scripts/test-bench.sh`](../test-bench.sh) sources `wait-progress.sh` and
  `restore-guard.sh`.
- [`scripts/checkpoint.sh`](../checkpoint.sh) sources `agent-auth.sh`.

## Key files

- `agent-auth.sh`: adds the agent's bearer token to calls to the agent API.
- `restore-guard.sh`: guards that stop a restore test from reporting a cold
  start as a restore.
- `wait-progress.sh`: waits for pod readiness while the pod shows progress.
- `restore-guard-test.sh`, `wait-progress-test.sh`: their tests; run them
  directly.
