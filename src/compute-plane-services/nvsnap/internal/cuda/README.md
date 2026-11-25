<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/cuda

Runs the `cuda-checkpoint` binary for the agent (lock, checkpoint, restore,
unlock a process) and finds the GPU processes on the node or in a container.

## Used by

- [`internal/agent`](../agent/): `agent.go` creates the `Manager` (`New`);
  `agent.go`, `checkpoint.go` and `restore_v2.go` call its
  `FindGPUProcesses` and `FindAllGPUPIDsForContainer`.

## Key files

- `checkpoint.go`: `Manager`: runs `cuda-checkpoint`, finds GPU processes,
  and locates the host NVIDIA libraries ([`internal/hostlibs`](../hostlibs/)).
- `discover.go`: `DiscoverCudaLibDir`, which reads a GPU process's
  `/proc/<pid>/maps` for its libcuda directory; `checkpoint.go` calls it.

## Configuration

The binary path is the agent flag `--cuda-checkpoint-path` (default
`/usr/local/bin/cuda-checkpoint`).
