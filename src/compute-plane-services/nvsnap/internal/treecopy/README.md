<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/treecopy

Copies a directory tree file by file with `sendfile(2)`, regular files in
parallel. By default it skips what does not belong in a copy of a live
root filesystem (sockets, FIFOs, devices, overlay whiteouts) and keeps
symlinks and hardlinks. Faithful mode instead reproduces a stored tree
exactly, owners, modes and device nodes included, with direct reads.

## Used by

- [`cmd/agent/capture_write.go`](../../cmd/agent/capture_write.go): the
  `capture-write` subcommand.
- [`internal/agent`](../agent/): `l2_writer.go` and `l2_fetch.go`
  (`NewCopier`, `SetFaithful`).
- [`internal/checkpointstore`](../checkpointstore/): `local.go`.

## Key files

- `copier.go`: `Copier`, `NewCopier` and the walk.
- `faithful.go`: `SetWorkers`, `SetFaithful` and faithful-mode copying.
