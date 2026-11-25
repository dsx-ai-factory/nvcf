<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/treecopy

Copies a directory tree file by file with `sendfile(2)`, regular files in
parallel. By default it skips what does not belong in a copy of a live
root filesystem (sockets, FIFOs, devices, overlay whiteouts) and keeps
symlinks and hardlinks. Faithful mode instead re-creates a stored
tree with owners, modes, device nodes and FIFOs, and keeps regular files'
modification times and extended attributes (except `trusted.overlay.*`),
reading with direct I/O. It does not keep directory modification times, or
extended attributes of symlinks, devices and FIFOs.

## Used by

- [`cmd/agent/capture_write.go`](../../cmd/agent/capture_write.go): the
  `capture-write` subcommand.
- [`internal/agent`](../agent/): `l2_writer.go` and `l2_fetch.go`
  (`NewCopier`, `SetFaithful`).
- [`internal/checkpointstore`](../checkpointstore/): `local.go`.

## Key files

- `copier.go`: `Copier`, `NewCopier` and the walk.
- `faithful.go`: `SetWorkers`, `SetFaithful` and faithful-mode copying.
