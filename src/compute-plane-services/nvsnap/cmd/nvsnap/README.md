<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# cmd/nvsnap

The `nvsnap` command-line client. `pods` reads pods from the Kubernetes API
directly, `version` prints the CLI version, and the other subcommands call
nvsnap-server over HTTP (`--server`, default `http://localhost:8080`).
`ps`, `checkpoint` and `restore` call `/api/v1/processes...` and
`/api/v1/checkpoints/{id}/restore`, which
[`internal/server`](../../internal/server/) does not register.

## Used by

- Operators, by hand. Bazel builds it (`go_binary` `nvsnap` in
  `BUILD.bazel`); no Dockerfile, Makefile target or chart ships it.
- Entry point: `main.go:main` (cobra root command).

## Key files

- `main.go`: the `ps`, `checkpoint`, `restore`, `checkpoints`,
  `delete`, `status` and `version` subcommands.
- `pods.go`: `nvsnap pods`, which classifies pods as cold-start or
  restored with [`internal/podtimings`](../../internal/podtimings/).

## See also

- The server it talks to: [`../nvsnap-server/`](../nvsnap-server/).
