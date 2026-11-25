<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# tests/gpushare/host

Python tests of the gpushare libraries and `nvsnap-gpu-suspend` that need
no GPU: each compiles what it tests (or a probe program around it) against
the CUDA driver header and checks one behaviour, for example launch gating,
control-socket timeouts, the verbs library or NIXL pause and resume.

## Used by

- The `nvsnap gpushare` GitHub workflow runs every `test_*.py` here with
  `CUDA_INC` pointing at a `cuda.h`, and fails a run in which a test
  reports itself skipped.
- By hand, from `src/compute-plane-services/nvsnap`:
  `python3 -I tests/gpushare/host/<test>.py` (each file's docstring shows
  how).

## Key files

Each `test_*.py` is self-contained; its docstring states the behaviour it
checks.
