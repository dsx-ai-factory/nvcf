<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# scripts/docs

Python checkers and generators for nvsnap's documentation. They run in the
`nvsnap docs` GitHub workflow and locally with `make check-docs`.

## Used by

- `.github/workflows/nvsnap-docs.yml` and the `check-docs` target of the
  [Makefile](../../Makefile), which run the unit tests, then
  `check-doc-links.py`, `docs-toc.py --check`, `reference.py --check` and
  `docs/figures/scripts/check.py`.

## Key files

- `check-doc-links.py`: fails when a git-tracked Markdown file under nvsnap
  links to a path or heading anchor that does not exist, or when code names
  a doc path that does not exist. It skips `docs/results/` and fenced code
  blocks.
- `docs-toc.py`: writes or checks the table of contents of every Markdown
  file under `docs/` except `docs/results/`.
- `reference.py`: writes or checks [docs/REFERENCE.md](../../docs/REFERENCE.md)
  from the Go sources.
- `test_docs_checks.py`: unit tests for the three scripts above
  (`python3 -m unittest test_docs_checks`, from this directory).
