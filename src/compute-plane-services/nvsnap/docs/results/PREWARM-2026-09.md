<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->
# Page-cache prewarm A/B, September 2026

Moved here unchanged from `docs/results/BENCHMARK.md` (removed; git history keeps it), where it was a subsection of
the cachedir results. It is the record behind the prewarm's defaults and the code comments that cite it. The nvsnap
build of these runs is not recorded, so [BENCHMARKS.md](../BENCHMARKS.md) does not list them.

The cachedir restore adds an `nvsnap-prewarm` init container that reads the
whole rox tree before the engine starts, so the engine's weight load hits page
cache instead of storage. Opt out per pod with `NVSNAP_PREWARM=0`. In this A/B
the sweep was a per-file `find | xargs -P 6 cat`; it is now `nvsnap-helper
prewarm`, which reads 256 MiB ranges with the storage profile's reader count
(`internal/webhook/reader_steps.go`, `cmd/nvsnap-helper`). Measured on Llama-3.1-70B-Instruct, vLLM v0.20.0 TP=4,
131.6 GB rox on a network block volume (NVMesh, ~1 GB/s per volume), same node,
page cache dropped before every run:

| Restore | Ready | Prewarm init |
|---|---:|---:|
| prewarm | 245 s, 249 s | 138 s, 137 s |
| no prewarm | 263 s, 251 s | -- |

Cold start on the same node: 632 s. The prewarm is neutral here: a single
reader already saturates the volume, so the prewarm only moves the same
storage-bound read ahead of the engine instead of overlapping it with process
bring-up. The prewarm pays off on volumes whose single-stream reads are
latency-bound but whose aggregate throughput is high (Hyperdisk ML), where the
engine's per-fault mmap reads leave the volume idle and the parallel sweep does
not. Treat the prewarm value as a property of the storage class, not the model:
the sweep reads files in 256 MiB ranges so a few large weights files still use every reader (an NVMesh volume on the H100 NVMesh test cluster read 270 MB/s per stream and 2.1 GB/s at eight, 2026-09-28), and its switch and reader count live in the storage profile (`prewarm`,
`prewarmParallelism` in the `nvsnap-storage-profiles` ConfigMap), see
`docs/design/STORAGE-AGNOSTIC-L2-PROMOTION.md`.

Measured on an NVCF container function (NIM Llama-3.1-8B, vLLM profile,
1x H100, NVMesh, 2026-09-28), health ready from pod creation: cold about
197 s; cachedir restore with the old per-file sweep 206 s (the sweep ran
one reader for 67 s); with prewarm off 180 s (the engine's own load read
15 GB single-stream in 33 s); with the range-parallel sweep 140 s
(weights 1.6 s; the sweep itself hit page cache from the previous run on
the same node, a cold sweep at 2 GB/s adds about 8 s). Of the remaining
140 s, 27 s are the function's own init containers, 80 s are Python
imports across the API server and engine processes, 25 s are CUDA graph
capture; cachedir restore does not touch those, only the CRIU path does.

Prewarm file selection, a documented heuristic. The sweep follows
symlinks and skips two path patterns: `*/original/*` and `*/blobs/*`.
Both come from the Hugging Face hub cache layout that `huggingface_hub`
produces for every engine that downloads through it (vLLM, SGLang, NIM,
TGI): `snapshots/<rev>/` holds entry names that are symlinks into
`blobs/`, and repos such as Llama and Mistral ship the original PyTorch
checkpoint under `original/` next to the safetensors. Reading by entry
name and skipping `blobs/` avoids reading the same bytes twice; skipping
`original/` avoids a file no engine opens (16 GB of the 30 GB Llama-3.1-8B
tree). On a tree without that layout the filters match nothing and the
sweep reads everything. The general replacement is a list of the files
the engine opened during capture, recorded in the manifest and replayed
by the sweep; until that exists this heuristic is the only model-shaped
knowledge in the restore path, and this paragraph is where it is
recorded.
