<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->
# Disaggregated vLLM with NIXL over RDMA: capture and restore of the prefill pod, 2026-11-25

The raw data behind the disaggregated row of [BENCHMARKS.md](../BENCHMARKS.md). Copied from the test's output files
(Kubernetes events and pod conditions, the agent's log lines, the test's own checks), with pod addresses, node
addresses and namespaces removed. One run. Times are UTC.

- **Workload:** vLLM `v0.31.0` (`docker.io/vllm/vllm-openai:v0.31.0`) serving `TinyLlama/TinyLlama-1.1B-Chat-v1.0`.
  One prefill pod and one decode pod, 1 GPU each, on two nodes, and a proxy pod that sends each request to prefill
  and then to decode. KV transfer: vLLM's `NixlConnector` (`kv_both`) over UCX with `UCX_TLS=rc,cuda_copy,self` on
  an RDMA device of the node.
- **Hardware:** GB300 (the agent's `gpu=NVIDIA-GB300` log field), CRI-O.
- **nvsnap:** agent `v0.2.91-rc22w` with `--webhook-nixl-pause=true` (`webhook.nixlPause.enabled: true`); the
  prefill pod annotated `nvsnap.io/nixl-pause: "true"`, `nvsnap.io/gpushare: "true"` and
  `nvsnap.io/gpushare-fabric: "false"`, and requesting `nvidia.com/mlnxnics: "4"` besides its GPU. The container
  downloads the model with `huggingface_hub.snapshot_download` and then runs `vllm serve`.
- **Storage:** the checkpoint stayed on the capture node's local disk
  (`/var/lib/nvsnap/checkpoints/...` in the agent's log); the restore ran on the same node.

## Timeline

| Time | Event | Source |
|---|---|---|
| 03:33:41 | Cold prefill pod scheduled | Kubernetes event `Scheduled` |
| 03:33:43 | Its container started | Kubernetes event `Started` |
| 03:35:52 | Its `/health` answered 200 (first poll that saw it) | the test's cold check |
| 03:36:30 | Capture started; NIXL connector released (`nixl pause: ok ... 0 RDMA device file(s) open, 0 mapped`) | agent log |
| 03:36:43 | Checkpoint complete: `outcome=captured seconds=13` | agent log, `nvsnap capture summary` |
| 03:36:44 | Replacement prefill pod scheduled | Kubernetes event `Scheduled`, pod condition `PodScheduled` |
| 03:36:46 | Its container started | Kubernetes event `Started` |
| 03:36:54 | CRIU restore of the container's processes began | agent log, `criu-v2: restoring the container's pid namespace` |
| 03:37:10 | Processes resumed; NIXL connector re-created at the new pod's address (`nixl resume ...: ok resumed as engine fb206d92...`); `criu-v2: restore completed duration=23.55s` | agent log |
| 03:37:12 | Replacement pod Ready: `restoredToReadySeconds=2 scheduledToReadySeconds=28` | pod condition `Ready`; agent log, `nvsnap pod ready summary` |

The cold pod's Kubernetes Ready is not comparable: the capture hold (`webhook.captureHold`) keeps a pod that may be
captured out of service until its capture, so the cold time above is to `/health`, 131 s after scheduling.

## Checks after the restore

- The same four prompts answered through the proxy before the capture and after the restore gave the same text
  (greedy decoding), the second time served by the restored engine.
- 5 minutes of load from 8 clients (03:37:59 to 03:43:01): the decode pod's NIXL metrics after the restore show
  19,144 KV transfers succeeded, 0 failed, 0 expired. The load script counted 18,821 requests good and 319 bad; its
  check calls a completion bad when the text is empty or the KV parameters are missing. Why 319 completions were
  empty is not verified.
- 0 container restarts on the prefill, decode and proxy pods at 03:43:37.
