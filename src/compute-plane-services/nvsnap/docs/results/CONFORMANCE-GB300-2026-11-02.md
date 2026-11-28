<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->
# Workload-shape conformance runs on GB300, 2026-11-02

The raw data behind the "tested on GPUs" evidence for several rows of [SUPPORT.md](../SUPPORT.md). Two runs of the
same scenario list, one on each of two GB300 clusters, copied from the harness's summary with cluster names removed.

- **Engine and model:** vLLM `v0.20.0` (`docker.io/vllm/vllm-openai:v0.20.0`) serving
  `TinyLlama/TinyLlama-1.1B-Chat-v1.0`, one GPU per pod.
- **nvsnap:** agent `v0.2.89-mv216`, `agent.captureMethod: criu`.
- **Pods:** each engine pod requests `nvidia.com/gpu: 1` and no RDMA device; `sts2` and `sts8` set
  `NCCL_MNNVL_ENABLE=0`. The NCCL transport used was not logged.
- **Clusters:** cluster B, containerd, and cluster A, CRI-O, both runs started on 2026-11-02. Both
  GB300.
- **What one scenario does:** deploy the workload cold (generation 1), wait until nvsnap has captured it, delete it,
  deploy it again (generation 2) and check that its pods were restored (or, for the scenarios that must not be,
  that they were not), then send 2 requests and check both answers. `cold_s`, `capture_s` and `restore_s` are the
  harness's own timers, not comparable across scenarios; they are kept for completeness.

## Scenarios

| Scenario | Shape |
|---|---|
| `setsid1` | One pod, the engine detached from the container's pid 1 with `setsid` |
| `sts1` | One pod from a StatefulSet, the engine a child of the shell |
| `deploy1` | One pod from a Deployment, the engine the container's pid 1 |
| `xnode1` | `deploy1`, with generation 2 kept off the capture node |
| `xnode2` | `sts1`, restored on another node |
| `scope1` | `deploy1`, with generation 2 run as another function: must start cold |
| `sts2` | One engine over two pods on two nodes (vLLM TP=2, one GPU per pod) from a StatefulSet; NCCL over the network |
| `bare` | A bare pod (no controller): must not be captured |
| `shpid1` | One pod whose containers share a pid namespace, with a sidecar |
| `comp1` | One engine pod with a companion pod (a small key-value server it talks to) |
| `mptcp1` | `comp1` with the companion holding an MPTCP listener: the CRIU capture must fail and fall back to cachedir |
| `libfail1` | A program that exits when the gpushare library is preloaded: must fall back to cachedir without the library |
| `sts8` | `sts2` widened to 8 pods on 8 nodes (TP=8) |

On cluster B the restore logs of `xnode1` and `xnode2` show the checkpoint fetched from a peer agent:
`EnsureLocal: trying peer`, then `EnsureLocal: peer fetch complete`. That cluster ran with a cluster storage tier
(`agent.l2.storageClass` set) and `agent.rails.enabled: true`, both off by default.

## Cluster B (containerd): 13 of 13 passed

```text
scenario result   cold_s capture_s restore_s  detail
setsid1  PASS        80       512        38  restored, answers: 2.
sts1     PASS        98       525        42  restored, answers: 2.
deploy1  PASS        98       481        37  restored, answers: 2.
xnode1   PASS        85       530        41  restored, answers: 2.
xnode2   PASS       114       530        40  restored, answers: 2.
scope1   PASS        85       524        97  another function's pods started cold, as required
sts2     PASS       126       545        43  restored, answers: 2.
bare     PASS        98         -         -  not captured, as required
shpid1   PASS        98       498        40  restored, answers: 2.
comp1    PASS        97       497        42  restored, answers: 2.
mptcp1   PASS       113        76        44  fell back (failed), cachedir captured, gen 2 seeded, answers: 2.
libfail1 PASS       114        33        45  fell back (gpushare-failed), cachedir captured, gen 2 seeded, answers: 2.
sts8     PASS       767       566        88  restored, answers: 2.
```

## Cluster A (CRI-O): 9 of 13 passed

The failures' causes are not in the summary.

```text
scenario result   cold_s capture_s restore_s  detail
setsid1  PASS        85       526        69  restored, answers: 2.
sts1     PASS       403       640       108  restored, answers: 2.
deploy1  FAIL        84       498         -  gen 2 not Ready in 20m
xnode1   FAIL       154      1200         -  capture gpushare-failed:
xnode2   PASS        85       561        92  restored, answers: 2.
scope1   PASS        92       509        85  another function's pods started cold, as required
sts2     FAIL       130       905         -  gen 2 not Ready in 20m
bare     PASS        92         -         -  not captured, as required
shpid1   PASS        79       510        79  restored, answers: 2.
comp1    PASS       402       641       337  restored, answers: 2.
mptcp1   PASS        94        78        53  fell back (failed), cachedir captured, gen 2 seeded, answers: 2.
libfail1 PASS        84        34        50  fell back (gpushare-failed), cachedir captured, gen 2 seeded, answers: 2.
sts8     FAIL         -         -         -  gen 1 not Ready in 30m
```
