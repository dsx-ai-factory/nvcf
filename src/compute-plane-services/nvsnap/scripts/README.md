<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# scripts/

Shell scripts run by hand or by CI: building images
(`build-agent.sh`, `versions.sh`), installing and removing nvsnap
(`install-nvsnap.sh`, `uninstall-nvsnap.sh`), checking the chart and gpushare
build in CI (`test-helm-chart.sh`, `check-gpushare-build.sh`), and the
cluster test suite described below.

Subdirectories: [`lib/`](lib/) (helpers the test scripts source),
[`docs/`](docs/) (documentation checks), [`repro/nvmesh-shared-ro/`](repro/nvmesh-shared-ro/)
(a standalone storage reproduction). `tests/` holds a test of
`uninstall-nvsnap.sh` that nothing runs. `splice-microbench/` and
`splice-parallel/` hold standalone Go programs that `make test` compiles
(through `go list ./...`) and nothing runs.

## nvsnap test suite

Two entry points:

| Script | Answers | Writes |
|---|---|---|
| `test-e2e.sh <workload>` | does capture/restore work for this workload | pass/fail + step timings to stdout |
| `test-bench.sh <workload>` | how long does cold vs warm vs restore take | a row in `docs/results/PDF-BENCH-RESULTS.md` |

Use `test-e2e.sh` to check correctness, `test-bench.sh` to produce numbers.

## Before you run anything

```sh
export KUBECONFIG=<your cluster kubeconfig>
kubectl get nodes                     # must work
./scripts/test-e2e.sh                 # no args: prints the workload list
```

`test-e2e.sh` refuses to start unless the deployed agent matches
`scripts/versions.sh`. That is deliberate: a run against a different build
produces numbers attributed to the wrong version. To test a build that is
already deployed, override rather than editing the file:

```sh
NVSNAP_APP_VERSION=v0.2.46 ./scripts/test-e2e.sh vllm-small
```

## Running

```sh
./scripts/test-e2e.sh vllm-small      # single GPU, CRIU path
./scripts/test-e2e.sh vllm-70b        # 4 GPUs, cachedir path
./scripts/test-bench.sh gpt-oss-120b  # benchmark row instead of pass/fail
```

`test-e2e.sh` deletes the source pod before it restores. At the end it
prints the command that deletes the restored pod and leaves that pod
running.

## Capture paths

`test-e2e.sh` picks the path in this order (`scripts/test-e2e.sh`, after the
node pre-flight):

- `CAPTURE_PATH` in the environment (`criu-v2` or `rootfs`) wins.
- A source manifest annotated `nvsnap.io/gpushare: "true"` -> `criu-v2` at any
  GPU count: CRIU of the live process, GPU memory saved through gpushare.
- 2+ GPUs requested by the first container -> `rootfs`: capture the pod's
  cache mount (model weights, compiled kernels), no process state.
- Otherwise (1 GPU) -> `criu-v2`.

A `criu-v2` run asks the agent for CRIU explicitly (`capturePath`, through
`scripts/checkpoint.sh`), which the agent honours whatever the cluster's
capture method; it refuses a workload CRIU cannot capture (a Riva or Triton
NIM), and the script stops with a hint (`internal/agent/nim_backend.go`
`resolveCheckpointRedirect`).

`test-e2e.sh` does not read the `nvsnap.io/path` annotation of a workload
manifest (nvsnap-server's demo page and `internal/manifests` do).
`test-bench.sh` uses `CAPTURE_PATH=rootfs` unless the environment sets it,
with two exceptions: the `deepseek-v4-flash` and `gpt-oss-120b` case arms set
`rootfs` themselves, before that default, so the environment does not change
them. A run also switches to `rootfs` when the agent redirects a CRIU capture
(exit code 42, a Riva or Triton NIM). The agent's `--pod-cache-dir` flag is
what decides
whether a `rootfs`-family capture is really cachedir. If those two disagree,
believe the flag.

## Guards, and why a run may refuse to start

If a guard fires, it is telling you the run would have measured something other
than what you asked for.

| Guard | Refuses when | Why |
|---|---|---|
| agent version (`test-e2e.sh` only) | deployed image != `versions.sh` | numbers would be attributed to the wrong build |
| image exists (`test-e2e.sh` only) | tag missing from the registry | catches a failed push before a long run |
| placeholder | any `__NAME__` token survived substitution, not just `__CAPTURE_HASH__` | the webhook ignores the pod and it cold-starts |
| restore admitted | the agent has no `--pod-cache-dir`, the named container is absent, the cache dir is not mounted, neither `HF_HOME` nor `NIM_CACHE_PATH` is set, or one of them points outside the cache dir | the pod cold-starts while looking like a restore |

The placeholder guard rejects any unresolved `__[A-Z_]+__` token, not only
`__CAPTURE_HASH__`, because a manifest that still carries `__NODE_NAME__` or
`__CHECKPOINT_ID__` is just as unusable. Tokens named inside comments are
ignored deliberately: templates explain their own placeholders, and a guard
that fails a correctly substituted manifest is worse than the problem it
was added for.

The restore-admitted guard is the one worth understanding. A pod the webhook
declined still starts, still serves, and passes every functional check -- it
just fetches its model again. Without the guard the run reports a restore time
that is really a cold start, and in `test-bench.sh` that number is published.

Two of its conditions are easy to misread as "nothing to check". Absent cache
env is a failure, not a pass: a restore pod that inherited none of the stamped
variables is not restoring from anything. And the named container must exist --
falling back to the first container would let a decorated sidecar vouch for a
workload that is cold-starting. Both fail closed.

Shared implementation: `scripts/lib/restore-guard.sh`, sourced by both scripts
so the contract cannot drift between them. `scripts/lib/restore-guard-test.sh`
covers it with fixtures and needs no cluster:

```sh
scripts/lib/restore-guard-test.sh
```

## When a run fails

```sh
kubectl get pods -n nvsnap-system                       # both pods are left behind
kubectl logs <pod> -n nvsnap-system --tail=100
kubectl logs -n nvsnap-system -l app=nvsnap-agent -c agent --since=30m | grep -i capture
```

Capture and restore logs land next to the checkpoint on the node:

```sh
# Read the cache dir from the deployed agent rather than assuming it, and use
# the node the workload actually ran on -- a different agent pod sees a
# different disk.
. scripts/lib/restore-guard.sh
CACHE=$(agent_pod_cache_dir)
NODE=$(kubectl get pod <workload-pod> -n nvsnap-system -o jsonpath='{.spec.nodeName}')
AGENT=$(kubectl get pods -n nvsnap-system -l app=nvsnap-agent -o wide \
    | awk -v n="$NODE" '$7==n {print $1}' | head -1)
kubectl exec -n nvsnap-system "$AGENT" -c agent -- \
    sh -c "ls -1dt $CACHE/*/ | head -3"
```

Copy anything you need out before re-running: a second run may reuse or replace
the capture, and the evidence goes with it.

## Re-capturing

Captures are content-addressed. A second run with the same pod identity reuses
the existing capture rather than making a new one -- normally what you want, and
confusing when you are trying to test the capture path itself.

The agent's store is a chain: the manifest ConfigMap in front of the
node-local cache, then the L2 PVC tier when it is configured
(`internal/agent/rootfsonly_integration.go` `buildAgentBackend`). The capture
is skipped when the first tier that claims the hash returns a manifest with
files in it (`internal/rootfsonly/orchestrator.go` `usableCapture`). Each tier
behaves differently:

- ConfigMap `nvsnap-capture-<short-hash>`: if it is missing but the node
  running the capture still has `<pod-cache-dir>/<hash>/manifest.json` from
  the earlier one, `Stat` rebuilds the ConfigMap from it and the capture is
  skipped again
  (`internal/checkpointstore/cmregistry.go`).
- L2 PVC `rox-<short-hash>`: its `Stat` returns only the hash, which is not a
  usable manifest, so on its own it does not skip the capture. But when the
  re-capture is promoted to L2, an existing `rox-<short-hash>` in the source
  pod's namespace is taken as already done and is reused, not rewritten
  (`internal/checkpointstore/percapture_pvc.go`).

For a clean test, remove all three before the run (the cache directory on
the node that captured):

```sh
kubectl delete cm -n nvsnap-system nvsnap-capture-<short-hash>
kubectl delete pvc -n nvsnap-system rox-<short-hash>   # the test scripts' namespace
kubectl exec -n nvsnap-system "$AGENT" -c agent -- rm -rf "$CACHE/<hash>"
```

`$AGENT` and `$CACHE` are found as in [When a run fails](#when-a-run-fails).
A schema change bumps `CaptureFormatVersion`, which changes the hash and
re-captures everything automatically.

## Adding a workload

1. `deploy/k8s/workloads/<id>.yaml` plus `<id>-restore.yaml` for
   `test-e2e.sh`; `deploy/k8s/benchmarks/` for `test-bench.sh`.
2. A rootfs restore manifest carries
   `nvsnap.io/restore-from: "__CAPTURE_HASH__"`; a criu-v2 restore manifest
   is generated by `go run ./internal/manifests/gen` and carries
   `__NODE_NAME__` and `__CHECKPOINT_ID__`. The scripts substitute them.
3. Add a `case` arm in both scripts with the pod names, port, model, and
   inference payloads.
4. Set the first container's `nvidia.com/gpu` request accurately, and annotate
   `nvsnap.io/gpushare: "true"` for a multi-GPU CRIU case: `test-e2e.sh`
   picks the capture path from them.
