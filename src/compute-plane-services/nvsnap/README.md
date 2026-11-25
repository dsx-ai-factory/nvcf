# NvSnap

**GPU checkpoint/restore for Kubernetes inference.** NvSnap checkpoints a running inference instance (its processes and
their GPU memory, across every pod and node of the instance) and restores it into new pods, so a scale-up resumes a
warm engine instead of downloading the model and starting the engine again. It works on unmodified inference images.

Measured on GB300 ([benchmarks](docs/BENCHMARKS.md)):

- **Kimi K3.1 Pro, 4 pods × 4 GPUs, Dynamo with vLLM.** A cold start took about 45 to 46 minutes from the first
  download start (scheduling and image pull not included), 30 to 33 of them downloading 1.56 TB per pod (agent v0.2.91-rc22d, 2026-11-22). On another GB300 cluster, deleting the captured
  instance's 7 pods and restoring them to all Ready took a median of 306.5 s, teardown included, in 30 of 32 cycles
  (agent v0.2.91-rc9 to rc14, 2026-11-12 and 13).
  [Raw data](docs/results/KIMI-K3-GB300-2026-11.md).
- **Disaggregated vLLM v0.31.0 with NIXL over RDMA, TinyLlama 1.1B.** The restored prefill pod was Ready 28 s after
  scheduling; the cold one answered `/health` 131 s after scheduling (agent v0.2.91-rc22w, 2026-11-25, one run).
  [Raw data](docs/results/DISAGG-NIXL-GB300-2026-11-25.md).

When a CRIU capture or restore of a configuration fails, its pods do not start under gpushare, or the cluster's
GPU drivers are too old for CRIU, nvsnap falls back to **cachedir**: it captures the engine's model and compile
caches, and the engine starts again with them (Qwen2.5-32B on 4x H100: 170 s against 325 s cold, agent
v0.2.75-election6, 2026-09-25, [raw data](docs/results/HELM-CHART-CACHE-2026-09.md)). cachedir is also the chart's
default method; GPU checkpoint/restore is `agent.captureMethod: criu`.

- What works for your workload, and what is refused: [docs/SUPPORT.md](docs/SUPPORT.md)
- Every measured number, with its raw data: [docs/BENCHMARKS.md](docs/BENCHMARKS.md)
- Install: [docs/operate/INSTALL.md](docs/operate/INSTALL.md) · how it works: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
  · every document: [docs/](docs/README.md)

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

---

## How it works

NvSnap has two capture methods. The cluster operator picks one with the
chart value `agent.captureMethod` (default `cachedir`); it applies to every
capture on the cluster:

- **GPU checkpoint/restore (`criu`).** CRIU saves the processes (CPU state,
  open files, sockets), and [gpushare](docs/design/GPUSHARE.md), an
  `LD_PRELOAD` library the webhook adds to GPU pods, saves their GPU memory,
  including memory the processes share, and re-creates it after the restore.
  Instances are captured only under gpushare
  (`internal/agent/criu_group.go`). Multi-pod instances (a StatefulSet,
  LeaderWorkerSet or Grove group, possibly spanning nodes) are captured and
  restored as one group, but only on request: pods annotated
  `nvsnap.io/criu-capture: "true"`, or matched by the chart value
  `agent.criuCaptureOptIn`.
- **cachedir (`cachedir`), any GPU count.** NvSnap captures the warm pod's
  model and JIT/compile caches into one cache directory and mounts it on
  restore. The engine starts again, but skips the weight download and reuses
  compiled kernels and graphs. A CRIU cluster uses it as the fallback for a
  configuration CRIU cannot serve
  ([SUPPORT.md](docs/SUPPORT.md#capture-method)).

A node-level **agent** (DaemonSet) performs capture/restore: it finds a
container through the runtime's API (containerd or CRI-O, detected at start,
`internal/runtime`) and its processes through `/proc`. A **server** provides a
REST API, web UI, and checkpoint catalog. An **admission webhook**, served by
the agent and on by default (`webhook.enabled`), prepares workload pods for
capture and restore.

Deeper design docs:
[architecture](docs/ARCHITECTURE.md) ·
[gpushare](docs/design/GPUSHARE.md).

---

## Quickstart

Prerequisites: a Kubernetes cluster with GPU nodes, an NVIDIA driver with the
CUDA checkpoint driver API (`cuCheckpointProcess*`; multi-GPU capture under
gpushare needs 610 or later, see [gpushare](docs/design/GPUSHARE.md#requirements-and-limits)),
containerd or CRI-O, and `kubectl` + `helm` 3.

```bash
# 1. Namespace + image pull secret for the project registry
kubectl create namespace nvsnap-system
kubectl create secret docker-registry nvsnap-pull-secret \
  --namespace=nvsnap-system \
  --docker-server=nvcr.io \
  --docker-username='$oauthtoken' \
  --docker-password='<NGC_API_KEY>'

# 2. Install (agent, server, webhook; also cert-manager and the GPU Operator
#    as subcharts unless you set cert-manager.enabled / gpu-operator.enabled false)
helm dependency build deploy/helm/nvsnap
helm install nvsnap deploy/helm/nvsnap --namespace nvsnap-system

# 3. Verify
kubectl -n nvsnap-system rollout status ds/nvsnap-agent
kubectl -n nvsnap-system get svc nvsnap-server
```

`scripts/install-nvsnap.sh` wraps the above (prereq checks, pull secret,
optional webhook). Full guide, options, and troubleshooting:
[docs/operate/INSTALL.md](docs/operate/INSTALL.md). Pull-secret details:
[docs/operate/PULL-SECRETS.md](docs/operate/PULL-SECRETS.md).

### Try it end-to-end

```bash
./scripts/test-e2e.sh vllm-small   # TinyLlama 1.1B on vLLM (single-GPU)
./scripts/test-e2e.sh vllm-8b      # Llama-3.1-8B on vLLM
./scripts/test-e2e.sh vllm-70b     # Llama-3.1-70B TP=4 (cachedir path)
```

---

## Architecture

```text
┌──────────────────┐     ┌───────────────────────────────────────┐
│  nvsnap-server   │     │            GPU Node                   │
│  REST API + UI   │◄───►│  nvsnap-agent (DaemonSet)             │
│  catalog (SQLite)│     │   ├─ runtime API + /proc discovery    │
│  metrics + audit │     │   ├─ CRIU orchestration (criu-v2)     │
└──────────────────┘     │   ├─ gpushare (GPU memory)            │
                         │   ├─ cachedir capture watcher           │
                         │   └─ peer-cascade HTTP server         │
                         │                                       │
                         │  GPU Pod (unmodified image)           │
                         │   └─ NIM / vLLM / SGLang / TRT-LLM    │
                         └───────────────────────────────────────┘
                                        │
              L1 node ─► L2 cluster storage ─► L3 peers ─► L4 object store
```

A restore fetches a checkpoint from the first tier that has it: the node's
own copy, then cluster storage, then peer agents (with `agent.rails.enabled`,
a checkpoint the rails tier judges small goes to the peers before cluster
storage, `internal/agent/cascade_fetch.go`). The cross-cluster object
store (L4) is filled and pulled by replication in the background, not tried
by a restore. Cluster config decides which tiers are present. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#6-where-bytes-live-and-how-they-move).

**Why the moving parts exist** (details in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)):

- **CRIU from outside the container (criu-v2)** — the agent dumps the
  container's pid namespace from outside it, with `criu dump --root
  /proc/<pid>/root` (`dumpPIDNamespace`, `internal/agent/pidns_capture.go`).
  Under process capture, the webhook preloads gpushare into the engine
  (and, with `webhook.gpushareIBV`, points NCCL at `libnvsnap_ibv.so`).
- **Forked CRIU** — io_uring checkpoint and restore, portable ghost files
  and CUDA plugin fixes on top of upstream criu-dev. See [docs/design/THIRD-PARTY-FORKS.md](docs/design/THIRD-PARTY-FORKS.md).

---

## Supported platforms

Runtimes, GPUs, drivers, storage and workload shapes, each with its status and evidence:
[docs/SUPPORT.md](docs/SUPPORT.md).

---

## Security requirements

The agent DaemonSet runs privileged, with `hostPID` and host-path mounts for
`/proc`, the container runtime's state and checkpoint storage: it enters the
workload's namespaces to dump and restore it. nvsnap never makes a workload
pod or restore placeholder privileged. One case changes the engine
container's security context: a restore from an older whole-rootfs capture
runs it as uid 0 with `DAC_OVERRIDE`, `CHOWN` and `FOWNER` added (and
`SYS_ADMIN` when the overlay is assembled in the pod), so it can write the
restored tree (`internal/webhook/bundle_mount.go` `securityContextPatches`,
`internal/webhook/mutate.go` `buildPatches`).

A checkpoint holds the captured processes' memory, which may include tokens,
keys and request data. A pod restores only a capture from its own tenant
scope, and the agent API takes a bearer token when `agent.auth.enabled` is
set. The security model is in
[docs/ARCHITECTURE.md §9](docs/ARCHITECTURE.md#9-security-model).

---

## REST API

`nvsnap-server` exposes a REST API and web UI on port 8080. Key endpoints:

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/checkpoints` | Checkpoint a running GPU pod |
| GET | `/api/v1/checkpoints` | List checkpoints (cursor pagination, filters) |
| GET | `/api/v1/checkpoints/{id}` | Checkpoint detail |
| DELETE | `/api/v1/checkpoints/{id}` | Delete checkpoint |
| POST | `/api/v1/restores` | Restore a checkpoint |
| GET/POST | `/api/v1/retention-policies` | Retention policies (CRUD) |
| GET | `/api/v1/audit` | Audit trail |
| GET | `/metrics` | Prometheus metrics |

The full endpoint list (including agent-side cascade endpoints) is in
[`internal/server/openapi.yaml`](internal/server/openapi.yaml).

---

## Project layout

```text
cmd/                    binary entry points (agent, server, mount-prep, rootfs-restore, l2-wait, helper, idle, CLI)
internal/               agent, server, webhook, CRIU, checkpointstore (Go)
deploy/helm/nvsnap/     Helm chart
deploy/k8s/             manifests + sample workloads
docker/                 Dockerfiles
scripts/                build / test / deploy / benchmark automation
docs/                   architecture, design, reference
terraform/              example GKE cluster provisioning
```

---

## Documentation

- [docs/](docs/README.md) — documentation index (operator + developer)
- [docs/operate/DEVELOPING.md](docs/operate/DEVELOPING.md) — build → capture → validate → merge
- [docs/operate/INSTALL.md](docs/operate/INSTALL.md) — full install guide + troubleshooting
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — system architecture
- [docs/START-HERE.md](docs/START-HERE.md) — what nvsnap is, in five minutes
- [docs/SUPPORT.md](docs/SUPPORT.md) — what nvsnap supports, refuses and has not tested
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) — every measured number, with its raw data
- [docs/GLOSSARY.md](docs/GLOSSARY.md) · [docs/REFERENCE.md](docs/REFERENCE.md) — words; annotations, env vars, agent routes
- [docs/design/THIRD-PARTY-FORKS.md](docs/design/THIRD-PARTY-FORKS.md) — forked dependencies + patch policy
- [CONTRIBUTING.md](CONTRIBUTING.md) — developer setup + build-from-source

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for developer setup, building from
source, and the forked-dependency layout. Issues and merge requests are
welcome.

## License

Apache 2.0 — see [LICENSE](LICENSE). Forked third-party components retain
their original licenses; see [NOTICE](NOTICE).
