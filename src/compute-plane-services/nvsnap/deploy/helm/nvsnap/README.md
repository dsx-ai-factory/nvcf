# NvSnap Helm chart

GPU checkpoint/restore for Kubernetes. The chart deploys the nvsnap agent
DaemonSet on GPU nodes (it also serves the mutating admission webhook), the
nvsnap server (REST API, web UI and catalog), the webhook registration and its
cert-manager certificate, and optionally cert-manager, the NVIDIA GPU Operator,
Jaeger, Prometheus and Grafana as subcharts.

Install, configuration choices, upgrade, uninstall and troubleshooting are in
[docs/operate/INSTALL.md](../../../docs/operate/INSTALL.md). Every value is
documented in [values.yaml](values.yaml); what the components do is in
[docs/ARCHITECTURE.md](../../../docs/ARCHITECTURE.md).

## Templates

| Template | Renders | When |
|---|---|---|
| `agent-daemonset.yaml`, `agent-rbac.yaml` | The agent DaemonSet, its Services (the node-local one only without `agent.hostNetwork`) and its RBAC | `agent.enabled` |
| `agent-token-secret.yaml` | The agent API token | `agent.auth.enabled` |
| `agent-servicemonitor.yaml`, `prometheus-rules.yaml` | Agent scrape config and alert rules | `agent.metrics.*.enabled` (`auto`: when the CRD exists) |
| `server.yaml` | The server Deployment, Service, RBAC and, with `server.persistence.enabled`, the catalog PVC and its mount | `server.enabled` |
| `nvsnapfunction-crd.yaml` | The `NvsnapFunction` CRD and a ClusterRole for it | `server.enabled` |
| `webhook.yaml` | The webhook Service, certificate and, with `webhook.inject`, the MutatingWebhookConfiguration | `webhook.enabled` |
| `ephemeral-container-policy.yaml` | A ValidatingAdmissionPolicy that keeps ephemeral containers off nvsnap volumes | `webhook.enabled` and `webhook.ephemeralContainerPolicy.enabled` |
| `cachedir-env-configmap.yaml` | The cachedir environment template the webhook reads | `agent.podCacheDir` set |
| `network-policy-restore-pods.yaml` | Lets restore pods in `agent.l2.restoreNamespaces` reach the server (and the agent with `webhook.restorePrepStrategy: init-container`) | `webhook.enabled` and that list set |
| `rails.yaml` | The RDMA claim template and shared secret of the rails fetch tier | `agent.rails.enabled` |
| `grafana-dashboard.yaml` | Overview dashboard ConfigMap from `files/` | `grafana.enabled` |
| `grafana-dashboard-history.yaml` | History dashboard ConfigMap from `files/` | `agent.metrics.dashboard.enabled` |
| `post-install-smoke.yaml` | The post-install health probe Job | `postInstallSmoke.enabled` |

`_helpers.tpl` holds shared template helpers and `NOTES.txt` the install
notes.

## Other directories

- `crds/`: the `GPUCheckpoint` and `GPURestore` CRDs; must match
  [`deploy/crds/nvsnap.io_gpucheckpoints.yaml`](../../crds/)
  (checked by `scripts/test-helm-chart.sh`).
- `files/`: the Grafana dashboards and the alert rules read by the
  templates above, the rules' promtool tests, and `test-rules.sh`, which runs
  them.

## Development

```bash
helm dependency build deploy/helm/nvsnap
helm lint deploy/helm/nvsnap
helm template nvsnap deploy/helm/nvsnap --namespace nvsnap-system
scripts/test-helm-chart.sh
```

When an image version moves, bump its tag in `values.yaml`.
