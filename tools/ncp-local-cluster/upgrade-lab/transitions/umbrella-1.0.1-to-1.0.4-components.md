# Component transition: umbrella-1.0.1-to-1.0.4

Qualification: unqualified. Inventory comparison does not prove runtime compatibility.

Route versions identify umbrella releases. Version 1.0.1 is a separate legacy baseline.

Comparison includes optional releases. Inventory differences do not establish profile applicability or runtime failure.

Blank cells mean no entry in that consumer's inventory. They do not establish runtime absence. Added and removed counts describe inventory entries.

Endpoint 1.0.1 reference: [Docs 1.0.1 matrix](https://docs.nvidia.com/nvcf/1.0.1/release-notes/compatibility-matrix).

Endpoint 1.0.4 original release: `nvcf/1.0.4` at `d46842d66fac896b7f7b9e8190a8f767f4db48cf`. Manifest `sha256:20656169ea9d017d9168df668ae3632e5dde3699c3e1737d4142ec73bcaf1421`; bundle `sha256:980a1c53818efce5afc01b7354819578c9449ee2350a1c9762a05878840239f1`.

Endpoint 1.0.4 reference: [Docs 1.0.4 matrix](https://docs.nvidia.com/nvcf/1.0.4/release-notes/compatibility-matrix).

Artifact lock: `sha256:cc80f91a1a1b758665576a12e38f38a2545df0fc4f9d09d8475a67f0677e0914`

Contract: `sha256:6f928142cc4ed67928b66da06f7f843454a7eb6492377c415e807807138843b7`

| Endpoint | Plane | Stack version | Source tag | Source commit | Inventory SHA-256 |
| --- | --- | --- | --- | --- | --- |
| 1.0.1 | control-plane | 1.0.1 | `deploy/stacks/self-managed/v1.0.1` | `67c010f24304b5c06d0493fa1b976a69b6ee8dea` | `sha256:126c831f631da5e7fccce9f26131cefba2c06b485e4a1985d707b9ba6166d614` |
| 1.0.1 | compute-plane | 1.0.0 | `deploy/stacks/nvcf-compute-plane/v1.0.0` | `63ac4456d3422508f2ff4b219c7dff8fba1c21c0` | `sha256:4417012d41bd09dd2b4076c28b1bb38b27199e1f8473b050d16d6a4011fba2a6` |
| 1.0.1 | observability | 1.0.0 | `deploy/stacks/observability/v1.0.0` | `b77e96a3f8c18e9318b93e697e33e596c4ede7d7` | `sha256:28ff55447c9223be0472ce9157fa9f4eb6dc7c5092a7c4281e29826e9f3df6de` |
| 1.0.4 | control-plane | 1.0.10 | `deploy/stacks/self-managed/v1.0.10` | `e4b11e6db4ec8ff2af228579493685eca391447f` | `sha256:aedbabdde512c3956bf716033a2d0d8ea08c2fbb1c7992e8e018f9ce2c2a81db` |
| 1.0.4 | compute-plane | 1.0.5 | `deploy/stacks/nvcf-compute-plane/v1.0.5` | `e6d55edf173c622f3e925cb4650051e3abc9ddfb` | `sha256:cd6a9081fc4777073a437fdfbd343e0cd365502a4478143896e73d5d79a74e33` |
| 1.0.4 | observability | 1.0.0 | `deploy/stacks/observability/v1.0.0` | `b77e96a3f8c18e9318b93e697e33e596c4ede7d7` | `sha256:28ff55447c9223be0472ce9157fa9f4eb6dc7c5092a7c4281e29826e9f3df6de` |

Changed: 55; added: 9; removed: 0; backward: 0; unordered: 0.

## Runtime artifact checks

- Resolve function/task launch helpers and inline receipt-hook images from effective configuration and live Pod specifications. An absent inventory entry does not establish runtime absence.
- Stack 1.0.1 NVCT remote configuration selects nvcf_worker_init:2.107.0, nvcf_worker_task:2.107.0 and ess-agent:1.1.0, omitted from its NVCT inventory entries. Original-reference access and actual payload identity require separate checks.

## Execution blockers

- The umbrella scope changes the control-plane endpoint to stack 1.0.10. Recheck action ordering, effective configuration, migration/runtime behavior and recovery for these exact inputs. Prior stack 1.0.4 rehearsals do not satisfy this gate.
- Before supported execution, bind enabled-profile artifact coverage and migration/runtime acceptance to the exact artifacts, configuration, topology and current run.
- Require matching HA preservation, mixed-fleet, consistent restore and end-to-end workload evidence for the intended profile. Scoped laboratory acceptance does not qualify broader configurations or failure recovery.
- Require native NATS stream/consumer configuration, storage-permission and replication checks across the repository change. Equal server binaries do not establish whole-image identity.
- Before backup or any hop action, require source-data-prerequisites with zero missing live requests, mismatches or failures; preserve the validated data epoch through the backup.
- Require original-reference identity for legacy NVCT helpers omitted from the source inventory before supported execution. Diagnostic repository access does not establish original byte equivalence.
- For drained-memory-cold-restore, require matching paused-source observations and consistent isolated full-cohort restore acceptance. Nonempty or in-flight memory recovery remains unsupported; an earlier restore does not establish recovery from a later migration failure.

## Component changes

| Plane | Consumer | Type | Name | Source | Target | Review |
| --- | --- | --- | --- | --- | --- | --- |
| compute-plane | dynamo-operator | container-image | nats-server-config-reloader | docker.io/natsio/nats-server-config-reloader:0.24.0 | docker.io/natsio/nats-server-config-reloader:0.25.0 |  |
| compute-plane | nvca-operator | container-image | byoo-otel-collector |  | nvcr.io/nvidia/nvcf/byoo-otel-collector:0.160.0-nv-0.2.9 |  |
| compute-plane | nvca-operator | container-image | nvca | nvcr.io/nvidia/nvcf/nvca:3.10.0 | nvcr.io/nvidia/nvcf/nvca:3.10.3 |  |
| compute-plane | nvca-operator | container-image | nvca-operator | nvcr.io/nvidia/nvcf/nvca-operator:3.10.0 | nvcr.io/nvidia/nvcf/nvca-operator:3.10.3 |  |
| compute-plane | nvca-operator | container-image | nvcf-image-credential-helper | nvcr.io/nvidia/nvcf/nvcf-image-credential-helper:0.11.1 | nvcr.io/nvidia/nvcf/nvcf-image-credential-helper:0.11.3 |  |
| compute-plane | nvca-operator | container-image | nvcf-otel-collector |  | nvcr.io/nvidia/nvcf/nvcf-otel-collector:0.160.0-nv-0.2.9 |  |
| compute-plane | nvca-operator | container-image | samba | nvcr.io/nvidia/nvcf/samba:1.0.5 | nvcr.io/nvidia/nvcf/samba:1.0.9 |  |
| compute-plane | nvcf-grove-topology | helm-chart | nvcf-cluster-topology | https://github.com/NVIDIA/nvcf/tree/63ac4456d3422508f2ff4b219c7dff8fba1c21c0/deploy/stacks/nvcf-compute-plane/charts/nvcf-cluster-topology@0.1.0 | https://github.com/NVIDIA/nvcf/tree/e6d55edf173c622f3e925cb4650051e3abc9ddfb/deploy/stacks/nvcf-compute-plane/charts/nvcf-cluster-topology@0.1.0 | Same version at a different location; verify byte identity |
| compute-plane | nvcf-kai-topology | helm-chart | nvcf-cluster-topology | https://github.com/NVIDIA/nvcf/tree/63ac4456d3422508f2ff4b219c7dff8fba1c21c0/deploy/stacks/nvcf-compute-plane/charts/nvcf-cluster-topology@0.1.0 | https://github.com/NVIDIA/nvcf/tree/e6d55edf173c622f3e925cb4650051e3abc9ddfb/deploy/stacks/nvcf-compute-plane/charts/nvcf-cluster-topology@0.1.0 | Same version at a different location; verify byte identity |
| control-plane | api-keys | container-image | nvcf-api-keys-service | nvcr.io/nvidia/nvcf/nvcf-api-keys-service:1.9.1 | nvcr.io/nvidia/nvcf/nvcf-api-keys-service:1.9.4 |  |
| control-plane | api-keys | helm-chart | helm-nvcf-api-keys | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-api-keys@1.8.0 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-api-keys@1.8.2 |  |
| control-plane | api | container-image | ess-agent | nvcr.io/nvidia/nvcf/ess-agent:1.4.1 | nvcr.io/nvidia/nvcf/ess-agent:1.4.4 |  |
| control-plane | api | container-image | nvcf-service-oss | nvcr.io/nvidia/nvcf/nvcf-service-oss:1.18.0 | nvcr.io/nvidia/nvcf/nvcf-service-oss:1.18.2 |  |
| control-plane | api | container-image | nvcf-worker-init-oss | nvcr.io/nvidia/nvcf/nvcf-worker-init-oss:1.2.1 | nvcr.io/nvidia/nvcf/nvcf-worker-init-oss:1.2.4 |  |
| control-plane | api | container-image | nvcf-worker-llm-credentials-oss | nvcr.io/nvidia/nvcf/nvcf-worker-llm-credentials-oss:1.1.2 | nvcr.io/nvidia/nvcf/nvcf-worker-llm-credentials-oss:1.1.5 |  |
| control-plane | api | container-image | nvcf-worker-utils-oss | nvcr.io/nvidia/nvcf/nvcf-worker-utils-oss:1.2.3 | nvcr.io/nvidia/nvcf/nvcf-worker-utils-oss:1.2.6 |  |
| control-plane | api | container-image | nvcf_worker_niclls | nvcr.io/nvidia/nvcf/nvcf_worker_niclls:2.110.0 | nvcr.io/nvidia/nvcf/nvcf_worker_niclls:2.118.2 |  |
| control-plane | api | helm-chart | helm-nvcf-api | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-api@1.27.1 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-api@1.27.5 |  |
| control-plane | cassandra | container-image | cassandra | nvcr.io/nvidia/nvcf/cassandra:5.0.9-nv-2.0.5 | nvcr.io/nvidia/nvcf/cassandra:5.0.9-nv-2.0.7 |  |
| control-plane | cassandra | container-image | nvcf-cassandra-migrations | nvcr.io/nvidia/nvcf/nvcf-cassandra-migrations:0.17.6 | nvcr.io/nvidia/nvcf/nvcf-cassandra-migrations:0.17.8 |  |
| control-plane | cassandra | helm-chart | helm-nvcf-cassandra | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-cassandra@0.21.3 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-cassandra@0.21.4 |  |
| control-plane | cert-manager | container-image | cert-manager-acmesolver | quay.io/jetstack/cert-manager-acmesolver:v1.20.2 | quay.io/jetstack/cert-manager-acmesolver:v1.20.5 |  |
| control-plane | cert-manager | container-image | cert-manager-cainjector | nvcr.io/nvidia/nvcf/cert-manager-cainjector:v1.20.2 | quay.io/jetstack/cert-manager-cainjector:v1.20.5 |  |
| control-plane | cert-manager | container-image | cert-manager-controller | nvcr.io/nvidia/nvcf/cert-manager-controller:v1.20.2 | quay.io/jetstack/cert-manager-controller:v1.20.5 |  |
| control-plane | cert-manager | container-image | cert-manager-startupapicheck | nvcr.io/nvidia/nvcf/cert-manager-startupapicheck:v1.20.2 | quay.io/jetstack/cert-manager-startupapicheck:v1.20.5 |  |
| control-plane | cert-manager | container-image | cert-manager-webhook | nvcr.io/nvidia/nvcf/cert-manager-webhook:v1.20.2 | quay.io/jetstack/cert-manager-webhook:v1.20.5 |  |
| control-plane | cert-manager | helm-chart | helm-nvcf-cert-manager | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-cert-manager@0.1.0 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-cert-manager@0.2.0 |  |
| control-plane | ess-api | container-image | nvcf-ess | nvcr.io/nvidia/nvcf/nvcf-ess:0.5.0 | nvcr.io/nvidia/nvcf/nvcf-ess:0.5.7 |  |
| control-plane | ess-api | helm-chart | helm-nvcf-ess-api | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-ess-api@1.8.2 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-ess-api@1.8.3 |  |
| control-plane | function-autoscaler | container-image | nvcf-function-autoscaler | nvcr.io/nvidia/nvcf/nvcf-function-autoscaler:1.21.8 | nvcr.io/nvidia/nvcf/nvcf-function-autoscaler:1.21.10 |  |
| control-plane | grpc-proxy | container-image | nvcf-grpc-proxy | nvcr.io/nvidia/nvcf/nvcf-grpc-proxy:1.33.5 | nvcr.io/nvidia/nvcf/nvcf-grpc-proxy:1.33.7 |  |
| control-plane | ingress | helm-chart | nvcf-gateway-routes | https://helm.ngc.nvidia.com/nvidia/nvcf/nvcf-gateway-routes@1.18.2 | https://helm.ngc.nvidia.com/nvidia/nvcf/nvcf-gateway-routes@1.18.3 |  |
| control-plane | invocation-service | container-image | nvcf-invocation-service | nvcr.io/nvidia/nvcf/nvcf-invocation-service:0.12.1 | nvcr.io/nvidia/nvcf/nvcf-invocation-service:0.12.3 |  |
| control-plane | invocation-service | helm-chart | helm-nvcf-invocation-service | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-invocation-service@1.6.1 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-invocation-service@1.6.2 |  |
| control-plane | llm-api-gateway | container-image | llm-api-gateway | nvcr.io/nvidia/nvcf/llm-api-gateway:0.14.2 | nvcr.io/nvidia/nvcf/llm-api-gateway:0.14.11 |  |
| control-plane | llm-request-router | container-image | nvcf-openbao-migrations | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.19.5 | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.19.11 |  |
| control-plane | llm-request-router | container-image | pylon |  | nvcr.io/nvidia/nvcf/pylon:0.18.3 |  |
| control-plane | llm-request-router | container-image | stargate | nvcr.io/nvidia/nvcf/stargate:0.18.0 | nvcr.io/nvidia/nvcf/stargate:0.18.3 |  |
| control-plane | llm-request-router | helm-chart | helm-nvcf-llm-request-router | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-llm-request-router@1.14.1 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-llm-request-router@1.14.3 |  |
| control-plane | nats-auth-callout-service | container-image | nvcf-nats-auth-callout-service | nvcr.io/nvidia/nvcf/nvcf-nats-auth-callout-service:0.8.3 | nvcr.io/nvidia/nvcf/nvcf-nats-auth-callout-service:0.8.6 |  |
| control-plane | nats | container-image | nats-box | nvcr.io/nvidia/nvcf/nats-box:0.19.7-nonroot | docker.io/natsio/nats-box:0.20.1-nonroot |  |
| control-plane | nats | container-image | nats-server | nvcr.io/nvidia/nvcf/nats-server:2.14.6-alpine3.22 | docker.io/synadia/nats-server:2.14.9-alpine3.22 |  |
| control-plane | nats | container-image | nats-server-config-reloader | docker.io/natsio/nats-server-config-reloader:0.24.0 | docker.io/natsio/nats-server-config-reloader:0.25.0 |  |
| control-plane | nats | helm-chart | helm-nvcf-nats | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-nats@0.8.4 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-nats@0.8.6 |  |
| control-plane | notary-service | container-image | nvcf-notary | nvcr.io/nvidia/nvcf/nvcf-notary:1.14.1 | nvcr.io/nvidia/nvcf/nvcf-notary:1.14.8 |  |
| control-plane | notary-service | helm-chart | helm-nvcf-notary-service | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-notary-service@1.6.0 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-notary-service@1.6.1 |  |
| control-plane | nvcf-ui | container-image | nvcf-openbao-migrations | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.19.5 | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.19.11 |  |
| control-plane | nvcf-ui | helm-chart | helm-nvcf-ui | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-ui@1.1.2 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-ui@1.1.4 |  |
| control-plane | nvct-api | container-image | ess-agent |  | nvcr.io/nvidia/nvcf/ess-agent:1.4.4 |  |
| control-plane | nvct-api | container-image | nvcf-worker-init-oss |  | nvcr.io/nvidia/nvcf/nvcf-worker-init-oss:1.2.4 |  |
| control-plane | nvct-api | container-image | nvcf-worker-task-oss |  | nvcr.io/nvidia/nvcf/nvcf-worker-task-oss:1.1.6 |  |
| control-plane | nvct-api | container-image | nvct-service-oss | nvcr.io/nvidia/nvcf/nvct-service-oss:1.66.0 | nvcr.io/nvidia/nvcf/nvct-service-oss:1.66.2 |  |
| control-plane | nvct-api | helm-chart | helm-nvcf-nvct-api | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-nvct-api@1.6.0 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-nvct-api@1.6.1 |  |
| control-plane | openbao-server | container-image | nvcf-openbao | nvcr.io/nvidia/nvcf/nvcf-openbao:2.6.2-nv-1.3.4 | nvcr.io/nvidia/nvcf/nvcf-openbao:2.6.3-nv-1.3.9 |  |
| control-plane | openbao-server | container-image | nvcf-openbao-migrations | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.19.5 | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.19.11 |  |
| control-plane | openbao-server | helm-chart | helm-nvcf-openbao-server | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-openbao-server@0.32.6 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-openbao-server@0.32.9 |  |
| control-plane | ratelimiter | container-image | nvcf-ratelimiter | nvcr.io/nvidia/nvcf/nvcf-ratelimiter:1.17.3 | nvcr.io/nvidia/nvcf/nvcf-ratelimiter:1.17.5 |  |
| control-plane | reval | container-image | reval-server | nvcr.io/nvidia/nvcf/reval-server:0.20.2 | nvcr.io/nvidia/nvcf/reval-server:0.20.3 |  |
| control-plane | sis | container-image | icms-service-oss | nvcr.io/nvidia/nvcf/icms-service-oss:0.7.2 | nvcr.io/nvidia/nvcf/icms-service-oss:0.7.7 |  |
| control-plane | sis | container-image | nvcf-openbao-migrations |  | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.19.11 |  |
| control-plane | sis | helm-chart | helm-nvcf-sis | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-sis@2.4.0 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-sis@2.4.2 |  |
| control-plane | upgrade-receipt | container-image | k8s |  | docker.io/alpine/k8s:1.37.0 |  |
| control-plane | upgrade-receipt | helm-chart | nvcf-upgrade-receipt |  | https://github.com/NVIDIA/nvcf/tree/e4b11e6db4ec8ff2af228579493685eca391447f/deploy/stacks/self-managed/charts/nvcf-upgrade-receipt@0.1.0 |  |
| control-plane | vanity-gateway | container-image | nvcf-ai-api-gateway-service | nvcr.io/nvidia/nvcf/nvcf-ai-api-gateway-service:1.35.1 | nvcr.io/nvidia/nvcf/nvcf-ai-api-gateway-service:1.35.3 |  |
