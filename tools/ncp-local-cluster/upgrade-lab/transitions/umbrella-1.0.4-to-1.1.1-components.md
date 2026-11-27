# Component transition: umbrella-1.0.4-to-1.1.1

Qualification: unqualified. Inventory comparison does not prove runtime compatibility.

Route versions identify umbrella releases. Version 1.0.1 is a separate legacy baseline.

Comparison includes optional releases. Inventory differences do not establish profile applicability or runtime failure.

Blank cells mean no entry in that consumer's inventory. They do not establish runtime absence. Added and removed counts describe inventory entries.

Endpoint 1.0.4 original release: `nvcf/1.0.4` at `d46842d66fac896b7f7b9e8190a8f767f4db48cf`. Manifest `sha256:20656169ea9d017d9168df668ae3632e5dde3699c3e1737d4142ec73bcaf1421`; bundle `sha256:980a1c53818efce5afc01b7354819578c9449ee2350a1c9762a05878840239f1`.

Endpoint 1.0.4 reference: [Docs 1.0.4 matrix](https://docs.nvidia.com/nvcf/1.0.4/release-notes/compatibility-matrix).

Endpoint 1.1.1 original release: `nvcf/1.1.1` at `2034fdcabd36f9119d4be9ff1ecd4ca9dd3e06b6`. Manifest `sha256:e05095b005c18c091655a5f37fb796cd20ca420d48f0f7875c1f8f4f318fe9ec`; bundle `sha256:8f122ad03f329d62cd06eb7f3aebac3c019e4e44b0a5105b0c7b0cf20a3e2f15`.

Endpoint 1.1.1 reference: [Docs 1.1.1 matrix](https://docs.nvidia.com/nvcf/1.1.1/release-notes/compatibility-matrix).

Artifact lock: `sha256:cc80f91a1a1b758665576a12e38f38a2545df0fc4f9d09d8475a67f0677e0914`

Contract: `sha256:63772835a40420415875b4c89607be9dcdf2fd7b4725f01ffc37113c0986f65b`

| Endpoint | Plane | Stack version | Source tag | Source commit | Inventory SHA-256 |
| --- | --- | --- | --- | --- | --- |
| 1.0.4 | control-plane | 1.0.10 | `deploy/stacks/self-managed/v1.0.10` | `e4b11e6db4ec8ff2af228579493685eca391447f` | `sha256:aedbabdde512c3956bf716033a2d0d8ea08c2fbb1c7992e8e018f9ce2c2a81db` |
| 1.0.4 | compute-plane | 1.0.5 | `deploy/stacks/nvcf-compute-plane/v1.0.5` | `e6d55edf173c622f3e925cb4650051e3abc9ddfb` | `sha256:cd6a9081fc4777073a437fdfbd343e0cd365502a4478143896e73d5d79a74e33` |
| 1.0.4 | observability | 1.0.0 | `deploy/stacks/observability/v1.0.0` | `b77e96a3f8c18e9318b93e697e33e596c4ede7d7` | `sha256:28ff55447c9223be0472ce9157fa9f4eb6dc7c5092a7c4281e29826e9f3df6de` |
| 1.1.1 | control-plane | 1.1.9 | `deploy/stacks/self-managed/v1.1.9` | `859ad9e45d45f0503ea3759d7b2d07f5a5eb75df` | `sha256:db0872dce895352cbd2df6049ff25e9b5a21927b6f69c2fba59bf14a43be2446` |
| 1.1.1 | compute-plane | 1.1.1 | `deploy/stacks/nvcf-compute-plane/v1.1.1` | `daf87d512a7980e3124a4d69d5e449458a5bb5e9` | `sha256:c522c360cba885ae1136905d47b30033dd14b9800df84058f0a5b0f0ea9ffbdb` |
| 1.1.1 | observability | 1.1.0 | `deploy/stacks/observability/v1.1.0` | `d1918a811df7e7bc6be7d6cc625844b6f5e30b42` | `sha256:3833771ad85f14af832fcd60b9de95a48dd05af2f5e666c5a5d6324a5af91c82` |

Changed: 58; added: 6; removed: 3; backward: 0; unordered: 3.

## Runtime artifact checks

- Resolve function/task launch helpers and inline receipt-hook images from effective configuration and live Pod specifications. An absent inventory entry does not establish runtime absence.

## Execution blockers

- This lab contract remains unqualified and supported execution remains disabled. Published guide validation does not validate this lab profile or WorkloadGate integration.
- The published guide did not live-test control 1.1.9 with compute 1.0.5 during the upgrade window. Require matching evidence before accepting that temporary state.
- Bind artifact access, enabled-profile renders and migration evidence to the exact artifacts, configuration, topology and run.
- Preserve current source-data checks and a reviewed consistent recovery boundary before any future upgrade execution.
- Review OpenBao chart and migration changes while retaining its versioned JWT lifecycle, original identities and required plugins.

## Conditional checks

Applicability requires effective source/target values and rendered manifests. The audit does not evaluate these rules.

### ha-mode-and-topology

Applies when: The selected highAvailability.mode or effective replica, placement and storage settings differ from the source profile.

- When applicable: Review capacity, replica sizing, Cassandra replication and placement before the affected action. HA conversion requires separate qualification.
- When not applicable: Retain effective values and renders that demonstrate unchanged topology.
- When unknown: Resolve the selected mode and render both endpoints before the affected action. The audit does not evaluate applicability.

Required evidence:

- effective highAvailability.mode and secrets-file overrides
- source and target replicas, placement, storage and Cassandra RF/DC settings

### optional-profile-artifacts

Applies when: Effective values enable or change optional components, worker sidecars or external image overrides.

- When applicable: Review the selected references, access and prerequisites for each enabled component.
- When not applicable: Retain values and renders that demonstrate absence. An inventory entry alone does not establish runtime use.
- When unknown: Resolve effective values and rendered references before the affected action.

Required evidence:

- effective feature flags and image overrides
- rendered references and matching inventories

## Component changes

| Plane | Consumer | Type | Name | Source | Target | Review |
| --- | --- | --- | --- | --- | --- | --- |
| compute-plane | nvca-operator | container-image | byoo-otel-collector | nvcr.io/nvidia/nvcf/byoo-otel-collector:0.160.0-nv-0.2.9 | nvcr.io/nvidia/nvcf/byoo-otel-collector:0.160.0-nv-0.4.9 |  |
| compute-plane | nvca-operator | container-image | nvca | nvcr.io/nvidia/nvcf/nvca:3.10.3 | nvcr.io/nvidia/nvcf/nvca:3.18.10 |  |
| compute-plane | nvca-operator | container-image | nvca-operator | nvcr.io/nvidia/nvcf/nvca-operator:3.10.3 | nvcr.io/nvidia/nvcf/nvca-operator:3.18.10 |  |
| compute-plane | nvca-operator | container-image | nvcf-image-credential-helper | nvcr.io/nvidia/nvcf/nvcf-image-credential-helper:0.11.3 | nvcr.io/nvidia/nvcf/nvcf-image-credential-helper:0.11.4 |  |
| compute-plane | nvca-operator | container-image | nvcf-otel-collector | nvcr.io/nvidia/nvcf/nvcf-otel-collector:0.160.0-nv-0.2.9 | nvcr.io/nvidia/nvcf/nvcf-otel-collector:0.160.0-nv-0.4.9 |  |
| compute-plane | nvca-operator | helm-chart | helm-nvca-operator | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvca-operator@1.28.0 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvca-operator@1.37.1 |  |
| compute-plane | nvcf-grove-topology | helm-chart | nvcf-cluster-topology | https://github.com/NVIDIA/nvcf/tree/e6d55edf173c622f3e925cb4650051e3abc9ddfb/deploy/stacks/nvcf-compute-plane/charts/nvcf-cluster-topology@0.1.0 | https://github.com/NVIDIA/nvcf/tree/daf87d512a7980e3124a4d69d5e449458a5bb5e9/deploy/stacks/nvcf-compute-plane/charts/nvcf-cluster-topology@0.1.0 | Same version at a different location; verify byte identity |
| compute-plane | nvcf-kai-topology | helm-chart | nvcf-cluster-topology | https://github.com/NVIDIA/nvcf/tree/e6d55edf173c622f3e925cb4650051e3abc9ddfb/deploy/stacks/nvcf-compute-plane/charts/nvcf-cluster-topology@0.1.0 | https://github.com/NVIDIA/nvcf/tree/daf87d512a7980e3124a4d69d5e449458a5bb5e9/deploy/stacks/nvcf-compute-plane/charts/nvcf-cluster-topology@0.1.0 | Same version at a different location; verify byte identity |
| control-plane | admin-issuer-proxy | container-image | admin-token-issuer-proxy | nvcr.io/nvidia/nvcf/admin-token-issuer-proxy:1.1.2 | nvcr.io/nvidia/nvcf/admin-token-issuer-proxy:1.1.3 |  |
| control-plane | admin-issuer-proxy | helm-chart | helm-admin-token-issuer-proxy | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-admin-token-issuer-proxy@1.5.3 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-admin-token-issuer-proxy@1.7.0 |  |
| control-plane | api-keys | container-image | nvcf-api-keys-service | nvcr.io/nvidia/nvcf/nvcf-api-keys-service:1.9.4 | nvcr.io/nvidia/nvcf/nvcf-api-keys-service:1.10.10 |  |
| control-plane | api-keys | helm-chart | helm-nvcf-api-keys | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-api-keys@1.8.2 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-api-keys@1.9.4 |  |
| control-plane | api | container-image | ess-agent | nvcr.io/nvidia/nvcf/ess-agent:1.4.4 | nvcr.io/nvidia/nvcf/ess-agent:1.4.5 |  |
| control-plane | api | container-image | nvcf-service-oss | nvcr.io/nvidia/nvcf/nvcf-service-oss:1.18.2 | nvcr.io/nvidia/nvcf/nvcf-service-oss:1.28.4 |  |
| control-plane | api | container-image | nvcf-worker-init-oss | nvcr.io/nvidia/nvcf/nvcf-worker-init-oss:1.2.4 | nvcr.io/nvidia/nvcf/nvcf-worker-init-oss:1.3.2 |  |
| control-plane | api | container-image | nvcf-worker-llm-credentials-oss | nvcr.io/nvidia/nvcf/nvcf-worker-llm-credentials-oss:1.1.5 | nvcr.io/nvidia/nvcf/nvcf-worker-llm-credentials-oss:1.2.3 |  |
| control-plane | api | container-image | nvcf-worker-utils-oss | nvcr.io/nvidia/nvcf/nvcf-worker-utils-oss:1.2.6 | nvcr.io/nvidia/nvcf/nvcf-worker-utils-oss:1.4.2 |  |
| control-plane | api | container-image | nvcf_worker_niclls | nvcr.io/nvidia/nvcf/nvcf_worker_niclls:2.118.2 | nvcr.io/nvidia/nvcf/nvcf_worker_niclls:2.118.5 |  |
| control-plane | api | helm-chart | helm-nvcf-api | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-api@1.27.5 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-api@1.35.0 |  |
| control-plane | cassandra | container-image | nvcf-cassandra-migrations | nvcr.io/nvidia/nvcf/nvcf-cassandra-migrations:0.17.8 | nvcr.io/nvidia/nvcf/nvcf-cassandra-migrations:0.19.4 |  |
| control-plane | cassandra | helm-chart | helm-nvcf-cassandra | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-cassandra@0.21.4 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-cassandra@0.26.0 |  |
| control-plane | ess-api | helm-chart | helm-nvcf-ess-api | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-ess-api@1.8.3 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-ess-api@1.11.1 |  |
| control-plane | event-ledger | container-image | nvcf-event-ledger |  | nvcr.io/nvidia/nvcf/nvcf-event-ledger:0.21.2 |  |
| control-plane | event-ledger | helm-chart | helm-nvcf-event-ledger |  | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-event-ledger@0.10.2 |  |
| control-plane | function-autoscaler | container-image | nvcf-function-autoscaler | nvcr.io/nvidia/nvcf/nvcf-function-autoscaler:1.21.10 | nvcr.io/nvidia/nvcf/nvcf-function-autoscaler:1.23.2 |  |
| control-plane | function-autoscaler | helm-chart | helm-nvcf-function-autoscaler | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-function-autoscaler@0.5.2 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-function-autoscaler@0.8.1 |  |
| control-plane | grpc-proxy | container-image | nvcf-grpc-proxy | nvcr.io/nvidia/nvcf/nvcf-grpc-proxy:1.33.7 | nvcr.io/nvidia/nvcf/nvcf-grpc-proxy:1.35.2 |  |
| control-plane | grpc-proxy | helm-chart | helm-nvcf-grpc-proxy | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-grpc-proxy@1.7.4 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-grpc-proxy@1.14.0 |  |
| control-plane | invocation-service | container-image | nvcf-invocation-service | nvcr.io/nvidia/nvcf/nvcf-invocation-service:0.12.3 | nvcr.io/nvidia/nvcf/nvcf-invocation-service:0.15.1 |  |
| control-plane | invocation-service | helm-chart | helm-nvcf-invocation-service | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-invocation-service@1.6.2 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-invocation-service@1.10.0 |  |
| control-plane | llm-api-gateway | container-image | llm-api-gateway | nvcr.io/nvidia/nvcf/llm-api-gateway:0.14.11 | nvcr.io/nvidia/nvcf/llm-api-gateway:0.20.1 |  |
| control-plane | llm-api-gateway | helm-chart | helm-nvcf-llm-api-gateway | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-llm-api-gateway@1.4.3 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-llm-api-gateway@1.12.0 |  |
| control-plane | llm-request-router | container-image | nvcf-openbao-migrations | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.19.11 |  | Verify removal and old consumers before contract changes |
| control-plane | llm-request-router | container-image | pylon | nvcr.io/nvidia/nvcf/pylon:0.18.3 | nvcr.io/nvidia/nvcf/pylon:0.29.3 |  |
| control-plane | llm-request-router | container-image | stargate | nvcr.io/nvidia/nvcf/stargate:0.18.3 |  | Multiple versions in this consumer; inspect rendered use without inferring a version pairing |
| control-plane | llm-request-router | container-image | stargate |  | nvcr.io/nvidia/nvcf/stargate:0.29.2 | Multiple versions in this consumer; inspect rendered use without inferring a version pairing |
| control-plane | llm-request-router | container-image | stargate |  | nvcr.io/nvidia/nvcf/stargate:0.29.3 | Multiple versions in this consumer; inspect rendered use without inferring a version pairing |
| control-plane | llm-request-router | helm-chart | helm-nvcf-llm-request-router | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-llm-request-router@1.14.3 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-llm-request-router@1.18.2 |  |
| control-plane | nats-auth-callout-service | container-image | nvcf-nats-auth-callout-service | nvcr.io/nvidia/nvcf/nvcf-nats-auth-callout-service:0.8.6 | nvcr.io/nvidia/nvcf/nvcf-nats-auth-callout-service:0.8.8 |  |
| control-plane | nats-auth-callout-service | helm-chart | helm-nvcf-nats-auth-callout-service | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-nats-auth-callout-service@1.2.1 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-nats-auth-callout-service@1.4.2 |  |
| control-plane | nats | helm-chart | helm-nvcf-nats | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-nats@0.8.6 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-nats@0.9.1 |  |
| control-plane | notary-service | helm-chart | helm-nvcf-notary-service | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-notary-service@1.6.1 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-notary-service@1.9.1 |  |
| control-plane | nvcf-pki | helm-chart | helm-nvcf-pki | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-pki@0.1.0 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-pki@0.2.0 |  |
| control-plane | nvcf-ui | container-image | nvcf-openbao-migrations | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.19.11 | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.23.4 |  |
| control-plane | nvct-api | container-image | ess-agent | nvcr.io/nvidia/nvcf/ess-agent:1.4.4 | nvcr.io/nvidia/nvcf/ess-agent:1.4.5 |  |
| control-plane | nvct-api | container-image | nvcf-worker-init-oss | nvcr.io/nvidia/nvcf/nvcf-worker-init-oss:1.2.4 | nvcr.io/nvidia/nvcf/nvcf-worker-init-oss:1.3.2 |  |
| control-plane | nvct-api | container-image | nvcf-worker-task-oss | nvcr.io/nvidia/nvcf/nvcf-worker-task-oss:1.1.6 | nvcr.io/nvidia/nvcf/nvcf-worker-task-oss:1.2.4 |  |
| control-plane | nvct-api | container-image | nvct-service-oss | nvcr.io/nvidia/nvcf/nvct-service-oss:1.66.2 | nvcr.io/nvidia/nvcf/nvct-service-oss:1.70.4 |  |
| control-plane | nvct-api | helm-chart | helm-nvcf-nvct-api | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-nvct-api@1.6.1 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-nvct-api@1.11.0 |  |
| control-plane | openbao-server | container-image | nvcf-openbao-migrations | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.19.11 | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.23.4 |  |
| control-plane | openbao-server | helm-chart | helm-nvcf-openbao-server | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-openbao-server@0.32.9 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-openbao-server@0.37.1 |  |
| control-plane | ratelimiter | container-image | nvcf-ratelimiter | nvcr.io/nvidia/nvcf/nvcf-ratelimiter:1.17.5 | nvcr.io/nvidia/nvcf/nvcf-ratelimiter:1.18.3 |  |
| control-plane | ratelimiter | helm-chart | helm-nvcf-rate-limiter | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-rate-limiter@1.2.1 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-rate-limiter@1.4.1 |  |
| control-plane | reval | container-image | reval-server | nvcr.io/nvidia/nvcf/reval-server:0.20.3 | nvcr.io/nvidia/nvcf/reval-server:0.20.4 |  |
| control-plane | reval | helm-chart | helm-reval | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-reval@1.4.1 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-reval@1.6.0 |  |
| control-plane | sis | container-image | icms-service-oss | nvcr.io/nvidia/nvcf/icms-service-oss:0.7.7 | nvcr.io/nvidia/nvcf/icms-service-oss:0.8.6 |  |
| control-plane | sis | container-image | nvcf-openbao-migrations | nvcr.io/nvidia/nvcf/nvcf-openbao-migrations:0.19.11 |  | Verify removal and old consumers before contract changes |
| control-plane | sis | helm-chart | helm-nvcf-sis | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-sis@2.4.2 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-sis@2.6.3 |  |
| control-plane | state-metrics | container-image | nvcf-state-metrics-service | nvcr.io/nvidia/nvcf/nvcf-state-metrics-service:1.23.7 | nvcr.io/nvidia/nvcf/nvcf-state-metrics-service:1.24.1 |  |
| control-plane | state-metrics | helm-chart | helm-nvcf-state-metrics | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-state-metrics@1.0.2 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-state-metrics@1.0.7 |  |
| control-plane | upgrade-receipt | helm-chart | nvcf-upgrade-receipt | https://github.com/NVIDIA/nvcf/tree/e4b11e6db4ec8ff2af228579493685eca391447f/deploy/stacks/self-managed/charts/nvcf-upgrade-receipt@0.1.0 | https://github.com/NVIDIA/nvcf/tree/859ad9e45d45f0503ea3759d7b2d07f5a5eb75df/deploy/stacks/self-managed/charts/nvcf-upgrade-receipt@0.1.0 | Same version at a different location; verify byte identity |
| control-plane | vanity-gateway | container-image | nvcf-ai-api-gateway-service | nvcr.io/nvidia/nvcf/nvcf-ai-api-gateway-service:1.35.3 | nvcr.io/nvidia/nvcf/nvcf-ai-api-gateway-service:1.42.2 |  |
| control-plane | vanity-gateway | helm-chart | helm-nvcf-vanity-gateway | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-vanity-gateway@0.5.0 | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-vanity-gateway@0.11.0 |  |
| control-plane | worker-callback-proxy | container-image | envoy |  | envoyproxy/envoy:v1.40.1 |  |
| control-plane | worker-callback-proxy | helm-chart | helm-nvcf-worker-callback-proxy |  | https://helm.ngc.nvidia.com/nvidia/nvcf/helm-nvcf-worker-callback-proxy@0.1.0 |  |
| observability | default-monitors | helm-chart | nvcf-default-monitors | https://github.com/NVIDIA/nvcf/tree/b77e96a3f8c18e9318b93e697e33e596c4ede7d7/deploy/stacks/observability/charts/nvcf-default-monitors@0.2.0 | https://github.com/dsx-ai-factory/nvcf/tree/d1918a811df7e7bc6be7d6cc625844b6f5e30b42/deploy/stacks/observability/charts/nvcf-default-monitors@0.3.0 |  |
| observability | otel-collector | helm-chart | nvcf-otel-collector | https://github.com/NVIDIA/nvcf/tree/b77e96a3f8c18e9318b93e697e33e596c4ede7d7/deploy/stacks/observability/charts/nvcf-otel-collector@0.2.0 | https://github.com/dsx-ai-factory/nvcf/tree/d1918a811df7e7bc6be7d6cc625844b6f5e30b42/deploy/stacks/observability/charts/nvcf-otel-collector@0.3.0 |  |
