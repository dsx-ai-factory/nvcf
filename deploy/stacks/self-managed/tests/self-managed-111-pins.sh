#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Asserts the dependency chart selection of the 1.1.x patch line.
set -euo pipefail

stack_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

fail() {
  echo "self-managed-111-pins: $*" >&2
  exit 1
}

chart_version() {
  awk -v r="$2" '$1=="-"&&$2=="name:"{rel=$3} rel==r&&$1=="version:"{print $2; exit}' "$stack_dir/helmfile.d/$1"
}

test "$(chart_version 01-dependencies.yaml.gotmpl openbao-server)" = "0.37.1" || fail "openbao-server chart is not 0.37.1"
test "$(chart_version 01-dependencies.yaml.gotmpl cert-manager)" = "0.2.0" || fail "cert-manager chart is not 0.2.0"
test "$(chart_version 01-dependencies.yaml.gotmpl nats)" = "0.9.1" || fail "nats chart is not 0.9.1"
test "$(chart_version 02-core.yaml.gotmpl invocation-service)" = "1.10.0" || fail "invocation-service chart is not 1.10.0"

work_dir="$(mktemp -d)"
test_stack_dir="$work_dir/self-managed"
environment_name="self-managed-111-pins-test"
trap 'rm -rf "$work_dir"' EXIT
mkdir -p "$test_stack_dir"
cp -R "$stack_dir"/. "$test_stack_dir"
printf '{}\n' >"$test_stack_dir/secrets/$environment_name-secrets.yaml"
cat >"$test_stack_dir/environments/$environment_name.yaml" <<'YAML'
global:
  image:
    registry: registry.example.com
    repository: example/nvcf
YAML

# render_values FILE RELEASE OUTPUT
render_values() {
  HELMFILE_ENV="$environment_name" \
    HELMFILE_CACHE_HOME="$work_dir/helmfile-cache" \
    helmfile \
      --file "$test_stack_dir/helmfile.d/$1" \
      --environment default \
      --state-values-set ingress.gatewayApi.controllerNamespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.shared.name=shared-gw \
      --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc-gw \
      --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy-gateway-system \
      --selector "name=$2" \
      write-values \
      --output-file-template "$3" >/dev/null ||
    fail "helmfile could not render $2"
}

assert_value() {
  yq -e "$2" "$1" >/dev/null 2>&1 || fail "$3"
}

render_values 01-dependencies.yaml.gotmpl nats "$work_dir/nats.yaml"
assert_value "$work_dir/nats.yaml" '.nats.container.image.tag == "2.14.9-alpine3.22"' "NATS server is not 2.14.9-alpine3.22"
test -z "$(yq -r '.nats.reloader.image.tag // ""' "$work_dir/nats.yaml")" || fail "the stack must leave the reloader tag to the chart (0.9.1 defaults to 0.25.0)"

# OpenBao: lifecycle mode, server and injected agent images, migrations tag.
render_values 01-dependencies.yaml.gotmpl openbao-server "$work_dir/openbao.yaml"
assert_value "$work_dir/openbao.yaml" '.openbao.jwtPluginLifecycle.mode == "versioned"' "OpenBao lifecycle mode is not versioned"
assert_value "$work_dir/openbao.yaml" '.openbao.server.image.tag == "2.6.3-nv-1.3.9"' "OpenBao server image is not 2.6.3-nv-1.3.9"
assert_value "$work_dir/openbao.yaml" '.openbao.injector.agentImage.tag == "2.6.3-nv-1.3.9"' "OpenBao injected agent image is not 2.6.3-nv-1.3.9"
assert_value "$work_dir/openbao.yaml" '.openbao.migrations.image.tag == "0.23.4"' "OpenBao migrations image is not 0.23.4"

# An existing installation sets the bridge mode and keeps its own image pins.
cat >"$test_stack_dir/environments/$environment_name.yaml" <<'YAML'
global:
  image:
    registry: registry.example.com
    repository: example/nvcf
openbao:
  jwtPluginLifecycle:
    mode: bridge
  server:
    image:
      tag: 2.6.3-nv-1.3.9
  injector:
    agentImage:
      tag: 2.6.3-nv-1.3.9
YAML
render_values 01-dependencies.yaml.gotmpl openbao-server "$work_dir/openbao-bridge.yaml"
assert_value "$work_dir/openbao-bridge.yaml" '.openbao.jwtPluginLifecycle.mode == "bridge"' "operator bridge mode was not passed through"

# The upgrade receipt hook uses the same alpine/k8s as the API bootstrap job.
test "$(yq -r '.image.tag' "$stack_dir/charts/nvcf-upgrade-receipt/values.yaml")" = "1.37.0" || fail "upgrade receipt helper image is not alpine/k8s 1.37.0"

# Component releases of the 1.1.1 patch, selected by the stack (the charts default to older tags).
cat >"$test_stack_dir/environments/$environment_name.yaml" <<'YAML'
global:
  image:
    registry: registry.example.com
    repository: example/nvcf
addons:
  llm:
    enabled: true
  vanityGateway:
    enabled: true
YAML
render_values 02-core.yaml.gotmpl llm-request-router "$work_dir/router.yaml"
assert_value "$work_dir/router.yaml" '.llmRequestRouter.image.tag == "0.29.3"' "stargate (and pylon) is not 0.29.3"
render_values 02-core.yaml.gotmpl vanity-gateway "$work_dir/vanity.yaml"
assert_value "$work_dir/vanity.yaml" '.vanityGateway.image.tag == "1.42.2"' "vanity gateway is not 1.42.2"
render_values 02-core.yaml.gotmpl sis "$work_dir/sis.yaml"
assert_value "$work_dir/sis.yaml" '.sis.image.tag == "0.8.6"' "ICMS is not 0.8.6"
render_values 02-core.yaml.gotmpl api "$work_dir/api.yaml"
assert_value "$work_dir/api.yaml" '.api.remoteConfig.workerImageTags.llmCredentials == "1.2.3"' "worker llm credentials is not 1.2.3"
render_values 03-observability.yaml.gotmpl function-autoscaler "$work_dir/autoscaler.yaml"
assert_value "$work_dir/autoscaler.yaml" '.functionautoscaler.image.tag == "1.23.2"' "function autoscaler is not 1.23.2"

# Operator overrides of these tags still win.
cat >"$test_stack_dir/environments/$environment_name.yaml" <<'YAML'
global:
  image:
    registry: registry.example.com
    repository: example/nvcf
sis:
  image:
    tag: 9.9.9
functionAutoscaler:
  image:
    tag: 9.9.8
api:
  remoteConfig:
    workerImageTags:
      llmCredentials: 9.9.7
YAML
render_values 02-core.yaml.gotmpl sis "$work_dir/sis-override.yaml"
assert_value "$work_dir/sis-override.yaml" '.sis.image.tag == "9.9.9"' "operator ICMS tag was lost"
render_values 03-observability.yaml.gotmpl function-autoscaler "$work_dir/autoscaler-override.yaml"
assert_value "$work_dir/autoscaler-override.yaml" '.functionautoscaler.image.tag == "9.9.8"' "operator function autoscaler tag was lost"
render_values 02-core.yaml.gotmpl api "$work_dir/api-override.yaml"
assert_value "$work_dir/api-override.yaml" '.api.remoteConfig.workerImageTags.llmCredentials == "9.9.7"' "operator worker llm credentials tag was lost"

echo "self-managed-111-pins: ok"
