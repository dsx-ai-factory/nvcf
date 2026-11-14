#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Operator environment files written for the 1.0 line pin service images and the
# LLS rotation client through these keys. They must keep reaching the charts.
set -euo pipefail

stack_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
test_stack_dir="$work_dir/self-managed"
environment_name="legacy-image-keys-passthrough-test"
environment_file="$test_stack_dir/environments/$environment_name.yaml"
trap 'rm -rf "$work_dir"' EXIT

fail() {
  echo "legacy-image-keys-passthrough: $*" >&2
  exit 1
}

mkdir -p "$test_stack_dir"
cp -R "$stack_dir"/. "$test_stack_dir"
printf '{}\n' >"$test_stack_dir/secrets/$environment_name-secrets.yaml"

# render_values RELEASE OUTPUT
render_values() {
  HELMFILE_ENV="$environment_name" \
    HELMFILE_CACHE_HOME="$work_dir/helmfile-cache" \
    helmfile \
      --file "$test_stack_dir/helmfile.d/02-core.yaml.gotmpl" \
      --environment default \
      --state-values-set ingress.gatewayApi.controllerNamespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.shared.name=shared-gw \
      --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc-gw \
      --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy-gateway-system \
      --selector "name=$1" \
      write-values \
      --output-file-template "$2" >/dev/null ||
    fail "helmfile could not render $1"
}

cat >"$environment_file" <<'YAML'
global:
  image:
    registry: registry.example.com
    repository: example/nvcf
apikeys:
  image:
    tag: 9.9.1
invocation:
  image:
    tag: 9.9.2
grpcproxy:
  image:
    tag: 9.9.3
natsAuthCalloutService:
  image:
    tag: 9.9.4
addons:
  lls:
    enabled: true
    hmacRotation:
      kubectlMinor: "1.36"
YAML

render_values api-keys "$work_dir/api-keys.yaml"
test "$(yq -r '.apikeys.image.tag // ""' "$work_dir/api-keys.yaml")" = "9.9.1" || fail "apikeys.image.tag did not reach the chart"
render_values invocation-service "$work_dir/invocation.yaml"
test "$(yq -r '.invocation.image.tag // ""' "$work_dir/invocation.yaml")" = "9.9.2" || fail "invocation.image.tag did not reach the chart"
render_values grpc-proxy "$work_dir/grpc.yaml"
test "$(yq -r '.grpcproxy.image.tag // ""' "$work_dir/grpc.yaml")" = "9.9.3" || fail "grpcproxy.image.tag did not reach the chart"
render_values nats-auth-callout-service "$work_dir/callout.yaml"
test "$(yq -r '.image.tag // ""' "$work_dir/callout.yaml")" = "9.9.4" || fail "natsAuthCalloutService.image.tag did not reach the chart"
render_values sis "$work_dir/sis.yaml"
test "$(yq -r '.sis.lls.hmacRotation.kubectlMinor // ""' "$work_dir/sis.yaml")" = "1.36" || fail "addons.lls.hmacRotation.kubectlMinor did not reach the chart"

# Without the keys the chart owns the tag; the LLS client follows the OpenBao migrations client.
cat >"$environment_file" <<'YAML'
global:
  image:
    registry: registry.example.com
    repository: example/nvcf
openbao:
  migrations:
    kubectlMinor: "1.37"
addons:
  lls:
    enabled: true
YAML
render_values api-keys "$work_dir/api-keys-default.yaml"
test -z "$(yq -r '.apikeys.image.tag // ""' "$work_dir/api-keys-default.yaml")" || fail "apikeys image tag must stay with the chart when unset"
render_values sis "$work_dir/sis-default.yaml"
test "$(yq -r '.sis.lls.hmacRotation.kubectlMinor // ""' "$work_dir/sis-default.yaml")" = "1.37" || fail "LLS rotation client did not follow openbao.migrations.kubectlMinor"

echo "legacy-image-keys-passthrough: ok"
