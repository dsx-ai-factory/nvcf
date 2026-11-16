#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Test that the global control-plane tolerations reach the chart value paths of
# NATS auth-callout, NVCT, the rate limiter and the gRPC proxy, with and without
# highAvailability.mode.

set -euo pipefail

stack_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
test_stack_dir="$work_dir/self-managed"
environment_name="control-tolerations-wiring-test"
environment_file="$test_stack_dir/environments/$environment_name.yaml"
secrets_file="$test_stack_dir/secrets/$environment_name-secrets.yaml"
trap 'rm -rf "$work_dir"' EXIT

fail() {
  echo "control-tolerations-wiring: $*" >&2
  exit 1
}

mkdir -p "$test_stack_dir"
cp -R "$stack_dir"/. "$test_stack_dir"
printf '{}\n' >"$secrets_file"

write_env() {
  local mode="$1" tolerations="$2"

  cat >"$environment_file" <<YAML
global:
  image:
    registry: registry.example.com
    repository: example/nvcf
  tolerations:
    enabled: $tolerations
    all:
      - key: nvidia.com/gpu
        operator: Exists
        effect: NoSchedule
highAvailability:
  mode: $mode
YAML
}

render_values() {
  local release="$1" output_file="$2"

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
    --selector "name=$release" \
    write-values \
    --output-file-template "$output_file" >/dev/null ||
    fail "helmfile could not render $release values"
}

# release name and the path its chart reads for Deployment tolerations
check_release() {
  local release="$1" path="$2" mode="$3" tolerations="$4" expected="$5" values_file actual

  write_env "$mode" "$tolerations"
  values_file="$work_dir/$release-$mode-$tolerations.yaml"
  render_values "$release" "$values_file"
  actual="$(yq -r "$path | length" "$values_file")"
  test "$actual" = "$expected" ||
    fail "$release $path has $actual entries with mode=$mode tolerations.enabled=$tolerations, expected $expected"
  if test "$expected" != 0; then
    test "$(yq -r "$path[0].key" "$values_file")" = "nvidia.com/gpu" ||
      fail "$release $path does not carry the configured toleration"
  fi
}

for mode in none enforced; do
  check_release nats-auth-callout-service '.tolerations' "$mode" true 1
  check_release nvct-api '.nvctApi.tolerations' "$mode" true 1
  check_release ratelimiter '.rateLimiter.tolerations' "$mode" true 1
  check_release grpc-proxy '.grpcproxy.deployment.tolerations' "$mode" true 1
done

# Disabled tolerations add nothing.
check_release nats-auth-callout-service '(.tolerations // [])' none false 0
check_release nvct-api '(.nvctApi.tolerations // [])' none false 0
check_release ratelimiter '(.rateLimiter.tolerations // [])' none false 0
check_release grpc-proxy '(.grpcproxy.deployment.tolerations // [])' none false 0

# The gRPC proxy keeps its HA placement next to the tolerations.
write_env enforced true
render_values grpc-proxy "$work_dir/grpc-ha.yaml"
yq -e '.grpcproxy.deployment.affinity and .grpcproxy.deployment.autoscaling' "$work_dir/grpc-ha.yaml" >/dev/null 2>&1 ||
  fail "grpc-proxy lost its HA placement or autoscaling under deployment"

echo "control-tolerations-wiring: ok"
