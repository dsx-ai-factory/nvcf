#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

stack_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_dir="$(cd "$stack_dir/../../.." && pwd)"
chart="$repo_dir/deploy/helm/vanity-gateway/helm-nvcf-vanity-gateway"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

# Render against a disposable stack copy so the test needs no local secrets.
test_stack_dir="$work_dir/self-managed"
cp -R "$stack_dir" "$test_stack_dir"
printf '{}\n' >"$test_stack_dir/secrets/base-secrets.yaml"

assert_limit() {
  local manifest="$1" expected="$2"
  grep -q "^  MAX_REQUEST_SIZE_BYTES: \"$expected\"$" "$manifest" || {
    echo "expected MAX_REQUEST_SIZE_BYTES=$expected in $manifest" >&2
    exit 1
  }
}

helm template vanity-gateway "$chart" >"$work_dir/default.yaml"
assert_limit "$work_dir/default.yaml" 26214400
for limit in 1 67108864; do
  helm template vanity-gateway "$chart" \
    --set "vanityGateway.config.maxRequestSizeBytes=$limit" >"$work_dir/custom.yaml"
  assert_limit "$work_dir/custom.yaml" "$limit"
done

for limit in 0 -1 1.5 nope null 9223372036854775808; do
  if helm template vanity-gateway "$chart" \
    --set "vanityGateway.config.maxRequestSizeBytes=$limit" >"$work_dir/invalid.log" 2>&1; then
    echo "chart accepted invalid request limit: $limit" >&2
    exit 1
  fi
  grep -q maxRequestSizeBytes "$work_dir/invalid.log"
done

for limit in default 67108864 0; do
  overrides=()
  expected=26214400
  if [[ "$limit" != default ]]; then
    overrides+=(--state-values-set "addons.vanityGateway.config.maxRequestSizeBytes=$limit")
    expected="$limit"
  fi
  (
    cd "$test_stack_dir"
    HELMFILE_ENV=base HELMFILE_CACHE_HOME="$work_dir/helmfile-cache" helmfile \
      --file helmfile.d/02-core.yaml.gotmpl --environment default \
      --state-values-set addons.vanityGateway.enabled=true \
      --state-values-set ingress.gatewayApi.gateways.shared.name=shared-gw \
      --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc-gw \
      --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy-gateway-system \
      ${overrides[@]+"${overrides[@]}"} --selector name=vanity-gateway \
      write-values --output-file-template "$work_dir/stack-values.yaml" >/dev/null
  )
  if [[ "$limit" == 0 ]]; then
    if helm template vanity-gateway "$chart" -f "$work_dir/stack-values.yaml" >"$work_dir/invalid.log" 2>&1; then
      echo "stack replaced explicit zero with a default" >&2
      exit 1
    fi
    grep -q maxRequestSizeBytes "$work_dir/invalid.log"
  else
    helm template vanity-gateway "$chart" -f "$work_dir/stack-values.yaml" >"$work_dir/stack.yaml"
    assert_limit "$work_dir/stack.yaml" "$expected"
  fi
done

echo "vanity-request-size: all checks passed"
