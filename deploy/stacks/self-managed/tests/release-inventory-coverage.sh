#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# The release inventory renders every release a state declares, using the
# state's fullOverrides to switch optional releases on. A release that stays
# disabled under those overrides has no rendered manifest, and the tag release
# job fails before it can publish the stack inventory. Assert that every
# release of every inventory state is enabled by its fullOverrides.
set -euo pipefail

stack_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
test_stack_dir="$work_dir/self-managed"
environment_name="release-inventory-coverage-test"
trap 'rm -rf "$work_dir"' EXIT

fail() {
  echo "release-inventory-coverage: $*" >&2
  exit 1
}

mkdir -p "$test_stack_dir"
cp -R "$stack_dir"/. "$test_stack_dir"
printf '{}\n' >"$test_stack_dir/secrets/$environment_name-secrets.yaml"
printf '{}\n' >"$test_stack_dir/environments/$environment_name.yaml"

state_count="$(yq -r '.states | length' "$stack_dir/release-inventory.yaml")"
[[ "$state_count" -gt 0 ]] || fail "release-inventory.yaml declares no states"

for ((i = 0; i < state_count; i++)); do
  state_file="$(yq -r ".states[$i].path" "$stack_dir/release-inventory.yaml")"
  helmfile_file="$test_stack_dir/helmfile.d/$(basename "$state_file")"
  args=()
  while IFS= read -r override; do
    [[ -n "$override" ]] && args+=(--state-values-set "$override")
  done < <(yq -r ".states[$i].fullOverrides[]?" "$stack_dir/release-inventory.yaml")
  disabled="$(
    HELMFILE_ENV="$environment_name" \
      HELMFILE_CACHE_HOME="$work_dir/helmfile-cache" \
      helmfile \
      --file "$helmfile_file" \
      --environment default \
      --state-values-set ingress.gatewayApi.controllerNamespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.shared.name=shared-gw \
      --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc-gw \
      --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy-gateway-system \
      ${args[@]+"${args[@]}"} \
      list --output json | yq -p=json -r '.[] | select(.enabled == false) | .name'
  )"
  [[ -z "$disabled" ]] ||
    fail "$(basename "$state_file"): fullOverrides leave these releases disabled, so the inventory cannot render them: $(tr '\n' ' ' <<<"$disabled")(add the setting that enables each to release-inventory.yaml)"
done

echo "release-inventory-coverage: OK"
