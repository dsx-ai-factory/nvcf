#!/bin/sh
# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -eu
namespace=${JWT_SERVER_NAMESPACE:?}
statefulset=${JWT_SERVER_STATEFULSET:?}
state=$(kubectl -n "$namespace" get statefulset "$statefulset" -o json)
selector=$(printf '%s' "$state" | jq -er '
  .spec.selector.matchLabels | select(type == "object" and length > 0) |
  to_entries | map("\(.key)=\(.value)") | join(",")')
expected=$(printf '%s' "$state" | jq -er '.spec.replicas | select(type == "number" and . > 0)')
pods=$(kubectl -n "$namespace" get pods -l "$selector" --field-selector=status.phase=Running -o json)
members=$(printf '%s' "$pods" | jq -ce '
  .items | map(select(.status.phase == "Running" and .metadata.deletionTimestamp == null))')
count=$(printf '%s' "$members" | jq 'length')
if [ "$count" != "$expected" ]; then
    echo "Expected $expected running, non-terminating members for StatefulSet $statefulset; found $count" >&2
    exit 1
fi
unready=$(printf '%s' "$members" | jq -r '[.[] | select(
  any(.status.conditions[]?; .type == "Ready" and .status == "True") | not) |
  .metadata.name] | join(", ")')
if [ -n "$unready" ]; then
    echo "Every OpenBao member must be Ready before changing lifecycle mode; not Ready: $unready" >&2
    exit 1
fi
versions_dir=${JWT_SHARED_VERSIONS_DIR:-/versions}
unset JWT_PLUGIN_BRIDGE_IMAGE
# shellcheck source=infra/openbao/jwt-plugin-versions.env
. "$versions_dir/manifest.env"
if [ -n "${JWT_PLUGIN_BRIDGE_IMAGE:-}" ] && [ -n "${BAO_JWT_PLUGIN_VERSION:-}" ] &&
   [ "$BAO_JWT_PLUGIN_VERSION" != "$JWT_PLUGIN_TARGET_VERSION" ]; then
    echo 'Target-only migrations must select the manifest target; remove the old-version override before upgrading' >&2
    exit 1
fi
target_sha=$(cat "$versions_dir/target.sha")
for pod in $(printf '%s' "$members" | jq -r '.[].metadata.name'); do
    # Variables in the quoted command expand on the member, not in this Job.
    # shellcheck disable=SC2016
    kubectl -n "$namespace" exec "$pod" -c openbao -- sh -ec '
      member_fail() { echo "JWT member check: $*" >&2; exit 1; }
      test -r /openbao/jwt-plugin-versions.env || member_fail "cannot read JWT version manifest"
      JWT_PLUGIN_TARGET_VERSION=$(
        unset JWT_PLUGIN_TARGET_VERSION
        . /openbao/jwt-plugin-versions.env || exit $?
        printf "%s" "${JWT_PLUGIN_TARGET_VERSION:-}"
      ) || member_fail "cannot load JWT version manifest"
      test -n "${JWT_PLUGIN_TARGET_VERSION:-}" || member_fail "target version missing from JWT version manifest"
      test "$JWT_PLUGIN_TARGET_VERSION" = "$1" ||
        member_fail "target version mismatch (expected $1, found $JWT_PLUGIN_TARGET_VERSION)"
      checksum=$(sha256sum "/openbao/plugins/vault-plugin-secrets-jwt-${JWT_PLUGIN_TARGET_VERSION}") ||
        member_fail "cannot read target plugin SHA"
      actual=${checksum%% *}
      test "$actual" = "$2" || member_fail "target plugin SHA mismatch (expected $2, found $actual)"
      jwt-plugin-lifecycle image-check || member_fail "image configuration validation failed"
      health_code=0
      health=$(BAO_ADDR=http://127.0.0.1:8200 bao status -format=json) || health_code=$?
      if ! printf "%s" "$health" | jq -e '\''type == "object" and
        (.initialized | type == "boolean") and (.sealed | type == "boolean")'\'' >/dev/null; then
        if [ "$health_code" -ne 0 ]; then
          member_fail "cannot read OpenBao health (bao status exited $health_code)"
        fi
        member_fail "invalid OpenBao health response"
      fi
      printf "%s" "$health" | jq -e '\''.initialized == true'\'' >/dev/null ||
        member_fail "OpenBao is not initialized"
      printf "%s" "$health" | jq -e '\''.sealed == false'\'' >/dev/null ||
        member_fail "OpenBao is sealed"
      test "$health_code" -eq 0 || member_fail "OpenBao status command failed (exit $health_code)"
    ' sh "$JWT_PLUGIN_TARGET_VERSION" "$target_sha" || {
        echo "JWT member check: failed to verify $namespace/$pod (StatefulSet $statefulset); inspect the reported error" >&2
        exit 1
    }
done
