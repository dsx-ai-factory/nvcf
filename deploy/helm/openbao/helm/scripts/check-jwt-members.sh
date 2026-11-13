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
printf '%s' "$members" | jq -e 'all(.[];
  any(.status.conditions[]?; .type == "Ready" and .status == "True"))' >/dev/null || {
    echo 'Every OpenBao member must be Ready before changing lifecycle mode' >&2
    exit 1
}
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
      . /openbao/jwt-plugin-versions.env
      test "$JWT_PLUGIN_TARGET_VERSION" = "$1"
      actual=$(sha256sum "/openbao/plugins/vault-plugin-secrets-jwt-${JWT_PLUGIN_TARGET_VERSION}" | awk "{print \$1}")
      test "$actual" = "$2"
      jwt-plugin-lifecycle image-check
      BAO_ADDR=http://127.0.0.1:8200 bao status -format=json |
        jq -e '\''.initialized == true and .sealed == false'\'' >/dev/null
    ' sh "$JWT_PLUGIN_TARGET_VERSION" "$target_sha" || {
        echo "Rotate member $pod to the compatible bridge before enabling versioned mode" >&2
        echo "Required upgrade stop: ${JWT_PLUGIN_BRIDGE_IMAGE:-the qualified bridge in the release upgrade guide}; complete existing-mount migration before target-only deployment" >&2
        exit 1
    }
done
