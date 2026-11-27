#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
# Never inherit tracing while reading an operator credential.
set +x

fail() { echo "JWT cluster lifecycle: $*" >&2; exit 1; }
usage() {
    echo 'Usage: jwt-plugin-cluster.sh {record-active|check-cleanup|cleanup} --context CONTEXT --namespace NAMESPACE --statefulset NAME --expected-image-digest sha256:PLATFORM_DIGEST --maintenance-window ID --evidence FILE [--image-manifest FILE] [--token-secret NAME] [--pod NAME --probe EXECUTABLE] [--confirm-rollback-window-closed]' >&2
    exit 1
}
[[ $# -gt 0 ]] || usage
action=$1
shift
case $action in record-active|check-cleanup|cleanup) ;; *) usage ;; esac
context='' namespace='' statefulset='' digest='' window='' evidence='' token_secret='' pod='' probe=''
image_manifest='' config_digest=''
closed=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --confirm-rollback-window-closed) closed=true; shift; continue ;;
        --context|--namespace|--statefulset|--expected-image-digest|--maintenance-window|--evidence|--token-secret|--pod|--probe|--image-manifest)
            [[ $# -ge 2 && -n $2 ]] || usage ;;
        *) usage ;;
    esac
    case $1 in
        --context) context=$2 ;; --namespace) namespace=$2 ;; --statefulset) statefulset=$2 ;;
        --expected-image-digest) digest=$2 ;; --maintenance-window) window=$2 ;;
        --evidence) evidence=$2 ;; --token-secret) token_secret=$2 ;;
        --pod) pod=$2 ;; --probe) probe=$2 ;;
        --image-manifest) image_manifest=$2 ;;
    esac
    shift 2
done
[[ -n $context && -n $namespace && -n $statefulset && -n $evidence ]] || usage
[[ $digest =~ ^sha256:[0-9a-f]{64}$ && $window =~ ^[a-zA-Z0-9._-]+$ ]] || usage
if [[ -n $image_manifest ]]; then
    [[ -f $image_manifest && -r $image_manifest ]] || fail 'unreadable platform image manifest'
    # Hash and parse one snapshot, preserving the manifest's trailing newlines.
    manifest=$(cat "$image_manifest" && printf '.') || fail 'cannot read platform image manifest'
    manifest=${manifest%.}
    if command -v sha256sum >/dev/null 2>&1; then
        actual=$(printf '%s' "$manifest" | sha256sum)
    else
        actual=$(printf '%s' "$manifest" | shasum -a 256)
    fi
    [[ sha256:${actual%% *} == "$digest" ]] || fail 'platform manifest bytes do not match the expected image digest'
    config_digest=$(jq -er 'select(.schemaVersion == 2 and
      (.mediaType == "application/vnd.oci.image.manifest.v1+json" or
       .mediaType == "application/vnd.docker.distribution.manifest.v2+json") and
      (.layers | type == "array")) | .config.digest |
      select(type == "string" and test("^sha256:[0-9a-f]{64}$"))' <<< "$manifest") ||
        fail 'invalid platform image manifest or config digest'
fi
token_secret=${token_secret:-${statefulset}-root-token}
if [[ $action == record-active ]]; then
    [[ -n $pod && $probe == /* && -f $probe && -x $probe ]] || usage
elif [[ $action == cleanup && $closed != true ]]; then
    fail 'cleanup requires --confirm-rollback-window-closed; ordinary old-plugin rollback ends at removal'
fi
kubectl_cmd=(kubectl --context "$context" --namespace "$namespace")

inventory() {
    local state selector expected pods
    state=$("${kubectl_cmd[@]}" get statefulset "$statefulset" -o json)
    selector=$(jq -er '.spec.selector.matchLabels | select(type == "object" and length > 0) |
      to_entries | map("\(.key)=\(.value)") | join(",")' <<< "$state")
    expected=$(jq -er '.spec.replicas | select(type == "number" and . > 0 and . == floor)' <<< "$state")
    pods=$("${kubectl_cmd[@]}" get pods -l "$selector" -o json)
    jq -Sce --argjson state "$state" --argjson expected "$expected" --arg digest "$digest" --arg config "$config_digest" '
      .items | select(type == "array") |
      map(select(.status.phase != "Failed" and .status.phase != "Succeeded")) |
      select(length == $expected) |
      select(all(.[]; .metadata.deletionTimestamp == null and .status.phase == "Running" and
        any(.metadata.ownerReferences[]?; .uid == $state.metadata.uid and .controller == true) and
        any(.status.conditions[]?; .type == "Ready" and .status == "True") and
        ([.status.containerStatuses[]? | select(.name == "openbao")] | length == 1) and
        all(.status.containerStatuses[] | select(.name == "openbao");
          .ready == true and (.state.running.startedAt | type == "string") and
          (.containerID | type == "string" and length > 0) and
          (.restartCount | type == "number") and
          (((.imageID | contains("@")) and (.imageID | split("@") | last) == $digest) or
           ($config != "" and (.imageID == $config or .imageID == $digest)))))) |
      {statefulset_uid: $state.metadata.uid, generation: $state.metadata.generation,
       members: map(. as $pod | .status.containerStatuses[] | select(.name == "openbao") |
         {name: $pod.metadata.name, uid: $pod.metadata.uid, container_id: .containerID,
          restarts: .restartCount, image_id: .imageID}) | sort_by(.name)} |
      select((.statefulset_uid | type == "string" and length > 0) and
        (.generation | type == "number") and all(.members[];
          (.name | type == "string" and length > 0) and (.uid | type == "string" and length > 0)))' \
      <<< "$pods" || fail 'incomplete, unready, terminating, foreign or wrong-image members; config-only runtime IDs require --image-manifest'
}

# This file is included in the chart package for operator use.
# The credential travels over stdin, never in kubectl argv or evidence. Each
# request addresses the member itself; leader checks cannot use a Service.
read -r -d '' remote_script <<'REMOTE' || true
set -euo pipefail
set +x
IFS= read -r BAO_TOKEN
test -n "$BAO_TOKEN"
export BAO_TOKEN BAO_ADDR=http://127.0.0.1:8200
. /openbao/jwt-plugin-versions.env
plugin=vault-plugin-secrets-jwt
jwt-plugin-lifecycle image-check >&2
if [[ -e "/openbao/plugins/$plugin" || -e "/openbao/plugins/$plugin-$JWT_PLUGIN_OLD_VERSION" ]]; then
    echo 'Member still contains legacy plugin paths; complete the target-only rollout before cleanup' >&2
    exit 1
fi
health=$(bao status -format=json)
jq -e '.initialized == true and .sealed == false and .ha_enabled == true' <<< "$health" >/dev/null
sha=$(sha256sum "/openbao/plugins/$plugin-$JWT_PLUGIN_TARGET_VERSION" | awk '{print $1}')
metadata=$(jq -ce --arg old "$JWT_PLUGIN_OLD_VERSION" --arg target "$JWT_PLUGIN_TARGET_VERSION" --arg sha "$sha" '
  {cluster_id: .cluster_id, old: $old, target: $target, sha: $sha} |
  select(.cluster_id | type == "string" and length > 0)' <<< "$health")
node_id=${BAO_RAFT_NODE_ID:-${VAULT_RAFT_NODE_ID:-}}
if [[ -z $node_id ]]; then node_id=$(cat /openbao/data/node-id); fi
test -n "$node_id"
leader() { curl --silent --show-error --fail --connect-timeout 5 --max-time 10 "$BAO_ADDR/v1/sys/leader"; }
case $1 in
    info)
        identity=$(leader)
        jq -cn --arg node_id "$node_id" --argjson metadata "$metadata" --argjson leader "$identity" \
          '{metadata: $metadata, node_id: $node_id, active: ($leader.is_self == true)}'
        ;;
    snapshot)
        leader | jq -e '.ha_enabled == true and .is_self == true' >/dev/null
        bao read -format=json sys/storage/raft/autopilot/state |
            jq -e '.data.healthy == true' >/dev/null
        jwt-plugin-lifecycle verify >&2
        mounts=$(jwt-plugin-lifecycle status | jq -Sce --arg target "$JWT_PLUGIN_TARGET_VERSION" --arg sha "$sha" '
          select(length > 0 and all(.[]; .value.plugin_version == $target and
            .value.running_plugin_version == $target and .value.running_sha256 == $sha)) |
          map({key, accessor: .value.accessor, config: .value.config, options: .value.options,
               configured: .value.plugin_version,
               running: .value.running_plugin_version, sha: .value.running_sha256}) | sort_by(.key)')
        peers=$(bao operator raft list-peers -format=json | jq -ce '
          .data.config.servers | select(type == "array" and length > 0 and all(.[]; .voter == true)) |
          map({node_id, address, voter}) | sort_by(.node_id)')
        leader | jq -e '.ha_enabled == true and .is_self == true' >/dev/null
        jq -cn --argjson metadata "$metadata" --argjson mounts "$mounts" --argjson peers "$peers" \
          '{metadata: $metadata, mounts: $mounts, raft_peers: $peers}'
        ;;
    catalog)
        jwt-plugin-lifecycle check-cleanup >&2
        bao plugin list -detailed -format=json secret
        ;;
    retire)
        [[ $2 == "$JWT_PLUGIN_OLD_VERSION" || -z $2 ]]
        leader | jq -e '.ha_enabled == true and .is_self == true' >/dev/null
        jwt-plugin-lifecycle check-cleanup >&2
        bao plugin deregister -version="$2" secret "$plugin" >&2
        ;;
    *) exit 1 ;;
esac
REMOTE

admin() {
    local member=$1
    shift
    "${kubectl_cmd[@]}" get secret "$token_secret" -o json |
        jq -er '.data.root_token | @base64d | select(length > 0 and (contains("\n") | not))' |
        "${kubectl_cmd[@]}" exec -i "$member" -c openbao -- bash -ec "$remote_script" sh "$@" || {
            echo "JWT cluster lifecycle: member $member failed operation $1" >&2
            return 1
        }
}

check_members() {
    local member info first=
    current_inventory=$(inventory)
    current_active=
    current_metadata='{}'
    current_nodes='{}'
    for member in $(jq -r '.members[].name' <<< "$current_inventory"); do
        info=$(admin "$member" info)
        jq -e '(.active | type == "boolean") and (.node_id | type == "string" and length > 0) and
          (.metadata | (.cluster_id | type == "string" and length > 0) and
          (.sha | type == "string" and test("^[0-9a-f]{64}$")))' <<< "$info" >/dev/null || fail "invalid member response: $member"
        if [[ -z $first ]]; then
            first=$(jq -Sc '.metadata' <<< "$info")
        else
            [[ $(jq -Sc '.metadata' <<< "$info") == "$first" ]] || fail "member cluster/version/hash mismatch: $member"
        fi
        current_metadata=$(jq -c --arg member "$member" --argjson info "$info" \
            '.[$member] = $info.metadata' <<< "$current_metadata")
        current_nodes=$(jq -c --arg member "$member" --argjson info "$info" \
            '.[$member] = $info.node_id' <<< "$current_nodes")
        if [[ $(jq -r '.active' <<< "$info") == true ]]; then
            [[ -z $current_active ]] || fail 'more than one active member'
            current_active=$member
        fi
    done
    [[ -n $current_active ]] || fail 'no active member'
}

check_peers() {
    jq -e --argjson nodes "$current_nodes" '
      (.raft_peers | map(.node_id) | sort) == ($nodes | [.[]] | sort) and
      ($nodes | [.[]] | length) == ($nodes | [.[]] | unique | length)' <<< "$1" >/dev/null ||
        fail 'Raft voters do not match the complete StatefulSet member inventory'
}

check_members
frozen_inventory=$current_inventory
if [[ -f $evidence ]]; then
    receipt=$(cat "$evidence")
    jq -e --arg context "$context" --arg namespace "$namespace" --arg statefulset "$statefulset" \
      --arg digest "$digest" --arg window "$window" --argjson inventory "$frozen_inventory" '
      .schema == 2 and .context == $context and .namespace == $namespace and
      .statefulset == $statefulset and .image_digest == $digest and .maintenance_window == $window and
      .inventory == $inventory and (.records | type == "object")' <<< "$receipt" >/dev/null ||
        fail 'stale or incompatible evidence; start a new evidence file and repeat the failover checks'
elif [[ $action == record-active ]]; then
    receipt=$(jq -cn --arg context "$context" --arg namespace "$namespace" --arg statefulset "$statefulset" \
      --arg digest "$digest" --arg window "$window" --argjson inventory "$frozen_inventory" \
      '{schema: 2, context: $context, namespace: $namespace, statefulset: $statefulset,
        image_digest: $digest, maintenance_window: $window, inventory: $inventory, records: {}}')
else
    fail 'missing failover evidence; record the active member before and after a normal failover'
fi

if [[ $action == record-active ]]; then
    [[ $pod == "$current_active" ]] || fail "member $pod is not the active member"
    before=$(admin "$pod" snapshot)
    check_peers "$before"
    JWT_PROBE_CONTEXT=$context JWT_PROBE_NAMESPACE=$namespace JWT_PROBE_POD=$pod \
      "$probe" >&2 || fail 'JWT signature/consumer probe failed; no evidence recorded'
    after=$(admin "$pod" snapshot)
    check_peers "$after"
    [[ $(jq -Sc . <<< "$before") == "$(jq -Sc . <<< "$after")" && $(inventory) == "$frozen_inventory" ]] ||
        fail 'member or mount state changed during the probe; no evidence recorded'
    receipt=$(jq --arg pod "$pod" --arg probe "$probe" --argjson snapshot "$after" \
        '.records[$pod] = {snapshot: $snapshot, probe: $probe, probe_passed: true}' <<< "$receipt")
    umask 077
    temp=$(mktemp "${evidence}.XXXXXX")
    trap 'rm -f "$temp"' EXIT
    printf '%s\n' "$receipt" > "$temp"
    mv "$temp" "$evidence"
    echo "Recorded active-member evidence for $pod in $evidence" >&2
    exit 0
fi

validate_evidence() {
    local snapshot
    check_members
    [[ $current_inventory == "$frozen_inventory" ]] || fail 'member inventory changed during cleanup'
    snapshot=$(admin "$current_active" snapshot)
    check_peers "$snapshot"
    jq -e --argjson inventory "$current_inventory" --argjson metadata "$current_metadata" \
      --argjson snapshot "$snapshot" '
      ($inventory.members | map(.name)) as $members |
      (.records | keys) as $recorded |
      ($recorded | length) >= (if ($members | length) > 1 then 2 else 1 end) and
      (($recorded - $members) | length) == 0 and
      all($inventory.members[]; .name as $name |
        $metadata[$name] == $snapshot.metadata) and
      all(.records[]; .probe_passed == true and .snapshot == $snapshot)' <<< "$receipt" >/dev/null ||
        fail 'missing, stale or mismatched before/after-failover consumer evidence'
    [[ $(inventory) == "$frozen_inventory" ]] || fail 'member inventory changed during verification'
}

validate_evidence
if [[ $action == cleanup ]]; then
    old=$(jq -er '.records | to_entries[0].value.snapshot.metadata.old' <<< "$receipt")
    for version in "$old" ""; do
        validate_evidence
        catalog=$(admin "$current_active" catalog)
        if jq -e --arg version "$version" \
          'any(.details[]; .name == "vault-plugin-secrets-jwt" and (.version // "") == $version)' <<< "$catalog" >/dev/null; then
            admin "$current_active" retire "$version"
        fi
    done
    validate_evidence
    catalog=$(admin "$current_active" catalog)
    target=$(jq -er '.records | to_entries[0].value.snapshot.metadata.target' <<< "$receipt")
    jq -e --arg target "$target" 'all(.details[] | select(.name == "vault-plugin-secrets-jwt");
      .version == $target)' <<< "$catalog" >/dev/null || fail 'old catalog entries remain after cleanup'
fi
echo "JWT cluster $action passed; maintenance window: $window" >&2
