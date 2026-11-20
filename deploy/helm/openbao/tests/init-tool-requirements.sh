#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT

# Exercise the scripts and invocation shipped in the chart package.
chart_dir="$test_root/chart"
cp -R "$project_dir/helm" "$chart_dir"
helm dependency build "$chart_dir" >/dev/null
helm package "$chart_dir" --destination "$test_root" >/dev/null
rendered="$test_root/rendered.yaml"
helm template openbao "$test_root"/*.tgz --namespace test \
    -f "$project_dir/../../../tools/ci/helm-validate-values/openbao.yaml" > "$rendered"
init_job='select(.kind == "Job" and .metadata.name == "openbao-server-initialize-cluster")'
test "$(yq -r "$init_job | .metadata.annotations.\"helm.sh/hook\"" "$rendered")" = post-install
test "$(yq -r "$init_job | .spec.template.spec.containers[0].command[0]" "$rendered")" = /bin/bash
test "$(yq -r "$init_job | .spec.template.spec.containers[0].args[0]" "$rendered")" = -c
read -r -a chart_command <<< "$(yq -r "$init_job | .spec.template.spec.containers[0].args[1]" "$rendered")"
test "${chart_command[0]}" = /scripts/deploy.sh

# Bootstrap must run while the StatefulSet is NotReady, before post-install.
bootstrap_job='select(.kind == "Job" and .metadata.labels."app.kubernetes.io/component" == "openbao-bootstrap")'
test "$(yq -r "$bootstrap_job | .metadata.annotations.\"helm.sh/hook\"" "$rendered")" = null
test "$(yq -r "$bootstrap_job | .spec.template.spec.containers[0].env[] | select(.name == \"OPENBAO_BOOTSTRAP_ONLY\") | .value" "$rendered")" = true
for name in bootstrap-script bootstrap-utils; do
    test "$(yq -r "select(.kind == \"ConfigMap\" and .metadata.name == \"openbao-server-$name\") | .metadata.annotations.\"helm.sh/hook\"" "$rendered")" = null
done
wait_job='select(.kind == "Job" and .metadata.name == "openbao-server-wait-bootstrap")'
test "$(yq -r "$wait_job | .metadata.annotations.\"helm.sh/hook\"" "$rendered")" = post-upgrade
test "$(yq -r "$wait_job | .metadata.annotations.\"helm.sh/hook-weight\"" "$rendered")" = -5
test "$(yq -r "$wait_job | .spec.template.spec.containers[0].env[] | select(.name == \"OPENBAO_WAIT_ONLY\") | .value" "$rendered")" = true
test "$(yq -r "$wait_job | .spec.template.spec.containers[0].env[] | select(.name == \"OPENBAO_BOOTSTRAP_ONLY\") | .value" "$rendered")" = false
for job in "$bootstrap_job" "$init_job" "$wait_job"; do
    test "$(yq -r "$job | .spec.template.spec.containers[0].env[] | select(.name == \"OPENBAO_BOOTSTRAP_JOB_NAME\") | .value" "$rendered")" = "$(yq -r "$bootstrap_job | .metadata.name" "$rendered")"
    script_map=$(yq -r "$job | .spec.template.spec.volumes[] | select(.name == \"init-script\") | .configMap.name" "$rendered")
    utils_map=$(yq -r "$job | .spec.template.spec.volumes[] | select(.name == \"utils-script\") | .configMap.name" "$rendered")
    test -n "$script_map" && test "$script_map" != null
    test -n "$utils_map" && test "$utils_map" != null
    test "$(yq -r "select(.kind == \"ConfigMap\" and .metadata.name == \"$script_map\") | .data | has(\"deploy.sh\")" "$rendered")" = true
    for script in utils log jwt-plugin bootstrap; do
        test "$(yq -r "select(.kind == \"ConfigMap\" and .metadata.name == \"$utils_map\") | .data | has(\"$script.sh\")" "$rendered")" = true
    done
done
script_map=$(yq -r "$init_job | .spec.template.spec.volumes[] | select(.name == \"init-script\") | .configMap.name" "$rendered")
utils_map=$(yq -r "$init_job | .spec.template.spec.volumes[] | select(.name == \"utils-script\") | .configMap.name" "$rendered")
expected_credential_timeout=$(yq -r '.openbao.hooks.initializeCluster.credentialTimeoutSeconds' "$project_dir/helm/values.yaml")
expected_api_timeout=$(yq -r '.openbao.hooks.initializeCluster.apiTimeoutSeconds' "$project_dir/helm/values.yaml")
expected_deadline=$((3 * expected_api_timeout + 4 * expected_credential_timeout + 60))
test "$(yq -r "$bootstrap_job | .spec.activeDeadlineSeconds" "$rendered")" = "$expected_deadline"
for job in "$init_job" "$wait_job"; do
    test "$(yq -r "$job | .spec.template.spec.containers[0].env[] | select(.name == \"OPENBAO_BOOTSTRAP_TIMEOUT_SECONDS\") | .value" "$rendered")" = "$((expected_deadline + 60))"
    test "$(yq -r "$job | .spec.template.spec.containers[0].env[] | select(.name == \"OPENBAO_BOOTSTRAP_TIMEOUT_SECONDS\") | .value | tag" "$rendered")" = '!!str'
done
test "$(yq -r "$bootstrap_job | .spec.template.spec.containers[0].env[] | select(.name == \"OPENBAO_CREDENTIAL_TIMEOUT_SECONDS\") | .value" "$rendered")" = "$expected_credential_timeout"
# Zero must reach script validation; only an absent key uses the default.
for key in credential api; do
    expected_default=$(yq -r ".openbao.hooks.initializeCluster.${key}TimeoutSeconds" "$chart_dir/values.yaml")
    helm template openbao "$test_root"/*.tgz --namespace test \
        -f "$project_dir/../../../tools/ci/helm-validate-values/openbao.yaml" \
        --set "openbao.hooks.initializeCluster.${key}TimeoutSeconds=0" > "$test_root/timeout.yaml"
    env_name=OPENBAO_CREDENTIAL_TIMEOUT_SECONDS
    if [ "$key" = api ]; then env_name=OPENBAO_API_TIMEOUT_SECONDS; fi
    for job in "$bootstrap_job" "$init_job" "$wait_job"; do
        test "$(yq -r "$job | .spec.template.spec.containers[0].env[] | select(.name == \"$env_name\") | .value" "$test_root/timeout.yaml")" = 0
        test "$(yq -r "$job | .spec.template.spec.containers[0].env[] | select(.name == \"$env_name\") | .value | tag" "$test_root/timeout.yaml")" = '!!str'
    done
    yq -i "del(.openbao.hooks.initializeCluster.${key}TimeoutSeconds)" "$chart_dir/values.yaml"
    helm template openbao "$chart_dir" --namespace test \
        -f "$project_dir/../../../tools/ci/helm-validate-values/openbao.yaml" > "$test_root/timeout.yaml"
    for job in "$bootstrap_job" "$init_job" "$wait_job"; do
        test "$(yq -r "$job | .spec.template.spec.containers[0].env[] | select(.name == \"$env_name\") | .value" "$test_root/timeout.yaml")" = "$expected_default"
        test "$(yq -r "$job | .spec.template.spec.containers[0].env[] | select(.name == \"$env_name\") | .value | tag" "$test_root/timeout.yaml")" = '!!str'
    done
done
test "$(yq -r 'select(.kind == "Role") | .rules[] | select(.apiGroups[0] == "batch" and .resources[0] == "jobs") | .verbs[0]' "$rendered")" = get
previous_name=""
for suffix in a b; do
    helm template openbao "$test_root"/*.tgz --namespace test \
        -f "$project_dir/../../../tools/ci/helm-validate-values/openbao.yaml" \
        --set "openbao.fullnameOverride=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-$suffix" > "$test_root/named.yaml"
    job_name=$(yq -r "$bootstrap_job | .metadata.name" "$test_root/named.yaml")
    test "${#job_name}" -le 63
    test "$job_name" != "$previous_name"
    test "$(yq -r 'select(.kind == "Role") | .rules[] | select(.resources[0] == "jobs") | .resourceNames[0]' "$test_root/named.yaml")" = "$job_name"
    test "$(yq -r 'select(.kind == "Job" and .metadata.annotations."helm.sh/hook" == "post-upgrade") | .spec.template.spec.containers[0].env[] | select(.name == "OPENBAO_BOOTSTRAP_JOB_NAME") | .value' "$test_root/named.yaml")" = "$job_name"
    previous_name=$job_name
done
helm template openbao "$test_root"/*.tgz --namespace test \
    -f "$project_dir/../../../tools/ci/helm-validate-values/openbao.yaml" \
    --set 'openbao.hooks.initializeCluster.schedulingGates[0].name=example.com/hold' > "$test_root/gated.yaml"
test "$(yq -r "$init_job | .spec.template.spec.schedulingGates[0].name" "$test_root/gated.yaml")" = example.com/hold
for job in "$bootstrap_job" "$wait_job"; do
    test "$(yq -r "$job | .spec.template.spec.schedulingGates" "$test_root/gated.yaml")" = null
done
if helm template openbao "$test_root"/*.tgz --namespace test \
    -f "$project_dir/../../../tools/ci/helm-validate-values/openbao.yaml" \
    --set openbao.server.podManagementPolicy=OrderedReady > "$test_root/ordered.log" 2>&1; then
    echo 'OrderedReady unexpectedly passed bootstrap validation' >&2
    exit 1
fi
grep -q 'bootstrap requires openbao.server.podManagementPolicy=Parallel' "$test_root/ordered.log"
probe_path=$(yq -r 'select(.kind == "StatefulSet") | .spec.template.spec.containers[] | select(.name == "openbao") | .readinessProbe.httpGet.path' "$rendered")
test "$probe_path" = '/v1/sys/health?standbyok=true'

bin_dir="$test_root/bin"
mkdir -p "$bin_dir"
for tool in dirname jq tr sort head; do
    ln -s "$(command -v "$tool")" "$bin_dir/$tool"
done
cat > "$bin_dir/kubectl" <<'MOCK'
#!/bin/sh
if [ "$1" = version ]; then
    printf '%s\n' '{"serverVersion":{"gitVersion":"v1.36.4"}}'
    exit 0
fi
printf '%s\n' "$*" >> "$KUBECTL_CALLS"
case "$*" in
    *containerStatuses*) printf '%s\n' true; exit 0 ;;
esac
exit 1
MOCK
chmod +x "$bin_dir/kubectl"
export OPENBAO_API_TIMEOUT_SECONDS=1
export KUBECTL_CALLS="$test_root/kubectl-calls"

for source_dir in "$project_dir" "$project_dir/helm/scripts"; do
    # Reproduce the chart's ConfigMap mounts without requiring a cluster.
    stage="$test_root/stage"
    mkdir -p "$stage/utils"
    hook_args=(test openbao helm)
    if [ -f "$source_dir/utils/utils.sh" ]; then
        cp "$source_dir/deploy.sh" "$stage/deploy.sh"
        cp "$source_dir/utils/utils.sh" "$source_dir/utils/log.sh" "$stage/utils/"
        mkdir -p "$stage/helm/scripts/utils"
        cp "$project_dir/helm/scripts/utils/"{jwt-plugin,bootstrap}.sh "$stage/helm/scripts/utils/"
    else
        yq -r "select(.kind == \"ConfigMap\" and .metadata.name == \"$script_map\") | .data.\"deploy.sh\"" "$rendered" > "$stage/deploy.sh"
        for script in utils log jwt-plugin bootstrap; do
            yq -r "select(.kind == \"ConfigMap\" and .metadata.name == \"$utils_map\") | .data.\"$script.sh\"" "$rendered" > "$stage/utils/$script.sh"
        done
        hook_args=("${chart_command[@]:1}")
    fi
    ln -sf "$(type -P true)" "$bin_dir/jwker"
    : > "$KUBECTL_CALLS"
    if PATH="$bin_dir" "$BASH" "$stage/deploy.sh" test openbao script > "$test_root/script.log" 2>&1; then
        echo 'Standalone mode unexpectedly succeeded without Helm' >&2
        exit 1
    fi
    grep -q 'helm check failed' "$test_root/script.log"
    test ! -s "$KUBECTL_CALLS"

    # This checks prerequisites, not successful initialization. The fake API
    # rejects later operations, so no cluster is contacted.
    for bootstrap_only in false true; do
        : > "$KUBECTL_CALLS"
        if OPENBAO_BOOTSTRAP_ONLY="$bootstrap_only" PATH="$bin_dir" "$BASH" "$stage/deploy.sh" "${hook_args[@]}" > "$test_root/hook.log" 2>&1; then
            echo 'Hook unexpectedly succeeded against the rejecting fake API' >&2
            exit 1
        fi
        grep -q 'jwker available' "$test_root/hook.log"
        test -s "$KUBECTL_CALLS"
        if [ "$bootstrap_only" = true ]; then
            grep -q 'Initializing OpenBao nodes' "$test_root/hook.log"
        fi
        if grep -q 'helm check failed' "$test_root/hook.log"; then
            echo 'Hook mode still requires Helm' >&2
            exit 1
        fi
    done

    rm "$bin_dir/jwker"
    : > "$KUBECTL_CALLS"
    if PATH="$bin_dir" "$BASH" "$stage/deploy.sh" "${hook_args[@]}" > "$test_root/missing-jwker.log" 2>&1; then
        echo 'Hook unexpectedly succeeded without jwker' >&2
        exit 1
    fi
    grep -q 'jwker is not available' "$test_root/missing-jwker.log"
    test ! -s "$KUBECTL_CALLS"
done

echo 'OpenBao initialization tool requirements passed'
