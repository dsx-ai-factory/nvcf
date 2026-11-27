#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: Apache-2.0

# Bound the whole kubectl process, including exec setup, without changing
# client flags that can disable service-account authentication in-cluster.
bootstrap_kubectl() {
    local limit=${bootstrap_request_timeout:-5} remaining command_pid watchdog_pid rc=0
    remaining=$limit
    if [ -n "${bootstrap_deadline:-}" ]; then
        remaining=$((bootstrap_deadline - SECONDS))
        [ "$remaining" -gt 0 ] || return 1
        [ "$remaining" -le "$limit" ] || remaining=$limit
    fi
    kubectl "$@" &
    command_pid=$!
    (
        timer_pid=""
        trap 'test -z "$timer_pid" || kill "$timer_pid" 2>/dev/null || true' EXIT
        trap 'exit 0' TERM INT
        /bin/sleep "$remaining" &
        timer_pid=$!
        wait "$timer_pid" || exit 0
        kill -TERM "$command_pid" 2>/dev/null || exit 0
        /bin/sleep 1 &
        timer_pid=$!
        wait "$timer_pid" || exit 0
        kill -KILL "$command_pid" 2>/dev/null || true
    ) </dev/null >/dev/null 2>&1 &
    watchdog_pid=$!
    wait "$command_pid" || rc=$?
    kill "$watchdog_pid" 2>/dev/null || true
    wait "$watchdog_pid" 2>/dev/null || true
    return "$rc"
}

bootstrap_start_wait() {
    local timeout=${OPENBAO_API_TIMEOUT_SECONDS-600}
    if ! [[ $timeout =~ ^[1-9][0-9]*$ ]]; then
        log_error "OPENBAO_API_TIMEOUT_SECONDS must be a positive integer"
        return 1
    fi
    bootstrap_deadline=${bootstrap_deadline:-$((SECONDS + timeout))}
    if [ -n "${bootstrap_job_deadline:-}" ] && [ "$bootstrap_deadline" -gt "$bootstrap_job_deadline" ]; then
        bootstrap_deadline=$bootstrap_job_deadline
    fi
}

# Credential reads and writes need a separate budget after operator init.
bootstrap_start_persistence() {
    local timeout=${OPENBAO_CREDENTIAL_TIMEOUT_SECONDS-120}
    if ! [[ $timeout =~ ^[1-9][0-9]*$ ]]; then
        log_error "OPENBAO_CREDENTIAL_TIMEOUT_SECONDS must be a positive integer"
        return 1
    fi
    bootstrap_deadline=$((SECONDS + timeout))
    if [ -n "${bootstrap_job_deadline:-}" ] && [ "$bootstrap_deadline" -gt "$bootstrap_job_deadline" ]; then
        bootstrap_deadline=$bootstrap_job_deadline
    fi
    bootstrap_request_timeout=$timeout
}

# Keep the init request and all three persistence phases ahead of Pod expiry.
# bootstrap_job_deadline is shared with later per-operation budget setup.
bootstrap_prepare_initialization() {
    local namespace=$1 api_timeout=${OPENBAO_API_TIMEOUT_SECONDS-600}
    local credential_timeout=${OPENBAO_CREDENTIAL_TIMEOUT_SECONDS-120} job remaining reserve
    bootstrap_job_deadline=""
    bootstrap_deadline=""
    bootstrap_start_wait || return 1
    if ! [[ $credential_timeout =~ ^[1-9][0-9]*$ ]]; then
        log_error "OPENBAO_CREDENTIAL_TIMEOUT_SECONDS must be a positive integer"
        return 1
    fi
    if [ -n "${OPENBAO_BOOTSTRAP_JOB_NAME:-}" ]; then
        job=$(bootstrap_kubectl get job "$OPENBAO_BOOTSTRAP_JOB_NAME" -n "$namespace" -o json) || return 1
        remaining=$(printf '%s' "$job" | jq -er '
            select(.spec.activeDeadlineSeconds | type == "number" and . > 0 and . == floor) |
            ((.status.startTime | fromdateiso8601) + .spec.activeDeadlineSeconds - now | floor)') || {
            log_error "Cannot determine the remaining bootstrap Job lifetime"
            return 1
        }
        reserve=$((api_timeout + 3 * credential_timeout + 60))
        if [ "$remaining" -lt "$reserve" ]; then
            log_error "Not enough bootstrap Job time remains to initialize and save credentials"
            return 1
        fi
        # Stop client work before Kubernetes starts terminating the Pod.
        bootstrap_job_deadline=$((SECONDS + remaining - 30))
    fi
    bootstrap_deadline=""
    bootstrap_start_wait || return 1
    bootstrap_request_timeout=$api_timeout
}

bootstrap_pause() {
    local remaining=$((bootstrap_deadline - SECONDS))
    [ "$remaining" -gt 0 ] || return 1
    [ "$remaining" -le 2 ] || remaining=2
    sleep "$remaining"
}

openbao_status() {
    local namespace=$1 pod=$2 output rc
    output=$(bootstrap_kubectl exec "$pod" -n "$namespace" -c openbao -- \
        env BAO_CLIENT_TIMEOUT=5s bao status -format=json 2>/dev/null)
    rc=$?
    if [ "$rc" != 0 ] && [ "$rc" != 2 ]; then
        return 1
    fi
    printf '%s' "$output" | jq -es '
        length == 1 and (.[0] | type == "object" and
        (.initialized | type == "boolean") and
        (.sealed | type == "boolean") and
        (.initialized or .sealed))' >/dev/null 2>&1 || return 1
    printf '%s' "$output"
}

wait_for_openbao_api() {
    local namespace=$1 statefulset=$2 replicas i
    local bootstrap_deadline=${bootstrap_deadline:-}
    bootstrap_start_wait || return 1
    while :; do
        replicas=$(bootstrap_kubectl get statefulset "$statefulset" -n "$namespace" \
            -o jsonpath='{.spec.replicas}' 2>/dev/null) || replicas=""
        if [[ $replicas =~ ^[1-9][0-9]*$ ]]; then
            break
        fi
        if [ "$SECONDS" -ge "$bootstrap_deadline" ]; then
            log_error "Timed out waiting for StatefulSet '$statefulset'"
            return 1
        fi
        bootstrap_pause || true
    done
    OPENBAO_REPLICAS=$replicas
    for ((i = 0; i < replicas; i++)); do
        until openbao_status "$namespace" "$statefulset-$i" >/dev/null; do
            if [ "$SECONDS" -ge "$bootstrap_deadline" ]; then
                log_error "Timed out waiting for OpenBao API in '$statefulset-$i'"
                return 1
            fi
            log_info "Waiting for OpenBao API in '$statefulset-$i'"
            bootstrap_pause || true
        done
    done
}

# Read back every write, including a failed response. Never replace a
# different saved credential or retry operator init to recover a lost value.
persist_bootstrap_secret() {
    local namespace=$1 name=$2 field=$3 value=$4 action=$5
    local digest=${6:-}
    local encoded existing attempt snapshot version
    local bootstrap_deadline bootstrap_request_timeout
    bootstrap_start_persistence || return 1
    encoded=$(printf '%s' "$value" | base64 | tr -d '\n') || return 1
    for attempt in 1 2 3; do
        if snapshot=$(bootstrap_kubectl get secret "$name" -n "$namespace" \
            --ignore-not-found -o json 2>/dev/null); then
            snapshot=${snapshot:-'{}'}
            existing=$(printf '%s' "$snapshot" | jq -er --arg field "$field" '.data[$field] // ""') || existing=""
            version=$(printf '%s' "$snapshot" | jq -r '.metadata.resourceVersion // ""') || return 1
            if [ "$existing" = "$encoded" ]; then
                return 0
            fi
            if [ -n "$existing" ]; then
                log_error "Secret '$name' contains a different bootstrap credential; refusing to overwrite it"
                return 1
            fi
            if [ "$action" = patch ]; then
                if [ -n "$version" ]; then
                    bootstrap_kubectl patch secret "$name" -n "$namespace" --type=merge \
                        --patch "$(jq -cn --arg field "$field" --arg value "$encoded" --arg version "$version" \
                            '{metadata: {resourceVersion: $version}, data: {($field): $value}}')" \
                        >/dev/null 2>&1 || true
                fi
            else
                local state_args=()
                if [ "$field" = root_token ]; then
                    state_args=(--from-literal=bootstrap_state=pending --from-literal="bootstrap_credential_digest=$digest")
                fi
                bootstrap_kubectl create secret generic "$name" -n "$namespace" \
                    --from-literal="$field=$value" "${state_args[@]}" >/dev/null 2>&1 || true
            fi
            existing=$(bootstrap_kubectl get secret "$name" -n "$namespace" \
                -o "jsonpath={.data.$field}" 2>/dev/null) || existing=""
            [ "$existing" != "$encoded" ] || return 0
        fi
        log_warn "Could not verify bootstrap Secret '$name' (attempt $attempt/3)"
        if [ "$attempt" -lt 3 ]; then bootstrap_pause || true; fi
    done
    log_error "Failed to persist bootstrap Secret '$name'; restore the saved credentials before retrying"
    return 1
}

persist_bootstrap_credentials() {
    local namespace=$1 statefulset=$2 suffix=$3 field=$4 value=$5 root=$6 action=$7 rc=0
    local mode=shamir digest
    [ "$suffix" != recovery-keys ] || mode=auto
    digest=$(bootstrap_credential_digest "$mode" "$value") || return 1
    persist_bootstrap_secret "$namespace" "$statefulset-$suffix" "$field" "$value" "$action" || rc=1
    # Attempt both writes while the original init output is still in memory.
    persist_bootstrap_secret "$namespace" "$statefulset-root-token" root_token "$root" create "$digest" || rc=1
    [ "$rc" = 0 ] || return "$rc"
    complete_bootstrap_credentials "$namespace" "$statefulset" "$mode" "$root" "$value"
}

bootstrap_credential_digest() {
    local mode=$1 value=$2 digest
    if [ "$mode" = auto ]; then
        value=$(printf '%s' "$value" | jq -ces '
            select(length == 1) | .[0] |
            select(type == "array" and length > 0 and all(.[]; type == "string" and length > 0))') || return 1
    fi
    [ -n "$value" ] || return 1
    if command -v sha256sum >/dev/null 2>&1; then
        digest=$(printf '%s:%s' "$mode" "$value" | sha256sum) || return 1
    else
        digest=$(printf '%s:%s' "$mode" "$value" | shasum -a 256) || return 1
    fi
    printf '%s' "${digest%% *}"
}

# Retry transport errors without resetting the caller's credential budget.
bootstrap_read_secret() {
    local namespace=$1 name=$2 attempt snapshot
    for attempt in 1 2 3; do
        if snapshot=$(bootstrap_kubectl get secret "$name" -n "$namespace" -o json 2>/dev/null); then
            printf '%s' "$snapshot"
            return 0
        fi
        if [ "$attempt" -lt 3 ]; then bootstrap_pause || break; fi
    done
    log_error "Could not read bootstrap Secret '$name' within the credential budget"
    return 1
}

# Revoke only the exact unverified revision observed by this attempt.
reset_bootstrap_completion() {
    local namespace=$1 statefulset=$2 version=$3 current attempt
    for attempt in 1 2 3; do
        if bootstrap_kubectl patch secret "$statefulset-root-token" -n "$namespace" --type=merge \
            --patch "$(jq -cn --arg version "$version" --arg state "$(printf pending | base64 | tr -d '\n')" \
                --arg verified "$(printf false | base64 | tr -d '\n')" \
                '{metadata: {resourceVersion: $version}, data: {bootstrap_state: $state, bootstrap_verified: $verified}}')" >/dev/null 2>&1; then
            return 0
        fi
        current=$(bootstrap_kubectl get secret "$statefulset-root-token" -n "$namespace" \
            -o jsonpath='{.metadata.resourceVersion}' 2>/dev/null) || current=$version
        # Another attempt may already have verified and exported the shares.
        [ "$current" = "$version" ] || return 0
        if [ "$attempt" -lt 3 ]; then bootstrap_pause || break; fi
    done
    log_error "Could not reset bootstrap completion; verify credentials before retrying"
    return 1
}

bootstrap_completion_matches() {
    local snapshot=$1 expected=$2 digest=$3 saved
    saved=$(printf '%s' "$snapshot" | jq -er '.data.root_token') || return 1
    [ "$saved" = "$expected" ] || return 1
    printf '%s' "$snapshot" | jq -e --arg digest "$digest" '
        (.data.bootstrap_credential_digest | @base64d) == $digest and
        (.data.bootstrap_state | @base64d) == "complete"' >/dev/null
}

# Older installations have no state field. New root Secrets are created with
# pending atomically, so exporting recovery keys is safe only after complete.
bootstrap_credentials_ready() {
    local namespace=$1 statefulset=$2 expected=${3:-} snapshot
    snapshot=$(bootstrap_kubectl get secret "$statefulset-root-token" -n "$namespace" -o json 2>/dev/null) || return 1
    if [ -n "$expected" ] && [ "$(printf '%s' "$snapshot" | jq -r '.data.root_token')" != "$expected" ]; then return 1; fi
    printf '%s' "$snapshot" | jq -e '
        (.data.root_token | type == "string" and length > 0) and
        ((.data.bootstrap_state // "" | @base64d) as $state |
        (.data.bootstrap_credential_digest // "") as $digest |
        ($state == "" and $digest == "") or
        ($state == "complete" and ($digest == "" or (.data.bootstrap_verified // "" | @base64d) == "true")))' >/dev/null 2>&1
}

complete_bootstrap_credentials() {
    local namespace=$1 statefulset=$2 mode=$3 root=$4 expected=${5:-}
    local bootstrap_deadline bootstrap_request_timeout
    bootstrap_start_persistence || return 1
    local attempt snapshot state version verification_version saved field suffix encoded digest actual verified secondary confirmed
    encoded=$(printf '%s' "$root" | base64 | tr -d '\n') || return 1
    for attempt in 1 2 3; do
        snapshot=$(bootstrap_read_secret "$namespace" "$statefulset-root-token") || return 1
        saved=$(printf '%s' "$snapshot" | jq -r '.data.root_token // ""') || return 1
        if [ -z "$encoded" ] || [ "$saved" != "$encoded" ]; then
            log_error "Saved root token changed before bootstrap completion; refusing to mark complete"
            return 1
        fi
        state=$(printf '%s' "$snapshot" | jq -r '.data.bootstrap_state // "" | @base64d') || return 1
        digest=$(printf '%s' "$snapshot" | jq -r '.data.bootstrap_credential_digest // "" | @base64d') || return 1
        verified=$(printf '%s' "$snapshot" | jq -r '.data.bootstrap_verified // "" | @base64d') || return 1
        version=$(printf '%s' "$snapshot" | jq -er '.metadata.resourceVersion') || return 1
        case "$state" in
            "")
                if [ -z "$digest" ]; then return 0; fi
                log_error "Bootstrap credential state is missing"; return 1 ;;
            complete)
                # This receipt is written only after the secondary reread.
                # Without it, even a later release must finish verification.
                if [ -z "$digest" ] || [ "$verified" = true ]; then return 0; fi
                ;;
            pending) ;;
            *) log_error "Unknown bootstrap credential state"; return 1 ;;
        esac
        suffix=unseal field=unseal_key
        if [ "$mode" = auto ]; then suffix=recovery-keys field=recovery_keys_b64; fi
        if ! [[ $digest =~ ^[a-f0-9]{64}$ ]] ||
            { [ -n "$expected" ] && [ "$(bootstrap_credential_digest "$mode" "$expected")" != "$digest" ]; }; then
            log_error "Missing or mismatched bootstrap credential digest; refusing to mark complete"
            return 1
        fi
        secondary=$(bootstrap_read_secret "$namespace" "$statefulset-$suffix") || return 1
        saved=$(printf '%s' "$secondary" | jq -er --arg field "$field" '.data[$field] | @base64d') || saved=""
        actual=$(bootstrap_credential_digest "$mode" "$saved") || actual=""
        if [ "$actual" != "$digest" ]; then
            if [ "$state" = complete ]; then reset_bootstrap_completion "$namespace" "$statefulset" "$version" || true; fi
            log_error "Bootstrap credentials are incomplete or mismatched; recover the original '$statefulset-$suffix' credential"
            return 1
        fi
        # An ambiguous write can return another attempt's revision. Restart
        # from fresh reads of both Secrets before verifying that revision.
        if [ "$state" = pending ]; then
            snapshot=$(bootstrap_kubectl patch secret "$statefulset-root-token" -n "$namespace" --type=merge \
                --patch "$(jq -cn --arg version "$version" --arg state "$(printf complete | base64 | tr -d '\n')" \
                    '{metadata: {resourceVersion: $version}, data: {bootstrap_state: $state}}')" -o json 2>/dev/null) || snapshot=""
            if [ -z "$snapshot" ]; then
                snapshot=$(bootstrap_read_secret "$namespace" "$statefulset-root-token") || return 1
                if bootstrap_completion_matches "$snapshot" "$encoded" "$digest" &&
                    [ "$(printf '%s' "$snapshot" | jq -r '.data.bootstrap_verified // "" | @base64d')" = true ]; then return 0; fi
                if [ "$attempt" -lt 3 ]; then bootstrap_pause || true; fi
                continue
            fi
        fi
        if ! bootstrap_completion_matches "$snapshot" "$encoded" "$digest"; then
            if [ "$attempt" -lt 3 ]; then bootstrap_pause || true; fi
            continue
        fi
        verified=$(printf '%s' "$snapshot" | jq -r '.data.bootstrap_verified // "" | @base64d') || return 1
        [ "$verified" != true ] || return 0
        verification_version=$(printf '%s' "$snapshot" | jq -er '.metadata.resourceVersion') || return 1
        # Secrets cannot be committed together. Check the secondary version and
        # data after the root CAS, then revoke completion on a concurrent change.
        confirmed=$(bootstrap_read_secret "$namespace" "$statefulset-$suffix") || confirmed='{}'
        if ! printf '%s\n%s\n' "$secondary" "$confirmed" | jq -es '
            length == 2 and (.[0].metadata.uid | type == "string") and
            .[0].metadata.uid == .[1].metadata.uid and
            .[0].metadata.resourceVersion == .[1].metadata.resourceVersion and .[0].data == .[1].data' >/dev/null; then
            reset_bootstrap_completion "$namespace" "$statefulset" "$verification_version" || true
            log_error "Bootstrap credential Secret changed during completion; refusing to continue"
            return 1
        fi
        snapshot=$(bootstrap_read_secret "$namespace" "$statefulset-root-token") || return 1
        if bootstrap_completion_matches "$snapshot" "$encoded" "$digest"; then
            verified=$(printf '%s' "$snapshot" | jq -r '.data.bootstrap_verified // "" | @base64d') || return 1
            [ "$verified" != true ] || return 0
            version=$(printf '%s' "$snapshot" | jq -er '.metadata.resourceVersion') || return 1
            if [ "$version" != "$verification_version" ]; then
                if [ "$attempt" -lt 3 ]; then bootstrap_pause || true; fi
                continue
            fi
            bootstrap_kubectl patch secret "$statefulset-root-token" -n "$namespace" --type=merge \
                --patch "$(jq -cn --arg version "$version" --arg verified "$(printf true | base64 | tr -d '\n')" \
                    '{metadata: {resourceVersion: $version}, data: {bootstrap_verified: $verified}}')" >/dev/null 2>&1 || true
            snapshot=$(bootstrap_read_secret "$namespace" "$statefulset-root-token") || return 1
            if bootstrap_completion_matches "$snapshot" "$encoded" "$digest" &&
                [ "$(printf '%s' "$snapshot" | jq -r '.data.bootstrap_verified // "" | @base64d')" = true ]; then return 0; fi
        fi
        if [ "$attempt" -lt 3 ]; then bootstrap_pause || true; fi
    done
    log_error "Could not record bootstrap credential completion"
    return 1
}

# BOOTSTRAP_INITIALIZED is read by the deployment script.
# shellcheck disable=SC2034
check_bootstrap_credentials() {
    local namespace=$1 statefulset=$2 mode=$3 status root key existing
    local bootstrap_deadline bootstrap_request_timeout
    bootstrap_start_persistence || return 1
    if ! bootstrap_credential_digest shamir preflight >/dev/null; then
        log_error "A working sha256sum or shasum is required before OpenBao initialization"
        return 1
    fi
    BOOTSTRAP_INITIALIZED=false
    status=$(openbao_status "$namespace" "$statefulset-0") || return 1
    if [ "$(printf '%s' "$status" | jq -r '.initialized')" = true ]; then
        root=$(get_root_token "$namespace" "$statefulset") || return 1
        if [ -z "$root" ] || [ "$root" = null ]; then
            log_error "OpenBao is initialized but the saved root token is missing; restore bootstrap credentials"
            return 1
        fi
        if [ "$mode" = shamir ]; then
            key=$(get_unseal_key "$namespace" "$statefulset") || return 1
            if [ -z "$key" ] || [ "$key" = null ]; then
                log_error "OpenBao is initialized but the saved unseal key is missing; restore bootstrap credentials"
                return 1
            fi
        fi
        complete_bootstrap_credentials "$namespace" "$statefulset" "$mode" "$root" || return 1
        BOOTSTRAP_INITIALIZED=true
        log_info "OpenBao is initialized; reusing saved bootstrap credentials"
        return 0
    fi
    local suffix
    for suffix in root-token recovery-keys; do
        existing=$(bootstrap_kubectl get secret "$statefulset-$suffix" \
            -n "$namespace" --ignore-not-found -o name) || return 1
        if [ -n "$existing" ]; then
            log_error "OpenBao is uninitialized but bootstrap credentials exist; restore the matching data"
            return 1
        fi
    done
    if [ "$mode" = shamir ]; then
        key=$(get_unseal_key "$namespace" "$statefulset") || return 1
        if [ -n "$key" ]; then
            log_error "OpenBao is uninitialized but an unseal key exists; restore the matching data"
            return 1
        fi
    fi
}

unseal_openbao_members() {
    local namespace=$1 statefulset=$2 mode=$3 i status key
    local bootstrap_deadline=""
    bootstrap_start_wait || return 1
    wait_for_openbao_api "$namespace" "$statefulset" || return 1
    if [ "$mode" = shamir ]; then
        # The saved credential read may use the remaining phase budget.
        key=$(bootstrap_request_timeout=$((bootstrap_deadline - SECONDS)) get_unseal_key "$namespace" "$statefulset") || return 1
        if [ -z "$key" ] || [ "$key" = null ]; then
            log_error "Cannot unseal OpenBao without the saved unseal key"
            return 1
        fi
        for ((i = 0; i < OPENBAO_REPLICAS; i++)); do
            while [ "$SECONDS" -lt "$bootstrap_deadline" ]; do
                status=$(openbao_status "$namespace" "$statefulset-$i") || status=""
                if [ -n "$status" ]; then
                    if printf '%s' "$status" | jq -e '.initialized and (.sealed | not)' >/dev/null; then
                        break
                    fi
                    if [ "$(printf '%s' "$status" | jq -r '.initialized')" = false ]; then
                        if [ "$i" = 0 ]; then
                            log_error "The primary member is not initialized"
                            return 1
                        fi
                        if ! bootstrap_kubectl exec "$statefulset-$i" -c openbao -n "$namespace" -- \
                            bao operator raft join "http://$statefulset-0.$statefulset-internal:8200"; then
                            log_warn "Raft join failed for '$statefulset-$i'; rechecking member status before retrying"
                            bootstrap_pause || true
                            continue
                        fi
                    fi
                    bootstrap_kubectl exec "$statefulset-$i" -c openbao -n "$namespace" -- \
                        bao operator unseal "$key" >/dev/null || log_warn "Unseal failed for '$statefulset-$i'; retrying"
                fi
                bootstrap_pause || true
            done
        done
    fi
    wait_for_openbao_members "$namespace" "$statefulset" false || return 1
    log_success "All OpenBao members are initialized and unsealed"
}

wait_for_openbao_members() {
    local namespace=$1 statefulset=$2 require_root=$3 i status complete replicas confirmed
    while :; do
        complete=true
        replicas=$(bootstrap_kubectl get statefulset "$statefulset" -n "$namespace" -o jsonpath='{.spec.replicas}') || replicas=""
        if [[ $replicas =~ ^[1-9][0-9]*$ ]]; then
            OPENBAO_REPLICAS=$replicas
        else
            complete=false
        fi
        for ((i = 0; i < OPENBAO_REPLICAS; i++)); do
            [ "$complete" = true ] || break
            status=$(openbao_status "$namespace" "$statefulset-$i") || status=""
            if [ -z "$status" ] || ! printf '%s' "$status" | jq -e '.initialized and (.sealed | not)' >/dev/null; then
                complete=false
                break
            fi
        done
        if [ "$complete" = true ] && [ "$require_root" = true ]; then
            bootstrap_credentials_ready "$namespace" "$statefulset" || complete=false
        fi
        if [ "$complete" = true ]; then
            confirmed=$(bootstrap_kubectl get statefulset "$statefulset" -n "$namespace" -o jsonpath='{.spec.replicas}') || confirmed=""
            if [ "$confirmed" = "$replicas" ] && [ "$SECONDS" -lt "$bootstrap_deadline" ]; then
                return 0
            fi
        fi
        if [ "$SECONDS" -ge "$bootstrap_deadline" ]; then
            log_error "Timed out waiting for OpenBao bootstrap to finish"
            return 1
        fi
        bootstrap_pause || true
    done
}

# Hooks must not race credential persistence, even without Helm job waits.
wait_for_openbao_bootstrap() {
    local namespace=$1 statefulset=$2 bootstrap_deadline="" job
    bootstrap_start_wait || return 1
    if [ -n "${OPENBAO_BOOTSTRAP_JOB_NAME:-}" ]; then
        local api_timeout=${OPENBAO_API_TIMEOUT_SECONDS-600} credential_timeout=${OPENBAO_CREDENTIAL_TIMEOUT_SECONDS-120} timeout
        if ! [[ $credential_timeout =~ ^[1-9][0-9]*$ ]]; then
            log_error "OPENBAO_CREDENTIAL_TIMEOUT_SECONDS must be a positive integer"
            return 1
        fi
        timeout=${OPENBAO_BOOTSTRAP_TIMEOUT_SECONDS-$((3 * api_timeout + 4 * credential_timeout + 120))}
        if ! [[ $timeout =~ ^[1-9][0-9]*$ ]]; then
            log_error "OPENBAO_BOOTSTRAP_TIMEOUT_SECONDS must be a positive integer"
            return 1
        fi
        bootstrap_deadline=$((SECONDS + timeout))
        while :; do
            job=$(bootstrap_kubectl get job "$OPENBAO_BOOTSTRAP_JOB_NAME" -n "$namespace" -o json) || job='{}'
            if printf '%s' "$job" | jq -e 'any(.status.conditions[]?; .type == "Complete" and .status == "True")' >/dev/null; then break; fi
            if printf '%s' "$job" | jq -e 'any(.status.conditions[]?; .type == "Failed" and .status == "True")' >/dev/null; then
                log_error "OpenBao bootstrap Job failed"
                return 1
            fi
            if [ "$SECONDS" -ge "$bootstrap_deadline" ]; then
                log_error "Timed out waiting for OpenBao bootstrap Job"
                return 1
            fi
            bootstrap_pause || true
        done
        bootstrap_deadline=""
        bootstrap_start_wait || return 1
    fi
    wait_for_openbao_api "$namespace" "$statefulset" || return 1
    wait_for_openbao_members "$namespace" "$statefulset" true
}
