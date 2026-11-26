#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/e2e-reverse-tunnel-liveness.sh [--no-build]

Runs a real Stargate with reverse tunnels, two Pylons, and two mock-dynamo
backends on loopback, and checks the reverse tunnel QUIC liveness flags
(--reverse-tunnel-keep-alive-interval-ms and
--reverse-tunnel-max-idle-timeout-ms). For each tunnel protocol:

  1. Both Pylons connect their reverse tunnels, and streaming chat
     completions sent to Stargate return 200 with a complete SSE stream
     from both backends.
  2. With no client traffic for three idle timeouts, neither tunnel closes
     or reconnects, and both backends still serve. Stargate's own 1 s
     health probes still cross the tunnels.
  3. One Pylon is frozen with SIGSTOP, so its tunnel goes silent without a
     close. Stargate must close that tunnel within the idle timeout plus
     3 s, and the other backend keeps serving.
  4. The frozen Pylon resumes with SIGCONT, reconnects, and serves again.

Before the cases, Stargate must refuse to start with a keep-alive interval
that is not below the idle timeout.

The script exits non-zero when any expectation fails.

Options:
  --no-build        Reuse the binaries in the release target directory.

Environment:
  E2E_TUNNEL_PROTOCOLS  Protocols to test, default "raw-quic http3 webtransport".
  E2E_KEEP_ALIVE_MS     --reverse-tunnel-keep-alive-interval-ms, default 200.
  E2E_IDLE_TIMEOUT_MS   --reverse-tunnel-max-idle-timeout-ms, default 1000.
  E2E_PORT_BASE         First loopback port, default 27200. Uses 32 ports.
  E2E_WORK_DIR          Logs and configs, kept after the run. Default: a new
                        mktemp directory, removed on success.
  CARGO_TARGET_DIR      Cargo target directory, default ./target.

Requires curl and jq. Building also needs cargo.
EOF
}

build=1
for arg in "$@"; do
    case "${arg}" in
        --no-build) build=0 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: ${arg}" >&2; usage >&2; exit 2 ;;
    esac
done

for tool in curl jq; do
    command -v "${tool}" >/dev/null || { echo "${tool} is required" >&2; exit 2; }
done

stargate_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_dir="${CARGO_TARGET_DIR:-${stargate_root}/target}"
release_dir="${target_dir}/release"
port_base="${E2E_PORT_BASE:-27200}"
work_dir="${E2E_WORK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/stargate-e2e-tunnel-liveness.XXXXXX")}"
mkdir -p "${work_dir}"

protocols="${E2E_TUNNEL_PROTOCOLS:-raw-quic http3 webtransport}"
keep_alive_ms="${E2E_KEEP_ALIVE_MS:-200}"
idle_timeout_ms="${E2E_IDLE_TIMEOUT_MS:-1000}"
# Seconds without client traffic, three idle timeouts and at least 3 s.
quiet_seconds=$(((3 * idle_timeout_ms + 999) / 1000))
[[ ${quiet_seconds} -ge 3 ]] || quiet_seconds=3
# Longest accepted time from SIGSTOP to Stargate closing the tunnel. The first
# packet Stargate sends after the frozen Pylon's last one restarts the idle
# timer; the 1 s health probes keep that extension under the 3 s margin.
detect_limit_seconds=$(((idle_timeout_ms + 3000 + 999) / 1000))
# Pylon keeps the quinn default 30 s idle timeout on its side, so after
# SIGCONT it may take that long to notice the closed tunnel and reconnect.
reconnect_limit_seconds=60

model="e2e-model"
mock_tokens=4

sg_grpc=$((port_base))
sg_http=$((port_base + 1))
sg_metrics=$((port_base + 2))
sg_discovery=$((port_base + 3))
sg_reverse=$((port_base + 4))
mock_port() { echo $((port_base + 10 + 10 * $1)); }
pylon_metrics_port() { echo $((port_base + 11 + 10 * $1)); }
cluster_names=(a b)

pids=()
cleanup() {
    local pid
    for pid in "${pids[@]+"${pids[@]}"}"; do
        kill -CONT "${pid}" 2>/dev/null || true
        kill "${pid}" 2>/dev/null || true
    done
    for pid in "${pids[@]+"${pids[@]}"}"; do
        wait "${pid}" 2>/dev/null || true
    done
    pids=()
}
on_exit() {
    local status=$?
    cleanup
    if [[ ${status} -ne 0 ]]; then
        echo "FAIL: logs kept in ${work_dir}" >&2
    elif [[ -z "${E2E_WORK_DIR:-}" ]]; then
        rm -rf "${work_dir}"
    fi
    exit "${status}"
}
trap on_exit EXIT
trap 'exit 130' INT TERM

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

port_open() {
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

wait_for() {
    local description="$1" deadline=$((SECONDS + 30))
    shift
    until "$@" >/dev/null 2>&1; do
        [[ ${SECONDS} -lt ${deadline} ]] || fail "timed out waiting for ${description}"
        sleep 0.2
    done
}

start() {
    local log="$1"
    shift
    "$@" >"${log}" 2>&1 &
    pids+=("$!")
    last_pid=$!
}

stargate_args() {
    local protocol="$1"
    printf '%s\n' \
        --stargate-id e2e-stargate \
        --listen-addr "127.0.0.1:${sg_grpc}" \
        --http-listen-addr "127.0.0.1:${sg_http}" \
        --model-discovery-listen-addr "127.0.0.1:${sg_discovery}" \
        --advertise-addr "127.0.0.1:${sg_grpc}" \
        --stargate-discovery-dns-name localhost \
        --disable-dns-discovery \
        --metrics-port "${sg_metrics}" \
        --lb-config-path "${work_dir}/lb-config.json" \
        --backend-connectivity=reverse \
        --reverse-tunnel-listen-addr "127.0.0.1:${sg_reverse}" \
        --advertised-hostname-template localhost \
        --tunnel-protocol "${protocol}" \
        --quic-insecure
}

# Stargate must reject a keep-alive that cannot fire before the idle timeout.
check_rejects_late_keep_alive() {
    local args=() log="${work_dir}/stargate-late-keep-alive.log" pid deadline=$((SECONDS + 10))
    while IFS= read -r line; do args+=("${line}"); done < <(stargate_args raw-quic)
    "${release_dir}/stargate" "${args[@]}" \
        --reverse-tunnel-keep-alive-interval-ms 1000 \
        --reverse-tunnel-max-idle-timeout-ms 1000 >"${log}" 2>&1 &
    pid=$!
    while kill -0 "${pid}" 2>/dev/null; do
        if [[ ${SECONDS} -ge ${deadline} ]]; then
            kill "${pid}" 2>/dev/null || true
            wait "${pid}" 2>/dev/null || true
            fail "stargate started with a keep-alive equal to the idle timeout"
        fi
        sleep 0.1
    done
    if wait "${pid}"; then
        fail "stargate exited 0 with a keep-alive equal to the idle timeout"
    fi
    grep -q "must be less than" "${log}" ||
        fail "stargate rejected the flags without the expected error: $(cat "${log}")"
    echo "PASS: stargate rejects a keep-alive that is not below the idle timeout"
}

tunnel_connects() {
    grep -c "reverse tunnel connected" "${run_dir}/pylon-$1.log" || true
}

tunnel_closes() {
    grep -E "reverse (tunnel|WebTransport) connection closed" "${run_dir}/stargate.log" |
        grep -c "pylon-$1" || true
}

# Sends one streaming chat completion to Stargate. Checks the status, that
# the stream carries mock_tokens content chunks, a stop, matching usage, and
# a final [DONE], then prints the Pylon that served it.
chat_request() {
    local request_id="$1" body="${run_dir}/response-$1.txt" status events backend
    status=$(curl -sS -N --max-time 20 -o "${body}" -D "${body}.headers" -w '%{http_code}' \
        "http://127.0.0.1:${sg_http}/v1/chat/completions" \
        -H 'content-type: application/json' \
        -H "x-request-id: ${request_id}" \
        -H "x-model: ${model}" \
        -H 'x-input-tokens: 8' \
        -H "x-cache-affinity-key: ${request_id}" \
        -d "{\"model\":\"${model}\",\"stream\":true,\"max_tokens\":${mock_tokens},\"stream_options\":{\"include_usage\":true},\"messages\":[{\"role\":\"user\",\"content\":\"hello\"}]}") ||
        { echo "curl-error"; return 0; }
    if [[ "${status}" != "200" ]]; then
        echo "status-${status}"
        return 0
    fi
    [[ "$(grep '^data: ' "${body}" | tail -1)" == "data: [DONE]" ]] ||
        fail "${request_id}: stream did not end with [DONE]: $(head -c 400 "${body}")"
    events=$(grep '^data: {' "${body}" | sed 's/^data: //' | jq -s -c '{
        content: [.[] | .choices[]? | .delta.content // empty] | length,
        stops: [.[] | .choices[]? | select(.finish_reason == "stop")] | length,
        usage: ([.[] | .usage // empty | .completion_tokens] | last)
    }') || fail "${request_id}: stream has invalid JSON events"
    [[ "${events}" == "{\"content\":${mock_tokens},\"stops\":1,\"usage\":${mock_tokens}}" ]] ||
        fail "${request_id}: unexpected stream contents ${events}"
    backend=$(tr -d '\r' <"${body}.headers" | awk -F': ' 'tolower($1) == "x-inference-server-id" {print $2}')
    [[ -n "${backend}" ]] || fail "${request_id}: response has no x-inference-server-id"
    echo "${backend}"
}

# Sends requests until both Pylons have served one, failing on any request
# that does not succeed.
expect_both_backends_serve() {
    local label="$1" index served seen_a=0 seen_b=0
    for index in $(seq 1 10); do
        served=$(chat_request "${label}-${index}")
        case "${served}" in
            pylon-a) seen_a=1 ;;
            pylon-b) seen_b=1 ;;
            *) fail "${label}-${index}: request failed (${served})" ;;
        esac
        [[ ${seen_a} -eq 1 && ${seen_b} -eq 1 ]] && return 0
    done
    fail "${label}: both backends should serve within 10 requests (a=${seen_a} b=${seen_b})"
}

start_stack() {
    local protocol="$1" index name port
    for port in "${sg_grpc}" "${sg_http}" "${sg_metrics}" "${sg_discovery}" \
        "$(mock_port 0)" "$(mock_port 1)" "$(pylon_metrics_port 0)" "$(pylon_metrics_port 1)"; do
        if port_open "${port}"; then
            fail "port ${port} is already in use; set E2E_PORT_BASE"
        fi
    done

    local args=()
    while IFS= read -r line; do args+=("${line}"); done < <(stargate_args "${protocol}")
    start "${run_dir}/stargate.log" env RUST_LOG=info NO_COLOR=1 \
        "${release_dir}/stargate" "${args[@]}" \
        --reverse-tunnel-keep-alive-interval-ms "${keep_alive_ms}" \
        --reverse-tunnel-max-idle-timeout-ms "${idle_timeout_ms}"

    for index in 0 1; do
        name="${cluster_names[${index}]}"
        start "${run_dir}/mock-${name}.log" \
            "${release_dir}/mock-dynamo" \
            --http-listen-addr "127.0.0.1:$(mock_port "${index}")" \
            --model-name "${model}" \
            --num-tokens "${mock_tokens}" \
            --token-delay-ms 5
    done
    wait_for "stargate metrics" curl -sf "http://127.0.0.1:${sg_metrics}/metrics"
    for index in 0 1; do
        name="${cluster_names[${index}]}"
        wait_for "mock-dynamo ${name}" curl -sf "http://127.0.0.1:$(mock_port "${index}")/health"
        start "${run_dir}/pylon-${name}.log" env RUST_LOG=info NO_COLOR=1 \
            "${release_dir}/pylon" \
            --upstream-http-base-url "http://127.0.0.1:$(mock_port "${index}")" \
            --model-name "${model}" \
            --stargate-address "127.0.0.1:${sg_grpc}" \
            --inference-server-id "pylon-${name}" \
            --cluster-id "cluster-${name}" \
            --backend-connectivity reverse \
            --tunnel-protocol "${protocol}" \
            --quic-insecure \
            --kv-cache-stats-path /kv-cache/stats \
            --min-update-interval-ms 100 \
            --disable-bringup \
            --active-canary-interval-ms=0 \
            --initial-input-tps 1000 \
            --metrics-host 127.0.0.1 \
            --metrics-port "$(pylon_metrics_port "${index}")"
        pylon_pids[index]=${last_pid}
    done
    wait_for "pylon-a reverse tunnel" grep -q "reverse tunnel connected" "${run_dir}/pylon-a.log"
    wait_for "pylon-b reverse tunnel" grep -q "reverse tunnel connected" "${run_dir}/pylon-b.log"
}

run_case() {
    local protocol="$1"
    run_dir="${work_dir}/${protocol}"
    mkdir -p "${run_dir}"
    echo "=== ${protocol}, keep-alive ${keep_alive_ms} ms, idle timeout ${idle_timeout_ms} ms ==="
    pylon_pids=()
    start_stack "${protocol}"

    # 1. Both backends serve complete streams through their tunnels.
    expect_both_backends_serve warmup
    expect_both_backends_serve serve
    echo "PASS: both reverse tunnels serve complete streams"

    # 2. Quiet tunnels stay open without reconnecting.
    local connects_a connects_b
    connects_a=$(tunnel_connects a)
    connects_b=$(tunnel_connects b)
    sleep "${quiet_seconds}"
    local name
    for name in a b; do
        [[ "$(tunnel_closes "${name}")" == 0 ]] ||
            fail "stargate closed pylon-${name}'s tunnel"
    done
    [[ "$(tunnel_connects a)" == "${connects_a}" && "$(tunnel_connects b)" == "${connects_b}" ]] ||
        fail "a pylon reconnected during the quiet period"
    expect_both_backends_serve after-quiet
    echo "PASS: tunnels stayed open for ${quiet_seconds} s without client traffic"

    # 3. A frozen Pylon's tunnel is closed within the idle timeout.
    # Count from here: a close and reconnect during after-quiet must not count.
    local started=${SECONDS} elapsed closes_a closes_b
    closes_a=$(tunnel_closes a)
    closes_b=$(tunnel_closes b)
    kill -STOP "${pylon_pids[1]}"
    until [[ "$(tunnel_closes b)" -gt "${closes_b}" ]]; do
        elapsed=$((SECONDS - started))
        [[ ${elapsed} -le ${detect_limit_seconds} ]] ||
            fail "stargate kept the frozen pylon-b tunnel for more than ${detect_limit_seconds} s"
        sleep 0.1
    done
    elapsed=$((SECONDS - started))
    [[ "$(tunnel_closes a)" == "${closes_a}" ]] || fail "stargate closed pylon-a's healthy tunnel"
    echo "PASS: stargate closed the frozen pylon-b tunnel after about ${elapsed} s (limit ${detect_limit_seconds} s)"

    local index served served_a=0 others=()
    for index in $(seq 1 6); do
        served=$(chat_request "frozen-${index}")
        case "${served}" in
            pylon-a) served_a=$((served_a + 1)) ;;
            *) others+=("${served}") ;;
        esac
    done
    [[ ${served_a} -gt 0 ]] || fail "pylon-a should keep serving while pylon-b is frozen"
    echo "PASS: pylon-a served ${served_a} of 6 requests while pylon-b was frozen; other results: ${others[*]:-none}"

    # 4. The resumed Pylon reconnects and serves again. Count from here, so a
    # reconnect during after-quiet does not satisfy the wait.
    connects_b=$(tunnel_connects b)
    kill -CONT "${pylon_pids[1]}"
    wait_for_reconnect "${connects_b}"
    expect_both_backends_serve after-resume
    echo "PASS: pylon-b reconnected and serves after resuming"
    echo "PASS: ${protocol}"
    cleanup
}

wait_for_reconnect() {
    local connects_before="$1" deadline=$((SECONDS + reconnect_limit_seconds))
    until [[ "$(tunnel_connects b)" -gt "${connects_before}" ]]; do
        [[ ${SECONDS} -lt ${deadline} ]] ||
            fail "pylon-b did not reconnect within ${reconnect_limit_seconds} s after resuming"
        sleep 0.2
    done
}

if [[ ${build} -eq 1 ]]; then
    (cd "${stargate_root}" && cargo build --locked --release -p stargate -p pylon -p mock-dynamo)
fi
for binary in stargate pylon mock-dynamo; do
    [[ -x "${release_dir}/${binary}" ]] || fail "missing ${release_dir}/${binary}"
done

echo '{"default": "round-robin"}' >"${work_dir}/lb-config.json"
check_rejects_late_keep_alive
for protocol in ${protocols}; do
    run_case "${protocol}"
done
echo "PASS: all reverse tunnel liveness checks"
