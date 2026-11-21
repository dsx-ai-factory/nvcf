#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [[ $# -ne 7 ]]; then
  echo "usage: $0 <endpoint> <tls-server-name> <alpn> <ca-secret> <namespace> <kube-context> <timeout-seconds>" >&2
  exit 64
fi

endpoint="$1"
tls_server_name="$2"
alpn="$3"
ca_secret="$4"
namespace="$5"
kube_context="$6"
timeout_seconds="$7"

for value_name in endpoint tls_server_name alpn ca_secret namespace kube_context; do
  if [[ -z "${!value_name}" ]]; then
    echo "$value_name must be non-empty" >&2
    exit 64
  fi
done
if ! [[ "$timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "timeout-seconds must be a positive integer, got: $timeout_seconds" >&2
  exit 64
fi
for tool in kubectl base64 openssl; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "required tool not found: $tool" >&2
    exit 127
  fi
done

ca_file="$(mktemp "${TMPDIR:-/tmp}/nvcf-bdd-tls-probe-ca.XXXXXX")"
output_file="$(mktemp "${TMPDIR:-/tmp}/nvcf-bdd-tls-probe-output.XXXXXX")"
trap 'rm -f "$ca_file" "$output_file"' EXIT

kubectl --context "$kube_context" get secret "$ca_secret" -n "$namespace" \
  -o 'jsonpath={.data.ca\.crt}' | base64 -d >"$ca_file"
if [[ ! -s "$ca_file" ]]; then
  echo "CA secret $namespace/$ca_secret did not contain ca.crt" >&2
  exit 1
fi

# openssl s_client has no connect or handshake timeout, so the script bounds
# it with checks every 0.2 seconds. Closed stdin ends the session after the
# handshake.
openssl s_client -connect "$endpoint" -servername "$tls_server_name" -alpn "$alpn" \
  -verify_return_error -CAfile "$ca_file" </dev/null >"$output_file" 2>&1 &
probe_pid=$!
remaining_checks=$(( timeout_seconds * 5 ))
while kill -0 "$probe_pid" 2>/dev/null; do
  if [[ "$remaining_checks" -le 0 ]]; then
    kill "$probe_pid" 2>/dev/null || true
    wait "$probe_pid" 2>/dev/null || true
    cat "$output_file"
    echo "TLS handshake with $endpoint did not finish within ${timeout_seconds}s" >&2
    exit 1
  fi
  remaining_checks=$(( remaining_checks - 1 ))
  sleep 0.2
done

set +e
wait "$probe_pid"
probe_status=$?
set -e
cat "$output_file"
exit "$probe_status"
