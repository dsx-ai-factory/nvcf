#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <endpoint> <duration-seconds>" >&2
  exit 64
fi

endpoint="$1"
duration_seconds="$2"

if [[ -z "$endpoint" ]]; then
  echo "endpoint must be non-empty" >&2
  exit 64
fi
if ! [[ "$duration_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "duration-seconds must be a positive integer, got: $duration_seconds" >&2
  exit 64
fi
if ! command -v grpcurl >/dev/null 2>&1; then
  echo "required tool not found: grpcurl" >&2
  exit 127
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd "$script_dir/../../.." && pwd -P)"
proto_path="$repo_root/src/libraries/rust/stargate/crates/proto/proto"

set +e
output="$(grpcurl -plaintext -max-time "$duration_seconds" -import-path "$proto_path" \
  -proto stargate.proto "$endpoint" stargate.StargateControlPlane/WatchStargates 2>&1)"
grpcurl_status=$?
set -e

printf '%s\n' "$output" >&2
if [[ "$grpcurl_status" -eq 0 ]]; then
  echo "plaintext Watch unexpectedly succeeded" >&2
  exit 1
fi
printf '%s\n' "$output" | bash "$script_dir/assert-grpcurl-plaintext-tls-rejection.sh" "$endpoint"
