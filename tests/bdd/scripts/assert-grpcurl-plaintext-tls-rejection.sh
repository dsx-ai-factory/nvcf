#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Normalize only the complete grpcurl 1.9.3 dial-stage diagnostic observed when
# plaintext HTTP/2 is sent to the verified TLS listener at the given endpoint.
# RPC stream deadlines, additional output, and unrelated command or endpoint
# failures remain errors.
#
# Usage:
#   assert-grpcurl-plaintext-tls-rejection.sh <endpoint> < grpcurl-output

set -euo pipefail

if [[ $# -ne 1 || -z "$1" ]]; then
  echo "usage: $0 <endpoint>" >&2
  exit 64
fi

diagnostic="$(cat)"
expected_diagnostic="Failed to dial target host \"$1\": context deadline exceeded"
if [[ "${diagnostic}" == "${expected_diagnostic}" ]]; then
  printf '%s\n' 'plaintext-watch-rejected=tls-listener-timeout'
  exit 0
fi

printf '%s\n' 'plaintext Watch failed without the expected TLS-listener dial timeout' >&2
exit 1
