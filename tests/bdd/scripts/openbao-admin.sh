#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

context=$1 namespace=$2 pod=$3
shift 3
root_token=$(kubectl --context "$context" -n "$namespace" get secret openbao-server-root-token \
    -o jsonpath='{.data.root_token}' | base64 -d)
test -n "$root_token"
# The remote shell reads the token from stdin and expands its own arguments.
# shellcheck disable=SC2016
printf '%s' "$root_token" | kubectl --context "$context" -n "$namespace" exec -i "$pod" \
    -c openbao -- sh -ec 'BAO_TOKEN=$(cat); export BAO_TOKEN; exec bao "$@"' qa-admin "$@"
