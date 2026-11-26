#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

case "$1" in
  paths)
    selected="$2"
    while IFS= read -r path || [ -n "$path" ]; do
      case "$path" in
        src/libraries/rust/stargate/*.md|src/libraries/rust/stargate/docs/*) ;;
        src/libraries/rust/stargate/*|\
        src/control-plane-services/cloud-functions/nvcf-core/src/main/proto/llm_gateway.proto|\
        src/invocation-plane-services/llm-api-gateway/nvcf/pb/llm_gateway.proto|\
        .github/workflows/bazel.yml) selected=true ;;
      esac
    done
    printf '%s\n' "$selected"
    ;;
  releases)
    if [ -z "$2" ] || [ -z "$3" ]; then
      echo "target and default branch are required" >&2
      exit 2
    fi
    if [ "${2#refs/heads/}" = "$3" ]; then echo true; else echo false; fi
    ;;
  *) echo "usage: compatibility_scope.sh paths <run-all> | releases <target> <default-branch>" >&2; exit 2 ;;
esac
