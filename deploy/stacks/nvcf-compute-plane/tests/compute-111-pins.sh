#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Asserts the emSign-fixed selection of the 1.1.x patch line.
set -euo pipefail

stack_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
base="${stack_dir}/environments/base.yaml"
dependencies="${stack_dir}/helmfile.d/01-dependencies.yaml.gotmpl"

fail() {
  echo "compute-111-pins: $*" >&2
  exit 1
}

check() {
  local label="$1" got="$2" want="$3"
  test "${got}" = "${want}" || fail "${label} is ${got}, expected ${want}"
}

check "BYOO OTel collector" "$(yq -er '.global.nvcaOperator.byooOtelCollector.imageTag' "${base}")" "0.160.0-nv-0.4.9"
check "NVCF OTel collector" "$(yq -er '.global.nvcaOperator.selfManaged.otelCollector.imageTag' "${base}")" "0.160.0-nv-0.4.9"
check "NVCA operator image" "$(yq -er '.global.nvcaOperator.imageTag' "${base}")" "3.18.10"
check "NVCA version" "$(yq -er '.global.nvcaOperator.selfManaged.nvcaVersion' "${base}")" "3.18.10"
reloader="$(awk '/Keep the bundled Dynamo NATS reloader aligned/{f=1} f&&/tag:/{gsub(/[" ]/,"",$2); print $2; exit}' "${dependencies}")"
check "Dynamo NATS reloader" "${reloader}" "0.25.0"

echo "compute-111-pins: ok"
