#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Liveness must use /livez on the admin port. The client port's /health fails
# while NATS is reconnecting or nvcf-api is unreachable, so using it for
# liveness restarts every grpc-proxy pod during a shared dependency outage.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chart_dir="${script_dir}/../grpc-proxy"
repo_root="$(cd "${script_dir}/../../../.." && pwd)"
ci_values="${repo_root}/tools/ci/helm-validate-values/grpc-proxy.yaml"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# check_probes MODE KIND [helm args...] renders the chart and checks the
# workload's probes and the admin port the liveness probe names.
check_probes() {
  local mode="$1" kind="$2"
  shift 2
  local manifest="${tmp_dir}/${mode}.yaml" container actual
  helm template grpc-proxy "${chart_dir}" --namespace nvcf \
    --values "${ci_values}" "$@" > "${manifest}"
  container="select(.kind == \"${kind}\") | .spec.template.spec.containers[0]"

  actual="$(yq -r "${container} | .livenessProbe.httpGet.port + \" \" + .livenessProbe.httpGet.path" "${manifest}")"
  [[ "${actual}" == "metrics /livez" ]] || fail "${mode}: liveness: expected 'metrics /livez', got '${actual}'"

  actual="$(yq -r "${container} | .readinessProbe.httpGet.port + \" \" + .readinessProbe.httpGet.path" "${manifest}")"
  [[ "${actual}" == "client /health" ]] || fail "${mode}: readiness: expected 'client /health', got '${actual}'"

  actual="$(yq -r "${container} | .ports[] | select(.name == \"metrics\") | .containerPort" "${manifest}")"
  [[ "${actual}" == "10083" ]] || fail "${mode}: port 'metrics': expected 10083, got '${actual}'"
}

check_probes deployment Deployment
check_probes daemonset DaemonSet --set grpcproxy.deploymentType=daemonset
check_probes http3 Deployment \
  --set grpcproxy.http3.enabled=true \
  --set grpcproxy.http3.tls.secretName=worker-callback-tls

echo "PASS: grpc-proxy liveness uses /livez on the admin port, readiness uses /health on the client port"
