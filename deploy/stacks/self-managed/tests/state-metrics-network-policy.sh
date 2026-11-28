#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

stack_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT
fail() { echo "state-metrics-network-policy: $*" >&2; exit 1; }

# Fail if values export or chart rendering invokes the former patch tool.
mkdir -p "$work_dir/bin"
printf '#!/bin/sh\necho unexpected-kustomize >&2\nexit 99\n' >"$work_dir/bin/kustomize"
chmod +x "$work_dir/bin/kustomize"
export PATH="$work_dir/bin:$PATH"

fixture_chart="$work_dir/chart"
test_stack_dir="$work_dir/self-managed"
mkdir -p "$fixture_chart/templates" "$test_stack_dir/helmfile.d" \
  "$test_stack_dir/environments" "$test_stack_dir/secrets"
printf 'apiVersion: v2\nname: state-metrics-fixture\nversion: 0.1.0\n' >"$fixture_chart/Chart.yaml"
cat >"$fixture_chart/values.yaml" <<'YAML'
stateMetrics:
  service:
    port: 19464
  networkPolicy:
    enabled: true
    additionalIngressPeers: []
YAML
# A small chart fixture models the native peers interface. Set
# NVCF_STATE_METRICS_CHART to test the same wiring with the real chart.
cat >"$fixture_chart/templates/policy.yaml" <<'YAML'
{{- if .Values.stateMetrics.networkPolicy.enabled }}
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: state-metrics
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/instance: state-metrics
  policyTypes: [Ingress, Egress]
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: prometheus
        {{- with .Values.stateMetrics.networkPolicy.additionalIngressPeers }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      ports:
        - port: {{ .Values.stateMetrics.service.port }}
          protocol: TCP
  egress:
    - ports:
        - port: 53
          protocol: UDP
{{- end }}
YAML
chart="${NVCF_STATE_METRICS_CHART:-$fixture_chart}"
cp "$stack_dir/environments/base.yaml" "$test_stack_dir/environments/base.yaml"
cp "$stack_dir/global.yaml.gotmpl" "$test_stack_dir/global.yaml.gotmpl"
cp "$stack_dir/secrets/secrets.yaml.template" "$test_stack_dir/secrets/base-secrets.yaml"
cp "$stack_dir/helmfile.d/03-observability.yaml.gotmpl" "$test_stack_dir/helmfile.d/03-observability.yaml.gotmpl"

render() {
  local profile="$1"
  shift
  HELMFILE_ENV=base helmfile --log-level error \
    --file "$test_stack_dir/helmfile.d/03-observability.yaml.gotmpl" \
    --environment default --selector name=state-metrics \
    --state-values-set ingress.gatewayApi.gateways.shared.name=shared \
    --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy \
    --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc \
    --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy \
    --state-values-set "observability.profile=$profile" \
    "$@" write-values --output-file-template "$work_dir/values.yaml" >/dev/null
  helm template state-metrics "$chart" --namespace nvcf \
    --values "$work_dir/values.yaml" \
    | yq 'select(.kind == "NetworkPolicy")' >"$work_dir/policy.yaml"
}

render disabled
cp "$work_dir/policy.yaml" "$work_dir/default.yaml"
for profile in control all compute disabled; do
  render "$profile"
  policy="$work_dir/policy.yaml"
  expected=1
  if [[ "$profile" == control || "$profile" == all ]]; then
    expected=2
    yq -e '.spec.ingress[0].from[1].namespaceSelector.matchLabels."kubernetes.io/metadata.name" == "monitoring"' "$policy" >/dev/null
    yq -e '.spec.ingress[0].from[1].podSelector.matchLabels."app.kubernetes.io/component" == "opentelemetry-collector"' "$policy" >/dev/null
    yq -e '.spec.ingress[0].from[1].podSelector.matchLabels."app.kubernetes.io/instance" == "monitoring.nvcf-observability"' "$policy" >/dev/null
  fi
  test "$(yq '.spec.ingress[0].from | length' "$policy")" = "$expected" || fail "$profile ingress peers"
  for field in '.spec.ingress[0].from[0]' '.spec.ingress[0].ports' '.spec.egress' '.spec.podSelector' '.spec.policyTypes'; do
    yq "$field" "$work_dir/default.yaml" >"$work_dir/expected.yaml"
    yq "$field" "$policy" >"$work_dir/actual.yaml"
    diff -u "$work_dir/expected.yaml" "$work_dir/actual.yaml"
  done
done

render all --state-values-set observability.namespace=custom-metrics \
  --state-values-set collector.collectorName=custom-collector
yq -e '.spec.ingress[0].from[1].namespaceSelector.matchLabels."kubernetes.io/metadata.name" == "custom-metrics"' "$work_dir/policy.yaml" >/dev/null
yq -e '.spec.ingress[0].from[1].podSelector.matchLabels."app.kubernetes.io/instance" == "custom-metrics.custom-collector"' "$work_dir/policy.yaml" >/dev/null
cat >"$test_stack_dir/environments/custom.yaml" <<'YAML'
stateMetrics:
  networkPolicy:
    additionalIngressPeers:
      - namespaceSelector:
          matchLabels:
            kubernetes.io/metadata.name: custom-scrapers
YAML
# Layer the user peer into the real stack environment and retain it before the collector.
yq eval-all -i "select(fileIndex == 0) * select(fileIndex == 1)" "$test_stack_dir/environments/base.yaml" "$test_stack_dir/environments/custom.yaml"
render control
test "$(yq '.spec.ingress[0].from | length' "$work_dir/policy.yaml")" = 3
yq -e '.spec.ingress[0].from[1].namespaceSelector.matchLabels."kubernetes.io/metadata.name" == "custom-scrapers"' "$work_dir/policy.yaml" >/dev/null
render all --state-values-set stateMetrics.networkPolicy.enabled=false
test ! -s "$work_dir/policy.yaml" || fail "disabled policy rendered"
echo "state-metrics-network-policy: all checks passed"
