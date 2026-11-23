#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Asserts the utils provenance wiring the chart renders. The defaults (audit, no
# username overrides) render no operator flags, so an upgrade does not restart the
# operator. A non-default mode and the controller username overrides render their
# flags, and the schema rejects an unknown mode before it reaches the operator.

set -euo pipefail

chart_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
chart="${chart_root}/nvca-operator"
work_dir="$(mktemp -d)"
trap 'rm -rf "${work_dir}"' EXIT

fail() {
  echo "utils-provenance-values: FAIL $*" >&2
  exit 1
}

render() {
  local out=${1}
  shift
  helm template test-release "${chart}" --namespace nvca-operator \
    --set-string "ngcConfig.serviceKey=fakekey" "$@" >"${out}"
}

operator_args='select(.kind == "Deployment") | .spec.template.spec.containers[0].args[]'

args_of() {
  yq "${operator_args}" "${1}" | grep -v -e '^---$' -e '^$' || true
}

default_out="${work_dir}/default.yaml"
render "${default_out}"
if args_of "${default_out}" | grep -q -- '--utils-provenance'; then
  fail "defaults must not render utils provenance flags"
fi

explicit_audit="${work_dir}/audit.yaml"
render "${explicit_audit}" --set agent.utilsProvenance.mode=audit
if args_of "${explicit_audit}" | grep -q -- '--utils-provenance'; then
  fail "mode audit is the default and must not render a flag"
fi

enforce_out="${work_dir}/enforce.yaml"
render "${enforce_out}" \
  --set agent.utilsProvenance.mode=enforce \
  --set-string agent.utilsProvenance.replicaSetControllerUser=rs-user \
  --set-string agent.utilsProvenance.deploymentControllerUser=dep-user
args_of "${enforce_out}" >"${work_dir}/enforce.args"
for want in '--utils-provenance-mode' 'enforce' \
  '--utils-provenance-replicaset-controller-user' 'rs-user' \
  '--utils-provenance-deployment-controller-user' 'dep-user'; do
  grep -qx -- "${want}" "${work_dir}/enforce.args" || fail "missing ${want} in the rendered operator args"
done

if render "${work_dir}/bad.yaml" --set agent.utilsProvenance.mode=block 2>"${work_dir}/bad.err"; then
  fail "an unknown mode must be rejected by the schema"
fi
grep -q "must be one of" "${work_dir}/bad.err" || fail "expected a schema enum error, got: $(cat "${work_dir}/bad.err")"

echo "utils-provenance-values: PASS"
