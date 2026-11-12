#!/usr/bin/env bash
# Test that the NVCF UI OpenBao migrations image tag follows the shared stack
# tag unless the add-on sets its own, and is a valid OCI image tag before the
# stack passes it to the chart.
set -euo pipefail

stack_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
test_stack_dir="$work_dir/self-managed"
environment_name="nvcf-ui-migrations-image-tag-test"
environment_file="$test_stack_dir/environments/$environment_name.yaml"
secrets_file="$test_stack_dir/secrets/$environment_name-secrets.yaml"
trap 'rm -rf "$work_dir"' EXIT

fail() {
  echo "nvcf-ui-migrations-image-tag: $*" >&2
  exit 1
}

mkdir -p "$test_stack_dir"
cp -R "$stack_dir"/. "$test_stack_dir"
printf '{}\n' >"$secrets_file"

write_env() {
  local tag="$1"

  cat >"$environment_file" <<YAML
global:
  image:
    registry: nvcr.io
    repository: test/nvcf
addons:
  nvcfUi:
    enabled: true
    openbaoMigrations:
      image:
        tag: "$tag"
YAML
}

# Shared stack tag only: no component tag set.
write_shared_env() {
  local tag="$1"

  cat >"$environment_file" <<YAML
global:
  image:
    registry: nvcr.io
    repository: test/nvcf
YAML
  if test -n "$tag"; then
    cat >>"$environment_file" <<YAML
  openbaoMigrations:
    image:
      tag: "$tag"
YAML
  fi
  cat >>"$environment_file" <<YAML
addons:
  nvcfUi:
    enabled: true
YAML
}

# LLS HMAC rotation: shared stack tag and an optional component tag.
write_lls_env() {
  local shared_tag="$1" lls_tag="$2"

  cat >"$environment_file" <<YAML
global:
  image:
    registry: nvcr.io
    repository: test/nvcf
YAML
  if test -n "$shared_tag"; then
    cat >>"$environment_file" <<YAML
  openbaoMigrations:
    image:
      tag: "$shared_tag"
YAML
  fi
  cat >>"$environment_file" <<YAML
addons:
  lls:
    enabled: true
YAML
  if test -n "$lls_tag"; then
    cat >>"$environment_file" <<YAML
    hmacRotation:
      image:
        tag: "$lls_tag"
YAML
  fi
}

assert_lls_tag() {
  local values_file="$1" expected="$2"

  yq -e ".sis.lls.hmacRotation.image.tag == \"$expected\"" "$values_file" >/dev/null 2>&1 ||
    fail "expected LLS HMAC rotation image tag $expected"
}

render_openbao_values() {
  local output_file="$1"

  HELMFILE_ENV="$environment_name" \
    HELMFILE_CACHE_HOME="$work_dir/helmfile-cache" \
    helmfile \
      --file "$test_stack_dir/helmfile.d/01-dependencies.yaml.gotmpl" \
      --environment default \
      --state-values-set ingress.gatewayApi.controllerNamespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.shared.name=shared-gw \
      --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc-gw \
      --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy-gateway-system \
      --selector name=openbao-server \
      write-values \
      --output-file-template "$output_file"
}

render_values() {
  local output_file="$1" release="${2:-nvcf-ui}"

  HELMFILE_ENV="$environment_name" \
    HELMFILE_CACHE_HOME="$work_dir/helmfile-cache" \
    helmfile \
      --file "$test_stack_dir/helmfile.d/02-core.yaml.gotmpl" \
      --environment default \
      --state-values-set ingress.gatewayApi.controllerNamespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.shared.name=shared-gw \
      --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc-gw \
      --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy-gateway-system \
      --selector "name=$release" \
      write-values \
      --output-file-template "$output_file"
}

assert_tag() {
  local values_file="$1" expected="$2"

  yq -e ".nvcfUi.openbaoMigrations.image.tag == \"$expected\"" "$values_file" >/dev/null 2>&1 ||
    fail "expected migrations image tag $expected"
}

write_env "0.19.6"
render_values "$work_dir/newer-values.yaml" >/dev/null
assert_tag "$work_dir/newer-values.yaml" "0.19.6"

write_env "0.19.5+build.1"
if render_values "$work_dir/invalid-values.yaml" >"$work_dir/invalid.log" 2>&1; then
  fail "accepted a SemVer build-metadata suffix that is invalid in an OCI image tag"
fi
grep -qF 'must be a valid OCI image tag' "$work_dir/invalid.log" || {
  cat "$work_dir/invalid.log" >&2
  fail "invalid OCI image tag did not produce the expected validation error"
}

# The stack default reaches both OpenBao and the UI hook as one tag.
write_shared_env ""
render_values "$work_dir/default-ui.yaml" >/dev/null
render_openbao_values "$work_dir/default-openbao.yaml" >/dev/null
assert_tag "$work_dir/default-ui.yaml" "0.23.1"
yq -e '.openbao.migrations.image.tag == "0.23.1"' "$work_dir/default-openbao.yaml" >/dev/null 2>&1 ||
  fail "OpenBao migrations tag is not the stack default 0.23.1"

# Changing the shared tag moves both together.
write_shared_env "0.23.4"
render_values "$work_dir/shared-ui.yaml" >/dev/null
render_openbao_values "$work_dir/shared-openbao.yaml" >/dev/null
assert_tag "$work_dir/shared-ui.yaml" "0.23.4"
yq -e '.openbao.migrations.image.tag == "0.23.4"' "$work_dir/shared-openbao.yaml" >/dev/null 2>&1 ||
  fail "OpenBao migrations tag did not follow the shared stack tag"

# The LLS HMAC rotation job gets the shared tag unless it sets its own.
write_lls_env "" ""
render_values "$work_dir/default-lls.yaml" sis >/dev/null
assert_lls_tag "$work_dir/default-lls.yaml" "0.23.1"

write_lls_env "0.23.4" ""
render_values "$work_dir/shared-lls.yaml" sis >/dev/null
assert_lls_tag "$work_dir/shared-lls.yaml" "0.23.4"

write_lls_env "0.23.4" "0.23.2"
render_values "$work_dir/component-lls.yaml" sis >/dev/null
assert_lls_tag "$work_dir/component-lls.yaml" "0.23.2"

echo "nvcf-ui-migrations-image-tag: ok"
