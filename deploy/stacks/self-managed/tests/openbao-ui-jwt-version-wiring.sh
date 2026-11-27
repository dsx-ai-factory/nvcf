#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# Offline stack/chart and image-probe contract tests. Images are simulated;
# qualify-openbao-ui-jwt-version checks selected published images separately.
set -euo pipefail
stack_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
repo_root=$(cd "$stack_dir/../../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fail() { echo "openbao-ui-jwt-version-wiring: $*" >&2; exit 1; }
cp -R "$stack_dir" "$work/stack"
mkdir -p "$work/bin" "$work/image/openbao/plugins" "$work/image/app/migrations"
cp -R "$repo_root/migrations/openbao/migrations/utils" "$work/image/app/migrations/"
cp "$repo_root/infra/openbao/jwt-plugin-versions.env" "$work/image/openbao/"
export TEST_IMAGE_ROOT="$work/image" TEST_DOCKER_CALLS="$work/docker-calls"
export TEST_MIGRATION_COMPATIBLE=true
export NVCF_RELEASE_NGC_API_KEY='' NVCF_RELEASE_HELM_REGISTRY=''
export DOCKER_CONFIG="$work/operator-docker"
mkdir -p "$DOCKER_CONFIG"
printf '{"currentContext":"fixture-remote","credsStore":"fixture","auths":{"mirror.example.com":{}}}\n' > "$DOCKER_CONFIG/config.json"
mkdir -p "$DOCKER_CONFIG/contexts/meta/fixture" "$DOCKER_CONFIG/contexts/tls/fixture"
printf '{"Name":"fixture-remote","Endpoints":{"docker":{"Host":"tcp://fixture.invalid:2376"}}}\n' > "$DOCKER_CONFIG/contexts/meta/fixture/meta.json"
printf 'fixture-tls\n' > "$DOCKER_CONFIG/contexts/tls/fixture/ca.pem"
cp "$DOCKER_CONFIG/config.json" "$work/original-docker.json"
export TEST_OPERATOR_DOCKER_CONFIG=$DOCKER_CONFIG
export HELM_REGISTRY_CONFIG="$work/operator-helm-registry.json"
export TEST_OPERATOR_HELM_REGISTRY_CONFIG=$HELM_REGISTRY_CONFIG
printf '{"auths":{"registry.example.com":{"auth":"fixture-auth"}}}\n' > "$HELM_REGISTRY_CONFIG"
# shellcheck disable=SC1091
source "$work/image/openbao/jwt-plugin-versions.env"
target=$JWT_PLUGIN_TARGET_VERSION old=$JWT_PLUGIN_OLD_VERSION
touch "$work/image/openbao/plugins/vault-plugin-secrets-jwt-$target"
chmod +x "$work/image/openbao/plugins/vault-plugin-secrets-jwt-$target"

# Execute the actual probe bodies against fixture files, including the current
# migration helper. No Docker daemon or OpenBao server is used by these tests.
cat > "$work/bin/docker" <<'BASH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$TEST_DOCKER_CALLS"
if [[ $1 == login ]]; then
  supplied=$(cat)
  [[ ${TEST_NGC_LOGIN_FAIL:-false} != true ]]
  [[ $supplied == fixture-registry-key ]]
  [[ $DOCKER_CONFIG != "$TEST_OPERATOR_DOCKER_CONFIG" ]]
  yq -e '(.auths | has("nvcr.io")) and (.credsStore == null) and (.credHelpers == null)' "$DOCKER_CONFIG/config.json" >/dev/null
  [[ $(yq -r '.currentContext' "$DOCKER_CONFIG/config.json") == fixture-remote ]]
  cmp "$TEST_OPERATOR_DOCKER_CONFIG/contexts/meta/fixture/meta.json" "$DOCKER_CONFIG/contexts/meta/fixture/meta.json"
  cmp "$TEST_OPERATOR_DOCKER_CONFIG/contexts/tls/fixture/ca.pem" "$DOCKER_CONFIG/contexts/tls/fixture/ca.pem"
  yq -i '.auths."nvcr.io".auth = "fixture"' "$DOCKER_CONFIG/config.json"
  echo 'Login Succeeded'
  exit
fi
[[ $1 == run ]]; shift
while (($#)); do
  case "$1" in
    --rm|--read-only) shift ;;
    --pull|--network|--cap-drop|--security-opt) shift 2 ;;
    --env) export "$2"; shift 2 ;;
    --entrypoint) entrypoint=$2; shift 2; break ;;
    *) exit 1 ;;
  esac
done
image=$1; shift
if [[ $image == nvcr.io/* && -n ${NVCF_RELEASE_NGC_DOCKER_CONFIG:-} ]]; then
  [[ $DOCKER_CONFIG == "$NVCF_RELEASE_NGC_DOCKER_CONFIG" ]]
  [[ -f $DOCKER_CONFIG/logged-in ]]
  [[ $(yq -r '.currentContext' "$DOCKER_CONFIG/config.json") == fixture-remote ]]
else
  [[ $DOCKER_CONFIG == "$TEST_OPERATOR_DOCKER_CONFIG" ]]
fi
[[ $1 == -ec ]]; shift
probe=$1
if [[ $entrypoint == /bin/bash && $TEST_MIGRATION_COMPATIBLE == noop ]]; then
  exit 0
fi
if [[ $entrypoint == /bin/bash && $TEST_MIGRATION_COMPATIBLE == joined ]]; then
  joined_helper='enable_secrets_mount() { bao "secrets enable -path=$1 -plugin-version=$BAO_JWT_PLUGIN_VERSION $2"; }'
  probe=${probe/source \/app\/migrations\/utils\/functions.sh/$joined_helper}
elif [[ $entrypoint == /bin/bash && $TEST_MIGRATION_COMPATIBLE != true ]]; then
  # An older helper accepts the input but creates an unversioned mount.
  legacy_helper='enable_secrets_mount() { bao secrets enable "-path=$1" "$2"; }'
  probe=${probe/source \/app\/migrations\/utils\/functions.sh/$legacy_helper}
fi
probe=${probe//\/openbao\//$TEST_IMAGE_ROOT/openbao/}
probe=${probe//\/app\//$TEST_IMAGE_ROOT/app/}
"$entrypoint" -ec "$probe"
BASH
chmod +x "$work/bin/docker"
export PATH="$work/bin:$PATH"
environment=ui-jwt-test
printf '{}\n' > "$work/stack/secrets/$environment-secrets.yaml"
checker="$repo_root/tools/ci/check-openbao-ui-jwt-version"
chart="$repo_root/src/uis/nvcf-ui/helm"

render() {
  local setting=$1
  cat > "$work/stack/environments/$environment.yaml" <<YAML
addons:
  nvcfUi:
    enabled: true
    openbaoMigrations:
$setting
YAML
  HELMFILE_ENV=$environment HELMFILE_CACHE_HOME="$work/helmfile-cache" \
    helmfile -f "$work/stack/helmfile.d/02-core.yaml.gotmpl" \
    --state-values-set ingress.gatewayApi.gateways.shared.name=shared-gw \
    --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy-gateway-system \
    --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc-gw \
    --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy-gateway-system \
    -l name=nvcf-ui write-values --output-file-template "$work/ui-values.yaml" >/dev/null
  helm template nvcf-ui "$chart" -n nvcf-ui -f "$work/ui-values.yaml" > "$work/ui.yaml"
}
check() { "$checker" "$work/server.yaml" "$work/ui.yaml"; }
reject() {
  if check > "$work/result" 2>&1; then fail "accepted $1"; fi
  grep -Fq "$2" "$work/result" || { cat "$work/result" >&2; fail "wrong error for $1"; }
}
# A rendered server fixture selects the image to inspect. Its version manifest
# is intentionally independent of the repository manifest after the drift case.
cat > "$work/server.yaml" <<'YAML'
kind: StatefulSet
spec:
  template:
    spec:
      containers:
        - name: openbao
          image: registry.example.com/selected/server:release
YAML

render "      jwtPluginVersion: $target"
test "$(yq -r '.nvcfUi.openbaoMigrations.jwtPluginVersion' "$work/ui-values.yaml")" = "$target"
check
grep -Fq 'registry.example.com/selected/server:release' "$TEST_DOCKER_CALLS"
ui_image=$(yq -rN 'select(.kind == "Job") | .spec.template.spec.containers[] | select(.name == "openbao-migrations") | .image' "$work/ui.yaml")
grep -Fq "$ui_image" "$TEST_DOCKER_CALLS"

# Fail before login if the isolated context config cannot be prepared.
: > "$TEST_DOCKER_CALLS"
printf '{invalid-json' > "$DOCKER_CONFIG/config.json"
if NVCF_RELEASE_NGC_API_KEY=fixture-registry-key check > "$work/result" 2>&1; then
  fail 'accepted malformed operator Docker config'
fi
if grep -q '^login ' "$TEST_DOCKER_CALLS"; then fail 'logged in after context setup failed'; fi
cp "$work/original-docker.json" "$DOCKER_CONFIG/config.json"

# Exercise qualification orchestration with real Helmfile and Helm. Substitute
# local charts for downloads; keep the actual saved environment and UI chart.
export TEST_HELMFILE_BIN
TEST_HELMFILE_BIN=$(command -v helmfile)
export TEST_UI_CHART="$chart" TEST_SERVER_CHART="$work/server-chart"
export TEST_REAL_HELM TEST_AUTH_HELM="$work/auth-helm" TEST_AUTH_LOG="$work/auth-log"
TEST_REAL_HELM=$(command -v helm)
export TEST_SERVER_CHART_VERSION TEST_UI_CHART_VERSION
TEST_SERVER_CHART_VERSION=$(HELMFILE_ENV=$environment "$TEST_HELMFILE_BIN" -f "$work/stack/helmfile.d/01-dependencies.yaml.gotmpl" -l name=openbao-server list --skip-charts --output json | yq -r '.[0].version')
TEST_UI_CHART_VERSION=$(HELMFILE_ENV=$environment "$TEST_HELMFILE_BIN" -f "$work/stack/helmfile.d/02-core.yaml.gotmpl" -l name=nvcf-ui list --skip-charts --output json | yq -r '.[0].version')
cat > "$TEST_AUTH_HELM" <<'BASH'
#!/usr/bin/env bash
set -euo pipefail
if [[ $1 == registry && $2 == login ]]; then
  [[ ( $3 == nvcr.io || $3 == nvcr.io:443 ) && $(cat) == fixture-registry-key ]]
  TEST_REGISTRY_HOST=$3 yq -e '(.auths | has(strenv(TEST_REGISTRY_HOST))) and (.credsStore == null) and (.credHelpers == null)' "$HELM_REGISTRY_CONFIG" >/dev/null
  TEST_REGISTRY_HOST=$3 yq -i '.auths[strenv(TEST_REGISTRY_HOST)].auth = "fixture-logged-in"' "$HELM_REGISTRY_CONFIG"
  echo oci-authenticated >> "$TEST_AUTH_LOG"
  exit
fi
if [[ ${TEST_OCI_RENDER:-false} == true && $1 == template && -n ${TEST_EXPECT_NGC_CONFIG:-} ]]; then
  [[ $HELM_REGISTRY_CONFIG != "$TEST_OPERATOR_HELM_REGISTRY_CONFIG" ]]
  [[ $(TEST_REGISTRY_HOST=$TEST_EXPECT_NGC_CONFIG yq -r '.auths[strenv(TEST_REGISTRY_HOST)].auth' "$HELM_REGISTRY_CONFIG") == fixture-logged-in ]]
  echo "oci-rendered $TEST_EXPECT_NGC_CONFIG" >> "$TEST_AUTH_LOG"
fi
if [[ $1 == pull ]]; then
  reference=$2
  [[ $reference == oci://* && $reference != oci://oci://* ]]
  host=${reference#oci://}; host=${host%%/*}
  chart_name=${reference##*/}
  shift 2
  destination='' requested_version=''
  while (($#)); do
    case "$1" in
      --destination) destination=$2; shift ;;
      --version) requested_version=$2; shift ;;
      --version=*) requested_version=${1#*=} ;;
    esac
    shift
  done
  [[ -n $destination ]]
  case "$host" in
    nvcr.io|nvcr.io:443)
      [[ $HELM_REGISTRY_CONFIG != "$TEST_OPERATOR_HELM_REGISTRY_CONFIG" ]]
      [[ $(TEST_REGISTRY_HOST=$host yq -r '.auths[strenv(TEST_REGISTRY_HOST)].auth' "$HELM_REGISTRY_CONFIG") == fixture-logged-in ]] ;;
    registry.example.com|'[2001:db8::1]:5000'|123) : ;;
    private.example.com)
      helper=fixture
      if [[ ${TEST_IMPLICIT_STORE:-false} == true ]]; then helper=pass; fi
      printf '%s' "$host" | "docker-credential-$helper" get | yq -e '.Username == "mirror-user" and .Secret == "mirror-password"' >/dev/null ;;
    *) exit 1 ;;
  esac
  case "$chart_name" in
    helm-nvcf-openbao-server) fixture=$TEST_SERVER_CHART; [[ $requested_version == "$TEST_SERVER_CHART_VERSION" ]] ;;
    helm-nvcf-ui) fixture=$TEST_UI_CHART; [[ $requested_version == "$TEST_UI_CHART_VERSION" ]] ;;
    *) exit 1 ;;
  esac
  mkdir -p "$destination"
  cp -R "$fixture" "$destination/$chart_name"
  echo "oci-pulled $reference" >> "$TEST_AUTH_LOG"
  exit
fi
if [[ $1 == repo && $2 == add ]]; then
  if [[ $3 == nvcf ]]; then
    supplied=$(cat)
    [[ $* == *--password-stdin* && $* != *fixture-registry-key* && $* != *mirror-password* ]]
    case "$4" in
      https://helm.ngc.nvidia.com/nvidia/nvcf)
        [[ $supplied == fixture-registry-key && $* == *'$oauthtoken'* ]]
        echo ngc-authenticated >> "$TEST_AUTH_LOG" ;;
      https://mirror.example.com/charts|https://helm.ngc.nvidia.com.example.com/charts)
        [[ $supplied == mirror-password && $* == *mirror-user* ]]
        echo mirror-authenticated >> "$TEST_AUTH_LOG" ;;
      *) exit 1 ;;
    esac
  fi
  exit 0
fi
exec "$TEST_REAL_HELM" "$@"
BASH
chmod +x "$TEST_AUTH_HELM"
cp "$TEST_AUTH_HELM" "$work/bin/helm"
mkdir -p "$TEST_SERVER_CHART/templates"
printf 'apiVersion: v2\nname: test-openbao\nversion: 0.0.0\n' > "$TEST_SERVER_CHART/Chart.yaml"
cp "$repo_root/deploy/helm/openbao/helm/values.yaml" "$TEST_SERVER_CHART/values.yaml"
cat > "$TEST_SERVER_CHART/templates/server.yaml" <<'YAML'
kind: StatefulSet
apiVersion: apps/v1
metadata:
  name: openbao-server
spec:
  template:
    spec:
      containers:
        - name: openbao
          image: "{{ .Values.openbao.server.image.registry }}/{{ .Values.openbao.server.image.repository }}:{{ .Values.openbao.server.image.tag }}"
YAML
cat > "$work/bin/helmfile" <<'BASH'
#!/usr/bin/env bash
set -euo pipefail
if [[ -n ${TEST_REMOTE_VALUES:-} ]]; then
  remote_args=()
  for arg in "$@"; do
    if [[ $arg == "$TEST_REMOTE_VALUES" ]]; then
      remote_args+=("$TEST_REMOTE_VALUES_FIXTURE")
      echo "remote-values-preserved $arg" >> "$TEST_AUTH_LOG"
    else
      remote_args+=("$arg")
    fi
  done
  set -- "${remote_args[@]}"
fi
for arg in "$@"; do
  if [[ $arg == list && ${TEST_CHART_OUTSIDE_ORIGIN:-false} == true ]]; then
    "$TEST_HELMFILE_BIN" "$@" | yq -o=json '.[].chart = "different/chart"'
    exit
  fi
  if [[ $arg == build && -n ${TEST_MUTATE_VALUES_FILE:-} ]]; then
    "$TEST_HELMFILE_BIN" "$@"
    if [[ ${TEST_MUTATE_VERSION:-false} == true ]]; then
      TEST_TARGET_VERSION=$TEST_EXPECTED_TARGET yq -i '.addons.nvcfUi.openbaoMigrations.jwtPluginVersion = strenv(TEST_TARGET_VERSION)' "$TEST_MUTATE_VALUES_FILE"
    else
      yq -i '.global.helm.sources.url = "https://mirror.example.com/charts" |
        .global.helm.sources.username = "mirror-user" |
        .global.helm.sources.password = "mirror-password"' "$TEST_MUTATE_VALUES_FILE"
    fi
    exit
  fi
  if [[ $arg == template ]]; then
    if [[ ${TEST_OCI_RENDER:-false} == true ]]; then
      exec "$TEST_HELMFILE_BIN" "$@" --skip-deps
    fi
    if [[ ${TEST_AUTHENTICATE:-false} == true ]]; then
      auth_args=()
      for auth_arg in "$@"; do
        if [[ $auth_arg == template ]]; then auth_args+=(repos); else auth_args+=("$auth_arg"); fi
      done
      "$TEST_HELMFILE_BIN" --helm-binary "$TEST_AUTH_HELM" "${auth_args[@]}" >&2
    fi
    if [[ $* == *openbao-server.yaml* ]]; then
      exec "$TEST_HELMFILE_BIN" "$@" --chart "$TEST_SERVER_CHART" --skip-deps
    else
      exec "$TEST_HELMFILE_BIN" "$@" --chart "$TEST_UI_CHART" --skip-deps
    fi
  fi
done
exec "$TEST_HELMFILE_BIN" "$@"
BASH
chmod +x "$work/bin/helmfile"
qualify() {
  NVCF_RELEASE_NGC_API_KEY=${TEST_NGC_API_KEY:-} "$repo_root/tools/ci/qualify-openbao-ui-jwt-version" "${TEST_STACK_PATH:-$work/stack}" "$environment" \
    --state-values-set ingress.gatewayApi.gateways.shared.name=shared-gw \
    --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy-gateway-system \
    --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc-gw \
    --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy-gateway-system "$@"
}
: > "$TEST_DOCKER_CALLS"
TEST_OCI_RENDER=true TEST_EXPECT_NGC_CONFIG=nvcr.io TEST_NGC_API_KEY=fixture-registry-key qualify
test "$(grep -c '^oci-rendered nvcr.io$' "$TEST_AUTH_LOG")" = 2
test "$(grep -c '^login nvcr.io ' "$TEST_DOCKER_CALLS")" = 1
# A source contexts directory may itself be a relative symlink.
mv "$DOCKER_CONFIG/contexts" "$work/shared-contexts"
ln -s ../shared-contexts "$DOCKER_CONFIG/contexts"
TEST_NGC_API_KEY=fixture-registry-key qualify
# Resolving a source-stack symlink must never rewrite the operator's files.
cp "$work/stack/helmfile.d/01-dependencies.yaml.gotmpl" "$work/original-dependencies"
cp "$work/stack/helmfile.d/02-core.yaml.gotmpl" "$work/original-core"
ln -s stack "$work/stack-link"
# The function's source can be overridden only by these ownership fixtures.
TEST_STACK_PATH="$work/stack-link" qualify
cmp "$work/original-dependencies" "$work/stack/helmfile.d/01-dependencies.yaml.gotmpl"
cmp "$work/original-core" "$work/stack/helmfile.d/02-core.yaml.gotmpl"
mv "$work/stack/helmfile.d" "$work/shared-helmfile.d"
ln -s ../shared-helmfile.d "$work/stack/helmfile.d"
qualify
cmp "$work/original-dependencies" "$work/shared-helmfile.d/01-dependencies.yaml.gotmpl"
cmp "$work/original-core" "$work/shared-helmfile.d/02-core.yaml.gotmpl"
rm "$work/stack/helmfile.d"
mv "$work/shared-helmfile.d" "$work/stack/helmfile.d"
# Both entry points must work with a relative TMPDIR and an active CDPATH.
mkdir "$work/temp-parent"
(cd "$work"; TMPDIR=temp-parent CDPATH=. TEST_NGC_API_KEY=fixture-registry-key qualify)
(cd "$work"; TMPDIR=temp-parent CDPATH=. NVCF_RELEASE_NGC_API_KEY=fixture-registry-key check)
# Pin distinct fixture image references to detect incorrect release selection.
yq -i '.global.image.registry = "mirror.example.com" |
  .global.image.repository = "selected" |
  .addons.nvcfUi.openbaoMigrations.image.tag = "fixture-migration"' "$work/stack/environments/$environment.yaml"
: > "$TEST_DOCKER_CALLS"
qualify
server_tag=$(yq -r '.openbao.server.image.tag' "$TEST_SERVER_CHART/values.yaml")
grep -Fq "mirror.example.com/selected/nvcf-openbao:$server_tag" "$TEST_DOCKER_CALLS"
grep -Fq 'mirror.example.com/selected/nvcf-openbao-migrations:fixture-migration' "$TEST_DOCKER_CALLS"
# The NGC key must not replace mirror chart credentials or leak at debug level.
yq -i '.global.helm.sources.url = "https://mirror.example.com/charts" |
  .global.helm.sources.username = "mirror-user" |
  .global.helm.sources.password = "mirror-password"' "$work/stack/environments/$environment.yaml"
TEST_AUTHENTICATE=true TEST_NGC_API_KEY=fixture-registry-key HELMFILE_DEBUG=true qualify --debug --log-level debug > "$work/result" 2>&1
if grep -Fq fixture-registry-key "$work/result"; then fail 'NGC key leaked in mirror debug output'; fi
if grep -Fq mirror-password "$work/result"; then fail 'mirror password leaked in debug output'; fi
test "$(grep -c mirror-authenticated "$TEST_AUTH_LOG")" = 2
# A stale NGC key must not prevent a mirror-only qualification.
TEST_AUTHENTICATE=true TEST_NGC_LOGIN_FAIL=true TEST_NGC_API_KEY=expired-key qualify
# An exact NGC origin may use the key without rendering it into Helmfile state.
TEST_AUTHENTICATE=true TEST_NGC_API_KEY=fixture-registry-key qualify --log-level debug \
  --state-values-set global.helm.sources.url=https://helm.ngc.nvidia.com/nvidia/nvcf > "$work/result" 2>&1
if grep -Fq fixture-registry-key "$work/result"; then fail 'NGC key leaked in NGC debug output'; fi
if grep -Fq mirror-password "$work/result"; then fail 'saved password leaked during NGC override'; fi
test "$(grep -c ngc-authenticated "$TEST_AUTH_LOG")" = 2
# External values may change after build. Authentication must use frozen state.
printf 'global:\n  helm:\n    sources:\n      url: https://helm.ngc.nvidia.com/nvidia/nvcf\n' > "$work/switch-values.yaml"
TEST_AUTHENTICATE=true TEST_MUTATE_VALUES_FILE="$work/switch-values.yaml" TEST_NGC_API_KEY=fixture-registry-key \
  qualify --state-values-file "$work/switch-values.yaml"
test "$(grep -c ngc-authenticated "$TEST_AUTH_LOG")" = 4
# A later edit must not mix a previously selected server with new UI values.
printf 'addons:\n  nvcfUi:\n    openbaoMigrations:\n      jwtPluginVersion: %s\n' "$old" > "$work/switch-version.yaml"
if TEST_MUTATE_VALUES_FILE="$work/switch-version.yaml" TEST_MUTATE_VERSION=true TEST_EXPECTED_TARGET=$target \
  qualify --state-values-file="$work/switch-version.yaml" > "$work/result" 2>&1; then
  fail 'accepted UI values changed after server selection'
fi
grep -Fq "expected $target" "$work/result"
# Native resolution preserves relative operands and sibling template includes.
TEST_OLD_VERSION=$old yq -i '.addons.nvcfUi.openbaoMigrations.jwtPluginVersion = strenv(TEST_OLD_VERSION)' "$work/stack/environments/$environment.yaml"
if qualify > "$work/result" 2>&1; then fail 'stale saved version unexpectedly qualified'; fi
grep -Fq "expected $target" "$work/result"
printf 'addons:\n  nvcfUi:\n    openbaoMigrations:\n      jwtPluginVersion: %s\n' "$target" > "$work/stack/environments/relative-values.yaml"
printf '{{ readFile "relative-values.yaml" }}\n' > "$work/stack/environments/relative-values.yaml.gotmpl"
qualify --state-values-file ../environments/relative-values.yaml
qualify --state-values-file=../environments/relative-values.yaml.gotmpl
mkdir -p "$work/external values"
cp "$work/stack/environments/relative-values.yaml" "$work/external values/jwt-values.yaml"
printf '{{ readFile "jwt-values.yaml" }}\n' > "$work/external values/overrides.yaml.gotmpl"
# A sibling include is resolved once even if it changes between release builds.
TEST_MUTATE_VALUES_FILE="$work/external values/jwt-values.yaml" TEST_MUTATE_VERSION=true TEST_EXPECTED_TARGET=$old \
  qualify --state-values-file "$work/external values/overrides.yaml.gotmpl"
test "$(yq -r '.addons.nvcfUi.openbaoMigrations.jwtPluginVersion' "$work/external values/jwt-values.yaml")" = "$old"
# Preserve Helmfile's --state-values-set precedence over file values.
qualify --state-values-file "$work/external values/overrides.yaml.gotmpl" \
  --state-values-set "addons.nvcfUi.openbaoMigrations.jwtPluginVersion=$target"
# Relative operands may also point outside the stack, from helmfile.d.
cp "$work/stack/environments/relative-values.yaml" "$work/external values/jwt-values.yaml"
qualify --state-values-file "../../external values/overrides.yaml.gotmpl"
# Remote references must reach the Helmfile loader without a local prefix.
for reference in https://config.example.com/qualification.yaml git::https://config.example.com/repo@qualification.yaml; do
  TEST_REMOTE_VALUES=$reference TEST_REMOTE_VALUES_FIXTURE="$work/stack/environments/relative-values.yaml" \
    qualify --state-values-file "$reference"
  grep -Fq "remote-values-preserved $reference" "$TEST_AUTH_LOG"
done
TEST_TARGET_VERSION=$target yq -i '.addons.nvcfUi.openbaoMigrations.jwtPluginVersion = strenv(TEST_TARGET_VERSION)' "$work/stack/environments/$environment.yaml"
# Release qualification must match the inventory's OCI chart origin.
TEST_OCI_RENDER=true NVCF_RELEASE_HELM_REGISTRY=registry.example.com/release/charts qualify \
  --state-values-set global.helm.sources.url= \
  --state-values-set global.helm.sources.registry=registry.example.com \
  --state-values-set global.helm.sources.repository=release/charts
if NVCF_RELEASE_HELM_REGISTRY=registry.example.com/release/charts qualify > "$work/result" 2>&1; then
  fail 'accepted chart origin different from release inventory'
fi
grep -Fq 'qualified chart origin differs' "$work/result"
if TEST_CHART_OUTSIDE_ORIGIN=true NVCF_RELEASE_HELM_REGISTRY=registry.example.com/release/charts qualify \
  --state-values-set global.helm.sources.url= \
  --state-values-set global.helm.sources.registry=registry.example.com \
  --state-values-set global.helm.sources.repository=release/charts > "$work/result" 2>&1; then
  fail 'accepted chart outside the release inventory origin'
fi
grep -Fq 'qualified chart origin differs' "$work/result"
# Numeric YAML values remain valid text in OCI source URLs in every state.
TEST_OCI_RENDER=true NVCF_RELEASE_HELM_REGISTRY=123/123 qualify \
  --state-values-set global.helm.sources.url= \
  --state-values-set global.helm.sources.registry=123 \
  --state-values-set global.helm.sources.repository=123
grep -Fq 'oci-pulled oci://123/123/helm-nvcf-ui' "$TEST_AUTH_LOG"
HELMFILE_ENV=$environment "$TEST_HELMFILE_BIN" -f "$work/stack/helmfile.d/03-observability.yaml.gotmpl" \
  --state-values-set global.helm.sources.url= \
  --state-values-set global.helm.sources.registry=123 \
  --state-values-set global.helm.sources.repository=123 \
  --chart "$TEST_SERVER_CHART" --allow-no-matching-release build > "$work/numeric-observability.yaml"
test "$(yq -r '.repositories[] | select(.name == "nvcf") | .url' "$work/numeric-observability.yaml")" = 123/123
# Normalize all trailing slashes and support the NGC TLS port explicitly.
TEST_OCI_RENDER=true TEST_EXPECT_NGC_CONFIG=nvcr.io:443 NVCF_RELEASE_HELM_REGISTRY=' nvcr.io:443/team/charts// ' TEST_NGC_API_KEY=fixture-registry-key qualify \
  --state-values-set global.helm.sources.url= \
  --state-values-set global.helm.sources.registry=nvcr.io:443 \
  --state-values-set global.helm.sources.repository=team/charts
test "$(grep -c '^oci-rendered nvcr.io:443$' "$TEST_AUTH_LOG")" = 2
# Normalize an explicitly prefixed OCI source before native Helmfile pulls.
TEST_OCI_RENDER=true TEST_EXPECT_NGC_CONFIG=nvcr.io:443 NVCF_RELEASE_HELM_REGISTRY=nvcr.io:443/team/charts TEST_NGC_API_KEY=fixture-registry-key qualify \
  --state-values-set global.helm.sources.url= \
  --state-values-set global.helm.sources.registry=oci://nvcr.io:443 \
  --state-values-set global.helm.sources.repository=team/charts
test "$(grep -c '^oci-rendered nvcr.io:443$' "$TEST_AUTH_LOG")" = 4
# A malformed repeated scheme must not qualify while a normal install fails.
if qualify --state-values-set global.helm.sources.url= \
  --state-values-set global.helm.sources.registry=oci://oci://nvcr.io:443 \
  --state-values-set global.helm.sources.repository=team/charts > "$work/result" 2>&1; then
  fail 'accepted repeated OCI scheme'
fi
grep -Fq 'bare host or a single oci:// prefix' "$work/result"
# Bracketed IPv6 is accepted by the publisher, native pull path, and renderer.
TEST_OCI_RENDER=true NVCF_RELEASE_HELM_REGISTRY='[2001:db8::1]:5000/team/charts' qualify \
  --state-values-set global.helm.sources.url= \
  --state-values-set 'global.helm.sources.registry=[2001:db8::1]:5000' \
  --state-values-set global.helm.sources.repository=team/charts
grep -Fq 'oci-pulled oci://[2001:db8::1]:5000/team/charts/helm-nvcf-ui' "$TEST_AUTH_LOG"
# A failed private-origin pull supplies host-specific login guidance.
if TEST_OCI_RENDER=true NVCF_RELEASE_HELM_REGISTRY=private.example.com/team/charts TEST_NGC_API_KEY=fixture-registry-key qualify \
  --state-values-set global.helm.sources.url= \
  --state-values-set global.helm.sources.registry=private.example.com \
  --state-values-set global.helm.sources.repository=team/charts > "$work/result" 2>&1; then
  fail 'rendered private release origin without credentials'
fi
grep -Fq 'chart pull or render failed for private.example.com; run helm registry login' "$work/result"
# Credential helpers are checked for the actual private host, without using an NGC key.
cat > "$work/bin/docker-credential-fixture" <<'BASH'
#!/usr/bin/env bash
set -euo pipefail
[[ $1 == get && $(cat) == private.example.com ]]
printf '{"Username":"mirror-user","Secret":"mirror-password"}\n'
BASH
chmod +x "$work/bin/docker-credential-fixture"
TEST_OCI_RENDER=true NVCF_RELEASE_HELM_REGISTRY=private.example.com/team/charts TEST_NGC_API_KEY=fixture-registry-key qualify \
  --state-values-set global.helm.sources.url= \
  --state-values-set global.helm.sources.registry=private.example.com \
  --state-values-set global.helm.sources.repository=team/charts
# Helm can discover a native helper even when neither config names it.
cp "$work/bin/docker-credential-fixture" "$work/bin/docker-credential-pass"
yq -i 'del(.credsStore)' "$DOCKER_CONFIG/config.json"
TEST_OCI_RENDER=true TEST_IMPLICIT_STORE=true NVCF_RELEASE_HELM_REGISTRY=private.example.com/team/charts qualify \
  --state-values-set global.helm.sources.url= \
  --state-values-set global.helm.sources.registry=private.example.com \
  --state-values-set global.helm.sources.repository=team/charts
cp "$work/original-docker.json" "$DOCKER_CONFIG/config.json"
# A lookalike hostname is still a mirror.
TEST_AUTHENTICATE=true TEST_NGC_API_KEY=fixture-registry-key qualify \
  --state-values-set global.helm.sources.url=https://helm.ngc.nvidia.com.example.com/charts
cmp "$work/original-docker.json" "$DOCKER_CONFIG/config.json"
if grep -Fq fixture-registry-key "$TEST_DOCKER_CALLS"; then fail 'registry credential appeared in command arguments'; fi
render "      jwtPluginVersion: $old"
if qualify > "$work/result" 2>&1; then fail 'qualification accepted stale saved environment values'; fi
grep -Fq "expected $target" "$work/result"
rm "$work/bin/helmfile"

render '      image: {}'
reject 'missing version' 'set addons.nvcfUi.openbaoMigrations.jwtPluginVersion'
render '      jwtPluginVersion: ""'
reject 'empty version' 'missing or empty'
render "      jwtPluginVersion: $old"
reject 'old version' "expected $target"
render '      jwtPluginVersion: v99.0.0'
reject 'mismatched version' "expected $target"

render "      jwtPluginVersion: $target"
OPENBAO_UI_JWT_TEST_VERSION=$target yq -i '(select(.kind == "Job") | .spec.template.spec.containers[] | select(.name == "openbao-migrations") | .env[] | select(.name == "BAO_JWT_PLUGIN_VERSION") | .value) = strenv(OPENBAO_UI_JWT_TEST_VERSION) + "\n"' "$work/ui.yaml"
reject 'version with trailing newline' "expected $target"
render "      jwtPluginVersion: $target"
export TEST_MIGRATION_COMPATIBLE=joined
reject 'migration helper joining arguments' 'does not support explicit JWT plugin version selection'
export TEST_MIGRATION_COMPATIBLE=false
reject 'migration image that ignores the version' 'does not support explicit JWT plugin version selection'
export TEST_MIGRATION_COMPATIBLE=true

export TEST_MIGRATION_COMPATIBLE=noop
reject 'migration helper that never enables a mount' 'did not create the mount'
export TEST_MIGRATION_COMPATIBLE=true

yq -i 'del(.spec.template.spec.containers[].env[] | select(.name == "BAO_JWT_PLUGIN_VERSION"))' "$work/ui.yaml"
reject 'a chart that drops the configured version' 'set addons.nvcfUi.openbaoMigrations.jwtPluginVersion'
render "      jwtPluginVersion: $target"
yq -i '(select(.kind == "Job") | .spec.template.spec.containers[] | select(.name == "openbao-migrations") | .env) += [{"name": "BAO_JWT_PLUGIN_VERSION", "value": ""}]' "$work/ui.yaml"
reject 'duplicate version entries' 'exactly one BAO_JWT_PLUGIN_VERSION'
render "      jwtPluginVersion: $target"

# Detect drift in the selected image even when the checkout manifest is unchanged.
drift=v99.1.2
printf 'JWT_PLUGIN_TARGET_VERSION=%s\n' "$drift" > "$work/image/openbao/jwt-plugin-versions.env"
cp "$work/image/openbao/plugins/vault-plugin-secrets-jwt-$target" "$work/image/openbao/plugins/vault-plugin-secrets-jwt-$drift"
reject 'selected image manifest drift' "expected $drift"
render "      jwtPluginVersion: $drift"
check

# A target-only image with no readable manifest must not fall back to legacy.
mv "$work/image/openbao/jwt-plugin-versions.env" "$work/manifest.env"
reject 'missing image manifest' 'cannot inspect JWT plugin layout'
mv "$work/manifest.env" "$work/image/openbao/jwt-plugin-versions.env"

# Published legacy and bridge images retain the unversioned executable.
cp "$work/image/openbao/plugins/vault-plugin-secrets-jwt-$target" "$work/image/openbao/plugins/vault-plugin-secrets-jwt"
render '      image: {}'
check
mv "$work/image/openbao/jwt-plugin-versions.env" "$work/manifest.env"
check

# A disabled UI requires neither registry access nor images.
printf 'unused: ref+file:///this-file-must-not-exist\naddons:\n  nvcfUi:\n    enabled: false\n' > "$work/stack/environments/$environment.yaml"
: > "$TEST_DOCKER_CALLS"
NVCF_RELEASE_NGC_API_KEY='' "$repo_root/tools/ci/qualify-openbao-ui-jwt-version" "${TEST_STACK_PATH:-$work/stack}" "$environment"
test ! -s "$TEST_DOCKER_CALLS"

echo 'openbao-ui-jwt-version-wiring: all checks passed (simulated images)'
