# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
@openbao-jwt-plugin-lifecycle
Feature: OpenBao JWT plugin versions preserve signing through upgrade and rollback
  # The operator supplies a new container built from infra/openbao/tests/Dockerfile.
  # No host ports, Kubernetes resources, or existing OpenBao storage are used.
  # runtime-server only starts and unseals a file-backed test server. All catalog
  # writes, mount transitions, and validation remain explicit below.

  Scenario: Recover migrations when the versioned server has no JWT mounts
    Given environment variable "OPENBAO_JWT_RECOVERY_CONTAINER" is set
    When I successfully run command "docker exec ${OPENBAO_JWT_RECOVERY_CONTAINER} bash /test/runtime-server.sh init target-only"
    And I successfully run command:
      """
      docker exec ${OPENBAO_JWT_RECOVERY_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        bao() {
          if [[ $1 == plugin && $2 == list && ! -e /tmp/nvcf-jwt-runtime/catalog-not-ready-once ]]; then
            touch /tmp/nvcf-jwt-runtime/catalog-not-ready-once
            echo "Injected transient catalog failure" >&2
            return 2
          fi
          /usr/bin/bao "$@"
        }
        export -f bao
        JWT_PLUGIN_READY_ATTEMPTS=3 JWT_PLUGIN_READY_DELAY=0 jwt-plugin-lifecycle wait-ready
        test -f /tmp/nvcf-jwt-runtime/catalog-not-ready-once
        unset -f bao
        test "$(jwt-plugin-lifecycle status | jq length)" = 0
        jwt-plugin-lifecycle deployment-preflight
      '
      """
    When I run command "docker exec ${OPENBAO_JWT_RECOVERY_CONTAINER} bash /test/runtime-bao.sh jwt-plugin-lifecycle verify"
    Then the command exit code should be 1
    When I run command "docker exec ${OPENBAO_JWT_RECOVERY_CONTAINER} bash /test/runtime-bao.sh jwt-plugin-lifecycle check-cleanup"
    Then the command exit code should be 1
    When I successfully run command:
      """
      docker exec ${OPENBAO_JWT_RECOVERY_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        . /openbao/jwt-plugin-versions.env
        bao secrets enable -path=jwt -plugin-version="$JWT_PLUGIN_TARGET_VERSION" vault-plugin-secrets-jwt
        bao write jwt/config key_ttl=1h jwt_ttl=30s
        bao write jwt/roles/test issuer=recovery.example.com
        token=$(bao write -force -field=token jwt/sign/test)
        /test/jwtverify "$token" "$BAO_ADDR/v1/jwt/jwks" | jq -e ".iss == \"recovery.example.com\""
        jwt-plugin-lifecycle deployment-preflight
        jwt-plugin-lifecycle verify
      '
      """

  Scenario: Migrate unversioned mounts, roll back, and retire legacy entries later
    Given environment variable "OPENBAO_JWT_TEST_CONTAINER" is set
    When I successfully run command "docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-server.sh init legacy"
    And I successfully run command:
      """
      docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        sha=$(sha256sum "$JWT_PLUGIN_DIR/vault-plugin-secrets-jwt" | awk "{print \$1}")
        bao plugin register -sha256="$sha" -command=vault-plugin-secrets-jwt secret vault-plugin-secrets-jwt
        bao secrets enable -path=jwt vault-plugin-secrets-jwt
        bao write jwt/config key_ttl=1h jwt_ttl=30s
        bao write jwt/roles/test issuer=lifecycle.example.com
        token=$(bao write -force -field=token jwt/sign/test)
        /test/jwtverify "$token" "$BAO_ADDR/v1/jwt/jwks" | jq -e ".iss == \"lifecycle.example.com\""
        curl -sf "$BAO_ADDR/v1/jwt/jwks" | jq -S . > /tmp/nvcf-jwt-runtime/original-jwks.json
        jwt-plugin-lifecycle bridge-preflight
      '
      """
    # An authentication failure cannot establish that a bridge migration is needed.
    When I run command:
      """
      docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        JWT_PLUGIN_DIR=/tmp/nvcf-jwt-runtime/target-only/plugins
        JWT_PLUGIN_CONFIG_DIR=/tmp/nvcf-jwt-runtime/target-only/plugin-config
        export JWT_PLUGIN_DIR JWT_PLUGIN_CONFIG_DIR
        BAO_TOKEN=invalid-preflight-test-token jwt-plugin-lifecycle deployment-preflight
      '
      """
    Then the command exit code should be 2
    And the command output should contain "Error listing available plugins:"
    And the command output should not contain "Required upgrade stop:"
    # A candidate target-only package must reject stored legacy state before rollout.
    When I run command:
      """
      docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        JWT_PLUGIN_DIR=/tmp/nvcf-jwt-runtime/target-only/plugins
        JWT_PLUGIN_CONFIG_DIR=/tmp/nvcf-jwt-runtime/target-only/plugin-config
        export JWT_PLUGIN_DIR JWT_PLUGIN_CONFIG_DIR
        jwt-plugin-lifecycle deployment-preflight
      '
      """
    Then the command exit code should be 1
    And the command output should contain "Required upgrade stop:"
    And I successfully run command "docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-server.sh restart bridge"
    And I successfully run command:
      """
      docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        jwt-plugin-lifecycle wait-ready
        jwt-plugin-lifecycle preflight
        token=$(bao write -force -field=token jwt/sign/test)
        /test/jwtverify "$token" "$BAO_ADDR/v1/jwt/jwks" | jq -e ".iss == \"lifecycle.example.com\""
        curl -sf "$BAO_ADDR/v1/jwt/jwks" | jq -S . | cmp - /tmp/nvcf-jwt-runtime/original-jwks.json
        jwt-plugin-lifecycle pin-old
        jwt-plugin-lifecycle pin-old
      '
      """
    # Tune alone must not claim that the running backend changed.
    When I run command "docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh jwt-plugin-lifecycle upgrade"
    Then the command exit code should be 1
    When I run command "docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh jwt-plugin-lifecycle check-cleanup"
    Then the command exit code should be 1
    When I successfully run command:
      """
      docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        . /openbao/jwt-plugin-versions.env
        bao plugin reload -plugin vault-plugin-secrets-jwt
        jwt-plugin-lifecycle status | jq -e --arg old "$JWT_PLUGIN_OLD_VERSION" ". | length > 0 and all(.[]; .value.plugin_version == \$old and .value.running_plugin_version == \$old)"
        bao secrets enable -path=new-jwt -plugin-version="$JWT_PLUGIN_TARGET_VERSION" vault-plugin-secrets-jwt
        jwt-plugin-lifecycle upgrade
      '
      """
    When I run command "docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh jwt-plugin-lifecycle check-cleanup"
    Then the command exit code should be 1
    When I successfully run command:
      """
      docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        bao plugin reload -plugin vault-plugin-secrets-jwt
        jwt-plugin-lifecycle verify
        jwt-plugin-lifecycle rollback
        bao plugin reload -plugin vault-plugin-secrets-jwt
        . /openbao/jwt-plugin-versions.env
        jwt-plugin-lifecycle status | jq -e --arg old "$JWT_PLUGIN_OLD_VERSION" ". | length > 0 and all(.[]; .value.plugin_version == \$old and .value.running_plugin_version == \$old)"
        token=$(bao write -force -field=token jwt/sign/test)
        /test/jwtverify "$token" "$BAO_ADDR/v1/jwt/jwks" | jq -e ".iss == \"lifecycle.example.com\""
        curl -sf "$BAO_ADDR/v1/jwt/jwks" | jq -S . | cmp - /tmp/nvcf-jwt-runtime/original-jwks.json
      '
      """
    When I run command "docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh jwt-plugin-lifecycle cleanup"
    Then the command exit code should be 1
    When I successfully run command:
      """
      docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        jwt-plugin-lifecycle upgrade
        bao plugin reload -plugin vault-plugin-secrets-jwt
        jwt-plugin-lifecycle check-cleanup
      '
      """
    # Retaining legacy paths must block catalog retirement even on target mounts.
    When I run command "docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh jwt-plugin-lifecycle cleanup"
    Then the command exit code should be 1
    When I successfully run command "docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-server.sh restart target-only"
    And I successfully run command:
      """
      docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        jwt-plugin-lifecycle verify
        jwt-plugin-lifecycle cleanup
        jwt-plugin-lifecycle cleanup
        token=$(bao write -force -field=token jwt/sign/test)
        /test/jwtverify "$token" "$BAO_ADDR/v1/jwt/jwks" | jq -e ".iss == \"lifecycle.example.com\""
        curl -sf "$BAO_ADDR/v1/jwt/jwks" | jq -S . | cmp - /tmp/nvcf-jwt-runtime/original-jwks.json
        . /openbao/jwt-plugin-versions.env
        bao secrets enable -path=after-cleanup -plugin-version="$JWT_PLUGIN_TARGET_VERSION" vault-plugin-secrets-jwt
        jwt-plugin-lifecycle verify
      '
      """
    When I run command:
      """
      docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        . /openbao/jwt-plugin-versions.env
        bao plugin info -version="$JWT_PLUGIN_OLD_VERSION" secret vault-plugin-secrets-jwt
      '
      """
    Then the command exit code should be 2
    When I run command "docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh jwt-plugin-lifecycle rollback"
    Then the command exit code should be 1
    # Post-removal recovery restores both packaged versions before changing selection.
    When I successfully run command:
      """
      docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        JWT_PLUGIN_DIR=/tmp/nvcf-jwt-runtime/bridge/plugins \
          JWT_PLUGIN_CONFIG_DIR=/tmp/nvcf-jwt-runtime/bridge/plugin-config \
          jwt-plugin-lifecycle deployment-preflight
      '
      """
    When I successfully run command "docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-server.sh restart bridge"
    And I successfully run command:
      """
      docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        jwt-plugin-lifecycle verify
        jwt-plugin-lifecycle rollback
        bao plugin reload -plugin vault-plugin-secrets-jwt
        . /openbao/jwt-plugin-versions.env
        jwt-plugin-lifecycle status | jq -e --arg old "$JWT_PLUGIN_OLD_VERSION" ". | length > 0 and all(.[]; .value.plugin_version == \$old and .value.running_plugin_version == \$old)"
        token=$(bao write -force -field=token jwt/sign/test)
        /test/jwtverify "$token" "$BAO_ADDR/v1/jwt/jwks" | jq -e ".iss == \"lifecycle.example.com\""
        curl -sf "$BAO_ADDR/v1/jwt/jwks" | jq -S . | cmp - /tmp/nvcf-jwt-runtime/original-jwks.json
        jwt-plugin-lifecycle upgrade
        bao plugin reload -plugin vault-plugin-secrets-jwt
        jwt-plugin-lifecycle verify
      '
      """
    And I successfully run command "docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-server.sh restart target-only"
    And I successfully run command:
      """
      docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bash -ec '
        jwt-plugin-lifecycle verify
        jwt-plugin-lifecycle cleanup
      '
      """
    When I run command "docker exec ${OPENBAO_JWT_TEST_CONTAINER} bash /test/runtime-bao.sh bao plugin info secret vault-plugin-secrets-jwt"
    Then the command exit code should be 2
