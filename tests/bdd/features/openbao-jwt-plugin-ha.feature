# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
@openbao-jwt-plugin-ha
Feature: JWT plugin transitions preserve data across normal Raft failover
  # Use a new disposable container. Each voter completes the checked rotation.
  # Probe the actual active before and after normal failover, without selecting
  # a target voter or requiring every voter to become active.

  Scenario: Migrate and roll back JWT mounts with normal HA failover
    Given environment variable "OPENBAO_RAFT_TEST_CONTAINER" is set
    When I successfully run command "docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash /test/runtime-raft.sh init legacy"
    And I successfully run command:
      """
      docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash -euo pipefail -c '
        leader=$(/test/runtime-raft.sh active)
        /test/runtime-raft-bao.sh "$leader" bash -ec '\''
          sha=$(sha256sum "$JWT_PLUGIN_DIR/vault-plugin-secrets-jwt" | awk "{print \$1}")
          bao plugin register -sha256="$sha" -command=vault-plugin-secrets-jwt secret vault-plugin-secrets-jwt
          bao secrets enable -path=jwt vault-plugin-secrets-jwt
          bao write jwt/config key_ttl=1h jwt_ttl=30s
          bao write jwt/roles/test issuer=ha-lifecycle.example.com
          curl -sf "$BAO_ADDR/v1/jwt/jwks" | jq -S . > /tmp/nvcf-raft-runtime/original-jwks.json
          bao operator raft list-peers -format=json | jq -S ".data.config.servers | map({node_id,address,voter}) | sort_by(.node_id)" > /tmp/nvcf-raft-runtime/original-peers.json
        '\''
      '
      """
    And I successfully run command "docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash /test/runtime-raft.sh restart bridge"
    When I successfully run command:
      """
      docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash -euo pipefail -c '
        leader=$(/test/runtime-raft.sh active)
        /test/runtime-raft-bao.sh "$leader" jwt-plugin-lifecycle pin-old
      '
      """
    And I successfully run command "docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash /test/runtime-raft.sh restart bridge"
    And I successfully run command:
      """
      docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash -euo pipefail -c '
        for observation in 0 1; do
          leader=$(/test/runtime-raft.sh active)
          /test/runtime-raft-bao.sh "$leader" bash -ec '\''
            . /openbao/jwt-plugin-versions.env
            expected=$JWT_PLUGIN_OLD_VERSION
            sha=$(bao plugin info -format=json -version="$expected" secret vault-plugin-secrets-jwt | jq -r .sha256)
            bao read -format=json sys/leader | jq -e ".data.is_self == true"
            jwt-plugin-lifecycle status | jq -e --arg version "$expected" --arg sha "$sha" "length > 0 and all(.[]; .value.plugin_version == \$version and .value.running_plugin_version == \$version and .value.running_sha256 == \$sha)"
            token=$(bao write -force -field=token jwt/sign/test)
            /test/jwtverify "$token" "$BAO_ADDR/v1/jwt/jwks" | jq -e ".iss == \"ha-lifecycle.example.com\""
            curl -sf "$BAO_ADDR/v1/jwt/jwks" | jq -S . | cmp - /tmp/nvcf-raft-runtime/original-jwks.json
            bao operator raft list-peers -format=json | jq -S ".data.config.servers | map({node_id,address,voter}) | sort_by(.node_id)" | cmp - /tmp/nvcf-raft-runtime/original-peers.json
            bao read -format=json sys/storage/raft/autopilot/state | jq -e ".data | .healthy and (.voters | length == 3)"
          '\''
          if [[ $observation == 0 ]]; then
            /test/runtime-raft-bao.sh "$leader" bao operator step-down
            moved=false
            for attempt in $(seq 1 60); do
              if next=$(/test/runtime-raft.sh active) && [[ $next != "$leader" ]]; then moved=true; break; fi
              sleep 1
            done
            test "$moved" = true
            recovered=false
            for attempt in $(seq 1 90); do
              if /test/runtime-raft-bao.sh "$next" bao read -format=json sys/storage/raft/autopilot/state | jq -e ".data | .healthy and (.voters | length == 3)" >/dev/null; then
                recovered=true
                break
              fi
              sleep 1
            done
            test "$recovered" = true
          fi
        done
      '
      """
    When I successfully run command:
      """
      docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash -euo pipefail -c '
        leader=$(/test/runtime-raft.sh active)
        /test/runtime-raft-bao.sh "$leader" jwt-plugin-lifecycle upgrade
      '
      """
    And I successfully run command "docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash /test/runtime-raft.sh restart bridge"
    And I successfully run command:
      """
      docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash -euo pipefail -c '
        for observation in 0 1; do
          leader=$(/test/runtime-raft.sh active)
          /test/runtime-raft-bao.sh "$leader" bash -ec '\''
            . /openbao/jwt-plugin-versions.env
            expected=$JWT_PLUGIN_TARGET_VERSION
            sha=$(bao plugin info -format=json -version="$expected" secret vault-plugin-secrets-jwt | jq -r .sha256)
            bao read -format=json sys/leader | jq -e ".data.is_self == true"
            jwt-plugin-lifecycle status | jq -e --arg version "$expected" --arg sha "$sha" "length > 0 and all(.[]; .value.plugin_version == \$version and .value.running_plugin_version == \$version and .value.running_sha256 == \$sha)"
            token=$(bao write -force -field=token jwt/sign/test)
            /test/jwtverify "$token" "$BAO_ADDR/v1/jwt/jwks" | jq -e ".iss == \"ha-lifecycle.example.com\""
            curl -sf "$BAO_ADDR/v1/jwt/jwks" | jq -S . | cmp - /tmp/nvcf-raft-runtime/original-jwks.json
            bao operator raft list-peers -format=json | jq -S ".data.config.servers | map({node_id,address,voter}) | sort_by(.node_id)" | cmp - /tmp/nvcf-raft-runtime/original-peers.json
            bao read -format=json sys/storage/raft/autopilot/state | jq -e ".data | .healthy and (.voters | length == 3)"
          '\''
          if [[ $observation == 0 ]]; then
            /test/runtime-raft-bao.sh "$leader" bao operator step-down
            moved=false
            for attempt in $(seq 1 60); do
              if next=$(/test/runtime-raft.sh active) && [[ $next != "$leader" ]]; then moved=true; break; fi
              sleep 1
            done
            test "$moved" = true
            recovered=false
            for attempt in $(seq 1 90); do
              if /test/runtime-raft-bao.sh "$next" bao read -format=json sys/storage/raft/autopilot/state | jq -e ".data | .healthy and (.voters | length == 3)" >/dev/null; then
                recovered=true
                break
              fi
              sleep 1
            done
            test "$recovered" = true
          fi
        done
      '
      """
    When I successfully run command:
      """
      docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash -euo pipefail -c '
        leader=$(/test/runtime-raft.sh active)
        /test/runtime-raft-bao.sh "$leader" jwt-plugin-lifecycle rollback
      '
      """
    And I successfully run command "docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash /test/runtime-raft.sh restart bridge"
    And I successfully run command:
      """
      docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash -euo pipefail -c '
        for observation in 0 1; do
          leader=$(/test/runtime-raft.sh active)
          /test/runtime-raft-bao.sh "$leader" bash -ec '\''
            . /openbao/jwt-plugin-versions.env
            expected=$JWT_PLUGIN_OLD_VERSION
            sha=$(bao plugin info -format=json -version="$expected" secret vault-plugin-secrets-jwt | jq -r .sha256)
            bao read -format=json sys/leader | jq -e ".data.is_self == true"
            jwt-plugin-lifecycle status | jq -e --arg version "$expected" --arg sha "$sha" "length > 0 and all(.[]; .value.plugin_version == \$version and .value.running_plugin_version == \$version and .value.running_sha256 == \$sha)"
            token=$(bao write -force -field=token jwt/sign/test)
            /test/jwtverify "$token" "$BAO_ADDR/v1/jwt/jwks" | jq -e ".iss == \"ha-lifecycle.example.com\""
            curl -sf "$BAO_ADDR/v1/jwt/jwks" | jq -S . | cmp - /tmp/nvcf-raft-runtime/original-jwks.json
            bao operator raft list-peers -format=json | jq -S ".data.config.servers | map({node_id,address,voter}) | sort_by(.node_id)" | cmp - /tmp/nvcf-raft-runtime/original-peers.json
            bao read -format=json sys/storage/raft/autopilot/state | jq -e ".data | .healthy and (.voters | length == 3)"
          '\''
          if [[ $observation == 0 ]]; then
            /test/runtime-raft-bao.sh "$leader" bao operator step-down
            moved=false
            for attempt in $(seq 1 60); do
              if next=$(/test/runtime-raft.sh active) && [[ $next != "$leader" ]]; then moved=true; break; fi
              sleep 1
            done
            test "$moved" = true
            recovered=false
            for attempt in $(seq 1 90); do
              if /test/runtime-raft-bao.sh "$next" bao read -format=json sys/storage/raft/autopilot/state | jq -e ".data | .healthy and (.voters | length == 3)" >/dev/null; then
                recovered=true
                break
              fi
              sleep 1
            done
            test "$recovered" = true
          fi
        done
      '
      """
    When I successfully run command:
      """
      docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash -euo pipefail -c '
        leader=$(/test/runtime-raft.sh active)
        /test/runtime-raft-bao.sh "$leader" jwt-plugin-lifecycle upgrade
      '
      """
    And I successfully run command "docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash /test/runtime-raft.sh restart bridge"
    And I successfully run command:
      """
      docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash -euo pipefail -c '
        for observation in 0 1; do
          leader=$(/test/runtime-raft.sh active)
          /test/runtime-raft-bao.sh "$leader" bash -ec '\''
            . /openbao/jwt-plugin-versions.env
            expected=$JWT_PLUGIN_TARGET_VERSION
            sha=$(bao plugin info -format=json -version="$expected" secret vault-plugin-secrets-jwt | jq -r .sha256)
            bao read -format=json sys/leader | jq -e ".data.is_self == true"
            jwt-plugin-lifecycle status | jq -e --arg version "$expected" --arg sha "$sha" "length > 0 and all(.[]; .value.plugin_version == \$version and .value.running_plugin_version == \$version and .value.running_sha256 == \$sha)"
            token=$(bao write -force -field=token jwt/sign/test)
            /test/jwtverify "$token" "$BAO_ADDR/v1/jwt/jwks" | jq -e ".iss == \"ha-lifecycle.example.com\""
            curl -sf "$BAO_ADDR/v1/jwt/jwks" | jq -S . | cmp - /tmp/nvcf-raft-runtime/original-jwks.json
            bao operator raft list-peers -format=json | jq -S ".data.config.servers | map({node_id,address,voter}) | sort_by(.node_id)" | cmp - /tmp/nvcf-raft-runtime/original-peers.json
            bao read -format=json sys/storage/raft/autopilot/state | jq -e ".data | .healthy and (.voters | length == 3)"
          '\''
          if [[ $observation == 0 ]]; then
            /test/runtime-raft-bao.sh "$leader" bao operator step-down
            moved=false
            for attempt in $(seq 1 60); do
              if next=$(/test/runtime-raft.sh active) && [[ $next != "$leader" ]]; then moved=true; break; fi
              sleep 1
            done
            test "$moved" = true
            recovered=false
            for attempt in $(seq 1 90); do
              if /test/runtime-raft-bao.sh "$next" bao read -format=json sys/storage/raft/autopilot/state | jq -e ".data | .healthy and (.voters | length == 3)" >/dev/null; then
                recovered=true
                break
              fi
              sleep 1
            done
            test "$recovered" = true
          fi
        done
      '
      """
    And I successfully run command "docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash /test/runtime-raft.sh restart target-only"
    And I successfully run command:
      """
      docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash -euo pipefail -c '
        for observation in 0 1; do
          leader=$(/test/runtime-raft.sh active)
          /test/runtime-raft-bao.sh "$leader" bash -ec '\''
            . /openbao/jwt-plugin-versions.env
            expected=$JWT_PLUGIN_TARGET_VERSION
            sha=$(bao plugin info -format=json -version="$expected" secret vault-plugin-secrets-jwt | jq -r .sha256)
            bao read -format=json sys/leader | jq -e ".data.is_self == true"
            jwt-plugin-lifecycle status | jq -e --arg version "$expected" --arg sha "$sha" "length > 0 and all(.[]; .value.plugin_version == \$version and .value.running_plugin_version == \$version and .value.running_sha256 == \$sha)"
            token=$(bao write -force -field=token jwt/sign/test)
            /test/jwtverify "$token" "$BAO_ADDR/v1/jwt/jwks" | jq -e ".iss == \"ha-lifecycle.example.com\""
            curl -sf "$BAO_ADDR/v1/jwt/jwks" | jq -S . | cmp - /tmp/nvcf-raft-runtime/original-jwks.json
            bao operator raft list-peers -format=json | jq -S ".data.config.servers | map({node_id,address,voter}) | sort_by(.node_id)" | cmp - /tmp/nvcf-raft-runtime/original-peers.json
            bao read -format=json sys/storage/raft/autopilot/state | jq -e ".data | .healthy and (.voters | length == 3)"
          '\''
          if [[ $observation == 0 ]]; then
            /test/runtime-raft-bao.sh "$leader" bao operator step-down
            moved=false
            for attempt in $(seq 1 60); do
              if next=$(/test/runtime-raft.sh active) && [[ $next != "$leader" ]]; then moved=true; break; fi
              sleep 1
            done
            test "$moved" = true
            recovered=false
            for attempt in $(seq 1 90); do
              if /test/runtime-raft-bao.sh "$next" bao read -format=json sys/storage/raft/autopilot/state | jq -e ".data | .healthy and (.voters | length == 3)" >/dev/null; then
                recovered=true
                break
              fi
              sleep 1
            done
            test "$recovered" = true
          fi
        done
      '
      """
    When I run command "docker exec ${OPENBAO_RAFT_TEST_CONTAINER} bash /test/runtime-raft-bao.sh 0 jwt-plugin-lifecycle cleanup"
    Then the command exit code should be 1
