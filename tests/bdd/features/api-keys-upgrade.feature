@upgrade @stack-change
Feature: An API key created before the API Keys upgrade keeps working
  As a self-managed NVCF operator,
  I want a key created with a legacy service ID before the upgrade to work after the upgrade,
  and a key created after it to carry the account ID,
  so that upgrading API Keys does not break existing keys.

  # Prerequisite: the installation must run the release before multi-tenant API
  # Keys Milestone 0 (M0). The first scenario fails if the installation is already
  # upgraded. The test upgrades it to M0 by running UPGRADE_APPLY_COMMAND, so a
  # run is single-use: install the release before M0 again before the next run.
  #
  # This feature changes the installation. The two scenarios must run in order:
  # the second one needs the upgraded installation.
  #
  # UPGRADE_APPLY_COMMAND is a full command line that upgrades the whole stack, so
  # the Cassandra and OpenBao migrations run before API Keys starts. The Go test
  # defaults it to "make -C deploy/stacks/self-managed install HELMFILE_ENV=<env>
  # KUBECONFIG_FILE=<kubeconfig>" from UPGRADE_ENV and UPGRADE_KUBECONFIG. That
  # only upgrades if the checkout pins the M0 versions, so the feature fails fast
  # when the API Keys image does not change. On a test cluster, lower the
  # account-index migration initial delay in the command
  # (APIKEYS_ACCOUNT_INDEX_MIGRATION_INITIAL_DELAY), because the default is 30
  # minutes and the wait below is 45 minutes.
  #
  # UPGRADE_NCA_ID is the account ID the upgraded API Keys is configured with
  # (NVCF_NCA_ID, default nvcf-default).

  Scenario: A key created with the legacy function service ID before the upgrade works after it
    Given these environment variables are set:
      | name                  |
      | NVCF_CLI              |
      | UPGRADE_CLI_CONFIG    |
      | UPGRADE_KUBECONFIG    |
      | UPGRADE_CONTEXT       |
      | UPGRADE_NCA_ID        |
      | UPGRADE_APPLY_COMMAND |
    And I use NVCF CLI config "${UPGRADE_CLI_CONFIG}"

    # The default upgrade command uses the kubeconfig's current context, so it must be
    # the cluster this feature checks.
    When I successfully run command:
      """
      /bin/bash -eu -c 'test "$(kubectl --kubeconfig "$1" config current-context)" = "$2"' api-keys-upgrade-context "${UPGRADE_KUBECONFIG}" "${UPGRADE_CONTEXT}"
      """

    # Only an upgraded installation has the completion record, so its absence shows
    # the installation still runs the previous release.
    When I run command:
      """
      /bin/bash ${REPO_ROOT}/tests/bdd/scripts/api-keys-cql.sh "${UPGRADE_KUBECONFIG}" "${UPGRADE_CONTEXT}" "SELECT record_key FROM api_keys_api.row_update_lock WHERE table_name = 'keys_by_account_index_copy' AND record_key = 'completed';"
      """
    Then the command exit code should be 0
    And the command output should contain "(0 rows)"

    # Before the upgrade: create a key with the legacy function service ID and use it.
    When I successfully run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" init
      """
    And I successfully generate a function API key with CLI options:
      | option        | value               |
      | --description | bdd-m0-legacy-key   |
      | --scopes      | list_cluster_groups |
    Given the HTTP request header "Authorization" is the NVCF CLI function API key with prefix "Bearer "
    When I send a "GET" request to "http://api.localhost:8080/v2/nvcf/clusterGroups"
    Then the HTTP status should be 200

    # Record the API Keys image so the test can tell that the upgrade changed it.
    When I successfully run command:
      """
      kubectl --kubeconfig "${UPGRADE_KUBECONFIG}" --context "${UPGRADE_CONTEXT}" -n api-keys get deploy api-keys -o jsonpath={.spec.template.spec.containers[0].image}
      """
    And I export command output to environment variable "UPGRADE_IMAGE_BEFORE"

    # Upgrade, then wait until every old API Keys pod is replaced and the key
    # migration has recorded that it is complete.
    When I successfully run command:
      """
      ${UPGRADE_APPLY_COMMAND}
      """
    Then deployment "api-keys" in namespace "api-keys" using context "${UPGRADE_CONTEXT}" should complete rollout within "10m"
    When I successfully run command:
      """
      kubectl --kubeconfig "${UPGRADE_KUBECONFIG}" --context "${UPGRADE_CONTEXT}" -n api-keys get deploy api-keys -o jsonpath={.spec.template.spec.containers[0].image}
      """
    And I export command output to environment variable "UPGRADE_IMAGE_AFTER"
    # Fails within seconds when the upgrade command did not change the API Keys image,
    # instead of waiting for a completion record that will never be written.
    When I run command:
      """
      /bin/bash -eu -c 'test "$1" != "$2"' api-keys-image-changed "${UPGRADE_IMAGE_BEFORE}" "${UPGRADE_IMAGE_AFTER}"
      """
    Then the command exit code should be 0
    When I poll command for up to 2700 seconds until output contains "(1 rows)":
      """
      /bin/bash ${REPO_ROOT}/tests/bdd/scripts/api-keys-cql.sh "${UPGRADE_KUBECONFIG}" "${UPGRADE_CONTEXT}" "SELECT record_key FROM api_keys_api.row_update_lock WHERE table_name = 'keys_by_account_index_copy' AND record_key = 'completed';"
      """

    # After the upgrade: the same key still works.
    Given the HTTP request header "Authorization" is the NVCF CLI function API key with prefix "Bearer "
    When I send a "GET" request to "http://api.localhost:8080/v2/nvcf/clusterGroups"
    Then the HTTP status should be 200

    # The key had no account ID before the upgrade. The migration assigns it in the
    # credential table and the account index. It does not backfill the old owner table.
    When I successfully run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" init
      """
    And I successfully run command:
      """
      /bin/bash -eu -c '"$1" --config "$2" api-key list --json | jq -r --arg d "$3" "first(.keys[] | select(.description == \$d)).id"' api-keys-key-id "${NVCF_CLI}" "${UPGRADE_CLI_CONFIG}" bdd-m0-legacy-key
      """
    And I export command output to environment variable "UPGRADE_LEGACY_KEY_ID"
    When I run command:
      """
      /bin/bash ${REPO_ROOT}/tests/bdd/scripts/api-keys-cql.sh "${UPGRADE_KUBECONFIG}" "${UPGRADE_CONTEXT}" "SELECT key_id FROM api_keys_api.keys_by_owner_and_account_and_service WHERE key_id = '${UPGRADE_LEGACY_KEY_ID}' AND nca_id = '${UPGRADE_NCA_ID}' ALLOW FILTERING;"
      """
    Then the command exit code should be 0
    And the command output should contain "(1 rows)"
    When I run command:
      """
      /bin/bash ${REPO_ROOT}/tests/bdd/scripts/api-keys-cql.sh "${UPGRADE_KUBECONFIG}" "${UPGRADE_CONTEXT}" "SELECT nca_id FROM api_keys_api.keys;"
      """
    Then the command exit code should be 0
    And the command output should not contain "null"

    When I successfully run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" api-key delete ${UPGRADE_LEGACY_KEY_ID} --force
      """

  Scenario: A key created after the upgrade carries the account ID in all three tables
    Given these environment variables are set:
      | name               |
      | NVCF_CLI           |
      | UPGRADE_CLI_CONFIG |
      | UPGRADE_KUBECONFIG |
      | UPGRADE_CONTEXT    |
      | UPGRADE_NCA_ID     |
    And I use NVCF CLI config "${UPGRADE_CLI_CONFIG}"
    When I successfully run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" init
      """
    And I successfully generate a function API key with CLI options:
      | option        | value               |
      | --description | bdd-m0-new-key      |
      | --scopes      | list_cluster_groups |
    Given the HTTP request header "Authorization" is the NVCF CLI function API key with prefix "Bearer "
    When I send a "GET" request to "http://api.localhost:8080/v2/nvcf/clusterGroups"
    Then the HTTP status should be 200

    When I successfully run command:
      """
      /bin/bash -eu -c '"$1" --config "$2" api-key list --json | jq -r --arg d "$3" "first(.keys[] | select(.description == \$d)).id"' api-keys-key-id "${NVCF_CLI}" "${UPGRADE_CLI_CONFIG}" bdd-m0-new-key
      """
    And I export command output to environment variable "UPGRADE_NEW_KEY_ID"

    # A new key is stored under the canonical service ID, not the legacy one.
    When I run command:
      """
      /bin/bash ${REPO_ROOT}/tests/bdd/scripts/api-keys-cql.sh "${UPGRADE_KUBECONFIG}" "${UPGRADE_CONTEXT}" "SELECT issuer_service_id FROM api_keys_api.keys_by_owner_and_account_and_service WHERE key_id = '${UPGRADE_NEW_KEY_ID}' ALLOW FILTERING;"
      """
    Then the command exit code should be 0
    And the command output should contain "cloud-functions"
    And the command output should not contain "nvidia-cloud-functions-ncp-service-id-aketm"

    # The account ID is in the account index, the old owner table, and the credential table.
    When I run command:
      """
      /bin/bash ${REPO_ROOT}/tests/bdd/scripts/api-keys-cql.sh "${UPGRADE_KUBECONFIG}" "${UPGRADE_CONTEXT}" "SELECT key_id FROM api_keys_api.keys_by_owner_and_account_and_service WHERE key_id = '${UPGRADE_NEW_KEY_ID}' AND nca_id = '${UPGRADE_NCA_ID}' ALLOW FILTERING;"
      """
    Then the command exit code should be 0
    And the command output should contain "(1 rows)"
    When I run command:
      """
      /bin/bash ${REPO_ROOT}/tests/bdd/scripts/api-keys-cql.sh "${UPGRADE_KUBECONFIG}" "${UPGRADE_CONTEXT}" "SELECT key_id FROM api_keys_api.keys_by_owner_and_service WHERE key_id = '${UPGRADE_NEW_KEY_ID}' AND nca_id = '${UPGRADE_NCA_ID}' ALLOW FILTERING;"
      """
    Then the command exit code should be 0
    And the command output should contain "(1 rows)"
    When I run command:
      """
      /bin/bash ${REPO_ROOT}/tests/bdd/scripts/api-keys-cql.sh "${UPGRADE_KUBECONFIG}" "${UPGRADE_CONTEXT}" "SELECT nca_id FROM api_keys_api.keys;"
      """
    Then the command exit code should be 0
    And the command output should not contain "null"

    When I successfully run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" api-key delete ${UPGRADE_NEW_KEY_ID} --force
      """
