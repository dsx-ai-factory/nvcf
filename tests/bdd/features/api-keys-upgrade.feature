@upgrade @existing-install
Feature: An API key created before the API Keys upgrade keeps working
  As a self-managed NVCF operator,
  I want a key created with a legacy service ID before the upgrade to work after the upgrade,
  so that upgrading API Keys does not break existing keys.

  # Run the before scenario against the old release, upgrade API Keys outside
  # this feature, wait for the account-index copy to complete, then run the
  # after scenario. The suite restores the CLI state file when a run ends, so
  # the before scenario also saves the generated key to UPGRADE_KEY_FILE and
  # the after scenario reads it from there.

  @api-keys-before-upgrade
  Scenario: Create a key with the legacy function service ID before the upgrade
    Given these environment variables are set:
      | name               |
      | NVCF_CLI           |
      | UPGRADE_CLI_CONFIG |
      | UPGRADE_KEY_FILE   |
    When I successfully run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" init
      """
    And I successfully run command:
      """
      /bin/bash -eu -c 'umask 077; "${@:2}" > "$1"' api-keys-legacy-key "${UPGRADE_KEY_FILE}" "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" api-key generate --for function --description bdd-m0-legacy-key --scopes list_cluster_groups --json
      """
    And I run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" cluster list
      """
    Then the command exit code should be 0
    And the command output should contain "Found"

  @api-keys-after-upgrade
  Scenario: Use the same key after the upgrade and the account-index copy
    Given these environment variables are set:
      | name               |
      | NVCF_CLI           |
      | UPGRADE_CLI_CONFIG |
      | UPGRADE_KEY_FILE   |
      | UPGRADE_KUBECONFIG |
      | UPGRADE_CONTEXT    |
    When I run command:
      """
      /bin/bash -eu -c 'helm --kubeconfig "$1" --kube-context "$2" -n cassandra-system get values cassandra -a -o json | jq -r .cassandra.dbUser.password | kubectl --kubeconfig "$1" --context "$2" -n cassandra-system exec -i cassandra-0 -c cassandra -- sh -c "/opt/cassandra/bin/cqlsh -u cassandra -p \"\$(cat)\" localhost -e \"\$0\"" "$3"' api-keys-marker "${UPGRADE_KUBECONFIG}" "${UPGRADE_CONTEXT}" "SELECT record_key FROM api_keys_api.row_update_lock WHERE table_name = 'keys_by_account_index_copy' AND record_key = 'completed';"
      """
    Then the command exit code should be 0
    And the command output should contain "(1 rows)"
    When I run command:
      """
      /bin/bash -eu -c 'NVCF_API_KEY="$(jq -r ".keys[0].apiKey" "$1")" exec "${@:2}"' api-keys-legacy-use "${UPGRADE_KEY_FILE}" "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" cluster list
      """
    Then the command exit code should be 0
    And the command output should contain "Found"
    When I successfully run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" init
      """
    And I run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" api-key list --json
      """
    Then the command exit code should be 0
    And the command output should contain "bdd-m0-legacy-key"
    When I successfully run command:
      """
      /bin/rm -f "${UPGRADE_KEY_FILE}"
      """

  @api-keys-new-key-after-upgrade
  Scenario: Create and use a new key after the upgrade
    Given these environment variables are set:
      | name               |
      | NVCF_CLI           |
      | UPGRADE_CLI_CONFIG |
      | UPGRADE_KUBECONFIG |
      | UPGRADE_CONTEXT    |
    When I successfully run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" init
      """
    And I successfully run command:
      """
      /bin/sh -c 'exec "$@" >/dev/null' nvcf-cli "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" api-key generate --for function --description bdd-m0-new-key --scopes list_cluster_groups
      """
    And I run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" cluster list
      """
    Then the command exit code should be 0
    And the command output should contain "Found"
    When I successfully run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" init
      """
    And I run command:
      """
      "${NVCF_CLI}" --config "${UPGRADE_CLI_CONFIG}" api-key list --json
      """
    Then the command exit code should be 0
    And the command output should contain "bdd-m0-new-key"
    When I run command:
      """
      /bin/bash -eu -c 'helm --kubeconfig "$1" --kube-context "$2" -n cassandra-system get values cassandra -a -o json | jq -r .cassandra.dbUser.password | kubectl --kubeconfig "$1" --context "$2" -n cassandra-system exec -i cassandra-0 -c cassandra -- sh -c "/opt/cassandra/bin/cqlsh -u cassandra -p \"\$(cat)\" localhost -e \"\$0\"" "$3"' api-keys-index "${UPGRADE_KUBECONFIG}" "${UPGRADE_CONTEXT}" "SELECT issuer_service_id FROM api_keys_api.keys_by_owner_and_account_and_service;"
      """
    Then the command exit code should be 0
    And the command output should contain "cloud-functions"
    And the command output should not contain "nvidia-cloud-functions-ncp-service-id-aketm"
