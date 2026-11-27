@upgrade-transition-audit
Feature: Audit the immutable inputs for one upgrade hop
  As an operator preparing an HA upgrade,
  I want to reject changed inputs before any workload changes,
  so that a component report cannot be mistaken for runtime qualification.

  Background:
    Given these environment variables are set:
      | name                 |
      | UPGRADE_LAB          |
      | TRANSITION_CONTRACTS |
      | TRANSITION_INPUTS    |
      | TRANSITION_OUTPUT    |

  Scenario: Compare both umbrella routes without qualifying either
    When I successfully run command:
      """
      cat "${TRANSITION_CONTRACTS}/manifests/1.0.4.json"
      """
    And I export command output JSON field "stacks.control-plane.version" to environment variable "UMBRELLA_104_CONTROL"
    And I export command output JSON field "stacks.compute-plane.version" to environment variable "UMBRELLA_104_COMPUTE"
    And I export command output JSON field "stacks.observability.version" to environment variable "UMBRELLA_104_OBSERVABILITY"
    When I successfully run command:
      """
      cat "${TRANSITION_CONTRACTS}/manifests/1.1.1.json"
      """
    And I export command output JSON field "stacks.control-plane.version" to environment variable "UMBRELLA_111_CONTROL"
    And I export command output JSON field "stacks.compute-plane.version" to environment variable "UMBRELLA_111_COMPUTE"
    And I export command output JSON field "stacks.observability.version" to environment variable "UMBRELLA_111_OBSERVABILITY"
    When I successfully run command:
      """
      "${UPGRADE_LAB}" audit --contracts "${TRANSITION_CONTRACTS}" --route umbrella-1.0.1-to-1.0.4 --inputs "${TRANSITION_INPUTS}" --out "${TRANSITION_OUTPUT}/101-104"
      """
    Then yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "summary.qualification" should equal "unqualified"
    And yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "comparison_scope" should equal "release-inventories"
    And yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "runtime_artifact_checks[0]" should not be empty
    And yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "version_scope" should equal "umbrella-release"
    And yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "selections[0].kind" should equal "legacy-baseline"
    And yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "selections[1].kind" should equal "umbrella-release"
    And yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "selections[1].manifest.sha256" should not be empty
    # These literals test the explicit legacy exception without a manifest.
    And yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "selections[0].stacks.control-plane.version" should equal "1.0.1"
    And yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "selections[1].stacks.control-plane.version" should equal "${UMBRELLA_104_CONTROL}"
    And yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "selections[0].stacks.compute-plane.version" should equal "1.0.0"
    And yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "selections[0].stacks.observability.version" should equal "1.0.0"
    And yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "selections[1].stacks.compute-plane.version" should equal "${UMBRELLA_104_COMPUTE}"
    And yaml file "${TRANSITION_OUTPUT}/101-104/audit.json" key "selections[1].stacks.observability.version" should equal "${UMBRELLA_104_OBSERVABILITY}"
    When I successfully run command:
      """
      "${UPGRADE_LAB}" audit --contracts "${TRANSITION_CONTRACTS}" --route umbrella-1.0.4-to-1.1.1 --inputs "${TRANSITION_INPUTS}" --out "${TRANSITION_OUTPUT}/104-111"
      """
    Then yaml file "${TRANSITION_OUTPUT}/104-111/audit.json" key "summary.qualification" should equal "unqualified"
    And yaml file "${TRANSITION_OUTPUT}/104-111/audit.json" key "selections[0].stacks.observability.version" should equal "${UMBRELLA_104_OBSERVABILITY}"
    And yaml file "${TRANSITION_OUTPUT}/104-111/audit.json" key "selections[0].stacks.compute-plane.version" should equal "${UMBRELLA_104_COMPUTE}"
    And yaml file "${TRANSITION_OUTPUT}/104-111/audit.json" key "selections[1].stacks.control-plane.version" should equal "${UMBRELLA_111_CONTROL}"
    And yaml file "${TRANSITION_OUTPUT}/104-111/audit.json" key "selections[0].stacks.control-plane.version" should equal "${UMBRELLA_104_CONTROL}"
    And yaml file "${TRANSITION_OUTPUT}/104-111/audit.json" key "selections[1].stacks.compute-plane.version" should equal "${UMBRELLA_111_COMPUTE}"
    And yaml file "${TRANSITION_OUTPUT}/104-111/audit.json" key "selections[1].stacks.observability.version" should equal "${UMBRELLA_111_OBSERVABILITY}"
    And yaml file "${TRANSITION_OUTPUT}/104-111/audit.json" key "conditional_checks[0].id" should equal "ha-mode-and-topology"
    And yaml file "${TRANSITION_OUTPUT}/104-111/audit.json" key "conditional_checks[0].when_applicable" should not be empty
    And yaml file "${TRANSITION_OUTPUT}/104-111/audit.json" key "conditional_checks[0].when_not_applicable" should not be empty
    And yaml file "${TRANSITION_OUTPUT}/104-111/audit.json" key "conditional_checks[0].when_unknown" should not be empty

  Scenario: Reject peers selected from another endpoint matrix
    Given I copy the file "${TRANSITION_CONTRACTS}/artifact-lock.json" to "${TRANSITION_OUTPUT}/wrong-matrix-contracts/artifact-lock.json"
    And I substitute a block in file "${TRANSITION_OUTPUT}/wrong-matrix-contracts/artifact-lock.json":
      """
      "peer_matrix_edition": "1.0.4"
      ---
      "peer_matrix_edition": "1.0.1"
      """
    When I run command:
      """
      "${UPGRADE_LAB}" audit --contracts "${TRANSITION_OUTPUT}/wrong-matrix-contracts" --route umbrella-1.0.1-to-1.0.4 --inputs "${TRANSITION_INPUTS}" --out "${TRANSITION_OUTPUT}/wrong-matrix-rejected"
      """
    Then the command exit code should be 1
    And the command output should contain "differs from docs matrix"

  Scenario: Reject a stack-only selection in an umbrella lock
    Given I copy the file "${TRANSITION_CONTRACTS}/artifact-lock.json" to "${TRANSITION_OUTPUT}/wrong-kind-contracts/artifact-lock.json"
    And I substitute a block in file "${TRANSITION_OUTPUT}/wrong-kind-contracts/artifact-lock.json":
      """
      "kind": "umbrella-release"
      ---
      "kind": "self-managed-stack"
      """
    When I run command:
      """
      "${UPGRADE_LAB}" audit --contracts "${TRANSITION_OUTPUT}/wrong-kind-contracts" --route umbrella-1.0.1-to-1.0.4 --inputs "${TRANSITION_INPUTS}" --out "${TRANSITION_OUTPUT}/wrong-kind-rejected"
      """
    Then the command exit code should be 1
    And the command output should contain "unsupported selection kind"

  Scenario: Reject changed umbrella manifest bytes
    Given I copy the file "${TRANSITION_CONTRACTS}/artifact-lock.json" to "${TRANSITION_OUTPUT}/changed-manifest-contracts/artifact-lock.json"
    And I copy the file "${TRANSITION_CONTRACTS}/manifests/1.0.4.json" to "${TRANSITION_OUTPUT}/changed-manifest-contracts/manifests/1.0.4.json"
    And I substitute a block in file "${TRANSITION_OUTPUT}/changed-manifest-contracts/manifests/1.0.4.json":
      """
      "version": "1.0.4"
      ---
      "version": "9.9.9"
      """
    When I run command:
      """
      "${UPGRADE_LAB}" audit --contracts "${TRANSITION_OUTPUT}/changed-manifest-contracts" --route umbrella-1.0.1-to-1.0.4 --inputs "${TRANSITION_INPUTS}" --out "${TRANSITION_OUTPUT}/manifest-rejected"
      """
    Then the command exit code should be 1
    And the command output should contain "umbrella manifest digest mismatch"

  Scenario: Reject a historical stack-only route ID
    When I run command:
      """
      "${UPGRADE_LAB}" audit --contracts "${TRANSITION_CONTRACTS}" --route 1.0.1-to-1.0.4 --inputs "${TRANSITION_INPUTS}" --out "${TRANSITION_OUTPUT}/old-route-rejected"
      """
    Then the command exit code should be 1
    And the command output should contain "provide an umbrella route ID"

  Scenario: Reject a changed source inventory before comparison
    Given I write yaml file "${TRANSITION_OUTPUT}/changed-inputs/control-plane-1.0.1.json" with values:
      | source.version | changed |
    When I run command:
      """
      "${UPGRADE_LAB}" audit --contracts "${TRANSITION_CONTRACTS}" --route umbrella-1.0.1-to-1.0.4 --inputs "${TRANSITION_OUTPUT}/changed-inputs" --out "${TRANSITION_OUTPUT}/rejected"
      """
    Then the command exit code should be 1
    And the command output should contain "inventory digest mismatch"

  Scenario: Reject a changed original umbrella bundle before comparison
    Given I copy the file "${TRANSITION_INPUTS}/releases/1.0.4/manifest.json" to "${TRANSITION_OUTPUT}/changed-release/releases/1.0.4/manifest.json"
    And I copy the file "${TRANSITION_INPUTS}/releases/1.0.4/release-assets.json" to "${TRANSITION_OUTPUT}/changed-release/releases/1.0.4/release-assets.json"
    When I successfully run command:
      """
      /bin/sh -c 'printf "changed bundle\n" > "$1"' _ "${TRANSITION_OUTPUT}/changed-release/releases/1.0.4/nvcf-1.0.4.tar.gz"
      """
    And I run command:
      """
      "${UPGRADE_LAB}" audit --contracts "${TRANSITION_CONTRACTS}" --route umbrella-1.0.4-to-1.1.1 --inputs "${TRANSITION_OUTPUT}/changed-release" --out "${TRANSITION_OUTPUT}/changed-bundle-rejected"
      """
    Then the command exit code should be 1
    And the command output should contain "release asset digest mismatch: nvcf-1.0.4.tar.gz"

  Scenario: Reject an undeclared direct hop
    When I run command:
      """
      "${UPGRADE_LAB}" audit --contracts "${TRANSITION_CONTRACTS}" --route umbrella-1.0.1-to-1.1.1 --inputs "${TRANSITION_INPUTS}" --out "${TRANSITION_OUTPUT}/direct-hop"
      """
    Then the command exit code should be 1

  Scenario: Refuse an unimplemented mutation phase before reading credentials
    When I run command:
      """
      "${UPGRADE_LAB}" rehearse --route umbrella-1.0.4-to-1.1.1 --run-config "${TRANSITION_OUTPUT}/absent-private-config.json" --run-dir "${TRANSITION_OUTPUT}/must-not-apply" --phase apply
      """
    Then the command exit code should be 1
    And the command output should contain "only prepare is implemented"

  Scenario: Reject recovery evidence that is not bound to immutable inputs
    When I successfully run command:
      """
      /bin/sh -c 'printf "{}\n" > "$1"' _ "${TRANSITION_OUTPUT}/empty-recovery.json"
      """
    When I run command:
      """
      "${UPGRADE_LAB}" check-recovery --contracts "${TRANSITION_CONTRACTS}" --route umbrella-1.0.4-to-1.1.1 --baseline "${TRANSITION_OUTPUT}/empty-recovery.json" --checkpoint "${TRANSITION_OUTPUT}/empty-recovery.json" --out "${TRANSITION_OUTPUT}/must-not-recover"
      """
    Then the command exit code should be 1
    And the command output should contain "recovery checkpoint does not match"

  Scenario: Reject the superseded umbrella target
    When I run command:
      """
      "${UPGRADE_LAB}" audit --contracts "${TRANSITION_CONTRACTS}" --route umbrella-1.0.4-to-1.1.0 --inputs "${TRANSITION_INPUTS}" --out "${TRANSITION_OUTPUT}/superseded-target"
      """
    Then the command exit code should be 1
