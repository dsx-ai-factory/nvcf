@fake-kit-sessions
Feature: Session-ending inference processes preserve healthy workers and recover from crashes
  The fixture runs in a published Python-capable image through the normal
  container-arguments API. Real utils HTTP readiness drives the Pod condition.
  The red image differs only in its legacy cumulative-restart predicate.

  Scenario: Operator validates clean session exits and unattended crash recovery with no spare GPUs
    Given these environment variables are set:
      | name                       |
      | FAKE_KIT_RED_AGENT_VERSION |
      | CAPACITY_NVCA_AGENT_VERSION |
      | FAKE_KIT_RUNTIME_IMAGE     |
      | FAKE_KIT_FILLER_IMAGE      |
    When I successfully run command:
      """
      python3 ${REPO_ROOT}/tests/bdd/scripts/check_fake_kit_sessions.py --cli ${NVCF_CLI} --cli-config-template ${REPO_ROOT}/tests/bdd/fixtures/nvcf-cli-local.yaml --context k3d-ncp-local --backend ncp-local --red-agent-version ${FAKE_KIT_RED_AGENT_VERSION} --green-agent-version ${CAPACITY_NVCA_AGENT_VERSION} --fake-kit-image ${FAKE_KIT_RUNTIME_IMAGE} --filler-image ${FAKE_KIT_FILLER_IMAGE} --fixture ${REPO_ROOT}/tests/bdd/fixtures/fake_kit.py --gpu H100 --instance-type NCP.GPU.H100_1x --region us-west-1 --account nvcf-default --api-url http://127.0.0.1:8080 --api-host api.localhost --icms-host sis.localhost --capacity 4 --minimum-pod-age 600 --observation-seconds 600 --session-ends 4 --function-active-timeout 90 --output-dir ${REPO_ROOT}/tests/bdd/out
      """
    Then the command output should contain "fake Kit sessions passed"
