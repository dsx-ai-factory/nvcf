@openbao @kubernetes @bootstrap
Feature: Bootstrap OpenBao with strict readiness and preserve initialized state
  As a self-managed operator,
  I want sealed and uninitialized members excluded from client routing,
  so bootstrap can complete before Kubernetes readiness.

  Background:
    Given these environment variables are set:
      | name                     |
      | OPENBAO_TEST_CONTEXT     |
      | OPENBAO_TEST_NAMESPACE   |
      | OPENBAO_TEST_RELEASE     |
      | OPENBAO_TEST_CHART       |
      | OPENBAO_TEST_VALUES      |
      | OPENBAO_TEST_ARTIFACTS   |

  Scenario: Install, seal, recover and upgrade a three-member OpenBao cluster
    When I successfully run command:
      """
      helm upgrade --install "${OPENBAO_TEST_RELEASE}" "${OPENBAO_TEST_CHART}" --kube-context "${OPENBAO_TEST_CONTEXT}" --namespace "${OPENBAO_TEST_NAMESPACE}" --create-namespace --values "${OPENBAO_TEST_VALUES}" --wait --wait-for-jobs --timeout 15m --set openbao.server.service.publishNotReadyAddresses=false
      """
    When I successfully run command:
      """
      /bin/bash -e -o pipefail -c 'kubectl --context "$1" -n "$2" get statefulset openbao-server -o json | jq -c '"'"'[.spec.template.spec.containers[] | select(.name == "openbao") | {name, path: .readinessProbe.httpGet.path}]'"'"'' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    Then the json output should contain rows:
      | name    | path                               |
      | openbao | /v1/sys/health?standbyok=true       |
    When I successfully run command:
      """
      /bin/bash -e -o pipefail -c 'for pod in openbao-server-0 openbao-server-1 openbao-server-2; do kubectl --context "$1" -n "$2" exec "$pod" -c openbao -- bao status -format=json | jq -c --arg pod "$pod" '"'"'{pod: $pod, initialized, sealed}'"'"' || exit; done | jq -sc .' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    Then the json output should contain rows:
      | pod              | initialized | sealed |
      | openbao-server-0 | true        | false  |
      | openbao-server-1 | true        | false  |
      | openbao-server-2 | true        | false  |
    When I successfully run command:
      """
      /bin/bash -e -o pipefail -c 'state=$(kubectl --context "$1" -n "$2" get secret openbao-server-root-token -o jsonpath='"'"'{.data.bootstrap_state}'"'"' | jq -Rer '"'"'select(@base64d == "complete")'"'"' ); test -n "$state" && printf complete' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    Then the command output should contain "complete"
    When I successfully run command:
      """
      /bin/bash -e -o pipefail -c 'mkdir -p "$3"; kubectl --context "$1" -n "$2" exec openbao-server-0 -c openbao -- bao status -format=json | jq -er '"'"'.cluster_id | strings | select(length > 0)'"'"' > "$3/cluster-id-before"; kubectl --context "$1" -n "$2" get secrets openbao-server-unseal openbao-server-root-token -o json | jq -c '"'"'[.items[] | {name: .metadata.name, data}] | sort_by(.name)'"'"' | shasum -a 256 > "$3/credentials-before"' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    When I successfully run command:
      """
      /bin/bash -e -o pipefail -c 'for pod in openbao-server-0 openbao-server-1 openbao-server-2; do if kubectl --context "$1" -n "$2" exec "$pod" -c openbao -- bao read -format=json sys/leader | jq -e .data.is_self >/dev/null; then printf "%s" "$pod" > "$3/sealed-member"; exit 0; fi; done; exit 1' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    When I successfully run command:
      """
      /bin/bash -e -o pipefail -c 'bash tests/bdd/scripts/openbao-unseal-monitor.sh "$1" "$2" "$(cat "$3/sealed-member")" pause' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    Then the command output should contain "monitor=pause"
    When I successfully run command:
      """
      /bin/bash -e -o pipefail -c 'bash tests/bdd/scripts/openbao-admin.sh "$1" "$2" "$(cat "$3/sealed-member")" operator seal' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    When I successfully run command:
      """
      /bin/bash -e -o pipefail -c 'kubectl --context "$1" -n "$2" wait "pod/$(cat "$3/sealed-member")" --for=condition=Ready=false --timeout=45s' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    When I poll command for up to 45 seconds until output contains "endpoint=excluded":
      """
      /bin/bash -e -o pipefail -c 'kubectl --context "$1" -n "$2" get endpointslices -l kubernetes.io/service-name=openbao-server -o json | jq -e --arg pod "$(cat "$3/sealed-member")" '"'"'(.items | length > 0) and ([.items[].endpoints[]? | select(.targetRef.name == $pod and .conditions.ready == true)] | length == 0) and any(.items[].endpoints[]?; .targetRef.name != $pod and .conditions.ready == true and (.targetRef.name // "" | startswith("openbao-server-")))'"'"' >/dev/null && echo endpoint=excluded' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    When I successfully run command:
      """
      /bin/bash -e -o pipefail -c 'bash tests/bdd/scripts/openbao-unseal-monitor.sh "$1" "$2" "$(cat "$3/sealed-member")" resume' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    Then the command output should contain "monitor=resume"
    When I successfully run command:
      """
      /bin/bash -e -o pipefail -c 'kubectl --context "$1" -n "$2" wait "pod/$(cat "$3/sealed-member")" --for=condition=Ready --timeout=120s' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    When I poll command for up to 45 seconds until output contains "endpoint=included":
      """
      /bin/bash -e -o pipefail -c 'kubectl --context "$1" -n "$2" get endpointslices -l kubernetes.io/service-name=openbao-server -o json | jq -e --arg pod "$(cat "$3/sealed-member")" '"'"'any(.items[].endpoints[]?; .targetRef.name == $pod and .conditions.ready == true)'"'"' >/dev/null && echo endpoint=included' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    When I successfully run command:
      """
      helm upgrade --install "${OPENBAO_TEST_RELEASE}" "${OPENBAO_TEST_CHART}" --kube-context "${OPENBAO_TEST_CONTEXT}" --namespace "${OPENBAO_TEST_NAMESPACE}" --create-namespace --values "${OPENBAO_TEST_VALUES}" --wait --wait-for-jobs --timeout 15m --set-string openbao.hooks.initializeCluster.podAnnotations.qa=upgrade --set openbao.server.service.publishNotReadyAddresses=false
      """
    When I successfully run command:
      """
      /bin/bash -e -o pipefail -c 'kubectl --context "$1" -n "$2" exec openbao-server-0 -c openbao -- bao status -format=json | jq -er '"'"'.cluster_id | strings | select(length > 0)'"'"' > "$3/cluster-id-after"; kubectl --context "$1" -n "$2" get secrets openbao-server-unseal openbao-server-root-token -o json | jq -c '"'"'[.items[] | {name: .metadata.name, data}] | sort_by(.name)'"'"' | shasum -a 256 > "$3/credentials-after"; cmp "$3/cluster-id-before" "$3/cluster-id-after" && cmp "$3/credentials-before" "$3/credentials-after" && echo state=preserved' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    Then the command output should contain "state=preserved"
    When I successfully run command:
      """
      /bin/bash -e -o pipefail -c 'for pod in openbao-server-0 openbao-server-1 openbao-server-2; do kubectl --context "$1" -n "$2" exec "$pod" -c openbao -- bao status -format=json | jq -c --arg pod "$pod" '"'"'{pod: $pod, initialized, sealed}'"'"' || exit; done | jq -sc .' qa "${OPENBAO_TEST_CONTEXT}" "${OPENBAO_TEST_NAMESPACE}" "${OPENBAO_TEST_ARTIFACTS}"
      """
    Then the json output should contain rows:
      | pod              | initialized | sealed |
      | openbao-server-0 | true        | false  |
      | openbao-server-1 | true        | false  |
      | openbao-server-2 | true        | false  |
