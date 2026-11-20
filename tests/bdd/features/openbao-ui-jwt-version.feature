# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
@openbao-ui-jwt-version
Feature: Target-only releases configure the UI JWT mount version explicitly
  # Supply a saved release environment with UI enabled and target-only images.
  # The check renders published charts and probes images without a cluster.

  Scenario: Qualify the actual target-only server and UI migration configuration
    Given these environment variables are set:
      | name                            |
      | OPENBAO_UI_JWT_STACK             |
      | OPENBAO_UI_JWT_ENVIRONMENT       |
    When I successfully run executable "tools/ci/qualify-openbao-ui-jwt-version" with arguments:
      | argument                         |
      | ${OPENBAO_UI_JWT_STACK}           |
      | ${OPENBAO_UI_JWT_ENVIRONMENT}     |
    Then the command output should contain "openbao-ui-jwt-version: target-only"
