/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.nvcf.rest.function.invocation;

import static com.nvidia.nvcf.IntegrationTestConfiguration.MOCK_OAUTH2_TOKEN_SERVER;
import static com.nvidia.nvcf.util.MockApiKeysServer.resetToDefault;
import static com.nvidia.nvcf.util.NvcfConstants.ADMIN_SCOPE_LIST_FUNCTIONS;
import static com.nvidia.nvcf.util.NvcfConstants.ADMIN_SCOPE_LIST_FUNCTIONS_DETAILS;
import static com.nvidia.nvcf.util.TestConstants.SCOPE_INVOKE_FUNCTION;
import static com.nvidia.nvcf.util.TestConstants.SCOPE_LIST_FUNCTIONS;
import static com.nvidia.nvcf.util.TestConstants.TEST_ADMIN_SUBJECT;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_ID_3;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_NAME;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_NAME_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_NAME_3;
import static com.nvidia.nvcf.util.TestConstants.TEST_NCA_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_NCA_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_1;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_3;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_4;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_5;
import static org.assertj.core.api.Assertions.assertThat;

import com.nvidia.boot.mock.ngc.MockCasServer;
import com.nvidia.boot.mock.ngc.MockNgcContainerRegistryServer;
import com.nvidia.nvcf.IntegrationTestConfiguration;
import com.nvidia.nvcf.NvcfTestApp;
import com.nvidia.nvcf.rest.account.TestAccountService;
import com.nvidia.nvcf.rest.azp.TestAuthorizedPartiesService;
import com.nvidia.nvcf.rest.azp.dto.AuthorizedPartyDto;
import com.nvidia.nvcf.rest.function.invocation.dto.InvocableFunctionDto;
import com.nvidia.nvcf.rest.function.invocation.dto.ListInvocableFunctionsResponse;
import com.nvidia.nvcf.rest.function.management.dto.ListFunctionsResponse;
import com.nvidia.nvcf.service.common.TestCommonService;
import com.nvidia.nvcf.util.MockApiKeysServer;
import com.nvidia.nvcf.util.MockEssServer;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
@AutoConfigureTestRestTemplate
@SpringBootTest(
        classes = {NvcfTestApp.class, IntegrationTestConfiguration.class},
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.active=test")
@ContextConfiguration(initializers = IntegrationTestConfiguration.Initializer.class)
class XAccountFunctionInvocationControllerTest {

    private static final String INVOCABLE_FUNCTIONS_URI =
            "/v2/nvcf/accounts/%s/invocable/functions/versions/ids";

    // TEST_NCA_ID owns three function versions, and TEST_NCA_ID_2 shares one of its two versions
    // with it.
    private static final List<InvocableFunctionDto> OWN_AND_SHARED = List.of(
            new InvocableFunctionDto(TEST_FUNCTION_ID, TEST_VERSION_ID_1),
            new InvocableFunctionDto(TEST_FUNCTION_ID, TEST_VERSION_ID_3),
            new InvocableFunctionDto(TEST_FUNCTION_ID_3, TEST_VERSION_ID_4),
            new InvocableFunctionDto(TEST_FUNCTION_ID_2, TEST_VERSION_ID_2));

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private TestCommonService testCommonService;

    @Autowired
    private TestAuthorizedPartiesService testAuthPartiesService;

    @Autowired
    private TestAccountService testAccountService;

    @Value("${nvcf.api-keys.base-url}")
    private String apiKeysBaseUrl;

    @Value("${nvcf.ess.base-url}")
    private String essBaseUrl;

    @Value("${nvcf.registries.recognized.helm.ngc.hostname}")
    private String casBaseUrl;

    @Value("${nvcf.registries.recognized.helm.ngc.oauth2.base-url}")
    private String authnBaseUrl;

    @Value("${nvcf.registries.recognized.container.ngc.hostname}")
    private String ngcContainerRegistryUrl;

    @BeforeAll
    void beforeAll() {
        log.info("{}: Started running tests", this.getClass().getSimpleName());
        MockEssServer.start(essBaseUrl);
        MockApiKeysServer.start(apiKeysBaseUrl);
        MockCasServer.start(authnBaseUrl, casBaseUrl);
        MockNgcContainerRegistryServer.start(ngcContainerRegistryUrl);

        testAccountService.createDefaultAccountsClientsAndRegistries();
    }

    @BeforeEach
    void beforeEach() {
        testAuthPartiesService.createTestFunction(TEST_FUNCTION_ID, TEST_VERSION_ID_1,
                                                  TEST_NCA_ID, TEST_FUNCTION_NAME);
        testAuthPartiesService.createTestFunction(TEST_FUNCTION_ID, TEST_VERSION_ID_3,
                                                  TEST_NCA_ID, TEST_FUNCTION_NAME);
        testAuthPartiesService.createTestFunction(TEST_FUNCTION_ID_3, TEST_VERSION_ID_4,
                                                  TEST_NCA_ID, TEST_FUNCTION_NAME_3);
        testAuthPartiesService.createTestFunction(TEST_FUNCTION_ID_2, TEST_VERSION_ID_2,
                                                  TEST_NCA_ID_2, TEST_FUNCTION_NAME_2);
        testAuthPartiesService.createTestFunction(TEST_FUNCTION_ID_2, TEST_VERSION_ID_5,
                                                  TEST_NCA_ID_2, TEST_FUNCTION_NAME_2);
        testAuthPartiesService.associateAuthParties(
                TEST_NCA_ID_2, TEST_FUNCTION_ID_2, Optional.of(TEST_VERSION_ID_2),
                Set.of(AuthorizedPartyDto.builder().ncaId(TEST_NCA_ID).build()));
    }

    @AfterAll
    void cleanup() {
        testAccountService.cleanupAccountsClientsAndRegistries();

        MockApiKeysServer.stop();
        MockEssServer.stop();
        MockCasServer.stop();
        MockNgcContainerRegistryServer.stop();

        log.info("{}: Completed running tests", this.getClass().getSimpleName());
    }

    @AfterEach
    void reset() {
        testCommonService.reset();
        resetToDefault();
    }

    Stream<Arguments> scopeArgs() {
        return Stream.of(
                Arguments.of(List.of(ADMIN_SCOPE_LIST_FUNCTIONS), HttpStatus.OK),
                Arguments.of(List.of(ADMIN_SCOPE_LIST_FUNCTIONS_DETAILS), HttpStatus.FORBIDDEN),
                Arguments.of(List.of(SCOPE_LIST_FUNCTIONS), HttpStatus.FORBIDDEN),
                Arguments.of(List.of(SCOPE_INVOKE_FUNCTION), HttpStatus.FORBIDDEN));
    }

    @ParameterizedTest
    @MethodSource("scopeArgs")
    void shouldAcceptOnlyAdminListFunctionsScope(List<String> scopes, HttpStatus expectedStatus) {
        var token = MOCK_OAUTH2_TOKEN_SERVER.getJwt(TEST_ADMIN_SUBJECT, scopes, 100);

        var response = listInvocableFunctions(token, TEST_NCA_ID, "visibility=private,authorized");

        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        if (expectedStatus.isError()) {
            return;
        }
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(OWN_AND_SHARED);
    }

    // The result is for the account in the path, not the caller's.
    @Test
    void shouldListTheFunctionVersionsOfTheAccountInThePath() {
        var token = MOCK_OAUTH2_TOKEN_SERVER.getJwt(TEST_ADMIN_SUBJECT,
                                                    List.of(ADMIN_SCOPE_LIST_FUNCTIONS), 100);

        var response = listInvocableFunctions(token, TEST_NCA_ID_2, "visibility=private");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions()).containsExactlyInAnyOrder(
                new InvocableFunctionDto(TEST_FUNCTION_ID_2, TEST_VERSION_ID_2),
                new InvocableFunctionDto(TEST_FUNCTION_ID_2, TEST_VERSION_ID_5));
    }

    // The result has exactly the versions of the cross-account function listing.
    @Test
    void shouldReturnTheVersionsOfTheCrossAccountFunctionListing() {
        var token = MOCK_OAUTH2_TOKEN_SERVER.getJwt(TEST_ADMIN_SUBJECT,
                                                    List.of(ADMIN_SCOPE_LIST_FUNCTIONS), 100);
        var query = "visibility=private,authorized";

        var response = listInvocableFunctions(token, TEST_NCA_ID, query);

        var request = RequestEntity
                .get(URI.create("/v2/nvcf/accounts/" + TEST_NCA_ID + "/functions?" + query))
                .header("Authorization", "Bearer " + token)
                .build();
        var listing = testRestTemplate.exchange(request, ListFunctionsResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(listing.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(listing.getBody()).isNotNull();
        assertThat(response.getBody().functions()).containsExactlyInAnyOrderElementsOf(
                listing.getBody().functions().stream()
                        .map(dto -> new InvocableFunctionDto(dto.id(), dto.versionId()))
                        .toList());
    }

    @Test
    void shouldRejectAnUnknownAccount() {
        var token = MOCK_OAUTH2_TOKEN_SERVER.getJwt(TEST_ADMIN_SUBJECT,
                                                    List.of(ADMIN_SCOPE_LIST_FUNCTIONS), 100);

        var response = listInvocableFunctions(token, "test-unknown-nca-id", "");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private ResponseEntity<ListInvocableFunctionsResponse> listInvocableFunctions(
            String token,
            String ncaId,
            String query) {
        var uri = INVOCABLE_FUNCTIONS_URI.formatted(ncaId) + (query.isEmpty() ? "" : "?" + query);
        var request = RequestEntity.get(URI.create(uri))
                .header("Authorization", "Bearer " + token)
                .build();
        return testRestTemplate.exchange(request, ListInvocableFunctionsResponse.class);
    }
}
