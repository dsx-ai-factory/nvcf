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
import static com.nvidia.nvcf.service.azp.AuthorizedPartiesService.AUTHORIZED_WILDCARD_ACCOUNT;
import static com.nvidia.nvcf.util.MockApiKeysServer.resetToDefault;
import static com.nvidia.nvcf.util.MockApiKeysServer.setResponse;
import static com.nvidia.nvcf.util.NvcfConstants.SCOPE_LIST_FUNCTIONS_DETAILS;
import static com.nvidia.nvcf.util.TestConstants.SCOPE_DEPLOY_FUNCTION;
import static com.nvidia.nvcf.util.TestConstants.SCOPE_INVOKE_FUNCTION;
import static com.nvidia.nvcf.util.TestConstants.SCOPE_LIST_FUNCTIONS;
import static com.nvidia.nvcf.util.TestConstants.TEST_CLIENT_SUBJECT;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_ID_3;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_NAME;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_NAME_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_FUNCTION_NAME_3;
import static com.nvidia.nvcf.util.TestConstants.TEST_NCA_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_NCA_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_OWNER_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_PUBLIC_FUNCTION_ID_1;
import static com.nvidia.nvcf.util.TestConstants.TEST_PUBLIC_FUNCTION_NAME_V1;
import static com.nvidia.nvcf.util.TestConstants.TEST_PUBLIC_FUNCTION_NAME_V2;
import static com.nvidia.nvcf.util.TestConstants.TEST_PUBLIC_FUNCTION_NCA_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_PUBLIC_FUNCTION_VERSION_ID_1;
import static com.nvidia.nvcf.util.TestConstants.TEST_PUBLIC_FUNCTION_VERSION_ID_2;
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
import com.nvidia.nvcf.service.apikeys.ApiKeyValidationResult.Resource;
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
import org.junit.jupiter.params.provider.ValueSource;
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
class FunctionInvocationControllerTest {

    private static final String INVOCABLE_FUNCTIONS_URI =
            "/v2/nvcf/invocable/functions/versions/ids";
    private static final String API_KEY = "nvapi-stg-some-key";

    private static final List<InvocableFunctionDto> OWN = List.of(
            new InvocableFunctionDto(TEST_FUNCTION_ID, TEST_VERSION_ID_1),
            new InvocableFunctionDto(TEST_FUNCTION_ID, TEST_VERSION_ID_3),
            new InvocableFunctionDto(TEST_FUNCTION_ID_3, TEST_VERSION_ID_4));
    private static final List<InvocableFunctionDto> SHARED = List.of(
            new InvocableFunctionDto(TEST_FUNCTION_ID_2, TEST_VERSION_ID_2));
    private static final List<InvocableFunctionDto> PUBLIC = List.of(
            new InvocableFunctionDto(TEST_PUBLIC_FUNCTION_ID_1, TEST_PUBLIC_FUNCTION_VERSION_ID_1),
            new InvocableFunctionDto(TEST_PUBLIC_FUNCTION_ID_1, TEST_PUBLIC_FUNCTION_VERSION_ID_2));
    private static final List<InvocableFunctionDto> OWN_AND_SHARED =
            Stream.concat(OWN.stream(), SHARED.stream()).toList();
    private static final List<InvocableFunctionDto> ALL =
            Stream.of(OWN, SHARED, PUBLIC).flatMap(List::stream).toList();

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

    // TEST_NCA_ID owns three function versions. TEST_NCA_ID_2 shares one of its two versions
    // with it, and another account makes both versions of a function public.
    @BeforeEach
    void beforeEach() {
        testAccountService.createAccountAndAssociateClients(TEST_PUBLIC_FUNCTION_NCA_ID, null);
        testAuthPartiesService.createTestFunction(TEST_PUBLIC_FUNCTION_ID_1,
                                                  TEST_PUBLIC_FUNCTION_VERSION_ID_1,
                                                  TEST_PUBLIC_FUNCTION_NCA_ID,
                                                  TEST_PUBLIC_FUNCTION_NAME_V1);
        testAuthPartiesService.createTestFunction(TEST_PUBLIC_FUNCTION_ID_1,
                                                  TEST_PUBLIC_FUNCTION_VERSION_ID_2,
                                                  TEST_PUBLIC_FUNCTION_NCA_ID,
                                                  TEST_PUBLIC_FUNCTION_NAME_V2);
        testAuthPartiesService.associateAuthParties(
                TEST_PUBLIC_FUNCTION_NCA_ID, TEST_PUBLIC_FUNCTION_ID_1, Optional.empty(),
                Set.of(AuthorizedPartyDto.builder().ncaId(AUTHORIZED_WILDCARD_ACCOUNT).build()));

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
        testAccountService.deleteAccount(TEST_PUBLIC_FUNCTION_NCA_ID);
    }

    // Only the list_functions scope is accepted: the response has no function details, and
    // listing is not invocation.
    Stream<Arguments> scopeArgs() {
        return Stream.of(
                Arguments.of(List.of(SCOPE_LIST_FUNCTIONS), HttpStatus.OK),
                Arguments.of(List.of(SCOPE_LIST_FUNCTIONS_DETAILS), HttpStatus.FORBIDDEN),
                Arguments.of(List.of(SCOPE_INVOKE_FUNCTION), HttpStatus.FORBIDDEN),
                Arguments.of(List.of(SCOPE_DEPLOY_FUNCTION), HttpStatus.FORBIDDEN));
    }

    @ParameterizedTest
    @MethodSource("scopeArgs")
    void shouldAcceptOnlyListFunctionsScopeForApiKeys(
            List<String> scopes,
            HttpStatus expectedStatus) {
        setResponse(TEST_NCA_ID, TEST_OWNER_ID,
                    List.of(new Resource("account-functions", "*"),
                            new Resource("authorized-functions", "*")),
                    scopes);

        var response = listInvocableFunctions(API_KEY, "visibility=private,authorized");

        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        if (expectedStatus.isError()) {
            return;
        }
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(OWN_AND_SHARED);
    }

    @ParameterizedTest
    @MethodSource("scopeArgs")
    void shouldAcceptOnlyListFunctionsScopeForTokens(
            List<String> scopes,
            HttpStatus expectedStatus) {
        var token = MOCK_OAUTH2_TOKEN_SERVER.getJwt(TEST_CLIENT_SUBJECT, scopes, 100);

        var response = listInvocableFunctions(token, "visibility=private,authorized");

        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        if (expectedStatus.isError()) {
            return;
        }
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(OWN_AND_SHARED);
    }

    // An API key's resources narrow the result, as they do for invocation.
    @Test
    void shouldListOnlyTheVersionsTheApiKeyResourcesAllow() {
        setResponse(TEST_NCA_ID, TEST_OWNER_ID,
                    List.of(new Resource("function", TEST_FUNCTION_ID + "/" + TEST_VERSION_ID_1),
                            new Resource("function", TEST_FUNCTION_ID_2 + "/*")),
                    List.of(SCOPE_LIST_FUNCTIONS));

        var response = listInvocableFunctions(API_KEY, "visibility=private,authorized");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions()).containsExactlyInAnyOrder(
                new InvocableFunctionDto(TEST_FUNCTION_ID, TEST_VERSION_ID_1),
                new InvocableFunctionDto(TEST_FUNCTION_ID_2, TEST_VERSION_ID_2));
    }

    Stream<Arguments> visibilityArgs() {
        return Stream.of(
                Arguments.of("visibility=private", OWN),
                Arguments.of("visibility=authorized", SHARED),
                Arguments.of("visibility=public", PUBLIC),
                Arguments.of("visibility=private,authorized", OWN_AND_SHARED),
                Arguments.of("visibility=private&visibility=authorized", OWN_AND_SHARED),
                Arguments.of("visibility=private,AUTHORIZED", OWN_AND_SHARED),
                Arguments.of("visibility=", ALL),
                Arguments.of("", ALL));
    }

    // Each visibility includes only its kind of function, and the result has exactly the
    // versions that the function listing returns for the same query.
    @ParameterizedTest
    @MethodSource("visibilityArgs")
    void shouldFilterByVisibilityLikeTheFunctionListing(
            String query,
            List<InvocableFunctionDto> expected) {
        var token = MOCK_OAUTH2_TOKEN_SERVER.getJwt(TEST_CLIENT_SUBJECT,
                                                    List.of(SCOPE_LIST_FUNCTIONS), 100);

        var response = listInvocableFunctions(token, query);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions()).containsExactlyInAnyOrderElementsOf(expected);

        var request = RequestEntity.get(URI.create("/v2/nvcf/functions?" + query))
                .header("Authorization", "Bearer " + token)
                .build();
        var listing = testRestTemplate.exchange(request, ListFunctionsResponse.class);
        assertThat(listing.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(listing.getBody()).isNotNull();
        assertThat(response.getBody().functions()).containsExactlyInAnyOrderElementsOf(
                listing.getBody().functions().stream()
                        .map(dto -> new InvocableFunctionDto(dto.id(), dto.versionId()))
                        .toList());
    }

    @ParameterizedTest
    @ValueSource(strings = {"visibility=invalid", "visibility=private,invalid"})
    void shouldRejectAnInvalidVisibility(String query) {
        var token = MOCK_OAUTH2_TOKEN_SERVER.getJwt(TEST_CLIENT_SUBJECT,
                                                    List.of(SCOPE_LIST_FUNCTIONS), 100);

        var response = listInvocableFunctions(token, query);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private ResponseEntity<ListInvocableFunctionsResponse> listInvocableFunctions(
            String token,
            String query) {
        var uri = query.isEmpty() ? INVOCABLE_FUNCTIONS_URI : INVOCABLE_FUNCTIONS_URI + "?" + query;
        var request = RequestEntity.get(URI.create(uri))
                .header("Authorization", "Bearer " + token)
                .build();
        return testRestTemplate.exchange(request, ListInvocableFunctionsResponse.class);
    }
}
