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
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.ACTIVE;
import static com.nvidia.nvcf.persistence.function.entity.FunctionStatus.DEGRADING;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.ALL;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.OWN;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.OWN_AND_SHARED;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.PUBLIC;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.SHARED;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.SHARED_DEGRADED_VERSION_ID;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.SHARED_FUNCTION_ID;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.SHARED_INACTIVE_VERSION_ID;
import static com.nvidia.nvcf.rest.function.invocation.TestInvocableFunctionsService.concat;
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
import static com.nvidia.nvcf.util.TestConstants.TEST_NCA_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_OWNER_ID;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_1;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_2;
import static com.nvidia.nvcf.util.TestConstants.TEST_VERSION_ID_5;
import static org.assertj.core.api.Assertions.assertThat;

import com.nvidia.boot.mock.ngc.MockCasServer;
import com.nvidia.boot.mock.ngc.MockNgcContainerRegistryServer;
import com.nvidia.nvcf.IntegrationTestConfiguration;
import com.nvidia.nvcf.NvcfTestApp;
import com.nvidia.nvcf.persistence.function.entity.FunctionStatus;
import com.nvidia.nvcf.rest.account.TestAccountService;
import com.nvidia.nvcf.rest.function.invocation.dto.InvocableFunctionDto;
import com.nvidia.nvcf.rest.function.invocation.dto.ListInvocableFunctionsResponse;
import com.nvidia.nvcf.rest.function.management.dto.FunctionStatusEnum;
import com.nvidia.nvcf.rest.function.management.dto.ListFunctionsResponse;
import com.nvidia.nvcf.service.apikeys.ApiKeyValidationResult.Resource;
import com.nvidia.nvcf.service.common.TestCommonService;
import com.nvidia.nvcf.util.MockApiKeysServer;
import com.nvidia.nvcf.util.MockEssServer;
import jakarta.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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
import org.junit.jupiter.params.provider.EnumSource;
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
    private static final String FUNCTIONS_URI = "/v2/nvcf/functions";
    private static final String API_KEY = "nvapi-stg-some-key";
    private static final List<Resource> ALL_RESOURCES = List.of(
            new Resource("account-functions", "*"),
            new Resource("authorized-functions", "*"));

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private TestCommonService testCommonService;

    @Autowired
    private TestAccountService testAccountService;

    @Autowired
    private TestInvocableFunctionsService testInvocableFunctionsService;

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
        testInvocableFunctionsService.createFunctionVersions();
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
        testInvocableFunctionsService.deleteAccounts();
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
        setResponse(TEST_NCA_ID, TEST_OWNER_ID, ALL_RESOURCES, scopes);

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
        var response = listInvocableFunctions(jwt(scopes), "visibility=private,authorized");

        assertThat(response.getStatusCode()).isEqualTo(expectedStatus);
        if (expectedStatus.isError()) {
            return;
        }
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(OWN_AND_SHARED);
    }

    @Test
    void shouldRejectARequestWithoutABearerToken() {
        var response = listInvocableFunctions(null, "");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    // A version is listed only in a status that accepts invocations, whether the account owns
    // it, it is shared with the account, or it is public, and whatever the bearer token.
    @ParameterizedTest
    @EnumSource(FunctionStatus.class)
    void shouldListAVersionOnlyInAnInvocableStatus(FunctionStatus status) {
        var versions = testInvocableFunctionsService.createFunctionVersionsInStatus(status);
        var expected = status == ACTIVE || status == DEGRADING ? concat(ALL, versions) : ALL;
        setResponse(TEST_NCA_ID, TEST_OWNER_ID, ALL_RESOURCES, List.of(SCOPE_LIST_FUNCTIONS));

        for (var bearer : List.of(jwt(List.of(SCOPE_LIST_FUNCTIONS)), API_KEY)) {
            var response = listInvocableFunctions(bearer, "");

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().functions())
                    .as("%s versions listed with %s", status,
                        API_KEY.equals(bearer) ? "an API key" : "a JWT")
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    // An API key's resources limit the versions that the account owns or that are shared with
    // it, as they do for invocation, and a version that they allow is still listed only in an
    // invocable status.
    Stream<Arguments> resourceArgs() {
        return Stream.of(
                Arguments.of(List.of(new Resource("account-functions", "*")), OWN),
                Arguments.of(List.of(new Resource("authorized-functions", "*")), SHARED),
                Arguments.of(List.of(function(TEST_FUNCTION_ID, TEST_VERSION_ID_1),
                                     function(TEST_FUNCTION_ID_2, null)),
                             List.of(new InvocableFunctionDto(TEST_FUNCTION_ID, TEST_VERSION_ID_1),
                                     new InvocableFunctionDto(TEST_FUNCTION_ID_2,
                                                              TEST_VERSION_ID_2))),
                // An ACTIVE version that is not shared with the account.
                Arguments.of(List.of(function(TEST_FUNCTION_ID_2, TEST_VERSION_ID_5)), List.of()),
                // Own and shared versions in statuses that do not accept invocations.
                Arguments.of(List.of(function(TEST_FUNCTION_ID_3, null),
                                     function(TEST_FUNCTION_ID_2, SHARED_DEGRADED_VERSION_ID),
                                     function(SHARED_FUNCTION_ID, SHARED_INACTIVE_VERSION_ID)),
                             List.of()));
    }

    @ParameterizedTest
    @MethodSource("resourceArgs")
    void shouldListOnlyTheVersionsThatTheApiKeyResourcesAllow(
            List<Resource> resources,
            List<InvocableFunctionDto> expected) {
        setResponse(TEST_NCA_ID, TEST_OWNER_ID, resources, List.of(SCOPE_LIST_FUNCTIONS));
        var query = "visibility=private,authorized";

        var response = listInvocableFunctions(API_KEY, query);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(expected)
                .containsExactlyInAnyOrderElementsOf(invocableVersionsOfListing(API_KEY, query));
    }

    // Public versions are not limited by an API key's resources, as for invocation.
    @Test
    void shouldListPublicVersionsWhateverTheApiKeyResources() {
        setResponse(TEST_NCA_ID, TEST_OWNER_ID,
                    List.of(function(TEST_FUNCTION_ID, TEST_VERSION_ID_1)),
                    List.of(SCOPE_LIST_FUNCTIONS));
        var allowed = List.of(new InvocableFunctionDto(TEST_FUNCTION_ID, TEST_VERSION_ID_1));

        var response = listInvocableFunctions(API_KEY, "");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(concat(allowed, PUBLIC))
                .containsExactlyInAnyOrderElementsOf(invocableVersionsOfListing(API_KEY, ""));
    }

    // A version that the account owns and makes public is listed once.
    @Test
    void shouldListAVersionOnce() {
        testInvocableFunctionsService.share(TEST_NCA_ID, TEST_FUNCTION_ID, Optional.empty(),
                                            AUTHORIZED_WILDCARD_ACCOUNT);

        var response = listInvocableFunctions(jwt(List.of(SCOPE_LIST_FUNCTIONS)), "");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions()).containsExactlyInAnyOrderElementsOf(ALL);
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
    // invocable versions of the function listing for the same query.
    @ParameterizedTest
    @MethodSource("visibilityArgs")
    void shouldListTheInvocableVersionsOfTheFunctionListing(
            String query,
            List<InvocableFunctionDto> expected) {
        var token = jwt(List.of(SCOPE_LIST_FUNCTIONS));

        var response = listInvocableFunctions(token, query);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().functions())
                .containsExactlyInAnyOrderElementsOf(expected)
                .containsExactlyInAnyOrderElementsOf(invocableVersionsOfListing(token, query));
    }

    @ParameterizedTest
    @ValueSource(strings = {"visibility=invalid", "visibility=private,invalid"})
    void shouldRejectAnInvalidVisibility(String query) {
        var response = listInvocableFunctions(jwt(List.of(SCOPE_LIST_FUNCTIONS)), query);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private static String jwt(List<String> scopes) {
        return MOCK_OAUTH2_TOKEN_SERVER.getJwt(TEST_CLIENT_SUBJECT, scopes, 100);
    }

    private static Resource function(UUID id, @Nullable UUID versionId) {
        return new Resource("function", id + "/" + (versionId == null ? "*" : versionId));
    }

    private ResponseEntity<ListInvocableFunctionsResponse> listInvocableFunctions(
            @Nullable String bearer,
            String query) {
        return get(withQuery(INVOCABLE_FUNCTIONS_URI, query), bearer,
                   ListInvocableFunctionsResponse.class);
    }

    // The function and version ids of the versions in an invocable status in the function
    // listing.
    private List<InvocableFunctionDto> invocableVersionsOfListing(String bearer, String query) {
        var response = get(withQuery(FUNCTIONS_URI, query), bearer, ListFunctionsResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        return response.getBody().functions().stream()
                .filter(function -> function.status() == FunctionStatusEnum.ACTIVE
                        || function.status() == FunctionStatusEnum.DEGRADING)
                .map(function -> new InvocableFunctionDto(function.id(), function.versionId()))
                .toList();
    }

    private <T> ResponseEntity<T> get(String uri, @Nullable String bearer, Class<T> type) {
        var builder = RequestEntity.get(URI.create(uri));
        if (bearer != null) {
            builder = builder.header("Authorization", "Bearer " + bearer);
        }
        return testRestTemplate.exchange(builder.build(), type);
    }

    private static String withQuery(String uri, String query) {
        return query.isEmpty() ? uri : uri + "?" + query;
    }
}
