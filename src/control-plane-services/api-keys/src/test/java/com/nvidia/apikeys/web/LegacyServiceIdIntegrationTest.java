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

package com.nvidia.apikeys.web;

import static com.nvidia.apikeys.TestData.API_KEY_1;
import static com.nvidia.apikeys.TestData.KEY_ID_1;
import static com.nvidia.apikeys.TestData.KEY_OWNER_VO_1;
import static com.nvidia.apikeys.TestData.KEY_VO_1;
import static com.nvidia.apikeys.TestData.SERVICE_ID_1;
import static com.nvidia.apikeys.TestData.TEST_TIME;
import static com.nvidia.apikeys.TestData.USER_KEY_OWNER_ID_1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.OK;

import com.nvidia.apikeys.App;
import com.nvidia.apikeys.config.IntegrationTestConfiguration;
import com.nvidia.apikeys.persistance.dao.KeysDao;
import com.nvidia.apikeys.utils.TestClock;
import com.nvidia.apikeys.vo.KeyVo;
import java.time.ZoneId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;

/**
 * Registers the service under its canonical ID. The legacy ID comes from the default
 * apikeys.legacy-service-ids entry. The key is written the way a pod from before the canonical
 * IDs would have written it.
 */
@ExtendWith(IntegrationTestConfiguration.TestCleanerExtension.class)
@AutoConfigureTestRestTemplate
@SpringBootTest(
        classes = {App.class, IntegrationTestConfiguration.class},
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.profiles.active=integrationtest",
                "apikeys.service-id-map.nvcf=cloud-functions",
                "apikeys.registrations=[{\"serviceId\":\"cloud-functions\","
                        + "\"serviceName\":\"test-service\","
                        + "\"audienceServiceIds\":[\"cloud-functions\"],"
                        + "\"maxApiKeysPerUser\":10,\"maxApiKeyTtlDays\":36524,"
                        + "\"maxAuthzSizeChars\":2048,\"minAuthzUpdateIntervalSeconds\":1}]"
        })
@ContextConfiguration(initializers = IntegrationTestConfiguration.Initializer.class)
class LegacyServiceIdIntegrationTest extends BaseIntegrationTest {

    private static final String CANONICAL_ID = "cloud-functions";

    @Autowired
    private KeysDao keysDao;

    @BeforeEach
    void storeKeyUnderLegacyIds() {
        TestClock.setBaseClock(TestClock.fixed(TEST_TIME, ZoneId.systemDefault()));
        KeyVo legacyKey = KEY_VO_1.toBuilder()
                .authorizations("{\"policies\":[{\"aud\":\"" + SERVICE_ID_1
                                        + "\",\"policy\":\"legacy-policy\"}]}")
                .build();
        keysDao.create(legacyKey, KEY_OWNER_VO_1);
        invalidateCaches();
    }

    @AfterEach
    void resetClock() {
        TestClock.resetToDefaults();
    }

    @Test
    void list_returnsLegacyStoredKeyForCanonicalAndLegacyIssuerId() {
        for (String issuerId : new String[] {CANONICAL_ID, SERVICE_ID_1}) {
            ResponseEntity<String> response = exchange("/v1/keys", HttpMethod.GET, issuerId, null);

            assertThat(response.getStatusCode()).isEqualTo(OK);
            assertThat(response.getBody()).contains(KEY_ID_1);
            assertThat(response.getBody()).contains(SERVICE_ID_1);
        }
    }

    @Test
    void get_returnsLegacyStoredKeyForCanonicalIssuerId() {
        ResponseEntity<String> response =
                exchange("/v1/keys/" + KEY_ID_1, HttpMethod.GET, CANONICAL_ID, null);

        assertThat(response.getStatusCode()).isEqualTo(OK);
        assertThat(response.getBody()).contains(KEY_ID_1);
    }

    @Test
    void delete_removesLegacyStoredKeyUsingCanonicalIssuerId() {
        ResponseEntity<String> deleted =
                exchange("/v1/keys/" + KEY_ID_1, HttpMethod.DELETE, CANONICAL_ID, null);
        assertThat(deleted.getStatusCode().is2xxSuccessful()).isTrue();

        ResponseEntity<String> list = exchange("/v1/keys", HttpMethod.GET, SERVICE_ID_1, null);
        assertThat(list.getBody()).doesNotContain(KEY_ID_1);
    }

    @Test
    void introspect_acceptsLegacyAndCanonicalAudienceForLegacyStoredKey() {
        for (String audience : new String[] {SERVICE_ID_1, CANONICAL_ID}) {
            invalidateCaches();
            ResponseEntity<String> response = introspect(audience);

            assertThat(response.getStatusCode()).isEqualTo(OK);
            assertThat(response.getBody()).contains(KEY_ID_1);
        }
    }

    @Test
    void introspect_rejectsOtherServiceAudience() {
        ResponseEntity<String> response = introspect("cloud-tasks");

        assertThat(response.getStatusCode()).isEqualTo(BAD_REQUEST);
    }

    @Test
    void authz_allowsLegacyStoredKeyInCanonicalNamespaceAndSelectsItsPolicy() {
        String requestBody = "{\"input\":{\"apiKey\":\"" + API_KEY_1 + "\"}}";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<String> response = restTemplate.exchange(
                "/v1/namespaces/{namespace}/evaluations/{policy-name}", HttpMethod.POST,
                new HttpEntity<>(requestBody, headers), String.class, "nvcf", "apikey.allow");

        assertThat(response.getStatusCode()).isEqualTo(OK);
        assertThat(response.getBody()).contains("\"allowed\":true");
        assertThat(response.getBody()).contains("legacy-policy");
    }

    @Test
    void services_resolvesCanonicalAndLegacyServiceId() {
        for (String serviceId : new String[] {CANONICAL_ID, SERVICE_ID_1}) {
            ResponseEntity<String> response = restTemplate.getForEntity(
                    "/v1/services/{id}", String.class, serviceId);

            assertThat(response.getStatusCode()).isEqualTo(OK);
            assertThat(response.getBody()).contains(CANONICAL_ID);
        }
    }

    private ResponseEntity<String> exchange(
            String path, HttpMethod method, String issuerId, String body) {
        HttpHeaders headers = addHeaderForServiceAndUser(issuerId, USER_KEY_OWNER_ID_1, null);
        return restTemplate.exchange(path, method, new HttpEntity<>(body, headers), String.class);
    }

    private ResponseEntity<String> introspect(String audience) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = "{\"key\":\"" + API_KEY_1 + "\",\"audience_service_id\":\"" + audience + "\"}";
        return restTemplate.exchange(
                "/v1/introspect", HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    }
}
