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

package com.nvidia.apikeys.services;

import java.util.Map;
import java.util.Set;
import com.nvidia.apikeys.vo.ServiceVo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static com.nvidia.apikeys.TestData.servicesService;
import static com.nvidia.apikeys.TestData.SERVICE_ID_1;
import static com.nvidia.apikeys.TestData.SERVICE_VO_1;
import static com.nvidia.apikeys.utils.TestUtils.assertThrowsExceptionWithDetails;
import static org.assertj.core.api.Assertions.assertThat;

import com.nvidia.apikeys.config.NakProperties;
import com.nvidia.boot.exceptions.NotFoundException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ServicesServiceTest {

    @Spy
    private NakProperties nakProperties = NakProperties.builder()
            .registrations(
                    """
                    [{"serviceId":"nvidia-cloud-functions-ncp-service-id-aketm",\
                    "serviceName":"test-service",\
                    "audienceServiceIds":["nvidia-cloud-functions-ncp-service-id-aketm"],\
                    "maxApiKeysPerUser":10,\
                    "maxApiKeyTtlDays":1,\
                    "maxAuthzSizeChars": 1024,\
                    "minAuthzUpdateIntervalSeconds":10}]""")
            .build();

    @InjectMocks
    private ServicesService service;


    @Test
    void loadById_returnsIfPresent() {
        assertThat(service.loadById(SERVICE_ID_1)).isEqualTo(SERVICE_VO_1);
    }

    @Test
    void loadById_throwsIfNotPresent() {
        assertThrowsExceptionWithDetails(
                NotFoundException.class,
                () -> service.loadById("not-existing"),
                "Service 'not-existing' not found");
    }

    @Test
    void listServices() {
        assertThat(service.listServices()).isEqualTo(List.of(SERVICE_VO_1));
    }


    private static final String CANONICAL_ID = "cloud-functions";
    private static final String OTHER_CANONICAL_ID = "cloud-tasks";
    private static final String OTHER_LEGACY_ID = "custom-legacy-tasks-id";

    private static ServicesService aliasedServices() {
        return servicesService(
                SERVICE_VO_1.toBuilder()
                        .serviceId(CANONICAL_ID)
                        .legacyServiceIds(Set.of(SERVICE_ID_1))
                        .audienceServiceIds(Set.of(CANONICAL_ID))
                        .build(),
                SERVICE_VO_1.toBuilder()
                        .serviceId(OTHER_CANONICAL_ID)
                        .legacyServiceIds(Set.of(OTHER_LEGACY_ID))
                        .audienceServiceIds(Set.of(OTHER_CANONICAL_ID))
                        .build());
    }

    @Test
    void getServiceById_findsByCanonicalAndLegacyId() {
        ServicesService aliased = aliasedServices();

        assertThat(aliased.loadById(CANONICAL_ID).getServiceId()).isEqualTo(CANONICAL_ID);
        assertThat(aliased.loadById(SERVICE_ID_1).getServiceId()).isEqualTo(CANONICAL_ID);
        assertThat(aliased.loadById(OTHER_LEGACY_ID).getServiceId()).isEqualTo(OTHER_CANONICAL_ID);
    }

    @Test
    void getServiceById_unknownIdIsNotFound() {
        assertThat(aliasedServices().getServiceById("not-existing")).isEmpty();
    }

    @Test
    void registrationWithoutAliasesBehavesAsBefore() {
        assertThat(service.getServiceById(SERVICE_ID_1)).contains(SERVICE_VO_1);
        assertThat(service.toCanonicalId(SERVICE_ID_1)).isEqualTo(SERVICE_ID_1);
        assertThat(service.storedServiceIds(SERVICE_ID_1)).containsExactly(SERVICE_ID_1);
    }

    @Test
    void toCanonicalId_mapsLegacyIdAndKeepsOthers() {
        ServicesService aliased = aliasedServices();

        assertThat(aliased.toCanonicalId(SERVICE_ID_1)).isEqualTo(CANONICAL_ID);
        assertThat(aliased.toCanonicalId(OTHER_LEGACY_ID)).isEqualTo(OTHER_CANONICAL_ID);
        assertThat(aliased.toCanonicalId(CANONICAL_ID)).isEqualTo(CANONICAL_ID);
        assertThat(aliased.toCanonicalId("unknown")).isEqualTo("unknown");
        assertThat(aliased.toCanonicalId(null)).isNull();
    }

    @Test
    void storedServiceIds_returnsCanonicalThenLegacyForEitherForm() {
        ServicesService aliased = aliasedServices();

        assertThat(aliased.storedServiceIds(CANONICAL_ID))
                .containsExactly(CANONICAL_ID, SERVICE_ID_1);
        assertThat(aliased.storedServiceIds(SERVICE_ID_1))
                .containsExactly(CANONICAL_ID, SERVICE_ID_1);
        assertThat(aliased.storedServiceIds("unknown")).containsExactly("unknown");
    }

    @Test
    void constructor_rejectsLegacyIdUsedByTwoServices() {
        ServiceVo first = SERVICE_VO_1.toBuilder()
                .serviceId(CANONICAL_ID).legacyServiceIds(Set.of(SERVICE_ID_1)).build();
        ServiceVo second = SERVICE_VO_1.toBuilder()
                .serviceId(OTHER_CANONICAL_ID).legacyServiceIds(Set.of(SERVICE_ID_1)).build();

        assertThrows(IllegalStateException.class, () -> servicesService(first, second));
    }

    @Test
    void constructor_rejectsLegacyIdEqualToRegisteredId() {
        ServiceVo first = SERVICE_VO_1.toBuilder()
                .serviceId(CANONICAL_ID).legacyServiceIds(Set.of(OTHER_CANONICAL_ID)).build();
        ServiceVo second = SERVICE_VO_1.toBuilder().serviceId(OTHER_CANONICAL_ID).build();

        assertThrows(IllegalStateException.class, () -> servicesService(first, second));
    }

    @Test
    void configuredLegacyId_becomesAliasOfRegistrationWithoutAliases() {
        ServicesService configured = servicesService(
                Map.of(CANONICAL_ID, SERVICE_ID_1),
                SERVICE_VO_1.toBuilder().serviceId(CANONICAL_ID).build());

        assertThat(configured.loadById(SERVICE_ID_1).getServiceId()).isEqualTo(CANONICAL_ID);
        assertThat(configured.storedServiceIds(CANONICAL_ID))
                .containsExactly(CANONICAL_ID, SERVICE_ID_1);
    }

    @Test
    void configuredLegacyId_overridesDefaultWhenOperatorSetsIt() {
        ServicesService configured = servicesService(
                Map.of(CANONICAL_ID, "operator-chosen-id"),
                SERVICE_VO_1.toBuilder().serviceId(CANONICAL_ID).build());

        assertThat(configured.loadById("operator-chosen-id").getServiceId())
                .isEqualTo(CANONICAL_ID);
        assertThat(configured.getServiceById(SERVICE_ID_1)).isEmpty();
    }

    @Test
    void configuredLegacyId_isMergedWithAliasFromRegistration() {
        ServicesService configured = servicesService(
                Map.of(CANONICAL_ID, "operator-chosen-id"),
                SERVICE_VO_1.toBuilder()
                        .serviceId(CANONICAL_ID)
                        .legacyServiceIds(Set.of(SERVICE_ID_1))
                        .build());

        assertThat(configured.storedServiceIds(CANONICAL_ID))
                .containsExactlyInAnyOrder(CANONICAL_ID, SERVICE_ID_1, "operator-chosen-id");
    }

    @Test
    void configuredLegacyId_sameAsRegistrationAliasIsNotDuplicated() {
        ServicesService configured = servicesService(
                Map.of(CANONICAL_ID, SERVICE_ID_1),
                SERVICE_VO_1.toBuilder()
                        .serviceId(CANONICAL_ID)
                        .legacyServiceIds(Set.of(SERVICE_ID_1))
                        .build());

        assertThat(configured.storedServiceIds(CANONICAL_ID))
                .containsExactly(CANONICAL_ID, SERVICE_ID_1);
    }

    @Test
    void configuredLegacyId_forUnregisteredServiceIsIgnored() {
        ServicesService configured = servicesService(
                Map.of(OTHER_CANONICAL_ID, OTHER_LEGACY_ID),
                SERVICE_VO_1);

        assertThat(configured.getServiceById(OTHER_LEGACY_ID)).isEmpty();
        assertThat(configured.storedServiceIds(SERVICE_ID_1)).containsExactly(SERVICE_ID_1);
    }
}
