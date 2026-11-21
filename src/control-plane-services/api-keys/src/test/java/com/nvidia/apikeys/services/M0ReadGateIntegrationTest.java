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

import static com.nvidia.apikeys.TestData.CREATE_KEY_REQUEST_1;
import static com.nvidia.apikeys.TestData.KEY_OWNER_VO_1;
import static com.nvidia.apikeys.TestData.KEY_VO_1;
import static com.nvidia.apikeys.TestData.SERVICE_ID_1;
import static com.nvidia.apikeys.TestData.TEST_TIME;
import static com.nvidia.apikeys.scheduler.KeysByAccountIndexMigrationTask.MARKER_RECORD;
import static com.nvidia.apikeys.scheduler.KeysByAccountIndexMigrationTask.MARKER_TABLE;
import static com.nvidia.apikeys.utils.TestUtils.assertThrowsExceptionWithDetails;
import static com.nvidia.apikeys.vo.KeyOwnerStatus.ACTIVE;
import static com.nvidia.apikeys.vo.KeyOwnerStatus.SUSPENDED;
import static com.nvidia.apikeys.vo.KeyStatus.EXPIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.nvidia.apikeys.App;
import com.nvidia.apikeys.caching.IntrospectionKeyOwnerValidator;
import com.nvidia.apikeys.config.IntegrationTestConfiguration;
import com.nvidia.apikeys.config.NakProperties;
import com.nvidia.apikeys.dto.keys.UpdateKeyOwnerStatusRequest;
import com.nvidia.apikeys.facade.AdminFacade;
import com.nvidia.apikeys.persistance.dao.KeyByOwnerAndServiceModelConverter;
import com.nvidia.apikeys.persistance.dao.KeysDao;
import com.nvidia.apikeys.persistance.dao.RowUpdateLockDao;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel;
import com.nvidia.apikeys.persistance.repositories.KeyByOwnerAndAccountAndServiceRepository;
import com.nvidia.apikeys.validators.KeysOperationByOwnerValidator;
import com.nvidia.apikeys.vo.KeyByOwnerAndAccountAndServiceVo;
import com.nvidia.apikeys.vo.KeyByOwnerAndServiceVo;
import com.nvidia.apikeys.utils.TestClock;
import com.nvidia.boot.exceptions.BadRequestException;
import com.nvidia.boot.exceptions.NotFoundException;
import com.nvidia.boot.jwt.services.mapping.EncryptedModelConverter;
import java.time.ZoneId;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.test.context.ContextConfiguration;

@ExtendWith(IntegrationTestConfiguration.TestCleanerExtension.class)
@AutoConfigureTestRestTemplate
@SpringBootTest(classes = App.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.active=integrationtest")
@ContextConfiguration(initializers = IntegrationTestConfiguration.Initializer.class)
class M0ReadGateIntegrationTest {

    @Autowired
    private KeysDao keysDao;
    @Autowired
    private ValidatingKeyLoader keyLoader;
    @Autowired
    private KeysOperationByOwnerValidator validator;
    @Autowired
    private ServicesService services;
    @Autowired
    private AdminFacade adminFacade;
    @Autowired
    private IntrospectionKeyOwnerValidator ownerValidator;
    @Autowired
    private RowUpdateLockDao markers;
    @Autowired
    private NakProperties properties;
    @Autowired
    private CqlSession session;
    @Autowired
    private CassandraTemplate template;
    @Autowired
    private KeyByOwnerAndServiceModelConverter ownerConverter;
    @Autowired
    private KeyByOwnerAndAccountAndServiceRepository accountRepository;
    @Autowired
    private EncryptedModelConverter<KeyByOwnerAndAccountAndServiceModel,
            KeyByOwnerAndAccountAndServiceVo> accountConverter;

    @BeforeEach
    void freezeClock() {
        TestClock.setBaseClock(TestClock.fixed(TEST_TIME, ZoneId.systemDefault()));
        ownerValidator.invalidateCaches();
    }

    @AfterEach
    void resetClock() {
        ownerValidator.invalidateCaches();
        TestClock.resetToDefaults();
    }

    @Test
    void listGetAndCountSwitchBothWaysWithoutRestart() {
        keysDao.create(KEY_VO_1, KEY_OWNER_VO_1);
        var legacy = new KeyByOwnerAndServiceVo(KEY_VO_1, KEY_OWNER_VO_1);
        template.insert(ownerConverter.voToModel(legacy.toBuilder()
                .keyId("legacy-only").keyHash("legacy-only-hash").build()));
        var account = new KeyByOwnerAndAccountAndServiceVo(legacy, properties.getNcaId())
                .toBuilder().description("account description").build();
        template.insert(accountConverter.voToModel(account));
        var service = services.loadById(SERVICE_ID_1).toBuilder().maxApiKeysPerUser(2).build();

        for (var enabled : new boolean[] {false, true, false}) {
            setMarker(enabled);
            var expectedCount = enabled ? 1 : 2;
            assertThat(keysDao.list(KEY_VO_1.getOwnerType(), KEY_VO_1.getOwnerId(), SERVICE_ID_1))
                    .hasSize(expectedCount);
            assertThat(keysDao.list(KEY_VO_1.getOwnerType(), KEY_VO_1.getOwnerId(),
                    service.serviceIdAliasGroup())).hasSize(expectedCount);
            assertThat(adminFacade.listKeysInAllServices(KEY_VO_1.getOwnerType(), KEY_VO_1.getOwnerId())
                    .getKeys()).hasSize(expectedCount);
            assertThat(keyLoader.loadKeyByOwnerAndServiceVo(KEY_OWNER_VO_1, SERVICE_ID_1,
                    KEY_VO_1.getKeyId()).getDescription())
                    .isEqualTo(enabled ? "account description" : KEY_VO_1.getDescription());
            if (enabled) {
                assertThat(validator.validateCreateKeyRequest(service, KEY_OWNER_VO_1, CREATE_KEY_REQUEST_1))
                        .isNotNull();
            } else {
                assertThrowsExceptionWithDetails(BadRequestException.class,
                        () -> validator.validateCreateKeyRequest(service, KEY_OWNER_VO_1, CREATE_KEY_REQUEST_1),
                        KeysOperationByOwnerValidator.REACHED_MAXIMUM_KEYS_IN_SERVICE);
            }
        }
    }

    @Test
    void managementDoesNotFindKeysFromAnotherAccountOrFallBackToLegacy() {
        var foreignKey = KEY_VO_1.toBuilder().ncaId("another-account").build();
        keysDao.create(foreignKey, KEY_OWNER_VO_1);
        assertThat(keysDao.list(foreignKey.getOwnerType(), foreignKey.getOwnerId())).hasSize(1);

        setMarker(true);

        assertThat(keysDao.list(foreignKey.getOwnerType(), foreignKey.getOwnerId())).isEmpty();
        assertThat(keysDao.list(foreignKey.getOwnerType(), foreignKey.getOwnerId(), SERVICE_ID_1)).isEmpty();
        assertThatThrownBy(() -> keyLoader.loadKeyVo(KEY_OWNER_VO_1, SERVICE_ID_1, foreignKey.getKeyId()))
                .isInstanceOf(NotFoundException.class);
        assertThatThrownBy(() -> validator.validateDeleteKeyRequest(
                services.loadById(SERVICE_ID_1), KEY_OWNER_VO_1, foreignKey.getKeyId()))
                .isInstanceOf(NotFoundException.class);
        assertThat(keyLoader.loadKeyByHash(foreignKey.getKeyHash()).getNcaId()).isEqualTo("another-account");

        setMarker(false);
        assertThat(keyLoader.loadKeyByOwnerAndServiceVo(KEY_OWNER_VO_1, SERVICE_ID_1, foreignKey.getKeyId())
                .getNcaId()).isEqualTo("another-account");
    }

    @Test
    void userStatusAndIntrospectionStayOnLegacyStorageAcrossMarkerChanges() {
        keysDao.create(KEY_VO_1, KEY_OWNER_VO_1);
        accountRepository.findByNcaIdAndOwnerTypeAndOwnerId(properties.getNcaId(),
                KEY_VO_1.getOwnerType(), KEY_VO_1.getOwnerId()).forEach(template::delete);

        for (var enabled : new boolean[] {false, true, false}) {
            setMarker(enabled);
            adminFacade.updateKeyOwnerStatus(KEY_VO_1.getOwnerType(), KEY_VO_1.getOwnerId(),
                    UpdateKeyOwnerStatusRequest.builder().status(SUSPENDED).build());
            assertThat(adminFacade.getKeyOwner(KEY_VO_1.getOwnerType(), KEY_VO_1.getOwnerId()).getStatus())
                    .isEqualTo(SUSPENDED);
            var key = keyLoader.loadKeyByHash(KEY_VO_1.getKeyHash());
            ownerValidator.invalidateCaches();
            assertThatThrownBy(() -> ownerValidator.assertKeyOwnerValid(key))
                    .isInstanceOf(NotFoundException.class);

            adminFacade.updateKeyOwnerStatus(KEY_VO_1.getOwnerType(), KEY_VO_1.getOwnerId(),
                    UpdateKeyOwnerStatusRequest.builder().status(ACTIVE).build());
            ownerValidator.invalidateCaches();
            ownerValidator.assertKeyOwnerValid(keyLoader.loadKeyByHash(KEY_VO_1.getKeyHash()));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void adminListsAndDeletesUnknownIssuersInTheSelectedTable(boolean enabled) {
        var unknown = KEY_VO_1.toBuilder().issuerServiceId("unregistered-issuer")
                .audienceServiceIds(Set.of("unregistered-audience")).build();
        keysDao.create(unknown, KEY_OWNER_VO_1);
        var foreignKey = unknown.toBuilder().keyId("foreign-key").keyHash("foreign-hash")
                .ncaId("another-account").build();
        keysDao.create(foreignKey, KEY_OWNER_VO_1);
        setMarker(enabled);

        assertThat(adminFacade.listKeysInAllServices(unknown.getOwnerType(), unknown.getOwnerId()).getKeys())
                .hasSize(enabled ? 1 : 2)
                .anySatisfy(key -> assertThat(key.getIssuerServiceId()).isEqualTo("unregistered-issuer"));
        adminFacade.deleteUserKeys(unknown.getOwnerType(), unknown.getOwnerId());

        assertThat(keysDao.getKeyByHash(unknown.getKeyHash())).isEmpty();
        assertThat(keysDao.getKeyByHash(foreignKey.getKeyHash()).isPresent()).isEqualTo(enabled);
        assertThat(accountRepository.findByNcaIdAndOwnerTypeAndOwnerId(properties.getNcaId(),
                unknown.getOwnerType(), unknown.getOwnerId())).isEmpty();
        setMarker(false);
        assertThat(adminFacade.listKeysInAllServices(unknown.getOwnerType(), unknown.getOwnerId()).getKeys())
                .hasSize(enabled ? 1 : 0);
    }

    @Test
    void accountReadsPreserveExpiredStatusAndEncryptedDetails() {
        var expired = KEY_VO_1.toBuilder().expiresAt(TEST_TIME.minusSeconds(1))
                .ncaId(properties.getNcaId()).ncaIdInDetails(properties.getNcaId()).build();
        keysDao.create(expired, KEY_OWNER_VO_1);
        setMarker(true);

        var key = keyLoader.loadKeyByOwnerAndServiceVo(KEY_OWNER_VO_1, SERVICE_ID_1, expired.getKeyId());

        assertThat(key).isEqualTo(new KeyByOwnerAndServiceVo(expired, KEY_OWNER_VO_1).toBuilder()
                .keyStatus(EXPIRED).ownerStatus(null).ownerStatusUpdatedAt(null).build());
    }

    private void setMarker(boolean enabled) {
        if (enabled) {
            markers.markCompleted(MARKER_TABLE, MARKER_RECORD, TEST_TIME);
        } else {
            session.execute("DELETE FROM row_update_lock WHERE table_name=? AND record_key=?",
                    MARKER_TABLE, MARKER_RECORD);
        }
    }
}
