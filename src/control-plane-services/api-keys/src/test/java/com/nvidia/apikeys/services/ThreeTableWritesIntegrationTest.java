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

import static com.nvidia.apikeys.TestData.API_KEY_HASH_1;
import static com.nvidia.apikeys.TestData.CREATE_KEY_REQUEST_VO;
import static com.nvidia.apikeys.TestData.DELETE_KEY_BY_ID_REQUEST_VO_1;
import static com.nvidia.apikeys.TestData.KEY_BY_OWNER_AND_SERVICE_VO_1;
import static com.nvidia.apikeys.TestData.KEY_OWNER_VO_1;
import static com.nvidia.apikeys.TestData.KEY_VO_1;
import static com.nvidia.apikeys.TestData.SUSPEND_KEYS_REQUEST_VO_1;
import static com.nvidia.apikeys.TestData.TEST_TIME;
import static com.nvidia.apikeys.TestData.UPDATE_KEY_REQUEST_VO_1;
import static com.nvidia.apikeys.vo.KeyStatus.SUSPENDED;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.nvidia.apikeys.App;
import com.nvidia.apikeys.config.IntegrationTestConfiguration;
import com.nvidia.apikeys.config.NakProperties;
import com.nvidia.apikeys.persistance.dao.KeysDao;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel;
import com.nvidia.apikeys.persistance.models.KeyModel;
import com.nvidia.apikeys.persistance.repositories.KeyByOwnerAndAccountAndServiceRepository;
import com.nvidia.apikeys.utils.TestClock;
import com.nvidia.apikeys.vo.KeyByOwnerAndAccountAndServiceVo;
import com.nvidia.apikeys.vo.KeyByOwnerAndServiceVo;
import com.nvidia.boot.jwt.services.mapping.EncryptedModelConverter;
import java.time.ZoneId;
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
@SpringBootTest(
        classes = App.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.active=integrationtest")
@ContextConfiguration(initializers = IntegrationTestConfiguration.Initializer.class)
class ThreeTableWritesIntegrationTest {

    @Autowired
    private KeysDao keysDao;
    @Autowired
    private KeysService keysService;
    @Autowired
    private KeyByOwnerAndAccountAndServiceRepository accountRepository;
    @Autowired
    private EncryptedModelConverter<KeyByOwnerAndAccountAndServiceModel,
            KeyByOwnerAndAccountAndServiceVo> accountConverter;
    @Autowired
    private CassandraTemplate cassandraTemplate;
    @Autowired
    private CqlSession session;
    @Autowired
    private NakProperties properties;

    @BeforeEach
    void freezeClock() {
        TestClock.setBaseClock(TestClock.fixed(TEST_TIME, ZoneId.systemDefault()));
    }

    @AfterEach
    void resetClock() {
        TestClock.resetToDefaults();
    }

    @Test
    void createWritesCompleteEncryptedAccountRowWithTheSameTimestamp() {
        var key = KEY_VO_1.toBuilder()
                .ncaId(properties.getNcaId())
                .ncaIdInDetails(properties.getNcaId())
                .build();

        keysDao.create(key, KEY_OWNER_VO_1);

        var rows = accountRepository.findByNcaIdAndOwnerTypeAndOwnerId(
                properties.getNcaId(), key.getOwnerType(), key.getOwnerId());
        assertThat(rows).hasSize(1);
        var accountRow = rows.getFirst();
        assertThat(accountConverter.modelToVo(accountRow)).isEqualTo(
                new KeyByOwnerAndAccountAndServiceVo(
                        new KeyByOwnerAndServiceVo(key, KEY_OWNER_VO_1),
                        properties.getNcaId()));
        assertThat(accountRow.getKeyDetails()).doesNotContain(key.getKeyHash(), key.getDescription());
        assertThat(keysDao.getKeyByHash(key.getKeyHash()).orElseThrow().getNcaId())
                .isEqualTo(properties.getNcaId());

        var keyTimestamp = session.execute("select writetime(status) as ts from keys "
                + "where api_key_hash=?", key.getKeyHash()).one().getLong("ts");
        var ownerTimestamp = session.execute("select writetime(key_status) as ts "
                + "from keys_by_owner_and_service where owner_type=? and owner_id=? "
                + "and issuer_service_id=? and key_id=?", key.getOwnerType().name(),
                key.getOwnerId(), key.getIssuerServiceId(), key.getKeyId()).one().getLong("ts");
        var accountTimestamp = session.execute("select writetime(key_status) as ts "
                + "from keys_by_owner_and_account_and_service where nca_id=? "
                + "and owner_type=? and owner_id=? and issuer_service_id=? and key_id=?",
                properties.getNcaId(), key.getOwnerType().name(), key.getOwnerId(),
                key.getIssuerServiceId(), key.getKeyId()).one().getLong("ts");
        assertThat(keyTimestamp).isPositive().isEqualTo(ownerTimestamp).isEqualTo(accountTimestamp);
    }

    @Test
    void updateCreatesACompleteAccountRowForAnUncopiedKey() {
        keysService.createKey(CREATE_KEY_REQUEST_VO);
        removeAccountRow();

        var updated = keysService.updateKey(UPDATE_KEY_REQUEST_VO_1);

        assertThat(accountConverter.modelToVo(accountRow())).isEqualTo(
                new KeyByOwnerAndAccountAndServiceVo(
                        new KeyByOwnerAndServiceVo(updated, KEY_OWNER_VO_1),
                        properties.getNcaId()).toBuilder().ncaIdInDetails(properties.getNcaId()).build());
    }

    @Test
    void suspendCreatesACompleteAccountRowForAnUncopiedKey() {
        keysService.createKey(CREATE_KEY_REQUEST_VO);
        removeAccountRow();

        keysService.suspendKeys(SUSPEND_KEYS_REQUEST_VO_1);

        var expected = new KeyByOwnerAndAccountAndServiceVo(
                KEY_BY_OWNER_AND_SERVICE_VO_1.toBuilder().keyStatus(SUSPENDED).build(),
                properties.getNcaId()).toBuilder().ncaIdInDetails(properties.getNcaId()).build();
        assertThat(accountConverter.modelToVo(accountRow())).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deleteRepairsAListedKeyThatHasNoCredential(boolean hasAccountRow) {
        keysService.createKey(CREATE_KEY_REQUEST_VO);
        cassandraTemplate.delete(KeyModel.builder().keyHash(API_KEY_HASH_1).build());
        if (!hasAccountRow) {
            removeAccountRow();
        }
        assertThat(keysDao.getKeyByHash(API_KEY_HASH_1)).isEmpty();
        assertThat(keysService.listKeys(KEY_OWNER_VO_1)).hasSize(1);

        keysService.deleteKeyById(DELETE_KEY_BY_ID_REQUEST_VO_1);
        keysDao.deleteKey(KEY_BY_OWNER_AND_SERVICE_VO_1);

        assertThat(keysDao.getKeyByHash(API_KEY_HASH_1)).isEmpty();
        assertThat(keysService.listKeys(KEY_OWNER_VO_1)).isEmpty();
        assertThat(accountRepository.findByNcaIdAndOwnerTypeAndOwnerId(
                properties.getNcaId(), KEY_VO_1.getOwnerType(), KEY_VO_1.getOwnerId())).isEmpty();
    }

    @Test
    void sessionUsesLocalQuorumForReadsAndWrites() {
        assertThat(session.getContext().getConfig().getDefaultProfile()
                .getString(DefaultDriverOption.REQUEST_CONSISTENCY)).isEqualTo("LOCAL_QUORUM");
    }

    private KeyByOwnerAndAccountAndServiceModel accountRow() {
        var rows = accountRepository.findByNcaIdAndOwnerTypeAndOwnerId(
                properties.getNcaId(), KEY_VO_1.getOwnerType(), KEY_VO_1.getOwnerId());
        assertThat(rows).hasSize(1);
        return rows.getFirst();
    }

    private void removeAccountRow() {
        cassandraTemplate.delete(accountRow());
    }
}
