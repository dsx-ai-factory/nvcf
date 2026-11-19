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

package com.nvidia.apikeys.scheduler;

import static com.nvidia.apikeys.TestData.KEY_BY_OWNER_AND_SERVICE_VO_1;
import static com.nvidia.apikeys.TestData.KEY_VO_1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.nvidia.apikeys.App;
import com.nvidia.apikeys.config.IntegrationTestConfiguration;
import com.nvidia.apikeys.config.KeysByAccountIndexMigrationProperties;
import com.nvidia.apikeys.config.NakProperties;
import com.nvidia.apikeys.facade.AdminFacade;
import com.nvidia.apikeys.persistance.dao.KeysByAccountIndexMigrationDao.TimedRow;
import com.nvidia.apikeys.persistance.dao.KeysByAccountIndexMigrationDao;
import com.nvidia.apikeys.persistance.dao.RowUpdateLockDao;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel;
import com.nvidia.apikeys.persistance.models.KeyModel;
import com.nvidia.apikeys.services.ServicesService;
import com.nvidia.apikeys.vo.KeyByOwnerAndAccountAndServiceVo;
import com.nvidia.apikeys.vo.KeyByOwnerAndServiceVo;
import com.nvidia.apikeys.vo.KeyStatus;
import com.nvidia.apikeys.vo.KeyVo;
import com.nvidia.boot.jwt.services.mapping.EncryptedModelConverter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import net.javacrumbs.shedlock.core.DefaultLockingTaskExecutor;
import net.javacrumbs.shedlock.core.LockAssert;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockExtender;
import net.javacrumbs.shedlock.core.LockProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.DeleteOptions;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.test.context.ContextConfiguration;

@ExtendWith(IntegrationTestConfiguration.TestCleanerExtension.class)
@AutoConfigureTestRestTemplate
@SpringBootTest(classes = App.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.active=integrationtest")
@ContextConfiguration(initializers = IntegrationTestConfiguration.Initializer.class)
class KeysByAccountIndexMigrationIntegrationTest {
    @Autowired
    private CassandraTemplate template;
    @Autowired
    private CqlSession session;
    @Autowired
    private KeysByAccountIndexMigrationDao dao;
    @Autowired
    private RowUpdateLockDao markers;
    @Autowired
    private NakProperties properties;
    @Autowired
    private KeysByAccountIndexMigrationProperties migrationProperties;
    @Autowired
    private ServicesService services;
    @Autowired
    private OpenTelemetry telemetry;
    @Autowired
    private LockProvider lockProvider;
    @Autowired
    private AdminFacade adminFacade;
    @Autowired
    private ScheduledTaskService scheduler;
    @Autowired
    private EncryptedModelConverter<KeyByOwnerAndServiceModel, KeyByOwnerAndServiceVo> ownerConverter;
    @Autowired
    private EncryptedModelConverter<KeyByOwnerAndAccountAndServiceModel,
            KeyByOwnerAndAccountAndServiceVo> accountConverter;
    @Autowired
    private EncryptedModelConverter<KeyModel, KeyVo> credentialConverter;

    private KeysByAccountIndexMigrationProperties savedConfig;
    private String savedNcaId;
    private long timestamp;
    private SimpleMeterRegistry registry;

    @BeforeEach
    void configure() {
        savedConfig = new KeysByAccountIndexMigrationProperties();
        BeanUtils.copyProperties(migrationProperties, savedConfig);
        savedNcaId = properties.getNcaId();
        BeanUtils.copyProperties(KeysByAccountIndexMigrationProperties.builder()
                .pageSize(1).batchDelay(Duration.ZERO).settleDelay(Duration.ofMillis(1)).build(), migrationProperties);
        timestamp = Instant.now().minusSeconds(600).toEpochMilli() * 1000;
        registry = new SimpleMeterRegistry();
    }

    @AfterEach
    void restore() {
        BeanUtils.copyProperties(savedConfig, migrationProperties);
        properties.setNcaId(savedNcaId);
        registry.close();
    }

    @Test
    void copiesRawEncryptedLegacyRowsAndKeepsCredentialDetailsAndStaticStatus() {
        var source = source().toBuilder().createdAt(Instant.parse("2023-10-03T08:25:24.123456789Z")).build();
        seed(source, true);
        var credentialBefore = dao.credential(source.getKeyHash()).orElseThrow().model();
        session.execute("UPDATE keys_by_owner_and_service USING TIMESTAMP ? SET owner_status=? "
                + "WHERE owner_type=? AND owner_id=?", timestamp + 10000, "SUSPENDED",
                source.getOwnerType().name(), source.getOwnerId());
        var task = task(dao);

        task.run(() -> { });

        var copied = account(source);
        assertThat(copied.getKeyStatus()).isEqualTo(KeyStatus.ACTIVE);
        assertThat(copied.getCreatedAt()).isEqualTo(source.getCreatedAt().truncatedTo(ChronoUnit.MILLIS));
        assertThat(copied.getExpiresAt()).isEqualTo(source.getExpiresAt());
        assertThat(copied.getDeletesAt()).isEqualTo(source.getDeletesAt());
        assertThat(copied.getIssuerServiceId()).isEqualTo(services.toCanonicalId(source.getIssuerServiceId()));
        assertThat(copied.getAudienceServiceIds()).isEqualTo(Set.of(
                services.toCanonicalId(source.getIssuerServiceId())));
        assertThat(copied.getNcaIdInDetails()).isEqualTo(savedNcaId);
        assertThat(dao.account(accountModel(source)).orElseThrow().timestamp()).isEqualTo(timestamp);
        assertThat(dao.ownerPage(null, 10).rows().getFirst().timestamp()).isEqualTo(timestamp);
        var credentialAfter = dao.credential(source.getKeyHash()).orElseThrow();
        assertThat(credentialAfter.model().getNcaId()).isEqualTo(savedNcaId);
        assertThat(credentialAfter.model().getKeyDetails()).isEqualTo(credentialBefore.getKeyDetails());
        assertThat(credentialAfter.timestamp()).isEqualTo(timestamp);
        assertThat(completed()).isFalse();

        task.run(() -> { });
        assertThat(completed()).isTrue();
        assertThat(result("already_copied")).isEqualTo(2);
    }

    @Test
    void copyPreservesIndividualColumnTimesAndDerivesCreatedAtFromDetails() {
        var source = source().toBuilder().ncaId(savedNcaId).ncaIdInDetails(savedNcaId)
                .expiresAt(Instant.parse("2030-01-01T00:00:00Z"))
                .deletesAt(Instant.parse("2031-01-01T00:00:00Z")).build();
        seed(source, false);
        var updatedDetails = ownerConverter.voToModel(source.toBuilder().description("updated details").build());
        session.execute("UPDATE keys_by_owner_and_service USING TIMESTAMP ? SET key_details=? "
                        + "WHERE owner_type=? AND owner_id=? AND issuer_service_id=? AND key_id=?",
                timestamp + 20, updatedDetails.getKeyDetails(), source.getOwnerType().name(), source.getOwnerId(),
                source.getIssuerServiceId(), source.getKeyId());
        session.execute("UPDATE keys_by_owner_and_service USING TIMESTAMP ? SET expires_at=?, deletes_at=? "
                        + "WHERE owner_type=? AND owner_id=? AND issuer_service_id=? AND key_id=?",
                timestamp + 10, source.getExpiresAt(), source.getDeletesAt(), source.getOwnerType().name(),
                source.getOwnerId(), source.getIssuerServiceId(), source.getKeyId());
        dao.setOwnerNcaId(ownerConverter.voToModel(source), savedNcaId, timestamp + 100);

        task(dao).run(() -> { });

        var copied = dao.account(accountModel(source)).orElseThrow();
        assertThat(copied.writeTimes()).containsEntry("key_status", timestamp)
                .containsEntry("expires_at", timestamp + 10).containsEntry("deletes_at", timestamp + 10)
                .containsEntry("key_details", timestamp + 20).containsEntry("created_at", timestamp + 20);
        assertThat(copied.timestamp()).isEqualTo(timestamp + 20);
        assertThat(account(source).getDescription()).isEqualTo("updated details");
        task(dao).run(() -> { });
        assertThat(completed()).isTrue();
    }

    @Test
    void interruptedCopyBetweenColumnTimesConvergesOnRetry() {
        var source = source();
        seed(source, false);
        session.execute("UPDATE keys_by_owner_and_service USING TIMESTAMP ? SET key_status=? "
                        + "WHERE owner_type=? AND owner_id=? AND issuer_service_id=? AND key_id=?",
                timestamp + 10, "SUSPENDED", source.getOwnerType().name(), source.getOwnerId(),
                source.getIssuerServiceId(), source.getKeyId());
        var interrupted = spy(dao);
        doAnswer(call -> {
            call.callRealMethod();
            throw new IllegalStateException("interrupted between column writes");
        }).when(interrupted).copy(any(), anyLong());

        task(interrupted).run(() -> { });

        assertThat(dao.account(accountModel(source)).orElseThrow().model().getKeyDetails()).isNotBlank();
        assertThat(completed()).isFalse();
        task(dao).run(() -> { });
        task(dao).run(() -> { });

        var copied = dao.account(accountModel(source)).orElseThrow();
        assertThat(copied.model().getKeyStatus()).isEqualTo(KeyStatus.SUSPENDED);
        assertThat(copied.writeTimes()).containsEntry("key_status", timestamp + 10)
                .containsEntry("key_details", timestamp);
        assertThat(completed()).isTrue();
    }

    @Test
    void newerAssignmentMetadataDoesNotPromoteStaleAccountValues() {
        var source = source().toBuilder().ncaId(savedNcaId).ncaIdInDetails(savedNcaId).build();
        seed(source, true);
        var newer = target(source).toBuilder().keyStatus(KeyStatus.SUSPENDED)
                .description("newer account details").expiresAt(Instant.parse("2030-01-01T00:00:00Z"))
                .deletesAt(Instant.parse("2031-01-01T00:00:00Z")).build();
        dao.copy(accountConverter.voToModel(newer), timestamp + 50);
        dao.setOwnerNcaId(ownerConverter.voToModel(source), savedNcaId, timestamp + 100);

        task(dao).run(() -> { });

        var stored = account(source);
        assertThat(stored.getKeyStatus()).isEqualTo(newer.getKeyStatus());
        assertThat(stored.getDescription()).isEqualTo(newer.getDescription());
        assertThat(stored.getExpiresAt()).isEqualTo(newer.getExpiresAt());
        assertThat(stored.getDeletesAt()).isEqualTo(newer.getDeletesAt());
        assertThat(dao.account(accountModel(source)).orElseThrow().timestamp()).isEqualTo(timestamp + 50);
        assertThat(completed()).isFalse();
    }

    @Test
    void newerAssignmentMetadataDoesNotResurrectDeletedAccountColumn() {
        var source = source().toBuilder().ncaId(savedNcaId).ncaIdInDetails(savedNcaId)
                .expiresAt(Instant.parse("2030-01-01T00:00:00Z")).build();
        seed(source, true);
        var target = accountModel(source);
        dao.copy(target, timestamp);
        session.execute("UPDATE keys_by_owner_and_account_and_service USING TIMESTAMP ? SET expires_at=null "
                        + "WHERE nca_id=? AND owner_type=? AND owner_id=? AND issuer_service_id=? AND key_id=?",
                timestamp + 50, target.getNcaId(), target.getOwnerType().name(), target.getOwnerId(),
                target.getIssuerServiceId(), target.getKeyId());
        dao.setOwnerNcaId(ownerConverter.voToModel(source), savedNcaId, timestamp + 100);

        task(dao).run(() -> { });

        assertThat(dao.account(target).orElseThrow().model().getExpiresAt()).isNull();
        assertThat(completed()).isFalse();
    }

    @Test
    void pagesAcrossKeysAndSkipsPartitionsThatOnlyHoldUserStatus() {
        for (var i = 0; i < 4; i++) {
            seed(source().toBuilder().keyId("key-" + i).keyHash("hash-" + i).build(), true);
        }
        session.execute("UPDATE keys_by_owner_and_service SET owner_status=? "
                + "WHERE owner_type=? AND owner_id=?", "ACTIVE", "USER", "status-only");
        var task = task(dao);

        task.run(() -> { });

        for (var i = 0; i < 4; i++) {
            assertThat(account(source().toBuilder().keyId("key-" + i).keyHash("hash-" + i).build())
                    .getKeyId()).isEqualTo("key-" + i);
        }
        assertThat(result("copied")).isEqualTo(4);
        assertThat(result("skipped")).isEqualTo(1);
        assertThat(completed()).isFalse();
        task.run(() -> { });
        assertThat(completed()).isTrue();
    }

    @Test
    void unknownIssuerAndLegacyOnlyPartialCreateAreCopiedWithoutFabricatingCredential() {
        var source = source().toBuilder().issuerServiceId("unregistered-issuer").build();
        seed(source, false);

        task(dao).run(() -> { });

        assertThat(account(source).getIssuerServiceId()).isEqualTo("unregistered-issuer");
        assertThat(dao.credential(source.getKeyHash())).isEmpty();
        assertThat(result("unmapped")).isEqualTo(1);
        assertThat(adminFacade.listKeysInAllServices(source.getOwnerType(), source.getOwnerId()).getKeys())
                .extracting("id").containsExactly(source.getKeyId());
        task(dao).run(() -> { });
        assertThat(completed()).isTrue();
    }

    @Test
    void legacyOnlyAssignmentSurvivesDefaultChangeWithoutCreatingCredential() {
        var source = source();
        seed(source, false);
        task(dao).run(() -> { });
        var originalAccount = accountModel(source);
        properties.setNcaId("changed-default");

        task(dao).run(() -> { });
        task(dao).run(() -> { });

        assertThat(dao.account(originalAccount)).isPresent();
        assertThat(dao.account(accountModel(source))).isEmpty();
        assertThat(dao.credential(source.getKeyHash())).isEmpty();
        assertThat(completed()).isTrue();
    }

    @Test
    void interruptedCopyRetainsItsAssignmentBeforeCredentialBackfill() {
        var source = source();
        seed(source, true);
        var interrupted = spy(dao);
        doAnswer(call -> { throw new IllegalStateException("credential backfill interrupted"); })
                .when(interrupted).setCredentialNcaId(anyString(), anyString(), anyLong());
        task(interrupted).run(() -> { });
        var originalAccount = accountModel(source);
        properties.setNcaId("changed-default");

        task(dao).run(() -> { });
        task(dao).run(() -> { });

        assertThat(dao.account(originalAccount)).isPresent();
        assertThat(dao.account(accountModel(source))).isEmpty();
        assertThat(dao.credential(source.getKeyHash()).orElseThrow().model().getNcaId()).isEqualTo(savedNcaId);
        assertThat(completed()).isTrue();
    }

    @Test
    void interruptedCopyBeforeAccountWriteRetainsAssignmentAtSourceTimestamp() {
        var source = source();
        seed(source, false);
        var interrupted = spy(dao);
        doAnswer(call -> { throw new IllegalStateException("account copy interrupted"); })
                .when(interrupted).copy(any(), any(TimedRow.class));

        task(interrupted).run(() -> { });

        var retained = dao.ownerPage(null, 10).rows().getFirst();
        assertThat(retained.model().getNcaId()).isEqualTo(savedNcaId);
        assertThat(retained.timestamp()).isEqualTo(timestamp);
        var originalAccount = accountModel(source);
        assertThat(dao.account(originalAccount)).isEmpty();
        properties.setNcaId("changed-default");

        task(dao).run(() -> { });
        task(dao).run(() -> { });

        assertThat(dao.account(originalAccount)).isPresent();
        assertThat(dao.account(accountModel(source))).isEmpty();
        assertThat(dao.credential(source.getKeyHash())).isEmpty();
        assertThat(completed()).isTrue();
    }

    @Test
    void deleteBeforeAssignmentWriteDoesNotRecreateLegacyRow() {
        var source = source();
        seed(source, false);
        var racing = spy(dao);
        doAnswer(call -> {
            template.delete(ownerConverter.voToModel(source), deleteAt(timestamp + 10));
            return call.callRealMethod();
        }).when(racing).setOwnerNcaId(any(), anyString(), anyLong());

        task(racing).run(() -> { });

        assertThat(dao.ownerPage(null, 10).rows()).allSatisfy(row -> {
            assertThat(row.model().getKeyId()).isNull();
            assertThat(row.model().getNcaId()).isNull();
        });
        assertThat(dao.account(accountModel(source))).isEmpty();
        assertThat(dao.credential(source.getKeyHash())).isEmpty();
        assertThat(result("retry")).isEqualTo(1);
        assertThat(completed()).isFalse();
    }

    @Test
    void newerOwnerAssignmentWinsAndBlocksConflictingCopy() {
        var source = source();
        seed(source, false);
        var racing = spy(dao);
        doAnswer(call -> {
            dao.setOwnerNcaId(ownerConverter.voToModel(source), "concurrent-account", timestamp + 10);
            return call.callRealMethod();
        }).when(racing).setOwnerNcaId(any(), anyString(), anyLong());

        task(racing).run(() -> { });

        var retained = dao.ownerPage(null, 10).rows().getFirst();
        assertThat(retained.model().getNcaId()).isEqualTo("concurrent-account");
        assertThat(dao.account(accountModel(source))).isEmpty();
        assertThat(result("conflict")).isEqualTo(1);
        assertThat(completed()).isFalse();
    }

    @Test
    void existingAccountOnlyAssignmentIsRecoveredBeforeUsingChangedDefault() {
        var source = source();
        seed(source, false);
        var originalAccount = accountModel(source);
        dao.copy(originalAccount, timestamp);
        properties.setNcaId("changed-default");

        task(dao).run(() -> { });
        task(dao).run(() -> { });

        assertThat(dao.account(originalAccount)).isPresent();
        assertThat(dao.account(accountModel(source))).isEmpty();
        assertThat(dao.credential(source.getKeyHash())).isEmpty();
        assertThat(completed()).isTrue();
    }

    @Test
    void conflictingAccountRowsBlockCompletionWithoutAssigningTheNewDefault() {
        var source = source();
        seed(source, false);
        dao.copy(accountModel(source), timestamp);
        var other = target(source).toBuilder().ncaId("other-account")
                .ncaIdInDetails("other-account").build();
        dao.copy(accountConverter.voToModel(other), timestamp);
        properties.setNcaId("changed-default");

        task(dao).run(() -> { });

        assertThat(result("conflict")).isPositive();
        assertThat(dao.account(accountModel(source))).isEmpty();
        assertThat(dao.credential(source.getKeyHash())).isEmpty();
        assertThat(completed()).isFalse();
    }

    @Test
    void assignmentsSurviveConfigurationChangesAndConflictsBlockCompletion() {
        var source = source().toBuilder().ncaId("assigned").ncaIdInDetails("assigned").build();
        seed(source, true);
        properties.setNcaId("new-default");

        task(dao).run(() -> { });

        var target = accountModel(source).toBuilder().ncaId("assigned").build();
        assertThat(dao.account(target)).isPresent();
        assertThat(dao.credential(source.getKeyHash()).orElseThrow().model().getNcaId()).isEqualTo("assigned");
        assertThat(dao.account(accountModel(source))).isEmpty();
        session.execute("UPDATE keys USING TIMESTAMP ? SET nca_id=? WHERE api_key_hash=?",
                timestamp + 1, "conflicting", source.getKeyHash());

        task(dao).run(() -> { });

        assertThat(result("conflict")).isEqualTo(1);
        assertThat(completed()).isFalse();
        assertThat(dao.credential(source.getKeyHash()).orElseThrow().model().getNcaId()).isEqualTo("conflicting");
        assertThat(dao.account(target).orElseThrow().timestamp()).isEqualTo(timestamp);
    }

    @Test
    void credentialAssignmentIsKeptAfterTheDefaultChangesAndStaleAccountIsRepaired() {
        var source = source();
        seed(source, true);
        session.execute("UPDATE keys USING TIMESTAMP ? SET nca_id=? WHERE api_key_hash=?",
                timestamp, "assigned", source.getKeyHash());
        properties.setNcaId("new-default");
        var stale = target(source).toBuilder().ncaId("assigned").ncaIdInDetails(null)
                .description("stale").build();
        dao.copy(accountConverter.voToModel(stale), timestamp - 1);

        task(dao).run(() -> { });

        var copied = accountConverter.modelToVo(dao.account(accountConverter.voToModel(stale))
                .orElseThrow().model());
        assertThat(copied.getNcaId()).isEqualTo("assigned");
        assertThat(copied.getNcaIdInDetails()).isEqualTo("assigned");
        assertThat(copied.getDescription()).isEqualTo(source.getDescription());
        assertThat(dao.credential(source.getKeyHash()).orElseThrow().model().getNcaId()).isEqualTo("assigned");
        task(dao).run(() -> { });
        assertThat(completed()).isTrue();
    }

    @Test
    void restartAfterCredentialWriteAndLaterLegacyWritesConvergeEvenAfterM0() {
        var first = source();
        seed(first, true);
        var interrupted = spy(dao);
        doAnswer(call -> {
            call.callRealMethod();
            throw new IllegalStateException("restart after credential write");
        }).when(interrupted).setCredentialNcaId(anyString(), anyString(), anyLong());
        task(interrupted).run(() -> { });
        assertThat(completed()).isFalse();

        task(dao).run(() -> { });
        assertThat(completed()).isTrue();
        var second = first.toBuilder().keyId("second-key").keyHash("second-hash").build();
        seed(second, true);
        task(dao).run(() -> { });
        assertThat(account(second).getKeyId()).isEqualTo("second-key");
        assertThat(completed()).isTrue();
    }

    @Test
    void concurrentSuspendWinsOverStaleCopyAndIsRepairedOnNextScan() {
        var source = source();
        seed(source, true);
        var racing = spy(dao);
        doAnswer(call -> {
            var updated = source.toBuilder().keyStatus(KeyStatus.SUSPENDED).description("concurrent update").build();
            template.insert(ownerConverter.voToModel(updated), insertAt(timestamp + 10));
            dao.copy(accountConverter.voToModel(target(updated)), timestamp + 10);
            return call.callRealMethod();
        }).when(racing).copy(any(), any(TimedRow.class));

        task(racing).run(() -> { });

        assertThat(account(source).getKeyStatus()).isEqualTo(KeyStatus.SUSPENDED);
        assertThat(account(source).getDescription()).isEqualTo("concurrent update");
        assertThat(result("retry")).isEqualTo(1);
        assertThat(completed()).isFalse();
        task(dao).run(() -> { });
        assertThat(completed()).isFalse();
        task(dao).run(() -> { });
        assertThat(completed()).isTrue();
    }

    @Test
    void concurrentDeleteWinsOverStaleCopyAndNeverResurrectsCredential() {
        var source = source();
        seed(source, true);
        var racing = spy(dao);
        doAnswer(call -> {
            template.delete(ownerConverter.voToModel(source), deleteAt(timestamp + 10));
            template.delete(accountModel(source), deleteAt(timestamp + 10));
            template.delete(KeyModel.builder().keyHash(source.getKeyHash()).build(), deleteAt(timestamp + 10));
            return call.callRealMethod();
        }).when(racing).copy(any(), any(TimedRow.class));

        task(racing).run(() -> { });

        assertThat(dao.account(accountModel(source))).isEmpty();
        assertThat(dao.credential(source.getKeyHash())).isEmpty();
        assertThat(result("retry")).isEqualTo(1);
        assertThat(completed()).isFalse();
    }

    @Test
    void credentialDeleteAfterReadDefeatsNcaOnlyUpdate() {
        var source = source();
        seed(source, true);
        var racing = spy(dao);
        doAnswer(call -> {
            template.delete(KeyModel.builder().keyHash(source.getKeyHash()).build(), deleteAt(timestamp + 10));
            return call.callRealMethod();
        }).when(racing).setCredentialNcaId(anyString(), anyString(), anyLong());

        task(racing).run(() -> { });

        assertThat(dao.credential(source.getKeyHash())).isEmpty();
        assertThat(result("retry")).isEqualTo(1);
        assertThat(completed()).isFalse();
    }

    @Test
    void agedOrphanDeleteBeatsCopyButLosesToLaterRealWrite() {
        var source = source();
        var model = accountModel(source);
        dao.copy(model, timestamp);

        task(dao).run(() -> { });
        assertThat(dao.account(model)).isEmpty();
        assertThat(result("orphan")).isEqualTo(1);
        dao.copy(model, timestamp);
        assertThat(dao.account(model)).isEmpty();
        dao.copy(model, timestamp + 2);
        assertThat(dao.account(model)).isPresent();
    }

    @Test
    void youngOrphanAndOwnerAppearingOnRecheckAreRetained() {
        var source = source();
        var model = accountModel(source);
        dao.copy(model, Instant.now().toEpochMilli() * 1000);
        task(dao).run(() -> { });
        assertThat(dao.account(model)).isPresent();
        assertThat(completed()).isFalse();

        template.getCqlOperations().execute("TRUNCATE keys_by_owner_and_account_and_service");
        dao.copy(model, timestamp);
        var racing = spy(dao);
        doAnswer(call -> {
            var absent = call.callRealMethod();
            seed(source, false);
            return absent;
        }).when(racing).owner(any(), any());
        task(racing).run(() -> { });
        assertThat(dao.account(model)).isPresent();
        assertThat(completed()).isFalse();
    }

    @Test
    void markerHasNoTtlAndDeletionIsObservedWithoutCaching() {
        task(dao).run(() -> { });
        assertThat(completed()).isTrue();
        var row = session.execute("SELECT TTL(updated_at) AS ttl FROM row_update_lock "
                + "WHERE table_name=? AND record_key=?", KeysByAccountIndexMigrationTask.MARKER_TABLE,
                KeysByAccountIndexMigrationTask.MARKER_RECORD).one();
        assertThat(row.isNull("ttl")).isTrue();
        assertThat(markers.markCompleted(KeysByAccountIndexMigrationTask.MARKER_TABLE,
                KeysByAccountIndexMigrationTask.MARKER_RECORD, Instant.now())).isFalse();
        session.execute("DELETE FROM row_update_lock WHERE table_name=? AND record_key=?",
                KeysByAccountIndexMigrationTask.MARKER_TABLE, KeysByAccountIndexMigrationTask.MARKER_RECORD);
        assertThat(completed()).isFalse();
        task(dao).run(() -> { });
        assertThat(completed()).isTrue();
    }

    @Test
    void renewsCassandraLockWithinSlowPageAndKeepsContenderOut() {
        migrationProperties.setPageSize(10);
        for (var i = 0; i < 3; i++) {
            seed(source().toBuilder().keyId("slow-key-" + i).keyHash("slow-hash-" + i).build(), false);
        }
        var nanos = new AtomicLong();
        var renewals = new AtomicInteger();
        var racing = spy(dao);
        doAnswer(call -> {
            // Advance elapsed time instead of sleeping through a full lease.
            nanos.addAndGet(Duration.ofSeconds(20).toNanos());
            return call.callRealMethod();
        }).when(racing).credential(anyString());
        var executor = new DefaultLockingTaskExecutor(lockProvider);
        executor.executeWithLock((Runnable) () -> {
            var renewal = new ScheduledTaskService.LockRenewal(() -> {
                var before = session.execute("SELECT lockuntil FROM lock WHERE name=?", "slow-page-test")
                        .one().getInstant("lockuntil");
                LockExtender.extendActiveLock(Duration.ofMinutes(1), Duration.ZERO);
                var after = session.execute("SELECT lockuntil FROM lock WHERE name=?", "slow-page-test")
                        .one().getInstant("lockuntil");
                assertThat(after).isAfter(before);
                assertThat(lockProvider.lock(new LockConfiguration(Instant.now(), "slow-page-test",
                        Duration.ofMinutes(1), Duration.ZERO))).isEmpty();
                renewals.incrementAndGet();
            }, Duration.ofSeconds(20), nanos::get);

            task(racing).run(renewal);
        }, new LockConfiguration(Instant.now(), "slow-page-test", Duration.ofSeconds(30), Duration.ZERO));

        assertThat(renewals.get()).isEqualTo(4);
        assertThat(result("copied")).isEqualTo(3);
        assertThat(completed()).isFalse();
    }

    @Test
    void schedulerUsesCassandraLockAndExtendsItBetweenPages() throws InterruptedException {
        scheduler.keysByAccountIndexCopy();
        assertThat(completed()).isTrue();
        var executor = new DefaultLockingTaskExecutor(lockProvider);
        var ran = new AtomicBoolean();
        executor.executeWithLock((Runnable) () -> {
            LockAssert.assertLocked();
            var before = session.execute("SELECT lockuntil FROM lock WHERE name=?", "contention-test")
                    .one().getInstant("lockuntil");
            LockExtender.extendActiveLock(Duration.ofMinutes(10), Duration.ZERO);
            var after = session.execute("SELECT lockuntil FROM lock WHERE name=?", "contention-test")
                    .one().getInstant("lockuntil");
            assertThat(after).isAfter(before);
            var contender = lockProvider.lock(new LockConfiguration(Instant.now(), "contention-test",
                    Duration.ofMinutes(1), Duration.ZERO));
            assertThat(contender).isEmpty();
            ran.set(true);
        }, new LockConfiguration(Instant.now(), "contention-test", Duration.ofMinutes(1), Duration.ZERO));
        assertThat(ran).isTrue();
        var next = lockProvider.lock(new LockConfiguration(Instant.now(), "contention-test",
                Duration.ofMinutes(1), Duration.ZERO));
        assertThat(next).isPresent();
        next.orElseThrow().unlock();
    }

    private KeysByAccountIndexMigrationTask task(KeysByAccountIndexMigrationDao migrationDao) {
        var task = new KeysByAccountIndexMigrationTask(migrationDao, markers, properties, migrationProperties, services,
                ownerConverter, accountConverter, registry, telemetry, Clock.systemUTC());
        task.initializeMetrics();
        return task;
    }

    private KeyByOwnerAndServiceVo source() {
        return KEY_BY_OWNER_AND_SERVICE_VO_1.toBuilder().ncaId(null).ncaIdInDetails(null)
                .audienceServiceIds(Set.of(KEY_BY_OWNER_AND_SERVICE_VO_1.getIssuerServiceId())).build();
    }

    private void seed(KeyByOwnerAndServiceVo source, boolean credential) {
        template.insert(ownerConverter.voToModel(source), insertAt(timestamp));
        if (credential) {
            var key = KEY_VO_1.toBuilder().keyHash(source.getKeyHash()).ncaId(source.getNcaId())
                    .ncaIdInDetails(source.getNcaIdInDetails()).build();
            template.insert(credentialConverter.voToModel(key), insertAt(timestamp));
        }
    }

    private KeyByOwnerAndAccountAndServiceVo target(KeyByOwnerAndServiceVo source) {
        return new KeyByOwnerAndAccountAndServiceVo(source, properties.getNcaId()).toBuilder()
                .issuerServiceId(services.toCanonicalId(source.getIssuerServiceId()))
                .audienceServiceIds(source.getAudienceServiceIds().stream().map(services::toCanonicalId)
                        .collect(java.util.stream.Collectors.toSet()))
                .ncaIdInDetails(properties.getNcaId()).build();
    }

    private KeyByOwnerAndAccountAndServiceModel accountModel(KeyByOwnerAndServiceVo source) {
        return accountConverter.voToModel(target(source));
    }

    private KeyByOwnerAndAccountAndServiceVo account(KeyByOwnerAndServiceVo source) {
        return accountConverter.modelToVo(dao.account(accountModel(source)).orElseThrow().model());
    }

    private InsertOptions insertAt(long value) {
        return InsertOptions.builder().timestamp(value).build();
    }

    private DeleteOptions deleteAt(long value) {
        return DeleteOptions.builder().timestamp(value).build();
    }

    private boolean completed() {
        return markers.isCompleted(KeysByAccountIndexMigrationTask.MARKER_TABLE,
                KeysByAccountIndexMigrationTask.MARKER_RECORD);
    }

    private double result(String value) {
        return registry.get("nvcf_api_keys_keys_by_account_index_copy_keys_total")
                .tag("result", value).counter().count();
    }
}
