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
import static com.nvidia.apikeys.TestData.TEST_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.nvidia.apikeys.config.KeysByAccountIndexMigrationProperties;
import com.nvidia.apikeys.config.NakProperties;
import com.nvidia.apikeys.persistance.dao.KeysByAccountIndexMigrationDao.Page;
import com.nvidia.apikeys.persistance.dao.KeysByAccountIndexMigrationDao.TimedRow;
import com.nvidia.apikeys.persistance.dao.KeysByAccountIndexMigrationDao;
import com.nvidia.apikeys.persistance.dao.RowUpdateLockDao;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel;
import com.nvidia.apikeys.services.ServicesService;
import com.nvidia.apikeys.vo.KeyByOwnerAndAccountAndServiceVo;
import com.nvidia.apikeys.vo.KeyByOwnerAndServiceVo;
import com.nvidia.apikeys.vo.KeyOwnerType;
import com.nvidia.boot.jwt.services.mapping.EncryptedModelConverter;
import com.nvidia.boot.migration.notification.service.DataMigration;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KeysByAccountIndexMigrationTaskTest {
    @Mock
    private KeysByAccountIndexMigrationDao dao;
    @Mock
    private RowUpdateLockDao markers;
    @Mock
    private ServicesService services;
    @Mock
    private EncryptedModelConverter<KeyByOwnerAndServiceModel, KeyByOwnerAndServiceVo> ownerConverter;
    @Mock
    private EncryptedModelConverter<KeyByOwnerAndAccountAndServiceModel,
            KeyByOwnerAndAccountAndServiceVo> accountConverter;

    private NakProperties properties;
    private KeysByAccountIndexMigrationProperties migrationProperties;
    private PrometheusMeterRegistry registry;
    private KeysByAccountIndexMigrationTask task;
    private final AtomicBoolean completed = new AtomicBoolean();

    @BeforeEach
    void setUp() {
        properties = NakProperties.builder().ncaId("configured-nca").build();
        migrationProperties = new KeysByAccountIndexMigrationProperties();
        var config = migrationProperties;
        config.setSettleDelay(Duration.ZERO);
        config.setBatchDelay(Duration.ZERO);
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        task = new KeysByAccountIndexMigrationTask(dao, markers, properties, migrationProperties, services,
                ownerConverter, accountConverter, registry, OpenTelemetry.noop(),
                Clock.fixed(TEST_TIME, ZoneOffset.UTC));
        when(markers.isCompleted(anyString(), anyString())).thenAnswer(invocation -> completed.get());
        task.initializeMetrics();
    }

    @Test
    void migrationMetadataIsAvailableToReleaseTooling() {
        var migration = KeysByAccountIndexMigrationTask.class.getAnnotation(DataMigration.class);

        assertThat(migration).isNotNull();
        assertThat(migration.keyspace()).isEqualTo("nvcf_api_keys");
        assertThat(migration.newTables()).containsExactly("keys_by_owner_and_account_and_service");
        assertThat(migration.newColumns()).containsExactlyInAnyOrder(
                "keys.nca_id", "keys_by_owner_and_service.nca_id");
        assertThat(migration.requiresTables()).containsExactlyInAnyOrder(
                "keys", "keys_by_owner_and_service", "row_update_lock", "lock");
        assertThat(migration.description()).isNotBlank();
    }

    @Test
    void disabledTaskDoesNotScan() {
        migrationProperties.setEnabled(false);

        task.runUnchecked();

        verifyNoInteractions(dao);
    }

    @Test
    void twoCleanScansMarkCompletedAndLaterRunsStillScan() {
        emptyPages();
        when(markers.markCompleted(anyString(), anyString(), any())).thenAnswer(invocation -> {
            completed.set(true);
            return true;
        });

        task.runUnchecked(() -> { });

        var order = inOrder(dao, markers);
        order.verify(dao).ownerPage(null, 500);
        order.verify(dao).accountPage(null, 500);
        order.verify(dao).ownerPage(null, 500);
        order.verify(dao).accountPage(null, 500);
        order.verify(markers).markCompleted("keys_by_account_index_copy", "completed", TEST_TIME);

        task.runUnchecked(() -> { });

        verify(dao, times(3)).ownerPage(null, 500);
        verify(dao, times(3)).accountPage(null, 500);
        verify(markers).markCompleted("keys_by_account_index_copy", "completed", TEST_TIME);
        assertThat(registry.get("nvcf_api_keys_keys_by_account_index_copy_completed").gauge().value()).isEqualTo(1);
    }

    @Test
    void pageFetchRetriesAreBoundedAndFailureDoesNotMarkCompletion() {
        when(dao.ownerPage(null, 500)).thenThrow(new IllegalStateException("fetch failed"));

        task.runUnchecked(() -> { });

        verify(dao, times(3)).ownerPage(null, 500);
        verify(markers, never()).markCompleted(anyString(), anyString(), any());
        assertThat(registry.get("nvcf_api_keys_keys_by_account_index_copy_runs_total")
                .tag("outcome", "failed").counter().count()).isEqualTo(1);
    }

    @Test
    void retryContinuesFromTheSamePageWithoutSkippingRows() {
        var nextPage = ByteBuffer.wrap(new byte[] {1});
        var staticRow = KeyByOwnerAndServiceModel.builder()
                .ownerType(KeyOwnerType.USER).ownerId("owner").build();
        when(dao.ownerPage(null, 500))
                .thenReturn(new Page<>(List.of(new TimedRow<>(staticRow, 0)), nextPage));
        when(dao.ownerPage(nextPage, 500))
                .thenThrow(new IllegalStateException("temporary fetch failure"))
                .thenReturn(new Page<>(List.of(), null));
        when(dao.accountPage(null, 500)).thenReturn(new Page<>(List.of(), null));
        when(markers.markCompleted(anyString(), anyString(), any())).thenAnswer(invocation -> {
            completed.set(true);
            return true;
        });

        task.runUnchecked(() -> { });

        verify(dao, times(2)).ownerPage(null, 500);
        verify(dao, times(3)).ownerPage(nextPage, 500);
        assertThat(completed).isTrue();
        assertThat(result("skipped")).isEqualTo(2);
    }

    @Test
    void ncaConflictWritesNothingAndBlocksCompletion() {
        var model = KeyByOwnerAndServiceModel.builder().ownerType(KeyOwnerType.USER)
                .ownerId("owner").keyId("key").build();
        when(dao.ownerPage(null, 500)).thenReturn(new Page<>(List.of(new TimedRow<>(model, 123)), null));
        when(dao.accountPage(null, 500)).thenReturn(new Page<>(List.of(), null));
        when(ownerConverter.modelToVo(model)).thenReturn(KEY_BY_OWNER_AND_SERVICE_VO_1.toBuilder()
                .ncaId("column-nca").ncaIdInDetails("details-nca").build());
        when(dao.credential(anyString())).thenReturn(Optional.empty());

        task.runUnchecked(() -> { });

        verify(dao, never()).copy(any(), any(TimedRow.class));
        verify(dao, never()).setCredentialNcaId(anyString(), anyString(), anyLong());
        verify(markers, never()).markCompleted(anyString(), anyString(), any());
        assertThat(result("conflict")).isEqualTo(1);
    }

    @Test
    void youngOrphanIsKeptAndBlocksCompletion() {
        var model = accountModel();
        when(dao.ownerPage(null, 500)).thenReturn(new Page<>(List.of(), null));
        when(dao.accountPage(null, 500)).thenReturn(new Page<>(List.of(
                new TimedRow<>(model, TEST_TIME.toEpochMilli() * 1000)), null));
        when(services.storedServiceIds("service")).thenReturn(Set.of("service"));

        task.runUnchecked(() -> { });

        verify(dao, never()).deleteOrphan(any(), anyLong());
        verify(markers, never()).markCompleted(anyString(), anyString(), any());
        assertThat(result("retry")).isEqualTo(1);
    }

    @Test
    void orphanIsNotDeletedWhenOwnerAppearsOnRecheck() {
        var model = accountModel();
        when(dao.ownerPage(null, 500)).thenReturn(new Page<>(List.of(), null));
        when(dao.accountPage(null, 500)).thenReturn(new Page<>(List.of(
                new TimedRow<>(model, TEST_TIME.minusSeconds(600).toEpochMilli() * 1000)), null));
        when(services.storedServiceIds("service")).thenReturn(Set.of("service"));
        when(dao.ownerExists(model, Set.of("service"))).thenReturn(false, true);

        task.runUnchecked(() -> { });

        verify(dao, never()).deleteOrphan(any(), anyLong());
        verify(markers, never()).markCompleted(anyString(), anyString(), any());
    }

    @Test
    void oldOrphanIsDeletedAtItsTimestampPlusOne() {
        var model = accountModel();
        var timestamp = TEST_TIME.minusSeconds(600).toEpochMilli() * 1000;
        when(dao.ownerPage(null, 500)).thenReturn(new Page<>(List.of(), null));
        when(dao.accountPage(null, 500)).thenReturn(new Page<>(List.of(new TimedRow<>(model, timestamp)), null));
        when(services.storedServiceIds("service")).thenReturn(Set.of("service"));
        when(dao.account(model)).thenReturn(Optional.empty());

        task.runUnchecked(() -> { });

        verify(dao).deleteOrphan(model, timestamp + 1);
        assertThat(result("orphan")).isEqualTo(1);
        verify(markers, never()).markCompleted(anyString(), anyString(), any());
    }

    @Test
    void rowFailureIsReportedAsFailedAndBlocksCompletion() {
        var model = KeyByOwnerAndServiceModel.builder().ownerType(KeyOwnerType.USER)
                .ownerId("owner").keyId("key").build();
        when(dao.ownerPage(null, 500)).thenReturn(new Page<>(List.of(new TimedRow<>(model, 123)), null));
        when(dao.accountPage(null, 500)).thenReturn(new Page<>(List.of(), null));
        when(ownerConverter.modelToVo(model)).thenThrow(new IllegalStateException("cannot decrypt"));

        task.runUnchecked(() -> { });

        assertThat(result("failed")).isEqualTo(1);
        assertThat(registry.get("nvcf_api_keys_keys_by_account_index_copy_runs_total")
                .tag("outcome", "failed").counter().count()).isEqualTo(1);
        verify(markers, never()).markCompleted(anyString(), anyString(), any());
    }

    @Test
    void lostLockStopsTheScanAndBlocksCompletion() {
        task.runUnchecked(() -> { throw new IllegalStateException("lock lost"); });

        verifyNoInteractions(dao);
        verify(markers, never()).markCompleted(anyString(), anyString(), any());
        assertThat(registry.get("nvcf_api_keys_keys_by_account_index_copy_runs_total")
                .tag("outcome", "failed").counter().count()).isEqualTo(1);
    }

    @Test
    void renewalFailureInsidePageStopsBeforeNextRow() {
        var model = KeyByOwnerAndServiceModel.builder().ownerType(KeyOwnerType.USER).ownerId("owner").build();
        var row = new TimedRow<>(model, 0);
        when(dao.ownerPage(null, 500)).thenReturn(new Page<>(List.of(row, row, row), null));

        task.runUnchecked(() -> {
            if (result("skipped") > 0) {
                throw new IllegalStateException("lock lost during page");
            }
        });

        assertThat(result("skipped")).isEqualTo(1);
        verify(markers, never()).markCompleted(anyString(), anyString(), any());
        assertThat(registry.get("nvcf_api_keys_keys_by_account_index_copy_runs_total")
                .tag("outcome", "failed").counter().count()).isEqualTo(1);
    }

    @Test
    void renewalFailureBetweenFetchAttemptsStopsRetries() {
        var attempts = new AtomicInteger();
        when(dao.ownerPage(null, 500)).thenAnswer(call -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("fetch failed");
        });

        task.runUnchecked(() -> {
            if (attempts.get() > 0) {
                throw new IllegalStateException("lock lost between retries");
            }
        });

        verify(dao).ownerPage(null, 500);
        verify(markers, never()).markCompleted(anyString(), anyString(), any());
    }

    @Test
    void renewalUsesElapsedTimeAndThrottlesRepeatedChecks() {
        var nanos = new AtomicLong(-Duration.ofSeconds(5).toNanos());
        var extensions = new AtomicInteger();
        var renewal = new KeysByAccountIndexMigrationTask.LockRenewal(
                extensions::incrementAndGet, Duration.ofSeconds(1), nanos::get);
        renewal.run();
        for (var i = 0; i < 500; i++) {
            nanos.addAndGet(Duration.ofMillis(1).toNanos());
            renewal.run();
        }
        assertThat(extensions.get()).isEqualTo(1);

        nanos.addAndGet(Duration.ofMillis(500).toNanos());
        renewal.run();
        assertThat(extensions.get()).isEqualTo(2);

        nanos.addAndGet(Duration.ofSeconds(1).toNanos());
        renewal.run();
        assertThat(extensions.get()).isEqualTo(3);
    }

    @Test
    void firstScrapeContainsZeroCountersAndCompletionGauge() {
        var scrape = registry.scrape();
        assertThat(scrape).contains("nvcf_api_keys_keys_by_account_index_copy_keys_total{result=\"copied\"} 0.0");
        assertThat(scrape).contains("nvcf_api_keys_keys_by_account_index_copy_keys_total{result=\"orphan\"} 0.0");
        assertThat(scrape).contains("nvcf_api_keys_keys_by_account_index_copy_runs_total{outcome=\"failed\"} 0.0");
        assertThat(scrape).contains("nvcf_api_keys_keys_by_account_index_copy_completed 0.0");
    }

    private void emptyPages() {
        when(dao.ownerPage(null, 500)).thenReturn(new Page<>(List.of(), null));
        when(dao.accountPage(null, 500)).thenReturn(new Page<>(List.of(), null));
    }

    private KeyByOwnerAndAccountAndServiceModel accountModel() {
        return KeyByOwnerAndAccountAndServiceModel.builder().ncaId("configured-nca")
                .ownerType(KeyOwnerType.USER).ownerId("owner").issuerServiceId("service").keyId("key").build();
    }

    private double result(String result) {
        return registry.get("nvcf_api_keys_keys_by_account_index_copy_keys_total")
                .tag("result", result).counter().count();
    }
}
