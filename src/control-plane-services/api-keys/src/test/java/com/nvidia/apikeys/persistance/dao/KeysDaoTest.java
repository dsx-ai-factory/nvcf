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

package com.nvidia.apikeys.persistance.dao;

import static com.nvidia.apikeys.TestData.API_KEY_HASH_1;
import static com.nvidia.apikeys.TestData.KEY_BY_OWNER_AND_SERVICE_MODEL_1;
import static com.nvidia.apikeys.TestData.KEY_BY_OWNER_AND_SERVICE_VO_1;
import static com.nvidia.apikeys.TestData.KEY_ID_1;
import static com.nvidia.apikeys.TestData.KEY_OWNER_VO_1;
import static com.nvidia.apikeys.TestData.KEY_VO_1;
import static com.nvidia.apikeys.TestData.SERVICE_ID_1;
import static com.nvidia.apikeys.TestData.USER_KEY_OWNER_ID_1;
import static com.nvidia.apikeys.utils.TestUtils.assertThrowsExceptionWithDetails;
import static com.nvidia.apikeys.vo.KeyOwnerType.USER;
import static com.nvidia.apikeys.vo.KeyStatus.ACTIVE;
import static com.nvidia.apikeys.scheduler.KeysByAccountIndexMigrationTask.MARKER_RECORD;
import static com.nvidia.apikeys.scheduler.KeysByAccountIndexMigrationTask.MARKER_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.nvidia.apikeys.config.exceptions.CassandraException;
import com.nvidia.apikeys.config.NakProperties;
import com.nvidia.apikeys.converters.KeyOwnerVoBuilder;
import com.nvidia.apikeys.persistance.repositories.KeyByOwnerAndAccountAndServiceRepository;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel;
import com.nvidia.apikeys.persistance.models.KeyModel;
import com.nvidia.apikeys.persistance.repositories.KeyByOwnerAndServiceRepository;
import com.nvidia.apikeys.persistance.repositories.KeyRepository;
import com.nvidia.apikeys.services.KeyNcaIdResolver;
import com.nvidia.apikeys.services.ServicesService;
import com.nvidia.apikeys.vo.KeyByOwnerAndAccountAndServiceVo;
import com.nvidia.apikeys.vo.SavedKeyVo;
import com.nvidia.boot.exceptions.ServiceUnavailableException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpHeaders;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.cassandra.CassandraWriteTimeoutException;
import org.springframework.data.cassandra.core.CassandraBatchOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.DeleteOptions;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.WriteResult;


@ExtendWith(MockitoExtension.class)
class KeysDaoTest {

    private static final long NOW_MICROS = 1_791_965_640_123_456L;
    private static final String NCA_ID = "test-nca-id";
    private static final KeyByOwnerAndAccountAndServiceVo KEY_BY_ACCOUNT_VO_1 =
            new KeyByOwnerAndAccountAndServiceVo(KEY_BY_OWNER_AND_SERVICE_VO_1, NCA_ID).toBuilder()
                    .ncaIdInDetails(NCA_ID).build();
    private static final KeyByOwnerAndAccountAndServiceModel KEY_BY_ACCOUNT_MODEL_1 =
            KeyByOwnerAndAccountAndServiceModel.builder()
                    .ncaId(NCA_ID)
                    .ownerType(USER)
                    .ownerId(USER_KEY_OWNER_ID_1)
                    .issuerServiceId(SERVICE_ID_1)
                    .keyId(KEY_ID_1)
                    .build();
    private static final InsertOptions DIRECT_INSERT_OPTIONS =
            InsertOptions.builder().timestamp(NOW_MICROS).idempotent(true).build();
    private static final DeleteOptions DIRECT_DELETE_OPTIONS =
            DeleteOptions.builder().timestamp(NOW_MICROS).idempotent(true).build();

    @Mock
    private RowUpdateLockDao markers;
    @Mock
    private NakProperties properties;
    @Mock
    private KeyByOwnerAndAccountAndServiceRepository accountRepository;
    @Mock
    private KeyOwnerVoBuilder keyOwnerVoBuilder;
    @Mock
    private KeyModel modelMock;
    @Mock
    private KeyRepository keyRepositoryMock;
    @Mock
    private KeyByOwnerAndServiceRepository keyByOwnerAndServiceRepositoryMock;
    @Mock
    private KeyModelConverter converterMock;
    @Mock
    private KeyByOwnerAndServiceModelConverter keyByOwnerAndServiceConverterMock;
    @Mock
    private KeyByOwnerAndAccountAndServiceModelConverter keyByAccountConverterMock;
    @Mock
    private KeyNcaIdResolver ncaIdResolverMock;
    @Mock
    private ServicesService servicesServiceMock;
    @Spy
    private MeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    @Mock
    private CassandraTemplate cassandraTemplateMock;
    @Mock
    private CassandraBatchOperations cassandraBatchOperationsMock;
    @Mock
    private WriteResult writeResultMock;
    @Mock
    private Clock clockMock;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CqlSession sessionMock;

    @InjectMocks
    private KeysDao dao;

    private final KeyModel expectedDeletedKeyModel = KeyModel.builder()
            .keyHash(API_KEY_HASH_1)
            .keyStatus(ACTIVE)
            .build();

    @Test
    void save() {
        mockSaveBatch();
        when(cassandraBatchOperationsMock.execute()).thenReturn(writeResultMock);
        when(writeResultMock.wasApplied()).thenReturn(true);
        mockReadBack();

        assertThat(dao.save(KEY_VO_1, KEY_OWNER_VO_1)).isEqualTo(expectedSavedKey());
        verify(cassandraTemplateMock, never()).insert(any(), any(InsertOptions.class));
        verify(cassandraBatchOperationsMock).insert(List.of(KEY_BY_ACCOUNT_MODEL_1));
        assertThat(writes("keys", "batch")).isEqualTo(1);
        assertThat(writes("keys_by_owner_and_service", "batch")).isEqualTo(1);
        assertThat(writes("keys_by_owner_and_account_and_service", "batch")).isEqualTo(1);
    }

    @Test
    void create_doesNotReadBack() {
        mockSaveBatch();
        when(cassandraBatchOperationsMock.execute()).thenReturn(writeResultMock);
        when(writeResultMock.wasApplied()).thenReturn(true);

        dao.create(KEY_VO_1, KEY_OWNER_VO_1);

        verify(cassandraBatchOperationsMock).execute();
        verifyNoInteractions(keyRepositoryMock, keyByOwnerAndServiceRepositoryMock);
    }

    @Test
    void save_throwsIfFailedToWrite() {
        mockSaveBatch();
        when(cassandraBatchOperationsMock.execute()).thenReturn(writeResultMock);
        when(writeResultMock.wasApplied()).thenReturn(false);

        assertThrowsExceptionWithDetails(
                CassandraException.class, () -> dao.save(KEY_VO_1, KEY_OWNER_VO_1),
                "Failed to write key into db");
        assertThat(writes("keys_by_owner_and_account_and_service", "failed")).isEqualTo(1);
    }

    @Test
    void save_writesRowsDirectlyWhenBatchlogWriteTimesOut() {
        mockSaveBatch();
        when(cassandraBatchOperationsMock.execute()).thenThrow(batchlogTimeout());
        mockReadBack();

        assertThat(dao.save(KEY_VO_1, KEY_OWNER_VO_1)).isEqualTo(expectedSavedKey());

        InOrder inOrder = inOrder(cassandraTemplateMock);
        inOrder.verify(cassandraTemplateMock).insert(
                KEY_BY_OWNER_AND_SERVICE_MODEL_1, DIRECT_INSERT_OPTIONS);
        inOrder.verify(cassandraTemplateMock).insert(
                KEY_BY_ACCOUNT_MODEL_1, DIRECT_INSERT_OPTIONS);
        inOrder.verify(cassandraTemplateMock).insert(modelMock, DIRECT_INSERT_OPTIONS);
        assertThat(writes("keys", "direct")).isEqualTo(1);
        assertThat(writes("keys_by_owner_and_service", "direct")).isEqualTo(1);
        assertThat(writes("keys_by_owner_and_account_and_service", "direct")).isEqualTo(1);
    }

    @Test
    void save_doesNotWriteKeyIfAccountRowFailsDirectly() {
        mockSaveBatch();
        when(cassandraBatchOperationsMock.execute()).thenThrow(batchlogTimeout());
        when(cassandraTemplateMock.insert(any(), any(InsertOptions.class)))
                .thenAnswer(invocation -> {
                    if (KEY_BY_ACCOUNT_MODEL_1.equals(invocation.getArgument(0))) {
                        throw writeTimeout("SIMPLE");
                    }
                    return null;
                });

        var ex = assertThrowsExceptionWithDetails(
                ServiceUnavailableException.class, () -> dao.save(KEY_VO_1, KEY_OWNER_VO_1),
                "Key storage is temporarily unavailable, the outcome of the write is unknown");

        assertThat(ex.getHeaders().containsHeader(HttpHeaders.RETRY_AFTER)).isFalse();
        verify(cassandraTemplateMock, never()).insert(eq(modelMock), any(InsertOptions.class));
        assertThat(writes("keys_by_owner_and_service", "direct")).isEqualTo(1);
        assertThat(writes("keys_by_owner_and_account_and_service", "failed")).isEqualTo(1);
        assertThat(writes("keys", "direct")).isZero();
    }

    @Test
    void save_throwsUnavailableIfDirectWriteFails() {
        mockSaveBatch();
        when(cassandraBatchOperationsMock.execute()).thenThrow(batchlogTimeout());
        when(cassandraTemplateMock.insert(eq(KEY_BY_OWNER_AND_SERVICE_MODEL_1),
                                          any(InsertOptions.class)))
                .thenThrow(writeTimeout("SIMPLE"));

        var ex = assertThrowsExceptionWithDetails(
                ServiceUnavailableException.class, () -> dao.save(KEY_VO_1, KEY_OWNER_VO_1),
                "Key storage is temporarily unavailable, the outcome of the write is unknown");
        assertThat(ex.getHeaders().containsHeader(HttpHeaders.RETRY_AFTER)).isFalse();
        verify(cassandraTemplateMock, never()).insert(eq(modelMock), any(InsertOptions.class));
    }

    @Test
    void getKeyByHash() {
        when(keyRepositoryMock.findByKeyHash(API_KEY_HASH_1)).thenReturn(Optional.of(modelMock));
        when(modelMock.getKeyStatus()).thenReturn(ACTIVE);
        when(modelMock.getKeyDetails()).thenReturn("details");
        when(converterMock.modelToVo(modelMock)).thenReturn(KEY_VO_1);

        assertThat(dao.getKeyByHash(API_KEY_HASH_1)).isEqualTo(Optional.of(KEY_VO_1));
    }

    @Test
    void deleteKey() {
        mockDeleteBatch();
        when(cassandraBatchOperationsMock.execute()).thenReturn(writeResultMock);
        when(writeResultMock.wasApplied()).thenReturn(true);

        dao.deleteKey(KEY_BY_OWNER_AND_SERVICE_VO_1);

        verify(cassandraBatchOperationsMock).delete(List.of(KEY_BY_OWNER_AND_SERVICE_MODEL_1));
        verify(cassandraBatchOperationsMock).delete(List.of(expectedDeletedKeyModel));
        verify(cassandraBatchOperationsMock).delete(List.of(KEY_BY_ACCOUNT_MODEL_1));
        verify(cassandraBatchOperationsMock).execute();
        verify(cassandraTemplateMock, never()).delete(any(), any(DeleteOptions.class));
    }

    @Test
    void deleteKey_throwsIfFailsToExecute() {
        mockDeleteBatch();
        when(cassandraBatchOperationsMock.execute()).thenReturn(writeResultMock);
        when(writeResultMock.wasApplied()).thenReturn(false);

        assertThrowsExceptionWithDetails(
                CassandraException.class, () -> dao.deleteKey(KEY_BY_OWNER_AND_SERVICE_VO_1),
                "Failed to delete key.");
    }

    @Test
    void deleteKey_deletesRowsDirectlyWhenBatchlogWriteTimesOut() {
        mockDeleteBatch();
        when(cassandraBatchOperationsMock.execute()).thenThrow(batchlogTimeout());

        dao.deleteKey(KEY_BY_OWNER_AND_SERVICE_VO_1);

        InOrder inOrder = inOrder(cassandraTemplateMock);
        inOrder.verify(cassandraTemplateMock).delete(
                expectedDeletedKeyModel, DIRECT_DELETE_OPTIONS);
        inOrder.verify(cassandraTemplateMock).delete(
                KEY_BY_OWNER_AND_SERVICE_MODEL_1, DIRECT_DELETE_OPTIONS);
        inOrder.verify(cassandraTemplateMock).delete(
                KEY_BY_ACCOUNT_MODEL_1, DIRECT_DELETE_OPTIONS);
    }

    @Test
    void deleteKey_throwsUnavailableIfDirectDeleteFails() {
        mockDeleteBatch();
        when(cassandraBatchOperationsMock.execute()).thenThrow(batchlogTimeout());
        when(cassandraTemplateMock.delete(eq(expectedDeletedKeyModel), any(DeleteOptions.class)))
                .thenThrow(writeTimeout("SIMPLE"));

        var ex = assertThrowsExceptionWithDetails(
                ServiceUnavailableException.class,
                () -> dao.deleteKey(KEY_BY_OWNER_AND_SERVICE_VO_1),
                "Key storage is temporarily unavailable, the outcome of the write is unknown");
        assertThat(ex.getHeaders().containsHeader(HttpHeaders.RETRY_AFTER)).isFalse();
    }

    @Test
    void metricsArePresentAtZeroOnFirstScrape() {
        dao.initializeMetrics();

        var scrape = ((PrometheusMeterRegistry) meterRegistry).scrape();
        for (var table : List.of("keys", "keys_by_owner_and_service",
                "keys_by_owner_and_account_and_service")) {
            for (var result : List.of("batch", "direct", "failed")) {
                assertThat(scrape).contains("nvcf_api_keys_dual_write_total{result=\""
                        + result + "\",table=\"" + table + "\"} 0.0");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void createStopsAtFailedDirectWrite(int failedRow) {
        mockSaveBatch();
        when(cassandraBatchOperationsMock.execute()).thenThrow(batchlogTimeout());
        var rows = List.of(KEY_BY_OWNER_AND_SERVICE_MODEL_1, KEY_BY_ACCOUNT_MODEL_1, modelMock);
        when(cassandraTemplateMock.insert(any(), any(InsertOptions.class)))
                .thenAnswer(invocation -> {
                    if (rows.get(failedRow).equals(invocation.getArgument(0))) {
                        throw writeTimeout("SIMPLE");
                    }
                    return null;
                });

        var ex = assertThrowsExceptionWithDetails(
                ServiceUnavailableException.class, () -> dao.create(KEY_VO_1, KEY_OWNER_VO_1),
                "Key storage is temporarily unavailable, the outcome of the write is unknown");

        assertThat(ex.getHeaders().containsHeader(HttpHeaders.RETRY_AFTER)).isFalse();
        var order = inOrder(cassandraTemplateMock);
        for (var i = 0; i <= failedRow; i++) {
            order.verify(cassandraTemplateMock).insert(rows.get(i), DIRECT_INSERT_OPTIONS);
        }
        for (var i = failedRow + 1; i < rows.size(); i++) {
            verify(cassandraTemplateMock, never()).insert(eq(rows.get(i)), any(InsertOptions.class));
        }
        verifyNoInteractions(keyRepositoryMock, keyByOwnerAndServiceRepositoryMock);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void deleteStopsAtFailedDirectWrite(int failedRow) {
        mockDeleteBatch();
        when(cassandraBatchOperationsMock.execute()).thenThrow(batchlogTimeout());
        var rows = List.of(expectedDeletedKeyModel,
                KEY_BY_OWNER_AND_SERVICE_MODEL_1, KEY_BY_ACCOUNT_MODEL_1);
        when(cassandraTemplateMock.delete(any(), any(DeleteOptions.class)))
                .thenAnswer(invocation -> {
                    if (rows.get(failedRow).equals(invocation.getArgument(0))) {
                        throw writeTimeout("SIMPLE");
                    }
                    return null;
                });

        var ex = assertThrowsExceptionWithDetails(
                ServiceUnavailableException.class,
                () -> dao.deleteKey(KEY_BY_OWNER_AND_SERVICE_VO_1),
                "Key storage is temporarily unavailable, the outcome of the write is unknown");

        assertThat(ex.getHeaders().containsHeader(HttpHeaders.RETRY_AFTER)).isFalse();
        var order = inOrder(cassandraTemplateMock);
        for (var i = 0; i <= failedRow; i++) {
            order.verify(cassandraTemplateMock).delete(rows.get(i), DIRECT_DELETE_OPTIONS);
        }
        for (var i = failedRow + 1; i < rows.size(); i++) {
            verify(cassandraTemplateMock, never()).delete(eq(rows.get(i)), any(DeleteOptions.class));
        }
    }

    @Test
    void managementReadsSwitchOnEveryMarkerChange() {
        when(markers.isCompleted(MARKER_TABLE, MARKER_RECORD)).thenReturn(false, true, false);
        when(properties.getNcaId()).thenReturn(NCA_ID);
        when(servicesServiceMock.toCanonicalId(SERVICE_ID_1)).thenReturn(SERVICE_ID_1);
        when(keyByOwnerAndServiceRepositoryMock.findByOwnerTypeAndOwnerIdAndIssuerServiceId(
                USER, USER_KEY_OWNER_ID_1, SERVICE_ID_1))
                .thenReturn(List.of(KEY_BY_OWNER_AND_SERVICE_MODEL_1));
        when(keyByOwnerAndServiceConverterMock.modelToVo(KEY_BY_OWNER_AND_SERVICE_MODEL_1))
                .thenReturn(KEY_BY_OWNER_AND_SERVICE_VO_1);
        when(accountRepository.findByNcaIdAndOwnerTypeAndOwnerIdAndIssuerServiceId(
                NCA_ID, USER, USER_KEY_OWNER_ID_1, SERVICE_ID_1)).thenReturn(List.of(KEY_BY_ACCOUNT_MODEL_1));
        var accountKey = KEY_BY_OWNER_AND_SERVICE_VO_1.toBuilder().description("account row").build();
        when(keyByAccountConverterMock.modelToVo(KEY_BY_ACCOUNT_MODEL_1)).thenReturn(accountKey);

        assertThat(dao.list(USER, USER_KEY_OWNER_ID_1, SERVICE_ID_1))
                .containsExactly(KEY_BY_OWNER_AND_SERVICE_VO_1);
        assertThat(dao.list(USER, USER_KEY_OWNER_ID_1, SERVICE_ID_1)).containsExactly(accountKey);
        assertThat(dao.list(USER, USER_KEY_OWNER_ID_1, SERVICE_ID_1))
                .containsExactly(KEY_BY_OWNER_AND_SERVICE_VO_1);
    }

    @Test
    void accountReadsDeduplicateCanonicalAliases() {
        when(markers.isCompleted(MARKER_TABLE, MARKER_RECORD)).thenReturn(true);
        when(properties.getNcaId()).thenReturn(NCA_ID);
        when(servicesServiceMock.toCanonicalId("legacy")).thenReturn(SERVICE_ID_1);
        when(servicesServiceMock.toCanonicalId(SERVICE_ID_1)).thenReturn(SERVICE_ID_1);
        when(accountRepository.findByNcaIdAndOwnerTypeAndOwnerIdAndIssuerServiceId(
                NCA_ID, USER, USER_KEY_OWNER_ID_1, SERVICE_ID_1)).thenReturn(List.of(KEY_BY_ACCOUNT_MODEL_1));
        when(keyByAccountConverterMock.modelToVo(KEY_BY_ACCOUNT_MODEL_1))
                .thenReturn(KEY_BY_OWNER_AND_SERVICE_VO_1);

        assertThat(dao.list(USER, USER_KEY_OWNER_ID_1, List.of("legacy", SERVICE_ID_1)))
                .containsExactly(KEY_BY_OWNER_AND_SERVICE_VO_1);
        verify(accountRepository).findByNcaIdAndOwnerTypeAndOwnerIdAndIssuerServiceId(
                NCA_ID, USER, USER_KEY_OWNER_ID_1, SERVICE_ID_1);
        verifyNoInteractions(keyByOwnerAndServiceRepositoryMock);
    }

    @Test
    void managementPointReadUsesAccountPartitionAndCanonicalIssuer() {
        when(markers.isCompleted(MARKER_TABLE, MARKER_RECORD)).thenReturn(true);
        when(properties.getNcaId()).thenReturn(NCA_ID);
        when(servicesServiceMock.toCanonicalId("legacy")).thenReturn(SERVICE_ID_1);
        when(accountRepository.findKey(NCA_ID, USER, USER_KEY_OWNER_ID_1, SERVICE_ID_1, KEY_ID_1))
                .thenReturn(Optional.of(KEY_BY_ACCOUNT_MODEL_1));
        when(keyByAccountConverterMock.modelToVo(KEY_BY_ACCOUNT_MODEL_1))
                .thenReturn(KEY_BY_OWNER_AND_SERVICE_VO_1);

        assertThat(dao.getKeyByOwnerAndServiceAndId(USER, USER_KEY_OWNER_ID_1, "legacy", KEY_ID_1))
                .contains(KEY_BY_OWNER_AND_SERVICE_VO_1);
        verifyNoInteractions(keyByOwnerAndServiceRepositoryMock);
    }

    @Test
    void accountReadFailureDoesNotFallBack() {
        when(markers.isCompleted(MARKER_TABLE, MARKER_RECORD)).thenReturn(true);
        when(properties.getNcaId()).thenReturn(NCA_ID);
        var failure = new IllegalStateException("account read failed");
        when(accountRepository.findByNcaIdAndOwnerTypeAndOwnerId(NCA_ID, USER, USER_KEY_OWNER_ID_1))
                .thenThrow(failure);

        assertThatThrownBy(() -> dao.list(USER, USER_KEY_OWNER_ID_1)).isSameAs(failure);
        verifyNoInteractions(keyByOwnerAndServiceRepositoryMock);
    }

    @Test
    void markerFailureDoesNotFallBack() {
        var failure = new IllegalStateException("marker read failed");
        when(markers.isCompleted(MARKER_TABLE, MARKER_RECORD)).thenThrow(failure);

        assertThatThrownBy(() -> dao.list(USER, USER_KEY_OWNER_ID_1)).isSameAs(failure);
        assertThatThrownBy(() -> dao.list(USER, USER_KEY_OWNER_ID_1, SERVICE_ID_1)).isSameAs(failure);
        assertThatThrownBy(() -> dao.list(USER, USER_KEY_OWNER_ID_1, List.of(SERVICE_ID_1)))
                .isSameAs(failure);
        assertThatThrownBy(() -> dao.getKeyByOwnerAndServiceAndId(
                USER, USER_KEY_OWNER_ID_1, SERVICE_ID_1, KEY_ID_1)).isSameAs(failure);
        verifyNoInteractions(keyByOwnerAndServiceRepositoryMock, accountRepository);
    }

    private void mockBatch() {
        when(sessionMock.getContext().getTimestampGenerator().next()).thenReturn(NOW_MICROS);
        when(cassandraTemplateMock.batchOps()).thenReturn(cassandraBatchOperationsMock);
        when(cassandraBatchOperationsMock.withTimestamp(NOW_MICROS))
                .thenReturn(cassandraBatchOperationsMock);
    }

    private void mockSaveBatch() {
        mockBatch();
        when(converterMock.voToModel(KEY_VO_1)).thenReturn(modelMock);
        when(keyByOwnerAndServiceConverterMock.voToModel(KEY_BY_OWNER_AND_SERVICE_VO_1))
                .thenReturn(KEY_BY_OWNER_AND_SERVICE_MODEL_1);
        when(cassandraBatchOperationsMock.insert(List.of(modelMock)))
                .thenReturn(cassandraBatchOperationsMock);
        when(cassandraBatchOperationsMock.insert(List.of(KEY_BY_OWNER_AND_SERVICE_MODEL_1)))
                .thenReturn(cassandraBatchOperationsMock);
        mockAccountRow();
        when(cassandraBatchOperationsMock.insert(List.of(KEY_BY_ACCOUNT_MODEL_1)))
                .thenReturn(cassandraBatchOperationsMock);
    }

    private void mockAccountRow() {
        when(servicesServiceMock.toCanonicalId(SERVICE_ID_1)).thenReturn(SERVICE_ID_1);
        when(ncaIdResolverMock.resolve(
                KEY_BY_OWNER_AND_SERVICE_VO_1.getNcaId(),
                KEY_BY_OWNER_AND_SERVICE_VO_1.getNcaIdInDetails())).thenReturn(NCA_ID);
        when(keyByAccountConverterMock.voToModel(KEY_BY_ACCOUNT_VO_1))
                .thenReturn(KEY_BY_ACCOUNT_MODEL_1);
    }

    private double writes(String table, String result) {
        return meterRegistry.counter(
                "nvcf_api_keys_dual_write_total", "table", table, "result", result).count();
    }

    private void mockDeleteBatch() {
        mockBatch();
        when(servicesServiceMock.storedServiceIds(SERVICE_ID_1)).thenReturn(Set.of(SERVICE_ID_1));
        when(keyByOwnerAndServiceConverterMock.voToModel(KEY_BY_OWNER_AND_SERVICE_VO_1))
                .thenReturn(KEY_BY_OWNER_AND_SERVICE_MODEL_1);
        when(cassandraBatchOperationsMock.delete(List.of(expectedDeletedKeyModel)))
                .thenReturn(cassandraBatchOperationsMock);
        when(cassandraBatchOperationsMock.delete(List.of(KEY_BY_OWNER_AND_SERVICE_MODEL_1)))
                .thenReturn(cassandraBatchOperationsMock);
        mockAccountRow();
        when(cassandraBatchOperationsMock.delete(List.of(KEY_BY_ACCOUNT_MODEL_1)))
                .thenReturn(cassandraBatchOperationsMock);
    }

    private void mockReadBack() {
        when(keyRepositoryMock.findByKeyHash(API_KEY_HASH_1)).thenReturn(Optional.of(modelMock));
        when(modelMock.getKeyStatus()).thenReturn(ACTIVE);
        when(modelMock.getKeyDetails()).thenReturn("details");
        when(converterMock.modelToVo(modelMock)).thenReturn(KEY_VO_1);
        when(keyByOwnerAndServiceRepositoryMock.findByOwnerTypeAndOwnerIdAndIssuerServiceIdAndKeyId(
                USER, USER_KEY_OWNER_ID_1, SERVICE_ID_1, KEY_ID_1))
                .thenReturn(Optional.of(KEY_BY_OWNER_AND_SERVICE_MODEL_1));
        when(keyByOwnerAndServiceConverterMock.modelToVo(KEY_BY_OWNER_AND_SERVICE_MODEL_1))
                .thenReturn(KEY_BY_OWNER_AND_SERVICE_VO_1);
    }

    private static SavedKeyVo expectedSavedKey() {
        return SavedKeyVo.builder()
                .keyByOwnerAndServiceVo(KEY_BY_OWNER_AND_SERVICE_VO_1)
                .keyVo(KEY_VO_1)
                .build();
    }

    private static CassandraWriteTimeoutException batchlogTimeout() {
        return writeTimeout("BATCH_LOG");
    }

    private static CassandraWriteTimeoutException writeTimeout(String writeType) {
        return new CassandraWriteTimeoutException(writeType, "Cassandra timeout during "
                + writeType + " write query at consistency TWO", null);
    }
}
