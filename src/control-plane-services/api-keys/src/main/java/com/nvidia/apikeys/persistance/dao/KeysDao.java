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

import static com.nvidia.apikeys.scheduler.KeysByAccountIndexMigrationTask.MARKER_RECORD;
import static com.nvidia.apikeys.scheduler.KeysByAccountIndexMigrationTask.MARKER_TABLE;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.nvidia.apikeys.config.NakProperties;
import com.nvidia.apikeys.config.exceptions.CassandraException;
import com.nvidia.apikeys.converters.KeyOwnerVoBuilder;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel;
import com.nvidia.apikeys.persistance.models.KeyModel;
import com.nvidia.apikeys.persistance.repositories.KeyByOwnerAndAccountAndServiceRepository;
import com.nvidia.apikeys.persistance.repositories.KeyByOwnerAndServiceRepository;
import com.nvidia.apikeys.persistance.repositories.KeyRepository;
import com.nvidia.apikeys.services.KeyNcaIdResolver;
import com.nvidia.apikeys.services.ServicesService;
import com.nvidia.apikeys.vo.KeyByOwnerAndAccountAndServiceVo;
import com.nvidia.apikeys.vo.KeyByOwnerAndServiceVo;
import com.nvidia.apikeys.vo.KeyOwnerType;
import com.nvidia.apikeys.vo.KeyOwnerVo;
import com.nvidia.apikeys.vo.KeyVo;
import com.nvidia.apikeys.vo.SavedKeyVo;
import com.nvidia.boot.exceptions.ServiceUnavailableException;
import com.nvidia.boot.exceptions.UnprocessableEntityException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.data.cassandra.core.CassandraBatchOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.DeleteOptions;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KeysDao {

    private final KeyRepository keyRepository;
    private final KeyByOwnerAndAccountAndServiceRepository accountRepository;
    private final RowUpdateLockDao rowUpdateLockDao;
    private final NakProperties properties;
    private final KeyByOwnerAndServiceRepository keyByOwnerAndServiceRepository;
    private final KeyModelConverter keyConverter;
    private final KeyByOwnerAndServiceModelConverter keyByOwnerAndServiceConverter;
    private final KeyByOwnerAndAccountAndServiceModelConverter keyByAccountConverter;
    private final KeyNcaIdResolver ncaIdResolver;
    private final ServicesService servicesService;
    private final MeterRegistry meterRegistry;
    private final KeyOwnerVoBuilder keyOwnerVoBuilder;
    private final Clock clock;
    private final CassandraTemplate cassandraTemplate;
    private final CqlSession session;

    private static final String DUAL_WRITE_METRIC = "nvcf_api_keys_dual_write_total";
    private static final String RESULT_BATCH = "batch";
    private static final String RESULT_DIRECT = "direct";
    private static final String RESULT_FAILED = "failed";
    private static final List<String> WRITTEN_TABLES = List.of(
            KeyModel.TABLE_NAME,
            KeyByOwnerAndServiceModel.TABLE_NAME,
            KeyByOwnerAndAccountAndServiceModel.TABLE_NAME);
    private static final List<String> WRITE_RESULTS = List.of(
            RESULT_BATCH, RESULT_DIRECT, RESULT_FAILED);

    @PostConstruct
    void initializeMetrics() {
        WRITTEN_TABLES.forEach(table -> WRITE_RESULTS.forEach(
                result -> dualWriteCounter(table, result)));
    }

    /**
     * Writes a new key without reading it back. A failed read after a successful write would
     * report a persisted key as not created.
     */
    public void create(KeyVo key, KeyOwnerVo owner) {
        write(key, owner);
    }

    public SavedKeyVo save(KeyVo key, KeyOwnerVo owner) {
        write(key, owner);

        // read saved values back
        key = getKeyByHash(key.getKeyHash())
                .orElseThrow(() -> new CassandraException("Failed to read saved key"));

        var keyByOwnerAndServiceVo = getKeyByOwnerAndServiceAndId(
                key.getOwnerType(), key.getOwnerId(), key.getIssuerServiceId(), key.getKeyId())
                .orElseThrow(() -> new CassandraException("Failed to read saved key owner"));

        return SavedKeyVo.builder()
                .keyVo(key)
                .keyByOwnerAndServiceVo(keyByOwnerAndServiceVo)
                .build();
    }

    private void write(KeyVo key, KeyOwnerVo owner) {
        // write key record
        KeyModel keyModel = keyConverter.voToModel(key);

        // populate reverse lookups
        KeyByOwnerAndServiceVo keyByOwnerAndServiceVo = new KeyByOwnerAndServiceVo(key, owner);
        var keyByOwnerAndServiceModel = keyByOwnerAndServiceConverter.voToModel(
                keyByOwnerAndServiceVo);
        var keyByAccountModel = toAccountModel(keyByOwnerAndServiceVo);

        long timestamp = writeTimestamp();
        CassandraBatchOperations batchOperations = cassandraTemplate.batchOps()
                .withTimestamp(timestamp)
                .insert(List.of(keyModel))
                .insert(List.of(keyByOwnerAndServiceModel))
                .insert(List.of(keyByAccountModel));

        try {
            if (!batchOperations.execute().wasApplied()) {
                throw new CassandraException("Failed to write key into db");
            }
            recordWrites(WRITTEN_TABLES, RESULT_BATCH);
        } catch (TransientDataAccessException | DataAccessResourceFailureException e) {
            log.atWarn().addKeyValue("keyId", key.getKeyId())
                    .addKeyValue("ncaId", keyByAccountModel.getNcaId())
                    .setCause(e).log("Logged batch write failed, writing rows directly");
            InsertOptions options = InsertOptions.builder()
                    .timestamp(timestamp)
                    .idempotent(true)
                    .build();
            // list the key before making it valid, so a partial write never leaves a hidden key
            writeDirectly(
                    () -> cassandraTemplate.insert(keyByOwnerAndServiceModel, options),
                    KeyByOwnerAndServiceModel.TABLE_NAME);
            writeDirectly(
                    () -> cassandraTemplate.insert(keyByAccountModel, options),
                    KeyByOwnerAndAccountAndServiceModel.TABLE_NAME);
            writeDirectly(
                    () -> cassandraTemplate.insert(keyModel, options),
                    KeyModel.TABLE_NAME);
        } catch (RuntimeException e) {
            recordWrites(WRITTEN_TABLES, RESULT_FAILED);
            throw e;
        }
    }

    private KeyByOwnerAndAccountAndServiceModel toAccountModel(KeyByOwnerAndServiceVo key) {
        var ncaId = ncaIdResolver.resolve(key.getNcaId(), key.getNcaIdInDetails());
        var accountKey = new KeyByOwnerAndAccountAndServiceVo(key, ncaId).toBuilder()
                .ncaIdInDetails(ncaId)
                .issuerServiceId(servicesService.toCanonicalId(key.getIssuerServiceId()))
                .audienceServiceIds(key.getAudienceServiceIds().stream()
                        .map(servicesService::toCanonicalId)
                        .collect(Collectors.toSet()))
                .build();
        return keyByAccountConverter.voToModel(accountKey);
    }

    public Optional<KeyVo> getKeyByHash(String hash) {
        // a row holding only nca_id is not a key
        return keyRepository.findByKeyHash(hash)
                .filter(model -> model.getKeyStatus() != null && model.getKeyDetails() != null)
                .map(this::modelToValidatedVo);
    }

    public Optional<KeyOwnerVo> getKeyOwner(KeyOwnerType type, String subject) {
        // need to be careful here when listing without service id to get key owner status
        // you're no guaranteed to have any keys so user may come back with all fields as null
        // except for partition key and static column values that will stick forever
        return keyByOwnerAndServiceRepository.findFirstByOwnerTypeAndOwnerId(type, subject)
                .map(keyOwnerVoBuilder::getKeyOwnerVoFromModel);
    }

    public Optional<KeyByOwnerAndServiceVo> getKeyByOwnerAndServiceAndId(
            KeyOwnerType type, String ownerId, String serviceId, String keyId) {
        if (rowUpdateLockDao.isCompleted(MARKER_TABLE, MARKER_RECORD)) {
            return accountRepository.findKey(properties.getNcaId(), type, ownerId,
                            servicesService.toCanonicalId(serviceId), keyId)
                    .map(keyByAccountConverter::modelToVo);
        }
        return keyByOwnerAndServiceRepository.findByOwnerTypeAndOwnerIdAndIssuerServiceIdAndKeyId(
                        type, ownerId, serviceId, keyId)
                .map(this::modelToValidatedVo);
    }

    public KeyOwnerVo save(KeyOwnerVo vo) {
        ResultSet resultSet = keyByOwnerAndServiceRepository.updateKeyOwner(
                vo.getOwnerType(), vo.getOwnerId(),
                vo.getOwnerStatus(), vo.getOwnerStatusUpdatedAt());

        if (resultSet == null || !resultSet.wasApplied()) {
            throw new UnprocessableEntityException("failed to update key owner");
        }

        return getKeyOwner(vo.getOwnerType(), vo.getOwnerId())
                .orElseThrow(() -> new CassandraException("failed to read saved key owner"));
    }

    public void deleteKey(KeyByOwnerAndServiceVo key) {
        KeyModel keyModel = KeyModel.builder()
                .keyHash(key.getKeyHash())
                .keyStatus(key.getKeyStatus())
                .build();

        var keyByOwnerAndServiceModel = keyByOwnerAndServiceConverter.voToModel(key);
        // Account rows use canonical issuers; legacy rows may still use any stored alias.
        var ownerModels = servicesService.storedServiceIds(key.getIssuerServiceId()).stream()
                .map(issuer -> keyByOwnerAndServiceModel.toBuilder().issuerServiceId(issuer).build())
                .toList();
        var keyByAccountModel = toAccountModel(key);

        long timestamp = writeTimestamp();
        CassandraBatchOperations batchOperations = cassandraTemplate.batchOps()
                .withTimestamp(timestamp)
                .delete(List.of(keyModel))
                .delete(ownerModels)
                .delete(List.of(keyByAccountModel));

        try {
            if (!batchOperations.execute().wasApplied()) {
                throw new CassandraException("Failed to delete key.");
            }
            recordWrites(WRITTEN_TABLES, RESULT_BATCH);
        } catch (TransientDataAccessException | DataAccessResourceFailureException e) {
            log.atWarn().addKeyValue("keyId", key.getKeyId())
                    .addKeyValue("ncaId", keyByAccountModel.getNcaId())
                    .setCause(e).log("Logged batch delete failed, deleting rows directly");
            DeleteOptions options = DeleteOptions.builder()
                    .timestamp(timestamp)
                    .idempotent(true)
                    .build();
            // revoke the key before removing it from the owner's lists
            writeDirectly(
                    () -> cassandraTemplate.delete(keyModel, options),
                    KeyModel.TABLE_NAME);
            for (var ownerModel : ownerModels) {
                writeDirectly(
                        () -> cassandraTemplate.delete(ownerModel, options),
                        KeyByOwnerAndServiceModel.TABLE_NAME);
            }
            writeDirectly(
                    () -> cassandraTemplate.delete(keyByAccountModel, options),
                    KeyByOwnerAndAccountAndServiceModel.TABLE_NAME);
        } catch (RuntimeException e) {
            recordWrites(WRITTEN_TABLES, RESULT_FAILED);
            throw e;
        }
    }

    // Reuse the batch timestamp because a timed-out batch may still replay.
    // Do not advertise a retry: another create generates a different key.
    private void writeDirectly(Runnable write, String table) {
        try {
            write.run();
            recordWrites(List.of(table), RESULT_DIRECT);
        } catch (TransientDataAccessException | DataAccessResourceFailureException e) {
            recordWrites(List.of(table), RESULT_FAILED);
            throw ServiceUnavailableException.withoutRetryAfter(
                    "Key storage is temporarily unavailable, the outcome of the write is unknown", e);
        } catch (RuntimeException e) {
            recordWrites(List.of(table), RESULT_FAILED);
            throw e;
        }
    }

    private void recordWrites(List<String> tables, String result) {
        tables.forEach(table -> dualWriteCounter(table, result).increment());
    }

    private Counter dualWriteCounter(String table, String result) {
        return meterRegistry.counter(DUAL_WRITE_METRIC, "table", table, "result", result);
    }

    private long writeTimestamp() {
        return session.getContext().getTimestampGenerator().next();
    }

    public List<KeyByOwnerAndServiceVo> list(
            KeyOwnerType ownerType, String ownerId, String serviceId) {
        if (rowUpdateLockDao.isCompleted(MARKER_TABLE, MARKER_RECORD)) {
            return listByAccount(ownerType, ownerId, servicesService.toCanonicalId(serviceId));
        }
        return listByService(ownerType, ownerId, serviceId);
    }

    private List<KeyByOwnerAndServiceVo> listByService(
            KeyOwnerType ownerType, String ownerId, String serviceId) {
        return keyByOwnerAndServiceRepository
                .findByOwnerTypeAndOwnerIdAndIssuerServiceId(ownerType, ownerId, serviceId)
                .stream()
                .filter(Objects::nonNull)
                .map(this::modelToValidatedVo)
                .toList();
    }

    public List<KeyByOwnerAndServiceVo> list(
            KeyOwnerType ownerType, String ownerId, Collection<String> serviceIds) {
        if (rowUpdateLockDao.isCompleted(MARKER_TABLE, MARKER_RECORD)) {
            return serviceIds.stream()
                    .map(servicesService::toCanonicalId)
                    .distinct()
                    .flatMap(serviceId -> listByAccount(ownerType, ownerId, serviceId).stream())
                    .toList();
        }
        return serviceIds.stream()
                .flatMap(serviceId -> listByService(ownerType, ownerId, serviceId).stream())
                .toList();
    }

    private List<KeyByOwnerAndServiceVo> listByAccount(
            KeyOwnerType ownerType, String ownerId, String serviceId) {
        return accountRepository.findByNcaIdAndOwnerTypeAndOwnerIdAndIssuerServiceId(
                        properties.getNcaId(), ownerType, ownerId, serviceId).stream()
                .map(keyByAccountConverter::modelToVo)
                .toList();
    }

    public List<KeyByOwnerAndServiceVo> list(KeyOwnerType ownerType, String ownerId) {
        if (rowUpdateLockDao.isCompleted(MARKER_TABLE, MARKER_RECORD)) {
            return accountRepository.findByNcaIdAndOwnerTypeAndOwnerId(
                            properties.getNcaId(), ownerType, ownerId).stream()
                    .map(keyByAccountConverter::modelToVo)
                    .toList();
        }
        return keyByOwnerAndServiceRepository
                .findByOwnerTypeAndOwnerId(ownerType, ownerId)
                .stream()
                .filter(Objects::nonNull)
                // we may get single record (user part, static columns) even when there are no keys
                // make sure only actual keys make it into response
                .filter(key -> key.getKeyId() != null)
                .map(this::modelToValidatedVo)
                .toList();
    }


    private KeyVo modelToValidatedVo(KeyModel keyModel) {
        return keyConverter.modelToVo(keyModel);
    }

    private KeyByOwnerAndServiceVo modelToValidatedVo(KeyByOwnerAndServiceModel model) {
        return keyByOwnerAndServiceConverter.modelToVo(model);
    }
}
