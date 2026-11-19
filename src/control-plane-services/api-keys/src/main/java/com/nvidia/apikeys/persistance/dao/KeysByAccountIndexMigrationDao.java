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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel;
import com.nvidia.apikeys.persistance.models.KeyModel;
import com.nvidia.apikeys.persistance.repositories.KeyByOwnerAndAccountAndServiceRepository;
import com.nvidia.apikeys.persistance.repositories.KeyByOwnerAndServiceRepository;
import com.nvidia.apikeys.persistance.repositories.KeyRepository;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import lombok.RequiredArgsConstructor;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.DeleteOptions;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

@Service
@RequiredArgsConstructor
public class KeysByAccountIndexMigrationDao {
    private final CassandraTemplate template;
    private final CqlSession session;
    private final KeyByOwnerAndServiceRepository ownerRepository;
    private final KeyByOwnerAndAccountAndServiceRepository accountRepository;
    private final KeyRepository keyRepository;
    private final OpenTelemetry openTelemetry;

    public record TimedRow<T>(T model, long timestamp, Map<String, Long> writeTimes) {
        public TimedRow(T model, long timestamp) {
            this(model, timestamp, Map.of());
        }
    }
    public record Page<T>(List<TimedRow<T>> rows, ByteBuffer nextPage) { }

    public Page<KeyByOwnerAndServiceModel> ownerPage(ByteBuffer pagingState, int pageSize) {
        return page("SELECT " + KeyByOwnerAndServiceModel.COLUMN_OWNER_TYPE
                        + ", " + KeyByOwnerAndServiceModel.COLUMN_OWNER_ID
                        + ", " + KeyByOwnerAndServiceModel.COLUMN_ISSUER_SERVICE_ID
                        + ", " + KeyByOwnerAndServiceModel.COLUMN_KEY_ID
                        + ", " + KeyByOwnerAndServiceModel.COLUMN_NCA_ID
                        + ", " + KeyByOwnerAndServiceModel.COLUMN_OWNER_STATUS
                        + ", " + KeyByOwnerAndServiceModel.COLUMN_OWNER_STATUS_UPDATED_AT
                        + ", " + KeyByOwnerAndServiceModel.COLUMN_EXPIRES_AT
                        + ", " + KeyByOwnerAndServiceModel.COLUMN_DELETES_AT
                        + ", " + KeyByOwnerAndServiceModel.COLUMN_KEY_STATUS
                        + ", " + KeyByOwnerAndServiceModel.COLUMN_KEY_DETAILS
                        + ", WRITETIME(" + KeyByOwnerAndServiceModel.COLUMN_NCA_ID + ") AS wt_"
                        + KeyByOwnerAndServiceModel.COLUMN_NCA_ID
                        + ", WRITETIME(" + KeyByOwnerAndServiceModel.COLUMN_EXPIRES_AT + ") AS wt_"
                        + KeyByOwnerAndServiceModel.COLUMN_EXPIRES_AT
                        + ", WRITETIME(" + KeyByOwnerAndServiceModel.COLUMN_DELETES_AT + ") AS wt_"
                        + KeyByOwnerAndServiceModel.COLUMN_DELETES_AT
                        + ", WRITETIME(" + KeyByOwnerAndServiceModel.COLUMN_KEY_STATUS + ") AS wt_"
                        + KeyByOwnerAndServiceModel.COLUMN_KEY_STATUS
                        + ", WRITETIME(" + KeyByOwnerAndServiceModel.COLUMN_KEY_DETAILS + ") AS wt_"
                        + KeyByOwnerAndServiceModel.COLUMN_KEY_DETAILS
                        + " FROM " + KeyByOwnerAndServiceModel.TABLE_NAME,
                pagingState, pageSize, KeyByOwnerAndServiceModel.class);
    }

    public Page<KeyByOwnerAndAccountAndServiceModel> accountPage(ByteBuffer pagingState, int pageSize) {
        return page("SELECT " + KeyByOwnerAndAccountAndServiceModel.COLUMN_NCA_ID
                        + ", " + KeyByOwnerAndAccountAndServiceModel.COLUMN_OWNER_TYPE
                        + ", " + KeyByOwnerAndAccountAndServiceModel.COLUMN_OWNER_ID
                        + ", " + KeyByOwnerAndAccountAndServiceModel.COLUMN_ISSUER_SERVICE_ID
                        + ", " + KeyByOwnerAndAccountAndServiceModel.COLUMN_KEY_ID
                        + ", " + KeyByOwnerAndAccountAndServiceModel.COLUMN_KEY_STATUS
                        + ", " + KeyByOwnerAndAccountAndServiceModel.COLUMN_CREATED_AT
                        + ", " + KeyByOwnerAndAccountAndServiceModel.COLUMN_EXPIRES_AT
                        + ", " + KeyByOwnerAndAccountAndServiceModel.COLUMN_DELETES_AT
                        + ", " + KeyByOwnerAndAccountAndServiceModel.COLUMN_KEY_DETAILS
                        + ", WRITETIME(" + KeyByOwnerAndAccountAndServiceModel.COLUMN_KEY_STATUS + ") AS wt_"
                        + KeyByOwnerAndAccountAndServiceModel.COLUMN_KEY_STATUS
                        + ", WRITETIME(" + KeyByOwnerAndAccountAndServiceModel.COLUMN_CREATED_AT + ") AS wt_"
                        + KeyByOwnerAndAccountAndServiceModel.COLUMN_CREATED_AT
                        + ", WRITETIME(" + KeyByOwnerAndAccountAndServiceModel.COLUMN_EXPIRES_AT + ") AS wt_"
                        + KeyByOwnerAndAccountAndServiceModel.COLUMN_EXPIRES_AT
                        + ", WRITETIME(" + KeyByOwnerAndAccountAndServiceModel.COLUMN_DELETES_AT + ") AS wt_"
                        + KeyByOwnerAndAccountAndServiceModel.COLUMN_DELETES_AT
                        + ", WRITETIME(" + KeyByOwnerAndAccountAndServiceModel.COLUMN_KEY_DETAILS + ") AS wt_"
                        + KeyByOwnerAndAccountAndServiceModel.COLUMN_KEY_DETAILS
                        + " FROM " + KeyByOwnerAndAccountAndServiceModel.TABLE_NAME,
                pagingState, pageSize, KeyByOwnerAndAccountAndServiceModel.class);
    }

    public Optional<TimedRow<KeyModel>> credential(String hash) {
        return Optional.ofNullable(traced("read-row", () -> keyRepository.findMigrationRow(hash).one()))
                .map(row -> map(row, KeyModel.class));
    }

    public Optional<TimedRow<KeyByOwnerAndAccountAndServiceModel>> account(
            KeyByOwnerAndAccountAndServiceModel key) {
        return Optional.ofNullable(traced("read-row", () -> accountRepository.findMigrationRow(
                        key.getNcaId(), key.getOwnerType(), key.getOwnerId(),
                        key.getIssuerServiceId(), key.getKeyId()).one()))
                .map(row -> map(row, KeyByOwnerAndAccountAndServiceModel.class));
    }

    public boolean ownerExists(KeyByOwnerAndAccountAndServiceModel key, Set<String> issuerIds) {
        for (var issuer : issuerIds) {
            if (traced("read-row", () -> ownerRepository.findByOwnerTypeAndOwnerIdAndIssuerServiceIdAndKeyId(
                    key.getOwnerType(), key.getOwnerId(), issuer, key.getKeyId())).isPresent()) {
                return true;
            }
        }
        return false;
    }

    public void copy(KeyByOwnerAndAccountAndServiceModel model, long timestamp) {
        traced("copy-account", () -> template.insert(model,
                InsertOptions.builder().timestamp(timestamp).withInsertNulls(false).idempotent(true).build()));
    }

    public void copy(KeyByOwnerAndAccountAndServiceModel model, TimedRow<KeyByOwnerAndServiceModel> source) {
        var writes = new LinkedHashMap<Long, KeyByOwnerAndAccountAndServiceModel>();
        var times = source.writeTimes();
        var detailsTime = times.get(KeyByOwnerAndServiceModel.COLUMN_KEY_DETAILS);
        Assert.notNull(detailsTime, "Source key details must have a write timestamp");
        // Write decryptable details first so an interrupted copy can be repaired on the next scan.
        var details = copyAt(writes, model, detailsTime);
        details.setKeyDetails(model.getKeyDetails());
        details.setCreatedAt(model.getCreatedAt());
        if (times.containsKey(KeyByOwnerAndServiceModel.COLUMN_KEY_STATUS)) {
            copyAt(writes, model, times.get(KeyByOwnerAndServiceModel.COLUMN_KEY_STATUS))
                    .setKeyStatus(model.getKeyStatus());
        }
        if (times.containsKey(KeyByOwnerAndServiceModel.COLUMN_EXPIRES_AT)) {
            copyAt(writes, model, times.get(KeyByOwnerAndServiceModel.COLUMN_EXPIRES_AT))
                    .setExpiresAt(model.getExpiresAt());
        }
        if (times.containsKey(KeyByOwnerAndServiceModel.COLUMN_DELETES_AT)) {
            copyAt(writes, model, times.get(KeyByOwnerAndServiceModel.COLUMN_DELETES_AT))
                    .setDeletesAt(model.getDeletesAt());
        }
        // Unset fields stay absent; never fabricate a timestamp for a null source cell.
        writes.forEach((timestamp, partial) -> copy(partial, timestamp));
    }

    private KeyByOwnerAndAccountAndServiceModel copyAt(
            Map<Long, KeyByOwnerAndAccountAndServiceModel> writes,
            KeyByOwnerAndAccountAndServiceModel key, long timestamp) {
        return writes.computeIfAbsent(timestamp, ignored -> KeyByOwnerAndAccountAndServiceModel.builder()
                .ncaId(key.getNcaId()).ownerType(key.getOwnerType()).ownerId(key.getOwnerId())
                .issuerServiceId(key.getIssuerServiceId()).keyId(key.getKeyId()).build());
    }

    public void deleteOrphan(KeyByOwnerAndAccountAndServiceModel model, long timestamp) {
        traced("delete-orphan", () -> template.delete(model,
                DeleteOptions.builder().timestamp(timestamp).idempotent(true).build()));
    }

    public void setCredentialNcaId(String hash, String ncaId, long timestamp) {
        traced("assign-nca", () -> keyRepository.setNcaIdAtTimestamp(hash, ncaId, timestamp));
    }

    private <T> Page<T> page(String cql, ByteBuffer pagingState, int pageSize, Class<T> type) {
        var query = SimpleStatement.builder(cql)
                .setPageSize(pageSize)
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
                .setIdempotence(true);
        if (pagingState != null) {
            query.setPagingState(pagingState.duplicate());
        }
        var result = traced("scan-page", () -> session.execute(query.build()));
        var available = result.getAvailableWithoutFetching();
        var iterator = result.iterator();
        var rows = new ArrayList<TimedRow<T>>(available);
        for (var i = 0; i < available; i++) {
            rows.add(map(iterator.next(), type));
        }
        return new Page<>(List.copyOf(rows), result.getExecutionInfo().getPagingState());
    }

    private <T> TimedRow<T> map(Row row, Class<T> type) {
        var writeTimes = new HashMap<String, Long>();
        StreamSupport.stream(row.getColumnDefinitions().spliterator(), false)
                .map(column -> column.getName().asInternal())
                .filter(column -> column.startsWith("wt_") && !row.isNull(column))
                .forEach(column -> writeTimes.put(column.substring(3), row.getLong(column)));
        var timestamp = writeTimes.values().stream().mapToLong(Long::longValue).max().orElse(0);
        return new TimedRow<>(template.getConverter().read(type, row), timestamp, Map.copyOf(writeTimes));
    }

    private <T> T traced(String operation, Supplier<T> action) {
        var span = openTelemetry.getTracer("com.nvidia.apikeys.migration")
                .spanBuilder("apikeys.keys-by-account-index-copy." + operation)
                .setSpanKind(SpanKind.CLIENT).startSpan();
        try (var ignored = span.makeCurrent()) {
            return action.get();
        } catch (RuntimeException e) {
            span.recordException(e).setAttribute("error", true).setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }
}
