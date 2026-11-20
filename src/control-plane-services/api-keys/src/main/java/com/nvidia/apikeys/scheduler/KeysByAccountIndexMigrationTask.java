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

import com.google.common.annotations.VisibleForTesting;
import com.nvidia.apikeys.config.KeysByAccountIndexMigrationProperties;
import com.nvidia.apikeys.config.NakProperties;
import com.nvidia.apikeys.persistance.dao.KeysByAccountIndexMigrationDao;
import com.nvidia.apikeys.persistance.dao.KeysByAccountIndexMigrationDao.Page;
import com.nvidia.apikeys.persistance.dao.KeysByAccountIndexMigrationDao.TimedRow;
import com.nvidia.apikeys.persistance.dao.RowUpdateLockDao;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel;
import com.nvidia.apikeys.persistance.models.KeyModel;
import com.nvidia.apikeys.services.ServicesService;
import com.nvidia.apikeys.vo.KeyByOwnerAndAccountAndServiceVo;
import com.nvidia.apikeys.vo.KeyByOwnerAndServiceVo;
import com.nvidia.boot.jwt.services.mapping.EncryptedModelConverter;
import com.nvidia.boot.migration.notification.service.DataMigration;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import jakarta.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockExtender;
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
@DataMigration(
        keyspace = "nvcf_api_keys",
        newTables = {KeyByOwnerAndAccountAndServiceModel.TABLE_NAME},
        newColumns = {KeyModel.TABLE_NAME + "." + KeyModel.COLUMN_NCA_ID,
                KeyByOwnerAndServiceModel.TABLE_NAME + "." + KeyByOwnerAndServiceModel.COLUMN_NCA_ID},
        requiresTables = {KeyModel.TABLE_NAME, KeyByOwnerAndServiceModel.TABLE_NAME, "row_update_lock", "lock"},
        description = "Copy legacy keys into the account index and backfill credential NCA IDs before M0")
public class KeysByAccountIndexMigrationTask {
    public static final String MARKER_TABLE = "keys_by_account_index_copy";
    public static final String MARKER_RECORD = "completed";
    private static final String KEYS_METRIC = "nvcf_api_keys_keys_by_account_index_copy_keys_total";
    private static final String RUNS_METRIC = "nvcf_api_keys_keys_by_account_index_copy_runs_total";
    private static final String COMPLETED_METRIC = "nvcf_api_keys_keys_by_account_index_copy_completed";
    private static final Set<String> RESULTS = Set.of("copied", "already_copied", "skipped", "unmapped",
            "conflict", "retry", "orphan", "failed");
    private static final Set<String> OUTCOMES = Set.of("clean", "changed", "failed", "interrupted");

    private final KeysByAccountIndexMigrationDao dao;
    private final RowUpdateLockDao markers;
    private final NakProperties properties;
    private final KeysByAccountIndexMigrationProperties migrationProperties;
    private final ServicesService services;
    private final EncryptedModelConverter<KeyByOwnerAndServiceModel, KeyByOwnerAndServiceVo> ownerConverter;
    private final EncryptedModelConverter<KeyByOwnerAndAccountAndServiceModel,
            KeyByOwnerAndAccountAndServiceVo> accountConverter;
    private final MeterRegistry registry;
    private final OpenTelemetry openTelemetry;
    private final Clock clock;
    private final AtomicInteger completed = new AtomicInteger();

    @PostConstruct
    void initializeMetrics() {
        RESULTS.forEach(result -> registry.counter(KEYS_METRIC, "result", result));
        OUTCOMES.forEach(outcome -> registry.counter(RUNS_METRIC, "outcome", outcome));
        registry.gauge(COMPLETED_METRIC, completed);
        completed.set(markers.isCompleted(MARKER_TABLE, MARKER_RECORD) ? 1 : 0);
    }

    public void run() {
        if (!migrationProperties.isEnabled()) {
            return;
        }
        runUnchecked();
    }

    @VisibleForTesting
    public void runUnchecked() {
        var atMost = migrationProperties.getLockAtMostFor();
        var atLeast = migrationProperties.getLockAtLeastFor();
        runUnchecked(new LockRenewal(() -> LockExtender.extendActiveLock(atMost, atLeast),
                atMost.dividedBy(3), System::nanoTime));
    }

    @VisibleForTesting
    void runUnchecked(Runnable renewLock) {
        var config = migrationProperties;
        var span = openTelemetry.getTracer("com.nvidia.apikeys.migration")
                .spanBuilder("apikeys.keys-by-account-index-copy.run").startSpan();
        span.setAttribute("nvcf.org.id", properties.getNcaId());
        var summary = new Summary();
        var outcome = "failed";
        try (var ignored = span.makeCurrent()) {
            completed.set(markers.isCompleted(MARKER_TABLE, MARKER_RECORD) ? 1 : 0);
            scan(summary, renewLock);
            if (summary.clean && completed.get() == 0) {
                pause(config.getSettleDelay(), renewLock);
                scan(summary, renewLock);
                if (summary.clean) {
                    markers.markCompleted(MARKER_TABLE, MARKER_RECORD, clock.instant());
                    completed.set(markers.isCompleted(MARKER_TABLE, MARKER_RECORD) ? 1 : 0);
                }
            }
            outcome = summary.counts.containsKey("failed") ? "failed" : summary.clean ? "clean" : "changed";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            outcome = "interrupted";
            span.setAttribute("error", true).setStatus(StatusCode.ERROR, "Interrupted");
            log.warn("Account-index migration interrupted");
        } catch (RuntimeException e) {
            span.recordException(e).setAttribute("error", true)
                    .setStatus(StatusCode.ERROR, e.getClass().getSimpleName());
            log.atError().setCause(e).log("Account-index migration failed");
        } finally {
            registry.counter(RUNS_METRIC, "outcome", outcome).increment();
            log.atInfo().addKeyValue("outcome", outcome).addKeyValue("results", summary.counts)
                    .addKeyValue("completed", completed.get() == 1)
                    .log("Account-index migration run finished");
            span.end();
        }
    }

    private void scan(Summary summary, Runnable renewLock) throws InterruptedException {
        pages(state -> dao.ownerPage(state, migrationProperties.getPageSize()), row -> copy(row, summary), renewLock);
        pages(state -> dao.accountPage(state, migrationProperties.getPageSize()), row -> removeOrphan(row, summary), renewLock);
    }

    private void copy(TimedRow<KeyByOwnerAndServiceModel> source, Summary summary) {
        var model = source.model();
        if (StringUtils.isBlank(model.getKeyId())) {
            record(summary, "skipped", false);
            return;
        }
        try {
            var key = ownerConverter.modelToVo(model);
            if (StringUtils.isBlank(key.getKeyHash())) {
                throw new IllegalStateException("Key hash is missing from owner-index details");
            }
            var credential = dao.credential(key.getKeyHash());
            var assignments = Stream.of(key.getNcaId(), key.getNcaIdInDetails(),
                            credential.map(row -> row.model().getNcaId()).orElse(null))
                    .map(StringUtils::trimToNull).filter(StringUtils::isNotBlank)
                    .collect(Collectors.toSet());
            if (assignments.size() > 1) {
                log.atError().addKeyValue("keyId", key.getKeyId())
                        .addKeyValue("ncaIds", assignments).log("Conflicting key NCA IDs block migration");
                record(summary, "conflict", true);
                return;
            }
            var ncaId = assignments.stream().findFirst().orElse(properties.getNcaId());
            if (services.getServiceById(key.getIssuerServiceId()).isEmpty()) {
                record(summary, "unmapped", false);
            }
            var target = new KeyByOwnerAndAccountAndServiceVo(key, ncaId).toBuilder()
                    .createdAt(key.getCreatedAt() == null ? null : key.getCreatedAt().truncatedTo(ChronoUnit.MILLIS))
                    .issuerServiceId(services.toCanonicalId(key.getIssuerServiceId()))
                    .audienceServiceIds(SetUtils.emptyIfNull(key.getAudienceServiceIds()).stream()
                            .map(services::toCanonicalId).collect(Collectors.toSet()))
                    .ncaIdInDetails(StringUtils.isBlank(key.getNcaIdInDetails()) ? ncaId : key.getNcaIdInDetails())
                    .build();
            var targetModel = accountConverter.voToModel(target);
            var stored = dao.account(targetModel);
            var changed = false;
            if (stored.isEmpty() || !sameAccount(stored.get().model(), target)) {
                dao.copy(targetModel, source);
                changed = true;
                var written = dao.account(targetModel);
                if (written.isEmpty() || !sameAccount(written.get().model(), target)) {
                    record(summary, "retry", true);
                    return;
                }
            }
            if (credential.isPresent() && StringUtils.isBlank(credential.get().model().getNcaId())) {
                var current = credential.get();
                dao.setCredentialNcaId(key.getKeyHash(), ncaId, current.timestamp());
                changed = true;
                var written = dao.credential(key.getKeyHash());
                if (written.isEmpty() || !ncaId.equals(written.get().model().getNcaId())) {
                    record(summary, "retry", true);
                    return;
                }
            }
            record(summary, changed ? "copied" : "already_copied", changed);
        } catch (RuntimeException e) {
            Span.current().recordException(e).setAttribute("error", true).setStatus(StatusCode.ERROR);
            log.atError().addKeyValue("keyId", model.getKeyId()).setCause(e)
                    .log("Failed to copy account-index key");
            record(summary, "failed", true);
        }
    }

    private boolean sameAccount(KeyByOwnerAndAccountAndServiceModel model,
            KeyByOwnerAndAccountAndServiceVo target) {
        return accountConverter.modelToVo(model).equals(target);
    }

    private void removeOrphan(TimedRow<KeyByOwnerAndAccountAndServiceModel> row, Summary summary) {
        var key = row.model();
        try {
            var issuers = services.storedServiceIds(key.getIssuerServiceId());
            if (dao.ownerExists(key, issuers)) {
                return;
            }
            var oldestAllowed = clock.instant().minus(migrationProperties.getOrphanMinAge()).toEpochMilli() * 1000;
            if (row.timestamp() > oldestAllowed || dao.ownerExists(key, issuers)) {
                record(summary, "retry", true);
                return;
            }
            dao.deleteOrphan(key, Math.addExact(row.timestamp(), 1));
            if (dao.account(key).isPresent()) {
                record(summary, "retry", true);
                return;
            }
            record(summary, "orphan", true);
        } catch (RuntimeException e) {
            Span.current().recordException(e).setAttribute("error", true).setStatus(StatusCode.ERROR);
            log.atError().addKeyValue("keyId", key.getKeyId()).addKeyValue("ncaId", key.getNcaId())
                    .setCause(e).log("Failed to check account-index orphan");
            record(summary, "failed", true);
        }
    }

    private <T> void pages(Function<ByteBuffer, Page<T>> fetch,
            Consumer<TimedRow<T>> process, Runnable renewLock) throws InterruptedException {
        ByteBuffer cursor = null;
        do {
            renewLock.run();
            var state = cursor;
            var page = fetch(() -> fetch.apply(state), renewLock);
            for (var row : page.rows()) {
                renewLock.run();
                process.accept(row);
            }
            renewLock.run();
            cursor = page.nextPage();
            pause(migrationProperties.getBatchDelay(), renewLock);
        } while (cursor != null);
    }

    private <T> Page<T> fetch(Supplier<Page<T>> fetch, Runnable renewLock) {
        RuntimeException failure = null;
        for (var attempt = 0; attempt < migrationProperties.getPageFetchAttempts(); attempt++) {
            renewLock.run();
            try {
                return fetch.get();
            } catch (RuntimeException e) {
                failure = e;
            }
        }
        throw failure;
    }

    private void pause(Duration delay, Runnable renewLock) throws InterruptedException {
        var remaining = delay.toMillis();
        var chunk = Math.max(1, Math.min(30_000, migrationProperties.getLockAtMostFor().toMillis() / 3));
        while (remaining > 0) {
            renewLock.run();
            var step = Math.min(remaining, chunk);
            Thread.sleep(step);
            remaining -= step;
        }
    }

    private void record(Summary summary, String result, boolean dirty) {
        registry.counter(KEYS_METRIC, "result", result).increment();
        summary.counts.merge(result, 1L, Long::sum);
        summary.clean &= !dirty;
    }

    private static class Summary {
        private final Map<String, Long> counts = new HashMap<>();
        private boolean clean = true;
    }

    static final class LockRenewal implements Runnable {
        private final Runnable extend;
        private final long intervalNanos;
        private final LongSupplier nanoTime;
        private long lastRenewal;
        private boolean renewed;

        LockRenewal(Runnable extend, Duration interval, LongSupplier nanoTime) {
            this.extend = extend;
            this.intervalNanos = interval.toNanos();
            this.nanoTime = nanoTime;
        }

        @Override
        public void run() {
            var now = nanoTime.getAsLong();
            if (!renewed || now - lastRenewal >= intervalNanos) {
                extend.run();
                lastRenewal = now;
                renewed = true;
            }
        }
    }
}
