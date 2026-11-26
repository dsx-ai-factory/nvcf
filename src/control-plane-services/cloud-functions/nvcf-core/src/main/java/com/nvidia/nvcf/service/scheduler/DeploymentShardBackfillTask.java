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
package com.nvidia.nvcf.service.scheduler;

import com.datastax.oss.driver.api.core.CqlSession;
import com.nvidia.boot.migration.notification.service.DataMigration;
import com.nvidia.nvcf.persistence.function.entity.FunctionDeploymentEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
@DataMigration(
        keyspace = "nvcf_api",
        newColumns = {"functions_deployment_v2.shard_id"},
        requiresTables = {"functions_deployment_v2"},
        description = "Populate shard_id for existing function deployments")
public class DeploymentShardBackfillTask {

    private static final int PAGE_SIZE = 500;

    private final CqlSession session;

    public void run() {
        long scanned = 0;
        long updated = 0;
        long skipped = 0;
        long failed = 0;
        try {
            var select = session.prepare("SELECT function_version_id, deployment_id, shard_id "
                    + "FROM functions_deployment_v2");
            // The condition prevents a concurrent write (or delete) from being overwritten.
            var update = session.prepare("UPDATE functions_deployment_v2 SET shard_id = ? "
                    + "WHERE function_version_id = ? IF deployment_id = ? AND shard_id = null");
            var rows = session.execute(select.bind().setPageSize(PAGE_SIZE));
            for (var row : rows) {
                scanned++;
                if (!row.isNull("shard_id")) {
                    skipped++;
                } else {
                    try {
                        var versionId = row.getUuid("function_version_id");
                        var deploymentId = row.getUuid("deployment_id");
                        var result = session.execute(update.bind(
                                FunctionDeploymentEntity.shardId(versionId), versionId, deploymentId));
                        if (result.wasApplied()) {
                            updated++;
                        } else {
                            skipped++;
                        }
                    } catch (Exception ex) {
                        failed++;
                        log.error("Failed to backfill deployment shard for function version {}",
                                row.getUuid("function_version_id"), ex);
                    }
                }
                if (scanned % PAGE_SIZE == 0) {
                    log.info("Deployment shard backfill progress: scanned={}, updated={}, skipped={}, failed={}",
                            scanned, updated, skipped, failed);
                }
            }
        } catch (Exception ex) {
            failed++;
            log.error("Failed to scan deployments for shard backfill", ex);
        } finally {
            log.info("Deployment shard backfill: scanned={}, updated={}, skipped={}, failed={}",
                    scanned, updated, skipped, failed);
        }
    }
}
