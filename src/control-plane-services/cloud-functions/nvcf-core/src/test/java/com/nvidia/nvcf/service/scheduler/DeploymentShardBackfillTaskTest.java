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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.nvidia.nvcf.persistence.function.entity.FunctionDeploymentEntity;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DeploymentShardBackfillTaskTest {

    @Test
    void rescansWithoutOverwritingPopulatedRows() {
        var session = mock(CqlSession.class);
        var select = mock(PreparedStatement.class);
        var update = mock(PreparedStatement.class);
        var selectBound = mock(BoundStatement.class);
        var updateBound = mock(BoundStatement.class);
        var rows = mock(ResultSet.class);
        var applied = mock(ResultSet.class);
        var missing = mock(Row.class);
        var populated = mock(Row.class);
        var versionId = UUID.randomUUID();
        var deploymentId = UUID.randomUUID();
        when(session.prepare("SELECT function_version_id, deployment_id, shard_id "
                + "FROM functions_deployment_v2")).thenReturn(select);
        when(session.prepare("UPDATE functions_deployment_v2 SET shard_id = ? "
                + "WHERE function_version_id = ? IF deployment_id = ? AND shard_id = null"))
                .thenReturn(update);
        when(select.bind()).thenReturn(selectBound);
        when(selectBound.setPageSize(500)).thenReturn(selectBound);
        when(session.execute(selectBound)).thenReturn(rows);
        when(rows.iterator()).thenAnswer(invocation -> List.of(missing, populated).iterator());
        when(missing.isNull("shard_id")).thenReturn(true, false);
        when(missing.getUuid("function_version_id")).thenReturn(versionId);
        when(missing.getUuid("deployment_id")).thenReturn(deploymentId);
        when(update.bind(FunctionDeploymentEntity.shardId(versionId), versionId, deploymentId))
                .thenReturn(updateBound);
        when(session.execute(updateBound)).thenReturn(applied);
        when(applied.wasApplied()).thenReturn(true);

        var task = new DeploymentShardBackfillTask(session);
        task.run();
        task.run();

        verify(session, times(1)).execute(updateBound);
        verify(session, times(2)).execute(selectBound);
        assertThat(FunctionDeploymentEntity.shardId(versionId)).isBetween(0, 127);
    }

    @Test
    void retriesScanAfterFailure() {
        var session = mock(CqlSession.class);
        when(session.prepare(any(String.class)))
                .thenThrow(new IllegalStateException("temporary failure"))
                .thenThrow(new IllegalStateException("temporary failure"));

        var task = new DeploymentShardBackfillTask(session);
        task.run();
        task.run();

        verify(session, times(2)).prepare(any(String.class));
    }

    @Test
    void retriesFailedRowOnNextRun() {
        var session = mock(CqlSession.class);
        var select = mock(PreparedStatement.class);
        var update = mock(PreparedStatement.class);
        var selectBound = mock(BoundStatement.class);
        var updateBound = mock(BoundStatement.class);
        var rows = mock(ResultSet.class);
        var applied = mock(ResultSet.class);
        var missing = mock(Row.class);
        var versionId = UUID.randomUUID();
        var deploymentId = UUID.randomUUID();
        when(session.prepare("SELECT function_version_id, deployment_id, shard_id "
                + "FROM functions_deployment_v2")).thenReturn(select);
        when(session.prepare("UPDATE functions_deployment_v2 SET shard_id = ? "
                + "WHERE function_version_id = ? IF deployment_id = ? AND shard_id = null"))
                .thenReturn(update);
        when(select.bind()).thenReturn(selectBound);
        when(selectBound.setPageSize(500)).thenReturn(selectBound);
        when(session.execute(selectBound)).thenReturn(rows);
        when(rows.iterator()).thenAnswer(invocation -> List.of(missing).iterator());
        when(missing.isNull("shard_id")).thenReturn(true);
        when(missing.getUuid("function_version_id")).thenReturn(versionId);
        when(missing.getUuid("deployment_id")).thenReturn(deploymentId);
        when(update.bind(FunctionDeploymentEntity.shardId(versionId), versionId, deploymentId))
                .thenReturn(updateBound);
        when(session.execute(updateBound))
                .thenThrow(new IllegalStateException("temporary failure"))
                .thenReturn(applied);
        when(applied.wasApplied()).thenReturn(true);

        var task = new DeploymentShardBackfillTask(session);
        task.run();
        task.run();

        verify(session, times(2)).execute(updateBound);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void backfillsLegacyWritesAfterSuccessfulScan(boolean replacedDuringFirstScan) {
        var session = mock(CqlSession.class);
        var select = mock(PreparedStatement.class);
        var update = mock(PreparedStatement.class);
        var selectBound = mock(BoundStatement.class);
        var updateBound = mock(BoundStatement.class);
        var rows = mock(ResultSet.class);
        var applied = mock(ResultSet.class);
        var missing = mock(Row.class);
        var versionId = UUID.randomUUID();
        var originalDeploymentId = UUID.randomUUID();
        var replacementDeploymentId = UUID.randomUUID();
        when(session.prepare("SELECT function_version_id, deployment_id, shard_id "
                + "FROM functions_deployment_v2")).thenReturn(select);
        when(session.prepare("UPDATE functions_deployment_v2 SET shard_id = ? "
                + "WHERE function_version_id = ? IF deployment_id = ? AND shard_id = null"))
                .thenReturn(update);
        when(select.bind()).thenReturn(selectBound);
        when(selectBound.setPageSize(500)).thenReturn(selectBound);
        when(session.execute(selectBound)).thenReturn(rows);
        when(rows.iterator()).thenReturn(
                (replacedDuringFirstScan ? List.of(missing) : List.<Row>of()).iterator(),
                List.of(missing).iterator());
        when(missing.isNull("shard_id")).thenReturn(true);
        when(missing.getUuid("function_version_id")).thenReturn(versionId);
        when(missing.getUuid("deployment_id")).thenReturn(
                replacedDuringFirstScan ? originalDeploymentId : replacementDeploymentId,
                replacementDeploymentId);
        when(update.bind(any(Integer.class), any(UUID.class), any(UUID.class)))
                .thenReturn(updateBound);
        when(session.execute(updateBound)).thenReturn(applied);
        when(applied.wasApplied()).thenReturn(!replacedDuringFirstScan, true);

        var task = new DeploymentShardBackfillTask(session);
        task.run();
        task.run();

        verify(session, times(2)).execute(selectBound);
        verify(update).bind(FunctionDeploymentEntity.shardId(versionId),
                versionId, replacementDeploymentId);
        verify(session, times(replacedDuringFirstScan ? 2 : 1)).execute(updateBound);
    }
}
