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
package com.nvidia.icms.service.scheduled.cleanup;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.nvidia.icms.configuration.bean.IcmsConfigurationProperties;
import com.nvidia.icms.inbound.rest.model.TerminateInstancesResponse;
import com.nvidia.icms.inbound.rest.model.TerminateInstancesResponse.TerminatingInstance;
import com.nvidia.icms.outbound.cassandra.instance.InstanceByDayRepo;
import com.nvidia.icms.outbound.cassandra.instance.InstanceV2Repo;
import com.nvidia.icms.outbound.cassandra.instance.InstanceV2Repository;
import com.nvidia.icms.outbound.cassandra.instance.entity.InstanceV2Entity;
import com.nvidia.icms.outbound.cassandra.request.InstanceRequestV2ByDayRepo;
import com.nvidia.icms.outbound.cassandra.request.InstanceRequestV2Repo;
import com.nvidia.icms.outbound.cassandra.request.InstanceRequestV2Repository;
import com.nvidia.icms.outbound.cassandra.request.entity.InstanceRequestV2Entity;
import com.nvidia.icms.service.LockProviderService;
import com.nvidia.icms.service.ExpiredInstanceTerminateService;
import com.nvidia.icms.service.TerminateInstanceService;
import com.nvidia.icms.service.telemetry.TelemetryEventClient;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DatabaseCleanupMainTableTest {

    @Test
    void mainTableReadsAreEnabledByDefault() {
        assertTrue(new IcmsConfigurationProperties().isDatabaseCleanupMainTableReadsEnabled());
    }

    @Test
    void mainTableCleanupKeepsByDayPartitionWhileRecordsRemain() {
        var executor = mock(DatabaseCleanupExecutor.class);
        when(executor.hasMainRecordsForDay(any())).thenReturn(true);
        runCleanup(executor, true);

        verify(executor, atLeastOnce()).cleanupRecordsFromMainTable(any());
        verify(executor, never()).getFirstRecord(any());
        verify(executor, never()).deleteRecordsByDay(any());
    }

    @Test
    void mainTableCleanupDeletesByDayPartitionAfterMainRecordsDisappear() {
        var executor = mock(DatabaseCleanupExecutor.class);
        when(executor.hasMainRecordsForDay(any())).thenReturn(true, false, true, false);
        runCleanup(executor, true);

        verify(executor, atLeastOnce()).cleanupRecordsFromMainTable(any());
        verify(executor, atLeastOnce()).deleteRecordsByDay(any());
    }

    @Test
    void disabledFlagUsesLegacyByDayDiscovery() {
        var executor = mock(DatabaseCleanupExecutor.class);
        runCleanup(executor, false);

        verify(executor, atLeastOnce()).getFirstRecord(any(Instant.class));
        verify(executor, never()).hasMainRecordsForDay(any());
        verify(executor, never()).cleanupRecordsFromMainTable(any());
    }

    @Test
    void requestScanProcessesAllResultsAndDeletesFromBothTables() {
        var repository = mock(InstanceRequestV2Repository.class);
        var mainRepo = mock(InstanceRequestV2Repo.class);
        var byDayRepo = mock(InstanceRequestV2ByDayRepo.class);
        var termination = mock(TerminateInstanceService.class);
        var day = Instant.parse("2026-01-01T00:00:00Z");
        var first = mock(InstanceRequestV2Entity.class);
        var second = mock(InstanceRequestV2Entity.class);
        when(repository.getInstanceRequestV2Repo()).thenReturn(mainRepo);
        when(repository.getInstanceRequestV2ByDayRepo()).thenReturn(byDayRepo);
        var matchingRows = requestRows("first", "second");
        var emptyRows = requestRows();
        when(mainRepo.findRequestIdsByCreationBucket(day)).thenReturn(matchingRows, emptyRows);
        when(first.getRequestId()).thenReturn("first");
        when(first.getCustomer()).thenReturn("customer");
        when(second.getRequestId()).thenReturn("second");
        when(second.getCustomer()).thenReturn("customer");
        when(repository.findRequestById("first")).thenReturn(Optional.of(first));
        when(repository.findRequestById("second")).thenReturn(Optional.of(second));
        var response = mock(TerminateInstancesResponse.class);
        when(response.getTerminatingInstances()).thenReturn(List.of());
        when(termination.terminateInstanceRequests(any(), any(), any()))
                .thenReturn(response);
        var executor = new RequestsByDayCleanupExecutor(repository, termination,
                mock(IcmsConfigurationProperties.class));

        executor.cleanupRecordsFromMainTable(day);

        verify(repository).delete(first);
        verify(repository).delete(second);
        verify(termination).terminateInstanceRequests(any(), eq(Set.of("first")), any());
        verify(termination).terminateInstanceRequests(any(), eq(Set.of("second")), any());
        executor.deleteRecordsByDay(day);
        verify(byDayRepo).deleteByKeyTruncatedTsByDay(day);
    }

    @Test
    void requestScanRetainsRequestWhileInstancesAreTerminating() {
        var repository = mock(InstanceRequestV2Repository.class);
        var mainRepo = mock(InstanceRequestV2Repo.class);
        var termination = mock(TerminateInstanceService.class);
        var day = Instant.parse("2026-01-01T00:00:00Z");
        var request = mock(InstanceRequestV2Entity.class);
        when(request.getRequestId()).thenReturn("pending");
        when(request.getCustomer()).thenReturn("customer");
        when(repository.getInstanceRequestV2Repo()).thenReturn(mainRepo);
        var matchingRows = requestRows("pending");
        when(mainRepo.findRequestIdsByCreationBucket(day)).thenReturn(matchingRows);
        when(repository.findRequestById("pending")).thenReturn(Optional.of(request));
        var response = mock(TerminateInstancesResponse.class);
        var terminating = mock(TerminatingInstance.class);
        when(response.getTerminatingInstances()).thenReturn(List.of(terminating));
        when(termination.terminateInstanceRequests(any(), any(), any())).thenReturn(response);

        new RequestsByDayCleanupExecutor(repository, termination,
                mock(IcmsConfigurationProperties.class)).cleanupRecordsFromMainTable(day);

        verify(termination, times(1))
                .terminateInstanceRequests(any(), eq(Set.of("pending")), any());
        verify(repository, never()).delete(request);
    }

    @Test
    void instanceScanProcessesAllResultsAndKeepsTerminationPath() {
        var repository = mock(InstanceV2Repository.class);
        var mainRepo = mock(InstanceV2Repo.class);
        var byDayRepo = mock(InstanceByDayRepo.class);
        var requests = mock(InstanceRequestV2Repository.class);
        var termination = mock(ExpiredInstanceTerminateService.class);
        var day = Instant.parse("2026-01-01T00:00:00Z");
        when(repository.getInstanceV2Repo()).thenReturn(mainRepo);
        when(repository.getInstanceByDayRepo()).thenReturn(byDayRepo);
        var matchingRows = instanceRows("first", "second");
        var emptyRows = instanceRows();
        when(mainRepo.findInstanceIdsByCreationBucket(day)).thenReturn(matchingRows, emptyRows);
        var first = mock(InstanceV2Entity.class);
        when(first.getInstanceId()).thenReturn("first");
        when(first.getRequestId()).thenReturn("request");
        when(repository.findInstanceById("first")).thenReturn(Optional.of(first));
        var second = mock(InstanceV2Entity.class);
        when(second.getInstanceId()).thenReturn("second");
        when(second.getRequestId()).thenReturn("request");
        when(repository.findInstanceById("second")).thenReturn(Optional.of(second));
        when(requests.findRequestById("request"))
                .thenReturn(Optional.of(mock(InstanceRequestV2Entity.class)), Optional.empty());
        var executor = new InstancesByDayCleanupExecutor(repository, requests, termination,
                mock(IcmsConfigurationProperties.class));

        executor.cleanupRecordsFromMainTable(day);

        verify(termination).terminateExpiredInstances(List.of(first));
        verify(repository).delete(second, true);
        executor.deleteRecordsByDay(day);
        verify(byDayRepo).deleteByKeyTruncatedTsByDay(day);
    }

    private ResultSet requestRows(String... ids) {
        return rows(InstanceRequestV2Entity.COLUMN_REQUEST_ID, ids);
    }

    private ResultSet instanceRows(String... ids) {
        return rows(InstanceV2Entity.COLUMN_INSTANCE_ID, ids);
    }

    private ResultSet rows(String column, String... ids) {
        var result = mock(ResultSet.class);
        var data = new ArrayList<Row>();
        for (String id : ids) {
            var row = mock(Row.class);
            when(row.getString(column)).thenReturn(id);
            data.add(row);
        }
        doReturn(data.iterator()).when(result).iterator();
        return result;
    }

    private <T> void runCleanup(DatabaseCleanupExecutor<T> executor, boolean mainReads) {
        var properties = mock(IcmsConfigurationProperties.class);
        when(properties.isDatabaseCleanupTaskEnabled()).thenReturn(true);
        when(properties.isDatabaseCleanupMainTableReadsEnabled()).thenReturn(mainReads);
        when(properties.getDatabaseCleanupLookupPeriodInDays()).thenReturn(2);
        when(properties.getDatabaseRecordsTtlInDays()).thenReturn(0);
        var locks = mock(LockProviderService.class);
        when(locks.obtainLockWithTtl(any(), anyInt())).thenReturn(true);
        var telemetry = mock(TelemetryEventClient.class);
        new DatabaseCleanupTask<T>(properties, telemetry, locks).execute(executor, "test");
    }
}
