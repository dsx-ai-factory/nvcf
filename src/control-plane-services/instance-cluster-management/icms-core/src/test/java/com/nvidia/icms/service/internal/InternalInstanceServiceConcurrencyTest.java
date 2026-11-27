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
package com.nvidia.icms.service.internal;

import static com.nvidia.icms.inbound.rest.model.SpotRequestStatusCode.PENDING_FULFILLMENT;
import static com.nvidia.icms.inbound.rest.model.SpotRequestStatusCode.PENDING_EVALUATION;
import static com.nvidia.icms.util.TestUtil.DUMMY_NON_BYOC_INSTANCE_TYPE;
import static com.nvidia.icms.util.TestUtil.DUMMY_NON_BYOC_NCA_ID;
import static com.nvidia.icms.util.TestUtil.DUMMY_REQUEST_ID;
import static com.nvidia.icms.util.TestUtil.getDummyClientRequestDataModel;
import static com.nvidia.icms.util.TestUtil.getDummyInstancePlacementValidationResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.nvidia.icms.configuration.bean.IcmsConfigurationProperties;
import com.nvidia.icms.errors.IcmsConflictException;
import com.nvidia.icms.errors.IcmsInternalServerException;
import com.nvidia.icms.errors.PreConditionFailedException;
import com.nvidia.icms.inbound.rest.model.CloudProvider;
import com.nvidia.icms.inbound.rest.model.ResourceProvider;
import com.nvidia.icms.inbound.rest.model.SpotInstanceRequestState;
import com.nvidia.icms.inbound.rest.model.SpotInstanceRequestStatusUpdateRequest;
import com.nvidia.icms.inbound.rest.model.SpotInstanceStatusUpdateRequest.InstancePlacement;
import com.nvidia.icms.inbound.rest.model.SpotRequestStatusCode;
import com.nvidia.icms.outbound.cassandra.request.InstanceRequestV2Repository;
import com.nvidia.icms.outbound.cassandra.request.entity.InstanceRequestV2Entity;
import com.nvidia.icms.outbound.cassandra.reservation.entity.ReservationEntity;
import com.nvidia.icms.outbound.cassandra.sqsmessage.SqsMessageRepository;
import com.nvidia.icms.outbound.cassandra.sqsmessage.entity.SqsMessageEntity;
import com.nvidia.icms.outbound.cassandra.sqsmessage.entity.SqsMessageKey;
import com.nvidia.icms.outbound.sqs.model.CapacityType;
import com.nvidia.icms.service.AppAuditService;
import com.nvidia.icms.service.ByocService;
import com.nvidia.icms.service.InstanceServiceHelper;
import com.nvidia.icms.service.platform.ComputePlatformTestFixtures;
import com.nvidia.icms.service.telemetry.TelemetryEventClient;
import com.nvidia.icms.util.TimeUtils;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class InternalInstanceServiceConcurrencyTest {

    private final InstanceRequestV2Repository requestRepository =
            mock(InstanceRequestV2Repository.class);
    private final SqsMessageRepository messageRepository = mock(SqsMessageRepository.class);
    private final ReservationCapacityValidationHelper reservationValidation =
            mock(ReservationCapacityValidationHelper.class);
    private final Map<String, SqsMessageEntity> claimedBatches = new ConcurrentHashMap<>();
    private InternalInstanceService firstService;
    private InternalInstanceService secondService;

    @BeforeEach
    void setUp() {
        var configuration = mock(IcmsConfigurationProperties.class);
        when(configuration.getRequestCancelDurationInMin()).thenReturn(30);
        when(configuration.isRequestStateTransitionToActiveEnabled()).thenReturn(true);
        var placementHelper = mock(InternalInstanceServiceHelper.class);
        when(placementHelper.validateInstancePlacement(any(), anyString(), eq(DUMMY_REQUEST_ID)))
                .thenAnswer(invocation -> getDummyInstancePlacementValidationResponse(
                        invocation.getArgument(0), CloudProvider.GFN, ResourceProvider.GFN));
        var instanceHelper = mock(InstanceServiceHelper.class);
        when(instanceHelper.getLaunchSpecificationForTelemetry(anyString()))
                .thenReturn(getDummyClientRequestDataModel(DUMMY_NON_BYOC_INSTANCE_TYPE,
                        DUMMY_NON_BYOC_NCA_ID, DUMMY_REQUEST_ID, CloudProvider.GFN.toString())
                        .getLaunchSpecification());
        when(requestRepository.findRequestById(DUMMY_REQUEST_ID)).thenAnswer(invocation ->
                Optional.of(requestEntity(PENDING_FULFILLMENT, true)));
        when(messageRepository.findByRequestIdAndMessageBatchId(eq(DUMMY_REQUEST_ID), anyString()))
                .thenAnswer(invocation -> Optional.ofNullable(
                        claimedBatches.get(invocation.getArgument(1))));
        doAnswer(invocation -> {
            SqsMessageEntity entity = invocation.getArgument(0);
            if (claimedBatches.putIfAbsent(entity.getKey().getMessageBatchId(), entity) != null) {
                throw new IcmsConflictException("Batch already claimed");
            }
            return null;
        }).when(messageRepository).insert(any());

        firstService = newService(configuration, placementHelper, instanceHelper);
        secondService = newService(configuration, placementHelper, instanceHelper);
    }

    @Test
    void concurrentZones_claimEachLogicalBatchOnceWithoutExceedingRequestedCount() throws Exception {
        // Every zone receives the same logical batches, whose counts sum to the original five.
        // Both service instances read before either can claim, reproducing separate-pod races.
        assertThat(claimConcurrently("batch-1", 2, "zone-a", "zone-b")).isEqualTo(1);
        assertThat(claimConcurrently("batch-2", 2, "zone-a", "zone-b")).isEqualTo(1);
        assertThat(claimConcurrently("batch-3", 1, "zone-a", "zone-b")).isEqualTo(1);

        assertThat(claimedBatches).hasSize(3);
        assertThat(claimedBatches.values().stream()
                .mapToInt(SqsMessageEntity::getAcknowledgedInstances).sum()).isEqualTo(5);
        verify(messageRepository, never()).update(any());
    }

    @Test
    void concurrentRetriesFromWinningZone_areIdempotent() throws Exception {
        assertThat(claimConcurrently("batch-1", 2, "zone-a", "zone-a")).isEqualTo(2);

        assertThat(claimedBatches).hasSize(1);
        assertThat(claimedBatches.get("batch-1").getAcknowledgedInstances()).isEqualTo(2);
        verify(messageRepository, never()).update(any());
    }

    @Test
    void concurrentFirstAcknowledgments_onlyWinningZoneUpdatesRequestStatus() throws Exception {
        when(requestRepository.findRequestById(DUMMY_REQUEST_ID)).thenAnswer(invocation ->
                Optional.of(requestEntity(PENDING_EVALUATION, false)));

        assertThat(claimConcurrently("batch-1", 2, "zone-a", "zone-b")).isEqualTo(1);

        verify(requestRepository).update(any());
        assertThat(claimedBatches).hasSize(1);
    }

    @Test
    void requestStatusWriteFailure_retryByWinningZoneRepairsRequestStatus() {
        when(requestRepository.findRequestById(DUMMY_REQUEST_ID)).thenAnswer(invocation ->
                Optional.of(requestEntity(PENDING_EVALUATION, false)));
        when(messageRepository.findByRequestId(DUMMY_REQUEST_ID))
                .thenAnswer(invocation -> List.copyOf(claimedBatches.values()));
        doThrow(new IcmsInternalServerException("Request status write failed"))
                .doNothing().when(requestRepository).update(any());

        assertThrows(IcmsInternalServerException.class, () ->
                tryClaim(firstService, "batch-1", 2, "zone-a"));
        assertThat(tryClaim(firstService, "batch-1", 2, "zone-a")).isEqualTo(1);

        verify(requestRepository, times(2)).update(any());
        assertThat(claimedBatches).hasSize(1);
        verify(messageRepository).insert(any());
    }

    @ParameterizedTest
    @EnumSource(value = SpotRequestStatusCode.class,
            names = {"PENDING_FULFILLMENT", "CANNOT_FULFILL", "SCHEDULE_EXPIRED"})
    void delayedDuplicateAcknowledgment_preservesFulfilledRequestStatus(
            SpotRequestStatusCode batchStatus) {
        var request = requestEntity(SpotRequestStatusCode.FULFILLED, true);
        request.setState(SpotInstanceRequestState.ACTIVE);
        when(requestRepository.findRequestById(DUMMY_REQUEST_ID)).thenReturn(Optional.of(request));
        var winner = terminalBatch(batchStatus);
        claimedBatches.put("batch-1", winner);
        when(messageRepository.findByRequestId(DUMMY_REQUEST_ID)).thenReturn(List.of(winner));

        assertThat(tryClaim(firstService, "batch-1", 2, "zone-a")).isEqualTo(1);

        assertThat(request.getStatusCode()).isEqualTo(SpotRequestStatusCode.FULFILLED.toString());
        assertThat(request.getState()).isEqualTo(SpotInstanceRequestState.ACTIVE);
        verify(requestRepository, never()).update(any());
    }

    @Test
    void concurrentRetriesWithDifferentCounts_preserveLegacySameZoneAcceptance() throws Exception {
        assertThat(claimConcurrently("batch-1", 2, 3, "zone-a", "zone-a")).isEqualTo(2);

        assertThat(claimedBatches).hasSize(1);
        assertThat(claimedBatches.get("batch-1").getAcknowledgedInstances()).isIn(2, 3);
        verify(messageRepository, never()).update(any());
    }

    @Test
    void repeatedAcknowledgmentWithDifferentCount_preservesOriginalClaim() {
        var winner = SqsMessageEntity.builder()
                .key(new SqsMessageKey(DUMMY_REQUEST_ID, "batch-1"))
                .acknowledgedInstances(2)
                .zone("zone-a")
                .status(PENDING_FULFILLMENT)
                .build();
        claimedBatches.put("batch-1", winner);
        when(messageRepository.findByRequestId(DUMMY_REQUEST_ID)).thenReturn(List.of(winner));

        assertThat(tryClaim(firstService, "batch-1", 3, "zone-a")).isEqualTo(1);

        assertThat(winner.getAcknowledgedInstances()).isEqualTo(2);
        verify(messageRepository, never()).insert(any());
    }

    @ParameterizedTest
    @EnumSource(value = SpotRequestStatusCode.class,
            names = {"CANNOT_FULFILL", "SCHEDULE_EXPIRED"})
    void terminalLogicalBatch_preservesLegacyCountsOnlyForWinningZone(
            SpotRequestStatusCode status) {
        var failed = terminalBatch(status);
        claimedBatches.put("batch-1", failed);
        when(messageRepository.findByRequestId(DUMMY_REQUEST_ID)).thenReturn(List.of(failed));

        assertThat(tryClaim(firstService, "batch-1", 2, "zone-a")).isEqualTo(1);
        assertThat(tryClaim(firstService, "batch-1", 2, "zone-a")).isEqualTo(1);
        assertThat(tryClaim(secondService, "batch-1", 2, "zone-b")).isZero();
        assertThat(tryClaim(firstService, "batch-1", 3, "zone-a")).isEqualTo(1);

        assertThat(claimedBatches.get("batch-1")).isEqualTo(failed);
        assertThat(failed.getStatus()).isEqualTo(status);
        verify(messageRepository, never()).insert(any());
        verify(messageRepository, never()).update(any());
    }

    @ParameterizedTest
    @EnumSource(value = SpotRequestStatusCode.class,
            names = {"CANNOT_FULFILL", "SCHEDULE_EXPIRED"})
    void staleBatchList_preservesWinningZoneRetryAfterTerminalStatus(
            SpotRequestStatusCode status) {
        var failed = terminalBatch(status);
        claimedBatches.put("batch-1", failed);
        when(messageRepository.findByRequestId(DUMMY_REQUEST_ID)).thenReturn(List.of());

        assertThat(tryClaim(firstService, "batch-1", 2, "zone-a")).isEqualTo(1);
        assertThat(tryClaim(secondService, "batch-1", 2, "zone-b")).isZero();
        assertThat(tryClaim(firstService, "batch-1", 3, "zone-a")).isEqualTo(1);

        assertThat(claimedBatches.get("batch-1")).isEqualTo(failed);
        assertThat(failed.getStatus()).isEqualTo(status);
        verify(messageRepository, times(3)).insert(any());
        verify(messageRepository, never()).update(any());
    }

    @ParameterizedTest
    @EnumSource(value = SpotRequestStatusCode.class,
            names = {"CANNOT_FULFILL", "SCHEDULE_EXPIRED"})
    void concurrentTerminalRetries_keepOriginalWinningZone(SpotRequestStatusCode status)
            throws Exception {
        var failed = terminalBatch(status);
        claimedBatches.put("batch-1", failed);

        assertThat(claimConcurrently("batch-1", 2, "zone-a", "zone-b")).isEqualTo(1);

        assertThat(claimedBatches).containsOnlyKeys("batch-1");
        assertThat(claimedBatches.get("batch-1")).isSameAs(failed);
        verify(messageRepository, times(2)).insert(any());
        verify(messageRepository, never()).update(any());
    }

    @ParameterizedTest
    @EnumSource(value = SpotRequestStatusCode.class,
            names = {"CANNOT_FULFILL", "SCHEDULE_EXPIRED"})
    void failedCreationCallback_allowsWinningZoneToRetryTheSameQueuedBatch(
            SpotRequestStatusCode failureStatus) {
        when(messageRepository.findByRequestId(DUMMY_REQUEST_ID))
                .thenAnswer(invocation -> List.copyOf(claimedBatches.values()));
        doAnswer(invocation -> {
            SqsMessageEntity entity = invocation.getArgument(0);
            claimedBatches.put(entity.getKey().getMessageBatchId(), entity);
            return null;
        }).when(messageRepository).update(any());

        assertThat(tryClaim(firstService, "batch-1", 2, "zone-a")).isEqualTo(1);
        firstService.updateInstanceRequestStatus("zone-a", DUMMY_REQUEST_ID,
                new SpotInstanceRequestStatusUpdateRequest(failureStatus,
                        "batch-1", 2, new InstancePlacement("zone-a"), null, CapacityType.SPOT),
                new HashMap<>());

        // Grapes leaves a failed creation message queued and retries its original batch ID.
        assertThat(tryClaim(firstService, "batch-1", 2, "zone-a")).isEqualTo(1);
        assertThat(tryClaim(secondService, "batch-1", 2, "zone-b")).isZero();

        assertThat(claimedBatches).containsOnlyKeys("batch-1");
        assertThat(claimedBatches.get("batch-1").getZone()).isEqualTo("zone-a");
        assertThat(claimedBatches.get("batch-1").getAcknowledgedInstances()).isEqualTo(2);
        verify(messageRepository).insert(any());
        verify(messageRepository).update(argThat(entity -> entity.getStatus() == failureStatus));
    }

    @ParameterizedTest
    @EnumSource(value = CapacityType.class, names = {"RESERVED", "RESERVED_BACKUP"})
    void failedReservationCreation_canRetryWithRefreshedQuotaWithoutChangingClaim(
            CapacityType capacityType) {
        var reservationId = UUID.randomUUID();
        var reservation = ReservationEntity.builder()
                .reservationId(reservationId)
                .ncaId(DUMMY_NON_BYOC_NCA_ID)
                .clusterId("zone-a")
                .gpuType("L40S")
                .reservedGpuCount(2)
                .availableGpuCount(2.0)
                .build();
        when(reservationValidation.validateAndGetReservationEntity(reservationId))
                .thenReturn(Optional.of(reservation));
        when(messageRepository.findByRequestId(DUMMY_REQUEST_ID))
                .thenAnswer(invocation -> List.copyOf(claimedBatches.values()));
        doAnswer(invocation -> {
            SqsMessageEntity entity = invocation.getArgument(0);
            claimedBatches.put(entity.getKey().getMessageBatchId(), entity);
            return null;
        }).when(messageRepository).update(any());
        var pending = new SpotInstanceRequestStatusUpdateRequest(PENDING_FULFILLMENT,
                "batch-1", 2, new InstancePlacement("zone-a"), reservationId, capacityType);

        firstService.updateInstanceRequestStatus("zone-a", DUMMY_REQUEST_ID,
                pending, new HashMap<>());
        firstService.updateInstanceRequestStatus("zone-a", DUMMY_REQUEST_ID,
                new SpotInstanceRequestStatusUpdateRequest(SpotRequestStatusCode.CANNOT_FULFILL,
                        "batch-1", 2, new InstancePlacement("zone-a"), reservationId, capacityType),
                new HashMap<>());

        // Grapes recomputes the servable count from current quota when retrying this message.
        pending.setInstanceCount(1);
        firstService.updateInstanceRequestStatus("zone-a", DUMMY_REQUEST_ID,
                pending, new HashMap<>());
        firstService.updateInstanceRequestStatus("zone-a", DUMMY_REQUEST_ID,
                pending, new HashMap<>());
        assertThrows(PreConditionFailedException.class, () -> secondService
                .updateInstanceRequestStatus("zone-b", DUMMY_REQUEST_ID,
                        new SpotInstanceRequestStatusUpdateRequest(PENDING_FULFILLMENT,
                                "batch-1", 1, new InstancePlacement("zone-b"), reservationId,
                                capacityType), new HashMap<>()));

        var claim = claimedBatches.get("batch-1");
        assertThat(claimedBatches).containsOnlyKeys("batch-1");
        assertThat(claim.getZone()).isEqualTo("zone-a");
        assertThat(claim.getAcknowledgedInstances()).isEqualTo(2);
        assertThat(claim.getReservationId()).isEqualTo(reservationId);
        assertThat(claim.getCapacityType()).isEqualTo(capacityType);
        verify(messageRepository).insert(any());
        verify(messageRepository).update(argThat(
                entity -> entity.getStatus() == SpotRequestStatusCode.CANNOT_FULFILL));
    }

    @ParameterizedTest
    @EnumSource(value = SpotRequestStatusCode.class,
            names = {"CANNOT_FULFILL", "SCHEDULE_EXPIRED"})
    void terminalUpdateFromLosingZone_doesNotReleaseWinningClaim(SpotRequestStatusCode status) {
        var winner = SqsMessageEntity.builder()
                .key(new SqsMessageKey(DUMMY_REQUEST_ID, "batch-1"))
                .acknowledgedInstances(2)
                .zone("zone-a")
                .status(PENDING_FULFILLMENT)
                .build();
        claimedBatches.put("batch-1", winner);
        var losingUpdate = new SpotInstanceRequestStatusUpdateRequest(status,
                "batch-1", 2, new InstancePlacement("zone-b"), null, CapacityType.SPOT);

        assertThrows(PreConditionFailedException.class, () -> firstService
                .updateInstanceRequestStatus("zone-b", DUMMY_REQUEST_ID,
                        losingUpdate, new HashMap<>()));

        assertThat(winner.getStatus()).isEqualTo(PENDING_FULFILLMENT);
        assertThat(winner.getZone()).isEqualTo("zone-a");
        assertThat(winner.getAcknowledgedInstances()).isEqualTo(2);
        verify(messageRepository, never()).update(any());
    }

    private int claimConcurrently(String batchId, int count, String firstZone, String secondZone)
            throws Exception {
        return claimConcurrently(batchId, count, count, firstZone, secondZone);
    }

    private int claimConcurrently(String batchId, int firstCount, int secondCount,
            String firstZone, String secondZone) throws Exception {
        var reads = new CyclicBarrier(2);
        doAnswer(invocation -> {
            reads.await(5, TimeUnit.SECONDS);
            return List.of();
        }).when(messageRepository).findByRequestId(DUMMY_REQUEST_ID);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() ->
                    tryClaim(firstService, batchId, firstCount, firstZone));
            var second = executor.submit(() ->
                    tryClaim(secondService, batchId, secondCount, secondZone));
            return first.get(10, TimeUnit.SECONDS) + second.get(10, TimeUnit.SECONDS);
        }
    }

    private int tryClaim(InternalInstanceService service, String batchId, int count, String zone) {
        var update = new SpotInstanceRequestStatusUpdateRequest(PENDING_FULFILLMENT,
                batchId, count, new InstancePlacement(zone), null, CapacityType.SPOT);
        try {
            service.updateInstanceRequestStatus(zone, DUMMY_REQUEST_ID, update, new HashMap<>());
            return 1;
        } catch (PreConditionFailedException expected) {
            return 0;
        }
    }

    private InternalInstanceService newService(IcmsConfigurationProperties configuration,
            InternalInstanceServiceHelper placementHelper, InstanceServiceHelper instanceHelper) {
        return new InternalInstanceService(requestRepository, mock(TelemetryEventClient.class),
                mock(AppAuditService.class), configuration, mock(InstanceUpdateService.class),
                mock(ByocService.class), messageRepository, placementHelper, instanceHelper,
                reservationValidation,
                ComputePlatformTestFixtures.nonByocComputePlatformService());
    }

    private InstanceRequestV2Entity requestEntity(SpotRequestStatusCode status, boolean trackBatches) {
        return InstanceRequestV2Entity.builder()
                .requestId(DUMMY_REQUEST_ID)
                .request("{}")
                .createTimeuuid(TimeUtils.getUuidFromTimeStamp(Instant.now()))
                .state(SpotInstanceRequestState.OPEN)
                .statusCode(status.toString())
                .checkBatchwiseInfo(trackBatches)
                .instanceCount(5)
                .resourceProvider(ResourceProvider.GFN)
                .build();
    }

    private SqsMessageEntity terminalBatch(SpotRequestStatusCode status) {
        return SqsMessageEntity.builder()
                .key(new SqsMessageKey(DUMMY_REQUEST_ID, "batch-1"))
                .acknowledgedInstances(2)
                .zone("zone-a")
                .status(status)
                .creationTime(Instant.now())
                .build();
    }
}
