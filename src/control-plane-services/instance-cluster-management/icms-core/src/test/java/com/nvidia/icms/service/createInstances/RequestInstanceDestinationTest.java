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
package com.nvidia.icms.service.createInstances;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.nvidia.icms.outbound.cassandra.byoc.entity.InstanceTypeV5Udt;
import com.nvidia.icms.outbound.sqs.model.CapacityType;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class RequestInstanceDestinationTest {

    @Test
    void getDestinationsForPublishing_emptyInput_returnsEmptyList() {
        assertEquals(List.of(), RequestInstanceDestination.getDestinationsForPublishing(Set.of()));
    }

    @Test
    void getDestinationsForPublishing_missingCapacityType_isNotPublished() {
        var unsupported = destination().toBuilder().capacityType(null).build();

        assertEquals(List.of(),
                RequestInstanceDestination.getDestinationsForPublishing(Set.of(unsupported)));
    }

    @Test
    void getDestinationsForPublishing_ignoresClusterAndRegionMetadata() {
        var first = destination();
        var duplicate = first.toBuilder().clusterId("other-zone").clusterName("other-cluster")
                .region("other-region").clusterGroupId("other-group-id")
                .clusterGroupName("other-group").ncaId("other-owner")
                .authorizedNcaIds(Set.of("other-account")).build();

        assertEquals(1, RequestInstanceDestination.getDestinationsForPublishing(
                Set.of(first, duplicate)).size());
    }

    @Test
    void getDestinationsForPublishing_missingInstanceType_isNullSafe() {
        var first = destination().toBuilder().instanceType(null).build();
        var duplicate = first.toBuilder().clusterId("other-zone").build();

        assertEquals(1, RequestInstanceDestination.getDestinationsForPublishing(
                Set.of(first, duplicate)).size());
    }

    @Test
    void getDestinationsForPublishing_preservesCapacityPriority() {
        var spot = destination();
        var reserved = spot.toBuilder().capacityType(CapacityType.RESERVED).build();
        var backup = spot.toBuilder().capacityType(CapacityType.RESERVED_BACKUP).build();
        var destinations = new LinkedHashSet<>(List.of(spot, backup, reserved));

        assertEquals(List.of(reserved, backup, spot),
                RequestInstanceDestination.getDestinationsForPublishing(destinations));
    }

    @ParameterizedTest
    @ValueSource(strings = {"instance-type", "instance-value", "gpu", "count", "batch"})
    void getDestinationsForPublishing_spotPayloadAlternativesOnSameQueue_publishOnce(
            String alternative) {
        var first = destination();
        var second = first.toBuilder().build();
        switch (alternative) {
            case "instance-type" -> second.setInstanceType(InstanceTypeV5Udt.builder()
                    .name("other-type").value("type-value").build());
            case "instance-value" -> second.setInstanceType(InstanceTypeV5Udt.builder()
                    .name("type-name").value("other-value").build());
            case "gpu" -> second.setGpuName("other-gpu");
            case "count" -> second.setMaxFulfillableInstances(2);
            case "batch" -> second.setInstanceBatchCount(2);
            default -> throw new IllegalArgumentException(alternative);
        }

        assertEquals(1, RequestInstanceDestination.getDestinationsForPublishing(
                Set.of(first, second)).size());
    }

    @Test
    void getDestinationsForPublishing_spotZoneQueues_publishEachOnce() {
        var first = destination();
        var second = first.toBuilder().clusterId("other-zone")
                .creationQueueUrl("other-zone-queue").build();

        assertEquals(Set.of(first, second), Set.copyOf(
                RequestInstanceDestination.getDestinationsForPublishing(Set.of(first, second))));
    }

    @ParameterizedTest
    @EnumSource(value = CapacityType.class, names = {"RESERVED", "RESERVED_BACKUP"})
    void getDestinationsForPublishing_reservedMetadataVariants_areRetained(
            CapacityType capacityType) {
        var first = destination().toBuilder().capacityType(capacityType)
                .reservationId(UUID.randomUUID()).build();
        var duplicate = first.toBuilder().clusterId("other-zone").region("other-region").build();

        assertEquals(Set.of(first, duplicate), Set.copyOf(
                RequestInstanceDestination.getDestinationsForPublishing(Set.of(first, duplicate))));
    }

    private RequestInstanceDestination destination() {
        return RequestInstanceDestination.builder()
                .creationQueueUrl("shared-queue")
                .capacityType(CapacityType.SPOT)
                .instanceType(InstanceTypeV5Udt.builder()
                        .name("type-name").value("type-value").build())
                .gpuName("gpu")
                .maxFulfillableInstances(1)
                .instanceBatchCount(1)
                .clusterId("zone")
                .region("region")
                .build();
    }

}
