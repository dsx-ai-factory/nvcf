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
package com.nvidia.icms.outbound.cassandra.byoc;

import com.nvidia.icms.errors.IcmsInternalServerException;
import com.nvidia.icms.inbound.rest.model.byoc.NodeTypeEnum;
import com.nvidia.icms.outbound.cassandra.byoc.entity.GpuUdt;
import com.nvidia.icms.outbound.cassandra.byoc.entity.GpuV5Udt;
import com.nvidia.icms.outbound.cassandra.byoc.entity.InstanceTypeUdt;
import com.nvidia.icms.outbound.cassandra.byoc.entity.InstanceTypeV5Udt;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class NvcaConverter {

    public static Set<GpuV5Udt> getGpusV5(Set<GpuUdt> gpuUdts) {
        return gpuUdts != null ?
                gpuUdts.stream().map(NvcaConverter::toGpuV5).collect(Collectors.toSet()) :
                Collections.emptySet();
    }

    // InstanceTypeUdt to InstanceTypeV5Udt converter
    public static InstanceTypeV5Udt toInstanceTypeV5(@NonNull InstanceTypeUdt instanceType) {
        return InstanceTypeV5Udt.builder()
                .value(instanceType.getValue())
                .name(instanceType.getName())
                .gpuCount(instanceType.getGpuCount())
                .description(instanceType.getDescription())
                .isDefault(instanceType.getIsDefault())
                .cpuCores(instanceType.getCpuCores())
                .systemMemory(instanceType.getSystemMemory())
                .gpuMemory(instanceType.getGpuMemory())
                .build();
    }

    private static GpuV5Udt toGpuV5(@NonNull GpuUdt gpuUdt) {
        return GpuV5Udt.builder()
                .name(gpuUdt.getName())
                .capacity(0)
                .instanceTypes(gpuUdt.getInstanceTypes()
                                       .stream()
                                       .map(NvcaConverter::toInstanceTypeV5)
                                       .collect(Collectors.toSet()))
                .build();
    }

    public static NodeTypeEnum getNodeTypeEnum(
            @NonNull InstanceTypeV5Udt instanceTypeV5Udt) {

        // If value is null in DB then returning default value
        String valueFromDb = instanceTypeV5Udt.getNodeType();
        if (valueFromDb == null) {
            return NodeTypeEnum.SINGLE;
        }

        try {
            return NodeTypeEnum.valueOf(instanceTypeV5Udt.getNodeType());

        } catch (Exception exception) {
            log.error(
                    "Failed to parse the nodeType, configured value in DB {}, error: {}, exception -",
                    valueFromDb, exception.getMessage(), exception);

            throw new IcmsInternalServerException(
                    String.format("Failed to parse instanceTypeUsage, configured value in DB - %s",
                                  valueFromDb));
        }
    }
}
