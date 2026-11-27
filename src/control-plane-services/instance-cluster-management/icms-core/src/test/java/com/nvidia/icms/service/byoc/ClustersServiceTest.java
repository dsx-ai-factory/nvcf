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
package com.nvidia.icms.service.byoc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.nvidia.icms.configuration.bean.CapacityGatingProperties;
import com.nvidia.icms.configuration.bean.CapacityGatingProperties.GatingPolicy;
import com.nvidia.icms.configuration.bean.IcmsConfigurationProperties;
import com.nvidia.icms.configuration.nvca.NvcaConfigurationProperties;
import com.nvidia.icms.inbound.rest.model.byoc.ClusterGroupResponse;
import com.nvidia.icms.inbound.rest.model.byoc.ClusterGroups;
import com.nvidia.icms.inbound.rest.model.byoc.ClusterGroups.GpuResponse;
import com.nvidia.icms.inbound.rest.model.byoc.ClusterStatusEnum;
import com.nvidia.icms.inbound.rest.model.byoc.InstanceTypeUsageEnum;
import com.nvidia.icms.inbound.rest.model.byoc.NodeTypeEnum;
import com.nvidia.icms.outbound.cassandra.byoc.ClusterRepository;
import com.nvidia.icms.outbound.cassandra.byoc.NvcaClusterRepository;
import com.nvidia.icms.outbound.cassandra.byoc.entity.ClusterByGroupIdAndIdEntity;
import com.nvidia.icms.outbound.cassandra.byoc.entity.ClusterByGroupIdAndIdKey;
import com.nvidia.icms.outbound.cassandra.byoc.entity.ClustersByAuthorizedAccountsEntity;
import com.nvidia.icms.outbound.cassandra.byoc.entity.ClustersByAuthorizedAccountsKey;
import com.nvidia.icms.outbound.cassandra.byoc.entity.GpuV5Udt;
import com.nvidia.icms.outbound.cassandra.byoc.entity.InstanceTypeV5Udt;
import com.nvidia.icms.service.InstanceServiceHelper;
import com.nvidia.icms.service.extensions.api.ClusterAuthorizationService;
import com.nvidia.icms.service.gating.CapacityGatingService;
import com.nvidia.icms.service.platform.ComputePlatformService;
import com.nvidia.icms.service.platform.ComputePlatformTestFixtures;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Covers authorized non-BYOC discovery alongside NVCA discovery and GPU gating.
 */
@ExtendWith(MockitoExtension.class)
class ClustersServiceTest {

    // Compute-platform group name/id from the shared fixture.
    private static final String PLATFORM_GROUP_ID = ComputePlatformTestFixtures.PLATFORM_CLUSTER_GROUP_ID;
    private static final String PLATFORM_GROUP_NAME = ComputePlatformTestFixtures.PLATFORM_CLUSTER_GROUP_NAME;
    private static final String BYOC_GROUP_ID = "byoc-group-id";
    private static final String BYOC_GROUP_NAME = "byoc-group";
    private static final String BYOC_CLUSTER_ID = "byoc-cluster";
    private static final String OTHER_BYOC_CLUSTER_ID = "other-byoc-cluster";
    private static final String PROVIDER_ZONE_ID = "provider-zone";
    private static final String PROVIDER_COMPATIBILITY_CLUSTER_ID = "provider-static-cluster";

    private static final String RESTRICTED_GPU = "dummy-restricted-gpu";
    private static final String OTHER_GPU = "dummy-gpu";
    private static final String ALLOWED_NCA = "allowed-nca";
    private static final String DISALLOWED_NCA = "disallowed-nca";
    private static final String GATED_NCA = "GatedNca1aBcDeF";
    private static final String WILDCARD = ClusterRepository.WILDCARD;

    @Mock
    private ClusterRepository clusterRepository;
    @Mock
    private NvcaClusterRepository nvcaClusterRepository;
    @Mock
    private NvcaConfigurationProperties nvcaConfigurationProperties;
    @Mock
    private InstanceServiceHelper instanceServiceHelper;
    @Mock
    private ClusterTargetingHelper clusterTargetingHelper;
    @Mock
    private ClusterAuthorizationService clusterAuthorizationService;

    private IcmsConfigurationProperties icmsConfigurationProperties;
    private CapacityGatingProperties capacityGatingProperties;
    private ComputePlatformService computePlatformService;
    private ClustersService clustersService;

    @BeforeEach
    void setUp() {
        icmsConfigurationProperties = new IcmsConfigurationProperties();
        capacityGatingProperties = new CapacityGatingProperties();
        computePlatformService = ComputePlatformTestFixtures.nonByocComputePlatformService();
        clustersService = new ClustersService(
                clusterRepository,
                nvcaClusterRepository,
                nvcaConfigurationProperties,
                instanceServiceHelper,
                icmsConfigurationProperties,
                capacityGatingProperties,
                clusterTargetingHelper,
                computePlatformService,
                clusterAuthorizationService,
                new CapacityGatingService(capacityGatingProperties, null));

        when(nvcaClusterRepository.getAllClustersInAuthorizedAccount(WILDCARD)).thenReturn(List.of());
    }

    @Test
    void getRegisteredClustersForNcaId_returnsNoPlatformGroupWhenNotAuthorized() {
        when(nvcaClusterRepository.getAllClustersInAuthorizedAccount(DISALLOWED_NCA))
                .thenReturn(List.of());

        ClusterGroupResponse response =
                clustersService.getRegisteredClustersForNcaId(DISALLOWED_NCA, InstanceTypeUsageEnum.DEFAULT);

        assertTrue(response.getClusterGroup().isEmpty());
        verify(clusterAuthorizationService).fetchAuthorizedClusterGroup(
                DISALLOWED_NCA, InstanceTypeUsageEnum.DEFAULT);
    }

    @Test
    void getRegisteredClustersForNcaId_keepsAuthorizedPlatformGpus() {
        stubAuthorizedGroup(Set.of(RESTRICTED_GPU, OTHER_GPU), ALLOWED_NCA);

        ClusterGroupResponse response =
                clustersService.getRegisteredClustersForNcaId(ALLOWED_NCA, InstanceTypeUsageEnum.DEFAULT);

        ClusterGroups platformGroup = findGroup(response, PLATFORM_GROUP_NAME);
        assertEquals(Set.of(RESTRICTED_GPU, OTHER_GPU), gpuNames(platformGroup),
                "discovery returns the authorized provider's GPU set");
        verify(clusterAuthorizationService).fetchAuthorizedClusterGroup(
                ALLOWED_NCA, InstanceTypeUsageEnum.DEFAULT);
    }

    @Test
    void getRegisteredClustersForNcaId_doesNotGateByocGroup_evenForDisallowedNca() {
        capacityGatingProperties.setGpuAllowedNcaIds(Map.of(RESTRICTED_GPU, List.of(ALLOWED_NCA)));
        stubNativeGroup(Set.of(RESTRICTED_GPU, OTHER_GPU), DISALLOWED_NCA, BYOC_CLUSTER_ID);

        ClusterGroupResponse response =
                clustersService.getRegisteredClustersForNcaId(DISALLOWED_NCA, InstanceTypeUsageEnum.DEFAULT);

        ClusterGroups byocGroup = findGroup(response, BYOC_GROUP_NAME);
        assertEquals(Set.of(RESTRICTED_GPU, OTHER_GPU), gpuNames(byocGroup), "BYOC groups are never gated");
    }

    @Test
    void getRegisteredClustersForNcaId_dropsGatedGpusAndInstanceTypes_forGatedNca() {
        gateGpus(Map.of(
                GATED_NCA, Map.of(OTHER_GPU, List.of(instanceTypeName(OTHER_GPU, 0)))));
        stubNativeGroup(Set.of(RESTRICTED_GPU, OTHER_GPU), GATED_NCA, BYOC_CLUSTER_ID);

        ClusterGroupResponse response =
                clustersService.getRegisteredClustersForNcaId(GATED_NCA, InstanceTypeUsageEnum.DEFAULT);

        ClusterGroups byocGroup = findGroup(response, BYOC_GROUP_NAME);
        assertEquals(Set.of(OTHER_GPU), gpuNames(byocGroup),
                "gpu gating applies to BYOC groups, unlike gpu-allowed-nca-ids");
        assertEquals(Set.of(instanceTypeName(OTHER_GPU, 0)), instanceTypeNames(byocGroup, OTHER_GPU),
                "only the allowed instance type of the allowed GPU survives");
    }

    @Test
    void getRegisteredClustersForNcaId_dropsWholeGroup_whenGatingRemovesEveryGpu() {
        gateGpus(Map.of(
                GATED_NCA, Map.of("dummy-gpu-not-in-any-group", List.of("dummy-instance-type"))));
        stubNativeGroup(Set.of(RESTRICTED_GPU, OTHER_GPU), GATED_NCA, BYOC_CLUSTER_ID);

        ClusterGroupResponse response =
                clustersService.getRegisteredClustersForNcaId(GATED_NCA, InstanceTypeUsageEnum.DEFAULT);

        assertTrue(response.getClusterGroup().isEmpty(),
                "cluster group must be dropped entirely when gating removes all of its GPUs");
    }

    /** A GPU whose every instance type is gated out is dropped, not returned empty. */
    @Test
    void getRegisteredClustersForNcaId_dropsGpu_whenAllItsInstanceTypesAreGatedOut() {
        gateGpus(Map.of(
                GATED_NCA, Map.of(OTHER_GPU, List.of("dummy-instance-type-not-on-this-gpu"))));
        stubNativeGroup(Set.of(OTHER_GPU), GATED_NCA, BYOC_CLUSTER_ID);

        ClusterGroupResponse response =
                clustersService.getRegisteredClustersForNcaId(GATED_NCA, InstanceTypeUsageEnum.DEFAULT);

        assertTrue(response.getClusterGroup().isEmpty(),
                "a GPU left with no instance types must be dropped, taking the empty group with it");
    }

    @Test
    void getRegisteredClustersForNcaId_leavesUngatedNcaUntouched() {
        gateGpus(Map.of(
                GATED_NCA, Map.of(OTHER_GPU, List.of(instanceTypeName(OTHER_GPU, 0)))));
        stubNativeGroup(Set.of(RESTRICTED_GPU, OTHER_GPU), ALLOWED_NCA, BYOC_CLUSTER_ID);

        ClusterGroupResponse response =
                clustersService.getRegisteredClustersForNcaId(ALLOWED_NCA, InstanceTypeUsageEnum.DEFAULT);

        ClusterGroups byocGroup = findGroup(response, BYOC_GROUP_NAME);
        assertEquals(Set.of(RESTRICTED_GPU, OTHER_GPU), gpuNames(byocGroup),
                "an account without a gating entry is unaffected");
        assertEquals(Set.of(instanceTypeName(OTHER_GPU, 0), instanceTypeName(OTHER_GPU, 1)),
                instanceTypeNames(byocGroup, OTHER_GPU));
    }

    @Test
    void getRegisteredClustersForNcaId_keepsProviderCompatibilityIdWithRealZonePolicy() {
        capacityGatingProperties.setCapacityGating(Map.of(GATED_NCA,
                GatingPolicy.builder().clusters(List.of(PROVIDER_ZONE_ID)).build()));
        stubAuthorizedGroup(Set.of(OTHER_GPU), GATED_NCA);

        ClusterGroupResponse response = clustersService.getRegisteredClustersForNcaId(
                GATED_NCA, InstanceTypeUsageEnum.DEFAULT);

        ClusterGroups group = findGroup(response, PLATFORM_GROUP_NAME);
        assertEquals(PLATFORM_GROUP_ID, group.getId());
        assertEquals(Set.of(PROVIDER_COMPATIBILITY_CLUSTER_ID), clusterIds(group));
        assertEquals(Set.of(OTHER_GPU), gpuNames(group));
    }

    @Test
    void getRegisteredClustersForNcaId_gatesNativeGroupsBeforeAddingProviderAggregate() {
        capacityGatingProperties.setCapacityGating(Map.of(GATED_NCA,
                GatingPolicy.builder()
                        .clusters(List.of(BYOC_CLUSTER_ID, PROVIDER_ZONE_ID)).build()));
        stubNativeGroup(Set.of(OTHER_GPU), GATED_NCA,
                BYOC_CLUSTER_ID, OTHER_BYOC_CLUSTER_ID);
        stubAuthorizedGroup(Set.of(OTHER_GPU), GATED_NCA);

        ClusterGroupResponse response = clustersService.getRegisteredClustersForNcaId(
                GATED_NCA, InstanceTypeUsageEnum.DEFAULT);

        assertEquals(2, response.getClusterGroup().size());
        assertEquals(Set.of(BYOC_CLUSTER_ID),
                clusterIds(findGroup(response, BYOC_GROUP_NAME)));
        assertEquals(Set.of(PROVIDER_COMPATIBILITY_CLUSTER_ID),
                clusterIds(findGroup(response, PLATFORM_GROUP_NAME)));
    }

    @Test
    void getRegisteredClustersForNcaId_dropsNativeGroupOutsideClusterPolicy() {
        capacityGatingProperties.setCapacityGating(Map.of(GATED_NCA,
                GatingPolicy.builder().clusters(List.of(PROVIDER_ZONE_ID)).build()));
        stubNativeGroup(Set.of(OTHER_GPU), GATED_NCA, BYOC_CLUSTER_ID);

        ClusterGroupResponse response = clustersService.getRegisteredClustersForNcaId(
                GATED_NCA, InstanceTypeUsageEnum.DEFAULT);

        assertTrue(response.getClusterGroup().isEmpty());
    }

    @Test
    void getRegisteredClustersForNcaId_doesNotFetchProviderWhenOrgDenied() {
        capacityGatingProperties.setCapacityGating(Map.of(GATED_NCA,
                GatingPolicy.builder().clusters(List.of()).build()));
        stubNativeGroup(Set.of(OTHER_GPU), GATED_NCA, BYOC_CLUSTER_ID);

        ClusterGroupResponse response = clustersService.getRegisteredClustersForNcaId(
                GATED_NCA, InstanceTypeUsageEnum.DEFAULT);

        assertTrue(response.getClusterGroup().isEmpty());
        verifyNoInteractions(clusterAuthorizationService);
    }

    @Test
    void getRegisteredClustersForNcaId_legacyProviderStillGatesCompatibilityId() {
        capacityGatingProperties.setCapacityGating(Map.of(GATED_NCA,
                GatingPolicy.builder().clusters(List.of(PROVIDER_ZONE_ID)).build()));
        stubAuthorizedGroup(Set.of(OTHER_GPU), GATED_NCA, false);

        ClusterGroupResponse response = clustersService.getRegisteredClustersForNcaId(
                GATED_NCA, InstanceTypeUsageEnum.DEFAULT);

        assertTrue(response.getClusterGroup().isEmpty(),
                "a legacy provider must retain the existing aggregate cluster filter");
    }

    @Test
    void getRegisteredClustersForNcaId_legacyProviderStillGatesGpuAndInstanceTypes() {
        gateGpus(Map.of(GATED_NCA,
                Map.of(OTHER_GPU, List.of(instanceTypeName(OTHER_GPU, 0)))));
        stubAuthorizedGroup(Set.of(RESTRICTED_GPU, OTHER_GPU), GATED_NCA, false);

        ClusterGroupResponse response = clustersService.getRegisteredClustersForNcaId(
                GATED_NCA, InstanceTypeUsageEnum.DEFAULT);

        ClusterGroups group = findGroup(response, PLATFORM_GROUP_NAME);
        assertEquals(Set.of(PROVIDER_COMPATIBILITY_CLUSTER_ID), clusterIds(group));
        assertEquals(Set.of(OTHER_GPU), gpuNames(group));
        assertEquals(Set.of(instanceTypeName(OTHER_GPU, 0)),
                instanceTypeNames(group, OTHER_GPU));
    }

    /** Installs GPU-only capacity policies, leaving clusters unrestricted for each account. */
    private void gateGpus(Map<String, Map<String, List<String>>> gpusByNcaId) {
        Map<String, GatingPolicy> policies = new HashMap<>();
        gpusByNcaId.forEach((ncaId, gpus) ->
                policies.put(ncaId, GatingPolicy.builder().gpus(gpus).build()));
        capacityGatingProperties.setCapacityGating(policies);
    }

    private void stubAuthorizedGroup(Set<String> gpuNames, String requestingNcaId) {
        stubAuthorizedGroup(gpuNames, requestingNcaId, true);
    }

    private void stubAuthorizedGroup(Set<String> gpuNames, String requestingNcaId,
                                     boolean appliesCapacityGating) {
        ClusterGroups group = ClusterGroups.builder()
                .id(PLATFORM_GROUP_ID)
                .name(PLATFORM_GROUP_NAME)
                .ncaId(WILDCARD)
                .authorizedNcaIds(Set.of())
                .clusters(Set.of(ClusterGroups.ClustersResponse.builder()
                        .id(PROVIDER_COMPATIBILITY_CLUSTER_ID).name("STATIC-ZONE").build()))
                .gpus(gpuNames.stream().map(name -> GpuResponse.builder().name(name)
                        .instanceTypes(Set.of(
                                ClusterGroups.InstanceTypeResponse.builder()
                                        .name(instanceTypeName(name, 0))
                                        .nodeType(NodeTypeEnum.SINGLE).build(),
                                ClusterGroups.InstanceTypeResponse.builder()
                                        .name(instanceTypeName(name, 1))
                                        .nodeType(NodeTypeEnum.SINGLE).build()))
                        .build()).collect(Collectors.toSet()))
                .build();
        when(clusterAuthorizationService.fetchAuthorizedClusterGroup(
                requestingNcaId, InstanceTypeUsageEnum.DEFAULT)).thenReturn(group);
        if (appliesCapacityGating) {
            when(clusterAuthorizationService.appliesCapacityGatingBeforeAggregation())
                    .thenReturn(true);
        }
    }

    private void stubNativeGroup(Set<String> gpuNames, String requestingNcaId,
                                 String... clusterIds) {
        Set<GpuV5Udt> gpus = gpuNames.stream().map(name -> GpuV5Udt.builder()
                .name(name)
                .instanceTypes(Set.of(
                        nativeInstanceType(name, 0), nativeInstanceType(name, 1)))
                .build()).collect(Collectors.toSet());
        List<ClustersByAuthorizedAccountsEntity> authorizedClusters = List.of(clusterIds).stream()
                .map(id -> ClustersByAuthorizedAccountsEntity.builder()
                        .key(new ClustersByAuthorizedAccountsKey(requestingNcaId, id))
                        .clusterName(id)
                        .clusterGroupId(BYOC_GROUP_ID)
                        .clusterGroupName(BYOC_GROUP_NAME)
                        .ncaId(requestingNcaId)
                        .authorizedNcaIds(Set.of(requestingNcaId))
                        .gpusV5(gpus).build())
                .toList();
        List<ClusterByGroupIdAndIdEntity> readyClusters = List.of(clusterIds).stream()
                .map(id -> ClusterByGroupIdAndIdEntity.builder()
                        .key(new ClusterByGroupIdAndIdKey(BYOC_GROUP_ID, id))
                        .clusterName(id)
                        .clusterGroupName(BYOC_GROUP_NAME)
                        .clusterStatus(ClusterStatusEnum.READY)
                        .ncaId(requestingNcaId)
                        .authorizedNcaIds(Set.of(requestingNcaId))
                        .gpusV5(gpus).build())
                .toList();
        when(nvcaClusterRepository.getAllClustersInAuthorizedAccount(requestingNcaId))
                .thenReturn(authorizedClusters);
        when(clusterRepository.getClustersFromClusterGroup(BYOC_GROUP_ID))
                .thenReturn(readyClusters);
    }

    private static InstanceTypeV5Udt nativeInstanceType(String gpuName, int index) {
        return InstanceTypeV5Udt.builder()
                .name(instanceTypeName(gpuName, index))
                .nodeType(NodeTypeEnum.SINGLE.name()).build();
    }

    private static Set<String> clusterIds(ClusterGroups group) {
        return group.getClusters().stream().map(ClusterGroups.ClustersResponse::getId)
                .collect(Collectors.toSet());
    }

    private ClusterGroups findGroup(ClusterGroupResponse response, String name) {
        return response.getClusterGroup().stream()
                .filter(cg -> name.equals(cg.getName()))
                .findFirst()
                .orElseThrow();
    }

    private Set<String> gpuNames(ClusterGroups clusterGroup) {
        return clusterGroup.getGpus().stream()
                .map(GpuResponse::getName)
                .collect(Collectors.toSet());
    }

    private Set<String> instanceTypeNames(ClusterGroups clusterGroup, String gpuName) {
        return clusterGroup.getGpus().stream()
                .filter(gpu -> gpuName.equals(gpu.getName()))
                .flatMap(gpu -> gpu.getInstanceTypes().stream())
                .map(ClusterGroups.InstanceTypeResponse::getName)
                .collect(Collectors.toSet());
    }

    private static String instanceTypeName(String gpuName, int index) {
        return gpuName + ".instance-type-" + index;
    }
}
