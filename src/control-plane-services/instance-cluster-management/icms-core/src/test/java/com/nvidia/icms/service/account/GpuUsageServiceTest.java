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
package com.nvidia.icms.service.account;

import static com.nvidia.icms.service.platform.ComputePlatformTestFixtures.PLATFORM_CLUSTER_GROUP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.nvidia.icms.configuration.bean.CapacityGatingProperties;
import com.nvidia.icms.configuration.bean.CapacityGatingProperties.GatingPolicy;
import com.nvidia.icms.configuration.bean.IcmsConfigurationProperties;
import com.nvidia.icms.inbound.rest.model.ResourceProvider;
import com.nvidia.icms.inbound.rest.model.SpotInstanceInternalState;
import com.nvidia.icms.inbound.rest.model.account.DeploymentGpuUsageResponse;
import com.nvidia.icms.inbound.rest.model.account.GpuUsageResponse;
import com.nvidia.icms.inbound.rest.model.byoc.ClusterStatusEnum;
import com.nvidia.icms.outbound.cassandra.instance.InstanceV2Repository;
import com.nvidia.icms.outbound.cassandra.instance.entity.InstanceV2Entity;
import com.nvidia.icms.outbound.cassandra.request.InstanceRequestV2Repository;
import com.nvidia.icms.outbound.cassandra.request.entity.InstanceRequestV2Entity;
import com.nvidia.icms.service.byoc.ClustersService.ReadyClusterInfo;
import com.nvidia.icms.service.gating.CapacityGatingService;
import com.nvidia.icms.service.platform.ComputePlatformService;
import com.nvidia.icms.service.platform.ComputePlatformTestFixtures;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Covers the interaction between {@code icms.capacity-gating} and the combining of
 * compute-platform clusters in the GPU usage response.
 *
 * <p>Compute-platform zones are reported to NGC users under one combined
 * {@code <regionName>-<computePlatform>} cluster id, because a user cannot target an individual
 * zone. A cluster allow list, however, names the real cluster id, which for a compute-platform row
 * is the zone name. Gating therefore has to run before the ids are combined. These tests pin that
 * ordering: before it was fixed, a cluster-gated account saw an empty usage response because every
 * compute-platform row was compared against the combined id and dropped.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GpuUsageServiceTest {

    private static final String GATED_NCA = "gated-nca-id";
    private static final String UNGATED_NCA = "ungated-nca-id";

    private static final String ALLOWED_ZONE = "allowed-zone";
    private static final String DENIED_ZONE = "denied-zone";
    private static final String BYOC_CLUSTER = "byoc-cluster";

    private static final String REGION = "us-west-2";
    /** What the two compute-platform zones above are reported as once combined. */
    private static final String COMBINED_CLUSTER_ID = REGION + "-" + ResourceProvider.OCI;

    private static final String GPU = "L40";
    private static final String INSTANCE_TYPE = "DGX-CLOUD.GPU.L40_1x";
    private static final String DEPLOYMENT_ID = "11111111-2222-3333-4444-555555555555";

    @Mock
    private InstanceRequestV2Repository instanceRequestV2Repository;
    @Mock
    private InstanceV2Repository instanceV2Repository;
    @Mock
    private IcmsConfigurationProperties icmsConfigurationProperties;
    @Mock
    private ClusterGpuInfoHelper clusterGpuInfoHelper;

    private final ComputePlatformService computePlatformService =
            ComputePlatformTestFixtures.nonByocComputePlatformService();

    @BeforeEach
    void setUp() {
        when(instanceRequestV2Repository.findRequestsPerNcaId(anyString()))
                .thenReturn(List.of(InstanceRequestV2Entity.builder().requestId("request-1").build()));
        when(instanceRequestV2Repository.findRequestsPerNcaIdAndDeploymentId(anyString(), any()))
                .thenReturn(List.of(InstanceRequestV2Entity.builder().requestId("request-1").build()));
        when(clusterGpuInfoHelper.getReadyClusterInfo(anyString())).thenReturn(Set.of(
                readyCluster(ALLOWED_ZONE, PLATFORM_CLUSTER_GROUP_NAME),
                readyCluster(DENIED_ZONE, PLATFORM_CLUSTER_GROUP_NAME),
                readyCluster(BYOC_CLUSTER, "byoc-group")));
    }

    @Test
    void getGpuUsage_whenNcaIsUngated_combinesComputePlatformZonesAndSumsCounts() {
        seedInstances(computePlatformInstance(ALLOWED_ZONE, SpotInstanceInternalState.RUNNING),
                      computePlatformInstance(DENIED_ZONE, SpotInstanceInternalState.RUNNING),
                      computePlatformInstance(DENIED_ZONE, SpotInstanceInternalState.STARTING));

        GpuUsageResponse response = serviceWith(ungated()).getGpuUsage(UNGATED_NCA);

        GpuUsageResponse.Cluster combined = onlyCluster(response);
        assertEquals(COMBINED_CLUSTER_ID, combined.getClusterId(),
                     "every compute-platform zone in a region is reported under one combined id");
        assertEquals(2, combined.getStatus().getActiveInstances(),
                     "combining must sum the counts of the zones it collapses");
        assertEquals(1, combined.getStatus().getPendingInstances());
    }

    @Test
    void getGpuUsage_whenClusterAllowListNamesAZone_keepsThatZoneUnderTheCombinedId() {
        seedInstances(computePlatformInstance(ALLOWED_ZONE, SpotInstanceInternalState.RUNNING),
                      computePlatformInstance(DENIED_ZONE, SpotInstanceInternalState.RUNNING));

        GpuUsageResponse response =
                serviceWith(clustersAllowed(ALLOWED_ZONE)).getGpuUsage(GATED_NCA);

        GpuUsageResponse.Cluster combined = onlyCluster(response);
        assertEquals(COMBINED_CLUSTER_ID, combined.getClusterId(),
                     "gating must not change the combined id the API contract reports");
        assertEquals(1, combined.getStatus().getActiveInstances(),
                     "only the allowed zone's instance may be counted");
    }

    @Test
    void getGpuUsage_whenEveryComputePlatformZoneIsDenied_dropsTheGpu() {
        seedInstances(computePlatformInstance(DENIED_ZONE, SpotInstanceInternalState.RUNNING));

        GpuUsageResponse response =
                serviceWith(clustersAllowed(ALLOWED_ZONE)).getGpuUsage(GATED_NCA);

        assertTrue(response.getGpus().isEmpty(),
                   "a GPU whose every cluster was gated out carries nothing and is dropped");
    }

    @Test
    void getGpuUsage_whenPolicyGatesOnlyGpus_leavesComputePlatformClustersIntact() {
        seedInstances(computePlatformInstance(ALLOWED_ZONE, SpotInstanceInternalState.RUNNING),
                      computePlatformInstance(DENIED_ZONE, SpotInstanceInternalState.RUNNING));

        CapacityGatingProperties gpusOnly = propertiesFor(GATED_NCA, GatingPolicy.builder()
                .gpus(Map.of(GPU, List.of(INSTANCE_TYPE)))
                .build());

        GpuUsageResponse.Cluster combined =
                onlyCluster(serviceWith(gpusOnly).getGpuUsage(GATED_NCA));
        assertEquals(COMBINED_CLUSTER_ID, combined.getClusterId());
        assertEquals(2, combined.getStatus().getActiveInstances(),
                     "a GPU-only policy withholds no cluster, so both zones still count");
    }

    @Test
    void getGpuUsage_gatesByocClustersOnTheirOwnId() {
        seedInstances(byocInstance(BYOC_CLUSTER, SpotInstanceInternalState.RUNNING),
                      computePlatformInstance(ALLOWED_ZONE, SpotInstanceInternalState.RUNNING));

        GpuUsageResponse response =
                serviceWith(clustersAllowed(ALLOWED_ZONE)).getGpuUsage(GATED_NCA);

        assertEquals(Set.of(COMBINED_CLUSTER_ID), clusterIds(response),
                     "a BYOC cluster is reported under its real id and is gated on that id");
    }

    @Test
    void getGpuUsage_whenOrgIsDenied_returnsNothing() {
        seedInstances(computePlatformInstance(ALLOWED_ZONE, SpotInstanceInternalState.RUNNING));

        CapacityGatingProperties killSwitch =
                propertiesFor(GATED_NCA, GatingPolicy.builder().clusters(List.of()).build());

        assertTrue(serviceWith(killSwitch).getGpuUsage(GATED_NCA).getGpus().isEmpty(),
                   "an empty cluster list denies everything");
    }

    @Test
    void getDeploymentGpuUsage_appliesTheSameGatingAndCombining() {
        seedInstances(computePlatformInstance(ALLOWED_ZONE, SpotInstanceInternalState.RUNNING),
                      computePlatformInstance(DENIED_ZONE, SpotInstanceInternalState.RUNNING));

        DeploymentGpuUsageResponse response = serviceWith(clustersAllowed(ALLOWED_ZONE))
                .getDeploymentGpuUsage(GATED_NCA, DEPLOYMENT_ID);

        List<GpuUsageResponse.Gpu> gpus = response.getDeployments().getFirst().getGpus();
        GpuUsageResponse.Cluster combined = onlyCluster(gpus);
        assertEquals(COMBINED_CLUSTER_ID, combined.getClusterId());
        assertEquals(1, combined.getStatus().getActiveInstances());
    }

    @ParameterizedTest
    @MethodSource("unknownZoneCases")
    void usage_combinesKnownAndUnknownZonesWithoutSplittingCounts(
            boolean deployment, boolean unknownZoneFirst, String unknownZone) {
        var known = computePlatformInstance(ALLOWED_ZONE, SpotInstanceInternalState.RUNNING);
        var unknownRunning = computePlatformInstance(unknownZone, SpotInstanceInternalState.RUNNING);
        var unknownPending = computePlatformInstance(unknownZone, SpotInstanceInternalState.STARTING);
        if (unknownZoneFirst) {
            seedInstances(unknownRunning, unknownPending, known);
        } else {
            seedInstances(known, unknownRunning, unknownPending);
        }

        var combined = onlyCluster(usage(serviceWith(ungated()), UNGATED_NCA, deployment));

        assertEquals(COMBINED_CLUSTER_ID, combined.getClusterId());
        assertEquals(2, combined.getStatus().getActiveInstances());
        assertEquals(1, combined.getStatus().getPendingInstances());
    }

    @ParameterizedTest
    @MethodSource("endpointAndOrderCases")
    void usage_keepsCombinedIdsScopedToEachResolvedRegion(
            boolean deployment, boolean unknownRegionFirst) {
        when(clusterGpuInfoHelper.getReadyClusterInfo(anyString())).thenReturn(Set.of());
        var known = computePlatformInstance(ALLOWED_ZONE, SpotInstanceInternalState.RUNNING);
        var unknown = computePlatformInstance(ALLOWED_ZONE, SpotInstanceInternalState.STARTING);
        unknown.setRegion(null);
        if (unknownRegionFirst) {
            seedInstances(unknown, known);
        } else {
            seedInstances(known, unknown);
        }

        var regions = usage(serviceWith(ungated()), UNGATED_NCA, deployment).stream()
                .flatMap(gpu -> gpu.getInstances().stream())
                .flatMap(instance -> instance.getRegions().stream())
                .collect(Collectors.toMap(GpuUsageResponse.Region::getRegionName,
                                          GpuUsageResponse.Region::getClusters));

        assertEquals(Set.of(REGION, "UNKNOWN"), regions.keySet());
        assertEquals(1, regions.get(REGION).size());
        var knownRegionCluster = regions.get(REGION).getFirst();
        assertEquals(COMBINED_CLUSTER_ID, knownRegionCluster.getClusterId());
        assertEquals(1, knownRegionCluster.getStatus().getActiveInstances());
        assertEquals(0, knownRegionCluster.getStatus().getPendingInstances());
        assertEquals(1, regions.get("UNKNOWN").size());
        var unknownRegionCluster = regions.get("UNKNOWN").getFirst();
        assertEquals("UNKNOWN-" + ResourceProvider.OCI, unknownRegionCluster.getClusterId());
        assertEquals(0, unknownRegionCluster.getStatus().getActiveInstances());
        assertEquals(1, unknownRegionCluster.getStatus().getPendingInstances());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void usage_preservesAllowedByocAndComputePlatformClusters(boolean deployment) {
        seedInstances(byocInstance(BYOC_CLUSTER, SpotInstanceInternalState.RUNNING),
                      byocInstance(BYOC_CLUSTER, SpotInstanceInternalState.STARTING),
                      computePlatformInstance(ALLOWED_ZONE, SpotInstanceInternalState.RUNNING),
                      computePlatformInstance(DENIED_ZONE, SpotInstanceInternalState.RUNNING));

        var result = usage(serviceWith(clustersAllowed(ALLOWED_ZONE, BYOC_CLUSTER)),
                           GATED_NCA, deployment);
        var clustersById = clusters(result).stream().collect(Collectors.toMap(
                GpuUsageResponse.Cluster::getClusterId, cluster -> cluster));

        assertEquals(Set.of(BYOC_CLUSTER, COMBINED_CLUSTER_ID), clustersById.keySet());
        var byoc = clustersById.get(BYOC_CLUSTER);
        assertEquals("byoc-group", byoc.getClusterGroupName());
        assertEquals(1, byoc.getStatus().getActiveInstances());
        assertEquals(1, byoc.getStatus().getPendingInstances());
        var platform = clustersById.get(COMBINED_CLUSTER_ID);
        assertEquals(PLATFORM_CLUSTER_GROUP_NAME, platform.getClusterGroupName());
        assertEquals(1, platform.getStatus().getActiveInstances());
        assertEquals(0, platform.getStatus().getPendingInstances());
    }

    // ---------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------

    private static Stream<Arguments> unknownZoneCases() {
        return Stream.of((String) null, "", "  ").flatMap(zone ->
                Stream.of(false, true).flatMap(deployment ->
                        Stream.of(false, true).map(unknownFirst ->
                                Arguments.of(deployment, unknownFirst, zone))));
    }

    private static Stream<Arguments> endpointAndOrderCases() {
        return Stream.of(false, true).flatMap(deployment ->
                Stream.of(false, true).map(unknownFirst ->
                        Arguments.of(deployment, unknownFirst)));
    }

    private static List<GpuUsageResponse.Gpu> usage(
            GpuUsageService service, String ncaId, boolean deployment) {
        if (!deployment) {
            return service.getGpuUsage(ncaId).getGpus();
        }

        var response = service.getDeploymentGpuUsage(ncaId, DEPLOYMENT_ID);
        assertEquals(1, response.getDeployments().size());
        var deploymentUsage = response.getDeployments().getFirst();
        assertEquals(UUID.fromString(DEPLOYMENT_ID), deploymentUsage.getDeploymentId());
        return deploymentUsage.getGpus();
    }

    private GpuUsageService serviceWith(CapacityGatingProperties properties) {
        return new GpuUsageService(instanceRequestV2Repository,
                                   instanceV2Repository,
                                   icmsConfigurationProperties,
                                   clusterGpuInfoHelper,
                                   computePlatformService,
                                   new CapacityGatingService(properties, null));
    }

    private void seedInstances(InstanceV2Entity... instances) {
        when(instanceV2Repository.findAllInstancesByCustomerAndRequestIds(any(), any(), anyBoolean()))
                .thenReturn(Map.of("request-1", Arrays.asList(instances)));
    }

    private static CapacityGatingProperties ungated() {
        return new CapacityGatingProperties();
    }

    private static CapacityGatingProperties clustersAllowed(String... clusterIds) {
        return propertiesFor(GATED_NCA,
                             GatingPolicy.builder().clusters(List.of(clusterIds)).build());
    }

    private static CapacityGatingProperties propertiesFor(String ncaId, GatingPolicy policy) {
        CapacityGatingProperties properties = new CapacityGatingProperties();
        Map<String, GatingPolicy> entries = new HashMap<>();
        entries.put(ncaId, policy);
        properties.setCapacityGating(entries);
        return properties;
    }

    private static ReadyClusterInfo readyCluster(String clusterId, String clusterGroupName) {
        return ReadyClusterInfo.builder()
                .clusterId(clusterId)
                .clusterName(clusterId)
                .clusterGroupName(clusterGroupName)
                .clusterStatus(ClusterStatusEnum.READY)
                .region(REGION)
                .gpu(GPU)
                .build();
    }

    private static InstanceV2Entity computePlatformInstance(String zone,
                                                            SpotInstanceInternalState state) {
        return instance(zone, state, ResourceProvider.OCI);
    }

    private static InstanceV2Entity byocInstance(String zone, SpotInstanceInternalState state) {
        return instance(zone, state, ResourceProvider.BYOC);
    }

    private static InstanceV2Entity instance(String zone, SpotInstanceInternalState state,
                                             ResourceProvider resourceProvider) {
        return InstanceV2Entity.builder()
                .requestId("request-1")
                .instanceId(UUID.randomUUID().toString())
                .gpu(GPU)
                .instanceType(INSTANCE_TYPE)
                .zone(zone)
                .region(REGION)
                .resourceProvider(resourceProvider)
                .instanceStateName(state)
                .build();
    }

    private static GpuUsageResponse.Cluster onlyCluster(GpuUsageResponse response) {
        return onlyCluster(response.getGpus());
    }

    private static GpuUsageResponse.Cluster onlyCluster(List<GpuUsageResponse.Gpu> gpus) {
        List<GpuUsageResponse.Cluster> clusters = clusters(gpus);
        assertEquals(1, clusters.size(), "expected exactly one cluster, got " + clusters);
        return clusters.getFirst();
    }

    private static Set<String> clusterIds(GpuUsageResponse response) {
        return clusters(response.getGpus()).stream()
                .map(GpuUsageResponse.Cluster::getClusterId)
                .collect(Collectors.toSet());
    }

    private static List<GpuUsageResponse.Cluster> clusters(List<GpuUsageResponse.Gpu> gpus) {
        return gpus.stream()
                .flatMap(gpu -> gpu.getInstances().stream())
                .flatMap(instance -> instance.getRegions().stream())
                .flatMap(region -> region.getClusters().stream())
                .toList();
    }
}
