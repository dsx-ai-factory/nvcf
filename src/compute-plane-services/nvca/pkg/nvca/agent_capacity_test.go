/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/

package nvca

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/core"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	nvcav2 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v2beta1"
	nvcalisters "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/client/listers/nvca/v2beta1"
	featuremock "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/featureflag/mock"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/nodefeatures"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/nvca/enforce"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/nvca/health"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

type capacityNodeFeatures struct {
	allocation nodefeatures.GPUAllocationGetter
	err        error
}

func (c *capacityNodeFeatures) GetAllBackendGPUs(context.Context) ([]types.BackendGPU, error) {
	return []types.BackendGPU{{Name: "L4", Capacity: 1, Static: true}}, c.err
}

func (c *capacityNodeFeatures) GetGPUResources(ctx context.Context, gpu types.GPUName) (types.GPUResource, error) {
	allocated, err := c.allocation.GetForGPU(ctx, gpu)
	return types.GPUResource{Capacity: 1, Allocated: allocated}, err
}

func newCapacityTestAgent(t *testing.T) (*Agent, cache.Indexer, cache.Indexer, *nvcav2.ICMSRequest, *corev1.Pod) {
	t.Helper()
	reqIndex := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	podIndex := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: RequestsNamespace}}
	require.NoError(t, podIndex.Add(pod))
	req := &nvcav2.ICMSRequest{ObjectMeta: metav1.ObjectMeta{Name: "request", Namespace: RequestsNamespace}}
	req.Spec.CreationMsgInfo.InstanceTypeValue = "L4-instance"
	req.Spec.CreationMsgInfo.GPUType = "L4"
	req.Spec.CreationMsgInfo.InstanceCount = 1
	req.Spec.CreationMsgInfo.RequestedGPUCount = 1
	req.Status.Instances = map[string]nvcav2.InstanceStatus{
		"worker": {
			ID: "worker", Type: nvcav2.InstanceTypePod,
			Status: string(types.ICMSInstanceTerminated), LastReportedStatus: string(types.ICMSInstanceRunning),
		},
	}
	require.NoError(t, reqIndex.Add(req))
	bc := &BackendK8sCache{
		icmsRequestLister: nvcalisters.NewICMSRequestLister(reqIndex),
		podSpecLister:     listersv1.NewPodLister(podIndex).Pods(RequestsNamespace),
		nodeLister:        listersv1.NewNodeLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})),
		clients: mockKubeClients(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Name: NVCADeploymentName, Namespace: SystemNamespace,
		}}),
		systemNamespace:     SystemNamespace,
		featureFlagFetcher:  &featuremock.Fetcher{},
		infraOverheadGetter: enforce.NoOpInfraOverheadGetter,
	}
	bc.icmsRequestHelper = K8sComputeBackend{bk8s: bc, clients: bc.clients}
	bc.nfClient = &capacityNodeFeatures{allocation: newGPUAllocationGetter(bc.icmsRequestLister, bc.icmsRequestHelper)}
	statusCache := health.NewBackendStatusCache(time.Hour,
		health.GetComponentStatusFunc(func(ctx context.Context) (types.AgentHealth, error) {
			usage, err := bc.getGPUUsageStats(ctx)
			return types.AgentHealth{Status: types.HealthStatusHealthy, GPUUsage: usage}, err
		}),
	)
	agent := &Agent{
		AgentOptions:    &AgentOptions{NCAId: "owner", ClusterName: "cluster", FeatureFlagFetcher: &featuremock.Fetcher{}},
		backendk8scache: bc, backendHealthCache: statusCache, icmsClient: &mockICMSClient{},
		resourceEventWorkerQueues: map[string]workqueue.Interface{EventTickUpdateHeartbeat: workqueue.New()},
	}
	t.Cleanup(agent.resourceEventWorkerQueues[EventTickUpdateHeartbeat].ShutDown)
	return agent, reqIndex, podIndex, req, pod
}

func TestCapacityHeartbeatWaitsForRelease(t *testing.T) {
	for _, reportFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "pod_removed_first", true: "termination_reported_first"}[reportFirst], func(t *testing.T) {
			ctx := newTestContext()
			a, reqIndex, podIndex, req, pod := newCapacityTestAgent(t)
			icms := a.icmsClient.(*mockICMSClient)
			require.NoError(t, a.PutNVCAStatusUpdate(ctx))
			require.EqualValues(t, 1, icms.healthStatusRequests[0].GPUUsage["L4"].Allocated)

			report := func() {
				updated := req.DeepCopy()
				inst := updated.Status.Instances["worker"]
				inst.LastReportedStatus = string(types.ICMSInstanceTerminated)
				updated.Status.Instances["worker"] = inst
				require.NoError(t, reqIndex.Update(updated))
			}
			remove := func() { require.NoError(t, podIndex.Delete(pod)) }
			first, second := remove, report
			if reportFirst {
				first, second = report, remove
			}
			first()
			a.apply(ctx, &core.Event{Kind: EventTickUpdateHeartbeat, ObjectMetaKey: "capacity-check"})
			require.Len(t, icms.healthStatusRequests, 1, "capacity must stay reserved until both release gates pass")
			second()
			a.apply(ctx, &core.Event{Kind: EventTickUpdateHeartbeat, ObjectMetaKey: "capacity-check"})
			require.Len(t, icms.healthStatusRequests, 2)
			require.Zero(t, icms.healthStatusRequests[1].GPUUsage["L4"].Allocated)
			require.EqualValues(t, 1, a.backendHealthCache.GetStatus().GPUUsage["L4"].Allocated,
				"the test must exercise a still-warm health cache")

			a.apply(ctx, &core.Event{Kind: EventTickUpdateHeartbeat, ObjectMetaKey: "capacity-check"})
			require.Len(t, icms.healthStatusRequests, 2, "unchanged capacity must not send extra heartbeats")
			require.NoError(t, a.PutNVCAStatusUpdate(ctx))
			require.Len(t, icms.healthStatusRequests, 3, "periodic heartbeat must still publish unchanged capacity")
			require.Zero(t, icms.healthStatusRequests[2].GPUUsage["L4"].Allocated,
				"periodic heartbeat must not republish the stale cached allocation")
		})
	}
}

func TestCapacityHeartbeatPublishesReleasedCapacityWithWarmCache(t *testing.T) {
	ctx := newTestContext()
	a, reqIndex, podIndex, req, pod := newCapacityTestAgent(t)
	icms := a.icmsClient.(*mockICMSClient)
	require.NoError(t, a.PutNVCAStatusUpdate(ctx))
	require.NoError(t, podIndex.Delete(pod))
	updated := req.DeepCopy()
	inst := updated.Status.Instances["worker"]
	inst.LastReportedStatus = string(types.ICMSInstanceTerminated)
	updated.Status.Instances["worker"] = inst
	require.NoError(t, reqIndex.Update(updated))
	a.apply(ctx, &core.Event{Kind: EventTickUpdateHeartbeat, ObjectMetaKey: "capacity-check"})
	require.Len(t, icms.healthStatusRequests, 2)
	require.Zero(t, icms.healthStatusRequests[1].GPUUsage["L4"].Allocated,
		"released GPU must reach ICMS before the periodic heartbeat or health-cache expiry")
}

type capacityHeartbeatClient struct {
	mockICMSClient
	err error
}

func (c *capacityHeartbeatClient) PutHealthStatus(ctx context.Context, req *types.HealthStatusRequest) (*types.HealthStatusResponse, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.mockICMSClient.PutHealthStatus(ctx, req)
}

func TestCapacityHeartbeatRetriesAndHandlesReadErrors(t *testing.T) {
	ctx := newTestContext()
	a, reqIndex, podIndex, req, pod := newCapacityTestAgent(t)
	icms := &capacityHeartbeatClient{}
	a.icmsClient = icms
	require.NoError(t, a.PutNVCAStatusUpdate(ctx))
	require.NoError(t, podIndex.Delete(pod))
	updated := req.DeepCopy()
	inst := updated.Status.Instances["worker"]
	inst.LastReportedStatus = string(types.ICMSInstanceTerminated)
	updated.Status.Instances["worker"] = inst
	require.NoError(t, reqIndex.Update(updated))

	nf := a.backendk8scache.nfClient.(*capacityNodeFeatures)
	nf.err = errors.New("GPU discovery unavailable")
	a.apply(ctx, &core.Event{Kind: EventTickUpdateHeartbeat, ObjectMetaKey: "capacity-check"})
	require.Len(t, icms.healthStatusRequests, 1)
	nf.err = nil
	icms.err = errors.New("ICMS unavailable")
	a.apply(ctx, &core.Event{Kind: EventTickUpdateHeartbeat, ObjectMetaKey: "capacity-check"})
	require.Len(t, icms.healthStatusRequests, 1)
	icms.err = nil
	a.apply(ctx, &core.Event{Kind: EventTickUpdateHeartbeat, ObjectMetaKey: "capacity-check"})
	require.Len(t, icms.healthStatusRequests, 2, "failed publication must be retried on the next capacity check")
	require.Zero(t, icms.healthStatusRequests[1].GPUUsage["L4"].Allocated)
}

func TestInstanceStatusTickCoalescesCapacityChecks(t *testing.T) {
	ctx := newTestContext()
	a, reqIndex, _, req, _ := newCapacityTestAgent(t)
	require.NoError(t, reqIndex.Delete(req))
	for range 3 {
		a.apply(ctx, &core.Event{Kind: EventTickSyncICMSRequestStatus})
	}
	q := a.resourceEventWorkerQueues[EventTickUpdateHeartbeat]
	require.Equal(t, 1, q.Len(), "status ticks must enqueue one coalesced check on the heartbeat worker")
	item, shutdown := q.Get()
	require.False(t, shutdown)
	q.Done(item)
	require.Equal(t, "capacity-check", item)
}
