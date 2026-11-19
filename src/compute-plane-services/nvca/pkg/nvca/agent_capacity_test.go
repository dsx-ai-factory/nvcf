/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/

package nvca

import (
	"context"
	"errors"
	"sync/atomic"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v1alpha1"
	nvcav2 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvca/v2beta1"
	nvcalisters "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/client/listers/nvca/v2beta1"
	featuremock "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/featureflag/mock"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/nodefeatures"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/nvca/enforce"
	nvcaerrors "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/nvca/errors"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/nvca/health"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

type capacityNodeFeatures struct {
	allocation nodefeatures.GPUAllocationGetter
	err        error
	capacities map[types.GPUName]uint64
}

func (c *capacityNodeFeatures) GetAllBackendGPUs(context.Context) ([]types.BackendGPU, error) {
	if c.capacities != nil {
		var gpus []types.BackendGPU
		for name, capacity := range c.capacities {
			gpus = append(gpus, types.BackendGPU{Name: name, Capacity: capacity, Static: true})
		}
		return gpus, c.err
	}
	return []types.BackendGPU{{Name: "L4", Capacity: 1, Static: true}}, c.err
}

func (c *capacityNodeFeatures) GetGPUResources(ctx context.Context, gpu types.GPUName) (types.GPUResource, error) {
	allocated, err := c.allocation.GetForGPU(ctx, gpu)
	capacity := uint64(1)
	if c.capacities != nil {
		capacity = c.capacities[gpu]
	}
	return types.GPUResource{Capacity: capacity, Allocated: allocated}, err
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
	err    error
	action types.HealthAction
}

func (c *capacityHeartbeatClient) PutHealthStatus(ctx context.Context, req *types.HealthStatusRequest) (*types.HealthStatusResponse, error) {
	if c.err != nil {
		return nil, c.err
	}
	response, err := c.mockICMSClient.PutHealthStatus(ctx, req)
	if c.action != "" {
		response.Action = c.action
	}
	return response, err
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

func TestCapacityHeartbeatNoGPUsAndRecovery(t *testing.T) {
	for _, refreshWait := range []time.Duration{0, time.Hour} {
		t.Run(refreshWait.String(), func(t *testing.T) {
			ctx := newTestContext()
			a, requests, _, req, _ := newCapacityTestAgent(t)
			require.NoError(t, requests.Delete(req))
			a.backendHealthCache = health.NewBackendStatusCache(refreshWait, a.backendk8scache)
			icms := a.icmsClient.(*mockICMSClient)
			require.NoError(t, a.PutNVCAStatusUpdate(ctx))
			require.Contains(t, icms.healthStatusRequests[0].GPUUsage, types.GPUName("L4"))

			nf := a.backendk8scache.nfClient.(*capacityNodeFeatures)
			nf.err = nvcaerrors.NotExistError(errors.New("no backend GPUs were found"))
			require.NoError(t, a.putNVCAStatusUpdate(ctx, true))
			require.Len(t, icms.healthStatusRequests, 1, "missing GPUs must not trigger a capacity publication")
			require.NoError(t, a.PutNVCAStatusUpdate(ctx))
			require.Len(t, icms.healthStatusRequests, 2)
			require.Equal(t, types.HealthStatusUnhealthy, icms.healthStatusRequests[1].Status)
			require.Empty(t, icms.healthStatusRequests[1].GPUUsage)

			nf.err = nil
			require.NoError(t, a.putNVCAStatusUpdate(ctx, true))
			require.Len(t, icms.healthStatusRequests, 3)
			require.Contains(t, icms.healthStatusRequests[2].GPUUsage, types.GPUName("L4"))
			require.EqualValues(t, 1, icms.healthStatusRequests[2].GPUUsage["L4"].Available())
		})
	}
}

func TestCapacityHeartbeatPeriodicUsesWarmHealthOnDiscoveryError(t *testing.T) {
	ctx := newTestContext()
	a, requests, _, req, _ := newCapacityTestAgent(t)
	require.NoError(t, requests.Delete(req))
	icms := a.icmsClient.(*mockICMSClient)
	require.NoError(t, a.PutNVCAStatusUpdate(ctx))
	a.backendk8scache.nfClient.(*capacityNodeFeatures).err = errors.New("discovery unavailable")
	require.NoError(t, a.PutNVCAStatusUpdate(ctx))
	require.Len(t, icms.healthStatusRequests, 2)
	require.Equal(t, icms.healthStatusRequests[0].GPUUsage, icms.healthStatusRequests[1].GPUUsage)
	require.Equal(t, types.HealthStatusUnhealthy, icms.healthStatusRequests[1].Status,
		"cached free capacity must not keep a failed discovery healthy")
	require.Error(t, a.putNVCAStatusUpdate(ctx, true))
	require.Len(t, icms.healthStatusRequests, 2)
}

func TestCapacityHeartbeatUsesAllocationAfterHealthRefresh(t *testing.T) {
	for _, capacityOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "periodic", true: "capacity"}[capacityOnly], func(t *testing.T) {
			ctx := newTestContext()
			a, requests, pods, req, pod := newCapacityTestAgent(t)
			insertAllocation := false
			a.backendHealthCache = health.NewBackendStatusCache(0,
				health.GetComponentStatusFunc(func(ctx context.Context) (types.AgentHealth, error) {
					if insertAllocation {
						insertAllocation = false
						replacement := req.DeepCopy()
						replacement.Name = "replacement"
						replacement.Status.Instances = map[string]nvcav2.InstanceStatus{
							"replacement": {ID: "replacement", Type: nvcav2.InstanceTypePod,
								LastReportedStatus: string(types.ICMSInstanceRunning)},
						}
						if err := requests.Add(replacement); err != nil {
							return types.AgentHealth{}, err
						}
					}
					return a.backendk8scache.GetComponentStatus(ctx)
				}),
			)
			icms := a.icmsClient.(*mockICMSClient)
			require.NoError(t, a.PutNVCAStatusUpdate(ctx))
			require.NoError(t, pods.Delete(pod))
			updated := req.DeepCopy()
			inst := updated.Status.Instances["worker"]
			inst.LastReportedStatus = string(types.ICMSInstanceTerminated)
			updated.Status.Instances["worker"] = inst
			require.NoError(t, requests.Update(updated))
			insertAllocation = true
			require.NoError(t, a.putNVCAStatusUpdate(ctx, capacityOnly))
			require.EqualValues(t, 1, a.backendHealthCache.GetStatus().GPUUsage["L4"].Allocated)
			if capacityOnly {
				require.Len(t, icms.healthStatusRequests, 1, "the apparent increase disappeared during health refresh")
			} else {
				require.Len(t, icms.healthStatusRequests, 2)
				require.Contains(t, icms.healthStatusRequests[1].GPUUsage, types.GPUName("L4"))
				require.EqualValues(t, 1, icms.healthStatusRequests[1].GPUUsage["L4"].Allocated)
			}
		})
	}
}

func TestCapacityHeartbeatMultipleGPUTypes(t *testing.T) {
	ctx := newTestContext()
	a, requests, pods, req, pod := newCapacityTestAgent(t)
	nf := a.backendk8scache.nfClient.(*capacityNodeFeatures)
	nf.capacities = map[types.GPUName]uint64{"L4": 1, "A100": 2}
	icms := a.icmsClient.(*mockICMSClient)
	require.NoError(t, a.PutNVCAStatusUpdate(ctx))
	nf.capacities["A100"] = 1
	require.NoError(t, a.putNVCAStatusUpdate(ctx, true))
	require.Len(t, icms.healthStatusRequests, 1, "a decrease alone must not trigger a capacity heartbeat")
	require.NoError(t, pods.Delete(pod))
	require.NoError(t, requests.Delete(req))
	require.NoError(t, a.putNVCAStatusUpdate(ctx, true))
	require.Len(t, icms.healthStatusRequests, 2)
	usage := icms.healthStatusRequests[1].GPUUsage
	require.Contains(t, usage, types.GPUName("L4"))
	require.Contains(t, usage, types.GPUName("A100"))
	require.EqualValues(t, 1, usage["L4"].Available())
	require.EqualValues(t, 1, usage["A100"].Capacity)
	nf.capacities["H100"] = 4
	require.NoError(t, a.putNVCAStatusUpdate(ctx, true))
	require.Len(t, icms.healthStatusRequests, 3)
	require.Contains(t, icms.healthStatusRequests[2].GPUUsage, types.GPUName("H100"))
}

func TestCapacityHeartbeatMiniServiceRelease(t *testing.T) {
	for _, reportFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "removed_first", true: "reported_first"}[reportFirst], func(t *testing.T) {
			ctx := newTestContext()
			a, requests, _, req, _ := newCapacityTestAgent(t)
			ms := &v1alpha1.MiniService{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}
			var gets atomic.Int32
			a.backendk8scache.clients.HelmV2 = ctrlfake.NewClientBuilder().
				WithScheme(newMiniServiceScheme()).WithObjects(ms).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
						obj client.Object, opts ...client.GetOption) error {
						gets.Add(1)
						return c.Get(ctx, key, obj, opts...)
					},
				}).Build()
			updated := req.DeepCopy()
			inst := updated.Status.Instances["worker"]
			inst.Type = nvcav2.InstanceTypeMiniService
			updated.Status.Instances["worker"] = inst
			require.NoError(t, requests.Update(updated))
			icms := a.icmsClient.(*mockICMSClient)
			require.NoError(t, a.PutNVCAStatusUpdate(ctx))
			require.Zero(t, gets.Load(), "unreported instances do not need a live object lookup")
			report := func() {
				reported := updated.DeepCopy()
				inst.LastReportedStatus = string(types.ICMSInstanceTerminated)
				reported.Status.Instances["worker"] = inst
				require.NoError(t, requests.Update(reported))
			}
			remove := func() { require.NoError(t, a.backendk8scache.clients.HelmV2.Delete(ctx, ms)) }
			first, second := remove, report
			if reportFirst {
				first, second = report, remove
			}
			first()
			require.NoError(t, a.putNVCAStatusUpdate(ctx, true))
			require.Len(t, icms.healthStatusRequests, 1)
			second()
			require.NoError(t, a.putNVCAStatusUpdate(ctx, true))
			require.Len(t, icms.healthStatusRequests, 2)
			require.Contains(t, icms.healthStatusRequests[1].GPUUsage, types.GPUName("L4"))
			require.Zero(t, icms.healthStatusRequests[1].GPUUsage["L4"].Allocated)
		})
	}
}

type blockingCapacityEviction struct {
	ICMSRequestHelper
	entered chan struct{}
	release chan struct{}
}

func (b *blockingCapacityEviction) PurgeInstanceID(context.Context, *nvcav2.ICMSRequest,
	map[string]nvcav2.InstanceStatus, string) bool {
	b.entered <- struct{}{}
	<-b.release
	return false
}

func (b *blockingCapacityEviction) AllInstancesTerminatedAndReported(ctx context.Context, req *nvcav2.ICMSRequest) bool {
	select {
	case <-b.release:
		return true
	default:
		return b.ICMSRequestHelper.AllInstancesTerminatedAndReported(ctx, req)
	}
}

func TestCapacityHeartbeatSelfDestructStartsOnce(t *testing.T) {
	for _, forced := range []bool{false, true} {
		t.Run(map[bool]string{false: "remote", true: "forced"}[forced], func(t *testing.T) {
			ctx := newTestContext()
			a, requests, pods, req, pod := newCapacityTestAgent(t)
			a.selfDestruct = &atomic.Bool{}
			a.ForceSelfDestruct = forced
			icms := &capacityHeartbeatClient{action: types.HealthActionSelfDestruct}
			if forced {
				icms.action = types.HealthActionAccepted
			}
			a.icmsClient = icms
			blocker := &blockingCapacityEviction{ICMSRequestHelper: a.backendk8scache.icmsRequestHelper,
				entered: make(chan struct{}, 4), release: make(chan struct{})}
			a.backendk8scache.icmsRequestHelper = blocker
			t.Cleanup(func() {
				close(blocker.release)
				require.Eventually(t, a.selfDestruct.Load, 5*time.Second, time.Millisecond)
			})
			require.NoError(t, a.PutNVCAStatusUpdate(ctx))
			select {
			case <-blocker.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("eviction did not start")
			}
			require.NoError(t, pods.Delete(pod))
			updated := req.DeepCopy()
			inst := updated.Status.Instances["worker"]
			inst.LastReportedStatus = string(types.ICMSInstanceTerminated)
			updated.Status.Instances["worker"] = inst
			require.NoError(t, requests.Update(updated))
			require.NoError(t, a.putNVCAStatusUpdate(ctx, true))
			require.Len(t, icms.healthStatusRequests, 2)
			require.False(t, a.selfDestruct.Load(), "status reporting must remain enabled during eviction")
			select {
			case <-blocker.entered:
				t.Fatal("a second heartbeat started concurrent eviction")
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

type observedCapacityHeartbeatClient struct {
	mockICMSClient
	sent chan *types.HealthStatusRequest
}

func (c *observedCapacityHeartbeatClient) PutHealthStatus(_ context.Context,
	req *types.HealthStatusRequest) (*types.HealthStatusResponse, error) {
	c.sent <- req
	return &types.HealthStatusResponse{Action: types.HealthActionAccepted}, nil
}

func TestCapacityHeartbeatRunsOnEventWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(newTestContext())
	t.Cleanup(cancel)
	a, requests, _, req, _ := newCapacityTestAgent(t)
	require.NoError(t, a.PutNVCAStatusUpdate(ctx))
	require.NoError(t, requests.Delete(req))
	icms := &observedCapacityHeartbeatClient{sent: make(chan *types.HealthStatusRequest, 4)}
	a.icmsClient = icms
	require.NoError(t, a.startEventProcessingWorkers(ctx))
	a.apply(ctx, &core.Event{Kind: EventTickSyncICMSRequestStatus})
	select {
	case heartbeat := <-icms.sent:
		require.Contains(t, heartbeat.GPUUsage, types.GPUName("L4"))
		require.EqualValues(t, 1, heartbeat.GPUUsage["L4"].Available())
	case <-time.After(5 * time.Second):
		t.Fatal("the heartbeat worker did not publish released capacity")
	}
}

func TestCapacityHeartbeatMiniServiceLookupErrorRetainsAllocation(t *testing.T) {
	ctx := newTestContext()
	a, requests, _, req, _ := newCapacityTestAgent(t)
	updated := req.DeepCopy()
	inst := updated.Status.Instances["worker"]
	inst.Type = nvcav2.InstanceTypeMiniService
	inst.LastReportedStatus = string(types.ICMSInstanceTerminated)
	updated.Status.Instances["worker"] = inst
	require.NoError(t, requests.Update(updated))
	a.backendk8scache.clients.HelmV2 = ctrlfake.NewClientBuilder().WithScheme(newMiniServiceScheme()).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return errors.New("API lookup unavailable")
			},
		}).Build()
	require.NoError(t, a.PutNVCAStatusUpdate(ctx))
	icms := a.icmsClient.(*mockICMSClient)
	require.Contains(t, icms.healthStatusRequests[0].GPUUsage, types.GPUName("L4"))
	require.EqualValues(t, 1, icms.healthStatusRequests[0].GPUUsage["L4"].Allocated)
	require.NoError(t, a.putNVCAStatusUpdate(ctx, true))
	require.Len(t, icms.healthStatusRequests, 1, "an unknown object state must not release capacity")
}

func TestCapacityHeartbeatNoGPUsStillProcessesSelfDestruct(t *testing.T) {
	ctx := newTestContext()
	a, requests, _, req, _ := newCapacityTestAgent(t)
	require.NoError(t, requests.Delete(req))
	a.selfDestruct = &atomic.Bool{}
	a.backendHealthCache = health.NewBackendStatusCache(0, a.backendk8scache)
	a.backendk8scache.nfClient.(*capacityNodeFeatures).err = nvcaerrors.NotExistError(errors.New("no GPUs"))
	a.icmsClient = &capacityHeartbeatClient{action: types.HealthActionSelfDestruct}
	require.NoError(t, a.PutNVCAStatusUpdate(ctx))
	require.Eventually(t, a.selfDestruct.Load, 5*time.Second, time.Millisecond)
}
