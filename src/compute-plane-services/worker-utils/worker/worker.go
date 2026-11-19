/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/nvkit/clients"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/nvkit/logs"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/nvkit/servers"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/nvkit/tracing"
	"github.com/google/uuid"
	"github.com/hellofresh/health-go/v5"
	"github.com/nats-io/nats.go"
	"github.com/panjf2000/ants/v2"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/worker-utils/worker/httpstream"
	utilsMetrics "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/worker-utils/worker/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/worker-utils/worker/progress"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/auth"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/consumer"
	infHealth "github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/health"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/metering"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/metrics"
	nvcfMetrics "github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/metrics/nvcf"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/nvcf"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/proto/nvcf"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/proxy"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/types"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/worker/utils"
)

const defaultBaseInferenceDomain = "127.0.0.1"
const defaultInferenceReadyTimeout = time.Duration(30) * time.Minute
const defaultEssAgentConfigDir = "/config/ess-agent"
const defaultSharedConfigDir = "/config/shared"

// EssTokenFileName is the filename the ESS agent writes the JWT assertion
// token to. Kept local to avoid a cross-service module dependency on
// worker-init (which owns the ESS agent that produces this file).
const EssTokenFileName = "jwt.token"

type Config struct {
	NVCFWorkerToken                     string        `mapstructure:"NVCF_WORKER_TOKEN"`
	NVCFFqdn                            string        `mapstructure:"NVCF_FQDN_GRPC"`
	NVCFFqdnNATS                        string        `mapstructure:"NVCF_FQDN_NATS"`
	NKeySeed                            *string       `mapstructure:"NKEY_SEED"`         // will use the worker token if not set
	NVCFNatsAccount                     string        `mapstructure:"NVCF_NATS_ACCOUNT"` // account the worker token login asks for
	InferencePort                       int           `mapstructure:"INFERENCE_PORT"`
	InferenceHealthEndpoint             string        `mapstructure:"INFERENCE_HEALTH_ENDPOINT"`
	InferenceHealthProtocol             string        `mapstructure:"INFERENCE_HEALTH_PROTOCOL"`
	InferenceHealthTimeout              time.Duration `mapstructure:"INFERENCE_HEALTH_TIMEOUT"`
	InferenceHealthPort                 int           `mapstructure:"INFERENCE_HEALTH_PORT"`
	InferenceHealthExpectedResponseCode int           `mapstructure:"INFERENCE_HEALTH_EXPECTED_RESPONSE_CODE"`
	HelmChartInferenceServiceName       string        `mapstructure:"HELM_CHART_INFERENCE_SERVICE_NAME"`
	HelmChartNamespace                  string        `mapstructure:"HELM_CHART_NAMESPACE"`
	// The time.Duration unmarshal converts from a string, so suffixes are expected (ms, s, etc).
	// Because it can vary, we won't set a unit on the variable name.
	InferenceReadyTimeout          time.Duration `mapstructure:"INFERENCE_READY_TIMEOUT"`
	InfraMeteringHeartbeatInterval time.Duration `mapstructure:"INFRA_METERING_HEARTBEAT_INTERVAL_SECS"`
	OTELExporterOTLPEndpoint       string        `mapstructure:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	TracingAccessToken             string        `mapstructure:"TRACING_ACCESS_TOKEN"`
	NcaId                          string        `mapstructure:"NCA_ID"`
	BillingNcaId                   string        `mapstructure:"BILLING_NCA_ID"`
	FunctionId                     string        `mapstructure:"FUNCTION_ID"`
	FunctionVersionId              string        `mapstructure:"FUNCTION_VERSION_ID"`
	FunctionName                   string        `mapstructure:"FUNCTION_NAME"`
	BaseAssetDir                   string        `mapstructure:"BASE_ASSET_DIR"`
	BaseResponseDir                string        `mapstructure:"BASE_RESPONSE_DIR"`
	HealthPort                     int           `mapstructure:"HEALTH_PORT"`
	CloudProvider                  string        `mapstructure:"CLOUD_PROVIDER"`
	CloudPlatform                  string        `mapstructure:"CLOUD_PLATFORM"`
	InstanceType                   string        `mapstructure:"INSTANCE_TYPE"`
	// SpotEnvironment is deprecated: use ICMSEnvironment (or set ICMS_ENVIRONMENT). If unset, ICMSEnvironment is set from this in setup.
	SpotEnvironment       string   `mapstructure:"SPOT_ENVIRONMENT"`
	ICMSEnvironment       string   `mapstructure:"ICMS_ENVIRONMENT"`
	ZoneName              string   `mapstructure:"ZONE_NAME"`
	GpuType               string   `mapstructure:"GPU_NAME"`
	GpuCount              int      `mapstructure:"ATTACHED_GPU_COUNT"`
	InstanceId            string   `mapstructure:"INSTANCE_ID"`
	MaxRequestConcurrency int      `mapstructure:"MAX_REQUEST_CONCURRENCY"`
	FunctionTags          []string `mapstructure:"FUNCTION_TAGS"` // csv
	EssAgentConfigDir     string   `mapstructure:"ESS_AGENT_CONFIG_DIR"`
	SecretsAssertionToken string   `mapstructure:"SECRETS_ASSERTION_TOKEN"`
	SharedConfigDir       string   `mapstructure:"SHARED_CONFIG_DIR"`
	// by default, compatibility mode is enabled
	V3BackwardsCompatibilityDisabled bool   `mapstructure:"V3_BACKWARDS_COMPATIBILITY_DISABLED"`
	InferenceServiceName             string `mapstructure:"INFERENCE_SERVICE_NAME"`
	InferenceNamespace               string `mapstructure:"INFERENCE_NAMESPACE"`
}

type NVCFWorker struct {
	globalCtx               context.Context
	config                  Config
	meteringConfig          *metering.Config
	baseAssetDir            string
	baseResponseDir         string
	nvcfClient              *nvcf.Client
	workerRestClient        *clients.HTTPClient
	baseInferenceDomain     string
	inferenceUrlWithoutPath string
	server                  servers.Server
	progressMonitor         *progress.Monitor
	health                  *health.Health
	httpProxy               *proxy.HttpProxy
	sharedProxyClient       httpstream.ProxyClient
	shutdownCtx             context.Context
	shutdownCancel          context.CancelFunc

	// set while NATS consumer setup keeps failing, so the worker reports not ready instead of looking able to
	// receive work
	natsConsumerSetupFailing atomic.Bool

	// in-flight requests keyed by request id; looked up on a NATS
	// cancel broadcast to fire the matching per-request cancel.
	cancelSubMu     sync.Mutex
	inFlightCancels map[string]context.CancelCauseFunc
	cancelSub       *nats.Subscription
}

func NewNVCFWorker(ctx context.Context, zapLogger *logs.ZapLogger, config Config) (*NVCFWorker, error) {
	_, err := uuid.Parse(config.FunctionId)
	if err != nil {
		return nil, types.NewInternalError(fmt.Errorf("invalid function id %s", config.FunctionId))
	}
	_, err = uuid.Parse(config.FunctionVersionId)
	if err != nil {
		return nil, types.NewInternalError(fmt.Errorf("invalid function version id %s", config.FunctionVersionId))
	}
	otlpHost, otlpInsecure, err := otlpTraceTarget(config.OTELExporterOTLPEndpoint)
	if err != nil {
		return nil, types.NewInternalError(err)
	}
	if otlpHost == "" {
		zap.L().Info("OTEL_EXPORTER_OTLP_ENDPOINT is not set, tracing is disabled",
			zap.String("function_id", config.FunctionId),
			zap.String("function_version_id", config.FunctionVersionId))
	}
	config.MaxRequestConcurrency = max(1, config.MaxRequestConcurrency) // at least 1

	if config.ICMSEnvironment == "" {
		config.ICMSEnvironment = config.SpotEnvironment
	}

	baseInferenceDomain := defaultBaseInferenceDomain

	if config.InferenceServiceName != "" && config.InferenceNamespace != "" {
		zap.L().Info("Inference configuration:  ",
			zap.String("namespace", config.InferenceNamespace),
			zap.String("service name", config.InferenceServiceName))
		baseInferenceDomain = fmt.Sprintf("%s.%s.svc.cluster.local", config.InferenceServiceName, config.InferenceNamespace)
	}

	if config.HelmChartInferenceServiceName != "" && config.HelmChartNamespace != "" {
		zap.L().Info("helm chart configuration",
			utils.PublicLogMarker,
			zap.String("namespace", config.HelmChartNamespace),
			zap.String("service name", config.HelmChartInferenceServiceName))
		baseInferenceDomain = fmt.Sprintf("%s.%s.svc.cluster.local", config.HelmChartInferenceServiceName, config.HelmChartNamespace)
	}

	baseInferenceUrl := fmt.Sprintf("http://%s:%d", baseInferenceDomain, config.InferencePort)
	clientConfig := clients.HTTPClientConfig{
		BaseClientConfig: &clients.BaseClientConfig{
			Type: "http",
		},
		NumRetries: 0,
	}
	client, err := clients.DefaultHTTPClient(&clientConfig, func(_ string, r *http.Request) string {
		return r.URL.Path
	})
	if err != nil {
		return nil, types.NewInternalError(err)
	}

	zap.L().Info("inference configuration",
		utils.PublicLogMarker,
		zap.String("domain", baseInferenceDomain),
		zap.Int("port", config.InferencePort))

	if config.SharedConfigDir == "" {
		config.SharedConfigDir = defaultSharedConfigDir
	}

	baseAssetDir := defaultBaseAssetDir
	if config.BaseAssetDir != "" {
		baseAssetDir = config.BaseAssetDir
	}

	baseResponseDir := defaultBaseResponseDir
	if config.BaseResponseDir != "" {
		baseResponseDir = config.BaseResponseDir
	}

	if config.HealthPort <= 0 {
		config.HealthPort = 8080
	}
	if config.NVCFFqdnNATS == "" {
		zap.L().Warn("using default NATS url")
		config.NVCFFqdnNATS = nats.DefaultURL
	}

	infraMeteringHeartbeatInterval := config.InfraMeteringHeartbeatInterval
	if infraMeteringHeartbeatInterval == 0 {
		infraMeteringHeartbeatInterval = metering.DefaultInfraMeteringHeartbeatInterval
	}
	billingNcaId := config.BillingNcaId
	if billingNcaId == "" {
		billingNcaId = config.NcaId
	}
	meteringConfig := metering.Config{
		Backend:                config.CloudProvider,
		NcaId:                  config.NcaId,
		BillingNcaId:           billingNcaId,
		NspectId:               metering.NspectIdFromEnv(),
		FunctionId:             config.FunctionId,
		FunctionVersionId:      config.FunctionVersionId,
		InstanceId:             config.InstanceId,
		InstanceType:           config.InstanceType,
		ICMSEnvironment:        config.ICMSEnvironment,
		ZoneName:               config.ZoneName,
		StartupTime:            time.Now(),
		InfraHeartbeatInterval: infraMeteringHeartbeatInterval,
		GpuCount:               config.GpuCount,
		GpuType:                config.GpuType,
		FunctionTags:           config.FunctionTags,
	}
	if meteringConfig.Backend == "NGN" {
		meteringConfig.Backend = "GFN"
	}

	progressMonitor := progress.New(baseResponseDir)
	utilsMetrics.InitProgressMonitorMetrics(progressMonitor)

	if config.InferenceReadyTimeout == 0 {
		config.InferenceReadyTimeout = defaultInferenceReadyTimeout
	}

	h, err := health.New(health.WithComponent(health.Component{
		Name: "gdn-nvcf-worker-service",
	}))
	if err != nil {
		return nil, types.NewInternalError(err)
	}

	healthCheckConfig, err := infHealth.HealthCheckConfig(baseInferenceDomain, config.InferenceHealthProtocol, config.InferenceHealthEndpoint, config.InferenceHealthTimeout, config.InferenceHealthExpectedResponseCode, config.InferenceHealthPort, config.InferencePort)
	if err != nil {
		return nil, types.NewUserActionableError(err)
	}

	err = h.Register(healthCheckConfig)
	if err != nil {
		return nil, types.NewInternalError(err)
	}

	shutdownCtx, shutdownCancel := context.WithCancel(ctx)

	var w *NVCFWorker
	w = &NVCFWorker{
		globalCtx:               ctx,
		shutdownCtx:             shutdownCtx,
		shutdownCancel:          shutdownCancel,
		config:                  config,
		meteringConfig:          &meteringConfig,
		workerRestClient:        client,
		baseInferenceDomain:     baseInferenceDomain,
		inferenceUrlWithoutPath: baseInferenceUrl,
		baseAssetDir:            baseAssetDir,
		baseResponseDir:         baseResponseDir,
		progressMonitor:         progressMonitor,
		health:                  h,
		sharedProxyClient:       httpstream.NewProxiedClient(),
		inFlightCancels:         make(map[string]context.CancelCauseFunc),
		server: servers.NewGRPCServer(&servers.GRPCConfig{
			HTTPAddr:  "0.0.0.0:9190",
			GRPCAddr:  "0.0.0.0:9191",
			AdminAddr: "0.0.0.0:" + strconv.Itoa(config.HealthPort),
			AdditionalServers: []servers.ServerFuncPair{
				{
					Execute: func() error {
						<-shutdownCtx.Done()
						return nil
					},
					Interrupt: func(err error) {
						if err != nil {
							zap.L().Info("received shutdown signal, initiating graceful shutdown...")
							shutdownCancel()
						}
					},
				},
			},

			BaseServerConfig: &servers.BaseServerConfig{
				ServiceName: "gdn-nvcf-worker-service",
				Version:     utils.Version,
				Tracing: tracing.OTELConfig{
					Enabled:     otlpHost != "",
					Endpoint:    otlpHost,
					Insecure:    otlpInsecure,
					AccessToken: config.TracingAccessToken,
					Attributes: tracing.Attributes{
						Extra: map[string]string{
							"function_id":             config.FunctionId,
							"function_version_id":     config.FunctionVersionId,
							"function_name":           config.FunctionName,
							"host.provider":           config.CloudProvider,
							"host.platform":           config.CloudPlatform,
							"instance.type":           config.InstanceType,
							"nca_id":                  config.NcaId,
							"host.id":                 config.InstanceId,
							"host.dc":                 config.ZoneName,
							"gpu.type":                config.GpuType,
							"max_request_concurrency": strconv.Itoa(config.MaxRequestConcurrency),
						},
					},
				},
			},
		},
			servers.WithLogger(zapLogger),
			// only running a healthcheck server
			servers.WithRegisterServer(func(*grpc.Server) {}),
			servers.WithHttpHealthEndpoints("/v1/health/live"),
			servers.WithAdditionalHTTPHandlers(map[string]http.Handler{"/v1/health/ready": http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				w.serveReady(rw, r, h.Measure)
			})}),
		),
	}
	return w, nil
}

func (w *NVCFWorker) Setup() error {
	err := w.SetupWorkDirs()
	if err != nil {
		return types.NewInternalError(err)
	}

	if err := w.server.Setup(); err != nil {
		return types.NewInternalError(err)
	}

	return nil
}

// SetupWorkDirs is visible for testing
func (w *NVCFWorker) SetupWorkDirs() error {
	_, err := os.Stat(w.baseResponseDir)
	if errors.Is(err, os.ErrNotExist) {
		err = utils.CreateDirectory(w.baseResponseDir, os.FileMode(0777))
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	_, err = os.Stat(w.baseAssetDir)
	if errors.Is(err, os.ErrNotExist) {
		err = utils.CreateDirectory(w.baseAssetDir, os.FileMode(0777))
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

// Run withHttpServer argument is for testing
func (w *NVCFWorker) Run(withHttpServer bool) error {
	zap.L().Info("starting worker",
		utils.PublicLogMarker,
		zap.String("instance id", w.config.InstanceId),
		zap.String("function id", w.config.FunctionId),
		zap.String("function version id", w.config.FunctionVersionId),
		zap.String("version", utils.Version))
	zap.L().Info("starting progress monitor")
	err := w.progressMonitor.Start()
	if err != nil {
		zap.L().Error("failed to start progress monitor", zap.Error(err))
		return types.NewInternalError(err)
	}
	defer utils.Close(func() error {
		zap.L().Info("stopping progress monitor")
		return w.progressMonitor.Stop()
	})
	if withHttpServer {
		go func() {
			// health server + framework config
			err := w.server.Run()
			if w.isFatalServerError(err) {
				err = types.NewInternalError(err)
				utils.ExitReason(err)
				zap.S().Panic(err)
			}
		}()
	}

	infraMetering := metering.NewInfraMetering(w.globalCtx, metering.Function, w.meteringConfig, metering.InfraInitializing)
	defer utils.Close(infraMetering.Close)

	nvcfClient, err := nvcf.CreateClient(w.config.NVCFFqdn,
		&w.config.NVCFFqdnNATS,
		w.config.NVCFWorkerToken,
		w.config.NKeySeed,
		w.config.NcaId,
		w.config.InstanceId,
		w.config.FunctionId,
		w.config.FunctionVersionId,
		w.config.SharedConfigDir,
		nvcf.DefaultNvcfClientTimeout,
		nvcf.WithNatsAccount(w.config.NVCFNatsAccount))
	if err != nil {
		return types.NewInternalError(err)
	}
	w.nvcfClient = nvcfClient
	defer w.nvcfClient.Close()

	defer ((*http.Client)(w.sharedProxyClient)).CloseIdleConnections()
	w.nvcfClient.StartMetadataCredentialsRefresher(w.globalCtx)

	// Start ess agent assertion token refreshment for container deployment
	if w.config.SecretsAssertionToken != "" {
		tokenDir := w.config.EssAgentConfigDir
		if tokenDir == "" {
			tokenDir = defaultEssAgentConfigDir
		}
		tokenPath := filepath.Join(tokenDir, EssTokenFileName)
		w.nvcfClient.StartAssertionTokenRefresher(w.globalCtx, tokenPath, true)
	} else {
		zap.L().Info("Skip refreshing ESS assertion token")
	}

	inferenceAddress := w.baseInferenceDomain + ":" + strconv.Itoa(w.config.InferencePort)
	httpProxy, err := proxy.NewHttpProxy(w.nvcfClient.NatsConn, w.nvcfClient.JetStream, w.config.FunctionId, w.config.FunctionVersionId, func(request *httputil.ProxyRequest) {
		request.Out.URL.Scheme = "http"
		request.Out.URL.Host = inferenceAddress
	}, nil, nil, nil)
	if err != nil {
		return types.NewInternalError(err)
	}
	w.httpProxy = httpProxy
	defer utils.Close(w.httpProxy.Close)

	// Wait for inference container to be ready.
	zap.L().Info("waiting for inference container to be ready",
		utils.PublicLogMarker,
		zap.Duration("timeout", w.config.InferenceReadyTimeout))
	checkInterval := min(w.config.InferenceReadyTimeout, 500*time.Millisecond)
	maxRetries := int(w.config.InferenceReadyTimeout / checkInterval)
	_, _, err = lo.AttemptWithDelay(maxRetries, checkInterval, func(i int, duration time.Duration) error {
		result := w.health.Measure(w.globalCtx)
		if result.Status != health.StatusOK {
			nvcfMetrics.HealthcheckCounter.WithLabelValues("failure").Inc()
			return errors.New("inference container not ready")
		}
		nvcfMetrics.HealthcheckCounter.WithLabelValues("success").Inc()
		return nil
	})
	if err != nil {
		zap.L().Info("inference container failed to become ready",
			utils.PublicLogMarker,
			zap.Duration("timeout", w.config.InferenceReadyTimeout))
		return types.NewUserActionableError(err)
	}
	zap.L().Info("inference container is ready")
	infraMetering.SetStatus(metering.InfraReady)

	connectedCtx, err := w.nvcfClient.ConnectIndefinitely(w.globalCtx)
	if err != nil {
		return types.NewInternalError(err)
	}

	// subscribe to cancel broadcasts so a client 504 / disconnect on the IS
	// can tear down our in-flight inference.
	cancelSubject := fmt.Sprintf("nvcf.cancel.%s", w.config.FunctionVersionId)
	cancelSub, err := w.nvcfClient.NatsConn.Subscribe(cancelSubject, w.handleCancelMessage)
	if err != nil {
		return types.NewInternalError(fmt.Errorf("subscribe %s: %w", cancelSubject, err))
	}
	w.cancelSub = cancelSub
	defer func() {
		if err := w.cancelSub.Unsubscribe(); err != nil {
			zap.L().Warn("failed to unsubscribe cancel subject", zap.Error(err))
		}
	}()

	// add nats health check now that we're connected
	err = w.health.Register(health.Config{
		Name:    "nats",
		Timeout: 1 * time.Second,
		Check: func(ctx context.Context) error {
			if !w.nvcfClient.NatsConn.IsConnected() {
				return fmt.Errorf("nats not connected")
			}
			return nil
		},
	})
	if err != nil {
		return types.NewInternalError(err)
	}

	err = metrics.Initialize(metrics.NvcfRootNamespace, w.nvcfClient.NatsConn)
	if err != nil {
		return types.NewInternalError(err)
	}

	nvcfMetrics.WorkerNatsServerGauge.WithLabelValues(w.config.NVCFFqdnNATS).Set(1)

	zap.L().Info("starting work sessions", zap.Int("concurrency", w.config.MaxRequestConcurrency))
	return w.workSession(connectedCtx)
}

func (w *NVCFWorker) workSession(ctx context.Context) error {
	nvcfMetrics.WorkerThreadCountGauge.Add(float64(w.config.MaxRequestConcurrency))
	defer nvcfMetrics.WorkerThreadCountGauge.Sub(float64(w.config.MaxRequestConcurrency))

	pool, err := ants.NewPool(w.config.MaxRequestConcurrency, ants.WithPreAlloc(true))
	if err != nil {
		return types.NewInternalError(err)
	}
	defer pool.Release()

	// Start health check in background
	inferenceHealth := infHealth.NewInferenceHealthStatus()
	go inferenceHealth.RunHealthCheckRoutine(ctx, w.health)

	for {
		// checking if the current ctx ended or the shutdownCtx ended, so we should not requeue another work request
		if ctx.Err() != nil || w.shutdownCtx.Err() != nil {
			return nil
		}

		// stop retrying setup when the worker starts shutting down
		setupCtx, cancelSetup := context.WithCancel(ctx)
		stopSetupAfterFunc := context.AfterFunc(w.shutdownCtx, cancelSetup)
		var connectedRegion *nvcf.ConnectionRegions
		globalNatsConsumer, err := newNatsConsumerWithRetry(setupCtx, newNatsConsumerSetupBackOff(), func() (*consumer.GlobalNatsConsumer, error) {
			connectedRegion = w.nvcfClient.ConnectedRegions.Load()
			c, err := consumer.NewNatsConsumer(setupCtx, w.nvcfClient.JetStream, func(ctx context.Context, region string) error {
				_, err := w.nvcfClient.Client.ProvisionRegionalWorker(ctx, &pb.ProvisionWorkerRequest{
					InstanceId:        w.config.InstanceId,
					FunctionId:        w.config.FunctionId,
					FunctionVersionId: w.config.FunctionVersionId,
					RegionToProvision: region,
				}, auth.GrpcTokenFromSource(w.nvcfClient.NvcfTokenProvider))
				return err
			}, w.config.FunctionVersionId, connectedRegion.Primary, connectedRegion.Secondaries, w.config.MaxRequestConcurrency)
			if err != nil {
				w.natsConsumerSetupFailing.Store(true)
			}
			return c, err
		}, zap.String("function_id", w.config.FunctionId), zap.String("nca_id", w.config.NcaId))
		stopSetupAfterFunc()
		cancelSetup()
		if err != nil {
			if ctx.Err() != nil || w.shutdownCtx.Err() != nil {
				return nil
			}
			return types.NewInternalError(err)
		}
		// close FetchWorkStream's ctx when region list changes to gracefully end the work stream and loop to fetch another
		fetchCtx, cancelFetch := context.WithCancel(ctx)
		stopAfterFunc := context.AfterFunc(w.shutdownCtx, cancelFetch)

		// Wrap cancelFetch with a function that also stops the afterFunc to prevent memory leaks
		wrappedCancelFetch := func() {
			stopAfterFunc()
			cancelFetch()
		}

		if !w.publishNatsConsumer(globalNatsConsumer, connectedRegion, &wrappedCancelFetch) {
			zap.L().Info("connected regions changed during nats consumer setup, setting it up again",
				zap.String("function_id", w.config.FunctionId), zap.String("nca_id", w.config.NcaId))
			wrappedCancelFetch()
			continue
		}
		inferenceHealth.CallBackFn.Store((*func())(&wrappedCancelFetch))

		zap.L().Info("waiting until inference container is healthy")
		err = inferenceHealth.WaitForHealthyState(fetchCtx)
		if err != nil {
			zap.L().Error("unable to perform health check", zap.Error(err))
			continue
		}
		zap.L().Info("inference container is healthy, will fetch work stream")

		var workRequests <-chan *consumer.WorkRequest
		// retry forever. cancellation is not gated by time.
		exponentialBackOff := backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0))
		err = backoff.Retry(func() error {
			zap.L().Debug("requesting new work stream")
			var err error
			// cancelling fetchCtx will cause the workRequests channel to gracefully close
			workRequests, err = globalNatsConsumer.FetchWorkStream(fetchCtx)
			return err
		}, backoff.WithContext(exponentialBackOff, fetchCtx))
		if err != nil {
			wrappedCancelFetch()
			continue
		}

		for work := range workRequests {
			zap.L().Info("got work request",
				utils.PublicLogMarker,
				zap.String("req id", work.RequestData.RequestId))

			err := pool.Submit(func() {
				defer utils.Close(work.Close)
				logWorkRequestResult(w.handleWorkRequest(ctx, work), work.RequestData.RequestId)
			})
			// failure to submit to the pool is a catastrophic error and should never happen
			// unless the pool is misconfigured. this should be dead code.
			if err != nil {
				zap.L().Debug("nacking message due to failed request submission", zap.String("subject", work.Subject()))
				_ = work.Nak()
				zap.L().Warn("failed to submit work request", zap.Error(err))
				_ = work.Close()
			}
		}
		wrappedCancelFetch()

		// A very long timeout to avoid the pool from being released without completing the already accepted requests
		if w.shutdownCtx.Err() != nil {
			pool.ReleaseTimeout(time.Second * 1000000)
		}

		zap.L().Debug("finished processing work stream")
	}
}

// Consumer setup retries quickly at first, so the worker resumes within seconds after a short outage such as a
// rolling restart of the NATS cluster. After natsConsumerFastRetryPeriod it retries less often, so workers that
// wait out a long outage do not keep calling NVCF every few seconds.
const (
	natsConsumerFastRetryPeriod  = 2 * time.Minute
	natsConsumerFastRetryMaxWait = 4 * time.Second
	natsConsumerSlowRetryMaxWait = 30 * time.Second
)

// newNatsConsumerSetupBackOff returns the retry schedule for consumer setup. It never stops on its own: the worker
// keeps waiting for NATS instead of exiting, because a restart does not bring an unreachable NATS back and each
// restart counts toward replacing the whole instance. The 25% jitter keeps fast waits at or under 5s.
func newNatsConsumerSetupBackOff() backoff.BackOff {
	return newNatsConsumerSetupBackOffWithClock(backoff.SystemClock)
}

func newNatsConsumerSetupBackOffWithClock(clock backoff.Clock) *phasedBackOff {
	newExp := func(firstWait, maxWait time.Duration) *backoff.ExponentialBackOff {
		return backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(firstWait),
			backoff.WithMaxInterval(maxWait),
			backoff.WithRandomizationFactor(0.25),
			backoff.WithMaxElapsedTime(0),
			backoff.WithClockProvider(clock),
		)
	}
	return &phasedBackOff{
		fast: newExp(backoff.DefaultInitialInterval, natsConsumerFastRetryMaxWait),
		// The slow phase starts where the fast phase capped, so waits do not shrink at the switch.
		slow:    newExp(natsConsumerFastRetryMaxWait, natsConsumerSlowRetryMaxWait),
		fastFor: natsConsumerFastRetryPeriod,
		now:     clock.Now,
	}
}

// phasedBackOff uses fast until fastFor has passed since the first wait, then slow.
type phasedBackOff struct {
	fast, slow backoff.BackOff
	fastFor    time.Duration
	now        func() time.Time
	start      time.Time
}

func (b *phasedBackOff) NextBackOff() time.Duration {
	if b.start.IsZero() {
		b.start = b.now()
	}
	if b.now().Sub(b.start) < b.fastFor {
		return b.fast.NextBackOff()
	}
	return b.slow.NextBackOff()
}

func (b *phasedBackOff) Reset() {
	b.start = time.Time{}
	b.fast.Reset()
	b.slow.Reset()
}

// newNatsConsumerWithRetry retries consumer setup with b until it succeeds, ctx ends, b stops, or setup fails with
// an error that retrying cannot fix. Without it, a NATS disconnect longer than the consumer lookup's own short
// retry window exits the worker process.
func newNatsConsumerWithRetry(ctx context.Context, b backoff.BackOff, create func() (*consumer.GlobalNatsConsumer, error), logFields ...zap.Field) (*consumer.GlobalNatsConsumer, error) {
	start := time.Now()
	failures := 0
	attempt := func() (*consumer.GlobalNatsConsumer, error) {
		c, err := create()
		if err != nil && isPermanentSetupError(err) {
			return nil, backoff.Permanent(err)
		}
		return c, err
	}
	c, err := backoff.RetryNotifyWithData(attempt, backoff.WithContext(b, ctx), func(err error, next time.Duration) {
		failures++
		fields := append([]zap.Field{
			zap.Error(err),
			zap.Duration("retryIn", next),
			zap.Int("failedAttempts", failures),
			zap.Duration("failingFor", time.Since(start)),
		}, logFields...)
		zap.L().Warn("failed to set up nats consumer, retrying", fields...)
	})
	if err == nil && failures > 0 {
		fields := append([]zap.Field{
			zap.Int("failedAttempts", failures),
			zap.Duration("failingFor", time.Since(start)),
		}, logFields...)
		zap.L().Info("set up nats consumer after retrying", fields...)
	}
	return c, err
}

// publishNatsConsumer stores the consumer and the trigger that ends its work stream, then reports whether the
// regions it was built for are still the connected regions. The region reload stores the new regions before it
// loads the consumer and trigger, so a region change is either seen here or triggers this consumer. A consumer setup
// failure stops being reported for readiness only once a consumer is accepted.
func (w *NVCFWorker) publishNatsConsumer(c *consumer.GlobalNatsConsumer, builtFor *nvcf.ConnectionRegions,
	trigger *func(),
) bool {
	w.nvcfClient.CurrentNatsConsumer.Store(c)
	w.nvcfClient.TriggerNewNatsConsumer.Store(trigger)
	if !sameRegions(builtFor, w.nvcfClient.ConnectedRegions.Load()) {
		return false
	}
	w.natsConsumerSetupFailing.Store(false)
	return true
}

func sameRegions(a, b *nvcf.ConnectionRegions) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Primary == b.Primary && slices.Equal(slices.Sorted(slices.Values(a.Secondaries)), slices.Sorted(slices.Values(b.Secondaries)))
}

// serveReady serves the health checks, and also reports not ready while NATS consumer setup keeps failing, for
// example when NATS is connected but the consumer cannot be attached, so the worker does not look able to receive
// work. The consumer state is kept out of w.health because the inference health loop also gates fetching on it.
func (w *NVCFWorker) serveReady(rw http.ResponseWriter, r *http.Request, measure func(context.Context) health.Check) {
	c := measure(r.Context())
	if w.natsConsumerSetupFailing.Load() {
		c.Status = health.StatusUnavailable
		if c.Failures == nil {
			c.Failures = map[string]string{}
		}
		c.Failures["nats-consumer"] = "nats consumer setup is failing"
	}
	data, err := json.Marshal(c)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	code := http.StatusOK
	if c.Status == health.StatusUnavailable {
		code = http.StatusServiceUnavailable
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(code)
	_, _ = rw.Write(data)
}

// isPermanentSetupError reports whether worker provisioning rejected the
// request itself, so waiting for NATS to come back will not help.
func isPermanentSetupError(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch s.Code() {
	case codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument:
		return true
	}
	return false
}

// Shutdown triggers a graceful shutdown of the worker -- This is only used by tests
func (w *NVCFWorker) Shutdown() {
	w.shutdownCancel()
}

// otlpTraceTarget parses OTEL_EXPORTER_OTLP_ENDPOINT into the host and
// transport security for the OTLP gRPC trace exporter. An empty or unset
// endpoint returns an empty host, which means tracing is disabled.
func otlpTraceTarget(endpoint string) (host string, insecure bool, err error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", false, nil
	}
	otelURL, err := url.Parse(endpoint)
	if err != nil {
		return "", false, err
	}
	if otelURL.Host == "" {
		return "", false, errors.New("OTEL_EXPORTER_OTLP_ENDPOINT must be a URL with a host, such as http://otel-collector:4317")
	}
	return otelURL.Host, otelURL.Scheme == "http", nil
}
