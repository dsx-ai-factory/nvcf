// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use stargate_runtime::{OwnedTask, TASK_SHUTDOWN_TIMEOUT};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::bringup::{
    BringupConfig, BringupError, BringupTaskConfig, CalibrationConfig, check_readiness_gate,
    check_upstream_health, run_bringup_task, run_calibration,
};
use crate::model_discovery::{ModelDiscoveryConfig, ModelDiscoveryError, discover_model_ids};
use crate::runtime_state::{ModelGeneration, PylonRuntimeState};
use crate::stats::{
    CalibrationOutcome, ModelStatsInitialization, PylonMetrics, StatsCollectorControl,
    StatsCollectorExit, StatsCollectorHandle,
};
use crate::upstream_health::{ReadinessGate, UpstreamHealthPaths};

const STATIC_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const STARTUP_HEALTH_RETRY_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone, Debug)]
pub enum ModelSource {
    Static(BTreeSet<String>),
    Discovered(ModelDiscoveryConfig),
}

#[derive(Clone, Debug)]
pub enum ModelInitialization {
    Calibration(CalibrationConfig),
    ConfiguredInputTps { input_tps: f64 },
    Uncalibrated,
}

#[derive(Clone, Debug)]
pub struct ModelLifecycleConfig {
    pub upstream_http_base_url: String,
    pub source: ModelSource,
    pub initialization: ModelInitialization,
    pub bringup: BringupConfig,
    pub health_paths: UpstreamHealthPaths,
    /// When set, every upstream probe at startup and in model initialization uses
    /// this strict check instead of `health_paths`; startup waits for it even
    /// with bringup off.
    pub readiness_gate: Option<ReadinessGate>,
    /// How long startup retries the upstream health probe before failing. Zero
    /// probes once and leaves recovery to the container restart.
    pub startup_health_wait: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ModelLifecycleError {
    #[error("model discovery failed: {0}")]
    Discovery(#[from] ModelDiscoveryError),
    #[error("model initialization failed: {0}")]
    Bringup(#[from] BringupError),
    #[error("stats collector stopped during model lifecycle transition")]
    StatsCollectorStopped,
    #[error("model generation transition lost exact ownership")]
    StaleGeneration,
    #[error("model generation counter exhausted")]
    GenerationCounterExhausted,
}

pub struct ModelLifecycleHandle {
    task: OwnedTask,
    stopped_by_stats_collector: Arc<AtomicBool>,
}

impl ModelLifecycleHandle {
    pub async fn wait_for_exit(&mut self) -> Result<(), tokio::task::JoinError> {
        self.task.wait_for_exit().await
    }

    /// Whether the supervisor stopped because the stats collector ended other
    /// than by its handle's shutdown, for example by a panic. The collector's
    /// own exit then follows promptly.
    pub fn stopped_by_stats_collector(&self) -> bool {
        self.stopped_by_stats_collector.load(Ordering::Acquire)
    }

    pub async fn shutdown(self) {
        self.task.shutdown(TASK_SHUTDOWN_TIMEOUT).await;
    }
}

/// The supervisor's view of a shutdown: its own stop, and how the stats
/// collector ended. Pylon stops both in one step, and the collector can stop
/// first.
struct SupervisorShutdown {
    stop: CancellationToken,
    stats_collector: StatsCollectorExit,
}

struct LiveModelGeneration {
    generation: ModelGeneration,
    canary: Option<OwnedTask>,
}

struct PendingInitialization {
    generation: ModelGeneration,
    retry_at: Instant,
}

type DiscoveryPollResult = (
    ModelDiscoveryConfig,
    Result<BTreeSet<String>, ModelDiscoveryError>,
);

enum InitializationAttemptOutcome {
    Ready,
    Retired,
    Cancelled,
    Failed(ModelLifecycleError),
}

struct ModelLifecycleSupervisor {
    config: ModelLifecycleConfig,
    runtime_state: PylonRuntimeState,
    stats: StatsCollectorControl,
    metrics: Option<Arc<PylonMetrics>>,
    http_client: reqwest::Client,
    live: HashMap<String, LiveModelGeneration>,
    pending: Vec<PendingInitialization>,
    next_generation: u64,
    next_poll_at: Option<Instant>,
}

pub async fn start_model_lifecycle(
    config: ModelLifecycleConfig,
    runtime_state: PylonRuntimeState,
    stats_collector: &StatsCollectorHandle,
    metrics: Option<Arc<PylonMetrics>>,
) -> Result<ModelLifecycleHandle, ModelLifecycleError> {
    let http_client = reqwest::Client::new();
    Url::parse(&config.upstream_http_base_url).map_err(ModelDiscoveryError::from)?;
    wait_for_upstream_health(&http_client, &config).await?;
    let desired = match &config.source {
        ModelSource::Static(model_ids) => model_ids.clone(),
        ModelSource::Discovered(discovery) => {
            let result = discover_model_ids(
                &http_client,
                &config.upstream_http_base_url,
                discovery.provider,
                discovery.request_timeout,
            )
            .await;
            observe_discovery_poll(metrics.as_deref(), discovery, &result);
            result?
        }
    };
    let next_poll_at = match &config.source {
        ModelSource::Static(_) => None,
        ModelSource::Discovered(discovery) => Some(Instant::now() + discovery.poll_interval),
    };
    let mut supervisor = ModelLifecycleSupervisor {
        config,
        runtime_state,
        stats: stats_collector.control(),
        metrics,
        http_client,
        live: HashMap::new(),
        pending: Vec::new(),
        next_generation: 1,
        next_poll_at,
    };
    supervisor.reconcile(desired).await?;
    while let Some(pending) = supervisor.pop_next_pending() {
        let generation = pending.generation;
        match supervisor.drive_initialization(&generation, None).await? {
            InitializationAttemptOutcome::Ready => {
                if let Err(error) = supervisor.admit(&generation).await {
                    supervisor.shutdown().await;
                    return Err(error);
                }
            }
            InitializationAttemptOutcome::Retired => {}
            InitializationAttemptOutcome::Failed(error) => {
                let Some(input_tps) = supervisor.fallback_input_tps(&error) else {
                    supervisor.shutdown().await;
                    return Err(error);
                };
                if let Err(error) = supervisor
                    .admit_at_input_tps(&generation, &error, input_tps)
                    .await
                {
                    supervisor.shutdown().await;
                    return Err(error);
                }
            }
            InitializationAttemptOutcome::Cancelled => {
                unreachable!("startup initialization has no cancellation token")
            }
        }
    }

    let stats_collector_exit = stats_collector.exit();
    let stopped_by_stats_collector = Arc::new(AtomicBool::new(false));
    let task = OwnedTask::spawn("model lifecycle supervisor", {
        let stopped_by_stats_collector = Arc::clone(&stopped_by_stats_collector);
        move |stop| async move {
            let shutdown = SupervisorShutdown {
                stop,
                stats_collector: stats_collector_exit,
            };
            match supervisor.run(&shutdown).await {
                Ok(()) => {}
                // A collector that its handle's shutdown ended stops this
                // supervisor as part of that shutdown. One that panicked, or
                // ended before any shutdown, is a failure, even if a shutdown
                // started before the supervisor got here.
                Err(error @ ModelLifecycleError::StatsCollectorStopped) => {
                    if shutdown.stats_collector.ended_by_shutdown().await {
                        tracing::debug!(
                            error = %error,
                            "model lifecycle supervisor stopped with the stats collector during shutdown"
                        );
                    } else {
                        stopped_by_stats_collector.store(true, Ordering::Release);
                        tracing::error!(error = %error, "model lifecycle supervisor failed");
                    }
                    // Requests that a registration drain still accepts need
                    // the admitted generations, so they stay until this
                    // supervisor's stop, which Pylon sends once the drain
                    // ends. Outside a drain they are retired now.
                    if supervisor.runtime_state.drain_started() {
                        shutdown.stop.cancelled().await;
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "model lifecycle supervisor failed");
                }
            }
            supervisor.shutdown().await;
        }
    });
    Ok(ModelLifecycleHandle {
        task,
        stopped_by_stats_collector,
    })
}

fn observe_discovery_poll(
    metrics: Option<&PylonMetrics>,
    discovery: &ModelDiscoveryConfig,
    result: &Result<BTreeSet<String>, ModelDiscoveryError>,
) {
    if let Some(metrics) = metrics {
        metrics.observe_model_discovery_poll(
            discovery.provider.as_str(),
            if result.is_ok() { "success" } else { "error" },
            result.as_ref().ok().map(BTreeSet::len),
        );
    }
}

impl ModelLifecycleSupervisor {
    async fn run(&mut self, shutdown: &SupervisorShutdown) -> Result<(), ModelLifecycleError> {
        let stop = &shutdown.stop;
        self.start_admitted_canaries(stop);
        loop {
            if stop.is_cancelled() {
                return Ok(());
            }

            if let Some(pending) = self.pop_ready_pending() {
                self.run_pending(pending, shutdown).await?;
                continue;
            }

            let retry_at = self.next_retry_at();
            tokio::select! {
                _ = stop.cancelled() => {
                    return Ok(());
                }
                _ = wait_until(self.next_poll_at) => {
                    let Some(result) = stop.run_until_cancelled(self.poll()).await else {
                        return Ok(());
                    };
                    result?;
                }
                _ = wait_until(retry_at) => {}
            }
        }
    }

    async fn run_pending(
        &mut self,
        pending: PendingInitialization,
        shutdown: &SupervisorShutdown,
    ) -> Result<(), ModelLifecycleError> {
        let generation = pending.generation;
        let stop = &shutdown.stop;
        match self
            .drive_initialization(&generation, Some(shutdown))
            .await?
        {
            InitializationAttemptOutcome::Ready => {
                self.admit(&generation).await?;
                self.start_canary(&generation, stop)?;
            }
            InitializationAttemptOutcome::Failed(error) => {
                if let Some(input_tps) = self.fallback_input_tps(&error) {
                    let generation = self
                        .admit_at_input_tps(&generation, &error, input_tps)
                        .await?;
                    self.start_canary(&generation, stop)?;
                    return Ok(());
                }
                tracing::warn!(
                    model_id = generation.model_id(),
                    error = %error,
                    "model initialization failed; keeping generation pending"
                );
                self.pending.push(PendingInitialization {
                    generation,
                    retry_at: Instant::now() + self.retry_interval(),
                });
            }
            InitializationAttemptOutcome::Retired | InitializationAttemptOutcome::Cancelled => {}
        }
        Ok(())
    }

    async fn poll(&mut self) -> Result<(), ModelLifecycleError> {
        let poll = self.next_discovery_poll();
        let result = poll.await;
        self.apply_discovery_poll(result).await
    }

    fn next_discovery_poll(&self) -> impl Future<Output = DiscoveryPollResult> + Send + 'static {
        let discovery = match &self.config.source {
            ModelSource::Static(_) => None,
            ModelSource::Discovered(discovery) => Some(discovery.clone()),
        };
        let next_poll_at = self.next_poll_at;
        let http_client = self.http_client.clone();
        let upstream_http_base_url = self.config.upstream_http_base_url.clone();
        async move {
            let Some(discovery) = discovery else {
                return std::future::pending::<DiscoveryPollResult>().await;
            };
            wait_until(next_poll_at).await;
            let result = discover_model_ids(
                &http_client,
                &upstream_http_base_url,
                discovery.provider,
                discovery.request_timeout,
            )
            .await;
            (discovery, result)
        }
    }

    async fn apply_discovery_poll(
        &mut self,
        result: DiscoveryPollResult,
    ) -> Result<(), ModelLifecycleError> {
        match self.record_discovery_poll(result) {
            Some(desired) => self.reconcile(desired).await,
            None => Ok(()),
        }
    }

    fn record_discovery_poll(
        &mut self,
        (discovery, result): DiscoveryPollResult,
    ) -> Option<BTreeSet<String>> {
        observe_discovery_poll(self.metrics.as_deref(), &discovery, &result);
        self.next_poll_at = Some(Instant::now() + discovery.poll_interval);
        match result {
            Ok(desired) => Some(desired),
            Err(error) => {
                tracing::warn!(error = %error, "model discovery poll failed; retaining last-known-good set");
                None
            }
        }
    }

    async fn reconcile(&mut self, desired: BTreeSet<String>) -> Result<(), ModelLifecycleError> {
        let removed = self
            .live
            .values()
            .filter(|model| !desired.contains(model.generation.model_id()))
            .map(|model| model.generation.clone())
            .collect::<BTreeSet<_>>();
        for generation in removed {
            self.retire(&generation).await;
        }

        let added = desired
            .iter()
            .filter(|model_id| !self.live.contains_key(*model_id))
            .cloned()
            .collect::<Vec<_>>();
        let initialization = match &self.config.initialization {
            ModelInitialization::Calibration(_) | ModelInitialization::Uncalibrated => {
                ModelStatsInitialization::Empty
            }
            ModelInitialization::ConfiguredInputTps { input_tps } => {
                ModelStatsInitialization::ConfiguredInputTps {
                    input_tps: *input_tps,
                }
            }
        };
        for model_id in added {
            let generation = self.begin_generation(model_id, initialization).await?;
            self.pending.push(PendingInitialization {
                generation,
                retry_at: Instant::now(),
            });
        }
        Ok(())
    }

    async fn begin_generation(
        &mut self,
        model_id: String,
        initialization: ModelStatsInitialization,
    ) -> Result<ModelGeneration, ModelLifecycleError> {
        let sequence = self.next_generation;
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .ok_or(ModelLifecycleError::GenerationCounterExhausted)?;
        let generation = ModelGeneration::new(model_id.clone(), sequence);
        if !self.runtime_state.begin_generation(generation.clone()) {
            return Err(ModelLifecycleError::StaleGeneration);
        }
        if !self
            .stats
            .begin_generation(generation.clone(), initialization)
            .await
            .map_err(|_| ModelLifecycleError::StatsCollectorStopped)?
        {
            let _ = self.runtime_state.retire_generation(&generation);
            return Err(ModelLifecycleError::StaleGeneration);
        }
        self.live.insert(
            model_id,
            LiveModelGeneration {
                generation: generation.clone(),
                canary: None,
            },
        );
        Ok(generation)
    }

    /// Returns the fallback input TPS for a failed calibration. An unhealthy
    /// upstream keeps its usual handling.
    fn fallback_input_tps(&self, error: &ModelLifecycleError) -> Option<f64> {
        match (&self.config.initialization, error) {
            (_, ModelLifecycleError::Bringup(BringupError::UnhealthyUpstream)) => None,
            (ModelInitialization::Calibration(calibration), _) => calibration.fallback_input_tps,
            _ => None,
        }
    }

    /// Replaces a generation whose calibration failed with one that starts at
    /// `input_tps`, as `--initial-input-tps` does, and admits it. A new generation
    /// also drops late observations from the failed ramp.
    async fn admit_at_input_tps(
        &mut self,
        failed: &ModelGeneration,
        error: &ModelLifecycleError,
        input_tps: f64,
    ) -> Result<ModelGeneration, ModelLifecycleError> {
        tracing::warn!(
            model_id = failed.model_id(),
            error = %error,
            fallback_input_tps = input_tps,
            "model calibration failed; starting the model at the fallback input TPS"
        );
        self.retire(failed).await;
        let generation = self
            .begin_generation(
                failed.model_id().to_string(),
                ModelStatsInitialization::ConfiguredInputTps { input_tps },
            )
            .await?;
        self.admit(&generation).await?;
        Ok(generation)
    }

    /// Runs an initialization attempt while it applies discovery polls.
    /// Startup passes no `shutdown`, as nothing cancels its attempts.
    async fn drive_initialization(
        &mut self,
        generation: &ModelGeneration,
        shutdown: Option<&SupervisorShutdown>,
    ) -> Result<InitializationAttemptOutcome, ModelLifecycleError> {
        if !self.is_current(generation) {
            return Ok(InitializationAttemptOutcome::Retired);
        }
        let attempt = initialization_attempt(
            self.http_client.clone(),
            self.config.clone(),
            self.stats.clone(),
            self.runtime_state.clone(),
            generation.clone(),
            self.metrics.clone(),
            shutdown.map(|shutdown| shutdown.stats_collector.clone()),
        );
        tokio::pin!(attempt);
        let mut deferred_desired = Vec::new();
        loop {
            let poll = self.next_discovery_poll();
            tokio::pin!(poll);
            tokio::select! {
                biased;
                _ = wait_for_cancellation(shutdown.map(|shutdown| &shutdown.stop)) => {
                    return Ok(InitializationAttemptOutcome::Cancelled);
                }
                result = &mut poll => {
                    if let Some(desired) = self.record_discovery_poll(result) {
                        if desired.contains(generation.model_id()) {
                            if deferred_desired.last() != Some(&desired) {
                                deferred_desired.push(desired);
                            }
                        } else {
                            self.reconcile(desired).await?;
                            return Ok(InitializationAttemptOutcome::Retired);
                        }
                    }
                }
                result = &mut attempt => {
                    let outcome = match result {
                        Ok(()) => InitializationAttemptOutcome::Ready,
                        // The calibration found the stats collector stopped.
                        // Without it no generation can initialize, so the
                        // supervisor stops instead of retrying.
                        Err(error @ ModelLifecycleError::StatsCollectorStopped) => {
                            return Err(error);
                        }
                        Err(error) => InitializationAttemptOutcome::Failed(error),
                    };
                    for desired in deferred_desired {
                        self.reconcile(desired).await?;
                        if !self.is_current(generation) {
                            return Ok(InitializationAttemptOutcome::Retired);
                        }
                    }
                    return Ok(outcome);
                }
            }
        }
    }

    async fn admit(&mut self, generation: &ModelGeneration) -> Result<(), ModelLifecycleError> {
        self.stats
            .flush_and_snapshot(generation)
            .await
            .map_err(|_| ModelLifecycleError::StatsCollectorStopped)?
            .ok_or(ModelLifecycleError::StaleGeneration)?;
        if !self.runtime_state.publish_generation(generation) {
            return Err(ModelLifecycleError::StaleGeneration);
        }
        Ok(())
    }

    fn start_admitted_canaries(&mut self, stop: &CancellationToken) {
        let generations = self
            .live
            .values()
            .filter(|model| {
                model.canary.is_none()
                    && self
                        .runtime_state
                        .generation_is_published(&model.generation)
            })
            .map(|model| model.generation.clone())
            .collect::<Vec<_>>();
        for generation in generations {
            self.start_canary(&generation, stop)
                .expect("published generation should remain live before supervisor starts");
        }
    }

    fn start_canary(
        &mut self,
        generation: &ModelGeneration,
        stop: &CancellationToken,
    ) -> Result<(), ModelLifecycleError> {
        let canary = self.spawn_canary(generation, stop);
        let model = self
            .live
            .get_mut(generation.model_id())
            .filter(|model| model.generation == *generation)
            .ok_or(ModelLifecycleError::StaleGeneration)?;
        model.canary = canary;
        Ok(())
    }

    fn spawn_canary(
        &self,
        generation: &ModelGeneration,
        stop: &CancellationToken,
    ) -> Option<OwnedTask> {
        self.config.bringup.enabled.then(|| {
            let task_config = BringupTaskConfig {
                upstream_http_base_url: self.config.upstream_http_base_url.clone(),
                generation: generation.clone(),
                config: self.config.bringup.clone(),
                health_paths: self.config.health_paths.clone(),
            };
            let runtime_state = self.runtime_state.clone();
            OwnedTask::spawn_child("model canary", stop, move |model_stop| {
                run_bringup_task(task_config, runtime_state, model_stop)
            })
        })
    }

    async fn retire(&mut self, generation: &ModelGeneration) {
        if !self.is_current(generation) {
            return;
        }
        let model = self
            .live
            .remove(generation.model_id())
            .expect("current generation should still be live");
        self.pending
            .retain(|pending| pending.generation != *generation);
        if let Some(canary) = model.canary {
            canary.shutdown(TASK_SHUTDOWN_TIMEOUT).await;
        }
        let retired_stats = self.runtime_state.retire_generation(generation);
        let _ = self.stats.retire_generation(generation).await;
        if let Some(metrics) = self.metrics.as_deref() {
            metrics.remove_model_gauges(generation.model_id(), retired_stats.as_ref());
        }
    }

    async fn shutdown(&mut self) {
        let generations = self
            .live
            .values()
            .map(|model| model.generation.clone())
            .collect::<Vec<_>>();
        for generation in generations {
            self.retire(&generation).await;
        }
    }

    fn is_current(&self, generation: &ModelGeneration) -> bool {
        self.live
            .get(generation.model_id())
            .is_some_and(|model| model.generation == *generation)
    }

    fn retry_interval(&self) -> Duration {
        match &self.config.source {
            ModelSource::Static(_) => STATIC_RETRY_INTERVAL,
            ModelSource::Discovered(discovery) => discovery.poll_interval,
        }
    }

    fn pop_next_pending(&mut self) -> Option<PendingInitialization> {
        let index = earliest_pending_index(&self.pending)?;
        Some(self.pending.remove(index))
    }

    fn pop_ready_pending(&mut self) -> Option<PendingInitialization> {
        let index = earliest_pending_index(&self.pending)?;
        (self.pending[index].retry_at <= Instant::now()).then(|| self.pending.remove(index))
    }

    fn next_retry_at(&self) -> Option<Instant> {
        self.pending.iter().map(|pending| pending.retry_at).min()
    }
}

struct CalibrationAttemptTimer {
    metrics: Option<Arc<PylonMetrics>>,
    model_id: String,
    started_at: Instant,
}

impl CalibrationAttemptTimer {
    fn start(metrics: Option<Arc<PylonMetrics>>, model_id: &str) -> Self {
        Self {
            metrics,
            model_id: model_id.to_string(),
            started_at: Instant::now(),
        }
    }

    fn finish(&mut self, result: &Result<(), BringupError>) {
        self.record(if result.is_ok() {
            CalibrationOutcome::Completed
        } else {
            CalibrationOutcome::Error
        });
    }

    fn record(&mut self, outcome: CalibrationOutcome) {
        let Some(metrics) = self.metrics.take() else {
            return;
        };
        metrics.observe_model_calibration_duration(
            &self.model_id,
            self.started_at.elapsed(),
            outcome,
        );
    }
}

impl Drop for CalibrationAttemptTimer {
    fn drop(&mut self) {
        self.record(CalibrationOutcome::Cancelled);
    }
}

/// Checks upstream health and calibrates when configured.
///
/// The supervisor passes how its stats collector ends. A calibration whose
/// stats collector request fails then returns
/// `ModelLifecycleError::StatsCollectorStopped`, so the supervisor stops
/// instead of retrying against a collector that is gone. The attempt is
/// recorded as cancelled when the collector's shutdown ended it, and as an
/// error, with a warning, when the collector ended any other way. Startup
/// passes nothing and keeps the calibration's own error.
async fn initialization_attempt(
    http_client: reqwest::Client,
    config: ModelLifecycleConfig,
    stats: StatsCollectorControl,
    runtime_state: PylonRuntimeState,
    generation: ModelGeneration,
    metrics: Option<Arc<PylonMetrics>>,
    stats_collector: Option<StatsCollectorExit>,
) -> Result<(), ModelLifecycleError> {
    if let Some(health_timeout) = health_probe_timeout(&config)
        && !is_upstream_ready(&http_client, &config, health_timeout).await
    {
        return Err(BringupError::UnhealthyUpstream.into());
    }
    if let ModelInitialization::Calibration(calibration) = &config.initialization {
        let mut timer = CalibrationAttemptTimer::start(metrics, generation.model_id());
        let result = run_calibration(
            &http_client,
            &config.upstream_http_base_url,
            &generation,
            calibration,
            &stats,
            &runtime_state,
        )
        .await;
        if let (Err(error @ BringupError::StatsCollectorStopped), Some(stats_collector)) =
            (&result, &stats_collector)
        {
            if stats_collector.ended_by_shutdown().await {
                // Part of that shutdown: dropping the timer records the
                // attempt as cancelled.
                return Err(ModelLifecycleError::StatsCollectorStopped);
            }
            timer.finish(&result);
            tracing::warn!(
                model_id = generation.model_id(),
                error = %error,
                "model calibration failed; the stats collector stopped"
            );
            return Err(ModelLifecycleError::StatsCollectorStopped);
        }
        timer.finish(&result);
        result?;
    }
    Ok(())
}

fn health_probe_timeout(config: &ModelLifecycleConfig) -> Option<Duration> {
    match &config.initialization {
        ModelInitialization::Calibration(calibration) => Some(calibration.health_timeout),
        ModelInitialization::ConfiguredInputTps { .. } | ModelInitialization::Uncalibrated
            if config.bringup.enabled =>
        {
            Some(config.bringup.canary_timeout)
        }
        ModelInitialization::ConfiguredInputTps { .. } | ModelInitialization::Uncalibrated => None,
    }
}

async fn wait_for_upstream_health(
    http_client: &reqwest::Client,
    config: &ModelLifecycleConfig,
) -> Result<(), ModelLifecycleError> {
    // The gate holds startup even where no per-model probe runs, so a model is
    // never admitted on a check the gate has not passed.
    let Some(health_timeout) = health_probe_timeout(config).or_else(|| {
        config
            .readiness_gate
            .as_ref()
            .map(|_| config.bringup.canary_timeout)
    }) else {
        return Ok(());
    };
    let deadline = Instant::now() + config.startup_health_wait;
    let mut reported = false;
    loop {
        if is_upstream_ready(http_client, config, health_timeout).await {
            return Ok(());
        }
        if Instant::now() >= deadline {
            if let Some(gate) = &config.readiness_gate {
                tracing::warn!(
                    readiness_gate = %gate.url(),
                    last_failure = ?gate.last_failure(),
                    wait = ?config.startup_health_wait,
                    "the readiness gate did not pass before the startup wait ended"
                );
            }
            return Err(BringupError::UnhealthyUpstream.into());
        }
        if reported {
            tracing::debug!(
                upstream = %config.upstream_http_base_url,
                "upstream is still not healthy; retrying"
            );
        } else {
            reported = true;
            tracing::warn!(
                upstream = %config.upstream_http_base_url,
                paths = ?config.health_paths.probe_order(),
                readiness_gate = ?config.readiness_gate.as_ref().map(ReadinessGate::url),
                wait = ?config.startup_health_wait,
                "waiting for the upstream to answer a health probe"
            );
        }
        tokio::time::sleep(STARTUP_HEALTH_RETRY_INTERVAL).await;
    }
}

async fn is_upstream_ready(
    http_client: &reqwest::Client,
    config: &ModelLifecycleConfig,
    timeout: Duration,
) -> bool {
    match &config.readiness_gate {
        Some(gate) => {
            check_readiness_gate(
                http_client,
                &config.upstream_http_base_url,
                timeout,
                gate,
                &config.health_paths,
            )
            .await
        }
        None => {
            check_upstream_health(
                http_client,
                &config.upstream_http_base_url,
                timeout,
                &config.health_paths,
            )
            .await
        }
    }
}

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn wait_for_cancellation(stop: Option<&CancellationToken>) {
    match stop {
        Some(stop) => stop.cancelled().await,
        None => std::future::pending().await,
    }
}

fn earliest_pending_index(pending: &[PendingInitialization]) -> Option<usize> {
    pending
        .iter()
        .enumerate()
        .min_by_key(|(_, pending)| pending.retry_at)
        .map(|(index, _)| index)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde_json::{Value, json};
    use stargate_proto::pb::InferenceServerStatus;
    use stargate_protocol::tunnel_contract::HEADER_REQUEST_ID;
    use stargate_runtime::OwnedTask;
    use tokio::sync::{Notify, RwLock, mpsc};
    use tokio_util::sync::CancellationToken;

    use super::{
        LiveModelGeneration, ModelInitialization, ModelLifecycleConfig, ModelLifecycleError,
        ModelLifecycleSupervisor, ModelSource, start_model_lifecycle,
    };
    use crate::generated_request_id::generated_request_generation;
    use crate::runtime_state::ModelGeneration;
    use crate::test_support::{
        RecordedTracingEvent, RecordingTracingSubscriber, TestHttpServer,
        assert_tracing_event_field,
    };
    use crate::upstream_health::{ReadinessGate, UpstreamHealthPaths};
    use crate::{
        BringupConfig, BringupError, CalibrationConfig, ModelDiscoveryConfig,
        ModelDiscoveryProvider, PylonMetrics, PylonRuntimeState, StatsCollectorConfig,
        start_stats_collector,
    };

    #[derive(Clone)]
    enum CompletionBehavior {
        Pending,
        SaturateAfter {
            completed_requests_per_generation: usize,
        },
        FailModel {
            model_id: &'static str,
            failing: Arc<AtomicBool>,
            completed_requests_per_generation: usize,
        },
    }

    #[derive(Clone)]
    struct TestUpstreamState {
        models: Arc<RwLock<Vec<String>>>,
        discovery_polls: Arc<AtomicUsize>,
        discovery_polled: Arc<Notify>,
        discovery_failing: Arc<AtomicBool>,
        discovery_blocked: Arc<AtomicBool>,
        health: HealthHold,
        calibrations: mpsc::UnboundedSender<ModelGeneration>,
        completed_requests: Arc<StdMutex<HashMap<ModelGeneration, usize>>>,
        completion_behavior: CompletionBehavior,
    }

    /// Holds `/health` probes while `held` is set.
    #[derive(Clone, Default)]
    struct HealthHold {
        held: Arc<AtomicBool>,
        held_probes: Arc<AtomicUsize>,
        released: Arc<Notify>,
    }

    struct TestUpstream {
        base_url: String,
        models: Arc<RwLock<Vec<String>>>,
        discovery_polls: Arc<AtomicUsize>,
        discovery_polled: Arc<Notify>,
        discovery_failing: Arc<AtomicBool>,
        discovery_blocked: Arc<AtomicBool>,
        health: HealthHold,
        calibrations: mpsc::UnboundedReceiver<ModelGeneration>,
        server: TestHttpServer,
    }

    impl TestUpstream {
        async fn spawn(initial_models: &[&str], completion_behavior: CompletionBehavior) -> Self {
            let models = Arc::new(RwLock::new(
                initial_models
                    .iter()
                    .map(|model_id| (*model_id).to_string())
                    .collect(),
            ));
            let discovery_polls = Arc::new(AtomicUsize::new(0));
            let discovery_polled = Arc::new(Notify::new());
            let discovery_failing = Arc::new(AtomicBool::new(false));
            let discovery_blocked = Arc::new(AtomicBool::new(false));
            let health = HealthHold::default();
            let (calibrations, calibration_rx) = mpsc::unbounded_channel();
            let state = TestUpstreamState {
                models: models.clone(),
                discovery_polls: discovery_polls.clone(),
                discovery_polled: discovery_polled.clone(),
                discovery_failing: discovery_failing.clone(),
                discovery_blocked: discovery_blocked.clone(),
                health: health.clone(),
                calibrations,
                completed_requests: Arc::new(StdMutex::new(HashMap::new())),
                completion_behavior,
            };
            let server = TestHttpServer::spawn(
                Router::new()
                    .route("/v1/models", get(test_models))
                    .route("/health", get(test_health))
                    .route("/v1/chat/completions", post(test_completion))
                    .with_state(state),
            )
            .await;
            Self {
                base_url: server.to_string(),
                models,
                discovery_polls,
                discovery_polled,
                discovery_failing,
                discovery_blocked,
                health,
                calibrations: calibration_rx,
                server,
            }
        }

        async fn set_models(&self, model_ids: &[&str]) {
            *self.models.write().await = model_ids
                .iter()
                .map(|model_id| (*model_id).to_string())
                .collect();
        }

        fn set_discovery_failing(&self, failing: bool) {
            self.discovery_failing.store(failing, Ordering::SeqCst);
        }

        fn set_discovery_blocked(&self, blocked: bool) {
            self.discovery_blocked.store(blocked, Ordering::SeqCst);
        }

        /// Holds `/health` probes until `release_health`. A model's
        /// initialization attempt probes the upstream before it calibrates.
        fn hold_health(&self) {
            self.health.held.store(true, Ordering::SeqCst);
        }

        fn release_health(&self) {
            self.health.held.store(false, Ordering::SeqCst);
            self.health.released.notify_waiters();
        }

        async fn wait_for_held_health_probe(&self) {
            wait_for("a held health probe should arrive", || {
                self.health.held_probes.load(Ordering::SeqCst) > 0
            })
            .await;
        }

        async fn next_calibration(&mut self) -> ModelGeneration {
            tokio::time::timeout(Duration::from_secs(2), self.calibrations.recv())
                .await
                .expect("calibration request should arrive")
                .expect("calibration request channel should remain open")
        }

        async fn shutdown(self) {
            self.server.shutdown().await;
        }
    }

    #[derive(Clone)]
    struct BlockingKvCacheState {
        blocked: Arc<AtomicBool>,
        blocked_polls: Arc<AtomicUsize>,
        released: Arc<Notify>,
        /// Answers each poll, once released, with a server error.
        rejecting: bool,
    }

    struct BlockingKvCacheServer {
        server: TestHttpServer,
        blocked: Arc<AtomicBool>,
        blocked_polls: Arc<AtomicUsize>,
        released: Arc<Notify>,
    }

    impl BlockingKvCacheServer {
        async fn spawn() -> Self {
            Self::spawn_answering(false).await
        }

        /// Like `spawn`, but each poll ends with a server error, which the
        /// stats collector logs as a warning.
        async fn spawn_rejecting() -> Self {
            Self::spawn_answering(true).await
        }

        async fn spawn_answering(rejecting: bool) -> Self {
            let blocked = Arc::new(AtomicBool::new(false));
            let blocked_polls = Arc::new(AtomicUsize::new(0));
            let released = Arc::new(Notify::new());
            let state = BlockingKvCacheState {
                blocked: blocked.clone(),
                blocked_polls: blocked_polls.clone(),
                released: released.clone(),
                rejecting,
            };
            let server = TestHttpServer::spawn(
                Router::new()
                    .route("/kv-cache", get(test_kv_cache))
                    .with_state(state),
            )
            .await;
            Self {
                server,
                blocked,
                blocked_polls,
                released,
            }
        }

        fn url(&self) -> String {
            format!("{}/kv-cache", self.server.as_str())
        }

        fn block(&self) {
            self.blocked.store(true, Ordering::SeqCst);
        }

        fn unblock(&self) {
            self.blocked.store(false, Ordering::SeqCst);
            self.released.notify_waiters();
        }

        fn blocked_poll_count(&self) -> usize {
            self.blocked_polls.load(Ordering::SeqCst)
        }

        async fn wait_for_blocked_poll_after(&self, count: usize) {
            wait_for("KV-cache stats poll should block", || {
                self.blocked_polls.load(Ordering::SeqCst) > count
            })
            .await;
        }

        async fn shutdown(self) {
            self.server.shutdown().await;
        }
    }

    async fn test_models(State(state): State<TestUpstreamState>) -> Response {
        state.discovery_polls.fetch_add(1, Ordering::SeqCst);
        state.discovery_polled.notify_one();
        if state.discovery_blocked.load(Ordering::SeqCst) {
            return std::future::pending().await;
        }
        if state.discovery_failing.load(Ordering::SeqCst) {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        let data = state
            .models
            .read()
            .await
            .iter()
            .map(|model_id| json!({"id": model_id}))
            .collect::<Vec<_>>();
        Json(json!({"data": data})).into_response()
    }

    async fn test_completion(
        State(state): State<TestUpstreamState>,
        headers: HeaderMap,
        Json(request): Json<Value>,
    ) -> Response {
        let model_id = request["model"].as_str().unwrap_or_default();
        let generation = headers
            .get(HEADER_REQUEST_ID)
            .and_then(|value| value.to_str().ok())
            .and_then(|request_id| generated_request_generation(request_id, model_id))
            .expect("calibration request should identify its exact generation");
        state
            .calibrations
            .send(generation.clone())
            .expect("test should still observe calibration traffic");
        let completed_requests_per_generation = match state.completion_behavior {
            CompletionBehavior::Pending => return std::future::pending().await,
            CompletionBehavior::SaturateAfter {
                completed_requests_per_generation,
            } => completed_requests_per_generation,
            CompletionBehavior::FailModel {
                model_id: failed_model,
                ref failing,
                ..
            } if model_id == failed_model && failing.load(Ordering::SeqCst) => {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            CompletionBehavior::FailModel {
                completed_requests_per_generation,
                ..
            } => completed_requests_per_generation,
        };
        let should_complete = {
            let mut completed = state
                .completed_requests
                .lock()
                .expect("completed request counts should not be poisoned");
            let completed = completed.entry(generation).or_default();
            if *completed < completed_requests_per_generation {
                *completed += 1;
                true
            } else {
                false
            }
        };
        if should_complete {
            Json(json!({"usage": {"completion_tokens": 1}})).into_response()
        } else {
            std::future::pending().await
        }
    }

    async fn test_health(State(state): State<TestUpstreamState>) -> &'static str {
        let hold = state.health;
        if hold.held.load(Ordering::SeqCst) {
            hold.held_probes.fetch_add(1, Ordering::SeqCst);
            loop {
                let released = hold.released.notified();
                if !hold.held.load(Ordering::SeqCst) {
                    break;
                }
                released.await;
            }
        }
        "ok"
    }

    async fn test_kv_cache(State(state): State<BlockingKvCacheState>) -> Response {
        if state.blocked.load(Ordering::SeqCst) {
            state.blocked_polls.fetch_add(1, Ordering::SeqCst);
            loop {
                let released = state.released.notified();
                if !state.blocked.load(Ordering::SeqCst) {
                    break;
                }
                released.await;
            }
        }
        if state.rejecting {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        Json(json!({
            "model": "model-a",
            "kv_cache_capacity_tokens": 1000,
            "kv_cache_used_tokens": 400,
            "kv_cache_free_tokens": 600
        }))
        .into_response()
    }

    fn discovery_config(base_url: &str) -> ModelLifecycleConfig {
        ModelLifecycleConfig {
            upstream_http_base_url: base_url.to_string(),
            source: ModelSource::Discovered(ModelDiscoveryConfig {
                provider: ModelDiscoveryProvider::Dynamo,
                poll_interval: Duration::from_millis(10),
                request_timeout: Duration::from_secs(1),
            }),
            initialization: ModelInitialization::ConfiguredInputTps { input_tps: 123.0 },
            bringup: BringupConfig {
                enabled: false,
                ..BringupConfig::default()
            },
            health_paths: UpstreamHealthPaths::default(),
            readiness_gate: None,
            startup_health_wait: Duration::ZERO,
        }
    }

    fn calibration_discovery_config(
        base_url: &str,
        calibration_timeout: Duration,
    ) -> ModelLifecycleConfig {
        ModelLifecycleConfig {
            initialization: ModelInitialization::Calibration(CalibrationConfig {
                health_timeout: Duration::from_secs(1),
                calibration_requests: 1,
                calibration_prompt_units: 256,
                calibration_max_concurrency: 1,
                calibration_timeout,
                fallback_input_tps: None,
            }),
            ..discovery_config(base_url)
        }
    }

    async fn wait_for(description: &str, mut reached: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut poll = tokio::time::interval(Duration::from_millis(1));
            loop {
                poll.tick().await;
                if reached() {
                    return;
                }
            }
        })
        .await
        .expect(description);
    }

    async fn wait_for_model_ids(runtime_state: &PylonRuntimeState, expected: &[&str]) {
        let expected = expected
            .iter()
            .map(|model_id| (*model_id).to_string())
            .collect::<Vec<_>>();
        wait_for("advertised model set should converge", || {
            runtime_state.advertised_model_ids() == expected
        })
        .await;
    }

    async fn wait_for_generation_retirement(runtime_state: &PylonRuntimeState, model_id: &str) {
        wait_for("model generation should retire", || {
            runtime_state.current_generation(model_id).is_none()
        })
        .await;
    }

    async fn wait_for_discovery_polls(counter: &AtomicUsize, expected: usize) {
        wait_for("model discovery should keep polling", || {
            counter.load(Ordering::SeqCst) >= expected
        })
        .await;
    }

    async fn wait_for_calibration_count(
        metrics: &PylonMetrics,
        model_id: &str,
        outcome: &str,
        minimum: u64,
    ) {
        let prefix = format!(
            "pylon_model_calibration_duration_ms_count{{model=\"{model_id}\",outcome=\"{outcome}\"}} "
        );
        wait_for("calibration metric should reach expected count", || {
            let body = metrics.gather_text().expect("metrics should encode");
            let count = body.lines().find_map(|line| {
                line.strip_prefix(&prefix)
                    .and_then(|value| value.parse::<u64>().ok())
            });
            count.is_some_and(|count| count >= minimum)
        })
        .await;
    }

    fn calibration_duration_sum_ms(metrics: &PylonMetrics, model_id: &str, outcome: &str) -> f64 {
        let prefix = format!(
            "pylon_model_calibration_duration_ms_sum{{model=\"{model_id}\",outcome=\"{outcome}\"}} "
        );
        metrics
            .gather_text()
            .expect("metrics should encode")
            .lines()
            .find_map(|line| {
                line.strip_prefix(&prefix)
                    .and_then(|value| value.parse::<f64>().ok())
            })
            .expect("calibration duration sum should exist")
    }

    #[tokio::test]
    async fn configured_models_publish_only_after_exact_stats_initialization() {
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());

        let lifecycle = start_model_lifecycle(
            ModelLifecycleConfig {
                upstream_http_base_url: "http://127.0.0.1:1".to_string(),
                source: ModelSource::Static(BTreeSet::from([
                    "model-a".to_string(),
                    "model-b".to_string(),
                ])),
                initialization: ModelInitialization::ConfiguredInputTps { input_tps: 123.0 },
                bringup: BringupConfig {
                    enabled: false,
                    ..BringupConfig::default()
                },
                health_paths: UpstreamHealthPaths::default(),
                readiness_gate: None,
                startup_health_wait: Duration::ZERO,
            },
            runtime_state.clone(),
            &stats,
            None,
        )
        .await
        .expect("static model lifecycle should initialize");

        let advertised = runtime_state.advertised_models();
        assert_eq!(
            advertised.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from(["model-a".to_string(), "model-b".to_string()])
        );
        assert!(advertised.values().all(|model| {
            model
                .stats
                .as_ref()
                .is_some_and(|stats| stats.last_mean_input_tps == 123.0)
        }));

        lifecycle.shutdown().await;
        stats.shutdown().await;
    }

    #[tokio::test]
    async fn startup_waits_for_an_upstream_that_is_not_healthy_yet() {
        let unhealthy_probes = Arc::new(AtomicUsize::new(2));
        let server_unhealthy_probes = unhealthy_probes.clone();
        let upstream = TestHttpServer::spawn(Router::new().route(
            "/health",
            get(move || {
                let remaining = server_unhealthy_probes.clone();
                async move {
                    if remaining.load(Ordering::SeqCst) > 0 {
                        remaining.fetch_sub(1, Ordering::SeqCst);
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    } else {
                        axum::http::StatusCode::OK
                    }
                }
            }),
        ))
        .await;
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());

        let lifecycle = start_model_lifecycle(
            ModelLifecycleConfig {
                upstream_http_base_url: upstream.to_string(),
                source: ModelSource::Static(BTreeSet::from(["model-a".to_string()])),
                initialization: ModelInitialization::ConfiguredInputTps { input_tps: 123.0 },
                bringup: BringupConfig {
                    active_canary_interval: Duration::ZERO,
                    ..BringupConfig::default()
                },
                health_paths: UpstreamHealthPaths::default(),
                readiness_gate: None,
                startup_health_wait: Duration::from_secs(10),
            },
            runtime_state.clone(),
            &stats,
            None,
        )
        .await
        .expect("startup should wait for the upstream to become healthy");

        assert_eq!(unhealthy_probes.load(Ordering::SeqCst), 0);
        assert_eq!(runtime_state.advertised_model_ids(), ["model-a"]);

        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn startup_fails_fast_when_no_health_wait_is_configured() {
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());

        let result = start_model_lifecycle(
            ModelLifecycleConfig {
                upstream_http_base_url: "http://127.0.0.1:1".to_string(),
                source: ModelSource::Static(BTreeSet::from(["model-a".to_string()])),
                initialization: ModelInitialization::ConfiguredInputTps { input_tps: 123.0 },
                bringup: BringupConfig {
                    active_canary_interval: Duration::ZERO,
                    ..BringupConfig::default()
                },
                health_paths: UpstreamHealthPaths::default(),
                readiness_gate: None,
                startup_health_wait: Duration::ZERO,
            },
            runtime_state.clone(),
            &stats,
            None,
        )
        .await;

        let Err(error) = result else {
            panic!("startup should not wait for an unreachable upstream");
        };
        assert!(matches!(
            error,
            ModelLifecycleError::Bringup(BringupError::UnhealthyUpstream)
        ));

        stats.shutdown().await;
    }

    #[tokio::test]
    async fn retire_cancels_canary_before_removing_runtime_generation() {
        let kv_cache = BlockingKvCacheServer::spawn().await;
        let stats_config = StatsCollectorConfig {
            kv_cache_stats_url: Some(kv_cache.url()),
            // Poll quickly and keep the request open so retirement parks on
            // stats cleanup after the canary ordering point under test.
            kv_cache_poll_interval: Duration::from_millis(1),
            kv_cache_request_timeout: Duration::from_secs(60),
            ..StatsCollectorConfig::default()
        };
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let generation = ModelGeneration::new("model-a", 1);
        assert!(runtime_state.begin_generation(generation.clone()));
        assert!(runtime_state.publish_generation(&generation));

        let parent_stop = CancellationToken::new();
        let canary_cancelled = Arc::new(AtomicBool::new(false));
        let canary_runtime = runtime_state.clone();
        let canary_model_id = generation.model_id().to_string();
        let canary_cancelled_task = canary_cancelled.clone();
        let canary =
            OwnedTask::spawn_child("test model canary", &parent_stop, move |stop| async move {
                loop {
                    if stop.is_cancelled() {
                        canary_cancelled_task.store(true, Ordering::SeqCst);
                        return;
                    }
                    if canary_runtime
                        .current_generation(&canary_model_id)
                        .is_none()
                    {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            });
        let mut supervisor = ModelLifecycleSupervisor {
            config: ModelLifecycleConfig {
                upstream_http_base_url: "http://127.0.0.1:1".to_string(),
                source: ModelSource::Static(BTreeSet::new()),
                initialization: ModelInitialization::ConfiguredInputTps { input_tps: 123.0 },
                bringup: BringupConfig {
                    enabled: false,
                    ..BringupConfig::default()
                },
                health_paths: UpstreamHealthPaths::default(),
                readiness_gate: None,
                startup_health_wait: Duration::ZERO,
            },
            runtime_state: runtime_state.clone(),
            stats: stats.control(),
            metrics: None,
            http_client: reqwest::Client::new(),
            live: HashMap::from([(
                generation.model_id().to_string(),
                LiveModelGeneration {
                    generation: generation.clone(),
                    canary: Some(canary),
                },
            )]),
            pending: Vec::new(),
            next_generation: 2,
            next_poll_at: None,
        };

        let blocked_polls = kv_cache.blocked_poll_count();
        kv_cache.block();
        kv_cache.wait_for_blocked_poll_after(blocked_polls).await;
        let retire_generation = generation.clone();
        let retire = tokio::spawn(async move {
            supervisor.retire(&retire_generation).await;
            supervisor
        });
        wait_for(
            "retirement should stop the canary before stats cleanup",
            || canary_cancelled.load(Ordering::SeqCst) || parent_stop.is_cancelled(),
        )
        .await;

        assert!(
            canary_cancelled.load(Ordering::SeqCst),
            "canary should observe cancellation before the runtime generation disappears"
        );
        assert!(
            !parent_stop.is_cancelled(),
            "normal retirement must not look like an unexpected canary exit"
        );

        kv_cache.unblock();
        let _supervisor = retire
            .await
            .expect("retirement task should not panic after stats resumes");
        stats.shutdown().await;
        kv_cache.shutdown().await;
    }

    #[tokio::test]
    async fn calibration_traffic_seeds_positive_observed_stats_before_publication() {
        let mut upstream = TestUpstream::spawn(
            &[],
            CompletionBehavior::SaturateAfter {
                completed_requests_per_generation: 7,
            },
        )
        .await;
        let stats_config = StatsCollectorConfig {
            duration_floor: Duration::ZERO,
            openai_fallback_stats_enabled: false,
            ..StatsCollectorConfig::default()
        };
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());

        let lifecycle_handle = {
            let lifecycle = start_model_lifecycle(
                ModelLifecycleConfig {
                    upstream_http_base_url: upstream.base_url.clone(),
                    source: ModelSource::Static(BTreeSet::from(["model-a".to_string()])),
                    initialization: ModelInitialization::Calibration(CalibrationConfig {
                        health_timeout: Duration::from_secs(1),
                        calibration_requests: 1,
                        calibration_prompt_units: 1024,
                        calibration_max_concurrency: 1,
                        calibration_timeout: Duration::from_secs(30),
                        fallback_input_tps: None,
                    }),
                    bringup: BringupConfig {
                        enabled: false,
                        ..BringupConfig::default()
                    },
                    health_paths: UpstreamHealthPaths::default(),
                    readiness_gate: None,
                    startup_health_wait: Duration::ZERO,
                },
                runtime_state.clone(),
                &stats,
                None,
            );
            tokio::pin!(lifecycle);
            for _ in 0..8 {
                tokio::select! {
                    _ = upstream.next_calibration() => {}
                    _ = &mut lifecycle => {
                        panic!("calibration completed before reaching saturation")
                    }
                }
            }
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(30)).await;
            lifecycle
                .as_mut()
                .await
                .expect("calibration timeout should complete startup")
        };
        tokio::time::resume();

        let published = runtime_state
            .advertised_models()
            .remove("model-a")
            .expect("calibrated model should be advertised");
        assert!(
            published
                .stats
                .is_some_and(|stats| stats.last_mean_input_tps > 0.0),
            "completed calibration traffic must seed the ordinary observed stats window"
        );

        lifecycle_handle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn initial_calibration_error_is_fatal_and_unadvertised() {
        let failing = Arc::new(AtomicBool::new(true));
        let upstream = TestUpstream::spawn(
            &[],
            CompletionBehavior::FailModel {
                model_id: "model-a",
                failing,
                completed_requests_per_generation: 7,
            },
        )
        .await;
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());

        let result = start_model_lifecycle(
            ModelLifecycleConfig {
                source: ModelSource::Static(BTreeSet::from(["model-a".to_string()])),
                ..calibration_discovery_config(&upstream.base_url, Duration::from_secs(1))
            },
            runtime_state.clone(),
            &stats,
            None,
        )
        .await;

        assert!(matches!(
            result,
            Err(super::ModelLifecycleError::Bringup(_))
        ));
        assert!(runtime_state.advertised_model_ids().is_empty());
        assert!(runtime_state.current_generation("model-a").is_none());

        stats.shutdown().await;
        upstream.shutdown().await;
    }

    const FALLBACK_INPUT_TPS: f64 = 100.0;

    fn fallback_calibration_config(
        base_url: &str,
        model_ids: &[&str],
        calibration_timeout: Duration,
    ) -> ModelLifecycleConfig {
        ModelLifecycleConfig {
            source: ModelSource::Static(
                model_ids
                    .iter()
                    .map(|model_id| (*model_id).to_string())
                    .collect(),
            ),
            initialization: ModelInitialization::Calibration(CalibrationConfig {
                health_timeout: Duration::from_secs(1),
                calibration_requests: 1,
                calibration_prompt_units: 256,
                calibration_max_concurrency: 1,
                calibration_timeout,
                fallback_input_tps: Some(FALLBACK_INPUT_TPS),
            }),
            ..discovery_config(base_url)
        }
    }

    fn start_test_stats() -> (PylonRuntimeState, crate::StatsCollectorHandle) {
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        (runtime_state, stats)
    }

    fn published_input_tps(runtime_state: &PylonRuntimeState) -> BTreeMap<String, f64> {
        runtime_state
            .advertised_models()
            .into_iter()
            .map(|(model_id, model)| {
                let stats = model.stats.expect("advertised model should carry stats");
                (model_id, stats.last_mean_input_tps)
            })
            .collect()
    }

    const FALLBACK_WARNING: &str =
        "model calibration failed; starting the model at the fallback input TPS";

    /// Asserts each seeded model publishes the fallback rate with one warning and
    /// every other model publishes a measured rate.
    fn assert_seeded_models(
        label: &str,
        runtime_state: &PylonRuntimeState,
        events: &[RecordedTracingEvent],
        seeded: &[&str],
    ) {
        for (model_id, input_tps) in published_input_tps(runtime_state) {
            if seeded.contains(&model_id.as_str()) {
                assert_eq!(input_tps, FALLBACK_INPUT_TPS, "{label}: {model_id}");
            } else {
                assert!(
                    input_tps > 0.0 && input_tps != FALLBACK_INPUT_TPS,
                    "{label}: {model_id} should publish a measured rate, got {input_tps}"
                );
            }
        }
        let warnings = events
            .iter()
            .filter(|event| {
                event.fields.get("message").map(String::as_str) == Some(FALLBACK_WARNING)
            })
            .collect::<Vec<_>>();
        let mut warned_models = Vec::new();
        for warning in warnings {
            assert_eq!(warning.level, tracing::Level::WARN, "{label}");
            assert_tracing_event_field(warning, "fallback_input_tps", "100.0");
            assert!(warning.fields.contains_key("error"), "{label}: {warning:?}");
            warned_models.push(warning.fields["model_id"].clone());
        }
        assert_eq!(warned_models, seeded, "{label}");
    }

    #[tokio::test]
    async fn fallback_calibration_measures_each_model_and_seeds_a_failed_one() {
        for (label, completion_behavior, model_ids, seeded) in [
            (
                "calibration request error",
                CompletionBehavior::FailModel {
                    model_id: "model-b",
                    failing: Arc::new(AtomicBool::new(true)),
                    completed_requests_per_generation: 4,
                },
                &["model-a", "model-b", "model-c"][..],
                &["model-b"][..],
            ),
            (
                "no completed request",
                CompletionBehavior::Pending,
                &["model-a", "model-b"][..],
                &["model-a", "model-b"][..],
            ),
        ] {
            let mut upstream = TestUpstream::spawn(&[], completion_behavior).await;
            let (runtime_state, stats) = start_test_stats();
            let subscriber = RecordingTracingSubscriber::default();
            let dispatch = tracing::Dispatch::new(subscriber.clone());
            let default_guard = tracing::dispatcher::set_default(&dispatch);

            let lifecycle = start_model_lifecycle(
                fallback_calibration_config(
                    &upstream.base_url,
                    model_ids,
                    Duration::from_millis(100),
                ),
                runtime_state.clone(),
                &stats,
                None,
            )
            .await
            .unwrap_or_else(|error| panic!("{label}: startup should fall back: {error}"));
            drop(default_guard);

            let mut calibrated_models = BTreeSet::new();
            while let Ok(generation) = upstream.calibrations.try_recv() {
                calibrated_models.insert(generation.model_id().to_string());
            }
            assert_eq!(
                calibrated_models,
                model_ids
                    .iter()
                    .map(|model_id| (*model_id).to_string())
                    .collect(),
                "{label}: every model should calibrate"
            );
            assert_eq!(runtime_state.advertised_model_ids(), model_ids, "{label}");
            assert_seeded_models(label, &runtime_state, &subscriber.events(), seeded);

            lifecycle.shutdown().await;
            stats.shutdown().await;
            upstream.shutdown().await;
        }
    }

    /// Declared health check on its own port, like a worker-readiness sidecar.
    /// It answers 503 `not_ready_probes` times, then clears `upstream_failing`
    /// and answers 200.
    async fn spawn_readiness_sidecar(
        not_ready_probes: usize,
        upstream_failing: Arc<AtomicBool>,
    ) -> (TestHttpServer, ReadinessGateProbes) {
        let remaining = Arc::new(AtomicUsize::new(not_ready_probes));
        let total = Arc::new(AtomicUsize::new(0));
        let server_remaining = remaining.clone();
        let server_total = total.clone();
        let server = TestHttpServer::spawn(Router::new().route(
            "/ready",
            get(move || {
                let remaining = server_remaining.clone();
                let upstream_failing = upstream_failing.clone();
                server_total.fetch_add(1, Ordering::SeqCst);
                async move {
                    if remaining.load(Ordering::SeqCst) > 0 {
                        remaining.fetch_sub(1, Ordering::SeqCst);
                        return StatusCode::SERVICE_UNAVAILABLE;
                    }
                    upstream_failing.store(false, Ordering::SeqCst);
                    StatusCode::OK
                }
            }),
        ))
        .await;
        (server, ReadinessGateProbes { remaining, total })
    }

    struct ReadinessGateProbes {
        remaining: Arc<AtomicUsize>,
        total: Arc<AtomicUsize>,
    }

    impl ReadinessGateProbes {
        fn not_ready_left(&self) -> usize {
            self.remaining.load(Ordering::SeqCst)
        }

        fn total(&self) -> usize {
            self.total.load(Ordering::SeqCst)
        }
    }

    fn sidecar_gate(upstream_base_url: &str, sidecar: &TestHttpServer) -> ReadinessGate {
        let port = url::Url::parse(sidecar.as_str())
            .expect("sidecar URL should parse")
            .port()
            .expect("sidecar URL should carry a port");
        ReadinessGate::new(upstream_base_url, "/ready", Some(port), 200)
            .expect("readiness gate should build")
    }

    #[tokio::test]
    async fn readiness_gate_holds_startup_while_the_upstream_health_passes() {
        // Bringup off and no calibration: without the gate, startup does not
        // probe the upstream at all.
        let upstream = TestUpstream::spawn(&[], CompletionBehavior::Pending).await;
        let (sidecar, probes) = spawn_readiness_sidecar(3, Arc::new(AtomicBool::new(false))).await;
        let (runtime_state, stats) = start_test_stats();

        let lifecycle = start_model_lifecycle(
            ModelLifecycleConfig {
                source: ModelSource::Static(BTreeSet::from(["model-a".to_string()])),
                readiness_gate: Some(sidecar_gate(&upstream.base_url, &sidecar)),
                startup_health_wait: Duration::from_secs(10),
                ..discovery_config(&upstream.base_url)
            },
            runtime_state.clone(),
            &stats,
            None,
        )
        .await
        .expect("startup should wait for the declared health check");

        assert_eq!(probes.not_ready_left(), 0);
        // One passing probe ends the startup wait; with bringup off no model
        // probes again.
        assert_eq!(probes.total(), 3 + 1);
        assert_eq!(runtime_state.advertised_model_ids(), ["model-a"]);

        lifecycle.shutdown().await;
        stats.shutdown().await;
        sidecar.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn readiness_gate_holds_calibration_until_the_declared_check_passes() {
        // The chat endpoint fails until the declared check passes, like a
        // frontend whose workers are still loading.
        let failing = Arc::new(AtomicBool::new(true));
        let upstream = TestUpstream::spawn(
            &[],
            CompletionBehavior::FailModel {
                model_id: "model-a",
                failing: failing.clone(),
                completed_requests_per_generation: 4,
            },
        )
        .await;
        let (sidecar, probes) = spawn_readiness_sidecar(3, failing).await;
        let (runtime_state, stats) = start_test_stats();
        let subscriber = RecordingTracingSubscriber::default();
        let dispatch = tracing::Dispatch::new(subscriber.clone());
        let default_guard = tracing::dispatcher::set_default(&dispatch);

        let lifecycle = start_model_lifecycle(
            ModelLifecycleConfig {
                readiness_gate: Some(sidecar_gate(&upstream.base_url, &sidecar)),
                startup_health_wait: Duration::from_secs(10),
                ..fallback_calibration_config(
                    &upstream.base_url,
                    &["model-a"],
                    Duration::from_millis(100),
                )
            },
            runtime_state.clone(),
            &stats,
            None,
        )
        .await
        .expect("startup should calibrate after the declared health check passes");
        drop(default_guard);

        assert_eq!(probes.not_ready_left(), 0);
        assert_seeded_models("readiness gate", &runtime_state, &subscriber.events(), &[]);

        lifecycle.shutdown().await;
        stats.shutdown().await;
        sidecar.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn fallback_calibration_seeds_a_discovered_model_whose_calibration_fails() {
        let upstream = TestUpstream::spawn(
            &["model-a"],
            CompletionBehavior::FailModel {
                model_id: "model-b",
                failing: Arc::new(AtomicBool::new(true)),
                completed_requests_per_generation: 4,
            },
        )
        .await;
        let (runtime_state, stats) = start_test_stats();
        let subscriber = RecordingTracingSubscriber::default();
        let dispatch = tracing::Dispatch::new(subscriber.clone());
        let default_guard = tracing::dispatcher::set_default(&dispatch);

        let lifecycle = start_model_lifecycle(
            ModelLifecycleConfig {
                source: discovery_config(&upstream.base_url).source,
                ..fallback_calibration_config(&upstream.base_url, &[], Duration::from_millis(100))
            },
            runtime_state.clone(),
            &stats,
            None,
        )
        .await
        .expect("calibrated startup should succeed");

        upstream.set_models(&["model-a", "model-b"]).await;
        wait_for_model_ids(&runtime_state, &["model-a", "model-b"]).await;
        drop(default_guard);
        assert_seeded_models(
            "discovered model",
            &runtime_state,
            &subscriber.events(),
            &["model-b"],
        );

        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn fallback_calibration_keeps_an_unhealthy_upstream_fatal() {
        let health_probes = Arc::new(AtomicUsize::new(0));
        let server_health_probes = health_probes.clone();
        // The startup health wait passes; the probe before calibration fails.
        let server = TestHttpServer::spawn(Router::new().route(
            "/health",
            get(move || {
                let probes = server_health_probes.clone();
                async move {
                    if probes.fetch_add(1, Ordering::SeqCst) == 0 {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        ))
        .await;
        let (runtime_state, stats) = start_test_stats();

        let result = start_model_lifecycle(
            fallback_calibration_config(
                server.as_str(),
                &["model-a", "model-b"],
                Duration::from_secs(30),
            ),
            runtime_state.clone(),
            &stats,
            None,
        )
        .await;

        assert!(matches!(
            result,
            Err(ModelLifecycleError::Bringup(
                BringupError::UnhealthyUpstream
            ))
        ));
        assert!(health_probes.load(Ordering::SeqCst) >= 2);
        assert!(runtime_state.advertised_model_ids().is_empty());
        assert!(runtime_state.current_generation("model-a").is_none());
        assert!(runtime_state.current_generation("model-b").is_none());

        stats.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test]
    async fn static_models_never_poll_the_discovery_endpoint() {
        let upstream = TestUpstream::spawn(&["ignored-model"], CompletionBehavior::Pending).await;
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let lifecycle = start_model_lifecycle(
            ModelLifecycleConfig {
                upstream_http_base_url: upstream.base_url.clone(),
                source: ModelSource::Static(BTreeSet::from(["model-a".to_string()])),
                initialization: ModelInitialization::ConfiguredInputTps { input_tps: 123.0 },
                bringup: BringupConfig {
                    enabled: false,
                    ..BringupConfig::default()
                },
                health_paths: UpstreamHealthPaths::default(),
                readiness_gate: None,
                startup_health_wait: Duration::ZERO,
            },
            runtime_state.clone(),
            &stats,
            None,
        )
        .await
        .expect("static model lifecycle should initialize");

        tokio::task::yield_now().await;
        assert_eq!(upstream.discovery_polls.load(Ordering::SeqCst), 0);
        assert_eq!(runtime_state.advertised_model_ids(), ["model-a"]);

        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn discovery_waits_the_configured_interval_after_success_and_error() {
        let upstream = TestUpstream::spawn(&["model-a"], CompletionBehavior::Pending).await;
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let mut config = discovery_config(&upstream.base_url);
        let ModelSource::Discovered(discovery) = &mut config.source else {
            unreachable!("test config must use discovery")
        };
        discovery.poll_interval = Duration::from_millis(500);
        let lifecycle = start_model_lifecycle(config, runtime_state, &stats, None)
            .await
            .expect("initial discovery should initialize");

        upstream.discovery_polled.notified().await;
        tokio::time::pause();
        assert_eq!(upstream.discovery_polls.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_millis(499)).await;
        tokio::task::yield_now().await;
        assert_eq!(upstream.discovery_polls.load(Ordering::SeqCst), 1);
        let second_poll = upstream.discovery_polled.notified();
        tokio::time::advance(Duration::from_millis(1)).await;
        second_poll.await;
        assert_eq!(upstream.discovery_polls.load(Ordering::SeqCst), 2);

        upstream.set_discovery_failing(true);
        tokio::time::advance(Duration::from_millis(499)).await;
        tokio::task::yield_now().await;
        assert_eq!(upstream.discovery_polls.load(Ordering::SeqCst), 2);
        let third_poll = upstream.discovery_polled.notified();
        tokio::time::advance(Duration::from_millis(1)).await;
        third_poll.await;
        assert_eq!(upstream.discovery_polls.load(Ordering::SeqCst), 3);

        tokio::time::resume();
        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn valid_empty_initial_discovery_keeps_polling_and_admits_a_later_model() {
        let upstream = TestUpstream::spawn(&[], CompletionBehavior::Pending).await;
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let lifecycle = start_model_lifecycle(
            discovery_config(&upstream.base_url),
            runtime_state.clone(),
            &stats,
            None,
        )
        .await
        .expect("empty initial discovery should initialize");

        assert!(runtime_state.advertised_model_ids().is_empty());
        upstream.set_models(&["model-a"]).await;
        wait_for_model_ids(&runtime_state, &["model-a"]).await;

        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn discovered_remove_and_readd_allocates_a_fresh_generation() {
        let upstream = TestUpstream::spawn(&["model-a"], CompletionBehavior::Pending).await;
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let lifecycle = start_model_lifecycle(
            discovery_config(&upstream.base_url),
            runtime_state.clone(),
            &stats,
            None,
        )
        .await
        .expect("initial discovery should initialize");
        let first = runtime_state
            .current_generation("model-a")
            .expect("first generation should exist");

        upstream.set_models(&[]).await;
        wait_for_model_ids(&runtime_state, &[]).await;
        upstream.set_models(&["model-a"]).await;
        wait_for_model_ids(&runtime_state, &["model-a"]).await;
        let replacement = runtime_state
            .current_generation("model-a")
            .expect("replacement generation should exist");

        assert_ne!(first, replacement);
        assert_eq!(
            runtime_state
                .model_stats("model-a")
                .expect("replacement stats should exist")
                .last_mean_input_tps,
            123.0
        );

        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn discovered_remove_and_readd_completes_two_calibration_plans() {
        let upstream = TestUpstream::spawn(
            &["model-a"],
            CompletionBehavior::SaturateAfter {
                completed_requests_per_generation: 5,
            },
        )
        .await;
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let stats_config = StatsCollectorConfig {
            duration_floor: Duration::ZERO,
            ..StatsCollectorConfig::default()
        };
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let lifecycle = start_model_lifecycle(
            calibration_discovery_config(&upstream.base_url, Duration::from_millis(20)),
            runtime_state.clone(),
            &stats,
            Some(metrics.clone()),
        )
        .await
        .expect("first calibration should complete");
        let first = runtime_state
            .current_generation("model-a")
            .expect("first generation should exist");

        upstream.set_models(&[]).await;
        wait_for_model_ids(&runtime_state, &[]).await;
        upstream.set_models(&["model-a"]).await;
        wait_for_model_ids(&runtime_state, &["model-a"]).await;
        let replacement = runtime_state
            .current_generation("model-a")
            .expect("replacement generation should exist");

        assert_ne!(first, replacement);
        wait_for_calibration_count(&metrics, "model-a", "completed", 2).await;

        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn discovery_errors_retain_last_known_good_until_a_valid_empty_set() {
        let upstream = TestUpstream::spawn(&["model-a"], CompletionBehavior::Pending).await;
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let lifecycle = start_model_lifecycle(
            discovery_config(&upstream.base_url),
            runtime_state.clone(),
            &stats,
            None,
        )
        .await
        .expect("initial discovery should initialize");

        upstream.set_models(&[]).await;
        upstream.set_discovery_failing(true);
        let polls = upstream.discovery_polls.load(Ordering::SeqCst);
        wait_for_discovery_polls(&upstream.discovery_polls, polls + 2).await;
        assert_eq!(runtime_state.advertised_model_ids(), ["model-a"]);

        upstream.set_discovery_failing(false);
        wait_for_model_ids(&runtime_state, &[]).await;

        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_interrupts_a_blocked_discovery_poll() {
        let upstream = TestUpstream::spawn(&["model-a"], CompletionBehavior::Pending).await;
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let lifecycle = start_model_lifecycle(
            discovery_config(&upstream.base_url),
            runtime_state,
            &stats,
            None,
        )
        .await
        .expect("initial discovery should initialize");
        upstream.set_discovery_blocked(true);
        let polls = upstream.discovery_polls.load(Ordering::SeqCst);
        wait_for_discovery_polls(&upstream.discovery_polls, polls + 1).await;

        tokio::time::timeout(Duration::from_secs(1), lifecycle.shutdown())
            .await
            .expect("shutdown should cancel the blocked discovery request");

        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn removal_cancels_blocked_calibration_without_publication() {
        let mut upstream = TestUpstream::spawn(&[], CompletionBehavior::Pending).await;
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let lifecycle = start_model_lifecycle(
            calibration_discovery_config(&upstream.base_url, Duration::from_secs(60)),
            runtime_state.clone(),
            &stats,
            Some(metrics.clone()),
        )
        .await
        .expect("empty initial discovery should start");

        upstream.set_models(&["model-a"]).await;
        let retired = upstream.next_calibration().await;
        assert!(runtime_state.advertised_model_ids().is_empty());
        upstream.set_models(&[]).await;
        wait_for_generation_retirement(&runtime_state, "model-a").await;
        let polls = upstream.discovery_polls.load(Ordering::SeqCst);
        wait_for_discovery_polls(&upstream.discovery_polls, polls + 2).await;

        assert!(runtime_state.advertised_model_ids().is_empty());
        assert_eq!(
            stats
                .control()
                .flush_and_snapshot(&retired)
                .await
                .expect("stats collector should respond"),
            None
        );
        wait_for_calibration_count(&metrics, "model-a", "cancelled", 1).await;
        let metric_body = metrics.gather_text().expect("metrics should encode");
        assert!(metric_body.contains(
            r#"pylon_model_calibration_duration_ms_count{model="model-a",outcome="cancelled"} 1"#
        ));
        assert!(!metric_body.contains(
            r#"pylon_model_calibration_duration_ms_count{model="model-a",outcome="completed"}"#
        ));

        upstream.set_models(&["model-a"]).await;
        let replacement = upstream.next_calibration().await;
        assert_ne!(retired, replacement);
        upstream.set_models(&[]).await;
        wait_for_generation_retirement(&runtime_state, "model-a").await;

        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn dynamic_calibration_errors_retry_without_withdrawing_siblings() {
        let failing = Arc::new(AtomicBool::new(false));
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let mut upstream = TestUpstream::spawn(
            &[],
            CompletionBehavior::FailModel {
                model_id: "model-b",
                failing: failing.clone(),
                completed_requests_per_generation: 7,
            },
        )
        .await;
        let stats_config = StatsCollectorConfig {
            duration_floor: Duration::ZERO,
            ..StatsCollectorConfig::default()
        };
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let mut config =
            calibration_discovery_config(&upstream.base_url, Duration::from_millis(100));
        let ModelSource::Discovered(discovery) = &mut config.source else {
            unreachable!("test config must use discovery")
        };
        discovery.poll_interval = Duration::from_millis(100);
        let lifecycle =
            start_model_lifecycle(config, runtime_state.clone(), &stats, Some(metrics.clone()))
                .await
                .expect("empty initial discovery should start");

        upstream.set_models(&["model-a"]).await;
        for _ in 0..8 {
            assert_eq!(upstream.next_calibration().await.model_id(), "model-a");
        }
        wait_for_model_ids(&runtime_state, &["model-a"]).await;

        failing.store(true, Ordering::SeqCst);
        upstream.set_models(&["model-a", "model-b"]).await;
        let first_attempt = upstream.next_calibration().await;
        assert_eq!(first_attempt.model_id(), "model-b");
        let mut error_observed = false;
        for _ in 0..100 {
            let body = metrics.gather_text().expect("metrics should encode");
            if body.contains(
                r#"pylon_model_calibration_duration_ms_count{model="model-b",outcome="error"} 1"#,
            ) {
                error_observed = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            error_observed,
            "the first failed attempt should be recorded"
        );
        assert_eq!(runtime_state.advertised_model_ids(), ["model-a"]);
        let second_attempt = upstream.next_calibration().await;
        assert_eq!(second_attempt.model_id(), "model-b");
        assert_eq!(first_attempt, second_attempt);
        assert_eq!(runtime_state.advertised_model_ids(), ["model-a"]);
        wait_for_calibration_count(&metrics, "model-b", "error", 2).await;

        failing.store(false, Ordering::SeqCst);
        wait_for_model_ids(&runtime_state, &["model-a", "model-b"]).await;
        let completed_metrics = metrics.gather_text().expect("metrics should encode");
        assert!(completed_metrics.contains(
            r#"pylon_model_calibration_duration_ms_count{model="model-b",outcome="completed"} 1"#
        ));

        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[test]
    fn pending_initializations_choose_the_earliest_retry_deadline() {
        let now = tokio::time::Instant::now();
        let pending = [
            super::PendingInitialization {
                generation: ModelGeneration::new("model-late", 1),
                retry_at: now + Duration::from_secs(2),
            },
            super::PendingInitialization {
                generation: ModelGeneration::new("model-ready", 2),
                retry_at: now,
            },
            super::PendingInitialization {
                generation: ModelGeneration::new("model-next", 3),
                retry_at: now + Duration::from_secs(1),
            },
        ];

        assert_eq!(super::earliest_pending_index(&pending), Some(1));
    }

    #[tokio::test]
    async fn shutdown_cancels_active_calibration_without_completing_it() {
        let mut upstream = TestUpstream::spawn(&[], CompletionBehavior::Pending).await;
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let lifecycle = start_model_lifecycle(
            calibration_discovery_config(&upstream.base_url, Duration::from_secs(60)),
            runtime_state.clone(),
            &stats,
            Some(metrics.clone()),
        )
        .await
        .expect("empty initial discovery should start");

        upstream.set_models(&["model-a"]).await;
        let generation = upstream.next_calibration().await;
        lifecycle.shutdown().await;

        assert!(runtime_state.advertised_model_ids().is_empty());
        assert_eq!(
            stats
                .control()
                .flush_and_snapshot(&generation)
                .await
                .expect("stats collector should respond"),
            None
        );
        wait_for_calibration_count(&metrics, "model-a", "cancelled", 1).await;
        let metric_body = metrics.gather_text().expect("metrics should encode");
        assert!(!metric_body.contains(
            r#"pylon_model_calibration_duration_ms_count{model="model-a",outcome="completed"}"#
        ));

        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn blocked_discovery_does_not_pause_or_inflate_active_calibration() {
        let mut upstream = TestUpstream::spawn(
            &[],
            CompletionBehavior::SaturateAfter {
                completed_requests_per_generation: 7,
            },
        )
        .await;
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let stats_config = StatsCollectorConfig {
            duration_floor: Duration::ZERO,
            ..StatsCollectorConfig::default()
        };
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let mut config =
            calibration_discovery_config(&upstream.base_url, Duration::from_millis(100));
        let ModelSource::Discovered(discovery) = &mut config.source else {
            unreachable!("test config must use discovery")
        };
        discovery.request_timeout = Duration::from_secs(60);
        let lifecycle =
            start_model_lifecycle(config, runtime_state.clone(), &stats, Some(metrics.clone()))
                .await
                .expect("empty initial discovery should start");

        upstream.set_models(&["model-a"]).await;
        upstream.next_calibration().await;
        upstream.set_discovery_blocked(true);
        let polls = upstream.discovery_polls.load(Ordering::SeqCst);
        wait_for_discovery_polls(&upstream.discovery_polls, polls + 1).await;

        tokio::time::timeout(
            Duration::from_millis(500),
            wait_for_model_ids(&runtime_state, &["model-a"]),
        )
        .await
        .expect("a blocked discovery request must not pause calibration");
        wait_for_calibration_count(&metrics, "model-a", "completed", 1).await;
        assert!(
            calibration_duration_sum_ms(&metrics, "model-a", "completed") < 250.0,
            "discovery request latency must not inflate calibration duration"
        );

        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn sibling_reconciliation_does_not_pause_or_inflate_active_calibration() {
        let mut upstream = TestUpstream::spawn(
            &[],
            CompletionBehavior::SaturateAfter {
                completed_requests_per_generation: 7,
            },
        )
        .await;
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let stats_config = StatsCollectorConfig {
            duration_floor: Duration::ZERO,
            ..StatsCollectorConfig::default()
        };
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let active = ModelGeneration::new("model-a", 1);
        let sibling = ModelGeneration::new("model-b", 2);
        assert!(runtime_state.begin_generation(active.clone()));
        assert!(runtime_state.begin_generation(sibling.clone()));
        assert!(runtime_state.publish_generation(&sibling));
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let mut config =
            calibration_discovery_config(&upstream.base_url, Duration::from_millis(100));
        let ModelSource::Discovered(discovery) = &mut config.source else {
            unreachable!("test config must use discovery")
        };
        // Poll quickly so sibling reconciliation competes with the active
        // calibration attempt before its saturation timeout fires.
        discovery.poll_interval = Duration::from_millis(1);
        upstream.set_models(&["model-a"]).await;
        let release_sibling_canary = Arc::new(Notify::new());
        let canary_release = release_sibling_canary.clone();
        let sibling_canary = OwnedTask::spawn("blocked sibling canary", move |_| async move {
            canary_release.notified().await;
        });
        let mut supervisor = ModelLifecycleSupervisor {
            config,
            runtime_state: runtime_state.clone(),
            stats: stats.control(),
            metrics: Some(metrics.clone()),
            http_client: reqwest::Client::new(),
            live: HashMap::from([
                (
                    active.model_id().to_string(),
                    LiveModelGeneration {
                        generation: active.clone(),
                        canary: None,
                    },
                ),
                (
                    sibling.model_id().to_string(),
                    LiveModelGeneration {
                        generation: sibling.clone(),
                        canary: Some(sibling_canary),
                    },
                ),
            ]),
            pending: Vec::new(),
            next_generation: 3,
            next_poll_at: Some(tokio::time::Instant::now()),
        };
        let active_for_drive = active.clone();
        let drive = tokio::spawn(async move {
            let outcome = supervisor
                .drive_initialization(&active_for_drive, None)
                .await;
            (outcome, supervisor)
        });
        let generation = upstream.next_calibration().await;
        assert_eq!(generation.model_id(), "model-a");
        wait_for_discovery_polls(&upstream.discovery_polls, 1).await;
        upstream.set_models(&["model-a", "model-b"]).await;
        let polls_after_readd = upstream.discovery_polls.load(Ordering::SeqCst);
        wait_for_discovery_polls(&upstream.discovery_polls, polls_after_readd + 1).await;

        tokio::time::timeout(
            Duration::from_millis(750),
            wait_for_calibration_count(&metrics, "model-a", "completed", 1),
        )
        .await
        .expect("sibling reconciliation must not pause active calibration completion");
        assert!(
            calibration_duration_sum_ms(&metrics, "model-a", "completed") < 250.0,
            "sibling reconciliation latency must not inflate calibration duration"
        );

        release_sibling_canary.notify_waiters();
        let (outcome, mut supervisor) = tokio::time::timeout(Duration::from_secs(1), drive)
            .await
            .expect("deferred reconciliation should finish after the sibling canary stops")
            .expect("initialization task should not panic");
        assert!(matches!(
            outcome,
            Ok(super::InitializationAttemptOutcome::Ready)
        ));
        let replacement = runtime_state
            .current_generation("model-b")
            .expect("reappearing sibling should start a fresh generation");
        assert_ne!(
            replacement, sibling,
            "queued remove/readd transitions must not leave the old sibling generation live"
        );
        assert!(
            !runtime_state.generation_is_published(&replacement),
            "reappearing sibling should calibrate before publication"
        );

        supervisor.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_cancels_blocked_discovery_during_active_calibration() {
        let mut upstream = TestUpstream::spawn(&[], CompletionBehavior::Pending).await;
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let mut config = calibration_discovery_config(&upstream.base_url, Duration::from_secs(60));
        let ModelSource::Discovered(discovery) = &mut config.source else {
            unreachable!("test config must use discovery")
        };
        discovery.request_timeout = Duration::from_secs(60);
        let lifecycle = start_model_lifecycle(config, runtime_state, &stats, Some(metrics.clone()))
            .await
            .expect("empty initial discovery should start");

        upstream.set_models(&["model-a"]).await;
        upstream.next_calibration().await;
        upstream.set_discovery_blocked(true);
        let polls = upstream.discovery_polls.load(Ordering::SeqCst);
        wait_for_discovery_polls(&upstream.discovery_polls, polls + 1).await;

        tokio::time::timeout(Duration::from_millis(500), lifecycle.shutdown())
            .await
            .expect("shutdown must cancel discovery while calibration is active");
        wait_for_calibration_count(&metrics, "model-a", "cancelled", 1).await;

        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn discovery_errors_during_calibration_retain_the_pending_generation() {
        let mut upstream = TestUpstream::spawn(
            &[],
            CompletionBehavior::SaturateAfter {
                completed_requests_per_generation: 7,
            },
        )
        .await;
        let stats_config = StatsCollectorConfig {
            duration_floor: Duration::ZERO,
            ..StatsCollectorConfig::default()
        };
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let lifecycle = start_model_lifecycle(
            calibration_discovery_config(&upstream.base_url, Duration::from_millis(100)),
            runtime_state.clone(),
            &stats,
            None,
        )
        .await
        .expect("empty initial discovery should start");

        upstream.set_models(&["model-a"]).await;
        let generation = upstream.next_calibration().await;
        upstream.set_discovery_failing(true);
        let polls = upstream.discovery_polls.load(Ordering::SeqCst);
        wait_for_discovery_polls(&upstream.discovery_polls, polls + 2).await;

        assert_eq!(
            runtime_state.current_generation("model-a"),
            Some(generation.clone())
        );
        assert!(runtime_state.advertised_model_ids().is_empty());
        wait_for_model_ids(&runtime_state, &["model-a"]).await;
        assert_eq!(
            runtime_state.current_generation("model-a"),
            Some(generation),
            "poll errors must retain the last-known-good generation"
        );

        upstream.set_models(&[]).await;
        upstream.set_discovery_failing(false);
        wait_for_model_ids(&runtime_state, &[]).await;
        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn initial_discovered_calibrations_run_sequentially() {
        let mut upstream = TestUpstream::spawn(
            &["model-d", "model-b", "model-a", "model-c"],
            CompletionBehavior::SaturateAfter {
                completed_requests_per_generation: 7,
            },
        )
        .await;
        let stats_config = StatsCollectorConfig {
            duration_floor: Duration::ZERO,
            ..StatsCollectorConfig::default()
        };
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let config = calibration_discovery_config(&upstream.base_url, Duration::from_secs(30));
        let start_runtime = runtime_state.clone();
        let start = tokio::spawn(async move {
            let result = start_model_lifecycle(config, start_runtime, &stats, None).await;
            (result, stats)
        });

        let mut order = Vec::new();
        for expected_model in ["model-a", "model-b", "model-c", "model-d"] {
            for _ in 0..8 {
                assert_eq!(upstream.next_calibration().await.model_id(), expected_model);
            }
            order.push(expected_model.to_string());
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(30)).await;
            tokio::time::resume();
        }
        let (lifecycle, stats) = start.await.expect("lifecycle startup should not panic");
        let lifecycle = lifecycle.expect("all expected timeouts should complete calibration");

        assert_eq!(order, ["model-a", "model-b", "model-c", "model-d"]);
        assert_eq!(
            runtime_state.advertised_model_ids(),
            ["model-a", "model-b", "model-c", "model-d"]
        );

        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn initial_removal_cancels_blocked_calibration_before_publication() {
        let mut upstream = TestUpstream::spawn(&["model-a"], CompletionBehavior::Pending).await;
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let stats_config = StatsCollectorConfig::default();
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        let config = calibration_discovery_config(&upstream.base_url, Duration::from_secs(60));
        let start_runtime = runtime_state.clone();
        let start_metrics = metrics.clone();
        let start = tokio::spawn(async move {
            let result =
                start_model_lifecycle(config, start_runtime, &stats, Some(start_metrics)).await;
            (result, stats)
        });

        let retired = upstream.next_calibration().await;
        upstream.set_models(&[]).await;
        let (lifecycle, stats) = tokio::time::timeout(Duration::from_secs(2), start)
            .await
            .expect("initial removal should cancel blocked calibration")
            .expect("lifecycle startup task should not panic");
        let lifecycle = lifecycle.expect("valid empty discovery should complete startup");

        assert!(runtime_state.advertised_model_ids().is_empty());
        assert_eq!(
            stats
                .control()
                .flush_and_snapshot(&retired)
                .await
                .expect("stats collector should respond"),
            None
        );
        wait_for_calibration_count(&metrics, "model-a", "cancelled", 1).await;

        lifecycle.shutdown().await;
        stats.shutdown().await;
        upstream.shutdown().await;
    }

    /// Waits at most two seconds for `future`, well below the supervisor's
    /// 5 s stop timeout, so a hang fails the test instead of stalling it.
    async fn within<F: std::future::Future>(what: &str, future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(2), future)
            .await
            .unwrap_or_else(|_| panic!("{what} did not finish within 2 s"))
    }

    /// Starts a stats collector whose KV-cache poll the test can hold open.
    /// While the poll is held, the collector reads no commands, so lifecycle
    /// requests stay queued and unread when the collector ends.
    fn start_holdable_stats(
        kv_cache: &BlockingKvCacheServer,
    ) -> (PylonRuntimeState, crate::StatsCollectorHandle) {
        let stats_config = StatsCollectorConfig {
            kv_cache_stats_url: Some(kv_cache.url()),
            kv_cache_poll_interval: Duration::from_millis(1),
            kv_cache_request_timeout: Duration::from_secs(60),
            ..StatsCollectorConfig::default()
        };
        let (runtime_state, observations) = PylonRuntimeState::observed(
            InferenceServerStatus::Active,
            &[],
            stats_config.observation_channel_capacity,
            None,
        );
        let stats = start_stats_collector(stats_config, observations, runtime_state.clone());
        (runtime_state, stats)
    }

    async fn hold_stats_collector(kv_cache: &BlockingKvCacheServer) {
        let blocked_polls = kv_cache.blocked_poll_count();
        kv_cache.block();
        kv_cache.wait_for_blocked_poll_after(blocked_polls).await;
    }

    /// Queues a begin that panics the collector, without waiting for it: a
    /// configured input TPS of zero breaks a stats initialization invariant.
    /// The command stays queued after its request is dropped.
    fn queue_stats_collector_panic(stats: &crate::StatsCollectorHandle) {
        let control = stats.control();
        let trigger = std::pin::pin!(control.begin_generation(
            ModelGeneration::new("model-panics", 1),
            crate::stats::ModelStatsInitialization::ConfiguredInputTps { input_tps: 0.0 },
        ));
        let polled = std::future::Future::poll(
            trigger,
            &mut std::task::Context::from_waker(std::task::Waker::noop()),
        );
        assert!(
            polled.is_pending(),
            "the held collector should leave the panic trigger unread"
        );
    }

    fn events_with_message<'a>(
        events: &'a [RecordedTracingEvent],
        message: &str,
    ) -> Vec<&'a RecordedTracingEvent> {
        events
            .iter()
            .filter(|event| event.fields.get("message").map(String::as_str) == Some(message))
            .collect()
    }

    fn error_events(events: &[RecordedTracingEvent]) -> Vec<&RecordedTracingEvent> {
        events
            .iter()
            .filter(|event| event.level == tracing::Level::ERROR)
            .collect()
    }

    const SUPERVISOR_FAILED: &str = "model lifecycle supervisor failed";
    const SUPERVISOR_STOPPED_IN_SHUTDOWN: &str =
        "model lifecycle supervisor stopped with the stats collector during shutdown";
    const STATS_COLLECTOR_STOPPED: &str =
        "stats collector stopped during model lifecycle transition";

    #[tokio::test]
    async fn collector_shutdown_during_a_lifecycle_request_logs_no_error() {
        let kv_cache = BlockingKvCacheServer::spawn().await;
        let upstream = TestUpstream::spawn(&["model-a"], CompletionBehavior::Pending).await;
        let (runtime_state, stats) = start_holdable_stats(&kv_cache);
        let mut lifecycle = within(
            "model lifecycle startup",
            start_model_lifecycle(
                discovery_config(&upstream.base_url),
                runtime_state.clone(),
                &stats,
                None,
            ),
        )
        .await
        .expect("initial discovery should initialize");
        let subscriber = RecordingTracingSubscriber::default();
        let dispatch = tracing::Dispatch::new(subscriber.clone());
        let default_guard = tracing::dispatcher::set_default(&dispatch);
        hold_stats_collector(&kv_cache).await;

        // A newly discovered model makes the supervisor begin a generation,
        // and the held collector leaves that request queued.
        upstream.set_models(&["model-a", "model-b"]).await;
        wait_for(
            "the supervisor should begin a generation for model-b",
            || runtime_state.current_generation("model-b").is_some(),
        )
        .await;
        // Only the collector shuts down, so the failed request alone ends the
        // supervisor's run. The shutdown is polled once and then left alone
        // until the supervisor has exited. The supervisor reads the
        // collector's record of why it ended, so a shutdown mark set after
        // that first poll would leave the shutdown out of that record.
        let shutdown = stats.shutdown();
        tokio::pin!(shutdown);
        assert!(
            futures::poll!(&mut shutdown).is_pending(),
            "the held collector should not end within the first poll of its shutdown"
        );
        within("model lifecycle supervisor exit", lifecycle.wait_for_exit())
            .await
            .expect("model lifecycle supervisor should not panic");
        within("stats collector shutdown", shutdown).await;
        drop(default_guard);

        let events = subscriber.events();
        let errors = error_events(&events);
        assert!(
            errors.is_empty(),
            "a stats collector shutdown should log no error: {errors:?}"
        );
        let stopped = events_with_message(&events, SUPERVISOR_STOPPED_IN_SHUTDOWN);
        assert_eq!(
            stopped.len(),
            1,
            "the in-flight request should end the supervisor with the collector's shutdown"
        );
        assert_eq!(stopped[0].level, tracing::Level::DEBUG);
        assert_tracing_event_field(stopped[0], "error", STATS_COLLECTOR_STOPPED);
        assert!(
            !lifecycle.stopped_by_stats_collector(),
            "a supervisor that stopped with the collector's shutdown should not report the \
             collector as the reason it stopped"
        );

        kv_cache.unblock();
        kv_cache.shutdown().await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn collector_ending_outside_a_shutdown_during_a_lifecycle_request_logs_an_error() {
        for panics in [false, true] {
            let ending = if panics { "panic" } else { "abort" };
            let kv_cache = BlockingKvCacheServer::spawn().await;
            let upstream = TestUpstream::spawn(&["model-a"], CompletionBehavior::Pending).await;
            let (runtime_state, mut stats) = start_holdable_stats(&kv_cache);
            let mut lifecycle = within(
                "model lifecycle startup",
                start_model_lifecycle(
                    discovery_config(&upstream.base_url),
                    runtime_state.clone(),
                    &stats,
                    None,
                ),
            )
            .await
            .expect("initial discovery should initialize");
            let subscriber = RecordingTracingSubscriber::default();
            let dispatch = tracing::Dispatch::new(subscriber.clone());
            let default_guard = tracing::dispatcher::set_default(&dispatch);
            hold_stats_collector(&kv_cache).await;
            assert!(
                !lifecycle.stopped_by_stats_collector(),
                "{ending}: a running supervisor should not report a collector stop"
            );

            if panics {
                // The panic trigger is queued ahead of the supervisor's
                // request. This relies on the collector reading queued
                // commands in the order they were sent, which flume does not
                // document; the supervisor exit below fails if that changes.
                queue_stats_collector_panic(&stats);
            }
            upstream.set_models(&["model-a", "model-b"]).await;
            wait_for(
                "the supervisor should begin a generation for model-b",
                || runtime_state.current_generation("model-b").is_some(),
            )
            .await;
            if panics {
                kv_cache.unblock();
                let exit = within("the collector exit", stats.wait_for_exit()).await;
                assert!(
                    matches!(&exit, Err(error) if error.is_panic()),
                    "the panic trigger should end the collector with a panic: {exit:?}"
                );
            } else {
                // Dropping the handle aborts the collector without a shutdown.
                drop(stats);
            }
            let supervisor_exit = if panics {
                "model lifecycle supervisor exit (if it does not exit, the collector may have \
                 answered its begin before the command queued ahead of it)"
            } else {
                "model lifecycle supervisor exit"
            };
            within(supervisor_exit, lifecycle.wait_for_exit())
                .await
                .expect("model lifecycle supervisor should not panic");
            drop(default_guard);

            let events = subscriber.events();
            let failed = events_with_message(&events, SUPERVISOR_FAILED);
            assert_eq!(
                failed.len(),
                1,
                "{ending}: the supervisor should report the collector that ended outside a shutdown"
            );
            assert_eq!(failed[0].level, tracing::Level::ERROR, "{ending}");
            assert_tracing_event_field(failed[0], "error", STATS_COLLECTOR_STOPPED);
            assert!(
                events_with_message(&events, SUPERVISOR_STOPPED_IN_SHUTDOWN).is_empty(),
                "{ending}: a collector that ended outside a shutdown is not part of one"
            );
            assert!(
                lifecycle.stopped_by_stats_collector(),
                "{ending}: the supervisor should report that the collector's end stopped it, so \
                 Pylon names the collector"
            );

            kv_cache.unblock();
            kv_cache.shutdown().await;
            upstream.shutdown().await;
        }
    }

    #[tokio::test]
    async fn other_lifecycle_failure_during_a_collector_shutdown_logs_an_error() {
        // Only a collector stop is part of the collector's shutdown. To fail
        // the supervisor another way while the shutdown mark is set, the test
        // takes model-b's runtime slot, so the supervisor's begin for model-b
        // reports a stale generation before it sends the collector anything.
        let upstream = TestUpstream::spawn(&["model-a"], CompletionBehavior::Pending).await;
        let (runtime_state, stats) = start_test_stats();
        let mut lifecycle = within(
            "model lifecycle startup",
            start_model_lifecycle(
                discovery_config(&upstream.base_url),
                runtime_state.clone(),
                &stats,
                None,
            ),
        )
        .await
        .expect("initial discovery should initialize");
        let subscriber = RecordingTracingSubscriber::default();
        let dispatch = tracing::Dispatch::new(subscriber.clone());
        let default_guard = tracing::dispatcher::set_default(&dispatch);

        within("stats collector shutdown", stats.shutdown()).await;
        assert!(runtime_state.begin_generation(ModelGeneration::new("model-b", 1)));
        upstream.set_models(&["model-a", "model-b"]).await;
        within("model lifecycle supervisor exit", lifecycle.wait_for_exit())
            .await
            .expect("model lifecycle supervisor should not panic");
        drop(default_guard);

        let events = subscriber.events();
        let failed = events_with_message(&events, SUPERVISOR_FAILED);
        assert_eq!(
            failed.len(),
            1,
            "the supervisor should report the stale generation"
        );
        assert_eq!(failed[0].level, tracing::Level::ERROR);
        assert_tracing_event_field(
            failed[0],
            "error",
            "model generation transition lost exact ownership",
        );
        assert!(
            events_with_message(&events, SUPERVISOR_STOPPED_IN_SHUTDOWN).is_empty(),
            "a failure other than a collector stop is not part of the collector's shutdown"
        );
        // Pylon waits for the collector's exit when this is set.
        assert!(
            !lifecycle.stopped_by_stats_collector(),
            "a supervisor that failed on its own should not report the collector as the reason"
        );

        upstream.shutdown().await;
    }

    /// How a stats collector ends without its handle's shutdown ending it.
    #[derive(Clone, Copy, Debug)]
    enum CollectorEndOutsideShutdown {
        /// It panics, and then its handle's shutdown starts.
        Panic,
        /// Its observation channel closes, so it ends on its own, and then its
        /// handle's shutdown starts.
        EndOnItsOwn,
        /// Its handle's shutdown starts, and it panics before it sees its stop.
        PanicDuringShutdown,
    }

    const KV_CACHE_STATS_REJECTED: &str = "KV-cache stats endpoint returned non-success status";

    /// A shutdown that `CollectorPanicHook` starts.
    type ArmedShutdown<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

    /// Records events with a `RecordingTracingSubscriber`. Once armed with a
    /// shutdown, it panics the stats collector from inside the collector's own
    /// run, after that shutdown has started: on the collector's next KV-cache
    /// warning it polls the shutdown once, and then it panics. The first poll
    /// of a stats collector's shutdown sets its shutdown mark and its stop,
    /// and that of a model lifecycle's shutdown cancels the supervisor's stop.
    /// The collector is busy logging, so it cannot see its own stop first, and
    /// on this single-threaded runtime the supervisor cannot run before the
    /// collector has ended.
    ///
    /// This relies on tracing calling the subscriber inside the call that logs
    /// the event, and on a panic there unwinding through that call, which
    /// tracing does not document. The tests check that the collector panicked,
    /// so they fail if that changes.
    struct CollectorPanicHook<T> {
        recorder: RecordingTracingSubscriber,
        armed: Arc<StdMutex<Option<ArmedShutdown<T>>>>,
        started: Arc<StdMutex<Option<ArmedShutdown<T>>>>,
    }

    impl<T> Default for CollectorPanicHook<T> {
        fn default() -> Self {
            Self {
                recorder: RecordingTracingSubscriber::default(),
                armed: Arc::default(),
                started: Arc::default(),
            }
        }
    }

    impl<T> Clone for CollectorPanicHook<T> {
        fn clone(&self) -> Self {
            Self {
                recorder: self.recorder.clone(),
                armed: Arc::clone(&self.armed),
                started: Arc::clone(&self.started),
            }
        }
    }

    impl<T> CollectorPanicHook<T> {
        fn arm(&self, shutdown: ArmedShutdown<T>) {
            *self
                .armed
                .lock()
                .expect("armed shutdown should not be poisoned") = Some(shutdown);
        }

        /// Returns the shutdown that the hook started once it has panicked the
        /// collector.
        async fn started_shutdown(&self) -> ArmedShutdown<T> {
            loop {
                if let Some(shutdown) = self
                    .started
                    .lock()
                    .expect("started shutdown should not be poisoned")
                    .take()
                {
                    return shutdown;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    #[derive(Default)]
    struct EventMessage(Option<String>);

    impl tracing::field::Visit for EventMessage {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = Some(format!("{value:?}"));
            }
        }
    }

    impl<T: 'static> tracing::Subscriber for CollectorPanicHook<T> {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            tracing::Subscriber::enabled(&self.recorder, metadata)
        }

        fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
            tracing::Subscriber::max_level_hint(&self.recorder)
        }

        fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::Subscriber::new_span(&self.recorder, attrs)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            tracing::Subscriber::event(&self.recorder, event);
            let mut message = EventMessage::default();
            event.record(&mut message);
            if message.0.as_deref() != Some(KV_CACHE_STATS_REJECTED) {
                return;
            }
            let Some(mut shutdown) = self
                .armed
                .lock()
                .expect("armed shutdown should not be poisoned")
                .take()
            else {
                return;
            };
            let polled = shutdown
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()));
            if polled.is_ready() {
                // Neither shutdown can finish while the collector runs here,
                // so this does not happen; started_shutdown then times out.
                return;
            }
            *self
                .started
                .lock()
                .expect("started shutdown should not be poisoned") = Some(shutdown);
            panic!("test panic in the stats collector after the armed shutdown started");
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    #[tokio::test]
    async fn collector_that_its_shutdown_did_not_end_logs_an_error_even_once_a_shutdown_started() {
        // The supervisor finds the collector gone only after the collector's
        // shutdown has started, so the shutdown mark is set by then. Only the
        // collector's record of why it ended tells it that the shutdown did
        // not end the collector.
        let rejecting_kv_cache = TestHttpServer::spawn(Router::new().route(
            "/kv-cache",
            get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        ))
        .await;
        for ending in [
            CollectorEndOutsideShutdown::Panic,
            CollectorEndOutsideShutdown::EndOnItsOwn,
            CollectorEndOutsideShutdown::PanicDuringShutdown,
        ] {
            let upstream = TestUpstream::spawn(&["model-a"], CompletionBehavior::Pending).await;
            let runtime_state = PylonRuntimeState::new(InferenceServerStatus::Active, &[]);
            let (observations, observation_rx) = flume::bounded(16);
            // A rejected KV-cache poll logs a warning on every poll, which is
            // where the hook panics the collector.
            let stats = start_stats_collector(
                StatsCollectorConfig {
                    kv_cache_stats_url: Some(format!("{}/kv-cache", rejecting_kv_cache.as_str())),
                    kv_cache_poll_interval: Duration::from_millis(5),
                    ..StatsCollectorConfig::default()
                },
                observation_rx,
                runtime_state.clone(),
            );
            let mut lifecycle = within(
                "model lifecycle startup",
                start_model_lifecycle(
                    discovery_config(&upstream.base_url),
                    runtime_state.clone(),
                    &stats,
                    None,
                ),
            )
            .await
            .expect("initial discovery should initialize");
            let hook = CollectorPanicHook::<anyhow::Result<()>>::default();
            let dispatch = tracing::Dispatch::new(hook.clone());
            let default_guard = tracing::dispatcher::set_default(&dispatch);

            match ending {
                CollectorEndOutsideShutdown::Panic => {
                    let begun = within(
                        "the request that panics the collector",
                        stats.control().begin_generation(
                            ModelGeneration::new("model-panics", 1),
                            crate::stats::ModelStatsInitialization::ConfiguredInputTps {
                                input_tps: 0.0,
                            },
                        ),
                    )
                    .await;
                    assert!(
                        begun.is_err(),
                        "{ending:?}: a configured input TPS of zero should panic the collector: \
                         {begun:?}"
                    );
                    let reported =
                        within("the collector shutdown", stats.shutdown_reporting_panic()).await;
                    assert_eq!(
                        reported.map_err(|error| error.to_string()),
                        Err("stats collector panicked".to_string()),
                        "{ending:?}"
                    );
                }
                CollectorEndOutsideShutdown::EndOnItsOwn => {
                    let mut stats = stats;
                    drop(observations);
                    let exit = within("the collector's own end", stats.wait_for_exit()).await;
                    assert!(
                        exit.is_ok(),
                        "{ending:?}: a closed observation channel should end the collector \
                         without a panic: {exit:?}"
                    );
                    let reported =
                        within("the collector shutdown", stats.shutdown_reporting_panic()).await;
                    assert!(reported.is_ok(), "{ending:?}: {reported:?}");
                }
                CollectorEndOutsideShutdown::PanicDuringShutdown => {
                    hook.arm(Box::pin(stats.shutdown_reporting_panic()));
                    let shutdown = within(
                        "the hook's panic in the collector's KV-cache warning",
                        hook.started_shutdown(),
                    )
                    .await;
                    let reported = within("the collector shutdown", shutdown).await;
                    assert_eq!(
                        reported.map_err(|error| error.to_string()),
                        Err("stats collector panicked".to_string()),
                        "{ending:?}: the hook's panic should end the collector; if it did not, \
                         tracing may no longer unwind a subscriber panic into the caller"
                    );
                }
            }
            // The supervisor is idle until its model set changes, so it has not
            // asked the collector for anything since the collector ended.
            assert!(
                hook.recorder.event_count(SUPERVISOR_FAILED) == 0
                    && hook.recorder.event_count(SUPERVISOR_STOPPED_IN_SHUTDOWN) == 0,
                "{ending:?}: the supervisor should find the collector gone only after the \
                 shutdown started"
            );

            upstream.set_models(&["model-a", "model-b"]).await;
            within("model lifecycle supervisor exit", lifecycle.wait_for_exit())
                .await
                .expect("model lifecycle supervisor should not panic");
            drop(default_guard);

            let events = hook.recorder.events();
            let failed = events_with_message(&events, SUPERVISOR_FAILED);
            assert_eq!(
                failed.len(),
                1,
                "{ending:?}: the supervisor should report a collector that its shutdown did not \
                 end"
            );
            assert_eq!(failed[0].level, tracing::Level::ERROR, "{ending:?}");
            assert_tracing_event_field(failed[0], "error", STATS_COLLECTOR_STOPPED);
            assert!(
                events_with_message(&events, SUPERVISOR_STOPPED_IN_SHUTDOWN).is_empty(),
                "{ending:?}: a collector that its shutdown did not end is not part of that \
                 shutdown"
            );
            assert!(
                lifecycle.stopped_by_stats_collector(),
                "{ending:?}: the supervisor should report that the collector's end stopped it"
            );

            upstream.shutdown().await;
        }
        rejecting_kv_cache.shutdown().await;
    }

    #[tokio::test]
    async fn collector_panic_found_after_the_supervisor_stop_logs_an_error() {
        // Pylon's shutdown cancels the supervisor's own stop too. A collector
        // that panics then, with a supervisor request in flight, was not ended
        // by its shutdown, so the supervisor reports it as an error although
        // its stop is cancelled by then. The supervisor handles the failed
        // request instead of taking its stop because the request runs under
        // `run_until_cancelled`, which tokio-util documents as biased toward
        // the future when the future completes as the token is cancelled.
        let kv_cache = BlockingKvCacheServer::spawn_rejecting().await;
        let upstream = TestUpstream::spawn(&["model-a"], CompletionBehavior::Pending).await;
        let (runtime_state, mut stats) = start_holdable_stats(&kv_cache);
        let lifecycle = within(
            "model lifecycle startup",
            start_model_lifecycle(
                discovery_config(&upstream.base_url),
                runtime_state.clone(),
                &stats,
                None,
            ),
        )
        .await
        .expect("initial discovery should initialize");
        let hook = CollectorPanicHook::<()>::default();
        let dispatch = tracing::Dispatch::new(hook.clone());
        let default_guard = tracing::dispatcher::set_default(&dispatch);
        hold_stats_collector(&kv_cache).await;

        // A newly discovered model makes the supervisor begin a generation,
        // and the held collector leaves that request queued.
        upstream.set_models(&["model-a", "model-b"]).await;
        wait_for(
            "the supervisor should begin a generation for model-b",
            || runtime_state.current_generation("model-b").is_some(),
        )
        .await;
        // The released poll is rejected. In that warning the hook cancels the
        // supervisor's stop and then panics the collector.
        hook.arm(Box::pin(lifecycle.shutdown()));
        kv_cache.unblock();
        let supervisor_shutdown = within(
            "the hook's panic in the collector's KV-cache warning",
            hook.started_shutdown(),
        )
        .await;
        let exit = within("the collector exit", stats.wait_for_exit()).await;
        assert!(
            matches!(&exit, Err(error) if error.is_panic()),
            "the hook's panic should end the collector; if it did not, tracing may no longer \
             unwind a subscriber panic into the caller: {exit:?}"
        );
        within("the supervisor shutdown", supervisor_shutdown).await;
        drop(default_guard);

        let events = hook.recorder.events();
        let failed = events_with_message(&events, SUPERVISOR_FAILED);
        let stopped = events_with_message(&events, SUPERVISOR_STOPPED_IN_SHUTDOWN);
        assert_eq!(
            failed.len(),
            1,
            "a collector panic that the supervisor finds after its own stop should be an error, \
             not part of a shutdown: {stopped:?}. Neither line means that the supervisor took \
             its stop before its failed request, against tokio-util's documented bias"
        );
        assert_eq!(failed[0].level, tracing::Level::ERROR);
        assert_tracing_event_field(failed[0], "error", STATS_COLLECTOR_STOPPED);
        assert!(
            stopped.is_empty(),
            "a collector panic is not part of the supervisor's shutdown: {stopped:?}"
        );

        kv_cache.shutdown().await;
        upstream.shutdown().await;
    }

    const PENDING_WARNING: &str = "model initialization failed; keeping generation pending";
    /// How long a calibration step waits for its requests before it asks the
    /// stats collector for a snapshot. With an upstream that never answers,
    /// every step ends after this long.
    const CALIBRATION_STEP: Duration = Duration::from_millis(300);
    const CALIBRATION_COLLECTOR_STOPPED_WARNING: &str =
        "model calibration failed; the stats collector stopped";
    const CALIBRATION_COLLECTOR_STOPPED: &str =
        "stats collector stopped during model initialization";

    fn calibration_config_with_fallback(
        base_url: &str,
        fallback_input_tps: Option<f64>,
    ) -> ModelLifecycleConfig {
        let mut config = calibration_discovery_config(base_url, CALIBRATION_STEP);
        let ModelInitialization::Calibration(calibration) = &mut config.initialization else {
            unreachable!("the test config calibrates")
        };
        calibration.fallback_input_tps = fallback_input_tps;
        config
    }

    /// Reads one calibration duration count, or zero when it was never recorded.
    fn calibration_count(metrics: &PylonMetrics, model_id: &str, outcome: &str) -> u64 {
        let prefix = format!(
            "pylon_model_calibration_duration_ms_count{{model=\"{model_id}\",outcome=\"{outcome}\"}} "
        );
        metrics
            .gather_text()
            .expect("metrics should encode")
            .lines()
            .find_map(|line| {
                line.strip_prefix(&prefix)
                    .and_then(|value| value.parse::<u64>().ok())
            })
            .unwrap_or(0)
    }

    /// The warnings for a failed calibration: the collector stopped, the model
    /// stays pending, or it starts at the fallback input TPS.
    fn calibration_failure_warnings(events: &[RecordedTracingEvent]) -> Vec<&RecordedTracingEvent> {
        events
            .iter()
            .filter(|event| {
                matches!(
                    event.fields.get("message").map(String::as_str),
                    Some(
                        CALIBRATION_COLLECTOR_STOPPED_WARNING | FALLBACK_WARNING | PENDING_WARNING
                    )
                )
            })
            .collect()
    }

    /// The warnings of a supervisor that handles a failed calibration by
    /// trying the model again: it keeps the model pending or starts it at the
    /// fallback input TPS.
    fn calibration_retry_warnings(events: &[RecordedTracingEvent]) -> Vec<&RecordedTracingEvent> {
        events
            .iter()
            .filter(|event| {
                matches!(
                    event.fields.get("message").map(String::as_str),
                    Some(FALLBACK_WARNING | PENDING_WARNING)
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn calibration_cut_short_by_a_collector_shutdown_is_cancelled_without_a_warning() {
        // Each mode has its own warning for a failed calibration.
        for fallback_input_tps in [None, Some(FALLBACK_INPUT_TPS)] {
            let label = format!("fallback input TPS {fallback_input_tps:?}");
            let kv_cache = BlockingKvCacheServer::spawn().await;
            let mut upstream = TestUpstream::spawn(&[], CompletionBehavior::Pending).await;
            let metrics = PylonMetrics::new().expect("metrics should initialize");
            let (runtime_state, stats) = start_holdable_stats(&kv_cache);
            let mut lifecycle = within(
                "model lifecycle startup",
                start_model_lifecycle(
                    calibration_config_with_fallback(&upstream.base_url, fallback_input_tps),
                    runtime_state,
                    &stats,
                    Some(metrics.clone()),
                ),
            )
            .await
            .expect("empty initial discovery should start");
            let subscriber = RecordingTracingSubscriber::default();
            let dispatch = tracing::Dispatch::new(subscriber.clone());
            let default_guard = tracing::dispatcher::set_default(&dispatch);

            upstream.set_models(&["model-a"]).await;
            upstream.next_calibration().await;
            // The upstream never answers, so the step ends and the calibration
            // asks the collector for a snapshot, which the held collector
            // leaves unread. Had the snapshot not been sent yet, it would fail
            // the same way once the collector is gone.
            hold_stats_collector(&kv_cache).await;
            tokio::time::sleep(CALIBRATION_STEP * 2).await;
            within("stats collector shutdown", stats.shutdown()).await;
            // A supervisor that took the cut-short calibration for a failure
            // warns and keeps the model pending instead of exiting, so the
            // warnings are checked before the exit.
            let exit =
                tokio::time::timeout(Duration::from_secs(2), lifecycle.wait_for_exit()).await;
            drop(default_guard);

            let events = subscriber.events();
            let warnings = calibration_failure_warnings(&events);
            assert!(
                warnings.is_empty(),
                "{label}: a calibration cut short by a collector shutdown should not warn: \
                 {warnings:?}"
            );
            exit.unwrap_or_else(|_| {
                panic!(
                    "{label}: the supervisor should stop with the collector's shutdown within 2 s"
                )
            })
            .expect("model lifecycle supervisor should not panic");
            let errors = error_events(&events);
            assert!(
                errors.is_empty(),
                "{label}: a collector shutdown should log no error: {errors:?}"
            );
            let stopped = events_with_message(&events, SUPERVISOR_STOPPED_IN_SHUTDOWN);
            assert_eq!(
                stopped.len(),
                1,
                "{label}: the calibration should stop the supervisor with the collector's shutdown"
            );
            assert_eq!(stopped[0].level, tracing::Level::DEBUG, "{label}");
            assert_eq!(
                calibration_count(&metrics, "model-a", "cancelled"),
                1,
                "{label}: the calibration should be recorded as cancelled"
            );
            assert_eq!(
                calibration_count(&metrics, "model-a", "error"),
                0,
                "{label}: the calibration should not be recorded as an error"
            );
            assert!(
                !lifecycle.stopped_by_stats_collector(),
                "{label}: a collector shutdown is not the collector ending on its own"
            );

            kv_cache.unblock();
            kv_cache.shutdown().await;
            upstream.shutdown().await;
        }
    }

    /// How the stats collector ends while the model's initialization attempt
    /// waits for its health probe, before the calibration starts.
    #[derive(Clone, Copy, Debug)]
    enum CalibrationCollectorEnding {
        /// The collector panics.
        Panic,
        /// Dropping the handle aborts the collector without a shutdown.
        Abort,
        /// The collector panics, and then its handle's shutdown starts.
        PanicThenShutdown,
    }

    #[tokio::test]
    async fn calibration_cut_short_by_a_collector_ending_outside_a_shutdown_warns_and_stops_the_supervisor()
     {
        // Each mode has its own way to try the model again after a failed
        // calibration: keep it pending, or start it at the fallback input TPS.
        for fallback_input_tps in [None, Some(FALLBACK_INPUT_TPS)] {
            for ending in [
                CalibrationCollectorEnding::Panic,
                CalibrationCollectorEnding::Abort,
                CalibrationCollectorEnding::PanicThenShutdown,
            ] {
                let label = format!("{ending:?}, fallback input TPS {fallback_input_tps:?}");
                let mut upstream = TestUpstream::spawn(&[], CompletionBehavior::Pending).await;
                let metrics = PylonMetrics::new().expect("metrics should initialize");
                let (runtime_state, stats) = start_test_stats();
                let mut config =
                    calibration_config_with_fallback(&upstream.base_url, fallback_input_tps);
                let ModelInitialization::Calibration(calibration) = &mut config.initialization
                else {
                    unreachable!("the test config calibrates")
                };
                // The test holds the attempt's health probe for as long as it
                // needs, without the probe timing out.
                calibration.health_timeout = Duration::from_secs(30);
                let mut lifecycle = within(
                    "model lifecycle startup",
                    start_model_lifecycle(config, runtime_state, &stats, Some(metrics.clone())),
                )
                .await
                .expect("empty initial discovery should start");
                let subscriber = RecordingTracingSubscriber::default();
                let dispatch = tracing::Dispatch::new(subscriber.clone());
                let default_guard = tracing::dispatcher::set_default(&dispatch);

                // The attempt for model-a probes the upstream's health before
                // it calibrates, and the probe waits until the test has ended
                // the collector and, for PanicThenShutdown, started the
                // collector's shutdown. Only then can the calibration step
                // start, end CALIBRATION_STEP later, and ask the collector for
                // its snapshot, however late this task runs.
                upstream.hold_health();
                upstream.set_models(&["model-a"]).await;
                upstream.wait_for_held_health_probe().await;
                if !matches!(ending, CalibrationCollectorEnding::Abort) {
                    let begun = within(
                        "the request that panics the collector",
                        stats.control().begin_generation(
                            ModelGeneration::new("model-panics", 1),
                            crate::stats::ModelStatsInitialization::ConfiguredInputTps {
                                input_tps: 0.0,
                            },
                        ),
                    )
                    .await;
                    assert!(
                        begun.is_err(),
                        "{label}: a configured input TPS of zero should panic the collector: \
                         {begun:?}"
                    );
                }
                match ending {
                    CalibrationCollectorEnding::Panic => {
                        let mut stats = stats;
                        let exit = within("the collector exit", stats.wait_for_exit()).await;
                        assert!(
                            matches!(&exit, Err(error) if error.is_panic()),
                            "{label}: the panic trigger should end the collector with a panic: \
                             {exit:?}"
                        );
                    }
                    CalibrationCollectorEnding::Abort => drop(stats),
                    CalibrationCollectorEnding::PanicThenShutdown => {
                        let reported =
                            within("the collector shutdown", stats.shutdown_reporting_panic())
                                .await;
                        assert_eq!(
                            reported.map_err(|error| error.to_string()),
                            Err("stats collector panicked".to_string()),
                            "{label}"
                        );
                    }
                }
                upstream.release_health();
                upstream.next_calibration().await;
                // A supervisor that tried the model again would keep running.
                within("model lifecycle supervisor exit", lifecycle.wait_for_exit())
                    .await
                    .expect("model lifecycle supervisor should not panic");
                drop(default_guard);

                let events = subscriber.events();
                let warnings = events_with_message(&events, CALIBRATION_COLLECTOR_STOPPED_WARNING);
                assert_eq!(
                    warnings.len(),
                    1,
                    "{label}: the calibration should warn that the collector stopped"
                );
                assert_eq!(warnings[0].level, tracing::Level::WARN, "{label}");
                assert_tracing_event_field(warnings[0], "error", CALIBRATION_COLLECTOR_STOPPED);
                assert_tracing_event_field(warnings[0], "model_id", "model-a");
                let retries = calibration_retry_warnings(&events);
                assert!(
                    retries.is_empty(),
                    "{label}: the supervisor should not try the model again against a collector \
                     that is gone: {retries:?}"
                );
                assert_eq!(
                    calibration_count(&metrics, "model-a", "error"),
                    1,
                    "{label}: the one calibration attempt should be recorded as an error"
                );
                assert_eq!(
                    calibration_count(&metrics, "model-a", "cancelled"),
                    0,
                    "{label}: the calibration should be recorded as an error, not as cancelled"
                );
                let failed = events_with_message(&events, SUPERVISOR_FAILED);
                assert_eq!(
                    failed.len(),
                    1,
                    "{label}: the calibration should stop the supervisor with the collector stop"
                );
                assert_eq!(failed[0].level, tracing::Level::ERROR, "{label}");
                assert_tracing_event_field(failed[0], "error", STATS_COLLECTOR_STOPPED);
                assert!(
                    events_with_message(&events, SUPERVISOR_STOPPED_IN_SHUTDOWN).is_empty(),
                    "{label}: a collector that ended outside a shutdown is not part of one"
                );
                assert!(
                    lifecycle.stopped_by_stats_collector(),
                    "{label}: the supervisor should report that the collector's end stopped it, \
                     so Pylon names the collector"
                );

                upstream.shutdown().await;
            }
        }
    }

    #[tokio::test]
    async fn calibration_failing_on_its_own_during_a_collector_shutdown_keeps_its_warning_and_error()
     {
        // Only a calibration whose stats request fails is part of the
        // collector's shutdown. This upstream rejects every calibration
        // request, so attempts keep failing on their own after the shutdown
        // mark is set, without asking the collector for a snapshot.
        let upstream = TestUpstream::spawn(
            &[],
            CompletionBehavior::FailModel {
                model_id: "model-a",
                failing: Arc::new(AtomicBool::new(true)),
                completed_requests_per_generation: 0,
            },
        )
        .await;
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let (runtime_state, stats) = start_test_stats();
        let lifecycle = within(
            "model lifecycle startup",
            start_model_lifecycle(
                calibration_config_with_fallback(&upstream.base_url, None),
                runtime_state,
                &stats,
                Some(metrics.clone()),
            ),
        )
        .await
        .expect("empty initial discovery should start");
        upstream.set_models(&["model-a"]).await;
        wait_for(
            "the rejected calibration should be recorded as an error",
            || calibration_count(&metrics, "model-a", "error") >= 1,
        )
        .await;

        within("stats collector shutdown", stats.shutdown()).await;
        let subscriber = RecordingTracingSubscriber::default();
        let dispatch = tracing::Dispatch::new(subscriber.clone());
        let default_guard = tracing::dispatcher::set_default(&dispatch);
        // Two more failures, so at least one attempt ran entirely after the mark.
        let failed_before = calibration_count(&metrics, "model-a", "error");
        wait_for(
            "calibrations rejected after the collector's shutdown mark should be recorded as \
             errors",
            || calibration_count(&metrics, "model-a", "error") >= failed_before + 2,
        )
        .await;
        drop(default_guard);

        let events = subscriber.events();
        let warnings = events_with_message(&events, PENDING_WARNING);
        assert!(
            !warnings.is_empty(),
            "a calibration that failed on its own should warn during a collector shutdown"
        );
        for warning in &warnings {
            assert_eq!(warning.level, tracing::Level::WARN);
            assert!(
                warning
                    .fields
                    .get("error")
                    .is_some_and(|error| error.contains("upstream rejected request")),
                "the warning should report the upstream rejection: {warning:?}"
            );
        }
        assert!(
            events_with_message(&events, SUPERVISOR_STOPPED_IN_SHUTDOWN).is_empty(),
            "a calibration that failed on its own should not stop the supervisor"
        );
        assert_eq!(
            calibration_count(&metrics, "model-a", "cancelled"),
            0,
            "a calibration that failed on its own should be recorded as an error"
        );

        within("model lifecycle shutdown", lifecycle.shutdown()).await;
        upstream.shutdown().await;
    }

    /// Where the supervisor finds its stats collector gone. Each path ends the
    /// supervisor's run with `StatsCollectorStopped`.
    #[derive(Clone, Copy, Debug)]
    enum CollectorStopPath {
        /// A discovery poll drops model-c and adds model-b. The retire request
        /// for model-c fails first, and then the begin for model-b.
        Discovery,
        /// model-b begins before the collector ends, and the snapshot that
        /// would admit it fails.
        Admit,
        /// model-b's calibration, which has no fallback input TPS, asks the
        /// collector for its snapshot, and that fails (R3a).
        Calibration,
        /// Like `Calibration`, with a fallback input TPS that the supervisor
        /// must not fall back to.
        CalibrationWithFallback,
    }

    /// How long a test watches a supervisor that should keep waiting for its
    /// stop. A supervisor that does not wait retires its generations and
    /// exits as soon as it runs again.
    const SUPERVISOR_WAIT_WATCH: Duration = Duration::from_millis(300);

    /// A supervisor that a stats collector panic has stopped, with model-a
    /// admitted before the panic.
    struct SupervisorStoppedByCollector {
        label: String,
        lifecycle: crate::ModelLifecycleHandle,
        runtime_state: PylonRuntimeState,
        metrics: Arc<PylonMetrics>,
        upstream: TestUpstream,
        subscriber: RecordingTracingSubscriber,
        /// Keeps `subscriber` recording until the test drops it.
        default_guard: tracing::dispatcher::DefaultGuard,
    }

    /// Panics the stats collector and waits for it to end: a configured input
    /// TPS of zero breaks a stats initialization invariant.
    async fn panic_the_stats_collector(label: &str, mut stats: crate::StatsCollectorHandle) {
        let begun = within(
            "the request that panics the collector",
            stats.control().begin_generation(
                ModelGeneration::new("model-panics", 1),
                crate::stats::ModelStatsInitialization::ConfiguredInputTps { input_tps: 0.0 },
            ),
        )
        .await;
        assert!(
            begun.is_err(),
            "{label}: a configured input TPS of zero should panic the collector: {begun:?}"
        );
        let exit = within("the collector exit", stats.wait_for_exit()).await;
        assert!(
            matches!(&exit, Err(error) if error.is_panic()),
            "{label}: the collector should end with a panic: {exit:?}"
        );
    }

    /// Starts a model lifecycle that admits model-a, and model-c for
    /// `Discovery`. With `drain` set, it then marks a registration drain as
    /// started. It panics the stats collector and returns once the supervisor
    /// has found the collector gone through `path`.
    async fn stop_the_supervisor_with_a_collector_panic(
        path: CollectorStopPath,
        drain: bool,
    ) -> SupervisorStoppedByCollector {
        let label = format!("{path:?}, drain {drain}");
        let (initial_models, completion_behavior) = match path {
            CollectorStopPath::Discovery => {
                (&["model-a", "model-c"][..], CompletionBehavior::Pending)
            }
            CollectorStopPath::Admit => (&["model-a"][..], CompletionBehavior::Pending),
            // Each generation's calibration completes four requests and then
            // saturates, so model-a measures a rate at startup.
            CollectorStopPath::Calibration | CollectorStopPath::CalibrationWithFallback => (
                &["model-a"][..],
                CompletionBehavior::SaturateAfter {
                    completed_requests_per_generation: 4,
                },
            ),
        };
        let mut upstream = TestUpstream::spawn(initial_models, completion_behavior).await;
        let config = match path {
            CollectorStopPath::Discovery => discovery_config(&upstream.base_url),
            // The initialization attempt probes the upstream's health after
            // model-b's begin and before its admission. Active canaries stay
            // off, so the probe is the only health traffic.
            CollectorStopPath::Admit => ModelLifecycleConfig {
                bringup: BringupConfig {
                    enabled: true,
                    active_canary_interval: Duration::ZERO,
                    canary_timeout: Duration::from_secs(30),
                    ..BringupConfig::default()
                },
                ..discovery_config(&upstream.base_url)
            },
            CollectorStopPath::Calibration | CollectorStopPath::CalibrationWithFallback => {
                let fallback_input_tps = matches!(path, CollectorStopPath::CalibrationWithFallback)
                    .then_some(FALLBACK_INPUT_TPS);
                let mut config =
                    calibration_config_with_fallback(&upstream.base_url, fallback_input_tps);
                let ModelInitialization::Calibration(calibration) = &mut config.initialization
                else {
                    unreachable!("the test config calibrates")
                };
                // The test holds the attempt's health probe for as long as it
                // needs, without the probe timing out.
                calibration.health_timeout = Duration::from_secs(30);
                config
            }
        };
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let (runtime_state, stats) = start_test_stats();
        let lifecycle = within(
            "model lifecycle startup",
            start_model_lifecycle(config, runtime_state.clone(), &stats, Some(metrics.clone())),
        )
        .await
        .unwrap_or_else(|error| {
            panic!("{label}: startup should admit the initial models: {error}")
        });
        assert_eq!(
            runtime_state.advertised_model_ids(),
            initial_models,
            "{label}: startup should admit the initial models"
        );
        if drain {
            runtime_state.mark_drain_started();
        }
        let subscriber = RecordingTracingSubscriber::default();
        let default_guard =
            tracing::dispatcher::set_default(&tracing::Dispatch::new(subscriber.clone()));

        match path {
            CollectorStopPath::Discovery => {
                panic_the_stats_collector(&label, stats).await;
                upstream.set_models(&["model-a", "model-b"]).await;
            }
            CollectorStopPath::Admit
            | CollectorStopPath::Calibration
            | CollectorStopPath::CalibrationWithFallback => {
                // model-b's attempt waits in its health probe until the
                // collector has ended, so its next collector request, the
                // admission's snapshot or the calibration's, finds it gone.
                while upstream.calibrations.try_recv().is_ok() {}
                upstream.hold_health();
                upstream.set_models(&["model-a", "model-b"]).await;
                upstream.wait_for_held_health_probe().await;
                panic_the_stats_collector(&label, stats).await;
                upstream.release_health();
            }
        }
        wait_for(
            "the supervisor should find its stats collector gone",
            || lifecycle.stopped_by_stats_collector(),
        )
        .await;
        SupervisorStoppedByCollector {
            label,
            lifecycle,
            runtime_state,
            metrics,
            upstream,
            subscriber,
            default_guard,
        }
    }

    /// Checks the log lines of a supervisor that a stats collector panic
    /// stopped: one ERROR for the stop (R1a), and for a calibration path, one
    /// WARN and one `error` outcome for model-b's calibration, which the
    /// supervisor neither tries again nor falls back from (R3a).
    fn assert_a_collector_panic_stopped_the_supervisor(
        run: &SupervisorStoppedByCollector,
        path: CollectorStopPath,
    ) {
        let label = &run.label;
        let events = run.subscriber.events();
        let failed = events_with_message(&events, SUPERVISOR_FAILED);
        assert_eq!(
            failed.len(),
            1,
            "{label}: the supervisor should report the collector panic once"
        );
        assert_eq!(failed[0].level, tracing::Level::ERROR, "{label}");
        assert_tracing_event_field(failed[0], "error", STATS_COLLECTOR_STOPPED);
        let errors = error_events(&events);
        assert_eq!(
            errors.len(),
            1,
            "{label}: the supervisor's report should be the only error: {errors:?}"
        );
        assert!(
            events_with_message(&events, SUPERVISOR_STOPPED_IN_SHUTDOWN).is_empty(),
            "{label}: a collector panic is not part of a shutdown"
        );
        if !matches!(
            path,
            CollectorStopPath::Calibration | CollectorStopPath::CalibrationWithFallback
        ) {
            return;
        }
        let warnings = events_with_message(&events, CALIBRATION_COLLECTOR_STOPPED_WARNING);
        assert_eq!(
            warnings.len(),
            1,
            "{label}: model-b's calibration should warn once that the collector stopped"
        );
        assert_eq!(warnings[0].level, tracing::Level::WARN, "{label}");
        assert_tracing_event_field(warnings[0], "model_id", "model-b");
        assert_tracing_event_field(warnings[0], "error", CALIBRATION_COLLECTOR_STOPPED);
        let retries = calibration_retry_warnings(&events);
        assert!(
            retries.is_empty(),
            "{label}: the supervisor should not try model-b again: {retries:?}"
        );
        assert_eq!(
            calibration_count(&run.metrics, "model-b", "error"),
            1,
            "{label}: model-b's calibration should be recorded once as an error"
        );
        assert_eq!(
            calibration_count(&run.metrics, "model-b", "cancelled"),
            0,
            "{label}: model-b's calibration should not be recorded as cancelled"
        );
    }

    /// During a drain, the supervisor that the collector stopped keeps model-a
    /// admitted until its own stop, and retires it then.
    async fn assert_a_drain_keeps_model_a_until_the_supervisor_stop(path: CollectorStopPath) {
        let mut run = stop_the_supervisor_with_a_collector_panic(path, true).await;
        let label = run.label.clone();
        while run.upstream.calibrations.try_recv().is_ok() {}

        let exit = tokio::time::timeout(SUPERVISOR_WAIT_WATCH, run.lifecycle.wait_for_exit()).await;
        assert!(
            exit.is_err(),
            "{label}: during a drain the supervisor should keep its generations until its stop \
             instead of exiting: {exit:?}"
        );
        assert_eq!(
            run.runtime_state.advertised_model_ids(),
            ["model-a"],
            "{label}: model-a should stay advertised through the drain"
        );
        assert!(
            matches!(
                run.runtime_state.request_generation_admission("model-a"),
                crate::runtime_state::RequestGenerationAdmission::Admitted(_)
            ),
            "{label}: requests for model-a should still be admitted during the drain"
        );
        if matches!(path, CollectorStopPath::Discovery) {
            assert!(
                run.runtime_state.current_generation("model-c").is_none(),
                "{label}: model-c left discovery, so its generation retires although the \
                 collector could not retire its stats"
            );
        }
        assert!(
            run.upstream.calibrations.try_recv().is_err(),
            "{label}: the waiting supervisor should not calibrate anything"
        );
        assert_a_collector_panic_stopped_the_supervisor(&run, path);

        let SupervisorStoppedByCollector {
            lifecycle,
            runtime_state,
            upstream,
            default_guard,
            ..
        } = run;
        within("the supervisor's stop", lifecycle.shutdown()).await;
        drop(default_guard);
        assert!(
            runtime_state.advertised_model_ids().is_empty()
                && runtime_state.current_generation("model-a").is_none(),
            "{label}: the supervisor's stop should retire model-a"
        );
        upstream.shutdown().await;
    }

    /// Outside a drain, the supervisor that the collector stopped retires
    /// model-a as it stops, without waiting for its own stop.
    async fn assert_no_drain_retires_model_a_at_once(path: CollectorStopPath) {
        let mut run = stop_the_supervisor_with_a_collector_panic(path, false).await;
        let label = run.label.clone();

        within(
            "the supervisor's exit without its stop",
            run.lifecycle.wait_for_exit(),
        )
        .await
        .expect("model lifecycle supervisor should not panic");
        assert!(
            run.runtime_state.advertised_model_ids().is_empty(),
            "{label}: outside a drain the supervisor should retire model-a as it stops"
        );
        assert_eq!(
            run.runtime_state.request_generation_admission("model-a"),
            crate::runtime_state::RequestGenerationAdmission::Unavailable,
            "{label}: requests for model-a should no longer be admitted"
        );
        assert_a_collector_panic_stopped_the_supervisor(&run, path);

        let SupervisorStoppedByCollector {
            lifecycle,
            upstream,
            default_guard,
            ..
        } = run;
        drop(default_guard);
        within("model lifecycle shutdown", lifecycle.shutdown()).await;
        upstream.shutdown().await;
    }

    #[tokio::test]
    async fn collector_end_found_by_discovery_during_a_drain_keeps_admitted_models_until_the_supervisor_stop()
     {
        assert_a_drain_keeps_model_a_until_the_supervisor_stop(CollectorStopPath::Discovery).await;
    }

    #[tokio::test]
    async fn collector_end_found_by_admission_during_a_drain_keeps_admitted_models_until_the_supervisor_stop()
     {
        assert_a_drain_keeps_model_a_until_the_supervisor_stop(CollectorStopPath::Admit).await;
    }

    #[tokio::test]
    async fn collector_end_found_by_calibration_during_a_drain_keeps_admitted_models_until_the_supervisor_stop()
     {
        assert_a_drain_keeps_model_a_until_the_supervisor_stop(CollectorStopPath::Calibration)
            .await;
    }

    #[tokio::test]
    async fn collector_end_found_by_fallback_calibration_during_a_drain_keeps_admitted_models_until_the_supervisor_stop()
     {
        assert_a_drain_keeps_model_a_until_the_supervisor_stop(
            CollectorStopPath::CalibrationWithFallback,
        )
        .await;
    }

    #[tokio::test]
    async fn collector_end_found_by_discovery_outside_a_drain_retires_admitted_models_at_once() {
        assert_no_drain_retires_model_a_at_once(CollectorStopPath::Discovery).await;
    }

    #[tokio::test]
    async fn collector_end_found_by_admission_outside_a_drain_retires_admitted_models_at_once() {
        assert_no_drain_retires_model_a_at_once(CollectorStopPath::Admit).await;
    }

    #[tokio::test]
    async fn collector_end_found_by_calibration_outside_a_drain_retires_admitted_models_at_once() {
        assert_no_drain_retires_model_a_at_once(CollectorStopPath::Calibration).await;
    }

    #[tokio::test]
    async fn collector_end_found_by_fallback_calibration_outside_a_drain_retires_admitted_models_at_once()
     {
        assert_no_drain_retires_model_a_at_once(CollectorStopPath::CalibrationWithFallback).await;
    }

    #[tokio::test]
    async fn collector_shutdown_during_a_drain_logs_no_error_and_waits_for_the_supervisor_stop() {
        let kv_cache = BlockingKvCacheServer::spawn().await;
        let upstream = TestUpstream::spawn(&["model-a"], CompletionBehavior::Pending).await;
        let (runtime_state, stats) = start_holdable_stats(&kv_cache);
        let mut lifecycle = within(
            "model lifecycle startup",
            start_model_lifecycle(
                discovery_config(&upstream.base_url),
                runtime_state.clone(),
                &stats,
                None,
            ),
        )
        .await
        .expect("initial discovery should initialize");
        runtime_state.mark_drain_started();
        let subscriber = RecordingTracingSubscriber::default();
        let default_guard =
            tracing::dispatcher::set_default(&tracing::Dispatch::new(subscriber.clone()));
        hold_stats_collector(&kv_cache).await;

        // A newly discovered model makes the supervisor begin a generation,
        // and the held collector leaves that request queued until its
        // shutdown ends it.
        upstream.set_models(&["model-a", "model-b"]).await;
        wait_for(
            "the supervisor should begin a generation for model-b",
            || runtime_state.current_generation("model-b").is_some(),
        )
        .await;
        within("stats collector shutdown", stats.shutdown()).await;
        wait_for(
            "the supervisor should stop with the collector's shutdown",
            || subscriber.event_count(SUPERVISOR_STOPPED_IN_SHUTDOWN) > 0,
        )
        .await;

        let exit = tokio::time::timeout(SUPERVISOR_WAIT_WATCH, lifecycle.wait_for_exit()).await;
        assert!(
            exit.is_err(),
            "during a drain the supervisor should keep its generations until its stop instead \
             of exiting: {exit:?}"
        );
        assert_eq!(
            runtime_state.advertised_model_ids(),
            ["model-a"],
            "model-a should stay advertised through the drain"
        );
        assert!(
            !lifecycle.stopped_by_stats_collector(),
            "a collector shutdown is not the collector ending on its own"
        );
        within("the supervisor's stop", lifecycle.shutdown()).await;
        drop(default_guard);
        assert!(
            runtime_state.advertised_model_ids().is_empty(),
            "the supervisor's stop should retire model-a"
        );

        let events = subscriber.events();
        let errors = error_events(&events);
        assert!(
            errors.is_empty(),
            "a stats collector shutdown during a drain should log no error: {errors:?}"
        );
        let stopped = events_with_message(&events, SUPERVISOR_STOPPED_IN_SHUTDOWN);
        assert_eq!(stopped.len(), 1);
        assert_eq!(stopped[0].level, tracing::Level::DEBUG);
        assert_tracing_event_field(stopped[0], "error", STATS_COLLECTOR_STOPPED);

        kv_cache.unblock();
        kv_cache.shutdown().await;
        upstream.shutdown().await;
    }
}
