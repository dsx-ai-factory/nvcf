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
package com.nvidia.boot.cassandra.configuration;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

// Restarts a pod whose Cassandra session is stuck.
//
// After a full Cassandra outage, a driver session can stay broken after Cassandra is back: it has
// no connections and cannot open new ones, and only a new session recovers it. This check reports
// DOWN when all of the following hold for the whole grace period:
//   - the application's health query fails,
//   - the application session has no pool connections, and
//   - a new session can query Cassandra.
// Otherwise it reports UP, because a restart would not help: the session is busy (it still has
// connections), its pools cannot be read, or Cassandra itself is down. A session whose connections
// keep opening and failing is not restarted, because each open connection resets the timer.
@Slf4j
public class CassandraSessionLivenessIndicator implements HealthIndicator, DisposableBean {

    private final BooleanSupplier sessionCheck;
    private final Supplier<CassandraSessionPools> pools;
    private final BooleanSupplier freshSessionCheck;
    private final Duration gracePeriod;
    private final Clock clock;
    // The check runs a query and may open a new session, which can take seconds. It runs on this
    // thread every check interval, and the liveness probe only reads the last result, so a slow
    // Cassandra cannot time out the probe and probes add no load on Cassandra.
    private final ScheduledExecutorService executor;

    private volatile Health health = Health.up().build();
    private Instant stuckSince;

    CassandraSessionLivenessIndicator(BooleanSupplier sessionCheck,
                                      Supplier<CassandraSessionPools> pools,
                                      BooleanSupplier freshSessionCheck,
                                      Duration gracePeriod, Clock clock) {
        this.sessionCheck = sessionCheck;
        this.pools = pools;
        this.freshSessionCheck = freshSessionCheck;
        this.gracePeriod = gracePeriod;
        this.clock = clock;
        this.executor = null;
    }

    CassandraSessionLivenessIndicator(BooleanSupplier sessionCheck,
                                      Supplier<CassandraSessionPools> pools,
                                      BooleanSupplier freshSessionCheck,
                                      Duration gracePeriod, Duration checkInterval) {
        this.sessionCheck = sessionCheck;
        this.pools = pools;
        this.freshSessionCheck = freshSessionCheck;
        this.gracePeriod = gracePeriod;
        this.clock = Clock.systemUTC();
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            var thread = new Thread(runnable, "cassandra-session-liveness");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(this::evaluateSafely, checkInterval.toMillis(),
                                        checkInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Health health() {
        return health;
    }

    private void evaluateSafely() {
        try {
            evaluate();
        } catch (RuntimeException e) {
            log.warn("Cassandra session liveness check failed unexpectedly", e);
        }
    }

    void evaluate() {
        if (sessionCheck.getAsBoolean()) {
            if (stuckSince != null) {
                log.info("Cassandra session recovered without a restart");
            }
            stayUp(Health.up());
            return;
        }
        // Reading the pools needs no network, so check them before building a new session.
        var snapshot = pools.get();
        log.info("Cassandra session health query failed; connection pools: {}", snapshot);
        if (!snapshot.allPoolsEmpty()) {
            stayUp(Health.up().withDetail("connectionPools",
                                          snapshot.complete() ? "connected" : "unknown"));
            return;
        }
        if (!freshSessionCheck.getAsBoolean()) {
            // Cassandra itself is unavailable. A restart cannot help, so stay live.
            stayUp(Health.up().withDetail("cassandra", "unavailable"));
            return;
        }
        var now = clock.instant();
        if (stuckSince == null) {
            stuckSince = now;
            log.warn("Cassandra session has no pool connections while a new session succeeds; "
                     + "restarting after {} if it does not recover", gracePeriod);
        }
        var stuckFor = Duration.between(stuckSince, now);
        if (stuckFor.compareTo(gracePeriod) >= 0) {
            log.error("Cassandra session has had no pool connections for {} while a new session "
                      + "succeeds; reporting liveness DOWN so the container restarts", stuckFor);
            health = Health.down().withDetail("stuckSince", stuckSince.toString()).build();
        } else {
            health = Health.up().withDetail("stuckSince", stuckSince.toString()).build();
        }
    }

    private void stayUp(Health.Builder builder) {
        stuckSince = null;
        health = builder.build();
    }

    @Override
    public void destroy() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
