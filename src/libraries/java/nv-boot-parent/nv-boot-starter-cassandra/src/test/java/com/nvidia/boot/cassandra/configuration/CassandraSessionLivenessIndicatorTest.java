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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;

class CassandraSessionLivenessIndicatorTest {

    private static final Duration GRACE = Duration.ofMinutes(3);
    private static final CassandraSessionPools EMPTY = pools(0);
    private static final CassandraSessionPools CONNECTED = pools(1);
    private static final CassandraSessionPools UNKNOWN =
            CassandraSessionPools.incomplete("unreadable");

    private final MutableClock clock = new MutableClock();
    private boolean sessionWorks;
    private CassandraSessionPools pools = EMPTY;
    private boolean freshSessionWorks;
    private int freshSessionChecks;
    private final CassandraSessionLivenessIndicator indicator =
            new CassandraSessionLivenessIndicator(
                    () -> sessionWorks, () -> pools,
                    () -> {
                        freshSessionChecks++;
                        return freshSessionWorks;
                    },
                    GRACE, clock);

    @Test
    void upWhenSessionWorks() {
        sessionWorks = true;
        indicator.evaluate();
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void staysUpForAnyLengthOfCassandraOutage() {
        sessionWorks = false;
        freshSessionWorks = false;
        for (int i = 0; i < 10; i++) {
            indicator.evaluate();
            clock.advance(Duration.ofMinutes(5));
        }
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void downOnlyAfterStuckForGracePeriod() {
        sessionWorks = false;
        freshSessionWorks = true;
        indicator.evaluate();
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);

        clock.advance(GRACE.minusSeconds(1));
        indicator.evaluate();
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);

        clock.advance(Duration.ofSeconds(1));
        indicator.evaluate();
        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    void recoversWhenSessionReconnects() {
        sessionWorks = false;
        freshSessionWorks = true;
        indicator.evaluate();
        clock.advance(GRACE);
        indicator.evaluate();
        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);

        sessionWorks = true;
        indicator.evaluate();
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void outageDuringStuckWindowRestartsTheGracePeriod() {
        sessionWorks = false;
        freshSessionWorks = true;
        indicator.evaluate();
        clock.advance(Duration.ofMinutes(2));

        freshSessionWorks = false;
        indicator.evaluate();
        clock.advance(Duration.ofMinutes(2));

        freshSessionWorks = true;
        indicator.evaluate();
        clock.advance(Duration.ofMinutes(2));
        indicator.evaluate();
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);

        clock.advance(GRACE.minusMinutes(2));
        indicator.evaluate();
        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
    }

    // An overloaded session fails its query but keeps its connections: never restart it, and do
    // not open a new session against Cassandra while the pod is already under load.
    @Test
    void sessionWithPoolConnectionsNeverRestarts() {
        sessionWorks = false;
        pools = CONNECTED;
        freshSessionWorks = true;
        for (int i = 0; i < 10; i++) {
            indicator.evaluate();
            clock.advance(GRACE);
        }
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
        assertThat(indicator.health().getDetails()).containsEntry("connectionPools", "connected");
        assertThat(freshSessionChecks).isZero();
    }

    @Test
    void unreadablePoolsNeverRestart() {
        sessionWorks = false;
        pools = UNKNOWN;
        freshSessionWorks = true;
        for (int i = 0; i < 10; i++) {
            indicator.evaluate();
            clock.advance(GRACE);
        }
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
        assertThat(indicator.health().getDetails()).containsEntry("connectionPools", "unknown");
    }

    @Test
    void connectionReturningRestartsTheGracePeriod() {
        assertGracePeriodRestartsAfterOneCheckWith(CONNECTED);
    }

    @Test
    void unreadablePoolsRestartTheGracePeriod() {
        assertGracePeriodRestartsAfterOneCheckWith(UNKNOWN);
    }

    private void assertGracePeriodRestartsAfterOneCheckWith(CassandraSessionPools interruption) {
        sessionWorks = false;
        freshSessionWorks = true;
        indicator.evaluate();
        clock.advance(Duration.ofMinutes(2));

        pools = interruption;
        indicator.evaluate();
        clock.advance(Duration.ofMinutes(2));

        pools = EMPTY;
        indicator.evaluate();
        clock.advance(GRACE.minusSeconds(1));
        indicator.evaluate();
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);

        clock.advance(Duration.ofSeconds(1));
        indicator.evaluate();
        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    void scheduledCheckReportsDownWhenSessionStaysStuck() throws InterruptedException {
        var scheduled = new CassandraSessionLivenessIndicator(
                () -> false, () -> EMPTY, () -> true, Duration.ZERO, Duration.ofMillis(10));
        try {
            var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (scheduled.health().getStatus() != Status.DOWN) {
                assertThat(System.nanoTime()).as("scheduled check reported DOWN")
                        .isLessThan(deadline);
                Thread.sleep(10);
            }
        } finally {
            scheduled.destroy();
        }
    }

    private static CassandraSessionPools pools(int poolSize) {
        return new CassandraSessionPools(null, List.of(new CassandraSessionPools.NodePool(
                "/10.0.0.1:9042", NodeState.UP, NodeDistance.LOCAL, true, true, poolSize + 1,
                poolSize, 0, poolSize * 1024, 0)));
    }

    private static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-01-01T00:00:00Z");

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
