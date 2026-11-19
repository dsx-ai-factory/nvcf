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

package com.nvidia.apikeys.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.nvidia.apikeys.config.KeysByAccountIndexMigrationProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import net.javacrumbs.shedlock.core.DefaultLockingTaskExecutor;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.event.ApplicationReadyEvent;

class ScheduledTaskServiceTest {
    private final KeysByAccountIndexMigrationTask task = mock(KeysByAccountIndexMigrationTask.class);
    private final ScheduledTaskService scheduler = new ScheduledTaskService(
            task, new KeysByAccountIndexMigrationProperties());

    @Test
    void waitsForApplicationReadinessAndAllowsSubsequentRuns() throws Exception {
        var acquired = new CountDownLatch(1);
        var executor = lockExecutor(acquired);
        var worker = Executors.newSingleThreadExecutor();
        Runnable invoke = () -> {
            try {
                executor.executeWithLock((LockingTaskExecutor.Task) scheduler::keysByAccountIndexCopy, lockConfig());
            } catch (Throwable failure) {
                throw new CompletionException(failure);
            }
        };
        try {
            var first = worker.submit(invoke);
            assertThat(acquired.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> first.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            verifyNoInteractions(task);

            scheduler.onApplicationEvent(mock(ApplicationReadyEvent.class));

            first.get(5, TimeUnit.SECONDS);
            worker.submit(invoke).get(5, TimeUnit.SECONDS);
            verify(task, times(2)).run(any());
        } finally {
            worker.shutdownNow();
            assertThat(worker.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void interruptionBeforeReadinessDoesNotRunMigration() throws Exception {
        var acquired = new CountDownLatch(1);
        var failure = new CompletableFuture<Throwable>();
        var executor = lockExecutor(acquired);
        var worker = Executors.newSingleThreadExecutor();
        try {
            worker.submit(() -> {
                try {
                    executor.executeWithLock((LockingTaskExecutor.Task) scheduler::keysByAccountIndexCopy, lockConfig());
                    failure.complete(null);
                } catch (Throwable caught) {
                    failure.complete(caught);
                }
            });
            assertThat(acquired.await(5, TimeUnit.SECONDS)).isTrue();

            worker.shutdownNow();

            assertThat(failure.get(5, TimeUnit.SECONDS)).isInstanceOf(InterruptedException.class);
            verifyNoInteractions(task);
        } finally {
            worker.shutdownNow();
            assertThat(worker.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private DefaultLockingTaskExecutor lockExecutor(CountDownLatch acquired) {
        return new DefaultLockingTaskExecutor(config -> {
            acquired.countDown();
            return Optional.of(() -> { });
        });
    }

    private LockConfiguration lockConfig() {
        return new LockConfiguration(Instant.now(), "keys-by-account-index-copy", Duration.ofMinutes(5), Duration.ZERO);
    }
}
