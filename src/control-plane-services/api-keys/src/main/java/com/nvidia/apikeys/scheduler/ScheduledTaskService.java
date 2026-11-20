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

import java.util.concurrent.CountDownLatch;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockAssert;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@EnableScheduling
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "apikeys.scheduler",
        name = "enabled", havingValue = "true", matchIfMissing = true)
public class ScheduledTaskService implements ApplicationListener<ApplicationReadyEvent> {
    private static final String KEYS_BY_ACCOUNT_INDEX_COPY = "keys-by-account-index-copy";
    private final CountDownLatch initialised = new CountDownLatch(1);
    private final KeysByAccountIndexMigrationTask task;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        initialised.countDown();
    }

    @Scheduled(initialDelayString = "${apikeys.account-index-migration.initial-delay:PT30M}",
            fixedDelayString = "${apikeys.account-index-migration.fixed-delay:PT5M}")
    @SchedulerLock(name = KEYS_BY_ACCOUNT_INDEX_COPY,
            lockAtLeastFor = "${apikeys.account-index-migration.lock-at-least-for:PT1S}",
            lockAtMostFor = "${apikeys.account-index-migration.lock-at-most-for:PT5M}")
    public void keysByAccountIndexCopy() throws InterruptedException {
        initialised.await();
        LockAssert.assertLocked();
        log.debug("Begin task: '{}'", KEYS_BY_ACCOUNT_INDEX_COPY);
        task.run();
        log.debug("End task: '{}'", KEYS_BY_ACCOUNT_INDEX_COPY);
    }
}
