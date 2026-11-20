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

package com.nvidia.apikeys.config;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Configuration
@RefreshScope
@Validated
@ConfigurationProperties(prefix = "apikeys.account-index-migration", ignoreUnknownFields = false)
public class KeysByAccountIndexMigrationProperties {
    @Builder.Default
    private boolean enabled = true;
    @NotNull
    @Builder.Default
    private Duration initialDelay = Duration.ofMinutes(30);
    @NotNull
    @Builder.Default
    private Duration fixedDelay = Duration.ofMinutes(5);
    @NotNull
    @Builder.Default
    private Duration settleDelay = Duration.ofMinutes(5);
    @Min(1)
    @Builder.Default
    private int pageSize = 500;
    @NotNull
    @Builder.Default
    private Duration batchDelay = Duration.ofMillis(200);
    @Min(1)
    @Builder.Default
    private int pageFetchAttempts = 3;
    @NotNull
    @Builder.Default
    private Duration orphanMinAge = Duration.ofMinutes(5);
    @NotNull
    @Builder.Default
    private Duration lockAtLeastFor = Duration.ofSeconds(1);
    @NotNull
    @Builder.Default
    private Duration lockAtMostFor = Duration.ofMinutes(5);
    @NotNull
    @Builder.Default
    private DefaultConsistencyLevel lockConsistency = DefaultConsistencyLevel.LOCAL_QUORUM;
    @NotNull
    @Builder.Default
    private DefaultConsistencyLevel lockSerialConsistency = DefaultConsistencyLevel.LOCAL_SERIAL;

    @AssertTrue(message = "migration delays must be nonnegative, with positive fixed, settle and orphan delays")
    public boolean isDelaysValid() {
        return nonnegative(initialDelay) && positive(fixedDelay) && positive(settleDelay)
                && nonnegative(batchDelay) && positive(orphanMinAge);
    }

    @AssertTrue(message = "lock-at-most-for must exceed lock-at-least-for and be at least 1 second")
    public boolean isLockDurationsValid() {
        return nonnegative(lockAtLeastFor) && lockAtMostFor != null
                && lockAtMostFor.compareTo(Duration.ofSeconds(1)) >= 0
                && lockAtMostFor.compareTo(lockAtLeastFor) > 0;
    }

    @AssertTrue(message = "lock consistency must use quorum reads and SERIAL or LOCAL_SERIAL")
    public boolean isLockConsistencyValid() {
        return (lockConsistency == DefaultConsistencyLevel.LOCAL_QUORUM
                || lockConsistency == DefaultConsistencyLevel.QUORUM
                || lockConsistency == DefaultConsistencyLevel.EACH_QUORUM
                || lockConsistency == DefaultConsistencyLevel.ALL)
                && (lockSerialConsistency == DefaultConsistencyLevel.LOCAL_SERIAL
                || lockSerialConsistency == DefaultConsistencyLevel.SERIAL);
    }

    private static boolean nonnegative(Duration duration) {
        return duration != null && !duration.isNegative();
    }

    private static boolean positive(Duration duration) {
        return nonnegative(duration) && duration.toMillis() > 0;
    }
}
