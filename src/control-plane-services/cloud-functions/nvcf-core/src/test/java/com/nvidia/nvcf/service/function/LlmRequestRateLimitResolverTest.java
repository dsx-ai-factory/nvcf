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
package com.nvidia.nvcf.service.function;

import static org.assertj.core.api.Assertions.assertThat;

import com.nvidia.nvcf.persistence.function.entity.RateLimitUdt;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class LlmRequestRateLimitResolverTest {

    private static final String CALLER = "nca-caller";

    private static RateLimitUdt policy(String rate) {
        return RateLimitUdt.builder().rate(rate).build();
    }

    static Stream<Arguments> accountRateCases() {
        return Stream.of(
                Arguments.of("function rate applies",
                        policy("5-M"), Optional.of("5-M")),
                Arguments.of("windows come back in S, M, H, D order",
                        policy("1000-D, 300-H,5-S"), Optional.of("5-S,300-H,1000-D")),
                Arguments.of("malformed entries are dropped, valid ones kept",
                        policy("5-M,bad,0-S,7-W"), Optional.of("5-M")),
                Arguments.of("the stricter entry for one unit wins",
                        policy("10-M,4-M"), Optional.of("4-M")),
                Arguments.of("a count too large for a long is dropped",
                        policy("9223372036854775808-S,5-M"), Optional.of("5-M")),
                Arguments.of("no rate means no limit",
                        policy(null), Optional.empty()),
                Arguments.of("exempt caller gets no function rate",
                        RateLimitUdt.builder().rate("5-M").exemptedNcaIds(Set.of(CALLER)).build(),
                        Optional.empty()),
                Arguments.of("another account's exemption does not apply",
                        RateLimitUdt.builder().rate("5-M").exemptedNcaIds(Set.of("nca-other")).build(),
                        Optional.of("5-M")),
                Arguments.of("caller's override replaces the function rate",
                        RateLimitUdt.builder().rate("5-M").perNcaIdRate(Map.of(CALLER, "50-M")).build(),
                        Optional.of("50-M")),
                Arguments.of("an invalid override falls back to the function rate",
                        RateLimitUdt.builder().rate("5-M").perNcaIdRate(Map.of(CALLER, "bad")).build(),
                        Optional.of("5-M")),
                Arguments.of("a valid override wins over the caller's exemption",
                        RateLimitUdt.builder().rate("5-M").exemptedNcaIds(Set.of(CALLER))
                                .perNcaIdRate(Map.of(CALLER, "50-M")).build(),
                        Optional.of("50-M")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("accountRateCases")
    void resolvesTheAccountRateLikeTheRateLimiter(String name, RateLimitUdt policy, Optional<String> expected) {
        var resolved = LlmRequestRateLimitResolver.resolve(List.of(policy), CALLER);

        assertThat(resolved.accountRate()).isEqualTo(expected);
    }

    @Test
    void exemptCallerStillGetsTheUserRate() {
        var policy = RateLimitUdt.builder().rate("5-M").exemptedNcaIds(Set.of(CALLER)).perUserRate("2-S").build();

        var resolved = LlmRequestRateLimitResolver.resolve(List.of(policy), CALLER);

        assertThat(resolved.accountRate()).isEmpty();
        assertThat(resolved.userRate()).contains("2-S");
    }

    @Test
    void takesTheStrictestRatePerUnitAcrossVersions() {
        var versionA = RateLimitUdt.builder().rate("5-M,1000-D").perUserRate("3-M").build();
        var versionB = RateLimitUdt.builder().rate("3-M,100-H").perUserRate("9-M").build();

        var resolved = LlmRequestRateLimitResolver.resolve(List.of(versionA, versionB), CALLER);

        assertThat(resolved.accountRate()).contains("3-M,100-H,1000-D");
        assertThat(resolved.userRate()).contains("3-M");
    }

    @Test
    void versionsWithoutAPolicyAddNoLimit() {
        var resolved = LlmRequestRateLimitResolver.resolve(Arrays.asList(null, policy("5-M")), CALLER);

        assertThat(resolved.accountRate()).contains("5-M");
        assertThat(resolved.userRate()).isEmpty();
    }

    @Test
    void noPoliciesMeansNoLimits() {
        var resolved = LlmRequestRateLimitResolver.resolve(List.of(), CALLER);

        assertThat(resolved.accountRate()).isEmpty();
        assertThat(resolved.userRate()).isEmpty();
    }
}
