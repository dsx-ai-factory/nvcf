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

import com.nvidia.nvcf.persistence.function.entity.RateLimitUdt;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Resolves the request rate limits an LLM function applies to one caller, for the LLM gateway to
 * enforce. It follows the rate limiter service's rules: a valid per-account override replaces the
 * function rate for that account, an exempted account skips the function rate, and the per-user
 * rate applies to every caller. Rates parse leniently: malformed entries are dropped and the
 * stricter of two entries for one unit wins. Across the function's serving versions the strictest
 * rate per unit applies, so the result is never looser than any version's policy.
 */
public final class LlmRequestRateLimitResolver {

    private static final Pattern RATE_ENTRY = Pattern.compile("^([1-9]\\d*)-([SMHD])$");
    private static final String UNIT_ORDER = "SMHD";
    private static final Comparator<Character> BY_UNIT = Comparator.comparingInt(UNIT_ORDER::indexOf);

    /**
     * The resolved rates, each a comma-separated list such as "5-M" or "5-S,300-H", or empty when
     * no limit applies.
     */
    public record ResolvedRequestRateLimits(Optional<String> accountRate, Optional<String> userRate) {
    }

    private LlmRequestRateLimitResolver() {
    }

    /**
     * @param policies the rate limit policy of each serving version; null entries are skipped
     * @param ncaId    the account the request counts against (the key owner's account)
     */
    public static ResolvedRequestRateLimits resolve(Collection<RateLimitUdt> policies, String ncaId) {
        var account = new TreeMap<Character, Long>(BY_UNIT);
        var user = new TreeMap<Character, Long>(BY_UNIT);
        for (var policy : policies) {
            if (policy == null) {
                continue;
            }
            mergeStrictest(account, accountRate(policy, ncaId));
            mergeStrictest(user, parse(policy.getPerUserRate()));
        }
        return new ResolvedRequestRateLimits(format(account), format(user));
    }

    private static Map<Character, Long> accountRate(RateLimitUdt policy, String ncaId) {
        if (ncaId != null && policy.getPerNcaIdRate() != null) {
            var override = parse(policy.getPerNcaIdRate().get(ncaId));
            if (!override.isEmpty()) {
                return override;
            }
        }
        if (ncaId != null && policy.getExemptedNcaIds() != null
                && policy.getExemptedNcaIds().contains(ncaId)) {
            return Map.of();
        }
        return parse(policy.getRate());
    }

    static Map<Character, Long> parse(String raw) {
        var rates = new TreeMap<Character, Long>(BY_UNIT);
        if (raw == null || raw.isBlank()) {
            return rates;
        }
        for (var part : raw.split(",")) {
            var matcher = RATE_ENTRY.matcher(part.trim());
            if (!matcher.matches()) {
                continue;
            }
            final long count;
            try {
                count = Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                continue;
            }
            rates.merge(matcher.group(2).charAt(0), count, Math::min);
        }
        return rates;
    }

    private static void mergeStrictest(Map<Character, Long> into, Map<Character, Long> rates) {
        rates.forEach((unit, count) -> into.merge(unit, count, Math::min));
    }

    private static Optional<String> format(Map<Character, Long> rates) {
        if (rates.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(rates.entrySet().stream()
                .map(e -> e.getValue() + "-" + e.getKey())
                .collect(Collectors.joining(",")));
    }
}
