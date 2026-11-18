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
package com.nvidia.nvcf.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class TerminationGracePeriodPropertiesTest {

    @Test
    void defaultsAndOverrides() {
        var defaults = new TerminationGracePeriodProperties();
        defaults.validate();
        assertThat(defaults.getMin()).isEqualTo(Duration.ofMinutes(1));
        assertThat(defaults.getMax()).isEqualTo(Duration.ofMinutes(5));

        var source = new MapConfigurationPropertySource(Map.of(
                "nvcf.llm.termination-grace-period.min", "2m",
                "nvcf.llm.termination-grace-period.max", "6m"));
        var overridden = new TerminationGracePeriodProperties();
        new Binder(source).bind("nvcf.llm.termination-grace-period",
                                Bindable.ofInstance(overridden));
        overridden.validate();
        assertThat(overridden.getMin()).isEqualTo(Duration.ofMinutes(2));
        assertThat(overridden.getMax()).isEqualTo(Duration.ofMinutes(6));
    }

    @Test
    void rejectsInvalidBounds() {
        var properties = new TerminationGracePeriodProperties();
        properties.setMin(Duration.ZERO);
        assertThatThrownBy(properties::validate).isInstanceOf(IllegalStateException.class);
        properties.setMin(Duration.ofMinutes(6));
        assertThatThrownBy(properties::validate).isInstanceOf(IllegalStateException.class);
    }
}
