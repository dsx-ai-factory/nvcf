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
package com.nvidia.nvct.service.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.nvidia.boot.registries.service.registry.RegistryMapperService;
import com.nvidia.nvct.persistence.task.entity.TaskEntity;
import com.nvidia.nvct.service.account.AccountService;
import com.nvidia.nvct.service.ess.EssService;
import com.nvidia.nvct.service.registry.dto.DockerConfigJsonAuthDto;
import com.nvidia.nvct.service.registry.dto.DockerConfigJsonDto;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.autoconfigure.ConfigurationPropertiesRebinderAutoConfiguration;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.cloud.context.refresh.ContextRefresher;
import org.springframework.core.env.MapPropertySource;
import tools.jackson.databind.json.JsonMapper;

// Drives a real ContextRefresher.refresh(), so this fails if the @RefreshScope is dropped.
class RegistryCredentialServiceRefreshTest {

    private static final String SECRET_KEY = "nvct.sidecars.image-pull-secret";
    private static final String HOSTNAME_KEY = "nvct.sidecars.hostname";
    private static final String HOSTNAME = "registry.example.com";
    private static final String INITIAL_SECRET = "initial-sidecar-pull-secret";
    private static final String UPDATED_SECRET = "updated-sidecar-pull-secret";

    private static final String OVERRIDES_SOURCE = "sidecar-secret-overrides";
    private static final JsonMapper JSON_MAPPER = JsonMapper.builder().build();

    @Test
    void updatedSidecarImagePullSecretAppliesWithoutRestart() {
        // The property source below wraps this map, so mutating it changes the environment.
        var overrides = new HashMap<String, Object>();
        overrides.put("spring.application.name", "nvct-api");
        overrides.put("spring.profiles.active", "test");
        overrides.put(SECRET_KEY, INITIAL_SECRET);
        overrides.put(HOSTNAME_KEY, HOSTNAME);

        runner(overrides).run(context -> {
            assertThat(sidecarAuths(context))
                    .containsOnlyKeys(HOSTNAME)
                    .hasEntrySatisfying(HOSTNAME, auth ->
                            assertThat(auth.auth()).isEqualTo(INITIAL_SECRET));

            overrides.put(SECRET_KEY, UPDATED_SECRET);
            context.getBean(ContextRefresher.class).refresh();

            assertThat(sidecarAuths(context))
                    .containsOnlyKeys(HOSTNAME)
                    .hasEntrySatisfying(HOSTNAME, auth ->
                            assertThat(auth.auth()).isEqualTo(UPDATED_SECRET));
        });
    }

    private static ApplicationContextRunner runner(Map<String, Object> overrides) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        RefreshAutoConfiguration.class,
                        ConfigurationPropertiesRebinderAutoConfiguration.class))
                .withUserConfiguration(RegistryCredentialService.class)
                .withBean(AccountService.class, () -> mock(AccountService.class))
                .withBean(RegistryMapperService.class, () -> mock(RegistryMapperService.class))
                .withBean(RegistryTaskMapperService.class,
                          () -> mock(RegistryTaskMapperService.class))
                .withBean(EssService.class, () -> mock(EssService.class))
                .withBean(JsonMapper.class, () -> JSON_MAPPER)
                .withInitializer(context -> context.getEnvironment().getPropertySources()
                        .addFirst(new MapPropertySource(OVERRIDES_SOURCE, overrides)))
                // copyEnvironment() keeps only default sources, so the overrides must be retained.
                .withPropertyValues("spring.cloud.refresh.additional-property-sources-to-retain="
                        + OVERRIDES_SOURCE);
    }

    private static Map<String, DockerConfigJsonAuthDto> sidecarAuths(
            AssertableApplicationContext context) {
        var task = new TaskEntity();
        task.setTaskId(UUID.randomUUID());
        task.setNcaId("test-nca-id");
        var encoded = context.getBean(RegistryCredentialService.class)
                .getBase64EncodedSidecarRegistryImagePullSecret(task);
        var json = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        return JSON_MAPPER.readValue(json, DockerConfigJsonDto.class).auths();
    }
}
