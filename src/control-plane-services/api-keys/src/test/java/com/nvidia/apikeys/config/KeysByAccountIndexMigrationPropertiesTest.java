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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.nvidia.apikeys.scheduler.KeysByAccountIndexMigrationTask;
import com.nvidia.apikeys.scheduler.ScheduledTaskService;
import jakarta.validation.Validation;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import net.javacrumbs.shedlock.core.LockProvider;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.UnboundConfigurationPropertiesException;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.cloud.context.scope.refresh.RefreshScope;
import org.springframework.core.env.MapPropertySource;
import org.springframework.scheduling.config.ScheduledTaskHolder;

class KeysByAccountIndexMigrationPropertiesTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(AopAutoConfiguration.class,
                    ConfigurationPropertiesAutoConfiguration.class, RefreshAutoConfiguration.class))
            .withUserConfiguration(KeysByAccountIndexMigrationProperties.class);

    @Test
    void bindsOperatorSettingsAndKeepsSafeDefaults() {
        var binder = new Binder(new MapConfigurationPropertySource(Map.of(
                "account-index-migration.page-size", "17",
                "account-index-migration.fixed-delay", "2m",
                "account-index-migration.lock-consistency", "QUORUM",
                "account-index-migration.lock-serial-consistency", "SERIAL")));
        var config = binder.bind("account-index-migration", Bindable.of(KeysByAccountIndexMigrationProperties.class))
                .orElseThrow(IllegalStateException::new);
        assertThat(config.getPageSize()).isEqualTo(17);
        assertThat(config.getFixedDelay()).isEqualTo(Duration.ofMinutes(2));
        assertThat(config.getLockConsistency()).isEqualTo(DefaultConsistencyLevel.QUORUM);
        assertThat(config.getLockSerialConsistency()).isEqualTo(DefaultConsistencyLevel.SERIAL);
        assertThat(config.isEnabled()).isTrue();
        assertThat(config.getInitialDelay()).isEqualTo(Duration.ofMinutes(30));
        assertThat(config.getOrphanMinAge()).isEqualTo(Duration.ofMinutes(5));
        assertValid(config);
    }

    @Test
    void rejectsUnsafeDelaysLockDurationsAndRetryLimits() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            var config = new KeysByAccountIndexMigrationProperties();
            config.setOrphanMinAge(Duration.ZERO);
            assertThat(validator.validate(config)).isNotEmpty();
            config.setOrphanMinAge(Duration.ofMinutes(5));
            config.setSettleDelay(Duration.ofSeconds(-1));
            assertThat(validator.validate(config)).isNotEmpty();
            config.setSettleDelay(Duration.ofMinutes(5));
            config.setLockAtMostFor(config.getLockAtLeastFor());
            assertThat(validator.validate(config)).isNotEmpty();
            config.setLockAtMostFor(Duration.ofMinutes(5));
            config.setPageFetchAttempts(0);
            assertThat(validator.validate(config)).isNotEmpty();
            config.setPageFetchAttempts(3);
            config.setLockSerialConsistency(DefaultConsistencyLevel.ONE);
            assertThat(validator.validate(config)).isNotEmpty();
        }
    }

    @Test
    void standaloneSettingsRefreshThroughInjectedBean() {
        var values = new HashMap<String, Object>();
        values.put("account-index-migration.page-size", "17");
        contextRunner.withInitializer(context -> context.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("migration-test", values))).run(context -> {
                    assertThat(context).hasNotFailed();
                    var config = context.getBean(KeysByAccountIndexMigrationProperties.class);
                    assertThat(config.getPageSize()).isEqualTo(17);

                    values.put("account-index-migration.page-size", "23");
                    context.getBean(RefreshScope.class).refreshAll();

                    assertThat(config.getPageSize()).isEqualTo(23);
                });
    }

    @Test
    void invalidBoundSettingsFailStartup() {
        contextRunner.withPropertyValues("account-index-migration.page-size=0").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class);
        });
    }

    @Test
    void unknownMigrationSettingsFailStartup() {
        contextRunner.withPropertyValues("account-index-migration.page-szie=17").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(UnboundConfigurationPropertiesException.class);
        });
    }

    @Test
    void disabledSchedulerKeepsLockProviderAvailable() {
        contextRunner.withUserConfiguration(DistributedLockConfiguration.class,
                        ScheduledTaskService.class)
                .withBean(CqlSession.class, () -> mock(CqlSession.class))
                .withPropertyValues("apikeys.scheduler.enabled=false", "account-index-migration.enabled=true").run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(KeysByAccountIndexMigrationProperties.class).isEnabled()).isTrue();
                    assertThat(context).doesNotHaveBean(ScheduledTaskService.class)
                            .hasSingleBean(LockProvider.class);
                });
    }

    @Test
    void enabledSchedulerRegistersScheduledJob() {
        contextRunner.withUserConfiguration(DistributedLockConfiguration.class,
                        ScheduledTaskService.class)
                .withBean(CqlSession.class, () -> mock(CqlSession.class))
                .withBean(KeysByAccountIndexMigrationTask.class, () -> mock(KeysByAccountIndexMigrationTask.class))
                .withPropertyValues("apikeys.scheduler.enabled=true").run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ScheduledTaskService.class)
                            .hasSingleBean(LockProvider.class);
                    assertThat(context.getBean(ScheduledTaskHolder.class).getScheduledTasks()).hasSize(1);
                });
    }

    @Test
    void disabledMigrationKeepsDefaultSchedulerRegistered() {
        contextRunner.withUserConfiguration(DistributedLockConfiguration.class, ScheduledTaskService.class)
                .withBean(CqlSession.class, () -> mock(CqlSession.class))
                .withBean(KeysByAccountIndexMigrationTask.class, () -> mock(KeysByAccountIndexMigrationTask.class))
                .withPropertyValues("account-index-migration.enabled=false").run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(KeysByAccountIndexMigrationProperties.class).isEnabled()).isFalse();
                    assertThat(context).hasSingleBean(ScheduledTaskService.class).hasSingleBean(LockProvider.class);
                    assertThat(context.getBean(ScheduledTaskHolder.class).getScheduledTasks()).hasSize(1);
                });
    }

    @Test
    void serviceSchedulerFlagDefaultsToEnabledAndBindsOverride() {
        serviceContext().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(NakProperties.class).getScheduler().isEnabled()).isTrue();
        });
        serviceContext().withPropertyValues("apikeys.scheduler.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(NakProperties.class).getScheduler().isEnabled()).isFalse();
        });
    }

    @Test
    void standaloneMigrationSettingsCoexistWithStrictServiceSettings() {
        serviceContext().withPropertyValues("account-index-migration.page-size=17").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(NakProperties.class).getNcaId()).isEqualTo("test-account");
            assertThat(context.getBean(KeysByAccountIndexMigrationProperties.class).getPageSize()).isEqualTo(17);
        });
        serviceContext().withPropertyValues("apikeys.unknown-setting=unexpected").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(UnboundConfigurationPropertiesException.class);
        });
    }

    private ApplicationContextRunner serviceContext() {
        return contextRunner.withUserConfiguration(NakProperties.class).withPropertyValues(
                "apikeys.nca-id=test-account", "apikeys.registrations=[]", "apikeys.service-id-map.nvcf=cloud-functions",
                "apikeys.key-prefix=test-", "apikeys.data-domain-key=test", "apikeys.keep-after-expired-duration=P2D",
                "apikeys.jwks.private-jwks=test", "apikeys.jwks.jwe-key-mapping.payload_jwe_kid=test",
                "apikeys.encrypted-model-converter.base-package=com.nvidia.apikeys");
    }

    private void assertValid(KeysByAccountIndexMigrationProperties config) {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            assertThat(factory.getValidator().validate(config)).isEmpty();
        }
    }
}
