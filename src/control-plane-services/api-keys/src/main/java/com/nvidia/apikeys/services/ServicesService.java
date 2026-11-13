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

package com.nvidia.apikeys.services;

import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import com.nvidia.apikeys.config.NakProperties;
import com.nvidia.apikeys.utils.JsonUtils;
import com.nvidia.apikeys.vo.ServiceVo;
import com.nvidia.boot.exceptions.NotFoundException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RefreshScope
public class ServicesService {

    private final Map<String, ServiceVo> services;
    private final Map<String, ServiceVo> servicesByAlias;

    public ServicesService(NakProperties config)
            throws JacksonException {
        log.info("Initializing services {}", config.getRegistrations());
        List<ServiceVo> registrations = JsonUtils.getRequestResponseJsonMapper().<List<ServiceVo>>readValue(
                        config.getRegistrations(), new TypeReference<>() {
                        })
                .stream()
                .map(registration -> withConfiguredLegacyId(registration, config))
                .toList();

        this.services = registrations.stream()
                .collect(Collectors.toUnmodifiableMap(ServiceVo::getServiceId, s -> s));
        this.servicesByAlias = indexAliases(registrations);

        log.debug("Registered services: {}", this.services.values());
    }

    private static ServiceVo withConfiguredLegacyId(ServiceVo registration, NakProperties config) {
        String configured = MapUtils.getString(
                config.getLegacyServiceIds(), registration.getServiceId());
        if (StringUtils.isBlank(configured)) {
            return registration;
        }
        Set<String> merged = new LinkedHashSet<>(
                SetUtils.emptyIfNull(registration.getLegacyServiceIds()));
        merged.add(configured);
        return registration.toBuilder().legacyServiceIds(merged).build();
    }

    private static Map<String, ServiceVo> indexAliases(List<ServiceVo> registrations) {
        Set<String> registeredIds = registrations.stream()
                .map(ServiceVo::getServiceId)
                .collect(Collectors.toSet());
        Map<String, ServiceVo> byAlias = new HashMap<>();
        for (ServiceVo registration : registrations) {
            for (String alias : registration.serviceIdAliasGroup()) {
                if (alias.equals(registration.getServiceId())) {
                    continue;
                }
                ServiceVo previous = byAlias.put(alias, registration);
                if (registeredIds.contains(alias) || previous != null) {
                    throw new IllegalStateException(String.format(
                            "Legacy service id '%s' of service '%s' is already in use",
                            alias, registration.getServiceId()));
                }
            }
        }
        return Map.copyOf(byAlias);
    }

    public ServiceVo loadById(String serviceId) {
        return getServiceById(serviceId)
                .orElseThrow(() -> new NotFoundException(
                        String.format("Service '%s' not found", serviceId)));
    }

    /** Finds a service by its registered ID or by one of its legacy IDs. */
    public Optional<ServiceVo> getServiceById(String serviceId) {
        ServiceVo service = services.get(serviceId);
        return Optional.ofNullable(service != null ? service : servicesByAlias.get(serviceId));
    }

    /**
     * Maps a legacy service ID to the registered ID. Any other value is returned unchanged.
     */
    public String toCanonicalId(String serviceId) {
        ServiceVo service = serviceId == null ? null : servicesByAlias.get(serviceId);
        return service != null ? service.getServiceId() : serviceId;
    }

    /**
     * The IDs that rows of the given service can be stored under: the registered ID and its legacy
     * IDs. An unknown ID maps to itself.
     */
    public Set<String> storedServiceIds(String serviceId) {
        return getServiceById(serviceId)
                .map(ServiceVo::serviceIdAliasGroup)
                .orElse(Set.of(serviceId));
    }

    public List<ServiceVo> listServices() {
        return List.copyOf(services.values());
    }
}
