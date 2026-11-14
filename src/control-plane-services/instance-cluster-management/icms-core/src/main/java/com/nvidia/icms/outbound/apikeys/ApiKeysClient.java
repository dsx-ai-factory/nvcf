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
package com.nvidia.icms.outbound.apikeys;

import com.nvidia.boot.exceptions.BootResponseException;
import com.nvidia.boot.exceptions.UpstreamException;
import com.nvidia.icms.configuration.staticclientauth.FixedBearerExchangeFilterFunction;
import com.nvidia.icms.configuration.staticclientauth.StaticClientAuthConfiguration.StaticClientApiKeysProperties;
import com.nvidia.icms.errors.IcmsAuthenticationException;
import com.nvidia.icms.outbound.apikeys.model.ApiKeyValidationRequest;
import com.nvidia.icms.outbound.apikeys.model.ApiKeyValidationResult;
import com.nvidia.icms.outbound.apikeys.model.ApiKeyValidationResponse;
import com.nvidia.icms.outbound.exception.ApiKeysException;
import com.nvidia.icms.util.IcmsOAuth2ClientUtils;
import com.nvidia.icms.util.IcmsOAuth2ClientUtils.ManagedHttpResources;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

@Component
@RefreshScope
public class ApiKeysClient {

    private static final String CLIENT_REGISTRATION_ID = "api-keys";

    private final WebClient webClient;
    private final ObjectMapper jsonMapper;
    private final String evaluationUri;
    private final String requestPropertyName;
    private final boolean selfHosted;

    public ApiKeysClient(
            @Value("${icms.api-keys.url}") String apiKeysBaseUrl,
            @Value("${icms.api-keys.namespace}") String namespace,
            @Value("${icms.api-keys.package-name}") String packageName,
            @Value("${icms.api-keys.policy-name}") String policyName,
            @Value("${icms.api-keys.request-property-name:apiKey}") String requestPropertyName,
            @Value("${spring.security.oauth2.client.registration.api-keys.client-id}") String clientId,
            @Value("${spring.security.oauth2.client.registration.api-keys.client-secret}") String clientSecret,
            @Value("${spring.security.oauth2.client.registration.api-keys.scope}") String scope,
            @Value("${spring.security.oauth2.client.provider.api-keys.token-uri}") String tokenUri,
            Optional<StaticClientApiKeysProperties> staticClientApiKeysProperties,
            ObjectMapper objectMapper,
            @Qualifier("apiKeysHttpResources") ManagedHttpResources httpResources,
            WebClient.Builder webClientBuilder) { // Prototype-scoped - Safe to mutate.

        this.jsonMapper = objectMapper;
        this.evaluationUri = "/v1/namespaces/%s/evaluations/%s.%s"
                .formatted(namespace, packageName, policyName);
        this.requestPropertyName = requestPropertyName;
        this.selfHosted = staticClientApiKeysProperties.isPresent();

        ExchangeFilterFunction authFilter = staticClientApiKeysProperties
                .map(p -> (ExchangeFilterFunction) new FixedBearerExchangeFilterFunction(p::getToken))
                .orElseGet(() -> IcmsOAuth2ClientUtils.getOAuth2ExchangeFilter(
                        webClientBuilder, CLIENT_REGISTRATION_ID, tokenUri,
                        clientId, clientSecret, scope));
        this.webClient = webClientBuilder
                .baseUrl(apiKeysBaseUrl)
                .clientConnector(httpResources.connector())
                .filter(IcmsOAuth2ClientUtils.getRetryableFilter(CLIENT_REGISTRATION_ID))
                .filter(authFilter)
                .filter(staticClientApiKeysProperties.isPresent()
                        ? ExchangeFilterFunction.ofResponseProcessor(response -> {
                            int status = response.statusCode().value();
                            if (status == 400 || status == 401 || status == 403 || status == 404) {
                                return Mono.error(new IcmsAuthenticationException(
                                        ApiKeysService.API_KEY_FAILURE_ERROR));
                            }
                            if (response.statusCode().isError()) {
                                return Mono.error(new ApiKeysException(
                                        "ApiKeys service error: " + response.statusCode(), true));
                            }
                            return Mono.just(response);
                        })
                        : IcmsOAuth2ClientUtils.getResponseFilterProcessor(CLIENT_REGISTRATION_ID))
                .build();
    }

    public ApiKeyValidationResult fetchApiKeyValidationResult(String apiKey) {
        try {
            ApiKeyValidationResponse response = webClient.post()
                    .uri(evaluationUri)
                    .accept(MediaType.APPLICATION_JSON)
                    .bodyValue(ApiKeyValidationRequest.builder()
                            .jsonField(requestPropertyName, apiKey).build())
                    .retrieve()
                    .bodyToMono(ApiKeyValidationResponse.class)
                    .block();
            return parseResponse(response);

        } catch (WebClientResponseException e) {
            int status = e.getStatusCode().value();
            if (selfHosted && (status == 400 || status == 401 || status == 403 || status == 404)) {
                throw new IcmsAuthenticationException(ApiKeysService.API_KEY_FAILURE_ERROR);
            }
            throw new ApiKeysException("ApiKeys service error: " + e.getStatusCode(), e,
                    e.getStatusCode().is5xxServerError());

        } catch (WebClientRequestException e) {
            throw new ApiKeysException("ApiKeys service is unreachable", e, true);
        } catch (UpstreamException e) {
            throw new ApiKeysException("ApiKeys service error", e,
                    !(e.getCause() instanceof BootResponseException cause
                            && cause.getStatusCode().is4xxClientError()));
        } catch (ApiKeysException e) {
            throw e;
        } catch (IcmsAuthenticationException e) {
            throw e;
        } catch (Exception e) {
            throw new ApiKeysException("Unknown error from ApiKeys service.", e);
        }
    }

    private ApiKeyValidationResult parseResponse(ApiKeyValidationResponse response) {
        if (response == null || response.getResult() == null) {
            throw new ApiKeysException("Unexpected response from ApiKeys service. No body returned.");
        }
        try {
            return jsonMapper.convertValue(response.getResult(), ApiKeyValidationResult.class);
        } catch (Exception exception) {
            throw new ApiKeysException("Failed to parse user details.", exception);
        }
    }
}
