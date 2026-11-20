/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.boot.jwt.jwks;

import com.nimbusds.jose.RemoteKeySourceException;
import com.nimbusds.jose.jwk.source.JWKSetUnavailableException;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.SecurityContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import java.net.MalformedURLException;
import java.net.URI;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

/** Creates decoders with bounded key retrieval and the existing Nimbus cache lifetime. */
public final class JwksJwtDecoder {
    public static final String UNAVAILABLE = "Signing key service is temporarily unavailable";

    private JwksJwtDecoder() {}

    public static NimbusJwtDecoder create(String uri, SignatureAlgorithm algorithm) {
        return create(uri, algorithm, GlobalOpenTelemetry.getOrNoop(), new SimpleMeterRegistry());
    }

    public static NimbusJwtDecoder create(String uri, SignatureAlgorithm algorithm,
            OpenTelemetry telemetry, MeterRegistry metrics) {
        return create(uri, algorithm, JWKSourceBuilder.DEFAULT_CACHE_TIME_TO_LIVE,
                telemetry, metrics);
    }

    static NimbusJwtDecoder create(String uri, SignatureAlgorithm algorithm, long cacheMillis) {
        return create(uri, algorithm, cacheMillis,
                GlobalOpenTelemetry.getOrNoop(), new SimpleMeterRegistry());
    }

    static NimbusJwtDecoder create(String uri, SignatureAlgorithm algorithm, long cacheMillis,
            OpenTelemetry telemetry, MeterRegistry metrics) {
        try {
            var source = JWKSourceBuilder.<SecurityContext>create(
                            URI.create(uri).toURL(), new RetryingJwksRetriever(telemetry, metrics))
                    .cache(cacheMillis, 3_000)
                    .refreshAheadCache(false)
                    .rateLimited(false)
                    .retrying(false)
                    .outageTolerant(false)
                    .build();
            return NimbusJwtDecoder.withJwkSource(source).jwsAlgorithm(algorithm).build();
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("Invalid signing key service URI", ex);
        }
    }

    public static boolean isUnavailable(Throwable failure) {
        for (int depth = 0; failure != null && depth < 32; depth++, failure = failure.getCause()) {
            if (failure instanceof JWKSetUnavailableException
                    || failure instanceof RemoteKeySourceException) {
                return true;
            }
        }
        return false;
    }
}
