/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.boot.jwt.jwks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

class JwksJwtDecoderTest {
    private static final String ISSUER = "https://issuer.example.com";
    private static RSAKey first;
    private static RSAKey second;
    private HttpServer server;
    private java.util.concurrent.ExecutorService executor;
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger status = new AtomicInteger(200);
    private final AtomicInteger failures = new AtomicInteger();
    private final AtomicReference<String> body = new AtomicReference<>();
    private volatile long delayMillis;
    private volatile boolean dripBody;
    private volatile String errorBody;
    private final AtomicReference<String> traceParent = new AtomicReference<>();

    @BeforeAll
    static void keys() throws Exception {
        first = new RSAKeyGenerator(2048).keyID("first").generate();
        second = new RSAKeyGenerator(2048).keyID("second").generate();
    }

    @BeforeEach
    void start() throws Exception {
        body.set(new JWKSet(first.toPublicJWK()).toString());
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        server.createContext("/jwks", exchange -> {
            calls.incrementAndGet();
            traceParent.set(exchange.getRequestHeaders().getFirst("traceparent"));
            try {
                Thread.sleep(delayMillis);
                int code = failures.getAndUpdate(v -> Math.max(0, v - 1)) > 0
                        ? 503 : status.get();
                byte[] payload = (code >= 400 && errorBody != null ? errorBody : body.get())
                        .getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(code, payload.length);
                if (dripBody) {
                    for (byte value : payload) {
                        exchange.getResponseBody().write(value);
                        exchange.getResponseBody().flush();
                        Thread.sleep(50);
                    }
                } else {
                    exchange.getResponseBody().write(payload);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
        executor.shutdownNow();
    }

    private NimbusJwtDecoder decoder() {
        return decoder(5 * 60_000);
    }

    private NimbusJwtDecoder decoder(long ttlMillis) {
        var value = JwksJwtDecoder.create("http://127.0.0.1:" + server.getAddress().getPort()
                + "/jwks", SignatureAlgorithm.RS256, ttlMillis);
        value.setJwtValidator(JwtValidators.createDefaultWithIssuer(ISSUER));
        return value;
    }

    private static String token(RSAKey key, String kid, String issuer, Instant expiry)
            throws Exception {
        var jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(kid).build(),
                new JWTClaimsSet.Builder().issuer(issuer).subject("qa")
                        .issueTime(Date.from(Instant.now().minusSeconds(600)))
                        .expirationTime(Date.from(expiry)).build());
        jwt.sign(new RSASSASigner(key));
        return jwt.serialize();
    }

    private static String token(RSAKey key) throws Exception {
        return token(key, key.getKeyID(), ISSUER, Instant.now().plusSeconds(120));
    }

    @Test
    void acceptsConfiguredEcSignaturesWithoutTrustingOtherAlgorithms() throws Exception {
        var ec = new ECKeyGenerator(Curve.P_256).keyID("ec").generate();
        body.set(new JWKSet(ec.toPublicJWK()).toString());
        var decoder = JwksJwtDecoder.create("http://127.0.0.1:"
                + server.getAddress().getPort() + "/jwks", SignatureAlgorithm.ES256);
        decoder.setJwtValidator(JwtValidators.createDefaultWithIssuer(ISSUER));
        var jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.ES256).keyID("ec").build(),
                new JWTClaimsSet.Builder().issuer(ISSUER).subject("qa")
                        .expirationTime(Date.from(Instant.now().plusSeconds(120))).build());
        jwt.sign(new ECDSASigner(ec));
        assertThat(decoder.decode(jwt.serialize()).getSubject()).isEqualTo("qa");
        String rsa = token(first);
        assertThatThrownBy(() -> decoder.decode(rsa)).isInstanceOf(BadJwtException.class)
                .satisfies(ex -> assertThat(JwksJwtDecoder.isUnavailable(ex)).isFalse());
    }

    @Test
    void sealedServiceRecoversWithinBoundedRetries() throws Exception {
        failures.set(2);
        assertThat(decoder().decode(token(first)).getSubject()).isEqualTo("qa");
        assertThat(calls).hasValue(3);
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 500, 502, 503, 504})
    void transientHttpFailureStopsAfterThreeAttempts(int code) throws Exception {
        status.set(code);
        var decoder = decoder();
        String jwt = token(first);
        long start = System.nanoTime();
        assertThatThrownBy(() -> decoder.decode(jwt)).isInstanceOf(JwtException.class)
                .satisfies(ex -> assertThat(JwksJwtDecoder.isUnavailable(ex)).isTrue());
        assertThat(calls).hasValue(3);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(4));
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 501})
    void permanentHttpFailureIsNotRetried(int code) throws Exception {
        status.set(code);
        String jwt = token(first);
        var decoder = decoder();
        assertThatThrownBy(() -> decoder.decode(jwt)).isInstanceOf(JwtException.class)
                .satisfies(ex -> assertThat(JwksJwtDecoder.isUnavailable(ex)).isTrue());
        assertThat(calls).hasValue(1);
    }

    @Test
    void malformedRedirectFailsClosedWithinTheTransportRetryBudget() throws Exception {
        status.set(301);
        var decoder = decoder();
        String jwt = token(first);
        long start = System.nanoTime();
        // HttpClient reports a redirect without Location as a transport IOException.
        assertThatThrownBy(() -> decoder.decode(jwt)).isInstanceOf(JwtException.class)
                .satisfies(ex -> assertThat(JwksJwtDecoder.isUnavailable(ex)).isTrue());
        assertThat(calls).hasValue(3);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(4));
    }

    @Test
    void preservesVerifiedSpringRedirectCompatibilityWithinTheFetchBudget() throws Exception {
        var redirectCalls = new AtomicInteger();
        server.createContext("/redirect", exchange -> {
            redirectCalls.incrementAndGet();
            exchange.getResponseHeaders().set("Location", "/jwks");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        var original = NimbusJwtDecoder.withJwkSetUri("http://127.0.0.1:"
                + server.getAddress().getPort() + "/redirect")
                .jwsAlgorithm(SignatureAlgorithm.RS256).build();
        original.setJwtValidator(JwtValidators.createDefaultWithIssuer(ISSUER));
        String valid = token(first);
        assertThat(original.decode(valid).getSubject()).isEqualTo("qa");
        var replacement = JwksJwtDecoder.create("http://127.0.0.1:"
                + server.getAddress().getPort() + "/redirect", SignatureAlgorithm.RS256);
        replacement.setJwtValidator(JwtValidators.createDefaultWithIssuer(ISSUER));
        assertThat(replacement.decode(valid).getSubject()).isEqualTo("qa");
        assertThat(redirectCalls).hasValue(2);
        assertThat(calls).hasValue(2);
    }

    @Test
    void cachedKnownKeyContinuesDuringAnOutage() throws Exception {
        var decoder = decoder();
        String jwt = token(first);
        decoder.decode(jwt);
        status.set(503);
        assertThat(decoder.decode(jwt).getSubject()).isEqualTo("qa");
        assertThat(calls).hasValue(1);
    }

    @Test
    void failedUnknownKeyRefreshDoesNotDiscardStillValidKeys() throws Exception {
        var decoder = decoder();
        String known = token(first);
        decoder.decode(known);
        status.set(503);
        String unknown = token(second);
        assertThatThrownBy(() -> decoder.decode(unknown)).isInstanceOf(JwtException.class);
        assertThat(decoder.decode(known).getSubject()).isEqualTo("qa");
        assertThat(calls).hasValue(4);
    }

    @Test
    void removedKeyIsRejectedAfterASuccessfulRefresh() throws Exception {
        var decoder = decoder();
        String retired = token(first);
        decoder.decode(retired);
        body.set(new JWKSet(second.toPublicJWK()).toString());
        decoder.decode(token(second));
        assertThatThrownBy(() -> decoder.decode(retired)).isInstanceOf(BadJwtException.class);
    }

    @Test
    void expiredCacheDoesNotAuthorizeFromStaleKeys() throws Exception {
        var decoder = decoder(1);
        String jwt = token(first);
        decoder.decode(jwt);
        status.set(503);
        Thread.sleep(10);
        assertThatThrownBy(() -> decoder.decode(jwt))
                .satisfies(ex -> assertThat(JwksJwtDecoder.isUnavailable(ex)).isTrue());
        assertThat(calls).hasValue(4);
    }

    @Test
    void concurrentUnknownKidRefreshesAreCoalesced() throws Exception {
        var decoder = decoder();
        decoder.decode(token(first));
        body.set(new JWKSet(second.toPublicJWK()).toString());
        delayMillis = 100;
        String rotated = token(second);
        var start = new CountDownLatch(1);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<Future<String>>();
            for (int i = 0; i < 24; i++) {
                futures.add(workers.submit(() -> {
                    start.await();
                    return decoder.decode(rotated).getSubject();
                }));
            }
            start.countDown();
            for (var future : futures) {
                assertThat(future.get(5, TimeUnit.SECONDS)).isEqualTo("qa");
            }
        }
        assertThat(calls).hasValue(2);
    }

    @Test
    void failedConcurrentRefreshesShareTheBoundedFetch() throws Exception {
        var decoder = decoder();
        decoder.decode(token(first));
        status.set(503);
        delayMillis = 100;
        String unknown = token(second);
        var start = new CountDownLatch(1);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<Future<Boolean>>();
            for (int i = 0; i < 24; i++) {
                futures.add(workers.submit(() -> {
                    start.await();
                    try {
                        decoder.decode(unknown);
                        return false;
                    } catch (JwtException ex) {
                        return JwksJwtDecoder.isUnavailable(ex);
                    }
                }));
            }
            start.countDown();
            for (var future : futures) assertThat(future.get(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(calls).hasValue(4);
    }

    @Test
    void unknownKidNeverUsesADifferentCachedKey() throws Exception {
        var decoder = decoder();
        decoder.decode(token(first));
        String unknown = token(second);
        assertThatThrownBy(() -> decoder.decode(unknown)).isInstanceOf(BadJwtException.class)
                .satisfies(ex -> assertThat(JwksJwtDecoder.isUnavailable(ex)).isFalse());
    }

    @Test
    void invalidSignatureExpiredTokenAndWrongIssuerRemainInvalid() throws Exception {
        var decoder = decoder();
        for (String invalid : new String[] {
                token(second, first.getKeyID(), ISSUER, Instant.now().plusSeconds(120)),
                token(first, first.getKeyID(), ISSUER, Instant.now().minusSeconds(300)),
                token(first, first.getKeyID(), "https://untrusted.example.com",
                        Instant.now().plusSeconds(120))}) {
            assertThatThrownBy(() -> decoder.decode(invalid)).isInstanceOf(BadJwtException.class)
                    .satisfies(ex -> assertThat(JwksJwtDecoder.isUnavailable(ex)).isFalse());
        }
    }

    @Test
    void malformedKeySetIsAnUnavailableDependencyAndIsNotRetried() throws Exception {
        body.set("not a key set");
        String jwt = token(first);
        var decoder = decoder();
        assertThatThrownBy(() -> decoder.decode(jwt)).isInstanceOf(JwtException.class)
                .satisfies(ex -> assertThat(JwksJwtDecoder.isUnavailable(ex)).isTrue());
        assertThat(calls).hasValue(1);
    }

    @Test
    void slowBodyCannotExceedTheWholeFetchBudget() throws Exception {
        dripBody = true;
        String jwt = token(first);
        var decoder = decoder();
        long start = System.nanoTime();
        assertThatThrownBy(() -> decoder.decode(jwt)).isInstanceOf(JwtException.class);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(4));
        assertThat(calls.get()).isBetween(1, 3);
    }

    @Test
    void oversized503BodyDoesNotSuppressRecovery() throws Exception {
        failures.set(1);
        errorBody = "x".repeat(256 * 1024 + 1);
        assertThat(decoder().decode(token(first)).getSubject()).isEqualTo("qa");
        assertThat(calls).hasValue(2);
    }

    @Test
    void stalled404BodyDoesNotTurnPermanentStatusIntoRetryableTimeout() throws Exception {
        status.set(404);
        dripBody = true;
        var decoder = decoder();
        String jwt = token(first);
        assertThatThrownBy(() -> decoder.decode(jwt)).isInstanceOf(JwtException.class)
                .satisfies(ex -> assertThat(JwksJwtDecoder.isUnavailable(ex)).isTrue());
        assertThat(calls).hasValue(1);
    }

    @Test
    void oversizedKeySetIsRejectedWithoutRetry() throws Exception {
        String validKeySet = body.get();
        String jwt = token(first);
        assertThat(decoder().decode(jwt).getSubject()).isEqualTo("qa");
        calls.set(0);
        body.set(validKeySet + " ".repeat(256 * 1024 + 1));
        var decoder = decoder();
        assertThatThrownBy(() -> decoder.decode(jwt)).isInstanceOf(JwtException.class)
                .satisfies(ex -> assertThat(JwksJwtDecoder.isUnavailable(ex)).isTrue());
        assertThat(calls).hasValue(1);
    }
    @Test
    void defaultFactoriesLeaveGlobalTelemetryRegistrationUntouched() throws Exception {
        var probe = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java")
                .toString(), "-cp", System.getProperty("java.class.path"),
                TelemetryFactoryProbe.class.getName()).redirectErrorStream(true).start();
        try {
            assertThat(probe.waitFor(15, TimeUnit.SECONDS)).isTrue();
            assertThat(probe.exitValue())
                    .withFailMessage(new String(probe.getInputStream().readAllBytes(),
                            StandardCharsets.UTF_8)).isZero();
        } finally {
            probe.destroyForcibly();
        }
    }

    public static class TelemetryFactoryProbe {
        public static void main(String[] args) {
            assertThat(GlobalOpenTelemetry.isSet()).isFalse();
            String uri = "http://127.0.0.1:1/jwks";
            JwksJwtDecoder.create(uri, SignatureAlgorithm.RS256);
            JwksJwtDecoder.create(uri, SignatureAlgorithm.RS256, 1_000);
            new RetryingJwksRetriever();
            assertThat(GlobalOpenTelemetry.isSet()).isFalse();
            try (var sdk = OpenTelemetrySdk.builder().buildAndRegisterGlobal()) {
                assertThat(GlobalOpenTelemetry.isSet()).isTrue();
            }
        }
    }

    @Test
    void recordsBoundedMetricsAndPropagatesTraceContext() throws Exception {
        var spans = InMemorySpanExporter.create();
        var metrics = new SimpleMeterRegistry();
        try (var sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(spans)).build())
                .build()) {
            failures.set(1);
            var decoder = JwksJwtDecoder.create("http://127.0.0.1:"
                    + server.getAddress().getPort() + "/jwks", SignatureAlgorithm.RS256,
                    sdk, metrics);
            decoder.setJwtValidator(JwtValidators.createDefaultWithIssuer(ISSUER));
            assertThat(metrics.find("nvcf_jwks_fetch_requests").counters()).hasSize(3);
            assertThat(metrics.find("nvcf_jwks_fetch_requests").counters().stream()
                    .mapToDouble(counter -> counter.count()).sum()).isZero();
            decoder.decode(token(first));
            var captured = spans.getFinishedSpanItems();
            assertThat(captured).hasSize(2);
            assertThat(captured.getFirst().getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
            assertThat(captured.getFirst().getAttributes().get(AttributeKey.booleanKey("error")))
                    .isTrue();
            assertThat(captured.getLast().getName()).isEqualTo("jwks.fetch");
            assertThat(traceParent.get()).contains(captured.getLast().getTraceId())
                    .contains(captured.getLast().getSpanId());
            assertThat(metrics.find("nvcf_jwks_fetch_requests").counters().stream()
                    .mapToDouble(counter -> counter.count()).sum()).isEqualTo(2);
            assertThat(metrics.find("nvcf_jwks_fetch_duration").timers().stream()
                    .mapToLong(timer -> timer.count()).sum()).isEqualTo(2);
        }
    }

}
