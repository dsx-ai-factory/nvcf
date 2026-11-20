/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.boot.jwt.jwks;

import com.nimbusds.jose.util.Resource;
import com.nimbusds.jose.util.ResourceRetriever;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import java.io.IOException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RetryingJwksRetriever implements ResourceRetriever {
    private static final Logger LOG = LoggerFactory.getLogger(RetryingJwksRetriever.class);
    private static final int MAX_ATTEMPTS = 3;
    private static final long BUDGET_NANOS = TimeUnit.MILLISECONDS.toNanos(2_500);
    private static final long ATTEMPT_NANOS = TimeUnit.MILLISECONDS.toNanos(750);
    private static final long FAILURE_COOLDOWN_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(500))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    private final HttpClient client;
    private final OpenTelemetry telemetry;
    private final MeterRegistry metrics;
    private volatile IOException lastFailure;
    private volatile long retryNotBefore;

    RetryingJwksRetriever() { this(CLIENT); }

    RetryingJwksRetriever(HttpClient client) {
        this(client, GlobalOpenTelemetry.getOrNoop(), new SimpleMeterRegistry());
    }

    RetryingJwksRetriever(OpenTelemetry telemetry, MeterRegistry metrics) {
        this(CLIENT, telemetry, metrics);
    }

    private RetryingJwksRetriever(HttpClient client, OpenTelemetry telemetry, MeterRegistry metrics) {
        this.client = client;
        this.telemetry = telemetry;
        this.metrics = metrics;
        for (String outcome : List.of("success", "retryable_error", "permanent_error")) {
            Counter.builder("nvcf_jwks_fetch_requests").tag("outcome", outcome).register(metrics);
            Timer.builder("nvcf_jwks_fetch_duration").tag("outcome", outcome).register(metrics);
        }
    }

    @Override
    public Resource retrieveResource(URL url) throws IOException {
        long started = System.nanoTime();
        IOException failure = lastFailure;
        // Queued refreshes share a failed fetch without extending key-cache validity.
        if (failure != null && started - retryNotBefore < 0) {
            throw failure;
        }
        long deadline = started + BUDGET_NANOS;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return fetch(url, deadline);
            } catch (IOException ex) {
                failure = ex;
                if (ex instanceof PermanentFailure || ex instanceof SSLException
                        || Thread.currentThread().isInterrupted() || attempt == MAX_ATTEMPTS) {
                    break;
                }
                long backoffMillis = 50L << (attempt - 1);
                long delay = TimeUnit.MILLISECONDS.toNanos(backoffMillis
                        + ThreadLocalRandom.current().nextLong(backoffMillis + 1));
                if (deadline - System.nanoTime() <= delay) {
                    break;
                }
                LOG.warn("JWKS fetch retry attempt={} failure_type={}",
                        attempt, ex.getClass().getSimpleName());
                if (deadline - System.nanoTime() <= delay) {
                    break;
                }
                try {
                    TimeUnit.NANOSECONDS.sleep(delay);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    failure = new IOException("Signing key retrieval interrupted", interrupted);
                    break;
                }
            }
        }
        if (!Thread.currentThread().isInterrupted()) {
            lastFailure = failure;
            retryNotBefore = System.nanoTime() + FAILURE_COOLDOWN_NANOS;
        }
        throw failure;
    }

    private static long remainingNanos(long deadline) throws HttpTimeoutException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new HttpTimeoutException("Signing key retrieval deadline exceeded");
        }
        return remaining;
    }

    private Resource fetch(URL url, long fetchDeadline) throws IOException {
        long deadline = System.nanoTime() + Math.min(ATTEMPT_NANOS, remainingNanos(fetchDeadline));
        var span = telemetry.getTracer("com.nvidia.boot.jwt").spanBuilder("jwks.fetch")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("http.request.method", "GET")
                .setAttribute("server.address", url.getHost())
                .setAttribute("url.path", url.getPath())
                .startSpan();
        long started = System.nanoTime();
        String outcome = "retryable_error";
        CompletableFuture<HttpResponse<byte[]>> pending = null;
        try (var ignored = span.makeCurrent()) {
            var request = HttpRequest.newBuilder(url.toURI())
                    .timeout(Duration.ofNanos(remainingNanos(deadline)))
                    .header("Accept", "application/json, application/jwk-set+json").GET();
            W3CTraceContextPropagator.getInstance().inject(Context.current(), request,
                    (builder, name, value) -> builder.header(name, value));
            pending = client.sendAsync(request.build(), info -> {
                int status = info.statusCode();
                span.setAttribute("http.response.status_code", status);
                return status >= 200 && status < 300 ? new LimitedBody() : new ErrorBody(status);
            });
            var response = pending.get(remainingNanos(deadline), TimeUnit.NANOSECONDS);
            int status = response.statusCode();
            span.setAttribute("http.response.status_code", status);
            if (status < 200 || status >= 300) {
                var failure = httpFailure(status, null);
                if (failure instanceof PermanentFailure) {
                    outcome = "permanent_error";
                }
                throw failure;
            }
            lastFailure = null;
            outcome = "success";
            return new Resource(new String(response.body(), StandardCharsets.UTF_8),
                    response.headers().firstValue("Content-Type").orElse("application/json"));
        } catch (TimeoutException ex) {
            var timeout = new HttpTimeoutException("Signing key retrieval deadline exceeded");
            timeout.initCause(ex);
            throw timeout;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Signing key retrieval interrupted", ex);
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof IOException io) {
                if (io instanceof SSLException || io instanceof PermanentFailure) {
                    outcome = "permanent_error";
                }
                throw io;
            }
            outcome = "permanent_error";
            throw new PermanentFailure("Signing key retrieval failed", ex.getCause());
        } catch (java.net.URISyntaxException ex) {
            outcome = "permanent_error";
            throw new PermanentFailure("Invalid signing key service URI", ex);
        } finally {
            if (pending != null && !pending.isDone()) {
                pending.cancel(true);
            }
            metrics.counter("nvcf_jwks_fetch_requests", "outcome", outcome).increment();
            metrics.timer("nvcf_jwks_fetch_duration", "outcome", outcome)
                    .record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
            if (!outcome.equals("success")) {
                span.setAttribute("error", true);
                span.setStatus(StatusCode.ERROR, "Signing key retrieval failed");
            }
            span.end();
        }
    }

    private static IOException httpFailure(int status, Throwable cause) {
        String message = "Signing key service returned HTTP " + status;
        return switch (status) {
            case 429, 500, 502, 503, 504 -> new IOException(message, cause);
            default -> new PermanentFailure(message, cause);
        };
    }

    private static final class ErrorBody implements HttpResponse.BodySubscriber<byte[]> {
        private final CompletableFuture<byte[]> body = new CompletableFuture<>();
        private final int status;

        private ErrorBody(int status) { this.status = status; }

        @Override
        public CompletionStage<byte[]> getBody() { return body; }
        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.cancel();
            body.completeExceptionally(httpFailure(status, null));
        }
        @Override
        public void onNext(List<ByteBuffer> buffers) {}
        @Override
        public void onError(Throwable error) {
            body.completeExceptionally(httpFailure(status, error));
        }
        @Override
        public void onComplete() { body.completeExceptionally(httpFailure(status, null)); }
    }

    private static final class PermanentFailure extends IOException {
        private PermanentFailure(String message) { super(message); }
        private PermanentFailure(String message, Throwable cause) { super(message, cause); }
    }

    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private static final int MAX_BYTES = 256 * 1024;
        private final HttpResponse.BodySubscriber<byte[]> delegate =
                HttpResponse.BodySubscribers.ofByteArray();
        private Flow.Subscription subscription;
        private int received;

        @Override
        public CompletionStage<byte[]> getBody() { return delegate.getBody(); }
        @Override
        public void onSubscribe(Flow.Subscription value) {
            subscription = value;
            delegate.onSubscribe(value);
        }
        @Override
        public void onNext(List<ByteBuffer> buffers) {
            for (var buffer : buffers) {
                received += buffer.remaining();
                if (received > MAX_BYTES) {
                    subscription.cancel();
                    delegate.onError(new PermanentFailure("Signing key response exceeds size limit"));
                    return;
                }
            }
            delegate.onNext(buffers);
        }
        @Override
        public void onError(Throwable error) { delegate.onError(error); }
        @Override
        public void onComplete() { delegate.onComplete(); }
    }
}
