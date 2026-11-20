/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.boot.jwt.jwks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;

class RetryingJwksRetrieverTest {
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void connectionRefusalIsRetriedAndThenRecovers() throws Exception {
        var client = mock(HttpClient.class);
        HttpResponse<byte[]> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.headers()).thenReturn(HttpHeaders.of(Map.of(), (a, b) -> true));
        when(response.body()).thenReturn("{\"keys\":[]}".getBytes(StandardCharsets.UTF_8));
        when(client.sendAsync(any(), any(HttpResponse.BodyHandler.class)))
                .thenReturn(CompletableFuture.failedFuture(new ConnectException("refused")))
                .thenReturn(CompletableFuture.failedFuture(new ConnectException("refused")))
                .thenReturn((CompletableFuture) CompletableFuture.completedFuture(response));
        var resource = new RetryingJwksRetriever(client)
                .retrieveResource(URI.create("http://keys.example.com/jwks").toURL());
        assertThat(resource.getContent()).isEqualTo("{\"keys\":[]}");
        verify(client, times(3)).sendAsync(any(), any(HttpResponse.BodyHandler.class));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void untrustedCertificateIsNotRetried() {
        var client = mock(HttpClient.class);
        when(client.sendAsync(any(), any(HttpResponse.BodyHandler.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new SSLHandshakeException("untrusted certificate")));
        var retriever = new RetryingJwksRetriever(client);
        assertThatThrownBy(() -> retriever.retrieveResource(
                URI.create("https://keys.example.com/jwks").toURL()))
                .isInstanceOf(SSLHandshakeException.class);
        verify(client).sendAsync(any(), any(HttpResponse.BodyHandler.class));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void interruptedCallerCancelsFetchWithoutPoisoningTheNextCaller() throws Exception {
        var client = mock(HttpClient.class);
        var pending = new CompletableFuture<HttpResponse<byte[]>>();
        when(client.sendAsync(any(), any(HttpResponse.BodyHandler.class)))
                .thenReturn((CompletableFuture) pending);
        var retriever = new RetryingJwksRetriever(client);
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> retriever.retrieveResource(
                    URI.create("http://keys.example.com/jwks").toURL()))
                    .isInstanceOf(IOException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(pending.isCancelled()).isTrue();
            verify(client).sendAsync(any(), any(HttpResponse.BodyHandler.class));
        } finally {
            Thread.interrupted();
        }
        HttpResponse<byte[]> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.headers()).thenReturn(HttpHeaders.of(Map.of(), (a, b) -> true));
        when(response.body()).thenReturn("{\"keys\":[]}".getBytes(StandardCharsets.UTF_8));
        when(client.sendAsync(any(), any(HttpResponse.BodyHandler.class)))
                .thenReturn((CompletableFuture) CompletableFuture.completedFuture(response));
        assertThat(retriever.retrieveResource(URI.create("http://keys.example.com/jwks").toURL())
                .getContent()).isEqualTo("{\"keys\":[]}");
        verify(client, times(2)).sendAsync(any(), any(HttpResponse.BodyHandler.class));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void dispatchTimeIsSubtractedBeforeWaitingForTheBody() throws Exception {
        var client = mock(HttpClient.class);
        var pending = mock(CompletableFuture.class);
        HttpResponse<byte[]> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.headers()).thenReturn(HttpHeaders.of(Map.of(), (a, b) -> true));
        when(response.body()).thenReturn("{\"keys\":[]}".getBytes(StandardCharsets.UTF_8));
        var waitedNanos = new AtomicLong();
        when(pending.get(anyLong(), eq(TimeUnit.NANOSECONDS))).thenAnswer(call -> {
            waitedNanos.set(call.getArgument(0));
            return response;
        });
        when(client.sendAsync(any(), any(HttpResponse.BodyHandler.class))).thenAnswer(call -> {
            Thread.sleep(100);
            return pending;
        });
        new RetryingJwksRetriever(client).retrieveResource(
                URI.create("http://keys.example.com/jwks").toURL());
        assertThat(waitedNanos.get()).isPositive().isLessThan(TimeUnit.MILLISECONDS.toNanos(700));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void errorBodyFailureKeepsTheHttpStatusAndCause() throws Exception {
        var client = mock(HttpClient.class);
        var bodyError = new IOException("body reset");
        when(client.sendAsync(any(), any(HttpResponse.BodyHandler.class))).thenAnswer(call -> {
            HttpResponse.BodyHandler<byte[]> handler = call.getArgument(1);
            var info = mock(HttpResponse.ResponseInfo.class);
            when(info.statusCode()).thenReturn(404);
            var subscriber = handler.apply(info);
            subscriber.onError(bodyError);
            return subscriber.getBody().toCompletableFuture();
        });
        assertThatThrownBy(() -> new RetryingJwksRetriever(client).retrieveResource(
                URI.create("http://keys.example.com/jwks").toURL()))
                .isInstanceOf(IOException.class).hasMessageContaining("HTTP 404")
                .hasCause(bodyError);
        verify(client).sendAsync(any(), any(HttpResponse.BodyHandler.class));
    }
}
