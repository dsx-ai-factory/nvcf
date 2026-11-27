/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.icms.configuration.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.nimbusds.jose.jwk.source.JWKSetRetrievalException;
import com.nvidia.boot.exceptions.ServiceUnavailableException;
import com.nvidia.icms.errors.GlobalErrorHandler;
import com.nvidia.icms.service.telemetry.TelemetryEventClient;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.server.resource.InvalidBearerTokenException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.servlet.HandlerExceptionResolver;
import tools.jackson.databind.json.JsonMapper;

class AuthEntryPointTest {
    private AuthEntryPoint entryPoint() {
        var advice = new GlobalErrorHandler();
        ReflectionTestUtils.setField(advice, "telemetryEventClient", mock(TelemetryEventClient.class));
        var mvc = MockMvcBuilders.standaloneSetup(new Object()).setControllerAdvice(advice).build();
        var resolver = mvc.getDispatcherServlet().getWebApplicationContext()
                .getBean("handlerExceptionResolver", HandlerExceptionResolver.class);
        var entry = new AuthEntryPoint();
        ReflectionTestUtils.setField(entry, "exceptionResolver", resolver);
        return entry;
    }

    @Test
    void keyServiceOutageIsResolvedAs503() throws Exception {
        var request = new MockHttpServletRequest();
        var response = new MockHttpServletResponse();
        var source = new JWKSetRetrievalException("unavailable", new IOException("refused"));
        entryPoint().commence(request, response,
                new AuthenticationServiceException("key fetch failed",
                        new JwtException("key fetch failed", source)));
        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(response.getHeader(HttpHeaders.RETRY_AFTER)).isEqualTo(
                Long.toString(ServiceUnavailableException.DEFAULT_RETRY_AFTER.toSeconds()));
        assertThat(response.getContentAsString())
                .contains("Signing key service is temporarily unavailable")
                .doesNotContain("refused");
        assertThat(response.getHeader(HttpHeaders.WWW_AUTHENTICATE)).isNull();
    }

    @Test
    void invalidBearerKeepsExisting401Handling() throws Exception {
        var request = new MockHttpServletRequest();
        var response = new MockHttpServletResponse();
        entryPoint().commence(request, response, new InvalidBearerTokenException("bad signature"));
        assertThat(response.getStatus()).isEqualTo(401);
        assertThat(response.getHeader(HttpHeaders.WWW_AUTHENTICATE))
                .contains("error=\"invalid_token\"");
        assertThat(new JsonMapper().readTree(response.getContentAsString()).path("error").asString())
                .isEqualTo("Authentication failure - bad signature");
    }
}
