/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.boot.jwt.jwks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.jwk.source.JWKSetRetrievalException;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.server.resource.InvalidBearerTokenException;
import org.springframework.security.oauth2.server.resource.web.authentication.BearerTokenAuthenticationFilter;

class JwksAuthenticationManagerTest {
    @Test
    void servletAuthenticationReturns503WithoutClaimingTheBearerIsInvalid() throws Exception {
        var source = new JWKSetRetrievalException("JWKS unavailable", new IOException("refused"));
        var manager = new JwksAuthenticationManager(auth -> {
            throw new AuthenticationServiceException("key fetch failed",
                    new JwtException("key fetch failed", source));
        });
        var filter = new BearerTokenAuthenticationFilter(manager);
        var request = new MockHttpServletRequest();
        request.addHeader("Authorization", "Bearer qa");
        var response = new MockHttpServletResponse();
        filter.doFilter(request, response, (req, res) -> {
            throw new AssertionError("An unverified request must not reach the application");
        });
        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(response.getHeader("WWW-Authenticate"))
                .contains("temporarily_unavailable").doesNotContain("invalid_token");
    }

    @Test
    void invalidBearerStillReturns401() throws Exception {
        var manager = new JwksAuthenticationManager(auth -> {
            throw new InvalidBearerTokenException("invalid token");
        });
        var filter = new BearerTokenAuthenticationFilter(manager);
        var request = new MockHttpServletRequest();
        request.addHeader("Authorization", "Bearer qa");
        var response = new MockHttpServletResponse();
        filter.doFilter(request, response, (req, res) -> {
            throw new AssertionError("Invalid credentials must not reach the application");
        });
        assertThat(response.getStatus()).isEqualTo(401);
        assertThat(response.getHeader("WWW-Authenticate")).contains("invalid_token");
    }

    @Test
    void unrelatedAuthenticationFailureIsPreserved() {
        var original = new AuthenticationServiceException("not a JWKS error");
        var manager = new JwksAuthenticationManager(auth -> { throw original; });
        assertThatThrownBy(() -> manager.authenticate(null)).isSameAs(original);
    }
}
