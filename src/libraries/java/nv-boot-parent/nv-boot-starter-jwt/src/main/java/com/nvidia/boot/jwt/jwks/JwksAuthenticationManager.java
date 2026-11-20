/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.boot.jwt.jwks;

import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.server.resource.BearerTokenError;

/** Keeps a key-service outage distinct from an invalid bearer token. */
public final class JwksAuthenticationManager implements AuthenticationManager {
    private final AuthenticationManager delegate;

    public JwksAuthenticationManager(AuthenticationManager delegate) {
        this.delegate = delegate;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        try {
            return delegate.authenticate(authentication);
        } catch (AuthenticationException ex) {
            if (!JwksJwtDecoder.isUnavailable(ex)) {
                throw ex;
            }
            var error = new BearerTokenError("temporarily_unavailable",
                    HttpStatus.SERVICE_UNAVAILABLE, JwksJwtDecoder.UNAVAILABLE, null);
            throw new OAuth2AuthenticationException(error, JwksJwtDecoder.UNAVAILABLE, ex);
        }
    }
}
