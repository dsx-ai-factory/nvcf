// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use anyhow::{Context, Result};
use quinn::ClientConfig;
use tracing::info;

use stargate_protocol::TunnelTransportProtocol;
use stargate_tls::ServerTlsIdentity;

use super::ReverseTunnelLiveness;

pub(super) fn build_client_config(
    cert_pem: Option<&[u8]>,
    insecure: bool,
    tunnel_protocol: TunnelTransportProtocol,
) -> Result<ClientConfig> {
    stargate_tls::build_quic_client_config(
        cert_pem,
        insecure,
        tunnel_protocol.alpn_protocols(),
        "TLS cert required when --quic-insecure is not set",
    )
}

/// Builds the reverse listener's QUIC server config.
///
/// Every server config for the reverse listener, including the ones rebuilt on
/// TLS reload, goes through here so the liveness settings survive rotation.
pub(super) fn build_server_config(
    tls_identity: &ServerTlsIdentity,
    tunnel_protocol: TunnelTransportProtocol,
    liveness: ReverseTunnelLiveness,
) -> Result<quinn::ServerConfig> {
    if matches!(tls_identity, ServerTlsIdentity::SelfSigned) {
        info!("no TLS cert/key provided, generating self-signed certificate");
    }
    let mut config =
        stargate_tls::build_quic_server_config(tls_identity, tunnel_protocol.alpn_protocols())?;
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(
        liveness
            .max_idle_timeout
            .try_into()
            .context("reverse tunnel max idle timeout must fit QUIC transport parameters")?,
    ));
    transport.keep_alive_interval(Some(liveness.keep_alive_interval));
    config.transport_config(Arc::new(transport));
    Ok(config)
}
