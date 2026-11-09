// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#[allow(dead_code)]
mod common;

use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;

use common::sse::{assert_sse_done, parse_sse_events};
use common::{
    init_crypto, make_stargate_runtime_with_lb, start_dummy_inst, wait_until,
    with_proxy_headers_cache_affinity,
};
use prost::Message;
use stargate_proto::pb::stargate_control_plane_client::StargateControlPlaneClient;
use stargate_proto::pb::{
    InferenceServerModelRegistration, InferenceServerRegistration, InferenceServerStatus,
    ModelStats,
};

// Frozen protoc output from stargate v0.19.3 (c60aca1375ccd15b992d6e5efe91cd8120ae01f7).
// Input: last_mean_input_tps:100 max_output_tps:50
// kv_cache_capacity_tokens:4096 kv_cache_free_tokens:4096. No field 18 exists.
// Do not regenerate using the candidate schema: the old wire contract is the fixture.
const LEGACY_STATS: &[u8] = &[
    0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x49, 0x40, 0x30, 0x80, 0x20, 0x40, 0x80, 0x20,
];

// Frozen protoc output from v0.25.0 (6a3fc45d18d692d8aeeac62d6f01e88af3e8616a),
// with the same inputs and max_input_tps:100.
const MAX_STATS: &[u8] = &[
    0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x49, 0x40, 0x30, 0x80, 0x20, 0x40, 0x80, 0x20, 0x91, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x59, 0x40,
];

#[tokio::test]
async fn released_stats_route_alone_mixed_and_after_rollback() {
    init_crypto();
    let legacy = ModelStats::decode(LEGACY_STATS).expect("released stats must decode");
    assert_eq!(legacy.last_mean_input_tps, 100.0);
    assert_eq!(legacy.max_output_tps, 50.0);
    assert_eq!(legacy.kv_cache_capacity_tokens, 4096);
    assert_eq!(legacy.kv_cache_free_tokens, 4096);
    assert_eq!(legacy.max_input_tps, None);
    let modern = ModelStats::decode(MAX_STATS).expect("released maximum stats must decode");
    assert_eq!(modern.max_input_tps, Some(100.0));

    for (algorithm, legacy_mode) in [
        ("power-of-n", true),
        ("pulsar", true),
        ("pulsar-wait-and-widen", true),
        ("pulsar", false),
        ("pulsar-wait-and-widen", false),
    ] {
        let mut policy = serde_json::json!({"algorithm": algorithm});
        if legacy_mode && algorithm.starts_with("pulsar") {
            policy["rendezvous_weight"] = "last-mean-input-tps".into();
        }
        let config_json = if algorithm == "power-of-n" {
            serde_json::json!({})
        } else {
            serde_json::json!({"models": {"compatibility": policy}})
        };
        let mut config = tempfile::NamedTempFile::new().unwrap();
        write!(config, "{config_json}").unwrap();
        let (grpc_addr, http_addr, runtime) = make_stargate_runtime_with_lb(
            "compatibility",
            Some(config.path().to_str().unwrap().to_owned()),
        );
        let handle = runtime.start().await.expect("Stargate must start");
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let mut registrations = Vec::new();
        let mut tunnels = Vec::new();

        let mut versions = vec![("modern", modern.clone())];
        if legacy_mode {
            versions.insert(0, ("legacy", legacy.clone()));
        }
        for (id, stats) in versions {
            let (_, quic_url, tunnel) = start_dummy_inst("compatibility").await;
            tunnels.push(tunnel);
            let mut grpc = StargateControlPlaneClient::connect(format!("http://{grpc_addr}"))
                .await
                .expect("gRPC must connect");
            let (tx, rx) = flume::bounded(8);
            let registration = InferenceServerRegistration {
                inference_server_id: id.to_owned(),
                cluster_id: id.to_owned(),
                inference_server_url: quic_url,
                reverse_tunnel: false,
                models: HashMap::from([(
                    "compatibility".to_owned(),
                    InferenceServerModelRegistration {
                        stats: Some(stats),
                        status: InferenceServerStatus::Active as i32,
                    },
                )]),
            };
            tx.send_async(registration.clone()).await.unwrap();
            let mut stream = grpc
                .register_inference_server(rx.into_stream())
                .await
                .expect("released registration must be accepted")
                .into_inner();
            tokio::time::timeout(Duration::from_secs(5), stream.message())
                .await
                .expect("registration ACK timeout")
                .expect("registration ACK error")
                .expect("registration stream closed");
            registrations.push((registration, tx, stream));
            wait_for_backend_count(&handle, registrations.len()).await;
            assert_stream(&client, http_addr).await;
        }

        if legacy_mode {
            // New-only, then restore the old backend before withdrawing the new one.
            let (registration, tx, _) = &mut registrations[0];
            registration.models.get_mut("compatibility").unwrap().status =
                InferenceServerStatus::Inactive as i32;
            tx.send_async(registration.clone()).await.unwrap();
            wait_for_backend_count(&handle, 1).await;
            assert_eq!(assert_stream(&client, http_addr).await, "modern");
            registration.models.get_mut("compatibility").unwrap().status =
                InferenceServerStatus::Active as i32;
            tx.send_async(registration.clone()).await.unwrap();
            wait_for_backend_count(&handle, 2).await;
            drop(registrations.pop());
            wait_for_backend_count(&handle, 1).await;
            assert_eq!(assert_stream(&client, http_addr).await, "legacy");
        }

        drop(registrations);
        handle.begin_shutdown();
        handle.wait_for_shutdown(Duration::from_secs(5)).await;
    }
}

async fn wait_for_backend_count(handle: &stargate::runtime::StargateHandle, count: usize) {
    wait_until(
        "compatibility backend membership",
        Duration::from_secs(5),
        Duration::from_millis(10),
        || async {
            let actual = handle.state().total_active_backend_count().await;
            (actual == count).then_some(()).ok_or(actual)
        },
    )
    .await;
}

async fn assert_stream(client: &reqwest::Client, addr: std::net::SocketAddr) -> String {
    let response = with_proxy_headers_cache_affinity(
        client.post(format!("http://{addr}/v1/chat/completions")),
        "compatibility",
        "compatibility-request",
        "stable-prefix",
    )
    .json(&serde_json::json!({
        "model": "compatibility", "messages": [{"role": "user", "content": "hello"}],
        "stream": true,
    }))
    .send()
    .await
    .expect("proxy request must complete");
    assert_eq!(response.status(), 200);
    let backend = response
        .headers()
        .get("x-inference-server-id")
        .expect("backend identity header")
        .to_str()
        .unwrap()
        .to_owned();
    let body = response.text().await.expect("stream must complete");
    assert_sse_done(&parse_sse_events(&body).expect("valid SSE"));
    backend
}
