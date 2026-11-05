// SPDX-License-Identifier: BUSL-1.1

//! OTLP/gRPC receiver on dedicated port 4317.
//!
//! Uses axum with HTTP/2 to handle gRPC-encoded protobuf requests.
//! gRPC framing: 1-byte compressed flag + 4-byte big-endian length + protobuf payload.
//!
//! Service paths (per OTLP spec):
//! - POST `/opentelemetry.proto.collector.metrics.v1.MetricsService/Export`
//! - POST `/opentelemetry.proto.collector.trace.v1.TraceService/Export`
//! - POST `/opentelemetry.proto.collector.logs.v1.LogsService/Export`

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use prost::Message;
use tracing::info;

use super::proto;
use super::receiver::{authenticate_otel, ingest_logs, ingest_metrics, ingest_traces};
use crate::control::state::SharedState;

/// Start the OTLP/gRPC receiver on the given address (default :4317).
pub async fn run(listen: SocketAddr, shared: Arc<SharedState>) -> std::io::Result<()> {
    let router = Router::new()
        .route(
            "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
            post(grpc_metrics),
        )
        .route(
            "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
            post(grpc_traces),
        )
        .route(
            "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
            post(grpc_logs),
        )
        .with_state(shared);

    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!(addr = %listen, "OTLP/gRPC receiver started");
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(std::io::Error::other)
}

async fn grpc_metrics(
    State(shared): State<Arc<SharedState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let identity = match authenticate_otel(&headers, &shared, &peer.to_string()).await {
        Ok(identity) => identity,
        Err(error) => return grpc_error(StatusCode::UNAUTHORIZED, &error.to_string()),
    };
    let payload = grpc_decode(&body);
    let Ok(req) = proto::ExportMetricsServiceRequest::decode(&payload[..]) else {
        return grpc_error(StatusCode::BAD_REQUEST, "invalid protobuf");
    };
    match ingest_metrics(&shared, &identity, &peer.to_string(), &req).await {
        Ok(_) => grpc_response(&proto::ExportMetricsServiceResponse {}),
        Err(error) => grpc_error(StatusCode::FORBIDDEN, &error.to_string()),
    }
}

async fn grpc_traces(
    State(shared): State<Arc<SharedState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let identity = match authenticate_otel(&headers, &shared, &peer.to_string()).await {
        Ok(identity) => identity,
        Err(error) => return grpc_error(StatusCode::UNAUTHORIZED, &error.to_string()),
    };
    let payload = grpc_decode(&body);
    let Ok(req) = proto::ExportTraceServiceRequest::decode(&payload[..]) else {
        return grpc_error(StatusCode::BAD_REQUEST, "invalid protobuf");
    };
    match ingest_traces(&shared, &identity, &peer.to_string(), &req).await {
        Ok(_) => grpc_response(&proto::ExportTraceServiceResponse {}),
        Err(error) => grpc_error(StatusCode::FORBIDDEN, &error.to_string()),
    }
}

async fn grpc_logs(
    State(shared): State<Arc<SharedState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let identity = match authenticate_otel(&headers, &shared, &peer.to_string()).await {
        Ok(identity) => identity,
        Err(error) => return grpc_error(StatusCode::UNAUTHORIZED, &error.to_string()),
    };
    let payload = grpc_decode(&body);
    let Ok(req) = proto::ExportLogsServiceRequest::decode(&payload[..]) else {
        return grpc_error(StatusCode::BAD_REQUEST, "invalid protobuf");
    };
    match ingest_logs(&shared, &identity, &peer.to_string(), &req).await {
        Ok(_) => grpc_response(&proto::ExportLogsServiceResponse {}),
        Err(error) => grpc_error(StatusCode::FORBIDDEN, &error.to_string()),
    }
}

/// Decode gRPC framing: skip 1-byte compressed flag + 4-byte length prefix.
fn grpc_decode(body: &Bytes) -> Vec<u8> {
    if body.len() < 5 {
        return body.to_vec();
    }
    // byte 0: compressed flag (0 = no compression)
    // bytes 1-4: big-endian u32 message length
    let _compressed = body[0];
    let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    let start = 5;
    let end = (start + len).min(body.len());
    body[start..end].to_vec()
}

/// Encode a gRPC response with framing + protobuf.
fn grpc_response<M: Message>(msg: &M) -> (StatusCode, [(String, String); 2], Vec<u8>) {
    let mut payload = Vec::new();
    let _ = msg.encode(&mut payload);

    // gRPC framing: 0 (no compression) + 4-byte BE length + payload.
    let len = payload.len() as u32;
    let mut framed = Vec::with_capacity(5 + payload.len());
    framed.push(0); // no compression
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(&payload);

    (
        StatusCode::OK,
        [
            ("content-type".to_string(), "application/grpc".to_string()),
            ("grpc-status".to_string(), "0".to_string()),
        ],
        framed,
    )
}

fn grpc_error(status: StatusCode, msg: &str) -> (StatusCode, [(String, String); 2], Vec<u8>) {
    (
        status,
        [
            ("content-type".to_string(), "application/grpc".to_string()),
            ("grpc-status".to_string(), "2".to_string()), // UNKNOWN
        ],
        msg.as_bytes().to_vec(),
    )
}
