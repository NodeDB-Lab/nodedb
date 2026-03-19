//! HTTP API server using axum.
//!
//! Endpoints:
//! - GET  /health       — liveness
//! - GET  /health/ready — readiness (WAL recovered)
//! - GET  /metrics      — Prometheus-format metrics (requires monitor role)
//! - POST /query        — execute DDL via HTTP (requires auth)

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use tokio::net::TcpListener;
use tracing::info;

use crate::config::auth::AuthMode;
use crate::control::state::SharedState;

use super::auth::AppState;
use super::routes;

/// Build the axum router with all endpoints.
fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(routes::health::health))
        .route("/health/ready", get(routes::health::ready))
        .route("/metrics", get(routes::metrics::metrics))
        .route("/query", post(routes::query::query))
        .with_state(state)
}

/// Start the HTTP API server.
///
/// Runs until the shutdown signal is received.
pub async fn run(
    listen: SocketAddr,
    shared: Arc<SharedState>,
    auth_mode: AuthMode,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> crate::Result<()> {
    let state = AppState { shared, auth_mode };

    let router = build_router(state);
    let listener = TcpListener::bind(listen).await?;
    let local_addr = listener.local_addr()?;

    info!(%local_addr, "HTTP API server listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .map_err(crate::Error::Io)?;

    Ok(())
}
