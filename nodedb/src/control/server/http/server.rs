//! HTTP API server using axum + axum-server (for TLS).
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
        .route("/status", get(routes::status::status))
        .route(
            "/collections/{name}/documents",
            post(routes::document::insert_document),
        )
        .route(
            "/collections/{name}/documents/{id}",
            get(routes::document::get_document).delete(routes::document::delete_document),
        )
        .route(
            "/collections/{name}/search",
            post(routes::search::vector_search),
        )
        .route(
            "/collections/{name}/crdt/apply",
            post(routes::crdt::crdt_apply),
        )
        .route("/ws", get(routes::ws_rpc::ws_handler))
        .with_state(state)
}

/// Start the HTTP API server (plain HTTP or HTTPS).
///
/// If `tls_settings` is provided, serves HTTPS via axum-server + rustls.
/// Otherwise serves plain HTTP via axum::serve.
pub async fn run(
    listen: SocketAddr,
    shared: Arc<SharedState>,
    auth_mode: AuthMode,
    tls_settings: Option<&crate::config::server::TlsSettings>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> crate::Result<()> {
    let query_ctx = Arc::new(
        crate::control::planner::context::QueryContext::with_catalog(
            Arc::clone(&shared.credentials),
            1, // default tenant for name resolution
        ),
    );
    let state = AppState {
        shared,
        auth_mode,
        query_ctx,
    };
    let router = build_router(state);

    if let Some(tls) = tls_settings {
        // HTTPS via axum-server + rustls.
        let rustls_config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path)
                .await
                .map_err(|e| crate::Error::Config {
                    detail: format!("HTTP TLS config error: {e}"),
                })?;

        info!(%listen, tls = true, "HTTPS API server listening");

        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            let _ = shutdown.changed().await;
            shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(5)));
        });

        axum_server::bind_rustls(listen, rustls_config)
            .handle(handle)
            .serve(router.into_make_service())
            .await
            .map_err(crate::Error::Io)?;
    } else {
        // Plain HTTP.
        let listener = tokio::net::TcpListener::bind(listen).await?;
        let local_addr = listener.local_addr()?;
        info!(%local_addr, "HTTP API server listening");

        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
            })
            .await
            .map_err(crate::Error::Io)?;
    }

    Ok(())
}
