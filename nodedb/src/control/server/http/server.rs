// SPDX-License-Identifier: BUSL-1.1

//! HTTP API server using axum + axum-server (for TLS).
//!
//! Probe routes (unversioned, always reachable):
//! - GET  /healthz      — k8s readiness/liveness (always reachable; 503 until GatewayEnable)
//! - GET  /health/live  — unconditional liveness probe
//! - GET  /health/ready — readiness (WAL recovered)
//! - POST /health/drain — trigger graceful drain
//! - GET  /metrics      — Prometheus-format metrics (requires monitor role)
//!
//! All other routes are versioned under `/v1/`.
//!
//! # API versioning policy
//!
//! The version segment is part of the route path (`/v1/...`), not a header or
//! query parameter. The contract is:
//!
//! - **Additive changes** within a major version (new fields, new routes, new
//!   optional query params) are non-breaking and ship under the existing
//!   `/vN/` prefix. Clients must ignore unknown response fields.
//! - **Breaking changes** (removed fields, changed semantics, renamed routes,
//!   changed status-code meaning) require a new `/vN+1/` prefix. The previous
//!   prefix continues to serve the old contract until the deprecation window
//!   elapses.
//! - **Deprecation window**: when `/vN+1/` ships, `/vN/` routes are kept for a
//!   minimum of two minor releases, with a `Deprecation` and `Sunset` header
//!   on every response. Removal happens in the release after the sunset date,
//!   never within a patch release.
//! - **Probe routes** (`/healthz`, `/health/*`, `/metrics`) are intentionally
//!   unversioned: they are operational contracts consumed by load balancers
//!   and scrapers, and their shape is governed by the underlying ecosystem
//!   (Kubernetes probes, Prometheus exposition format), not by NodeDB.
//!
//! When adding `/v2/` routes in the future, mount them on this same router
//! alongside the existing `/v1/` entries — do not fork the router. Shared
//! middleware (auth, startup-gate, tracing) must apply uniformly across
//! versions.

use std::sync::Arc;

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post, put};
use tracing::info;

use super::version::stamp_content_type;

use crate::config::auth::AuthMode;
use crate::control::state::SharedState;

use super::auth::AppState;
use super::routes;

/// Build the axum router with all endpoints.
///
/// JSON routes (all `/v1/` routes except SSE streams and the WebSocket
/// endpoint) get `stamp_content_type` applied via `map_response` so every
/// response carries `application/vnd.nodedb.v1+json; charset=utf-8` without
/// per-handler boilerplate.
///
/// SSE and WebSocket routes are kept on a separate sub-router that does NOT
/// carry the `map_response` layer — those handlers set their own
/// `Content-Type` (text/event-stream, or the WS upgrade response).
fn build_router(state: AppState) -> Router {
    // ── Streaming / non-JSON routes (no Content-Type stamp) ──────────────────
    let streaming_routes = Router::new()
        // WebSocket RPC — upgrade response, not JSON.
        .route("/v1/ws", get(routes::ws_rpc::ws_handler))
        // SSE CDC stream — text/event-stream.
        .route("/v1/cdc/{collection}", get(routes::cdc::sse_stream))
        // SSE named-stream events — text/event-stream.
        .route(
            "/v1/streams/{stream}/events",
            get(routes::stream_sse::stream_events),
        );

    // ── JSON routes (Content-Type stamped to v1 vendor type) ─────────────────
    let mut json_routes = Router::new()
        .route("/v1/query", post(routes::query::query))
        .route("/v1/query/stream", post(routes::query::query_ndjson))
        .route("/v1/status", get(routes::status::status))
        .route("/v1/cluster/status", get(routes::cluster::cluster_status))
        .route(
            "/v1/cluster/debug/raft/{group_id}",
            get(routes::cluster_debug::raft::raft_debug),
        )
        .route(
            "/v1/cluster/debug/transport",
            get(routes::cluster_debug::transport::transport_debug),
        )
        .route(
            "/v1/cluster/debug/catalog/descriptors",
            get(routes::cluster_debug::catalog::catalog_debug),
        )
        .route(
            "/v1/cluster/debug/leases",
            get(routes::cluster_debug::leases::leases_debug),
        )
        .route(
            "/v1/cluster/debug/quarantined-segments",
            get(routes::cluster_debug::quarantined_segments::quarantined_segments),
        )
        .route(
            "/v1/auth/exchange-key",
            post(routes::auth_key::exchange_key),
        )
        .route(
            "/v1/auth/session",
            post(routes::auth_session::create_session).delete(routes::auth_session::delete_session),
        )
        .route(
            "/v1/collections/{name}/crdt/apply",
            post(routes::crdt::crdt_apply).layer(DefaultBodyLimit::max(
                routes::crdt::CRDT_HTTP_BODY_MAX_BYTES,
            )),
        )
        .route(
            "/v1/functions/{name}/wasm",
            put(routes::wasm_upload::upload_wasm).layer(DefaultBodyLimit::max(
                routes::wasm_upload::MAX_WASM_MODULE_BYTES,
            )),
        )
        .route("/v1/cdc/{collection}/poll", get(routes::cdc::poll_changes))
        .route(
            "/v1/streams/{stream}/poll",
            get(routes::stream_poll::poll_stream),
        );

    json_routes = json_routes
        .route(
            "/v1/obsv/api/v1/query",
            get(routes::promql::instant_query).post(routes::promql::instant_query),
        )
        .route(
            "/v1/obsv/api/v1/query_range",
            get(routes::promql::range_query).post(routes::promql::range_query),
        )
        .route("/v1/obsv/api/v1/series", get(routes::promql::series_query))
        .route("/v1/obsv/api/v1/labels", get(routes::promql::label_names))
        .route(
            "/v1/obsv/api/v1/label/{name}/values",
            get(routes::promql::label_values),
        )
        .route(
            "/v1/obsv/api/v1/status/buildinfo",
            get(routes::promql::buildinfo),
        )
        .route("/v1/obsv/api/v1/metadata", get(routes::promql::metadata))
        .route("/v1/obsv/api/v1/write", post(routes::promql::remote_write))
        .route("/v1/obsv/api/v1/read", post(routes::promql::remote_read))
        .route(
            "/v1/obsv/api/v1/annotations",
            post(routes::promql::annotations),
        );

    // Stamp the v1 vendor Content-Type on every response from JSON routes.
    let json_routes = json_routes.layer(axum::middleware::map_response(stamp_content_type));

    // ── Probe routes (unversioned, always reachable) ──────────────────────────
    let probe_routes = Router::new()
        .route("/healthz", get(routes::health::healthz))
        .route("/health/live", get(routes::health::live))
        .route("/health/ready", get(routes::health::ready))
        .route("/health/drain", post(routes::health::drain))
        .route("/metrics", get(routes::metrics::metrics));

    probe_routes
        .merge(json_routes)
        .merge(streaming_routes)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            startup_gate_middleware,
        ))
        .with_state(state)
}

/// Axum middleware that gates non-health routes on [`StartupPhase::GatewayEnable`].
///
/// All `/health*` paths (liveness, readiness, drain) are always let through so
/// k8s probes can observe startup progress. All other routes receive a
/// `503 Service Unavailable` until the node reaches `GatewayEnable`.
async fn startup_gate_middleware(
    State(app_state): State<AppState>,
    req: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let path = req.uri().path();
    // Health-probe paths bypass the gate — these must be reachable during startup.
    let is_health_path = path == "/healthz" || path.starts_with("/health/");

    if !is_health_path {
        let gate = &app_state.shared.startup;
        let snap = gate.current_phase();
        if let Some(err) = gate.is_failed() {
            let body = serde_json::json!({
                "status": "failed",
                "error": err.to_string(),
            });
            return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
        }
        if snap < crate::control::startup::StartupPhase::GatewayEnable {
            let body = serde_json::json!({
                "status": "starting",
                "phase": snap.name(),
            });
            return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
        }
    }

    next.run(req).await
}

/// Start the HTTP API server from an already-bound [`tokio::net::TcpListener`].
///
/// Alias of [`run`], kept for call sites (tests, harnesses) that bind an
/// ephemeral-port listener first so the port is known without a race.
pub async fn run_with_listener(
    listener: tokio::net::TcpListener,
    shared: Arc<SharedState>,
    auth_mode: AuthMode,
    tls_settings: Option<&crate::config::server::TlsSettings>,
    bus: crate::control::shutdown::ShutdownBus,
) -> crate::Result<()> {
    run(listener, shared, auth_mode, tls_settings, bus).await
}

/// Start the HTTP API server (plain HTTP or HTTPS) on a pre-bound listener.
///
/// The socket is bound by `bootstrap::listeners::bind_listeners` before any
/// accept loop starts, so a port conflict is boot-fatal rather than an error
/// logged from inside an already-detached task. If `tls_settings` is provided,
/// serves HTTPS via axum-server + rustls; otherwise plain HTTP via axum::serve.
pub async fn run(
    listener: tokio::net::TcpListener,
    shared: Arc<SharedState>,
    auth_mode: AuthMode,
    tls_settings: Option<&crate::config::server::TlsSettings>,
    bus: crate::control::shutdown::ShutdownBus,
) -> crate::Result<()> {
    // HTTP graceful shutdown may retain active connections for up to five
    // seconds, so it must remain a barrier before later shutdown phases.
    let drain_guard = bus.register_critical_task(
        crate::control::shutdown::ShutdownPhase::DrainingListeners,
        "http",
    );
    let mut shutdown_rx = bus.handle().flat_watch().raw_receiver();

    // A top-level listener plans with descriptor leases, the array catalog
    // and the WAL handle, same as a pgwire connection.
    let query_ctx =
        Arc::new(crate::control::planner::context::QueryContext::for_state_with_lease(&shared));
    let state = AppState {
        shared,
        auth_mode,
        // Share the coordinator that owns this listener's drain guard.
        shutdown_bus: bus.clone(),
        query_ctx,
    };
    let router = build_router(state);
    let local_addr = listener.local_addr()?;

    if let Some(tls) = tls_settings {
        // HTTPS via axum-server + rustls, on the pre-bound socket.
        let rustls_config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path)
                .await
                .map_err(|e| crate::Error::Config {
                    detail: format!("HTTP TLS config error: {e}"),
                })?;

        info!(%local_addr, tls = true, "HTTPS API server listening");

        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            let _ = shutdown_rx.changed().await;
            shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(5)));
        });

        // `into_std` keeps the socket in nonblocking mode, which is what
        // axum-server needs to hand it back to Tokio.
        //
        // The acceptor is NodeDB's own rather than `from_tcp_rustls`'s: it
        // wraps the same rustls handshake but reads the negotiated version off
        // the accepted stream and stamps it onto every request served over
        // that connection, which is the only point where the TLS session is
        // still reachable.
        axum_server::from_tcp(listener.into_std()?)
            .map_err(crate::Error::Io)?
            .acceptor(super::tls_accept::TransportCapturingAcceptor::new(
                rustls_config,
            ))
            .handle(handle)
            .serve(router.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await
            .map_err(crate::Error::Io)?;
    } else {
        // Plain HTTP.
        info!(%local_addr, "HTTP API server listening");

        // A plaintext listener has one fixed transport, so it is stamped on
        // the router rather than per connection. Every request must carry the
        // extension on both paths — routes fail closed without it.
        let router = router.layer(axum::Extension(
            crate::control::security::tls_policy::TransportSecurity::Cleartext,
        ));

        // `with_connect_info` is required so routes that take
        // `ConnectInfo<SocketAddr>` (e.g. `/api/auth/session` for the
        // fingerprint-bound session handle path) resolve the peer address.
        // Without it, axum rejects those requests with 500.
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.changed().await;
        })
        .await
        .map_err(crate::Error::Io)?;
    }

    drain_guard.report_drained();
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::{Body, Bytes};
    use axum::extract::DefaultBodyLimit;
    use axum::http::{Method, Request, StatusCode};
    use axum::routing::{get, post, put};
    use tower::ServiceExt;

    use crate::control::server::http::routes;

    /// Build a stub router with the same path registrations as the real
    /// server but using trivial `StatusCode::OK` handlers. Used to assert
    /// the route table itself.
    fn stub_router() -> Router {
        async fn ok() -> StatusCode {
            StatusCode::OK
        }

        async fn wasm_upload(_: Bytes) -> StatusCode {
            StatusCode::OK
        }

        Router::new()
            // Probe routes (unversioned).
            .route("/healthz", get(ok))
            .route("/health/live", get(ok))
            .route("/health/ready", get(ok))
            .route("/health/drain", post(ok))
            .route("/metrics", get(ok))
            // Versioned API routes.
            .route("/v1/query", post(ok))
            .route("/v1/query/stream", post(ok))
            .route("/v1/status", get(ok))
            .route("/v1/cluster/status", get(ok))
            .route("/v1/cluster/debug/raft/{group_id}", get(ok))
            .route("/v1/cluster/debug/transport", get(ok))
            .route("/v1/cluster/debug/catalog/descriptors", get(ok))
            .route("/v1/cluster/debug/leases", get(ok))
            .route("/v1/auth/exchange-key", post(ok))
            .route("/v1/auth/session", post(ok).delete(ok))
            .route("/v1/collections/{name}/crdt/apply", post(ok))
            .route(
                "/v1/functions/{name}/wasm",
                put(wasm_upload).layer(DefaultBodyLimit::max(
                    routes::wasm_upload::MAX_WASM_MODULE_BYTES,
                )),
            )
            .route("/v1/ws", get(ok))
            .route("/v1/streams/{stream}/poll", get(ok))
            .route("/v1/streams/{stream}/events", get(ok))
            .route("/v1/cdc/{collection}", get(ok))
            .route("/v1/cdc/{collection}/poll", get(ok))
            // PromQL nested under /v1/obsv.
            .route("/v1/obsv/api/v1/query", get(ok).post(ok))
    }

    async fn status_for(router: &Router, method: Method, path: &str) -> StatusCode {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .unwrap();
        router.clone().oneshot(req).await.unwrap().status()
    }

    #[tokio::test]
    async fn probe_paths_unversioned() {
        let r = stub_router();
        assert_eq!(
            status_for(&r, Method::GET, "/healthz").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::GET, "/health/live").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::GET, "/health/ready").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::POST, "/health/drain").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::GET, "/metrics").await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn v1_routes_exist() {
        let r = stub_router();
        assert_eq!(
            status_for(&r, Method::POST, "/v1/query").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::POST, "/v1/query/stream").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::GET, "/v1/status").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::GET, "/v1/cluster/status").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::POST, "/v1/auth/exchange-key").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::POST, "/v1/auth/session").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::DELETE, "/v1/auth/session").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::POST, "/v1/collections/mycoll/crdt/apply").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::PUT, "/v1/functions/my_func/wasm").await,
            StatusCode::OK
        );
        assert_eq!(status_for(&r, Method::GET, "/v1/ws").await, StatusCode::OK);
        assert_eq!(
            status_for(&r, Method::GET, "/v1/streams/mystream/poll").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::GET, "/v1/streams/mystream/events").await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn wasm_upload_route_rejects_oversized_bodies() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/v1/functions/my_func/wasm")
            .body(Body::from(vec![
                0;
                routes::wasm_upload::MAX_WASM_MODULE_BYTES
                    + 1
            ]))
            .expect("request");

        let response = stub_router().oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn promql_route_exists() {
        let r = stub_router();
        assert_eq!(
            status_for(&r, Method::POST, "/v1/obsv/api/v1/query").await,
            StatusCode::OK
        );
        assert_eq!(
            status_for(&r, Method::GET, "/v1/obsv/api/v1/query").await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn versioned_cdc_routes_exist() {
        let r = stub_router();
        assert_eq!(
            status_for(&r, Method::GET, "/v1/cdc/mycoll").await,
            StatusCode::OK,
        );
        assert_eq!(
            status_for(&r, Method::GET, "/v1/cdc/mycoll/poll").await,
            StatusCode::OK,
        );
    }
}
