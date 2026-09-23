// SPDX-License-Identifier: BUSL-1.1

//! Health check endpoints.
//!
//! | Endpoint          | Method | Purpose                     | k8s probe     |
//! |-------------------|--------|-----------------------------|---------------|
//! | `/healthz`        | GET    | Ready to serve traffic      | readiness     |
//! | `/health/live`    | GET    | Process alive (always 200)  | liveness      |
//! | `/health/ready`   | GET    | WAL recovered               | readiness alt |
//! | `/health/drain`   | POST   | Trigger graceful drain      | preStop hook  |

use std::sync::atomic::Ordering;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use nodedb_cluster::ClusterInfoSnapshot;
use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use serde_json::json;

use super::super::admission::{admit_without_rate_limit, identity_database};
use super::super::auth::{ApiError, AppState, ResolvedIdentity};
use super::super::peer::PeerAddr;

/// GET /health/live — unconditional liveness probe.
///
/// Always returns 200. If this endpoint fails to respond, the
/// process is dead and should be restarted. No internal state is
/// checked — the mere ability to respond proves the event loop and
/// HTTP listener are alive.
pub async fn live() -> impl IntoResponse {
    (StatusCode::OK, axum::Json(json!({ "status": "alive" })))
}

/// GET /healthz — k8s-style readiness probe.
///
/// Returns `200 OK` when the node has reached `GatewayEnable`, is
/// serving traffic, is NOT draining/decommissioned, and — on a node that
/// runs a Calvin sequencer — can actually sequence a cross-shard write.
/// Returns `503 Service Unavailable` otherwise.
///
/// Every condition is evaluated live on each call, not latched: sequencer
/// leadership and the epoch seed can both be lost long after `GatewayEnable`.
pub async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    // The coordinator signals this canonical watch before progressing drain
    // phases, so readiness must fail immediately even before lifecycle state
    // has been updated by other shutdown participants.
    if state.shared.shutdown.is_shutdown() {
        let body = json!({
            "status": "draining",
            "reason": "shutdown_signaled",
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body));
    }

    // One snapshot, shared by the decommission check and the sequencer
    // serve-readiness check below — it walks every hosted Raft group, and
    // `/healthz` is polled.
    let cluster = state
        .shared
        .cluster_observer
        .get()
        .map(|obs| obs.snapshot());

    // Check decommission state via the cluster observer (if present).
    if let Some(snap) = cluster.as_ref() {
        let label = snap.lifecycle_label();
        if label == "draining" || label == "decommissioned" || label == "failed" {
            let body = json!({
                "status": "draining",
                "lifecycle": label,
                "node_id": state.shared.node_id,
            });
            return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body));
        }
    }
    // A permanently wedged metadata applier is invisible to the startup gate:
    // the node booted cleanly and only stopped making progress afterwards. It
    // must fail readiness anyway, or it keeps taking traffic that can only end
    // in a descriptor-lease timeout naming nothing about the real cause.
    if let Some(report) = state.shared.metadata_apply_wedge.report() {
        let body = json!({
            "status": "failed",
            "reason": "metadata_apply_wedged",
            "node_id": state.shared.node_id,
            "raft_index": report.raft_index,
            "last_applied_watermark": report.last_applied_watermark,
            "entry_kind": report.entry_kind,
            "error": report.error,
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body));
    }

    // A halted sequencer leaves the node serving everything that does not route
    // through Calvin, so the startup gate and every read path still look fine.
    // Report it anyway: silently dropping a whole write class is exactly what an
    // operator needs told, and it is the reason nothing takes this node out of
    // rotation on its own.
    if let Some(halt) = state.shared.sequencer_halt.report() {
        let body = json!({
            "status": "degraded",
            "reason": "sequencer_halted",
            "node_id": state.shared.node_id,
            "expected_epoch": halt.expected_epoch,
            "found_epoch": halt.found_epoch,
            "txns_in_batch": halt.txns_in_batch,
            "raft_index": halt.raft_index,
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body));
    }

    // A halted Calvin scheduler holds one vShard's sequenced txns unapplied.
    // The node serves everything else, so it reports degraded, like a halted
    // sequencer.
    if let Some(halt) = state.shared.sequencer_halt.apply_halt().report() {
        let body = json!({
            "status": "degraded",
            "reason": "calvin_apply_halted",
            "node_id": state.shared.node_id,
            "vshard_id": halt.vshard_id,
            "epoch": halt.epoch,
            "position": halt.position,
            "halt_reason": halt.reason,
            "step": halt.step,
            "error": halt.error,
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body));
    }

    // A fail-stopped core refuses every request routed to it: its state is
    // unknown until restart. The other cores serve, so the node is degraded.
    if let Some(stops) = state
        .shared
        .system_metrics
        .as_ref()
        .map(|metrics| &metrics.core_fail_stops)
        && let Some(report) = stops.report()
    {
        let (status, mut body) = crate::control::metrics::system::core_fail_stop::to_http_response(
            report,
            stops.stopped_cores(),
        );
        body["node_id"] = json!(state.shared.node_id);
        return (status, axum::Json(body));
    }

    // A core that stops completing event-loop iterations panics nothing, so
    // the per-core panic watchdog stays quiet and every other check above
    // still passes. Fail readiness and name the cores: work routed to a
    // stalled core only ever ends in a deadline expiry that says nothing
    // about which core stopped.
    if let Some(stalled_cores) = state.shared.core_stall.report() {
        let (status, mut body) =
            crate::control::cluster::core_stall::to_http_response(&stalled_cores);
        body["node_id"] = json!(state.shared.node_id);
        return (status, axum::Json(body));
    }

    let health = crate::control::startup::health::observe(&state.shared.startup);
    let (status, body) = crate::control::startup::health::to_http_response(&health);
    // Checked only once the startup gate is otherwise green, so a node still
    // advancing through phases keeps reporting the phase it is stuck in.
    if status == StatusCode::OK
        && let Some(reason) = sequencer_not_servable(&state, cluster.as_ref())
    {
        let body = json!({
            "status": "starting",
            "reason": reason,
            "node_id": state.shared.node_id,
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body));
    }
    (status, axum::Json(body))
}

/// Why a cross-shard Calvin write would be refused on this node right now,
/// or `None` when one would be accepted.
///
/// Mirrors the two refusals `control::planner::calvin::submit` raises before a
/// transaction ever reaches the inbox, so a client that waits for `/healthz`
/// and then writes cannot be told "ready" and then rejected.
fn sequencer_not_servable(
    state: &AppState,
    cluster: Option<&ClusterInfoSnapshot>,
) -> Option<&'static str> {
    // No Calvin stack on this node (embedded / local boot with the sequencer
    // never started): nothing to wait for, readiness is unchanged.
    state.shared.sequencer_inbox.get()?;

    // Same resolution `submit_calvin_routed` performs: a missing group entry is
    // leader 0, which is exactly the state that refuses a submit.
    let leader = cluster?
        .groups
        .iter()
        .find(|g| g.group_id == SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0);
    if leader == 0 {
        return Some("sequencer_leader_pending");
    }

    // A remote leader owns its own seed and queues the forwarded submit until
    // it holds one; only a submit sequenced HERE needs this node's seed.
    if leader == state.shared.node_id {
        let seeded = state
            .shared
            .sequencer_metrics
            .get()
            .is_some_and(|m| m.epoch_seeded.load(Ordering::Relaxed));
        if !seeded {
            return Some("sequencer_epoch_seed_pending");
        }
    }
    None
}

/// GET /health/ready — readiness check (WAL recovered, cores initialized).
pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let wal_ready = state.shared.wal.next_lsn().as_u64() > 0;
    let status = if wal_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let body = json!({
        "status": if wal_ready { "ready" } else { "not_ready" },
        "wal_lsn": state.shared.wal.next_lsn().as_u64(),
        "node_id": state.shared.node_id,
    });
    (status, axum::Json(body))
}

/// POST /health/drain — trigger graceful connection drain.
///
/// Initiates the shared phased shutdown coordinator. It signals the canonical
/// `ShutdownWatch` and then drives every registered drain phase. Subsequent
/// `/healthz` calls return 503, which causes the k8s readiness probe to fail
/// and the service mesh to stop routing new connections to this node.
///
/// Designed for use as an authenticated Kubernetes `preStop` hook. In
/// password mode, inject `NODEDB_DRAIN_TOKEN` from a Secret containing a
/// superuser credential:
///
/// ```yaml
/// lifecycle:
///   preStop:
///     exec:
///       command:
///         - /bin/sh
///         - -c
///         - >-
///           curl -fsS -X POST
///           -H "Authorization: Bearer ${NODEDB_DRAIN_TOKEN}"
///           http://127.0.0.1:8080/health/drain
/// ```
pub async fn drain(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    // Blacklist + account status, no rate limit: a drain is a one-shot
    // lifecycle action a preStop hook must never see throttled, but a
    // blacklisted IP or suspended/banned account must not be able to take a
    // node out of rotation. Runs before the role check so the refusal is on
    // identity alone, as on every other transport.
    admit_without_rate_limit(
        &state,
        &identity.0,
        identity_database(&identity.0),
        peer.as_str(),
    )?;

    // State-changing administrative health actions require authenticated superuser authority.
    if !identity.0.is_superuser() {
        return Err(ApiError::Forbidden("superuser role required".into()));
    }

    tracing::info!(node_id = state.shared.node_id, "drain requested via HTTP");
    // Dropping a Tokio JoinHandle detaches the coordinator task; shutdown
    // progress remains observable through the shared bus.
    drop(state.shutdown_bus.initiate());
    Ok((
        StatusCode::OK,
        axum::Json(json!({
            "status": "draining",
            "node_id": state.shared.node_id,
        })),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::config::auth::AuthMode;
    use crate::control::cluster::CalvinApplyHalt;
    use crate::control::state::SharedState;
    use crate::wal::WalManager;

    fn app_state(dir: &tempfile::TempDir) -> AppState {
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("health.wal")).expect("open WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).expect("shared state");
        AppState {
            shutdown_bus: crate::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown))
                .0,
            query_ctx: Arc::new(crate::control::planner::context::QueryContext::for_state(
                &shared,
            )),
            shared,
            auth_mode: AuthMode::Trust,
        }
    }

    async fn healthz_body(state: AppState) -> (StatusCode, serde_json::Value) {
        let response = healthz(State(state)).await.into_response();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read healthz body");
        let body = sonic_rs::from_slice::<serde_json::Value>(&bytes).expect("healthz body is JSON");
        (status, body)
    }

    #[tokio::test]
    async fn healthz_reports_a_halted_calvin_scheduler_as_degraded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = app_state(&dir);
        state
            .shared
            .sequencer_halt
            .apply_halt()
            .record(CalvinApplyHalt {
                vshard_id: 12,
                epoch: 40,
                position: 3,
                reason: "flush_failed",
                step: "flush",
                error: "CalvinFlush returned Error".to_string(),
            });

        let (status, body) = healthz_body(state).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "degraded");
        assert_eq!(body["reason"], "calvin_apply_halted");
        assert_eq!(body["vshard_id"], 12);
        assert_eq!(body["epoch"], 40);
        assert_eq!(body["position"], 3);
        assert_eq!(body["halt_reason"], "flush_failed");
        assert_eq!(body["step"], "flush");
    }

    #[tokio::test]
    async fn healthz_without_a_calvin_halt_names_no_calvin_halt() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = app_state(&dir);

        let (_status, body) = healthz_body(state).await;

        assert_ne!(body["reason"], "calvin_apply_halted");
    }
}
