// SPDX-License-Identifier: BUSL-1.1

//! `GET /v1/cluster/debug/quarantined-segments` — list segments quarantined
//! due to repeated CRC failures.

use axum::extract::State;
use axum::response::Response;

use super::super::super::auth::{AppState, ResolvedIdentity};
use super::super::super::peer::PeerAddr;
use super::guard::{ensure_debug_access, ok_json};

/// Individual entry in the quarantine list.
///
/// `database_id`, `tenant_id`, and `collection` are `null` for a Raft
/// snapshot chunk. `field` is set only for an FTS field index.
#[derive(serde::Serialize)]
struct QuarantinedSegmentEntry {
    engine: &'static str,
    database_id: Option<u64>,
    tenant_id: Option<u64>,
    collection: Option<String>,
    field: Option<String>,
    segment_id: String,
    quarantined_at_unix_ms: u64,
    last_error_summary: String,
    strikes: u32,
}

/// Response shape for `GET /v1/cluster/debug/quarantined-segments`.
#[derive(serde::Serialize)]
struct QuarantinedSegmentsResponse {
    segments: Vec<QuarantinedSegmentEntry>,
}

/// GET /v1/cluster/debug/quarantined-segments
pub async fn quarantined_segments(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
) -> Response {
    if let Some(resp) = ensure_debug_access(&state, &identity, peer.as_str()) {
        return resp;
    }

    let snapshot = state.shared.quarantine_registry.quarantined_snapshot();
    let mut entries: Vec<QuarantinedSegmentEntry> = snapshot
        .into_iter()
        .map(|s| {
            let scope = s.key.scope();
            QuarantinedSegmentEntry {
                engine: s.key.engine().as_str(),
                database_id: scope.map(|sc| sc.database_id),
                tenant_id: scope.map(|sc| sc.tenant_id),
                collection: scope.map(|sc| sc.collection.clone()),
                field: s.key.field().map(str::to_string),
                segment_id: s.key.segment_label(),
                quarantined_at_unix_ms: s.quarantined_at_unix_ms,
                last_error_summary: s.last_error_summary,
                strikes: s.strikes,
            }
        })
        .collect();
    // Deterministic order: engine, database, tenant, collection, field, segment.
    entries.sort_by(|a, b| {
        a.engine
            .cmp(b.engine)
            .then_with(|| a.database_id.cmp(&b.database_id))
            .then_with(|| a.tenant_id.cmp(&b.tenant_id))
            .then_with(|| a.collection.cmp(&b.collection))
            .then_with(|| a.field.cmp(&b.field))
            .then_with(|| a.segment_id.cmp(&b.segment_id))
    });

    ok_json(&QuarantinedSegmentsResponse { segments: entries })
}
