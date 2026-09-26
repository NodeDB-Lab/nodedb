// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal audit-retention enforcement loop.
//!
//! Runs on the Event Plane (Tokio, `Send + Sync`). Every tick:
//!
//! 1. Snapshot the [`BitemporalRetentionRegistry`].
//! 2. For each entry with `audit_retain_ms > 0`, compute
//!    `cutoff_system_ms = now - audit_retain_ms`.
//! 3. Dispatch the appropriate `MetaOp::TemporalPurge{EdgeStore,
//!    DocumentStrict, Columnar}` to the owning Data Plane core via the
//!    shared `sync_dispatch` async path.
//! 4. On success, parse the returned purge count from the response
//!    payload and append a `RecordType::TemporalPurge` record to the
//!    WAL for durable audit.
//!
//! NEVER does storage I/O directly — all physical work happens in the
//! Data Plane. The only persistent side-effect emitted from this loop
//! is the WAL audit record.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{info, warn};

use super::registry::{BitemporalEngineKind, BitemporalRetentionRegistry, Entry};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::MetaOp;

/// Default tick interval when no shorter deadline is needed. One hour
/// matches the timeseries retention loop's default; operators can lower
/// by providing a shorter `tick_interval`.
const DEFAULT_TICK_MS: u64 = 3_600_000;

/// Dispatch deadline per purge op. Purges can scan many segments /
/// redb ranges; a 30-second bound matches the existing retention path.
const DISPATCH_DEADLINE_SECS: u64 = 30;

/// Startup delay so the loop doesn't race the Data Plane warm-up.
const STARTUP_DELAY_SECS: u64 = 10;

/// Spawn the bitemporal-retention enforcement loop as a background
/// Tokio task. Returns a `JoinHandle` for shutdown coordination.
pub fn spawn_bitemporal_retention_loop(
    shared_state: Arc<SharedState>,
    registry: Arc<BitemporalRetentionRegistry>,
    shutdown: watch::Receiver<bool>,
    tick_interval: Option<Duration>,
) -> tokio::task::JoinHandle<()> {
    let tick = tick_interval.unwrap_or(Duration::from_millis(DEFAULT_TICK_MS));
    tokio::spawn(async move {
        enforcement_loop(shared_state, registry, shutdown, tick).await;
    })
}

async fn enforcement_loop(
    state: Arc<SharedState>,
    registry: Arc<BitemporalRetentionRegistry>,
    mut shutdown: watch::Receiver<bool>,
    tick: Duration,
) {
    // Shutdown ends the startup delay: a server stopped within it must not
    // wait it out.
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(STARTUP_DELAY_SECS)) => {}
        _ = shutdown.wait_for(|stopping| *stopping) => {
            info!("bitemporal retention loop shutting down");
            return;
        }
    }

    loop {
        tokio::select! {
            _ = tokio::time::sleep(tick) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("bitemporal retention loop shutting down");
                    return;
                }
            }
        }

        let entries = registry.snapshot();
        if entries.is_empty() {
            continue;
        }
        for entry in entries {
            run_one(&state, &entry).await;
        }
    }
}

async fn run_one(state: &Arc<SharedState>, entry: &Entry) {
    let audit_ms = entry.retention.audit_retain_ms;
    if audit_ms == 0 {
        return; // "retain forever" — no purge.
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let cutoff_system_ms = now_ms.saturating_sub(audit_ms as i64);

    let tenant_id = entry.tenant_id;
    let plan = match entry.engine {
        BitemporalEngineKind::EdgeStore => PhysicalPlan::Meta(MetaOp::TemporalPurgeEdgeStore {
            tenant_id: tenant_id.as_u64(),
            collection: nodedb_types::QualifiedCollection::new(
                entry.database_id,
                &entry.collection,
            ),
            cutoff_system_ms,
        }),
        BitemporalEngineKind::DocumentStrict => {
            PhysicalPlan::Meta(MetaOp::TemporalPurgeDocumentStrict {
                tenant_id: tenant_id.as_u64(),
                collection: nodedb_types::QualifiedCollection::new(
                    entry.database_id,
                    &entry.collection,
                ),
                cutoff_system_ms,
            })
        }
        BitemporalEngineKind::Columnar => PhysicalPlan::Meta(MetaOp::TemporalPurgeColumnar {
            tenant_id: tenant_id.as_u64(),
            collection: nodedb_types::QualifiedCollection::new(
                entry.database_id,
                &entry.collection,
            ),
            cutoff_system_ms,
        }),
        BitemporalEngineKind::Crdt => PhysicalPlan::Meta(MetaOp::TemporalPurgeCrdt {
            tenant_id: tenant_id.as_u64(),
            collection: nodedb_types::QualifiedCollection::new(
                entry.database_id,
                &entry.collection,
            ),
            cutoff_system_ms,
        }),
        // For Array entries `entry.collection` carries the array_id (see
        // hydration in main.rs — registered under the catalog `name` field).
        BitemporalEngineKind::Array => PhysicalPlan::Meta(MetaOp::TemporalPurgeArray {
            tenant_id: tenant_id.as_u64(),
            array_id: entry.collection.clone(),
            cutoff_system_ms,
        }),
    };

    match crate::control::server::shared::ddl::sync_dispatch::dispatch_system(
        state,
        crate::control::server::shared::ddl::sync_dispatch::SystemTask::new(
            crate::control::server::shared::ddl::sync_dispatch::SystemReason::RetentionEnforcement,
            tenant_id,
            entry.database_id,
            &entry.collection,
            plan,
        ),
        Duration::from_secs(DISPATCH_DEADLINE_SECS),
    )
    .await
    {
        Ok(payload) => {
            let purged = parse_count_from_payload(entry.engine, &payload);
            if purged > 0
                && let Err(e) = state
                    .wal
                    .appender(crate::wal::manager::NO_APPLY_KEY)
                    .append_temporal_purge(
                        tenant_id,
                        entry.engine.wire_tag(),
                        &entry.collection,
                        cutoff_system_ms,
                        purged,
                    )
            {
                warn!(
                    tenant = tenant_id.as_u64(),
                    collection = %entry.collection,
                    error = %e,
                    "temporal-purge wal append failed"
                );
            }
            if purged > 0 {
                info!(
                    tenant = tenant_id.as_u64(),
                    collection = %entry.collection,
                    engine = ?entry.engine,
                    purged,
                    cutoff_ms = cutoff_system_ms,
                    "bitemporal audit-retention purge"
                );
            }
        }
        Err(e) => {
            warn!(
                tenant = tenant_id.as_u64(),
                collection = %entry.collection,
                engine = ?entry.engine,
                error = %e,
                "bitemporal temporal-purge dispatch failed"
            );
        }
    }
}

/// Decode the purge count from a response payload.
///
/// EdgeStore / Columnar → 8 bytes LE u64 (count).
/// DocumentStrict → 16 bytes: docs LE u64 ++ index-entries LE u64; we
///   sum them for the audit-record count so operators see total rows
///   reclaimed across the two versioned tables.
fn parse_count_from_payload(engine: BitemporalEngineKind, payload: &[u8]) -> u64 {
    match engine {
        BitemporalEngineKind::EdgeStore
        | BitemporalEngineKind::Columnar
        | BitemporalEngineKind::Crdt
        | BitemporalEngineKind::Array => {
            if payload.len() >= 8 {
                u64::from_le_bytes(payload[..8].try_into().unwrap_or([0; 8]))
            } else {
                0
            }
        }
        BitemporalEngineKind::DocumentStrict => {
            if payload.len() >= 16 {
                let docs = u64::from_le_bytes(payload[..8].try_into().unwrap_or([0; 8]));
                let idx = u64::from_le_bytes(payload[8..16].try_into().unwrap_or([0; 8]));
                docs.saturating_add(idx)
            } else {
                0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_count_edgestore_8_bytes() {
        let payload = 42u64.to_le_bytes().to_vec();
        assert_eq!(
            parse_count_from_payload(BitemporalEngineKind::EdgeStore, &payload),
            42
        );
    }

    #[test]
    fn parse_count_columnar_8_bytes() {
        let payload = 7u64.to_le_bytes().to_vec();
        assert_eq!(
            parse_count_from_payload(BitemporalEngineKind::Columnar, &payload),
            7
        );
    }

    #[test]
    fn parse_count_document_strict_sums_docs_and_idx() {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&5u64.to_le_bytes());
        payload.extend_from_slice(&11u64.to_le_bytes());
        assert_eq!(
            parse_count_from_payload(BitemporalEngineKind::DocumentStrict, &payload),
            16
        );
    }

    #[test]
    fn parse_count_array_8_bytes() {
        let payload = 17u64.to_le_bytes().to_vec();
        assert_eq!(
            parse_count_from_payload(BitemporalEngineKind::Array, &payload),
            17
        );
    }

    #[test]
    fn parse_count_truncated_returns_zero() {
        assert_eq!(
            parse_count_from_payload(BitemporalEngineKind::EdgeStore, &[1, 2, 3]),
            0
        );
        assert_eq!(
            parse_count_from_payload(BitemporalEngineKind::DocumentStrict, &[1; 8]),
            0
        );
    }
}
