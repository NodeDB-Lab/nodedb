// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of restored CRDT tenant state.
//!
//! The snapshot-install path lands CRDT state via a per-node DIRECT dispatch:
//! `RestoreTenantSnapshot` → `restore_crdt_state` → `import_snapshot_bytes`.
//! That dispatch is race-prone — on a freshly spawned cluster, if a data
//! group has not yet elected a leader at restore time, that group's nodes are
//! skipped and a later read returns `NotFound` — and it is not durable across
//! restart (no WAL record, no Raft entry).
//!
//! RESTORE instead re-issues each collection's Loro snapshot durably through
//! Raft, branching on cluster vs single-node exactly like the columnar /
//! timeseries reissue:
//!
//! - Cluster (`async_raft_proposer` present): build a `ReplicatedEntry` via
//!   `to_replicated_entry` (which maps `CrdtOp::ImportSnapshot` →
//!   `ReplicatedWrite::CrdtImportCollection`) and propose it through Raft. Every
//!   replica of the data group applies `import_snapshot_bytes`, a monotonic,
//!   idempotent, commutative Loro merge that converges deterministically.
//! - Single-node: WAL-append the plan (durable for restart replay), then
//!   dispatch it into the Data Plane so it is installed live.
//!
//! Each collection owns its own Loro doc, so routing is exact: a snapshot for
//! `(tenant, collection)` is issued to the single data group that owns that
//! collection's vshard — no multi-group fan-out or representative selection.

use std::time::Duration;

use nodedb_types::id::DatabaseId;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::server::wal_dispatch::wal_append_if_write;
use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::CrdtOp;

/// Per-import dispatch timeout. Generous: a collection's Loro snapshot may be
/// large.
const REISSUE_TIMEOUT: Duration = Duration::from_secs(120);

/// Re-issue one collection's snapshot import to the data group owning its
/// vshard.
///
/// Branches identically to a normal write (and to `reissue_timeseries_durably`):
/// - Cluster: `to_replicated_entry` + `propose_replicated_entry`.
/// - Single-node: `wal_append_if_write` then `sync_dispatch::dispatch_async`.
async fn reissue_crdt_collection(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    bytes: Vec<u8>,
) -> crate::Result<()> {
    let vshard = VShardId::from_collection_in_database(database_id, collection);
    let plan = PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
        tenant_id: tenant_id.as_u64(),
        collection: collection.to_string(),
        bytes,
    });

    if let Some(proposer) = state.async_raft_proposer.get() {
        let entry = crate::control::wal_replication::to_replicated_entry(
            tenant_id,
            database_id,
            vshard,
            &plan,
        )
        .ok_or_else(|| Error::Internal {
            detail: "restore reissue: crdt import did not map to a replicated write".into(),
        })?;
        crate::control::wal_replication::propose_replicated_entry(state, proposer, entry).await?;
        return Ok(());
    }

    // Single-node: WAL first (durable for restart replay), then install live.
    wal_append_if_write(&state.wal, tenant_id, vshard, database_id, &plan)?;
    sync_dispatch::dispatch_async(
        state,
        tenant_id,
        database_id,
        collection,
        plan,
        REISSUE_TIMEOUT,
    )
    .await?;
    Ok(())
}

/// Durably re-issue every restored CRDT collection snapshot.
///
/// `crdt_state` entries are `(tenant_id, collection, snapshot_bytes)`; each is
/// routed to the single data group owning that collection's vshard. Returns the
/// number of imports issued.
pub(crate) async fn reissue_crdt_snapshots(
    state: &SharedState,
    crdt_state: Vec<(u64, u64, String, Vec<u8>)>,
) -> crate::Result<usize> {
    let mut imported = 0usize;

    for (database_id, tid, collection, bytes) in crdt_state {
        reissue_crdt_collection(
            state,
            TenantId::new(tid),
            DatabaseId::new(database_id),
            &collection,
            bytes,
        )
        .await?;
        imported += 1;
    }

    Ok(imported)
}
