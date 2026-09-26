// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of restored CRDT tenant state.
//!
//! Direct-dispatch snapshot install (`RestoreTenantSnapshot` →
//! `import_snapshot_bytes`) is race-prone on a freshly spawned cluster (a
//! leaderless group is skipped) and not durable across restart. RESTORE
//! instead re-issues each collection's Loro snapshot through Raft (cluster)
//! or WAL + live dispatch (single-node), routed to the vshard that owns it.

use std::time::Duration;

use nodedb_types::id::DatabaseId;

use crate::Error;
use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::server::dispatch_utils::{AutocommitWrite, dispatch_autocommit_write};
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::CrdtOp;

/// Per-import dispatch timeout. Generous: a collection's Loro snapshot may be
/// large.
const REISSUE_TIMEOUT: Duration = Duration::from_secs(120);

/// Re-issue one collection's snapshot import to the data group owning its
/// vshard.
///
/// Branches identically to a normal write (and to `durable::reissue_plan_durably`):
/// - Cluster: `to_replicated_entry` + `propose_replicated_entry`.
/// - Single-node: the autocommit funnel appends the redo and installs it.
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
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
        bytes,
    });

    if let Some(proposer) = state.async_raft_proposer() {
        let entry = crate::control::wal_replication::to_replicated_entry(
            tenant_id,
            database_id,
            vshard,
            &crate::control::wal_replication::ReplicableWrite::decide_for_replication(&plan)?,
        )?
        .ok_or_else(|| Error::Internal {
            detail: "restore reissue: crdt import did not map to a replicated write".into(),
        })?;
        crate::control::wal_replication::propose_replicated_entry(state, proposer, entry).await?;
        return Ok(());
    }

    // Single-node: hold the frontier slot across WAL append, live import, and
    // the durable-at-ack fsync barrier. The clustered branch above is already
    // sequenced by its public proposer.
    state
        .vshard_admission_sequencer
        .run(vshard, || async {
            let response = tokio::time::timeout(
                REISSUE_TIMEOUT,
                dispatch_autocommit_write(
                    state,
                    AutocommitWrite {
                        tenant_id,
                        database_id,
                        vshard_id: vshard,
                        plan,
                        trace_id: crate::types::TraceId::ZERO,
                        event_source: EventSource::CrdtSync,
                        txn_id: None,
                    },
                ),
            )
            .await
            .map_err(|_| Error::Internal {
                detail: format!(
                    "restore reissue: CRDT import timed out after {}ms",
                    REISSUE_TIMEOUT.as_millis()
                ),
            })??;
            if response.status != Status::Ok {
                return Err(response
                    .error_code
                    .as_deref()
                    .cloned()
                    .map(Error::DataPlane)
                    .unwrap_or_else(|| Error::Internal {
                        detail: "restore reissue: CRDT import failed without an error code".into(),
                    }));
            }
            Ok(())
        })
        .await
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
