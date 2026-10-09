// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of restored CRDT tenant state.
//!
//! Direct-dispatch snapshot install (`RestoreTenantSnapshot` →
//! `import_snapshot_bytes`) is race-prone on a freshly spawned cluster (a
//! leaderless group is skipped) and not durable across restart. RESTORE
//! instead re-issues each collection's Loro snapshot through Raft, routed to
//! the vshard that owns it.

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::TenantId;
use nodedb_physical::physical_plan::CrdtOp;

use super::target::{DatabaseTarget, RestoredName};

/// Re-issue one collection's snapshot import to the data group owning its
/// vshard.
///
/// Proposes the import as a normal replicated write does (and as
/// `durable::reissue_plan_durably` does): `to_replicated_entry` +
/// `propose_replicated_entry`. The entry carries `target.restore_id`, so
/// every replica marks the write under that restore.
async fn reissue_crdt_collection(
    state: &SharedState,
    tenant_id: TenantId,
    target: DatabaseTarget,
    name: RestoredName,
    bytes: Vec<u8>,
) -> crate::Result<()> {
    let database_id = target.dest;
    let vshard = name.key(database_id).vshard();
    let plan = PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
        tenant_id: tenant_id.as_u64(),
        collection: name.stored,
        bytes,
    });

    let proposer = state.async_raft_proposer()?;
    let entry = crate::control::wal_replication::to_replicated_entry(
        tenant_id,
        database_id,
        vshard,
        &crate::control::wal_replication::ReplicableWrite::decide_for_replication(&plan)?,
    )?
    .ok_or_else(|| Error::Internal {
        detail: "restore reissue: crdt import did not map to a replicated write".into(),
    })?
    .with_event_source(EventSource::Restore)
    .with_restore_id(target.restore_id);
    let deadline = crate::control::wal_replication::statement_propose_deadline(state);
    crate::control::wal_replication::propose_replicated_entry(state, proposer, entry, deadline)
        .await?;
    Ok(())
}

/// Durably re-issue every restored CRDT collection snapshot of one database.
///
/// `crdt_state` entries are `(database_id, tenant_id, collection,
/// snapshot_bytes)`, the collection named as the source Data Plane stored it.
/// Each is routed to the single data group owning its destination
/// collection's vshard. Returns the number of imports issued.
pub(crate) async fn reissue_crdt_snapshots(
    state: &SharedState,
    target: DatabaseTarget,
    crdt_state: Vec<(u64, u64, String, Vec<u8>)>,
) -> crate::Result<usize> {
    let mut imported = 0usize;

    for (database_id, tid, collection, bytes) in crdt_state {
        if database_id != target.source.as_u64() {
            return Err(Error::Internal {
                detail: format!(
                    "invalid backup format: CRDT state of '{collection}' names database \
                     {database_id}, but sits with database {}",
                    target.source.as_u64()
                ),
            });
        }
        let name = target.resolve(&collection)?;
        reissue_crdt_collection(state, TenantId::new(tid), target, name, bytes).await?;
        imported += 1;
    }

    Ok(imported)
}
