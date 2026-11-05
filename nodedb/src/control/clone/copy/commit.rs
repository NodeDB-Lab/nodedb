// SPDX-License-Identifier: BUSL-1.1

//! Commit copied rows into a clone as redo records of origin `Restore`.
//!
//! Each record goes through the target vShard's apply log, so every replica
//! installs it. The write funnel raises only a session commit's stamps to its
//! record ordinal (`wal::redo::stamp_floor`). A `Restore` record keeps every
//! stamp it carries, so each copy lands at its source version's ordinal.
//!
//! A copied row passed its rules when the source first wrote it, so the
//! apply runs the UNIQUE check alone, and a copy fires no AFTER trigger.

use nodedb_physical::physical_plan::RedoOrigin;
use nodedb_types::{CollectionKey, DatabaseId, QualifiedCollection, TenantId};

use crate::control::state::SharedState;
use crate::control::wal_replication::transaction_redo::{
    RedoTarget, TransactionRedoPayload, propose_transaction_redo,
};
use crate::event::EventSource;
use crate::types::{HomedRecord, RecordHomes};
use crate::wal::RedoRecord;

use super::rows::{CopyRecordPart, DocCopy, KvCopy};

/// Most rows one copy record carries.
const ROWS_PER_RECORD: usize = 512;

/// The clone collection copied rows install into.
#[derive(Clone, Copy)]
pub struct CopyTarget<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The bare catalog name of the clone collection.
    pub collection: &'a str,
}

/// Install `rows` in `target`, in order, each at its source version's
/// stamps. Returns once every record is durable and installed here.
pub async fn commit_doc_copies(
    state: &SharedState,
    target: CopyTarget<'_>,
    rows: Vec<DocCopy>,
) -> crate::Result<()> {
    commit_copies(state, target, rows, DocCopy::record_part).await
}

/// Install `rows` in `target`, each key's live row at its source ordinal and
/// with its source expiry. Returns once every record is durable and installed
/// here.
pub async fn commit_kv_copies(
    state: &SharedState,
    target: CopyTarget<'_>,
    rows: Vec<KvCopy>,
) -> crate::Result<()> {
    commit_copies(state, target, rows, KvCopy::record_part).await
}

/// Commit `rows` in records of at most [`ROWS_PER_RECORD`] rows, in order.
async fn commit_copies<R>(
    state: &SharedState,
    target: CopyTarget<'_>,
    rows: Vec<R>,
    part: fn(R, &str, &str) -> crate::Result<CopyRecordPart>,
) -> crate::Result<()> {
    let stored = QualifiedCollection::new(target.database_id, target.collection);
    let redo_target = RedoTarget {
        tenant_id: target.tenant_id,
        database_id: target.database_id,
        vshard_id: RecordHomes::of(HomedRecord::Row(CollectionKey::from_bare(
            target.database_id,
            target.collection,
        )))
        .owner(),
        system_ordinal: None,
    };
    let mut pending = rows.into_iter().peekable();
    while pending.peek().is_some() {
        let mut parts = Vec::with_capacity(ROWS_PER_RECORD);
        for row in pending.by_ref().take(ROWS_PER_RECORD) {
            parts.push(part(row, stored.as_str(), target.collection)?);
        }
        let payload = copy_payload(stored.as_str(), parts);
        let deadline =
            crate::control::server::shared::session::StatementDeadline::of_statement(state)
                .tokio_instant();
        propose_transaction_redo(state, redo_target, &payload, deadline).await?;
    }
    Ok(())
}

/// One record of copied rows into the collection `stored`.
fn copy_payload(stored: &str, parts: Vec<CopyRecordPart>) -> TransactionRedoPayload {
    let mut ops = Vec::with_capacity(parts.len());
    let mut identities = Vec::with_capacity(parts.len());
    for part in parts {
        ops.push(part.op);
        identities.push(part.identity);
    }
    TransactionRedoPayload {
        redo: RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            // A copy changes no row a clone reader sees: the row read through
            // the source before it, as the same version.
            row_changes: Vec::new(),
            // A copy writes as the system, which holds no role.
            user_roles: Vec::new(),
        },
        collections: vec![stored.to_string()],
        // A copy writes rows of one user collection.
        incarnation_names: vec![(
            stored.to_string(),
            nodedb_cluster::calvin::types::IncarnationKind::Collection,
        )],
        sum_targets: Vec::new(),
        identities,
        event_source: EventSource::Restore,
        origin: RedoOrigin::Restore,
        calvin: None,
        // A copy installs committed source versions and reads nothing to
        // validate.
        reads: Vec::new(),
        snapshot_ordinal: None,
        // A copy writes rows into the clone.
        adds_storage: true,
    }
}
