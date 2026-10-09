// SPDX-License-Identifier: BUSL-1.1

//! The write gate a data-group leader runs on every proposal to its group.
//!
//! A replicated write applies on every replica in log order, with its
//! ordering decided once: on the leader, before the entry enters the log.
//! [`LeaderWriteGate`] admits the entry there with the same gate a local
//! write passes:
//!
//! - Uncontended: the leader proposes the entry, and its keys stay held until
//!   the leader's apply loop starts the entry (see `holds`).
//! - Contended, and the write routes (see `admission_keys`): the propose
//!   refuses with `RouteToSequencer`, and the proposer submits the write to
//!   the Calvin sequencer. An append never routes.
//! - Contended otherwise: the gate waits holding no key, until the keys are
//!   free or the proposer's deadline passes. A forwarded entry carries what
//!   remains of that deadline.
//!
//! Followers apply the committed entry with no gate: the leader ordered it.

use std::sync::{Arc, Weak};

use nodedb_cluster::{CalvinError, ClusterError, DataProposeGate, ProposeHold};

use crate::control::state::SharedState;
use crate::control::wal_replication::decode::decode_parsed_entry;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::types::VShardId;
use nodedb_types::QualifiedCollection;

use super::admission_keys::collection_admission_keys;
use super::gate::{
    WriteAdmission, WriteAdmissionGuard, WriteTarget, admit_request, admit_routed, lock_manager_of,
};
use super::route::calvin_route_keeps;

/// The host side of [`DataProposeGate`]. Weak: `SharedState` outlives the
/// Raft loop that holds the gate only while the node runs.
pub struct LeaderWriteGate {
    state: Weak<SharedState>,
}

impl LeaderWriteGate {
    pub fn new(state: &Arc<SharedState>) -> Self {
        Self {
            state: Arc::downgrade(state),
        }
    }
}

#[async_trait::async_trait]
impl DataProposeGate for LeaderWriteGate {
    async fn admit(
        &self,
        vshard_id: u32,
        entry: &[u8],
        deadline: tokio::time::Instant,
    ) -> nodedb_cluster::Result<Option<Box<dyn ProposeHold>>> {
        // A node that is shutting down proposes nothing its gate must order.
        let Some(state) = self.state.upgrade() else {
            return Ok(None);
        };
        let vshard = VShardId::new(vshard_id);
        // Bytes that are not a replicated entry apply as nothing on any replica.
        let Some(entry) = ReplicatedEntry::from_bytes(entry) else {
            return Ok(None);
        };
        admit_redo_stream(&state, &entry, deadline).map_err(gate_error)?;
        let guard = match admit_entry(&state, vshard, &entry).map_err(gate_error)? {
            WriteAdmission::ExemptRead
            | WriteAdmission::FastPath { guard: None }
            // No scheduler runs for the vShard here: the log alone orders
            // the write.
            | WriteAdmission::FastPathBlocking { .. } => return Ok(None),
            WriteAdmission::FastPath { guard: Some(guard) } => guard,
            WriteAdmission::RouteToCalvin => {
                return Err(ClusterError::Calvin(CalvinError::RouteToSequencer));
            }
            WriteAdmission::Wait(wait) => wait.acquire(deadline).await.map_err(gate_error)?,
        };
        Ok(Some(Box::new(LandedHold {
            state,
            vshard,
            guard,
        })))
    }
}

/// An admitted entry's keys, held until the entry lands in the log.
struct LandedHold {
    state: Arc<SharedState>,
    vshard: VShardId,
    guard: WriteAdmissionGuard,
}

impl ProposeHold for LandedHold {
    fn landed(self: Box<Self>, group_id: u64, log_index: u64) {
        let LandedHold {
            state,
            vshard,
            guard,
        } = *self;
        state
            .calvin
            .admission_holds
            .hold(group_id, log_index, vshard, guard);
    }
}

/// What the gate locks for one replicated entry.
enum EntryScope {
    /// The entry writes no row, or its Calvin scheduler holds its locks.
    Exempt,
    /// The entry writes these collections whole.
    Collections(Vec<String>),
    /// The entry carries a plan the gate locks as it locks a local write.
    Plan,
}

/// Admit the first chunk of a chunked redo stream against this node's open
/// stream bytes. Every replica takes the chunks the log holds, so the leader
/// is the one place a stream is refused for memory. Any other entry passes.
fn admit_redo_stream(
    state: &SharedState,
    entry: &ReplicatedEntry,
    deadline: tokio::time::Instant,
) -> crate::Result<()> {
    let ReplicatedWrite::RedoChunk {
        stream,
        index: 0,
        len,
        ..
    } = &entry.write
    else {
        return Ok(());
    };
    state
        .redo_chunks
        .admit_stream(*stream, *len, deadline.into_std())
        .map_err(crate::Error::from)
}

/// Admit `entry`, proposed to `vshard`'s group.
pub(crate) fn admit_entry(
    state: &SharedState,
    vshard: VShardId,
    entry: &ReplicatedEntry,
) -> crate::Result<WriteAdmission> {
    let database_id = crate::types::DatabaseId::new(entry.database_id);
    match entry_scope(&entry.write, database_id) {
        EntryScope::Exempt => Ok(WriteAdmission::ExemptRead),
        EntryScope::Collections(collections) => Ok(admit_collections(state, vshard, &collections)),
        EntryScope::Plan => {
            let Some((_, (tenant_id, _, plan, _))) = decode_parsed_entry(entry)? else {
                return Ok(WriteAdmission::ExemptRead);
            };
            // An entry the scheduler cannot apply as proposed waits for its
            // keys rather than route.
            let may_route = calvin_route_keeps(
                &plan,
                crate::event::EventSource::from(entry.event_source),
                entry.restore_id,
            );
            admit_routed(
                state,
                &WriteTarget {
                    tenant_id,
                    database_id,
                    vshard_id: vshard,
                    plan: &plan,
                },
                may_route,
            )
        }
    }
}

/// The lock scope of `write`, an entry of `database_id`.
fn entry_scope(write: &ReplicatedWrite, database_id: crate::types::DatabaseId) -> EntryScope {
    match write {
        // Calvin bookkeeping, a backup cut, a topic message, a surrogate
        // binding and an array schema write no row.
        ReplicatedWrite::CalvinReadResult { .. }
        | ReplicatedWrite::CutBarrier { .. }
        | ReplicatedWrite::TopicPublish { .. }
        | ReplicatedWrite::SurrogateBind { .. }
        | ReplicatedWrite::ArraySchema { .. } => EntryScope::Exempt,
        // A Calvin transaction's redo carries its sequencer stamp, and its
        // scheduler holds its locks. A session transaction's redo writes
        // every collection it names.
        ReplicatedWrite::TransactionRedo {
            collections,
            calvin,
            ..
        } => {
            if calvin.is_some() {
                EntryScope::Exempt
            } else {
                EntryScope::Collections(collections.clone())
            }
        }
        // A redo chunk and an abandon write no row: the stream's final entry
        // takes the locks.
        ReplicatedWrite::RedoChunk { .. } | ReplicatedWrite::RedoAbandon { .. } => {
            EntryScope::Exempt
        }
        ReplicatedWrite::ArrayOp { array, .. } => EntryScope::Collections(vec![
            QualifiedCollection::new(database_id, array)
                .as_str()
                .to_owned(),
        ]),
        ReplicatedWrite::PointPut { .. }
        | ReplicatedWrite::PointInsert { .. }
        | ReplicatedWrite::PointDelete { .. }
        | ReplicatedWrite::PointUpdate { .. }
        | ReplicatedWrite::DocUpsert { .. }
        | ReplicatedWrite::DocBatchInsert { .. }
        | ReplicatedWrite::VectorInsert { .. }
        | ReplicatedWrite::VectorBatchInsert { .. }
        | ReplicatedWrite::VectorDelete { .. }
        | ReplicatedWrite::SetVectorParams { .. }
        | ReplicatedWrite::SparseInsert { .. }
        | ReplicatedWrite::SparseDelete { .. }
        | ReplicatedWrite::MultiVectorInsert { .. }
        | ReplicatedWrite::MultiVectorDelete { .. }
        | ReplicatedWrite::DeleteBySurrogate { .. }
        | ReplicatedWrite::DirectUpsert { .. }
        | ReplicatedWrite::CrdtApply { .. }
        | ReplicatedWrite::ColumnarIngest { .. }
        | ReplicatedWrite::TimeseriesIngest { .. }
        | ReplicatedWrite::FtsIndex { .. }
        | ReplicatedWrite::FtsDelete { .. }
        | ReplicatedWrite::SpatialInsert { .. }
        | ReplicatedWrite::SpatialDelete { .. }
        | ReplicatedWrite::EdgePut { .. }
        | ReplicatedWrite::EdgeDelete { .. }
        | ReplicatedWrite::SetNodeLabels { .. }
        | ReplicatedWrite::RemoveNodeLabels { .. }
        | ReplicatedWrite::EdgePutBatch { .. }
        | ReplicatedWrite::EdgeDeleteBatch { .. }
        | ReplicatedWrite::KvPut { .. }
        | ReplicatedWrite::KvDelete { .. }
        | ReplicatedWrite::KvInsert { .. }
        | ReplicatedWrite::KvInsertIfAbsent { .. }
        | ReplicatedWrite::KvInsertOnConflictUpdate { .. }
        | ReplicatedWrite::KvBatchPut { .. }
        | ReplicatedWrite::KvExpire { .. }
        | ReplicatedWrite::KvPersist { .. }
        | ReplicatedWrite::KvIncr { .. }
        | ReplicatedWrite::KvIncrFloat { .. }
        | ReplicatedWrite::KvCas { .. }
        | ReplicatedWrite::KvGetSet { .. }
        | ReplicatedWrite::KvRegisterSortedIndex { .. }
        | ReplicatedWrite::KvDropSortedIndex { .. }
        | ReplicatedWrite::KvRegisterIndex { .. }
        | ReplicatedWrite::KvDropIndex { .. }
        | ReplicatedWrite::KvFieldSet { .. }
        | ReplicatedWrite::KvTransfer { .. }
        | ReplicatedWrite::KvTransferItem { .. }
        | ReplicatedWrite::BulkDml { .. }
        | ReplicatedWrite::ColumnarBulkDml { .. }
        | ReplicatedWrite::InsertSelect { .. }
        | ReplicatedWrite::CrdtImportCollection { .. }
        | ReplicatedWrite::CrdtListInsert { .. }
        | ReplicatedWrite::CrdtListDelete { .. }
        | ReplicatedWrite::CrdtListMove { .. }
        | ReplicatedWrite::CrdtDocUpsert { .. }
        | ReplicatedWrite::CrdtDocDelete { .. }
        | ReplicatedWrite::DocTruncate { .. }
        | ReplicatedWrite::KvTruncate { .. }
        | ReplicatedWrite::ConstraintChange { .. }
        | ReplicatedWrite::ArrayCellPut { .. }
        | ReplicatedWrite::ArrayCellDelete { .. }
        | ReplicatedWrite::CrdtApplyFenced { .. }
        | ReplicatedWrite::CrdtApplyAuthenticated { .. }
        | ReplicatedWrite::DropVectorIndex { .. }
        | ReplicatedWrite::ApplyBalanceDelta { .. }
        | ReplicatedWrite::ColumnarBulkDmlResolved { .. }
        | ReplicatedWrite::ColumnarTruncate { .. }
        | ReplicatedWrite::TimeseriesTruncate { .. }
        | ReplicatedWrite::KvResolvedWrite { .. }
        | ReplicatedWrite::KvPredicateUpdate { .. }
        | ReplicatedWrite::KvPredicateDelete { .. }
        | ReplicatedWrite::DocumentResolvedWrite { .. }
        | ReplicatedWrite::VectorDirectDelete { .. }
        | ReplicatedWrite::VectorDirectTruncate { .. }
        | ReplicatedWrite::VectorDirectUpdate { .. }
        | ReplicatedWrite::VectorResolvedDirectWrite { .. }
        | ReplicatedWrite::MergeApply { .. }
        | ReplicatedWrite::UpdateFromJoinApply { .. } => EntryScope::Plan,
    }
}

/// Admit a write that locks each of `collections` whole.
fn admit_collections(
    state: &SharedState,
    vshard: VShardId,
    collections: &[String],
) -> WriteAdmission {
    match lock_manager_of(state, vshard) {
        Some(lock_manager) => admit_request(
            state,
            vshard,
            lock_manager,
            collection_admission_keys(collections),
        ),
        None => WriteAdmission::FastPath { guard: None },
    }
}

/// The cluster error a gate failure answers the proposer with. A deadline
/// keeps its class across the hop. Every other error crosses typed.
fn gate_error(error: crate::Error) -> ClusterError {
    match error {
        crate::Error::DeadlineExceeded { .. } => {
            ClusterError::Calvin(CalvinError::AdmissionTimedOut)
        }
        other => {
            let detail = format!("data-group write gate: {other}");
            ClusterError::ShardExecution {
                error: Box::new(
                    crate::control::cluster::execution_error_wire::execution_error_to_typed(other),
                ),
                detail,
            }
        }
    }
}
