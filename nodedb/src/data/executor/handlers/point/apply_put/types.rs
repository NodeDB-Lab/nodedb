// SPDX-License-Identifier: BUSL-1.1

//! Parameter and outcome types for [`CoreLoop::apply_point_put`], plus the
//! enforcement-error mapping shared with the delete path.

use nodedb_types::Surrogate;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::spatial_key::SpatialIndexKey;
use crate::engine::document::store::StorageKey;
use nodedb_physical::physical_plan::ResolvedSumTarget;

/// Parameters for [`CoreLoop::apply_point_put`](crate::data::executor::core_loop::CoreLoop::apply_point_put).
pub(in crate::data::executor) struct PointPutParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub storage_key: StorageKey,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    /// Whether to index the document's text into the inverted BM25 index.
    ///
    /// `true` for native writes (PointPut/Insert/Upsert/batch/insert-select),
    /// which own the full write. `false` for CRDT-sync materialization: that
    /// path receives text via a separate `FtsIndexDoc` sync frame, so indexing
    /// here too would double-index the same surrogate.
    pub index_text: bool,
    /// Roles held by the authenticated user, consumed by role-gated state
    /// transition constraints. Empty for internal/system callers.
    pub user_roles: &'a [String],
    /// Whether to run stateless PUT enforcement (append-only, period lock,
    /// state transitions, transition-check predicates).
    ///
    /// `true` for user-DML callers (PointPut/Insert/Upsert/batch/
    /// insert-select), which must be admission-checked. `false` for
    /// CRDT-sync materialization: those deltas already passed admission on
    /// their origin replica (CRDT constraint validation happens at the Raft
    /// commit phase), so re-running enforcement here would double-check
    /// already-accepted writes.
    pub enforce: bool,
    /// WAL LSN the Control Plane allocated for this write (`None` for writes
    /// with no threaded LSN — e.g. some internal/materialization paths). Used
    /// to advance the checkpoint watermark of any secondary vector index this
    /// document feeds, so startup WAL replay can skip a straddling-segment
    /// record the vector checkpoint already absorbed. On the replay paths this
    /// carries the record's own LSN.
    pub wal_lsn: Option<crate::types::Lsn>,
    /// `(target collection, join-key value)` → target row surrogate, resolved
    /// on the Control Plane at plan time — read by period-lock enforcement to
    /// find its reference row. Empty for a caller whose statement type
    /// resolves nothing, or for `enforce: false` callers, which never read it.
    pub resolved_targets: &'a [ResolvedSumTarget],
}

/// Capture of the mutations an [`CoreLoop::apply_point_put`](crate::data::executor::core_loop::CoreLoop::apply_point_put)
/// performed, so a transactional caller can build an undo entry that fully
/// reverses it.
pub(in crate::data::executor) struct PointPutOutcome {
    /// Prior stored bytes when this put replaced an existing row, else `None`.
    pub prior_value: Option<Vec<u8>>,
    /// The exact bytes this put handed to storage — a Binary Tuple on a strict
    /// collection, MessagePack otherwise, with generated columns evaluated and
    /// `_rowid` injected. A `RETURNING` projection reads THIS, never the
    /// caller's submitted body, so it reports the row that landed rather than
    /// the row that was asked for.
    pub stored_value: Vec<u8>,
    /// System-time key the bitemporal version row (and its versioned index
    /// entries) were appended at. `Some(t)` on the bitemporal branch, `None`
    /// on the plain overwrite branch.
    pub bitemporal_sys_from_ms: Option<i64>,
    /// `(field, value)` pairs whose versioned index entries this op wrote at
    /// `bitemporal_sys_from_ms`. Empty when not bitemporal / none written.
    pub bitemporal_index_tuples: Vec<(String, String)>,
    /// `(field, value)` pairs this op INSERTED into the plain (non-bitemporal)
    /// secondary index. Empty on the bitemporal path (which uses
    /// `bitemporal_index_tuples`). A transactional caller pushes the reverse
    /// (remove) on rollback. Autocommit callers ignore it.
    pub secondary_index_added: Vec<(String, String)>,
    /// `(field, value)` pairs this op REMOVED from the plain (non-bitemporal)
    /// secondary index because an UPDATE changed the field value. Empty on the
    /// bitemporal path. A transactional caller re-inserts these on rollback.
    /// Autocommit callers ignore it.
    pub secondary_index_removed: Vec<(String, String)>,
    /// Vector index mutations this put performed, so a transactional caller
    /// can push `UndoEntry::InsertVector` reversals (which also undo the
    /// paired `vector_doc_map` entry). Empty when the document had no vector
    /// fields. Autocommit callers ignore it.
    pub vector_inserts: Vec<super::vector::VectorIndexDelta>,
    /// `(spatial_index_key, entry_id)` pairs this put inserted into per-field
    /// spatial R-trees, so a transactional caller can push
    /// `UndoEntry::SpatialInsert` reversals. Empty when the document had no
    /// spatial fields. Autocommit callers ignore it.
    pub spatial_inserts: Vec<(SpatialIndexKey, u64)>,
    /// Pre-images of the column-stats read-modify-write this put performed, so a
    /// transactional caller can push `UndoEntry::StatsRestore` reversals. Each
    /// element is `(stats_key, prior_bytes)`: `prior_bytes = Some(b)` restores
    /// the exact `ColumnStats` that existed before, `None` removes a key the op
    /// created. Autocommit callers ignore it.
    pub stats_prior: Vec<crate::engine::sparse::stats::StatsPreImage>,
}

/// Map an enforcement check's `ErrorCode` onto the crate's typed `Error`.
///
/// The enforcement modules under `enforcement/` are shared with the
/// transactional path (`tx_point_put`), which surfaces `ErrorCode` directly.
/// `apply_point_put` runs inside `crate::Result`, so violations are
/// translated here to the equivalent `crate::Error` variant.
pub(in crate::data::executor) fn map_enforcement_error(e: ErrorCode) -> crate::Error {
    match e {
        ErrorCode::AppendOnlyViolation { collection } => crate::Error::AppendOnlyViolation {
            collection,
            detail: "append-only collection: UPDATE rejected".to_string(),
        },
        ErrorCode::PeriodLocked { collection } => crate::Error::PeriodLocked {
            collection,
            detail: "period is closed or locked".to_string(),
        },
        ErrorCode::PeriodLockMisconfigured {
            collection,
            ref_table,
            status_column,
            row_identity,
        } => crate::Error::PeriodLockMisconfigured {
            collection,
            ref_table,
            status_column,
            row_identity,
        },
        ErrorCode::StateTransitionViolation { collection, detail } => {
            crate::Error::StateTransitionViolation { collection, detail }
        }
        ErrorCode::TransitionCheckViolation { collection, detail } => {
            crate::Error::TransitionCheckViolation { collection, detail }
        }
        ErrorCode::RetentionViolation { collection } => crate::Error::RetentionViolation {
            collection,
            detail: "row is younger than the configured retention period".to_string(),
        },
        ErrorCode::LegalHoldActive { collection } => crate::Error::LegalHoldActive {
            collection,
            detail: "collection has an active legal hold: DELETE rejected".to_string(),
        },
        other => crate::Error::Storage {
            engine: "enforcement".into(),
            detail: format!("unexpected enforcement error: {other:?}"),
        },
    }
}
