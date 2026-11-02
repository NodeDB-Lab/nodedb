// SPDX-License-Identifier: BUSL-1.1

//! Shared "apply a PointDelete inside an externally-owned transaction" helper.
//!
//! Reused by both the autocommit PointDelete path and the transactional
//! `tx_point_delete` path. Every side-effect it performs (including the EXTRA
//! spatial-removal + node-tombstone cascade) is captured in the returned
//! [`PointDeleteOutcome`] so a transactional caller can build a fully
//! reversible undo log.

use redb::WriteTransaction;
use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::{append_only, retention};
use nodedb_physical::physical_plan::ResolvedSumTarget;
use nodedb_types::Surrogate;

use crate::data::executor::handlers::point::apply_put::SpatialEntryId;
use crate::data::executor::handlers::point::apply_put::VectorIndexDelta;
use crate::data::executor::handlers::point::apply_put::map_enforcement_error;
use crate::data::executor::handlers::transaction::undo::UndoEntry;

/// Parameters for [`CoreLoop::apply_point_delete`].
pub(in crate::data::executor) struct PointDeleteParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    /// The row's client identity. The delete marks deleted the node this
    /// names.
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    /// Roles held by the authenticated user. Currently unused by DELETE
    /// enforcement (no role-gated delete checks exist yet), but threaded
    /// through for symmetry with `PointPutParams` and future-proofing.
    pub user_roles: &'a [String],
    /// Whether to run stateless DELETE enforcement (append-only, period
    /// lock, retention/legal-hold).
    ///
    /// `true` for user-DML callers (autocommit PointDelete, and the
    /// transactional path in a later unit). `false` for system-sourced
    /// deletes (e.g. CRDT-sync materialization) whose admission already
    /// happened on their origin replica.
    pub enforce: bool,
    /// `(target collection, join-key value)` → target row surrogate, resolved
    /// on the Control Plane at plan time — read by period-lock enforcement to
    /// find its reference row. Empty for `enforce: false` callers, which
    /// never read it.
    pub resolved_targets: &'a [ResolvedSumTarget],
}

/// Capture of the mutations an [`CoreLoop::apply_point_delete`] performed, so
/// a transactional caller can build an undo entry that fully reverses it.
pub(in crate::data::executor) struct PointDeleteOutcome {
    /// Prior stored bytes when a row was actually removed, else `None`.
    pub prior_value: Option<Vec<u8>>,
    /// System-time ordinal the tombstone version (and its index tombstones)
    /// were appended at. `Some` exactly when a live row was closed.
    pub version_sys_from: Option<i64>,
    /// `(field, value)` pairs whose index tombstones this op wrote at
    /// `version_sys_from`. A transactional caller removes them on rollback.
    pub index_tuples: Vec<(String, String)>,
    /// `Some` when the delete installed its tombstone below the row's newest
    /// version: the index entries it wrote at its successor's ordinal. Such
    /// a delete ran no cascade.
    pub history: Option<super::history_install::SuccessorIndexWrites>,
    /// Undo entries for the in-memory cascades, in the order they ran:
    /// - `SpatialDelete` for each R-tree entry and `spatial_doc_map` record
    ///   removed
    /// - `MarkNodeDeleted` when this delete newly marked the row's node
    ///   deleted. A node a prior write marked is never un-marked.
    /// - `DeleteVector` for each vector node soft-deleted and each
    ///   `vector_doc_map` entry removed
    /// - `SparseDoc` for each sparse-vector document removed
    ///
    /// Dropping the caller's transaction does not reverse these. A caller
    /// that abandons the delete reverses them with `undo_memory_effects`.
    pub memory_undo: Vec<UndoEntry>,
}

impl CoreLoop {
    /// Apply a PointDelete within an externally-owned WriteTransaction.
    ///
    /// Appends the tombstone version and the index tombstones, and runs all
    /// cascades (inverted index, spatial R-tree, node-deleted bookkeeping,
    /// doc cache invalidation). The node's graph edges are
    /// tombstoned by the transaction's own `EdgeDelete` tasks. Does NOT
    /// commit the transaction.
    ///
    /// Every redb write this performs on the sparse database — the tombstone
    /// version, the index tombstones, and the inverted index removal — goes
    /// into `txn`.
    /// That is what makes the row and the indexes that describe it one
    /// all-or-nothing durable unit, and it is also required: those cascades
    /// share the sparse engine's redb database, which permits exactly one
    /// writer, so they cannot open transactions of their own while the caller
    /// holds this one. The graph edge store is a separate redb database and
    /// the spatial / vector / sparse-vector removals are in-memory, so those
    /// cascades are unaffected by the caller's transaction.
    ///
    /// On `Err` the caller MUST drop `txn` without committing. An `Err`
    /// leaves no in-memory change behind.
    ///
    /// Does NOT emit WriteEvents, mark checkpoints dirty, or build
    /// RETURNING payloads — those stay with the caller.
    ///
    /// Returns a [`PointDeleteOutcome`] capturing the prior stored bytes
    /// (present when a row was actually removed) plus the tombstone's system
    /// time and the index tombstone tuples written, so a transactional
    /// caller can build a fully-reversible undo entry. A caller that drops
    /// `txn` uncommitted after `Ok` reverses `memory_undo` with
    /// `abandon_write`.
    pub(in crate::data::executor) fn apply_point_delete(
        &mut self,
        txn: &WriteTransaction,
        params: PointDeleteParams<'_>,
    ) -> crate::Result<PointDeleteOutcome> {
        let PointDeleteParams {
            database_id,
            tid,
            collection,
            document_id,
            surrogate,
            user_roles,
            enforce,
            resolved_targets,
        } = params;
        let _ = user_roles;

        let storage_key = crate::engine::document::store::StorageKey::for_surrogate(surrogate);
        // A stamp in `apply_scope.bitemporal_stamps` (a committed redo delete)
        // fixes the tombstone at the EXACT resolve-time system time, so every
        // replica and every restart tombstones the same version key. Absent a
        // stamp the delete takes its ordinal here.
        let carried_sys_from = self
            .apply_scope
            .bitemporal_stamps
            .get(&surrogate.as_u32())
            .map(|stamp| stamp.sys_from);
        let bitemporal =
            carried_sys_from.is_some() || self.is_bitemporal(database_id, tid, collection);
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );

        // A carried stamp below the row's newest version installs a history
        // tombstone only.
        if let Some(sys_from) = carried_sys_from
            && let Some(slot) =
                self.history_slot_in_txn(txn, database_id, tid, collection, &storage_key, sys_from)?
        {
            if enforce
                && slot.predecessor.is_some()
                && let Some(config) = self.doc_configs.get(&config_key)
            {
                run_delete_enforcement(
                    self,
                    database_id,
                    tid,
                    collection,
                    config,
                    slot.predecessor.as_deref(),
                    resolved_targets,
                )?;
            }
            return self.apply_history_delete(
                txn,
                super::history_install::HistoryDelete {
                    database_id,
                    tid,
                    collection,
                    storage_key: &storage_key,
                    sys_from,
                    slot,
                },
            );
        }

        // Append a tombstone version and an index tombstone for every value
        // the closed version held. `prior` is the pre-delete body so the
        // Event Plane sees `old_value` correctly. Current-state-only indexes
        // (text, graph, spatial, vector) are cascaded below.
        let prior = self
            .sparse
            .get_in_txn(txn, database_id, tid, collection, &storage_key)?;
        // A bitemporal delete judges only a row that exists. Any other
        // delete judges whatever is stored.
        if enforce
            && (prior.is_some() || !bitemporal)
            && let Some(config) = self.doc_configs.get(&config_key)
        {
            run_delete_enforcement(
                self,
                database_id,
                tid,
                collection,
                config,
                prior.as_deref(),
                resolved_targets,
            )?;
        }
        let mut version_sys_from: Option<i64> = None;
        let mut index_tuples: Vec<(String, String)> = Vec::new();
        if let Some(ref body) = prior {
            let sys_from = match carried_sys_from {
                Some(sys_from) => sys_from,
                None => self.row_version_ordinal(database_id, tid, collection, &storage_key)?,
            };
            version_sys_from = Some(sys_from);
            // A `body` that does not decode fails the delete: its index
            // entries left live keep the row findable by its old values.
            index_tuples = self.tombstone_version_in_txn(
                txn,
                super::delete_version::VersionTombstone {
                    database_id,
                    tid,
                    collection,
                    storage_key: &storage_key,
                    current_body: body,
                    sys_from,
                },
            )?;
        }
        // The system time the row's index entries end at: the tombstone's,
        // or a fresh one when no row was stored.
        let index_ordinal = match version_sys_from {
            Some(sys_from) => sys_from,
            None => self.bitemporal_ordinal(),
        };

        // Cascade 1: Remove from full-text inverted index. The inverted
        // index was populated by `apply_point_put` with the substrate row
        // key (hex surrogate), not the user-visible PK — keep the cascade
        // keyed the same way so a delete actually wipes the term postings.
        //
        // Propagated, not logged: the removal strips postings, clears the term
        // set and decrements the corpus counters, all in the caller's txn. A
        // failure part-way through leaves that work half-done, so continuing
        // would commit an inverted index that disagrees with itself and with
        // the removed row. Returning drops the caller's txn un-committed, which
        // reverses the partial strip along with the row removal.
        if let Err(e) = self.inverted.remove_document_in_txn(
            txn,
            crate::engine::sparse::inverted::IndexDocScope {
                database_id,
                tid: crate::types::TenantId::new(tid),
                collection,
                surrogate,
                ordinal: index_ordinal,
            },
        ) {
            warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index removal failed; rejecting the delete");
            return Err(e);
        }

        // The row's graph node keeps its edges here. The Control Plane
        // tombstones the node's edges in its collection with `EdgeDelete`
        // tasks of the delete's own transaction, at the ordinal that
        // transaction decides on both homes of each edge.

        // Cascade 3: Remove from spatial R-tree indexes + reverse map, and
        // record the node deletion for edge referential integrity. Both are
        // captured in `memory_undo` and reversed on rollback or abort, so
        // they run unconditionally for both the autocommit and transactional
        // delete paths.
        //
        // `apply_point_put` hashes the substrate row key as the R-tree entry
        // id, so delete must hash the same key to find the entry. Hashing the
        // user PK would leak ghost bbox entries that survive the row's removal.
        //
        // No step from here on fails, so an `Err` from this function leaves
        // no in-memory change behind.
        let mut memory_undo: Vec<UndoEntry> = Vec::new();
        self.remove_document_spatial_indexes_with_undo(
            database_id,
            tid,
            collection,
            SpatialEntryId::from_storage_key(storage_key),
            index_ordinal,
            &mut memory_undo,
        );

        // Record deletion for edge referential integrity. Capture the id
        // for undo ONLY when this call newly marked it — un-marking a node
        // a prior committed op already tombstoned would wrongly resurrect
        // it as a valid edge target.
        if self.mark_node_deleted(database_id, tid, collection, document_id) {
            memory_undo.push(UndoEntry::MarkNodeDeleted {
                database_id,
                tid,
                collection: collection.to_string(),
                node_id: document_id.to_string(),
            });
        }

        // Cascade 4 (CORE, UNCONDITIONAL): end, at the tombstone's ordinal,
        // any HNSW vector entries this document produced. Runs for BOTH autocommit and transactional
        // callers — leaving them behind orphans the vector index forever (a
        // deleted doc keeps scoring in KNN). The reverse map `vector_doc_map`
        // was populated by `apply_point_put_vector_indexes` under the same hex
        // surrogate row key used here. Soft-delete (not hard) so a rolled-back
        // transactional delete can `undelete` the exact vector id.
        //
        // The candidate fields are known from the same schema/vector_params
        // enumeration the put path uses, so each `vector_doc_map` entry is
        // looked up by its exact key rather than scanning the whole map on
        // every delete. Shared with the PointUpdate re-index path.
        memory_undo.extend(
            self.remove_document_vector_indexes(
                database_id,
                tid,
                collection,
                storage_key,
                index_ordinal,
            )
            .into_iter()
            .map(VectorIndexDelta::into_delete_undo),
        );

        // Sparse inverted-index cleanup, mirroring the dense-vector cascade
        // above: drop this document's sparse posting entries under the same hex
        // surrogate row key the put path indexed them by. A no-op unless the
        // strict schema declares a `SparseVector` column.
        self.remove_document_sparse_indexes(
            database_id,
            tid,
            collection,
            storage_key,
            index_ordinal,
            &mut memory_undo,
        );

        // Invalidate document cache.
        self.doc_cache
            .invalidate(database_id, tid, collection, &storage_key);

        // Invalidate aggregate cache — a delete changes count(*) for this
        // collection. Only needed when a row was actually removed.
        if prior.is_some() {
            self.invalidate_aggregate_cache_for_collection(database_id, tid, collection);
        }

        Ok(PointDeleteOutcome {
            prior_value: prior,
            version_sys_from,
            index_tuples,
            history: None,
            memory_undo,
        })
    }
}

/// Stateless DELETE enforcement, unified across the autocommit
/// (`apply_point_delete`) and transactional (`tx_point_delete`) paths.
/// These checks have no persistent side effect, so a violation here
/// simply aborts before the write.
pub(in crate::data::executor) fn run_delete_enforcement(
    core: &CoreLoop,
    database_id: u64,
    tid: u64,
    collection: &str,
    config: &crate::engine::document::store::CollectionConfig,
    old_value: Option<&[u8]>,
    resolved_targets: &[ResolvedSumTarget],
) -> crate::Result<()> {
    append_only::check_point_delete(collection, &config.enforcement)
        .map_err(map_enforcement_error)?;
    if let Some(ref pl) = config.enforcement.period_lock
        && let Some(old_bytes) = old_value
    {
        core.check_period_lock_at(
            database_id,
            tid,
            collection,
            old_bytes,
            pl,
            resolved_targets,
        )
        .map_err(map_enforcement_error)?;
    }
    let created_at = old_value.and_then(retention::extract_created_at_secs);
    retention::check_delete_allowed(collection, &config.enforcement, created_at)
        .map_err(map_enforcement_error)?;
    Ok(())
}
