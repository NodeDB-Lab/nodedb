// SPDX-License-Identifier: BUSL-1.1

//! Undo entries that reverse one document write, built from the outcome the
//! shared write path (`apply_point_put` / `apply_point_delete`) returned.
//!
//! The transaction batch and the committed-redo apply build them here, so a
//! rolled-back write reverses the same side effects on both paths: the row,
//! its secondary and versioned index entries, its HNSW vectors, its R-tree
//! entries, its column stats, its hash-chain head, the edges a delete
//! cascaded, and every materialized-sum target row the write folded into.
//!
//! Entries are pushed in the order the writes happened. Rollback runs the log
//! in reverse.

use nodedb_types::{RowIdentity, StorageKey};

use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::data::executor::handlers::point::apply_delete::PointDeleteOutcome;
use crate::data::executor::handlers::point::apply_put::PointPutOutcome;

use super::UndoEntry;

/// The document row one write touched.
pub(in crate::data::executor::handlers) struct DocumentRow<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub storage_key: StorageKey,
    /// The row's client identity, as its event names it.
    pub identity: RowIdentity,
}

/// Push the undo entries that reverse every materialized-sum target row the
/// write folded into. A target write is a full document write, so it has
/// index, vector, spatial and stats side effects of its own. The targets stay
/// with the caller, which also reports them in its response.
pub(in crate::data::executor::handlers) fn push_target_undo(
    undo_log: &mut Vec<UndoEntry>,
    targets: &[TargetWrite],
) {
    for target in targets {
        let outcome = &target.outcome;
        undo_log.push(UndoEntry::PutDocument {
            collection: target.collection.clone(),
            document_id: StorageKey::for_surrogate(target.surrogate),
            identity: target.identity.clone(),
            old_value: outcome.prior_value.clone(),
            bitemporal_sys_from_ms: outcome.bitemporal_sys_from_ms,
            bitemporal_index_tuples: outcome.bitemporal_index_tuples.clone(),
            secondary_index_added: outcome.secondary_index_added.clone(),
            secondary_index_removed: outcome.secondary_index_removed.clone(),
            chain_hash_prior: None,
        });
        push_put_side_effects(
            undo_log,
            outcome.vector_inserts.clone(),
            outcome.spatial_inserts.clone(),
            outcome.stats_prior.clone(),
        );
    }
}

/// Push the undo entries that reverse one document put. `chain_hash_prior`
/// is the hash-chain head before the put, `None` when the put did not touch
/// the chain.
pub(in crate::data::executor::handlers) fn push_put_undo(
    undo_log: &mut Vec<UndoEntry>,
    row: DocumentRow<'_>,
    outcome: PointPutOutcome,
    chain_hash_prior: Option<Option<String>>,
) {
    undo_log.push(UndoEntry::PutDocument {
        collection: row.collection.to_string(),
        document_id: row.storage_key,
        identity: row.identity,
        old_value: outcome.prior_value,
        bitemporal_sys_from_ms: outcome.bitemporal_sys_from_ms,
        bitemporal_index_tuples: outcome.bitemporal_index_tuples,
        secondary_index_added: outcome.secondary_index_added,
        secondary_index_removed: outcome.secondary_index_removed,
        chain_hash_prior,
    });
    push_put_side_effects(
        undo_log,
        outcome.vector_inserts,
        outcome.spatial_inserts,
        outcome.stats_prior,
    );
}

/// Push the undo entries that reverse one document delete. A delete that
/// removed no row reverses only the side effects it still had.
pub(in crate::data::executor::handlers) fn push_delete_undo(
    undo_log: &mut Vec<UndoEntry>,
    row: DocumentRow<'_>,
    outcome: PointDeleteOutcome,
) {
    if let Some(old_value) = outcome.prior_value {
        undo_log.push(UndoEntry::DeleteDocument {
            collection: row.collection.to_string(),
            document_id: row.storage_key,
            identity: row.identity,
            old_value,
            bitemporal_sys_from_ms: outcome.bitemporal_sys_from_ms,
            bitemporal_index_tuples: outcome.bitemporal_index_tuples,
            secondary_index_tuples: outcome.secondary_index_tuples,
            chain_hash_prior: None,
        });
    }
    for delta in outcome.vector_deletes {
        undo_log.push(UndoEntry::DeleteVector {
            index_key: delta.index_key,
            vector_id: delta.vector_id,
            collection: delta.collection,
            field: delta.field,
            doc_id: Some(delta.doc_id),
        });
    }
    for (key, entry_id, bbox, document_id) in outcome.spatial_deletes {
        undo_log.push(UndoEntry::SpatialDelete {
            key,
            entry_id,
            bbox,
            document_id,
        });
    }
    // `Some` only when this delete newly marked the node: a tombstone a prior
    // committed write left is never un-marked.
    if let Some(node_id) = outcome.mark_node_deleted {
        undo_log.push(UndoEntry::MarkNodeDeleted {
            database_id: row.database_id,
            tid: row.tid,
            node_id,
        });
    }
    for (collection, src_id, label, dst_id, old_properties) in outcome.edge_deletes {
        undo_log.push(UndoEntry::DeleteEdge {
            collection,
            src_id,
            label,
            dst_id,
            old_properties,
        });
    }
}

fn push_put_side_effects(
    undo_log: &mut Vec<UndoEntry>,
    vector_inserts: Vec<crate::data::executor::handlers::point::apply_put::VectorIndexDelta>,
    spatial_inserts: Vec<(crate::data::executor::spatial_key::SpatialIndexKey, u64)>,
    stats_prior: Vec<crate::engine::sparse::stats::StatsPreImage>,
) {
    for delta in vector_inserts {
        undo_log.push(UndoEntry::InsertVector {
            index_key: delta.index_key,
            vector_id: delta.vector_id,
            collection: delta.collection,
            field: delta.field,
            doc_id: Some(delta.doc_id),
        });
    }
    for (key, entry_id) in spatial_inserts {
        undo_log.push(UndoEntry::SpatialInsert { key, entry_id });
    }
    for (key, prior) in stats_prior {
        undo_log.push(UndoEntry::StatsRestore { key, prior });
    }
}
