// SPDX-License-Identifier: BUSL-1.1

//! Undo entries that reverse one document write, built from the outcome the
//! shared write path (`apply_point_put` / `apply_point_delete`) returned.
//!
//! The transaction batch and the committed-redo apply build them here, so a
//! rolled-back write reverses the same side effects on both paths: the row,
//! its secondary and versioned index entries, its HNSW vectors, its R-tree
//! entries, its column stats, its hash-chain head, and every
//! materialized-sum target row the write folded into.
//!
//! Entries are pushed in the order the writes happened. Rollback runs the log
//! in reverse.

use nodedb_types::StorageKey;

use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::data::executor::handlers::point::apply_delete::PointDeleteOutcome;
use crate::data::executor::handlers::point::apply_put::PointPutOutcome;

use crate::data::executor::handlers::point::history_install::SuccessorIndexWrites;

use super::UndoEntry;
use super::history_version::HistoryVersionUndo;

/// The document row one write touched.
pub(in crate::data::executor::handlers) struct DocumentRow<'a> {
    pub collection: &'a str,
    pub storage_key: StorageKey,
}

/// Push the undo entries that reverse every materialized-sum target row the
/// write folded into. A target write is a full document write, so it has
/// index, vector, spatial and stats side effects of its own. The targets stay
/// with the caller, which also reports them in its response. Their in-memory
/// undo entries move onto `undo_log`.
pub(in crate::data::executor::handlers) fn push_target_undo(
    undo_log: &mut Vec<UndoEntry>,
    targets: &mut [TargetWrite],
) {
    for target in targets {
        let memory_undo = std::mem::take(&mut target.outcome.memory_undo);
        let outcome = &target.outcome;
        undo_log.push(put_entry(
            target.collection.clone(),
            StorageKey::for_surrogate(target.surrogate),
            outcome.version_sys_from,
            outcome.index_tuples.clone(),
            outcome.history.clone(),
            None,
        ));
        push_put_side_effects(undo_log, memory_undo, outcome.stats_prior.clone());
    }
}

/// Push the undo entries that reverse one document put. `chain_hash_prior`
/// is the hash-chain head before the put, `None` when the put did not touch
/// the chain.
pub(in crate::data::executor::handlers) fn push_put_undo(
    undo_log: &mut Vec<UndoEntry>,
    row: DocumentRow<'_>,
    outcome: PointPutOutcome,
    chain_hash_prior: Option<Option<crate::types::hash_chain::ChainHead>>,
) {
    undo_log.push(put_entry(
        row.collection.to_string(),
        row.storage_key,
        outcome.version_sys_from,
        outcome.index_tuples,
        outcome.history,
        chain_hash_prior,
    ));
    push_put_side_effects(undo_log, outcome.memory_undo, outcome.stats_prior);
}

/// Push the undo entries that reverse one document delete. A delete that
/// removed no row reverses only the side effects it still had.
pub(in crate::data::executor::handlers) fn push_delete_undo(
    undo_log: &mut Vec<UndoEntry>,
    row: DocumentRow<'_>,
    outcome: PointDeleteOutcome,
) {
    if let (Some(old_value), Some(version_sys_from)) =
        (outcome.prior_value, outcome.version_sys_from)
    {
        undo_log.push(match outcome.history {
            // A history tombstone ran no cascade, so its undo restores no
            // text posting.
            Some(successor) => UndoEntry::HistoryVersion(Box::new(HistoryVersionUndo {
                collection: row.collection.to_string(),
                document_id: row.storage_key,
                version_sys_from,
                index_tuples: outcome.index_tuples,
                successor,
                chain_hash_prior: None,
            })),
            None => UndoEntry::DeleteDocument {
                collection: row.collection.to_string(),
                document_id: row.storage_key,
                old_value,
                version_sys_from,
                index_tuples: outcome.index_tuples,
                chain_hash_prior: None,
            },
        });
    }
    undo_log.extend(outcome.memory_undo);
}

/// The entry that removes one put's version. A put installed below the
/// row's newest version also removes the entries it wrote at its successor,
/// and reverses no current-state side effect.
fn put_entry(
    collection: String,
    document_id: StorageKey,
    version_sys_from: i64,
    index_tuples: Vec<(String, String)>,
    history: Option<SuccessorIndexWrites>,
    chain_hash_prior: Option<Option<crate::types::hash_chain::ChainHead>>,
) -> UndoEntry {
    match history {
        Some(successor) => UndoEntry::HistoryVersion(Box::new(HistoryVersionUndo {
            collection,
            document_id,
            version_sys_from,
            index_tuples,
            successor,
            chain_hash_prior,
        })),
        None => UndoEntry::PutDocument {
            collection,
            document_id,
            version_sys_from,
            index_tuples,
            chain_hash_prior,
        },
    }
}

fn push_put_side_effects(
    undo_log: &mut Vec<UndoEntry>,
    memory_undo: Vec<UndoEntry>,
    stats_prior: Vec<crate::engine::sparse::stats::StatsPreImage>,
) {
    undo_log.extend(memory_undo);
    for (key, prior) in stats_prior {
        undo_log.push(UndoEntry::StatsRestore { key, prior });
    }
}
