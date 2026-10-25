// SPDX-License-Identifier: BUSL-1.1

//! Removing one row of a TRUNCATE: its current version, its secondary-index
//! entries and the materialized-sum deltas it owes, in one transaction, then
//! its journal entry, its delete event and its in-memory cascades.

use nodedb_physical::physical_plan::ResolvedSumTarget;
use nodedb_types::columnar::StrictSchema;
use nodedb_types::{StorageKey, Surrogate};

use crate::bridge::envelope::{ErrorCode, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::removed_row_image;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::partial_refusal::refusal_after_rows;
use crate::data::executor::handlers::point::delete_version::CurrentRowRemoval;
use crate::data::executor::handlers::transaction::stage_write::stored_row_identity;
use crate::data::executor::task::ExecutionTask;

/// What every row of one TRUNCATE is removed under.
pub(in crate::data::executor) struct TruncateScope<'a> {
    pub task: &'a ExecutionTask,
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    /// The collection's declared `PRIMARY KEY` column, when it has one.
    pub declared_primary_key: Option<&'a str>,
    /// `Some` for a strict collection, whose pre-images are Binary Tuples.
    pub strict_schema: Option<&'a StrictSchema>,
    pub has_vectors: bool,
    /// The tombstones' system time on a bitemporal collection, `None` on any
    /// other.
    pub bitemporal_sys_from: Option<i64>,
}

/// What a TRUNCATE has removed so far.
#[derive(Default)]
pub(in crate::data::executor) struct TruncateProgress {
    /// Rows removed.
    pub truncated: u64,
    /// One `Delete` redo entry per removed row, in removal order, each
    /// followed by the target rows its fold rewrote. The plan mints no
    /// pre-dispatch record, so these entries are the removals' only record.
    pub write_set: Vec<WriteSetEntry>,
    /// Surrogates of the removed rows. A refusal part-way removes their text
    /// from the inverted index.
    pub removed: Vec<Surrogate>,
}

impl CoreLoop {
    /// Remove one row and account it into `progress`.
    ///
    /// The row's removal, its secondary-index entries and the
    /// materialized-sum deltas it owes share one transaction, as in
    /// `execute_bulk_delete`. `Err` is the refusal code. The rows removed
    /// before it stay removed and journalled.
    pub(in crate::data::executor) fn truncate_row(
        &mut self,
        s: &TruncateScope<'_>,
        storage_key: &StorageKey,
        progress: &mut TruncateProgress,
    ) -> Result<(), ErrorCode> {
        let refuse = |truncated: u64, e: crate::Error| refusal_after_rows(truncated, e);
        let (database_id, tid, collection) = (s.database_id, s.tid, s.collection);
        let row_txn = self
            .sparse
            .begin_write()
            .map_err(|e| refuse(progress.truncated, e))?;
        // A delete error refuses the TRUNCATE: read as an absent row, it
        // leaves the row stored under a success.
        let removal = self
            .remove_current_row_in_txn(
                &row_txn,
                CurrentRowRemoval {
                    database_id,
                    tid,
                    collection,
                    storage_key,
                    bitemporal_sys_from: s.bitemporal_sys_from,
                },
            )
            .map_err(|e| refuse(progress.truncated, e))?;
        let Some(deleted_bytes) = removal.prior else {
            return Ok(());
        };
        self.sparse
            .delete_indexes_for_document_in_txn(&row_txn, database_id, tid, collection, storage_key)
            .map_err(|e| refuse(progress.truncated, e))?;
        // The row's BALANCED contribution was settled for the whole statement
        // before any row was removed; taking it again here counts it twice.
        let outcome = write_hook::run(
            self,
            &row_txn,
            &write_hook::HookCtx {
                database_id,
                tid,
                collection,
                resolved_targets: s.resolved_sum_targets,
                deferred_sum_targets: &[],
                wal_lsn: s.task.wal_lsn(),
            },
            write_hook::WriteImages::Delete {
                old: write_hook::ImageBody::Stored(&deleted_bytes),
            },
        )
        .map_err(|e| refusal_after_rows(progress.truncated, e))?;
        row_txn.commit().map_err(|e| {
            refuse(
                progress.truncated,
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("truncate commit: {e}"),
                },
            )
        })?;

        let surrogate = storage_key.surrogate();
        // The identity INSERT minted for this row: the declared primary key
        // when the collection declares one, else the decimal surrogate. The
        // redo entry and the delete event share it.
        let row_identity = stored_row_identity(
            &deleted_bytes,
            s.strict_schema,
            s.declared_primary_key,
            *storage_key,
        );
        // The removed rows' index values record no version, so the first
        // removal records a wipe that bounds every index read.
        if progress.truncated == 0 {
            self.note_task_collection_wipe(s.task, tid, collection);
        }
        self.note_surrogate_write(s.task, tid, collection, surrogate.as_u32());
        // The row's text leaves the inverted index with every other row's,
        // in one purge once the statement's rows are removed.
        progress.removed.push(surrogate);
        // The put path indexed this row's vectors under its surrogate. A
        // leaked vector keeps scoring in KNN search in the same process.
        if s.has_vectors {
            self.remove_document_vector_indexes(database_id, tid, collection, *storage_key);
        }
        // A bitemporal tombstone carries its version key, so replay closes
        // the same version.
        progress.write_set.push(removed_row_image(
            surrogate.as_u32(),
            row_identity.clone(),
            removal.tombstone_sys_from,
        ));
        // The collection's edges keep their place here: the TRUNCATE's
        // transaction cuts them with one `TruncateEdges` per vShard.
        self.doc_cache
            .invalidate(database_id, tid, collection, storage_key);
        // One delete event per removed row, as point and bulk delete emit:
        // ROW-level AFTER-DELETE triggers match on per-row events.
        self.emit_document_delete_event(
            s.task,
            tid,
            collection,
            row_identity,
            Some(deleted_bytes.as_slice()),
        );
        progress.truncated += 1;
        progress
            .write_set
            .extend(write_hook::target_write_set(&outcome.target_writes));
        Ok(())
    }
}
