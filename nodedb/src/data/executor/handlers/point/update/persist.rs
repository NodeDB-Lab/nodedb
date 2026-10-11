// SPDX-License-Identifier: BUSL-1.1

//! Landing the post-update image, with its secondary indexes and everything the
//! collection's constraints derive from it, in one write.
//!
//! Separate from image construction because the concern here is atomicity, not
//! value. The body and its index diff land through
//! `CoreLoop::update_body_in_txn`, the write shape every update of a stored
//! row shares. Every shape re-indexes the row's full-text postings from the
//! new body in the same transaction.
//!
//! All of it runs inside ONE transaction this function owns, and image-folding
//! enforcement runs inside that same transaction before it commits. That is why
//! the enforcement lives here rather than in the caller that sequences the
//! statement: a materialized-sum target write is a document write of its own,
//! and running it after this function returned would put it in a SECOND
//! transaction — a crash between the two leaves the row updated and the total
//! it feeds stale, which is exactly the divergence the constraint exists to
//! rule out.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::unique::PostImage;
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::types::{DatabaseId, Lsn, TenantId};
use nodedb_physical::physical_plan::ResolvedSumTarget;

/// Inputs to [`CoreLoop::persist_point_update`].
pub(in crate::data::executor) struct PointUpdatePersist<'a> {
    pub(in crate::data::executor) config_key: &'a (DatabaseId, TenantId, String),
    pub(in crate::data::executor) database_id: u64,
    pub(in crate::data::executor) tid: u64,
    pub(in crate::data::executor) collection: &'a str,
    /// The same storage key, typed — what every storage call below takes.
    pub(in crate::data::executor) storage_key: &'a crate::engine::document::store::StorageKey,
    /// The row as it was before this update — the old side of the index diff,
    /// and the pre-image every folded constraint subtracts.
    pub(in crate::data::executor) current_bytes: &'a [u8],
    /// The row as it will be stored.
    pub(in crate::data::executor) updated_bytes: &'a [u8],
    pub(in crate::data::executor) bitemporal: bool,
    pub(in crate::data::executor) sys_from_ms: i64,
    pub(in crate::data::executor) wal_lsn: Option<Lsn>,
    /// The stamp the update's index values record under, `None` without a
    /// WAL record.
    pub(in crate::data::executor) write_stamp:
        Option<crate::data::executor::core_loop::write_index::WriteStamp>,
    /// `(target collection, join-key value)` → target row surrogate for every
    /// materialized-sum target this update may touch — BOTH sides when the
    /// update moves a row between targets by changing its join key. Resolved on
    /// the Control Plane.
    pub(in crate::data::executor) resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Write the post-update body, reconcile the collection's secondary indexes
    /// with it, and apply everything its declared constraints derive from the
    /// change — all in one transaction.
    ///
    /// Returns the redo entries for any derived target rows, which the caller
    /// carries back on its response so each is journalled against its own
    /// collection.
    pub(in crate::data::executor) fn persist_point_update(
        &mut self,
        params: PointUpdatePersist<'_>,
    ) -> crate::Result<Vec<crate::bridge::envelope::WriteSetEntry>> {
        let PointUpdatePersist {
            config_key,
            database_id,
            tid,
            collection,
            storage_key,
            current_bytes,
            updated_bytes,
            bitemporal,
            sys_from_ms,
            wal_lsn,
            write_stamp,
            resolved_sum_targets,
        } = params;

        // A point update is a unit of one row: its claims meet the committed
        // index, where its own prior values do not count.
        if let Some(config) = self.unique_config(database_id, tid, collection) {
            let new_doc = self.decode_stored_document(config, updated_bytes)?;
            self.check_unit_unique(
                database_id,
                tid,
                collection,
                &[PostImage {
                    surrogate: storage_key.surrogate().as_u32(),
                    doc: Some(&new_doc),
                    judged: true,
                }],
            )?;
        }

        // One transaction for the body, its index diff, and every derived write
        // the collection's constraints imply. Dropped un-committed on any error
        // below, so a failure leaves neither a body without its index nor a
        // total without the row that moved it.
        let txn = self.sparse.begin_write()?;

        let touched = self.update_body_in_txn(
            &txn,
            super::super::update_reindex::UpdateBody {
                config_key,
                database_id,
                tid,
                collection,
                storage_key,
                current_bytes,
                updated_bytes,
                bitemporal_sys_from_ms: bitemporal.then_some(sys_from_ms),
            },
        )?;

        // The row's postings follow its new text in the same transaction.
        // A registered collection's stored image must decode; an
        // unregistered one indexes what `decode_document` reads, and a body
        // it cannot read has no fields to index, as on the insert path.
        let new_doc = match self.doc_configs.get(config_key) {
            Some(cfg) => Some(self.decode_stored_document(cfg, updated_bytes)?),
            None => crate::data::executor::doc_format::decode_document(updated_bytes).ok(),
        };
        if let Some(new_doc) = &new_doc {
            self.update_reindex_text(
                &txn,
                super::super::update_reindex_text::UpdateTextReindex {
                    database_id,
                    tid,
                    collection,
                    surrogate: storage_key.surrogate(),
                    new_doc,
                },
            )?;
        }

        // Image-folding enforcement, inside the transaction the body just landed
        // in. Both images are STORED bytes: `current_bytes` came off the store
        // and `updated_bytes` was re-encoded in the collection's own mode, so a
        // strict collection's Binary Tuples decode as tuples on both sides.
        //
        // A join-key change is an ordinary UPDATE here — the fold derives the
        // two-target split from `old_doc[join] != new_doc[join]` itself, moving
        // the amount off one target and onto the other.
        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets: &[],
            wal_lsn,
        };
        let enforcement = write_hook::run(
            self,
            &txn,
            &hook_ctx,
            WriteImages::Update {
                old: ImageBody::Stored(current_bytes),
                new: ImageBody::Stored(updated_bytes),
            },
        )?;
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);

        // An update contributes both legs — the old amount out, the new one in
        // — so a single-row update that moves an amount unbalances its group.
        // Settled before the commit; `?` drops `txn` un-committed.
        self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)?;

        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("point update commit: {e}"),
        })?;

        // Index write-versions are published only once the write they describe
        // is durable.
        if let Some(stamp) = write_stamp
            && !touched.is_empty()
        {
            self.note_index_write_values(
                DatabaseId::new(database_id),
                TenantId::new(tid),
                collection,
                &touched,
                stamp,
            );
        }

        Ok(target_write_set)
    }
}
