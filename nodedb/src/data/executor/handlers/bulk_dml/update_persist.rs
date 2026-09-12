// SPDX-License-Identifier: BUSL-1.1

//! Landing one bulk-UPDATE row: the write transaction the row's body, its
//! secondary-index diff, and its materialized-sum deltas share.
//!
//! Its own file because the transaction boundary is the concern — the bulk
//! handler decides WHICH rows change and what they become, and this decides
//! when that becomes durable. Every sparse-database write a row produces is
//! staged into the transaction opened here and lands on its commit, so a row
//! that fails at any step drops the transaction un-committed and is skipped
//! whole rather than left with a body the index no longer describes.
//!
//! The materialized-sum fold runs inside that same transaction, one level above
//! the row's own write, so a credited target row can never survive a row whose
//! commit did not happen.

use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::images::RowImages;
use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::data::executor::enforcement::write_hook::{self, HookCtx};
use crate::data::executor::enforcement::{funnel, images};
use crate::data::executor::handlers::point::update_reindex::NonbitemporalUpdateReindex;

/// What landing one bulk-UPDATE row produced for the caller to account for.
pub(super) struct PersistedBulkUpdateRow {
    /// The `(field, value)` tuples the index diff touched, for the caller to
    /// publish into the per-index write-value substrate — that recording
    /// describes a durable write, so it belongs after the commit, never before.
    pub(super) touched: Vec<(String, String)>,
    /// Target rows this row's materialized-sum bindings updated, for the caller
    /// to turn into durable redo entries.
    pub(super) target_writes: Vec<TargetWrite>,
}

impl CoreLoop {
    /// Write one bulk-UPDATE row's post-image, reconcile its plain `INDEXES`
    /// entries, and fold its materialized-sum deltas — committing all three
    /// together.
    ///
    /// `Ok(None)` is the row's own write failing: it is skipped, exactly as it
    /// always was, and the transaction is dropped un-committed so it leaves
    /// nothing behind. `Err` is an enforcement REJECTION, which is not
    /// skippable — a rejected constraint must fail the statement, not quietly
    /// shrink its affected count.
    pub(super) fn persist_bulk_update_row(
        &mut self,
        p: NonbitemporalUpdateReindex<'_>,
        hook: &HookCtx<'_>,
    ) -> crate::Result<Option<PersistedBulkUpdateRow>> {
        // Hoisted before the transaction opens: the images below borrow them,
        // and a collection that declares no image-folding enforcement must not
        // pay for the fold at all.
        let doc_id = *p.storage_key;
        // Copies of the borrows, not of the documents: they outlive `p`'s move
        // into the reindex below because they borrow the caller's images, not
        // the params struct.
        let old_doc: &serde_json::Value = p.old_doc;
        let new_doc: &serde_json::Value = p.new_doc;
        let folds = write_hook::folds_images(self, hook);

        let txn = match self.sparse.begin_write() {
            Ok(txn) => txn,
            Err(e) => {
                warn!(%doc_id, error = %e, "bulk update: write txn failed, skipping document");
                return Ok(None);
            }
        };
        let touched = match self.nonbitemporal_update_reindex(&txn, p) {
            Ok(touched) => touched,
            Err(e) => {
                warn!(%doc_id, error = %e, "update reindex failed, skipping document");
                return Ok(None);
            }
        };

        // Both images were materialized by the caller for the index diff, so the
        // fold re-reads and re-decodes nothing. `RowImages::Update` is the only
        // shape a bulk UPDATE can produce, and it demands BOTH images — which is
        // what stops the row's whole new value being credited on top of the
        // contribution it already holds.
        let target_writes = if folds {
            let ctx = images::EnforcementCtx {
                database_id: hook.database_id,
                tid: hook.tid,
                collection: hook.collection,
                resolved_targets: hook.resolved_targets,
                deferred_sum_targets: hook.deferred_sum_targets,
                wal_lsn: hook.wal_lsn,
            };
            // Only the target writes are taken here. The row's BALANCED
            // contribution was settled for the whole statement before the first
            // row was written — this path commits one transaction per row, so a
            // violation found here could no longer be undone, and taking the
            // entries again would count the same update twice.
            funnel::run_write_enforcement(self, &txn, ctx, RowImages::Update { old_doc, new_doc })?
                .target_writes
        } else {
            Vec::new()
        };

        match txn.commit() {
            Ok(()) => Ok(Some(PersistedBulkUpdateRow {
                touched,
                target_writes,
            })),
            Err(e) => {
                warn!(
                    %doc_id,
                    error = %e,
                    "bulk update commit failed, skipping document"
                );
                Ok(None)
            }
        }
    }
}
