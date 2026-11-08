// SPDX-License-Identifier: BUSL-1.1

//! Landing one bulk-UPDATE row: the write transaction the row's body, its
//! secondary-index diff, its full-text postings and its materialized-sum
//! deltas share.
//!
//! Its own file because the transaction boundary is the concern — the bulk
//! handler decides WHICH rows change and what they become, and this decides
//! when that becomes durable. Every sparse-database write a row produces is
//! staged into the transaction opened here and lands on its commit, so a row
//! that fails at any step drops the transaction un-committed and fails the
//! statement, rather than being left with a body the index no longer
//! describes or dropped from the affected count.
//!
//! The materialized-sum fold runs inside that same transaction, one level above
//! the row's own write, so a credited target row can never survive a row whose
//! commit did not happen.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::images::RowImages;
use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::data::executor::enforcement::write_hook::{self, HookCtx};
use crate::data::executor::enforcement::{funnel, images};
use crate::data::executor::handlers::point::update_reindex::UpdateReindex;

/// What landing one bulk-UPDATE row produced for the caller to account for.
pub(super) struct PersistedBulkUpdateRow {
    /// Target rows this row's materialized-sum bindings updated, for the caller
    /// to turn into durable redo entries.
    pub(super) target_writes: Vec<TargetWrite>,
}

impl CoreLoop {
    /// Write one bulk-UPDATE row's post-image as a new version, reconcile
    /// its index entries, and fold its materialized-sum deltas — committing
    /// all three together.
    ///
    /// Any failure is an `Err` and fails the statement: the row's write, its
    /// index diff, its full-text postings, an enforcement rejection, or the
    /// commit. The transaction is dropped un-committed, so the row leaves
    /// nothing behind. A row is never skipped, because a skipped row would
    /// report a smaller affected count than the predicate matched.
    ///
    /// The row lands at `p.sys_from` through the same write the point update
    /// lands. `old_doc` is the row's pre-image: the old side of the index
    /// diff and of the materialized-sum fold.
    ///
    /// `user_roles` are the roles of the identity that writes. They reach the
    /// transition check that runs here, against the row this transaction
    /// reads, not the image the statement's projection saw.
    pub(super) fn persist_bulk_update_row(
        &mut self,
        p: UpdateReindex<'_>,
        old_doc: &serde_json::Value,
        hook: &HookCtx<'_>,
        user_roles: &[String],
    ) -> crate::Result<PersistedBulkUpdateRow> {
        // Hoisted before the transaction opens: the images below borrow them,
        // and a collection that declares no image-folding enforcement must not
        // pay for the fold at all.
        let doc_id = *p.doc_id;
        // Copies of the borrows, not of the documents: they outlive `p`'s move
        // into the reindex below because they borrow the caller's images, not
        // the params struct.
        let new_doc: &serde_json::Value = p.new_doc;
        let folds = write_hook::folds_images(self, hook);

        let txn = self.sparse.begin_write()?;
        // The statement's gate judged each row against the image the
        // projection read. The apply loop commits one transaction per row, so
        // a writer that lands in between moves the pre-image. The body this
        // transaction reads is the one the write replaces. A bitemporal write
        // appends a version, so it keeps the projected judgement.
        if !self.is_bitemporal(p.database_id, p.tid, p.collection) {
            self.judge_stored_transition(&txn, &p, user_roles)?;
        }
        let text = crate::data::executor::handlers::point::update_reindex_text::UpdateTextReindex {
            database_id: p.database_id,
            tid: p.tid,
            collection: p.collection,
            surrogate: doc_id.surrogate(),
            new_doc,
            ordinal: p.sys_from,
        };
        self.update_reindex(
            &txn,
            UpdateReindex {
                old_doc: Some(old_doc),
                ..p
            },
        )?;
        // The row's postings follow its new text in the same transaction.
        self.update_reindex_text(&txn, text)?;

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

        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("bulk update commit of row {doc_id}: {e}"),
        })?;
        Ok(PersistedBulkUpdateRow { target_writes })
    }

    /// Judge the transition this write lands: the row's current body, read
    /// inside `txn`, against the post-image the caller built.
    ///
    /// A row that is no longer stored is not judged, because the write then
    /// creates it rather than moving it. A collection with no rule costs one
    /// read and no decode.
    fn judge_stored_transition(
        &self,
        txn: &redb::WriteTransaction,
        p: &UpdateReindex<'_>,
        user_roles: &[String],
    ) -> crate::Result<()> {
        let Some(current) =
            self.current_row_body_in_txn(txn, p.database_id, p.tid, p.collection, p.doc_id)?
        else {
            return Ok(());
        };
        let config_key = (
            crate::types::DatabaseId::new(p.database_id),
            crate::types::TenantId::new(p.tid),
            p.collection.to_string(),
        );
        self.check_stored_update_transitions(
            &config_key,
            p.collection,
            &current,
            p.new_body,
            user_roles,
        )
        .map_err(crate::Error::DataPlane)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;
    use crate::engine::document::store::CollectionConfig;
    use crate::types::{DatabaseId, TenantId};
    use nodedb_physical::physical_plan::document::{StateTransitionDef, TransitionRule};
    use nodedb_types::{StorageKey, Surrogate};

    const TID: u64 = 1;
    const COLLECTION: &str = "shipments";
    const SURROGATE: Surrogate = Surrogate(7);

    /// A collection that admits `draft -> active` and nothing else on
    /// `status`, so every other change of that column is refused.
    fn draft_to_active(core: &mut CoreLoop, database_id: u64) {
        let mut config = CollectionConfig::new(COLLECTION);
        config.enforcement.state_constraints = vec![StateTransitionDef {
            name: "flow".to_string(),
            column: "status".to_string(),
            transitions: vec![TransitionRule {
                from: "draft".to_string(),
                to: "active".to_string(),
                required_role: None,
            }],
        }];
        core.doc_configs.insert(
            (
                DatabaseId::new(database_id),
                TenantId::new(TID),
                COLLECTION.to_string(),
            ),
            config,
        );
    }

    fn seed(core: &mut CoreLoop, database_id: u64, status: &str) {
        let row = serde_json::json!({"status": status});
        core.sparse
            .put(
                database_id,
                TID,
                COLLECTION,
                &StorageKey::for_surrogate(SURROGATE),
                &doc_format::encode_to_msgpack(&row),
                core.bitemporal_ordinal(),
            )
            .expect("seed row");
    }

    fn stored_status(core: &CoreLoop, database_id: u64) -> serde_json::Value {
        let stored = core
            .sparse
            .get(
                database_id,
                TID,
                COLLECTION,
                &StorageKey::for_surrogate(SURROGATE),
            )
            .expect("read row")
            .expect("row exists");
        doc_format::decode_document(&stored)
            .expect("row decodes")
            .get("status")
            .cloned()
            .unwrap_or(serde_json::Value::Null)
    }

    /// Land one row whose post-image is `active`. `stale` is the pre-image
    /// the statement's projection read, which the caller believes is still
    /// current.
    fn land_active(core: &mut CoreLoop, database_id: u64) -> crate::Result<()> {
        let stale = serde_json::json!({"status": "draft"});
        let post = serde_json::json!({"status": "active"});
        let post_bytes = doc_format::encode_to_msgpack(&post);
        let key = StorageKey::for_surrogate(SURROGATE);
        let sys_from = core
            .row_version_ordinal(database_id, TID, COLLECTION, &key)
            .expect("version ordinal");
        core.persist_bulk_update_row(
            UpdateReindex {
                database_id,
                tid: TID,
                collection: COLLECTION,
                doc_id: &key,
                sys_from,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                new_body: &post_bytes,
                index_paths: &[],
                old_doc: Some(&stale),
                new_doc: &post,
            },
            &stale,
            &HookCtx {
                database_id,
                tid: TID,
                collection: COLLECTION,
                resolved_targets: &[],
                deferred_sum_targets: &[],
                wal_lsn: None,
            },
            &[],
        )
        .map(|_| ())
    }

    /// The projection read `draft`, and another writer moved the row to
    /// `archived` before this row's transaction opened. `active` lands here,
    /// and `archived -> active` is not declared, so the row must be refused
    /// even though the projection's own judgement admitted it.
    #[test]
    fn a_transition_is_judged_against_the_row_the_write_replaces() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let database_id = make_default_task().request.database_id.as_u64();
        draft_to_active(&mut core, database_id);
        seed(&mut core, database_id, "archived");

        let landed = land_active(&mut core, database_id);

        assert!(
            landed.is_err(),
            "archived -> active is not a declared transition"
        );
        assert_eq!(
            stored_status(&core, database_id),
            serde_json::json!("archived"),
            "a refused row keeps its stored body"
        );
    }

    /// The control: the row is still the `draft` the projection read, so the
    /// same call lands the declared transition.
    #[test]
    fn a_transition_judged_on_the_same_row_still_lands() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let database_id = make_default_task().request.database_id.as_u64();
        draft_to_active(&mut core, database_id);
        seed(&mut core, database_id, "draft");

        land_active(&mut core, database_id).expect("draft -> active is admitted");

        assert_eq!(
            stored_status(&core, database_id),
            serde_json::json!("active")
        );
    }
}
