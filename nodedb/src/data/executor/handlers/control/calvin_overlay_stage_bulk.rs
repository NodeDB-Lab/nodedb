// SPDX-License-Identifier: BUSL-1.1

//! Calvin overlay staging for predicate DML — `DocumentOp::BulkUpdate` /
//! `BulkDelete` — split out of `calvin_overlay_stage.rs` to stay within the
//! file-size limit.
//!
//! # The determinism rule
//!
//! The Calvin flush apply (`execute_bulk_delete` / `execute_bulk_update`)
//! mutates EXACTLY the CP-injected `ollp_predicted_surrogates` set, verbatim,
//! on every replica — see `super::super::bulk_dml::delete`'s `apply_ids`
//! derivation and `super::super::bulk_dml::scan::ollp_predicted_doc_ids`. A
//! live predicate rescan is NOT used as the apply set when a prediction is
//! present, because a follower's local snapshot can legitimately lag the
//! leader's verified prediction window; re-deriving the row set locally would
//! diverge across replicas.
//!
//! Staging here must therefore resolve rows from `ollp_predicted_surrogates`
//! — the SAME set the flush applies — via the SAME `ollp_predicted_doc_ids`
//! primitive, never via a fresh `scan_matching_documents` predicate scan (the
//! `stage_bulk_delete` / `stage_bulk_update` session-transaction handlers do
//! exactly that live rescan and are NOT reused here for this reason).
//!
//! Reading each predicted surrogate's CURRENT body (for `BulkUpdate`'s
//! post-image and read-your-own-writes) is still safe to source from local
//! BASE ∪ OVERLAY: Calvin's deterministic total order guarantees every
//! replica has applied the identical prior-ops prefix before this op stages,
//! so the content at a fixed, already-agreed-upon key is identical across
//! replicas — unlike predicate *membership*, which is what the surrogate-set
//! fixing above protects against.
//!
//! `BulkUpdate`'s per-row transform reuses `CoreLoop::stage_apply_update`
//! verbatim — the exact same decode → apply-updates → recompute-generated →
//! re-encode pipeline `execute_bulk_update` and `stage_point_update` already
//! share — so the staged post-image is byte-identical to what the flush
//! apply would produce for the same input body.

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::bulk_dml::scan::ollp_predicted_doc_ids;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Loudly reject a Calvin bulk predicate plan that reached overlay staging
/// without a predicted surrogate set. A Calvin-reachable bulk plan always
/// carries one (injected at Control-Plane recon before dispatch); a plan
/// shape missing it must never be staged as if it were a no-op, or the redo
/// this staging feeds would silently omit the write entirely.
fn missing_prediction_error(collection: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "calvin bulk predicate write reached overlay staging without \
             ollp_predicted_surrogates for collection '{collection}'; a Calvin bulk \
             plan must be recon-injected before dispatch"
        ),
    }
}

impl CoreLoop {
    /// Stage a Calvin `BulkDelete` into the overlay: one tombstone per
    /// predicted surrogate, resolved to its doc-id via
    /// `ollp_predicted_doc_ids` — the identical primitive the flush apply
    /// uses to derive `apply_ids`. NOT a live predicate rescan.
    pub(in crate::data::executor) fn stage_calvin_bulk_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
        ollp_predicted_surrogates: Option<&[u32]>,
    ) -> crate::Result<()> {
        let Some(predicted) = ollp_predicted_surrogates else {
            return Err(missing_prediction_error(collection));
        };
        let coll_key: (DatabaseId, TenantId, String) = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );

        let mut predicted_sorted: Vec<u32> = predicted.to_vec();
        predicted_sorted.sort_unstable();
        let doc_ids = ollp_predicted_doc_ids(predicted);

        let overlay = self.txn_overlay_mut(txn_id);
        for (surrogate, doc_id) in predicted_sorted.into_iter().zip(doc_ids) {
            overlay.insert_tombstone(coll_key.clone(), surrogate, &doc_id);
        }
        Ok(())
    }

    /// Stage a Calvin `BulkUpdate` into the overlay: for each predicted
    /// surrogate, read its current BASE ∪ OVERLAY body (read-your-own-writes
    /// against earlier ops in the same Calvin transaction), apply `updates`
    /// via the exact same per-row transform `execute_bulk_update` /
    /// `stage_point_update` use (`CoreLoop::stage_apply_update`), and stage
    /// the post-image as a `Put`. Row set = predicted surrogates, matching
    /// the flush apply set exactly (and correctly excluding rows a
    /// same-transaction `INSERT` created after the predicted set was
    /// computed at recon).
    ///
    /// A predicted surrogate that resolves to no current body (already
    /// tombstoned in this transaction, or absent from BASE) is skipped — the
    /// identical `continue`-on-miss behavior `execute_bulk_update` exhibits
    /// for its own `apply_ids` loop.
    pub(in crate::data::executor) fn stage_calvin_bulk_update(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
        updates: &[(String, UpdateValue)],
        ollp_predicted_surrogates: Option<&[u32]>,
    ) -> crate::Result<()> {
        let Some(predicted) = ollp_predicted_surrogates else {
            return Err(missing_prediction_error(collection));
        };
        let database_id = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (database_id, TenantId::new(tid), collection.to_string());
        let bitemporal = self.is_bitemporal(database_id.as_u64(), tid, collection);

        let mut predicted_sorted: Vec<u32> = predicted.to_vec();
        predicted_sorted.sort_unstable();

        for surrogate in predicted_sorted {
            let doc_id = surrogate_to_doc_id(Surrogate::new(surrogate));

            // Current body: overlay wins over base (read-your-own-writes),
            // mirroring `stage_point_update`'s exact overlay-then-base read.
            let overlay_cur = self
                .txn_overlays
                .get(&txn_id)
                .and_then(|o| o.get(&coll_key, surrogate))
                .cloned();
            let current_bytes = match overlay_cur {
                Some(Staged::Put(body)) => body,
                Some(Staged::Tombstone) => continue,
                None => {
                    let read = if bitemporal {
                        self.sparse.versioned_get_current(
                            database_id.as_u64(),
                            tid,
                            collection,
                            &doc_id,
                        )
                    } else {
                        self.sparse
                            .get(database_id.as_u64(), tid, collection, &doc_id)
                    };
                    match read {
                        Ok(Some(bytes)) => bytes,
                        Ok(None) => continue,
                        Err(e) => return Err(e),
                    }
                }
            };

            let new_body = self.stage_apply_update(
                database_id.as_u64(),
                tid,
                collection,
                &current_bytes,
                updates,
            )?;
            self.stage_bulk_put_capped(txn_id, &coll_key, surrogate, &doc_id, new_body)?;
        }
        Ok(())
    }
}
