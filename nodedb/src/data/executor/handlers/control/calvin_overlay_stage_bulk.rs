// SPDX-License-Identifier: BUSL-1.1

//! Calvin overlay staging for predicate DML — `DocumentOp::BulkUpdate` /
//! `BulkDelete` — split out of `calvin_overlay_stage.rs` to stay within the
//! file-size limit.
//!
//! # The determinism rule
//!
//! Staging resolves EXACTLY the CP-injected `ollp_predicted_surrogates` set,
//! verbatim, in ascending surrogate order, as `ollp_predicted_doc_ids` orders
//! it. The install applies the redo record `CalvinResolve` builds from this
//! staging, so the staged rows are the rows the install writes.
//!
//! # The prediction check
//!
//! Before a bulk plan stages, the leader checks its prediction against the
//! rows the predicate matches at the plan's turn. BASE is read at the plan's
//! match cut: the transaction's snapshot inside a block, the current version
//! otherwise. The overlay of the plans staged before it folds in on top. A
//! session stage on a single shard matches the same way, for documents and
//! columnar alike. The surrogate set, the implicit edges of matched edge
//! documents, and the materialized-sum coverage must all hold. A miss
//! refuses the stage with `OllpRetryRequired`, which votes
//! `PredictionDrift`. Only the data-group leader stages, and a bulk plan
//! holds its collection's lock, so a lagging follower never judges.
//!
//! The stage reads each predicted row's body from the same match: the
//! version at the cut with the overlay on top. A delete takes its identity
//! from it, and an update applies its assignments to it.
//!
//! # Why a snapshot match stays serializable
//!
//! The install writes over the current state, not the snapshot. The slice's
//! serialization vote (`calvin::snapshot_vote`) runs after the stage. A
//! predicate write records a predicate read of its collection, so the vote
//! commits only while no row of the collection on this vShard holds a
//! committed version above the snapshot. Under a commit vote each row's
//! current version is its version at the snapshot. The match, the bodies and
//! the post-images are then the ones a current-state match yields. Under any
//! other vote the transaction aborts, and nothing the stage built installs.
//!
//! `BulkUpdate`'s per-row transform reuses `CoreLoop::stage_apply_update`,
//! the decode → apply-updates → recompute-generated → re-encode pipeline
//! `execute_bulk_update` and `stage_point_update` share.
//!
//! Each stage returns the number of rows it staged, the affected count the
//! statement reports.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::{ResolvedSumTarget, UpdateValue};
use nodedb_types::StorageKey;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::materialized_sum::divergence::SumTargetCheck;
use crate::data::executor::handlers::bulk_dml::{TxnPrediction, decode_bulk_filters};
use crate::data::executor::handlers::transaction::stage_write::{
    StagedStatement, stored_row_identity,
};
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// What a Calvin bulk plan carries for its prediction check.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct CalvinBulkCheck<'a> {
    /// The predicate's serialized filters.
    pub filters: &'a [u8],
    /// The cut BASE is matched at: `None` for the current rows, `Some` for
    /// the transaction's snapshot ordinal.
    pub system_as_of: Option<i64>,
    /// The rows the Control Plane predicted. `None` when the plan carries none.
    pub prediction: Option<TxnPrediction<'a>>,
    /// The materialized-sum resolution the plan carries.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

/// Borrowed inputs for [`CoreLoop::stage_calvin_bulk_delete`], grouped so the
/// method stays within the argument-count limit.
pub(in crate::data::executor) struct CalvinBulkDeleteStage<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub check: CalvinBulkCheck<'a>,
    /// Compiled RLS write policy deciding each removed row's pre-image.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// The collection's DDL-declared primary key, when it has one. Names the
    /// column each removed row's identity is read from.
    pub declared_primary_key: Option<&'a str>,
}

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

/// Borrowed inputs for [`CoreLoop::stage_calvin_bulk_update`], grouped so the
/// method stays within the argument-count limit.
pub(in crate::data::executor) struct CalvinBulkUpdateStage<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub updates: &'a [(String, UpdateValue)],
    pub check: CalvinBulkCheck<'a>,
    /// Compiled RLS write policy gating each staged post-image.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// The collection's DDL-declared primary key, when it has one. A staged
    /// post-image that nulls it is refused.
    pub declared_primary_key: Option<&'a str>,
}

impl CoreLoop {
    /// Stage a Calvin `BulkDelete` into the overlay: one tombstone per
    /// predicted surrogate, in ascending order, keyed by its surrogate's
    /// storage key. NOT a live predicate rescan. Returns the number of
    /// predicted rows that exist.
    pub(in crate::data::executor) fn stage_calvin_bulk_delete(
        &mut self,
        params: CalvinBulkDeleteStage<'_>,
    ) -> crate::Result<usize> {
        let CalvinBulkDeleteStage {
            task,
            tid,
            txn_id,
            collection,
            check,
            rls_write_check,
            declared_primary_key,
        } = params;
        let Some(prediction) = check.prediction else {
            return Err(missing_prediction_error(collection));
        };
        let database_id = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (database_id, TenantId::new(tid), collection.to_string());
        let matched = self.check_calvin_prediction(txn_id, &coll_key, &[], &check, prediction)?;

        // Each row's identity is read once from its matched body, by the rule
        // INSERT minted it with, so an earlier plan of the same transaction
        // is observed. A row with no body removes nothing; its tombstone is
        // keyed by the decimal surrogate.
        let strict_schema = self.resolve_strict_schema(database_id.as_u64(), tid, collection);
        let mut rows: Vec<(u32, nodedb_types::RowIdentity, Option<Vec<u8>>)> =
            Vec::with_capacity(prediction.surrogates.len());
        for (surrogate, doc_id, body) in predicted_rows(prediction.surrogates, matched) {
            let identity = match &body {
                Some(body) => {
                    stored_row_identity(body, strict_schema.as_ref(), declared_primary_key, doc_id)
                }
                None => doc_id.to_identity(),
            };
            rows.push((surrogate, identity, body));
        }

        // Decide every predicted row's pre-deletion image against the write
        // policy BEFORE any tombstone is staged, so a rejected row cannot leave
        // the rows ahead of it hidden from the rest of the transaction. A row
        // with no current body removes nothing and is admitted.
        if !matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            for (_, identity, body) in &rows {
                if let Some(body) = body {
                    self.stage_admit_write(
                        rls_write_check,
                        body,
                        identity,
                        database_id.as_u64(),
                        tid,
                        collection,
                    )?;
                }
            }
        }

        let removed = rows.iter().filter(|(_, _, body)| body.is_some()).count();
        let overlay = self.txn_overlay_mut(txn_id);
        for (surrogate, identity, _body) in &rows {
            overlay.insert_tombstone(coll_key.clone(), *surrogate, identity);
        }
        Ok(removed)
    }

    /// Stage a Calvin `BulkUpdate` into the overlay: for each predicted
    /// surrogate, take its matched body: BASE at the plan's match cut with
    /// the overlay of earlier ops of the same Calvin transaction on top
    /// (read-your-own-writes). Apply `updates`
    /// via the exact same per-row transform `execute_bulk_update` /
    /// `stage_point_update` use (`CoreLoop::stage_apply_update`), and stage
    /// the post-image as a `Put`. Row set = predicted surrogates, matching
    /// the install set exactly (and correctly excluding rows a
    /// same-transaction `INSERT` created after the predicted set was
    /// computed at recon).
    ///
    /// A predicted surrogate with no matched body (already tombstoned in
    /// this transaction, or absent at the cut) is skipped — the
    /// identical `continue`-on-miss behavior `execute_bulk_update` exhibits
    /// for its own `apply_ids` loop. Returns the number of rows staged.
    pub(in crate::data::executor) fn stage_calvin_bulk_update(
        &mut self,
        params: CalvinBulkUpdateStage<'_>,
    ) -> crate::Result<usize> {
        let CalvinBulkUpdateStage {
            task,
            tid,
            txn_id,
            collection,
            updates,
            check,
            rls_write_check,
            declared_primary_key,
        } = params;
        let Some(prediction) = check.prediction else {
            return Err(missing_prediction_error(collection));
        };
        let database_id = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (database_id, TenantId::new(tid), collection.to_string());
        let matched =
            self.check_calvin_prediction(txn_id, &coll_key, updates, &check, prediction)?;
        let strict_schema = self.resolve_strict_schema(database_id.as_u64(), tid, collection);

        let mut post_images = Vec::new();
        for (surrogate, storage_key, body) in predicted_rows(prediction.surrogates, matched) {
            // The matched body: the version at the cut with the overlay on
            // top (read-your-own-writes).
            let Some(current_bytes) = body else {
                continue;
            };

            let new_body = self.stage_apply_update(
                database_id.as_u64(),
                tid,
                collection,
                &current_bytes,
                updates,
                declared_primary_key,
            )?;
            // Decide the staged post-image against the write policy: this is
            // the row the stamped redo install writes.
            let identity = stored_row_identity(
                &new_body,
                strict_schema.as_ref(),
                declared_primary_key,
                storage_key,
            );
            self.stage_admit_write(
                rls_write_check,
                &new_body,
                &identity,
                database_id.as_u64(),
                tid,
                collection,
            )?;
            post_images.push((surrogate, identity, new_body));
        }

        // UNIQUE holds at the end of the statement: the rows are judged
        // together, so a value one of them releases is free for another.
        let judged: Vec<(u32, &[u8])> = post_images
            .iter()
            .map(|(surrogate, _, body)| (*surrogate, body.as_slice()))
            .collect();
        self.stage_stored_unique_check(
            &StagedStatement {
                database_id: database_id.as_u64(),
                tid,
                txn_id,
                coll_key: &coll_key,
            },
            &judged,
        )?;

        let updated = post_images.len();
        for (surrogate, identity, new_body) in post_images {
            self.stage_bulk_put_capped(txn_id, &coll_key, surrogate, &identity, new_body)?;
        }
        Ok(updated)
    }

    /// Refuse a Calvin bulk plan whose prediction does not hold at its turn
    /// in the transaction: BASE at the plan's match cut with the overlay of
    /// the plans staged before it. The surrogate set and the implicit edges
    /// must equal the rows'. The materialized-sum resolution must cover every
    /// join value the rows require, read after `updates` for an update. A
    /// miss refuses with `OllpRetryRequired`, which votes `PredictionDrift`.
    /// Returns the matched rows.
    fn check_calvin_prediction(
        &self,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        updates: &[(String, UpdateValue)],
        check: &CalvinBulkCheck<'_>,
        prediction: TxnPrediction<'_>,
    ) -> crate::Result<Vec<(StorageKey, Vec<u8>)>> {
        let drift = || crate::Error::DataPlane(ErrorCode::OllpRetryRequired);
        let filters = decode_bulk_filters(check.filters)?;
        let rows = self.txn_matching_rows(txn_id, coll_key, &filters, check.system_as_of)?;
        if !prediction.holds(&rows) {
            return Err(drift());
        }
        let sum_check = SumTargetCheck {
            database_id: coll_key.0.as_u64(),
            tid: coll_key.1.as_u64(),
            collection: coll_key.2.as_str(),
            updates,
            resolved: check.resolved_sum_targets,
        };
        if !self.declares_materialized_sums(&sum_check) {
            return Ok(rows);
        }
        let images: Vec<serde_json::Value> = rows
            .iter()
            .filter_map(|(_, body)| self.decode_source_row(&sum_check, body))
            .collect();
        if self.sum_targets_diverged(&sum_check, &images) {
            return Err(drift());
        }
        Ok(rows)
    }
}

/// Each predicted surrogate in ascending order, with its storage key and its
/// body among `matched`. `None` when `matched` holds no row for it.
fn predicted_rows(
    predicted: &[u32],
    matched: Vec<(StorageKey, Vec<u8>)>,
) -> Vec<(u32, StorageKey, Option<Vec<u8>>)> {
    let mut bodies: BTreeMap<u32, Vec<u8>> = matched
        .into_iter()
        .map(|(key, body)| (key.surrogate().as_u32(), body))
        .collect();
    let mut sorted = predicted.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    sorted
        .into_iter()
        .map(|surrogate| {
            let key = StorageKey::for_surrogate(nodedb_types::Surrogate::new(surrogate));
            (surrogate, key, bodies.remove(&surrogate))
        })
        .collect()
}
