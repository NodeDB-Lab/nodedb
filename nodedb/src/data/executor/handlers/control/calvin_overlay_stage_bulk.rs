// SPDX-License-Identifier: BUSL-1.1

//! Calvin overlay staging for predicate DML — `DocumentOp::BulkUpdate` /
//! `BulkDelete` — split out of `calvin_overlay_stage.rs` to stay within the
//! file-size limit.
//!
//! # The determinism rule
//!
//! Staging resolves EXACTLY the CP-injected `ollp_predicted_surrogates` set,
//! verbatim, on every replica, via the `ollp_predicted_doc_ids` primitive. A
//! live predicate rescan is NOT used as the row set, because a follower's
//! local snapshot can legitimately lag the leader's verified prediction
//! window. Re-deriving the row set locally would diverge across replicas.
//! The flush installs the redo record `CalvinResolve` builds from this
//! staging, so the staged rows are the rows the flush writes. The
//! `stage_bulk_delete` / `stage_bulk_update` session-transaction handlers do
//! a live rescan and are NOT reused here for this reason.
//!
//! Reading each predicted surrogate's CURRENT body (for `BulkUpdate`'s
//! post-image and read-your-own-writes) is still safe to source from local
//! BASE ∪ OVERLAY: Calvin's deterministic total order guarantees every
//! replica has applied the identical prior-ops prefix before this op stages,
//! so the content at a fixed, already-agreed-upon key is identical across
//! replicas — unlike predicate *membership*, which is what the surrogate-set
//! fixing above protects against.
//!
//! `BulkUpdate`'s per-row transform reuses `CoreLoop::stage_apply_update`,
//! the decode → apply-updates → recompute-generated → re-encode pipeline
//! `execute_bulk_update` and `stage_point_update` share.
//!
//! Each stage returns the number of rows it staged, the affected count the
//! statement reports.

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::bulk_dml::scan::ollp_predicted_doc_ids;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::handlers::transaction::stage_write::stored_row_identity;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Borrowed inputs for [`CoreLoop::stage_calvin_bulk_delete`], grouped so the
/// method stays within the argument-count limit.
pub(in crate::data::executor) struct CalvinBulkDeleteStage<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub ollp_predicted_surrogates: Option<&'a [u32]>,
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
    pub ollp_predicted_surrogates: Option<&'a [u32]>,
    /// Compiled RLS write policy gating each staged post-image.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// The collection's DDL-declared primary key, when it has one. A staged
    /// post-image that nulls it is refused.
    pub declared_primary_key: Option<&'a str>,
}

impl CoreLoop {
    /// Stage a Calvin `BulkDelete` into the overlay: one tombstone per
    /// predicted surrogate, resolved to its doc-id via
    /// `ollp_predicted_doc_ids` — the identical primitive the flush apply
    /// uses to derive `apply_ids`. NOT a live predicate rescan. Returns the
    /// number of predicted rows that exist.
    pub(in crate::data::executor) fn stage_calvin_bulk_delete(
        &mut self,
        params: CalvinBulkDeleteStage<'_>,
    ) -> crate::Result<usize> {
        let CalvinBulkDeleteStage {
            task,
            tid,
            txn_id,
            collection,
            ollp_predicted_surrogates,
            rls_write_check,
            declared_primary_key,
        } = params;
        let Some(predicted) = ollp_predicted_surrogates else {
            return Err(missing_prediction_error(collection));
        };
        let database_id = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (database_id, TenantId::new(tid), collection.to_string());

        let mut predicted_sorted: Vec<u32> = predicted.to_vec();
        predicted_sorted.sort_unstable();
        let doc_ids = ollp_predicted_doc_ids(predicted);

        // Each row's identity is read once from its current body under
        // BASE ∪ OVERLAY, by the rule INSERT minted it with, so an earlier
        // plan of the same transaction is observed. A row with no current
        // body removes nothing; its tombstone is keyed by the decimal
        // surrogate.
        let strict_schema = self.resolve_strict_schema(database_id.as_u64(), tid, collection);
        let bitemporal = self.is_bitemporal(database_id.as_u64(), tid, collection);
        let mut rows: Vec<(u32, nodedb_types::RowIdentity, Option<Vec<u8>>)> =
            Vec::with_capacity(doc_ids.len());
        for (surrogate, doc_id) in predicted_sorted.into_iter().zip(doc_ids) {
            let body = self.calvin_current_body(CalvinRowRead {
                txn_id,
                coll_key: &coll_key,
                surrogate,
                storage_key: &doc_id,
                bitemporal,
            })?;
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
            ollp_predicted_surrogates,
            rls_write_check,
            declared_primary_key,
        } = params;
        let Some(predicted) = ollp_predicted_surrogates else {
            return Err(missing_prediction_error(collection));
        };
        let database_id = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (database_id, TenantId::new(tid), collection.to_string());
        let bitemporal = self.is_bitemporal(database_id.as_u64(), tid, collection);

        let mut predicted_sorted: Vec<u32> = predicted.to_vec();
        predicted_sorted.sort_unstable();
        let strict_schema = self.resolve_strict_schema(database_id.as_u64(), tid, collection);

        let mut updated = 0usize;
        for surrogate in predicted_sorted {
            let storage_key = nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate));

            // Current body: overlay wins over base (read-your-own-writes),
            // mirroring `stage_point_update`'s exact overlay-then-base read.
            let Some(current_bytes) = self.calvin_current_body(CalvinRowRead {
                txn_id,
                coll_key: &coll_key,
                surrogate,
                storage_key: &storage_key,
                bitemporal,
            })?
            else {
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
            // the row the Calvin flush will install.
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
            self.stage_bulk_put_capped(txn_id, &coll_key, surrogate, &identity, new_body)?;
            updated += 1;
        }
        Ok(updated)
    }
}

/// One row a Calvin bulk stage reads under BASE ∪ OVERLAY.
struct CalvinRowRead<'a> {
    txn_id: TxnId,
    coll_key: &'a (DatabaseId, TenantId, String),
    surrogate: u32,
    storage_key: &'a nodedb_types::StorageKey,
    bitemporal: bool,
}

impl CoreLoop {
    /// The row's current stored body inside the Calvin transaction. A staged
    /// put wins over base. A staged tombstone or a staged TRUNCATE hides the
    /// row. Otherwise the body is base storage: the current version on a
    /// bitemporal collection.
    fn calvin_current_body(&self, read: CalvinRowRead<'_>) -> crate::Result<Option<Vec<u8>>> {
        let overlay = self.txn_overlays.get(&read.txn_id);
        match overlay.and_then(|o| o.get(read.coll_key, read.surrogate)) {
            Some(Staged::Put(body)) => return Ok(Some(body.clone())),
            Some(Staged::Tombstone) => return Ok(None),
            None => {}
        }
        if overlay.is_some_and(|o| !o.base_visible(read.coll_key)) {
            return Ok(None);
        }
        let (database_id, tenant, collection) = read.coll_key;
        if read.bitemporal {
            self.sparse.versioned_get_current(
                database_id.as_u64(),
                tenant.as_u64(),
                collection,
                read.storage_key,
            )
        } else {
            self.sparse.get(
                database_id.as_u64(),
                tenant.as_u64(),
                collection,
                read.storage_key,
            )
        }
    }
}
