// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for predicate `UPDATE ... WHERE <predicate>`
//! (`DocumentOp::BulkUpdate`) inside a transaction. A `RETURNING` clause does
//! not change what is staged — the matched rows' post-images are recorded
//! identically; the reply projects those post-images per the clause.
//!
//! Mirrors the point-write staging in `dispatch.rs`: the matched rows are
//! evaluated against BASE ∪ OVERLAY (via [`CoreLoop::txn_matching_rows`])
//! so a same-transaction earlier write to a row is observed, each matched
//! row's update is applied with the SAME per-row primitive point `UPDATE`
//! uses ([`CoreLoop::stage_apply_update`]), and the result is staged into the
//! overlay as a `Put` — never written durably here. COMMIT's buffered plan
//! replay remains the sole durable apply.

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::RowIdentity;

use super::body::stored_row_identity;
use super::constraint::StagedStatement;
use super::stage_returning::{StageReturning, StagedRowsOf};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::bulk_dml::{TxnPrediction, decode_bulk_filters};
use crate::data::executor::handlers::generated;
use crate::data::executor::handlers::transaction::overlay::MAX_TXN_OVERLAY_BYTES;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Routing identity + payload for one staged `BulkUpdate`, bundled to keep
/// the entry point within the argument-count budget.
pub(in crate::data::executor) struct StageBulkUpdateParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub filter_bytes: &'a [u8],
    pub updates: &'a [(String, UpdateValue)],
    /// Compiled RLS write policy gating each matched row's staged post-image.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Declared `PRIMARY KEY` column of a schemaless collection, `None`
    /// otherwise — see `stage_apply_update`'s post-image guard.
    pub declared_primary_key: Option<&'a str>,
    /// The plan's `RETURNING` spec, `None` when it carries none.
    pub returning: Option<StageReturning<'a>>,
    /// The rows the Control Plane read at this statement. `None` when the
    /// plan carries none.
    pub prediction: Option<TxnPrediction<'a>>,
    /// The cut BASE is matched at: `None` for each row's current version,
    /// `Some` for the block's snapshot ordinal.
    pub system_as_of: Option<i64>,
}

impl CoreLoop {
    /// Stage a predicate `UPDATE` at statement time: resolve the current
    /// BASE ∪ OVERLAY matching set, apply the SET-list to each match, and
    /// record the new body as a staged `Put`. Returns `{"affected": N}` in
    /// the same shape `execute_bulk_update` returns for the autocommit path.
    pub(in crate::data::executor) fn stage_bulk_update(
        &mut self,
        params: StageBulkUpdateParams<'_>,
    ) -> Response {
        let StageBulkUpdateParams {
            task,
            tid,
            txn_id,
            collection,
            filter_bytes,
            updates,
            rls_write_check,
            declared_primary_key,
            returning,
            prediction,
            system_as_of,
        } = params;
        let database_id = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (database_id, TenantId::new(tid), collection.to_string());

        // Reject direct updates to generated columns, matching the durable
        // and point-write staging paths.
        let config_key = (database_id, TenantId::new(tid), collection.to_string());
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) =
                generated::check_generated_readonly(updates, &config.enforcement.generated_columns)
        {
            return self.response_error(task, e);
        }

        // The BASE ∪ OVERLAY matching set: the transaction's own staged
        // writes fold into BASE, read at the match's cut.
        let rows = match decode_bulk_filters(filter_bytes)
            .and_then(|filters| self.txn_matching_rows(txn_id, &coll_key, &filters, system_as_of))
        {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };

        // The carried prediction is the read the Control Plane took at this
        // statement. Rows that differ refuse the stage before anything stages,
        // and the Control Plane reads again.
        if let Some(prediction) = prediction
            && !prediction.holds(&rows)
        {
            return self.response_error(task, ErrorCode::OllpRetryRequired);
        }

        let strict_schema = self.resolve_strict_schema(database_id.as_u64(), tid, collection);
        let mut post_images: Vec<(u32, RowIdentity, Vec<u8>)> = Vec::with_capacity(rows.len());
        for (row_key, current_body) in &rows {
            let surrogate = row_key.surrogate().as_u32();
            let new_body = match self.stage_apply_update(
                database_id.as_u64(),
                tid,
                collection,
                current_body,
                updates,
                declared_primary_key,
            ) {
                Ok(b) => b,
                Err(e) => return self.response_error(task, e),
            };
            // Gate this row's staged post-image on the collection's write
            // policy. A rejected row fails the statement rather than being
            // skipped: skipping would under-report `affected` while the rest of
            // the predicate's matches were still rewritten.
            let identity = stored_row_identity(
                &new_body,
                strict_schema.as_ref(),
                declared_primary_key,
                *row_key,
            );
            if let Err(e) = self.stage_admit_write(
                rls_write_check,
                &new_body,
                &identity,
                database_id.as_u64(),
                tid,
                collection,
            ) {
                return self.response_error(task, e);
            }
            post_images.push((surrogate, identity, new_body));
        }

        // UNIQUE holds at the end of the statement: the matched rows are
        // judged together, so a value one of them releases is free for
        // another.
        let judged: Vec<(u32, &[u8])> = post_images
            .iter()
            .map(|(surrogate, _, body)| (*surrogate, body.as_slice()))
            .collect();
        if let Err(e) = self.stage_stored_unique_check(
            &StagedStatement {
                database_id: database_id.as_u64(),
                tid,
                txn_id,
                coll_key: &coll_key,
            },
            &judged,
        ) {
            return self.response_error(task, e);
        }

        let mut affected = 0u64;
        let mut staged: Vec<(RowIdentity, Vec<u8>)> = Vec::new();
        for (surrogate, identity, new_body) in post_images {
            if returning.is_some() {
                staged.push((identity.clone(), new_body.clone()));
            }
            if let Err(e) =
                self.stage_bulk_put_capped(txn_id, &coll_key, surrogate, &identity, new_body)
            {
                return self.response_error(task, e);
            }
            affected += 1;
        }

        if let Some(returning) = returning {
            let rows: Vec<(&RowIdentity, &[u8])> = staged
                .iter()
                .map(|(identity, body)| (identity, body.as_slice()))
                .collect();
            return self.staged_returning_response(
                StagedRowsOf {
                    task,
                    database_id: database_id.as_u64(),
                    tid,
                    collection,
                },
                returning,
                affected,
                &rows,
            );
        }

        match response_codec::encode_json_as_msgpack(&serde_json::json!({ "affected": affected })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Stage a bulk-matched row's new body after enforcing the same
    /// per-transaction overlay memory cap point writes enforce
    /// (`stage_put_capped` in `dispatch.rs`): reject with the identical
    /// [`crate::Error::TxnOverlayMemoryExceeded`] once the cumulative staged
    /// byte total would exceed [`MAX_TXN_OVERLAY_BYTES`]. Rows staged before
    /// the row that trips the cap remain staged — the same
    /// stage-then-fail-on-overflow behavior `stage_put_capped` exhibits for a
    /// single point write, applied per matched row here.
    pub(in crate::data::executor) fn stage_bulk_put_capped(
        &mut self,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
        doc_id: &RowIdentity,
        body: Vec<u8>,
    ) -> crate::Result<()> {
        let current = self
            .txn_overlays
            .get(&txn_id)
            .map(|o| o.memory_size_estimate())
            .unwrap_or(0);
        if current.saturating_add(body.len()) > MAX_TXN_OVERLAY_BYTES {
            return Err(crate::Error::TxnOverlayMemoryExceeded {
                limit: MAX_TXN_OVERLAY_BYTES,
            });
        }
        self.txn_overlay_mut(txn_id)
            .insert_put(coll_key.clone(), surrogate, doc_id, body);
        Ok(())
    }
}
