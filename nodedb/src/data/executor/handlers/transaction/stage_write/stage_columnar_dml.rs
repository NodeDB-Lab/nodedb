// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for columnar predicate DML —
//! `ColumnarOp::Delete` (`DELETE ... WHERE <predicate>`) and
//! `ColumnarOp::Update` (`UPDATE ... SET col = ... WHERE <predicate>`) —
//! issued inside a `BEGIN..COMMIT` block.
//!
//! Mirrors the Document predicate-DML staging (`stage_bulk_delete` /
//! `stage_bulk_update`) onto the columnar overlay: the predicate is evaluated
//! at statement time against the CURRENT in-transaction view (committed
//! memtable rows folded with this transaction's own already-staged overlay
//! via [`CoreLoop::merge_overlay_into_columnar_scan`]), so a same-transaction
//! scan observes the delete/update (read-your-own-writes) and the statement
//! reports its real affected-row count immediately.
//!
//! Staged representation:
//! - DELETE stages one `Staged::Tombstone` per affected surrogate — the merge
//!   drops tombstoned surrogates from the in-transaction scan.
//! - UPDATE stages one `Staged::Put` per affected surrogate carrying the
//!   updated row encoded exactly like `stage_columnar_insert` does
//!   (`Value::Array` via `nodedb_types::value_to_msgpack`), so the merge
//!   supersedes the base row with the new body via the same `decode_staged_row`
//!   path a staged INSERT surfaces through. A value-patch `Put` keyed by the
//!   same surrogate is the supersede — no separate tombstone+insert pair is
//!   needed because the overlay is surrogate-keyed and last-writer-wins.
//!
//! Because the matching set is resolved through the overlay merge, a row this
//! transaction just staged (a staged INSERT, or a row an earlier staged
//! UPDATE moved into the predicate) is affected too, not only committed base
//! rows.
//!
//! Row-level security: the write policy decides the whole matching set here —
//! a delete against the row it removes, an update against the post-image the
//! assignments produce — before any overlay entry is written. Deferring that to
//! COMMIT would report `{"affected": N}` for a statement the transaction can
//! never keep, and expose the refused image to its own reads meanwhile.
//!
//! COMMIT resolves the staged post-images and tombstones into the
//! transaction's redo record (`resolve::columnar_image`). The first statement
//! that stages a base row records that row's primary key
//! (`stage_columnar_base_key`), so the redo names the base row a key-changing
//! UPDATE or a DELETE removes. The staged set is resolved from the current
//! rows, flushed and in the memtable (plus overlay), the scope the
//! autocommit handlers (`execute_columnar_delete` / `execute_columnar_update`)
//! match against.

use crate::data::executor::core_loop::bitemporal_time::raised_above;
use crate::data::executor::handlers::columnar_write::system_stamp::stored_stamp;
use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::value::Value;
use nodedb_types::{RowIdentity, Surrogate, value_to_pk_string};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_assignments::ColumnarAssignments;
use crate::data::executor::handlers::columnar_read::convert::row_to_value;
use crate::data::executor::handlers::columnar_read::filter::row_matches_filters;
use crate::data::executor::handlers::columnar_resolve::CurrentColumnarRow;
use crate::data::executor::handlers::transaction::overlay::{
    ColumnarMatchedRow, ColumnarOverlayMergeParams,
};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

/// The client identity of a columnar row: the schema's primary-key value
/// when the row carries one that stringifies, else the decimal surrogate.
pub(super) fn columnar_row_identity(
    schema: &ColumnarSchema,
    row: &[Value],
    surrogate: u32,
) -> RowIdentity {
    schema
        .columns
        .iter()
        .position(|c| c.primary_key)
        .and_then(|idx| row.get(idx))
        .and_then(value_to_pk_string)
        .map(RowIdentity::from_user_key)
        .unwrap_or_else(|| RowIdentity::for_surrogate(Surrogate::new(surrogate)))
}

/// Routing identity + payload for one staged columnar predicate `DELETE`.
pub(in crate::data::executor) struct StageColumnarDeleteParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub filter_bytes: &'a [u8],
    /// Compiled row-level-security WRITE predicate carried by the plan,
    /// decided against the pre-image of every row this would remove.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

/// Routing identity + payload for one staged columnar predicate `UPDATE`.
pub(in crate::data::executor) struct StageColumnarUpdateParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub filter_bytes: &'a [u8],
    /// Field assignments, the same shape `execute_columnar_update` applies
    /// on the durable path.
    pub updates: &'a [(String, UpdateValue)],
    /// Compiled row-level-security WRITE predicate carried by the plan,
    /// decided against each row's post-image once the assignments are applied.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

impl CoreLoop {
    /// Stage a columnar predicate `DELETE` at statement time: resolve the
    /// current BASE ∪ OVERLAY matching set and tombstone each affected
    /// surrogate. Returns `{"affected": N}` in the same shape
    /// `execute_columnar_delete` returns for the autocommit path.
    pub(in crate::data::executor) fn stage_columnar_delete(
        &mut self,
        params: StageColumnarDeleteParams<'_>,
    ) -> Response {
        let StageColumnarDeleteParams {
            task,
            tid,
            txn_id,
            collection,
            filter_bytes,
            rls_write_check,
        } = params;

        let coll_key = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );

        let schema = match self.columnar_engine_schema(task, tid, collection) {
            Ok(s) => s,
            Err(resp) => return resp,
        };

        let affected_rows =
            match self.columnar_txn_matching_rows(task, tid, txn_id, collection, filter_bytes) {
                Ok(rows) => rows,
                Err(resp) => return resp,
            };

        // The image a delete is governed by is the row it removes. Decided for
        // the whole matching set before the first tombstone, so a refusal
        // leaves the overlay untouched.
        if !matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) && let Err(response) = self.stage_admit_columnar_rows(
            task,
            rls_write_check,
            affected_rows.iter().map(|(_, row)| row.as_slice()),
            &schema,
            tid,
            collection,
        ) {
            return response;
        }

        let affected = affected_rows.len();
        for (surrogate, row) in affected_rows {
            if let Err(e) =
                self.stage_note_columnar_base_row(txn_id, &coll_key, &schema, surrogate, &row)
            {
                return self.response_error(task, e);
            }
            let identity = columnar_row_identity(&schema, &row, surrogate);
            self.txn_overlay_mut(txn_id)
                .insert_tombstone(coll_key.clone(), surrogate, &identity);
        }

        self.stage_columnar_dml_response(task, affected)
    }

    /// Stage a columnar predicate `UPDATE` at statement time: resolve the
    /// current BASE ∪ OVERLAY matching set, apply the SET-list to each match,
    /// and record the new body as a staged `Put`. Returns `{"affected": N}` in
    /// the same shape `execute_columnar_update` returns for the autocommit
    /// path.
    pub(in crate::data::executor) fn stage_columnar_update(
        &mut self,
        params: StageColumnarUpdateParams<'_>,
    ) -> Response {
        let StageColumnarUpdateParams {
            task,
            tid,
            txn_id,
            collection,
            filter_bytes,
            updates,
            rls_write_check,
        } = params;

        let coll_key = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );

        // Column-name -> index resolution needs the schema; the resolver
        // already validates the engine + PK requirement and clones the schema,
        // but fetch it once here for the update application below.
        let schema = match self.columnar_engine_schema(task, tid, collection) {
            Ok(s) => s,
            Err(resp) => return resp,
        };

        let affected_rows =
            match self.columnar_txn_matching_rows(task, tid, txn_id, collection, filter_bytes) {
                Ok(rows) => rows,
                Err(resp) => return resp,
            };

        // Resolve every post-image and let the policy decide all of them before
        // the first staged put: the post-image is what the policy governs, and
        // it only exists once the assignments are applied. A refusal partway
        // through would leave the rows ahead of it staged and visible to this
        // transaction's own reads.
        //
        // An expression assignment evaluates against each row's pre-image, and
        // each post-image meets the declared column rule, as on the durable
        // path.
        let assignments = match ColumnarAssignments::bind(&schema, updates) {
            Ok(a) => a,
            Err(e) => return self.response_error(task, e),
        };
        let affected = affected_rows.len();
        let crs = self.crs_context();
        let mut new_rows: Vec<(u32, Vec<Value>)> = Vec::with_capacity(affected);
        let mut base_rows: Vec<(u32, Vec<Value>)> = Vec::with_capacity(affected);
        for (surrogate, row) in affected_rows {
            match assignments.apply(&schema, row.clone(), &crs) {
                Ok(r) => new_rows.push((surrogate, r)),
                Err(e) => return self.response_error(task, e),
            }
            base_rows.push((surrogate, row));
        }
        // A bitemporal row's new version takes the statement's system time,
        // raised above the version it supersedes, never the pre-image's.
        if let Some(ts_idx) = schema.ts_system_idx() {
            let base = self.columnar_system_ordinal(None, 0);
            for ((_, new_row), (_, row)) in new_rows.iter_mut().zip(&base_rows) {
                let stamp = raised_above(base, stored_stamp(&schema, row));
                self.observe_bitemporal_ordinal(stamp);
                if let Some(cell) = new_row.get_mut(ts_idx) {
                    *cell = Value::Integer(stamp);
                }
            }
        }
        if let Err(response) = self.stage_admit_columnar_rows(
            task,
            rls_write_check,
            new_rows.iter().map(|(_, row)| row.as_slice()),
            &schema,
            tid,
            collection,
        ) {
            return response;
        }

        // The key the pre-image carried names the base row a key-changing
        // update removes; recorded before the put below stages the surrogate.
        for (surrogate, row) in &base_rows {
            if let Err(e) =
                self.stage_note_columnar_base_row(txn_id, &coll_key, &schema, *surrogate, row)
            {
                return self.response_error(task, e);
            }
        }
        for (surrogate, new_row) in new_rows {
            let identity = columnar_row_identity(&schema, &new_row, surrogate);
            let body = match nodedb_types::value_to_msgpack(&Value::Array(new_row)) {
                Ok(b) => b,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("columnar update: row encode failed: {e}"),
                        },
                    );
                }
            };
            if let Err(e) =
                self.stage_bulk_put_capped(txn_id, &coll_key, surrogate, &identity, body)
            {
                return self.response_error(task, e);
            }
        }

        self.stage_columnar_dml_response(task, affected)
    }

    /// Resolve the CURRENT in-transaction matching set for a columnar
    /// predicate DELETE/UPDATE: committed current rows matching the WHERE
    /// predicate, folded with this transaction's own staged overlay
    /// (tombstones dropped, staged puts/inserts surfaced) via
    /// [`Self::merge_overlay_into_columnar_scan`]. Returns each affected row's
    /// `(surrogate, schema-ordered values)`.
    ///
    /// Mirrors `execute_columnar_delete` / `execute_columnar_update`: the base
    /// set is every current row, flushed and in the memtable (the same scope
    /// the durable handlers mutate), so the staged affected set matches
    /// exactly what COMMIT replay applies.
    pub(super) fn columnar_txn_matching_rows(
        &self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
        filter_bytes: &[u8],
    ) -> Result<Vec<(u32, Vec<Value>)>, Response> {
        let schema = self.columnar_engine_schema(task, tid, collection)?;

        let filter_predicates: Vec<ScanFilter> = if filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return Err(self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("columnar predicate DML: malformed filters: {e}"),
                        },
                    ));
                }
            }
        };

        let coll_key = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );

        // BASE: current rows, flushed and in the memtable, matching the
        // predicate, carried as the shared `ColumnarMatchedRow` tuple the
        // overlay merge consumes. A missing engine visits no row, so the only
        // affected rows are overlay-only staged inserts, which the merge
        // appends below.
        let mut matched: Vec<ColumnarMatchedRow> = Vec::new();
        let crs = self.crs_context();
        let base =
            self.for_each_current_columnar_row(&coll_key, "columnar_txn_dml_resolve", |row| {
                let CurrentColumnarRow { surrogate, values } = row;
                if !filter_predicates.is_empty()
                    && !row_matches_filters(&values, &schema, &filter_predicates, &crs)?
                {
                    return Ok(());
                }
                let obj = row_to_value(&values, &schema);
                matched.push((surrogate, values, obj));
                Ok(())
            });
        if let Err(e) = base {
            return Err(self.response_error(task, e));
        }

        // Fold the transaction's own staged writes into the base set: drops
        // tombstoned surrogates, re-checks staged puts against the predicate,
        // and appends overlay-only staged inserts that now match — so a row
        // this txn just inserted (or an earlier staged update moved into the
        // predicate) is affected too.
        if let Err(e) = self.merge_overlay_into_columnar_scan(
            ColumnarOverlayMergeParams {
                txn_id,
                coll_key: &coll_key,
                schema: &schema,
                projection: &[],
                filter_predicates: &filter_predicates,
                rls_predicates: &[],
                computed_cols: &[],
                all_versions: false,
            },
            &mut matched,
        ) {
            return Err(self.response_error(task, e));
        }

        Ok(matched
            .into_iter()
            .filter_map(|(surrogate, row, _)| surrogate.map(|s| (s.0, row)))
            .collect())
    }

    /// Fetch and clone the columnar engine schema for `collection`, enforcing
    /// the same PRIMARY KEY requirement the durable columnar mutation handlers
    /// enforce (`execute_columnar_update` / `execute_columnar_delete`) so a
    /// no-PK columnar UPDATE/DELETE fails at the statement inside a
    /// transaction exactly as it fails in autocommit — keeping statement-time
    /// success/failure aligned with COMMIT replay.
    pub(super) fn columnar_engine_schema(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
    ) -> Result<ColumnarSchema, Response> {
        let engine_key = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );
        let Some(engine) = self.columnar_engines.get(&engine_key) else {
            return Err(self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("columnar engine not found for collection '{collection}'"),
                },
            ));
        };
        let schema = engine.schema().clone();
        if !schema.columns.iter().any(|c| c.primary_key) {
            return Err(self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar UPDATE/DELETE requires a PRIMARY KEY column".into(),
                },
            ));
        }
        Ok(schema)
    }

    /// Encode the shared `{"affected": N}` payload columnar predicate DML
    /// returns (matching `execute_columnar_delete` / `execute_columnar_update`).
    pub(super) fn stage_columnar_dml_response(
        &self,
        task: &ExecutionTask,
        affected: usize,
    ) -> Response {
        match response_codec::encode_json_as_msgpack(&serde_json::json!({ "affected": affected })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
