// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for `ColumnarOp::ResolvedUpdate` /
//! `ColumnarOp::ResolvedDelete` — the resolved-row-set form of columnar
//! predicate DML, built only for a collection carrying an RLS write policy.
//!
//! The Control Plane already resolved the predicate to concrete rows and
//! decided the write policy against their exact images while the writing
//! identity was live (`rls_write_check` carries the verdict
//! `DecidedEarlierInRequest`, not a predicate to re-evaluate — see
//! `nodedb-physical`'s `ColumnarOp` doc comments). Staging therefore does no
//! predicate evaluation of its own: it locates each shipped PK in the CURRENT
//! in-transaction view — committed memtable rows folded with this
//! transaction's own staged overlay — by reusing
//! [`CoreLoop::columnar_txn_matching_rows`] with a synthetic `PK IN (...)`
//! filter over exactly the shipped PK set, then records the overlay entry the
//! same way [`super::stage_columnar_dml`]'s predicate staging does.
//!
//! ## Drift check
//!
//! The row set was resolved before this statement reached the Data Plane, so
//! a shipped PK can have vanished from the current in-transaction view by the
//! time staging runs (a sibling statement in this same transaction, or a
//! commit on another connection this transaction's snapshot has not observed
//! yet). If any shipped PK is not found, staging nothing and returning
//! `ErrorCode::OllpRetryRequired` keeps this statement-time check aligned
//! with the same drift check the durable apply
//! (`execute_columnar_resolved_update` / `execute_columnar_resolved_delete`)
//! runs at COMMIT replay — both refuse a shipped-but-vanished row rather than
//! silently dropping it from the affected count.
//!
//! COMMIT durable replay is unchanged: the buffered `ColumnarOp::ResolvedUpdate`
//! / `ColumnarOp::ResolvedDelete` plan is still replayed through
//! `execute_columnar_resolved_update` / `execute_columnar_resolved_delete`
//! inside the COMMIT `TransactionBatch`, which remains the sole durable apply.

use std::collections::HashMap;

use nodedb_columnar::pk_index::encode_pk;
use nodedb_query::scan_filter::FilterOp;
use nodedb_types::value::Value;
use nodedb_types::{RowIdentity, value_to_pk_string};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

/// The client identity of a resolved columnar row: its shipped primary-key
/// value. The PK is mandatory on the resolved variants, so a value with no
/// primary-key string form is a plan error, never a fallback.
fn resolved_pk_identity(collection: &str, pk: &Value) -> crate::Result<RowIdentity> {
    value_to_pk_string(pk)
        .map(RowIdentity::from_user_key)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!(
                "columnar resolved DML on '{collection}': primary key value {pk:?} has no \
                 primary-key string form"
            ),
        })
}

/// Routing identity + payload for a staged columnar `ResolvedUpdate`.
pub(in crate::data::executor) struct StageColumnarResolvedUpdateParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    /// `(primary key, full post-image)` for each row the Control Plane
    /// resolved and the write policy admitted.
    pub rows: &'a [(Value, Vec<Value>)],
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

/// Routing identity + payload for a staged columnar `ResolvedDelete`.
pub(in crate::data::executor) struct StageColumnarResolvedDeleteParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub pks: &'a [Value],
}

impl CoreLoop {
    /// Stage a columnar `ResolvedUpdate`: locate each shipped PK in the
    /// current BASE ∪ OVERLAY view and record its post-image as a staged
    /// `Put`, keyed by the surrogate that view resolves the PK to. Returns
    /// `{"affected": N}` in the same shape `execute_columnar_resolved_update`
    /// returns for the autocommit path.
    pub(in crate::data::executor) fn stage_columnar_resolved_update(
        &mut self,
        params: StageColumnarResolvedUpdateParams<'_>,
    ) -> Response {
        let StageColumnarResolvedUpdateParams {
            task,
            tid,
            txn_id,
            collection,
            rows,
            rls_write_check,
        } = params;

        if rows.is_empty() {
            return self.stage_columnar_dml_response(task, 0);
        }

        let coll_key = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );

        let schema = match self.columnar_engine_schema(task, tid, collection) {
            Ok(s) => s,
            Err(resp) => return resp,
        };
        let Some(pk_idx) = schema.columns.iter().position(|c| c.primary_key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar UPDATE requires a PRIMARY KEY column".into(),
                },
            );
        };
        let pk_col_name = schema.columns[pk_idx].name.clone();

        let shipped_pks: Vec<Value> = rows.iter().map(|(pk, _)| pk.clone()).collect();
        let matched = match self.columnar_txn_matched_by_pk(
            task,
            tid,
            txn_id,
            collection,
            &pk_col_name,
            &shipped_pks,
        ) {
            Ok(m) => m,
            Err(resp) => return resp,
        };

        // Drift check BEFORE staging anything: every shipped PK must resolve
        // to a surrogate in the current in-transaction view.
        let mut by_pk: HashMap<Vec<u8>, u32> = HashMap::with_capacity(matched.len());
        for (surrogate, row) in &matched {
            by_pk.insert(encode_pk(&row[pk_idx]), *surrogate);
        }
        let mut resolved: Vec<(u32, RowIdentity, &Vec<Value>)> = Vec::with_capacity(rows.len());
        for (pk, new_row) in rows {
            let identity = match resolved_pk_identity(collection, pk) {
                Ok(identity) => identity,
                Err(e) => return self.response_error(task, e),
            };
            match by_pk.get(&encode_pk(pk)) {
                Some(surrogate) => resolved.push((*surrogate, identity, new_row)),
                None => return self.response_error(task, ErrorCode::OllpRetryRequired),
            }
        }

        // The gate stays on every write path even though `DecidedEarlierInRequest`
        // makes this a no-op — mirrors `execute_columnar_resolved_update`.
        if let Err(response) = self.stage_admit_columnar_rows(
            task,
            rls_write_check,
            resolved.iter().map(|(_, _, row)| row.as_slice()),
            &schema,
            tid,
            collection,
        ) {
            return response;
        }

        let affected = resolved.len();
        for (surrogate, identity, new_row) in resolved {
            let body = match nodedb_types::value_to_msgpack(&Value::Array(new_row.clone())) {
                Ok(b) => b,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("columnar resolved update: row encode failed: {e}"),
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

    /// Stage a columnar `ResolvedDelete`: locate each shipped PK in the
    /// current BASE ∪ OVERLAY view and tombstone the surrogate it resolves
    /// to. Returns `{"affected": N}` in the same shape
    /// `execute_columnar_resolved_delete` returns for the autocommit path.
    ///
    /// Does not carry `rls_write_check`: a delete has no post-image to
    /// decide, and the Control Plane already decided the pre-image while
    /// resolving the row set — nothing here re-evaluates the policy.
    pub(in crate::data::executor) fn stage_columnar_resolved_delete(
        &mut self,
        params: StageColumnarResolvedDeleteParams<'_>,
    ) -> Response {
        let StageColumnarResolvedDeleteParams {
            task,
            tid,
            txn_id,
            collection,
            pks,
        } = params;

        if pks.is_empty() {
            return self.stage_columnar_dml_response(task, 0);
        }

        let coll_key = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );

        let schema = match self.columnar_engine_schema(task, tid, collection) {
            Ok(s) => s,
            Err(resp) => return resp,
        };
        let Some(pk_idx) = schema.columns.iter().position(|c| c.primary_key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar DELETE requires a PRIMARY KEY column".into(),
                },
            );
        };
        let pk_col_name = schema.columns[pk_idx].name.clone();

        let matched =
            match self.columnar_txn_matched_by_pk(task, tid, txn_id, collection, &pk_col_name, pks)
            {
                Ok(m) => m,
                Err(resp) => return resp,
            };

        let mut by_pk: HashMap<Vec<u8>, u32> = HashMap::with_capacity(matched.len());
        for (surrogate, row) in &matched {
            by_pk.insert(encode_pk(&row[pk_idx]), *surrogate);
        }

        // Drift check BEFORE tombstoning anything: every shipped PK must
        // resolve to a surrogate in the current in-transaction view.
        let mut surrogates: Vec<(u32, RowIdentity)> = Vec::with_capacity(pks.len());
        for pk in pks {
            let identity = match resolved_pk_identity(collection, pk) {
                Ok(identity) => identity,
                Err(e) => return self.response_error(task, e),
            };
            match by_pk.get(&encode_pk(pk)) {
                Some(surrogate) => surrogates.push((*surrogate, identity)),
                None => return self.response_error(task, ErrorCode::OllpRetryRequired),
            }
        }

        let affected = surrogates.len();
        for (surrogate, identity) in surrogates {
            self.txn_overlay_mut(txn_id)
                .insert_tombstone(coll_key.clone(), surrogate, &identity);
        }

        self.stage_columnar_dml_response(task, affected)
    }

    /// Resolve the CURRENT in-transaction view (BASE ∪ OVERLAY, via
    /// [`Self::columnar_txn_matching_rows`]) restricted to exactly the given
    /// PK set, via a synthetic `pk_col IN (...)` filter. Shared by the
    /// resolved-update and resolved-delete staging paths above.
    fn columnar_txn_matched_by_pk(
        &self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
        pk_col_name: &str,
        pks: &[Value],
    ) -> Result<Vec<(u32, Vec<Value>)>, Response> {
        let filter_bytes = match zerompk::to_msgpack_vec(&vec![ScanFilter {
            field: pk_col_name.to_string(),
            op: FilterOp::In,
            value: Value::Array(pks.to_vec()),
            clauses: Vec::new(),
            expr: None,
        }]) {
            Ok(b) => b,
            Err(e) => {
                return Err(self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar resolved DML: filter encode failed: {e}"),
                    },
                ));
            }
        };
        self.columnar_txn_matching_rows(task, tid, txn_id, collection, &filter_bytes)
    }
}
