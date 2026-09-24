// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for KV predicate `UPDATE ... WHERE <predicate>`
//! (`KvOp::PredicateUpdate`) and `DELETE ... WHERE <predicate>`
//! (`KvOp::PredicateDelete`) inside a transaction. The KV siblings of
//! `stage_bulk_update.rs` / `stage_bulk_delete.rs`.
//!
//! The row set is the live handler's BASE scan
//! ([`CoreLoop::kv_predicate_matches`]) folded with the transaction's overlay
//! via [`CoreLoop::merge_kv_overlay_into_scan`], so a same-transaction earlier
//! write to a row is observed: tombstoned and truncated rows drop out, staged
//! puts are re-checked against the predicate, and overlay-only rows that match
//! are appended. Each match is then staged exactly as its keyed sibling
//! stages it — a `Put` of the merged post-image for an update, a tombstone
//! for a delete — never written durably here. COMMIT's buffered plan replay
//! remains the sole durable apply.

use nodedb_types::RowIdentity;

use super::stage_kv::kv_row_identity;
use crate::bridge::envelope::Response;
use crate::bridge::scan_filter::{ScanFilter, decode_scan_filters};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::field_compute::merge_field_updates;
use crate::data::executor::response_codec;
use crate::data::executor::scan_normalize::kv_row_to_doc;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Routing identity + payload for one staged `KvOp::PredicateUpdate`.
pub(in crate::data::executor) struct StageKvPredicateUpdateParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    /// Serialized `Vec<ScanFilter>` (MessagePack). Empty matches every row.
    pub filter_bytes: &'a [u8],
    /// Field assignments: `(field_name, msgpack_value_bytes)`.
    pub updates: &'a [(String, Vec<u8>)],
    /// Compiled RLS write policy gating each matched row's staged post-image.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

/// Routing identity + payload for one staged `KvOp::PredicateDelete`.
pub(in crate::data::executor) struct StageKvPredicateDeleteParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    /// Serialized `Vec<ScanFilter>` (MessagePack). Empty matches every row.
    pub filter_bytes: &'a [u8],
    /// Compiled RLS write policy gating each matched row's removal, decided
    /// against its pre-deletion image.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

/// One matched row under BASE ∪ OVERLAY: its overlay identity, the
/// surrogate the staged entry binds to, and the current body.
struct KvStageMatch {
    doc_id: RowIdentity,
    surrogate: u32,
    body: Vec<u8>,
}

impl CoreLoop {
    /// Stage a KV predicate `UPDATE` at statement time: resolve the current
    /// BASE ∪ OVERLAY matching set, merge the SET-list into each match, and
    /// record the post-image as a staged `Put`. Replies `{"affected": N}`,
    /// the same shape `execute_kv_predicate_update` reports in autocommit.
    pub(in crate::data::executor) fn stage_kv_predicate_update(
        &mut self,
        params: StageKvPredicateUpdateParams<'_>,
    ) -> Response {
        let StageKvPredicateUpdateParams {
            task,
            tid,
            txn_id,
            collection,
            filter_bytes,
            updates,
            rls_write_check,
        } = params;
        let did = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (did, TenantId::new(tid), collection.to_string());

        let matched = match self.stage_kv_predicate_rows(task, txn_id, &coll_key, filter_bytes) {
            Ok(rows) => rows,
            Err(resp) => return resp,
        };

        let mut affected = 0usize;
        for row in matched {
            let computed = match merge_field_updates(collection, Some(row.body.as_slice()), updates)
            {
                Ok(c) => c,
                Err(e) => return self.response_error(task, e),
            };
            // A rejected row fails the statement rather than being skipped:
            // skipping would under-report `affected` while the rest of the
            // predicate's matches were still rewritten.
            if let Err(e) = self.stage_admit_write(
                rls_write_check,
                &computed.new_value,
                &row.doc_id,
                did.as_u64(),
                tid,
                collection,
            ) {
                return self.response_error(task, e);
            }
            if let Err(e) = self.stage_bulk_put_capped(
                txn_id,
                &coll_key,
                row.surrogate,
                &row.doc_id,
                computed.new_value,
            ) {
                return self.response_error(task, e);
            }
            affected += 1;
        }
        self.stage_count_response(task, affected)
    }

    /// Stage a KV predicate `DELETE` at statement time: resolve the current
    /// BASE ∪ OVERLAY matching set and tombstone each match. Replies
    /// `{"deleted": N}`, the same key the keyed `execute_kv_delete` the live
    /// predicate delete delegates to reports.
    pub(in crate::data::executor) fn stage_kv_predicate_delete(
        &mut self,
        params: StageKvPredicateDeleteParams<'_>,
    ) -> Response {
        let StageKvPredicateDeleteParams {
            task,
            tid,
            txn_id,
            collection,
            filter_bytes,
            rls_write_check,
        } = params;
        let did = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (did, TenantId::new(tid), collection.to_string());

        let matched = match self.stage_kv_predicate_rows(task, txn_id, &coll_key, filter_bytes) {
            Ok(rows) => rows,
            Err(resp) => return resp,
        };

        // Gate every matched row on the write policy BEFORE any tombstone is
        // staged, so a rejected row cannot leave the rows ahead of it already
        // hidden from the rest of the transaction. The current BASE ∪ OVERLAY
        // body is the pre-deletion image the policy decides.
        if !matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            for row in &matched {
                if let Err(e) = self.stage_admit_write(
                    rls_write_check,
                    &row.body,
                    &row.doc_id,
                    did.as_u64(),
                    tid,
                    collection,
                ) {
                    return self.response_error(task, e);
                }
            }
        }

        let deleted = matched.len();
        for row in &matched {
            self.txn_overlay_mut(txn_id).insert_tombstone(
                coll_key.clone(),
                row.surrogate,
                &row.doc_id,
            );
        }
        match response_codec::encode_count("deleted", deleted) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }

    /// The BASE ∪ OVERLAY row set a KV predicate DML stages against, each
    /// row bound to the surrogate its staged entry lands on. The base set is
    /// the live handler's own scan, so a staged statement and its COMMIT
    /// replay select rows by one rule.
    fn stage_kv_predicate_rows(
        &self,
        task: &ExecutionTask,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        filter_bytes: &[u8],
    ) -> Result<Vec<KvStageMatch>, Response> {
        let (did, tid) = (coll_key.0.as_u64(), coll_key.1.as_u64());
        let collection = coll_key.2.as_str();
        let filters: Vec<ScanFilter> =
            decode_scan_filters(filter_bytes, "kv predicate dml filters")
                .map_err(|e| self.response_error(task, e))?;
        let mut rows = self
            .kv_predicate_matches(did, tid, collection, filter_bytes, self.kv_read_now_ms())
            .map_err(|e| self.response_error(task, e))?;

        // `merge_kv_overlay_into_scan` takes an infallible predicate, so a
        // division/modulo-by-zero is captured via this `Cell` side-channel
        // and checked once the merge returns.
        let predicate_err: std::cell::Cell<Option<nodedb_query::EvalError>> =
            std::cell::Cell::new(None);
        let matches = |key: &[u8], value: &[u8]| {
            if filters.is_empty() {
                return true;
            }
            let (_key_str, row) = kv_row_to_doc(key, value);
            match ScanFilter::all_match_binary(&filters, &row) {
                Ok(b) => b,
                Err(e) => {
                    predicate_err.set(Some(e));
                    false
                }
            }
        };
        self.merge_kv_overlay_into_scan(txn_id, coll_key, &mut rows, &matches);
        if let Some(e) = predicate_err.take() {
            return Err(self.response_error(task, crate::Error::from(e)));
        }

        let mut matched = Vec::with_capacity(rows.len());
        for (key, body) in rows {
            // Every row the merge kept is present under BASE ∪ OVERLAY, so a
            // `None` here can only be a row that expired between the scan and
            // this lookup: it is no longer a match.
            let Some(surrogate) = self.resolve_kv_stage_surrogate(txn_id, coll_key, &key) else {
                continue;
            };
            matched.push(KvStageMatch {
                doc_id: kv_row_identity(&key),
                surrogate: surrogate.as_u32(),
                body,
            });
        }
        Ok(matched)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_types::Surrogate;

    use super::*;
    use crate::bridge::envelope::{
        Admission, ExemptReason, PhysicalPlan, Priority, Request, Status,
    };
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::transaction::overlay::Staged;
    use crate::engine::kv::KvPutParams;
    use crate::engine::kv::current_ms;
    use crate::types::*;

    fn make_task() -> ExecutionTask {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
            document_id: "y".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    fn body(n: i64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({ "n": n })).unwrap()
    }

    fn filters_n_gt(n: i64) -> Vec<u8> {
        let f = vec![ScanFilter {
            field: "n".into(),
            op: nodedb_query::scan_filter::FilterOp::Gt,
            value: nodedb_types::Value::Integer(n),
            clauses: Vec::new(),
            expr: None,
        }];
        zerompk::to_msgpack_vec(&f).unwrap()
    }

    fn put_base(core: &mut CoreLoop, key: &[u8], n: i64, surrogate: u32) {
        core.kv_engine.put(KvPutParams {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            collection: "c",
            key,
            value: &body(n),
            ttl_ms: 0,
            now_ms: current_ms(),
            surrogate: Surrogate::new(surrogate),
        });
    }

    fn coll_key() -> (DatabaseId, TenantId, String) {
        (DatabaseId::DEFAULT, TenantId::new(1), "c".to_string())
    }

    /// The count under `key` in a `{"<key>": n}` reply.
    fn count_field(resp: &Response, key: &str) -> u64 {
        let v: serde_json::Value =
            nodedb_types::json_from_msgpack(resp.payload.as_bytes()).unwrap();
        v[key].as_u64().unwrap()
    }

    #[test]
    fn predicate_delete_tombstones_base_matches_and_reports_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        put_base(&mut core, b"a", 1, 10);
        put_base(&mut core, b"b", 5, 11);
        let task = make_task();
        let txn_id = TxnId::new(1);

        let resp = core.stage_kv_predicate_delete(StageKvPredicateDeleteParams {
            task: &task,
            tid: 1,
            txn_id,
            collection: "c",
            filter_bytes: &filters_n_gt(2),
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(count_field(&resp, "deleted"), 1);
        let overlay = core.txn_overlays.get(&txn_id).unwrap();
        assert_eq!(
            overlay.get_by_doc_id(&coll_key(), &kv_row_identity(b"b")),
            Some(&Staged::Tombstone)
        );
        assert_eq!(overlay.get(&coll_key(), 11), Some(&Staged::Tombstone));
        assert_eq!(
            overlay.get_by_doc_id(&coll_key(), &kv_row_identity(b"a")),
            None
        );
    }

    #[test]
    fn predicate_update_sees_staged_put_and_hides_staged_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        put_base(&mut core, b"a", 5, 10);
        put_base(&mut core, b"b", 5, 11);
        let task = make_task();
        let txn_id = TxnId::new(1);
        // `a` is tombstoned this transaction; `z` is staged this transaction.
        core.txn_overlay_mut(txn_id)
            .insert_tombstone(coll_key(), 10, &kv_row_identity(b"a"));
        core.txn_overlay_mut(txn_id)
            .insert_put(coll_key(), 12, &kv_row_identity(b"z"), body(9));

        let updates = vec![(
            "n".to_string(),
            nodedb_types::json_to_msgpack(&serde_json::json!(0)).unwrap(),
        )];
        let resp = core.stage_kv_predicate_update(StageKvPredicateUpdateParams {
            task: &task,
            tid: 1,
            txn_id,
            collection: "c",
            filter_bytes: &filters_n_gt(2),
            updates: &updates,
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            count_field(&resp, "affected"),
            2,
            "base `b` and staged `z` match; tombstoned `a` does not"
        );
        let overlay = core.txn_overlays.get(&txn_id).unwrap();
        for (key, surrogate) in [(&b"b"[..], 11u32), (&b"z"[..], 12u32)] {
            let Some(Staged::Put(staged)) = overlay.get(&coll_key(), surrogate) else {
                panic!("{key:?} must be a staged put");
            };
            let v: serde_json::Value = nodedb_types::json_from_msgpack(staged).unwrap();
            assert_eq!(v["n"], serde_json::json!(0));
        }
        assert_eq!(overlay.get(&coll_key(), 10), Some(&Staged::Tombstone));
    }

    #[test]
    fn predicate_dml_matching_nothing_reports_zero_and_stages_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        put_base(&mut core, b"a", 1, 10);
        let task = make_task();
        let txn_id = TxnId::new(1);

        let resp = core.stage_kv_predicate_delete(StageKvPredicateDeleteParams {
            task: &task,
            tid: 1,
            txn_id,
            collection: "c",
            filter_bytes: &filters_n_gt(100),
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(count_field(&resp, "deleted"), 0);
        assert!(core.txn_overlays.get(&txn_id).is_none_or(|o| o.is_empty()));
    }
}
