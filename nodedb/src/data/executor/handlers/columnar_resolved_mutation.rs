// SPDX-License-Identifier: BUSL-1.1

//! Columnar UPDATE/DELETE handlers for a Control-Plane-resolved row set
//! (`ColumnarOp::ResolvedUpdate` / `ColumnarOp::ResolvedDelete`).
//!
//! These exist for a collection carrying an RLS write policy. Unlike the
//! predicate forms in `columnar_mutation.rs`, the Control Plane already
//! resolved the predicate to concrete rows and decided the policy against
//! their exact images while the writing identity was live — the plan carries
//! the verdict (`rls_write_check: RlsWriteCheck::DecidedEarlierInRequest`),
//! not a predicate to re-evaluate. There is no filter scan here: the rows/PKs
//! shipped on the plan are the entire apply set.
//!
//! ## Drift check
//!
//! Between the Control Plane resolving the rows and this apply running, the
//! committed log may have advanced (a concurrent write on another connection,
//! replicated ahead of this one). Every replica must reach the SAME decision
//! on a shipped row that no longer exists, or replicas diverge. So every
//! shipped PK is verified present in the engine's PK index BEFORE the first
//! mutation; if any is missing, nothing is mutated and the caller gets
//! `ErrorCode::OllpRetryRequired` — the same retry contract
//! `admit_bulk_predicate_write` uses for OLLP surrogate-set drift (see
//! `handlers/bulk_dml/admission.rs`). This check runs unconditionally on every
//! replica (not leader-gated): each one applies the check against its own
//! committed log prefix, so leader and followers reach the same verdict.

use nodedb_columnar::pk_index::encode_pk;
use nodedb_types::Value;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Handle `ColumnarOp::ResolvedUpdate`: apply exactly the shipped
    /// `(pk, post-image)` rows, no filter scan, no assignment recomputation.
    ///
    /// Undo capture mirrors `execute_columnar_update` exactly: it is the same
    /// delete-old-PK + insert-new-row mutation, so the same pre-image capture
    /// (tombstoned original's location, replacement's PK, any displaced row)
    /// applies unchanged.
    pub(in crate::data::executor) fn execute_columnar_resolved_update(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        rows: &[(Value, Vec<Value>)],
        rls_write_check: &nodedb_types::RlsWriteCheck,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        debug!(core = self.core_id, %collection, "columnar resolved update");

        let key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let engine = match self.columnar_engines.get(&key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar engine not found for collection '{collection}'"),
                    },
                );
            }
        };

        let schema = engine.schema().clone();
        if !schema.columns.iter().any(|c| c.primary_key) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar UPDATE requires a PRIMARY KEY column".into(),
                },
            );
        }

        // Drift check BEFORE any mutation: every shipped PK must still exist
        // in the engine's PK index, or the whole statement is refused with
        // nothing mutated. See the module doc comment.
        for (pk, _new_row) in rows {
            let pk_bytes = encode_pk(pk);
            if engine.pk_index().get(&pk_bytes).is_none() {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
        }

        // The gate stays on every write path even though `DecidedEarlierInRequest`
        // makes this a no-op — a single path that skips it entirely is a hole
        // future callers can fall into.
        for (_pk, new_row) in rows {
            if let Err(error) = crate::data::executor::handlers::rls_write_gate::admit_columnar_row(
                rls_write_check,
                new_row,
                &schema,
                task.request.tenant_id.as_u64(),
                collection,
            ) {
                return self.response_error(task, error);
            }
        }

        let row_count_before = engine.memtable().row_count();
        let mut undo_log = undo_log;
        let outcome =
            self.apply_columnar_update_rows(task, &key, &schema, rows, undo_log.as_deref_mut());
        let affected = outcome.affected;

        if let Some(log) = undo_log {
            log.push(UndoEntry::ColumnarUpdate {
                collection_key: key,
                row_count_before,
                inserted_pks: outcome.inserted_pks,
                displaced: outcome.displaced,
                restored: outcome.restored,
            });
        }

        // Same floor-advance rationale as `execute_columnar_update`: an
        // UPDATE that mutated rows without raising the floor would sit above
        // a checkpoint stamp that already contains it, and replay would
        // duplicate the row.
        if affected > 0 {
            self.note_collection_write_lsn(task, collection);
        }

        debug!(core = self.core_id, %collection, affected, "columnar resolved update complete");
        let result = serde_json::json!({ "affected": affected });
        match super::super::response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Handle `ColumnarOp::ResolvedDelete`: remove exactly the shipped PKs,
    /// no filter scan.
    ///
    /// No per-row `admit_columnar_row` call here: a delete has no post-image
    /// to decide, and the Control Plane already decided the pre-image while
    /// resolving the row set (`rls_write_check` carries the verdict, not a
    /// predicate). Undo capture mirrors `execute_columnar_delete` exactly.
    pub(in crate::data::executor) fn execute_columnar_resolved_delete(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        pks: &[Value],
        _rls_write_check: &nodedb_types::RlsWriteCheck,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        debug!(core = self.core_id, %collection, "columnar resolved delete");

        let key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let engine = match self.columnar_engines.get(&key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar engine not found for collection '{collection}'"),
                    },
                );
            }
        };

        let schema = engine.schema().clone();
        if !schema.columns.iter().any(|c| c.primary_key) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar DELETE requires a PRIMARY KEY column".into(),
                },
            );
        }

        // Drift check BEFORE any mutation: every shipped PK must still exist.
        for pk in pks {
            let pk_bytes = encode_pk(pk);
            if engine.pk_index().get(&pk_bytes).is_none() {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
        }

        let mut undo_log = undo_log;
        let outcome = self.apply_columnar_delete_pks(&key, &schema, pks, undo_log.as_deref_mut());
        let affected = outcome.affected;

        if let Some(log) = undo_log {
            log.push(UndoEntry::ColumnarDelete {
                collection_key: key,
                restored: outcome.restored,
            });
        }

        if affected > 0 {
            self.note_collection_write_lsn(task, collection);
        }

        debug!(core = self.core_id, %collection, affected, "columnar resolved delete complete");
        let result = serde_json::json!({ "affected": affected });
        match super::super::response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_types::RlsWriteCheck;
    use std::sync::Arc;

    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    const TID: u64 = 1;
    const COLLECTION: &str = "resolved_m";

    fn task_for(_core: &CoreHarness) -> ExecutionTask {
        CoreLoop::replay_task(
            TenantId::new(TID),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            crate::bridge::envelope::PhysicalPlan::Columnar(
                nodedb_physical::physical_plan::ColumnarOp::Scan {
                    collection: nodedb_types::QualifiedCollection::new(
                        DatabaseId::DEFAULT,
                        COLLECTION,
                    ),
                    projection: Vec::new(),
                    limit: 0,
                    filters: Vec::new(),
                    rls_filters: Vec::new(),
                    sort_keys: Vec::new(),
                    system_time: nodedb_types::temporal::SystemTimeScope::Current,
                    valid_at_ms: None,
                    prefilter: None,
                    computed_columns: Vec::new(),
                },
            ),
            None,
        )
    }

    fn row(id: i64, v: i64) -> Value {
        Value::Object(std::collections::HashMap::from([
            ("id".to_string(), Value::Integer(id)),
            ("v".to_string(), Value::Integer(v)),
        ]))
    }

    fn insert_rows(core: &mut CoreLoop, task: &ExecutionTask, rows: Vec<Value>) {
        let payload =
            nodedb_types::value_to_msgpack(&Value::Array(rows)).expect("encode insert payload");
        let resp = core.execute_columnar_insert(
            task,
            crate::data::executor::handlers::columnar_write::ColumnarInsertParams {
                collection: COLLECTION,
                payload: &payload,
                format: "msgpack",
                intent: nodedb_physical::physical_plan::ColumnarInsertIntent::Insert,
                on_conflict_updates: &[],
                surrogates: &[],
                schema_bytes: &[],
                provenance: None,
                rls_write_check: &RlsWriteCheck::already_decided_elsewhere(),
                returning: None,
                rls_filters: &[],
                spatial_undo: None,
            },
        );
        assert_eq!(
            resp.status,
            crate::bridge::envelope::Status::Ok,
            "seed insert failed: {:?}",
            resp.error_code
        );
    }

    fn scan_ids(core: &mut CoreLoop) -> Vec<(i64, i64)> {
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            COLLECTION.to_string(),
        );
        let engine = core
            .columnar_engines
            .get(&key)
            .expect("columnar engine present");
        let schema = engine.schema();
        let id_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "id")
            .expect("id column");
        let v_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "v")
            .expect("v column");
        engine
            .scan_memtable_rows()
            .map(|r| {
                let id = match &r[id_idx] {
                    Value::Integer(n) => *n,
                    other => panic!("expected integer id, got {other:?}"),
                };
                let v = match &r[v_idx] {
                    Value::Integer(n) => *n,
                    other => panic!("expected integer v, got {other:?}"),
                };
                (id, v)
            })
            .collect()
    }

    /// Build a schema-ordered post-image row `(id, v)` for a resolved-update
    /// test. Column order is inferred from `HashMap` iteration when the
    /// engine's schema is created from the first inserted row (see
    /// `scan_ids`'s doc comment), so it cannot be assumed positionally —
    /// this always looks it up.
    fn resolved_row(core: &CoreLoop, id_v: i64, v_v: i64) -> Vec<Value> {
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            COLLECTION.to_string(),
        );
        let schema = core
            .columnar_engines
            .get(&key)
            .expect("columnar engine present")
            .schema();
        let id_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "id")
            .expect("id column");
        let v_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "v")
            .expect("v column");
        let mut out = vec![Value::Null; schema.columns.len()];
        out[id_idx] = Value::Integer(id_v);
        out[v_idx] = Value::Integer(v_v);
        out
    }

    #[test]
    fn resolved_update_applies_exactly_the_shipped_post_images() {
        let mut h = make_core();
        let task = task_for(&h);
        insert_rows(&mut h.core, &task, vec![row(1, 10), row(2, 20), row(3, 30)]);

        let rows = vec![
            (Value::Integer(1), resolved_row(&h.core, 1, 999)),
            (Value::Integer(3), resolved_row(&h.core, 3, 888)),
        ];
        let resp = h.core.execute_columnar_resolved_update(
            &task,
            COLLECTION,
            &rows,
            &RlsWriteCheck::decided_earlier_in_request(),
            None,
        );
        assert_eq!(resp.status, crate::bridge::envelope::Status::Ok);

        let mut ids = scan_ids(&mut h.core);
        ids.sort();
        assert_eq!(ids, vec![(1, 999), (2, 20), (3, 888)]);
    }

    #[test]
    fn resolved_delete_removes_exactly_the_shipped_pks() {
        let mut h = make_core();
        let task = task_for(&h);
        insert_rows(&mut h.core, &task, vec![row(1, 10), row(2, 20), row(3, 30)]);

        let pks = vec![Value::Integer(2)];
        let resp = h.core.execute_columnar_resolved_delete(
            &task,
            COLLECTION,
            &pks,
            &RlsWriteCheck::decided_earlier_in_request(),
            None,
        );
        assert_eq!(resp.status, crate::bridge::envelope::Status::Ok);

        let mut ids = scan_ids(&mut h.core);
        ids.sort();
        assert_eq!(ids, vec![(1, 10), (3, 30)]);
    }

    #[test]
    fn missing_shipped_pk_retries_and_mutates_nothing() {
        let mut h = make_core();
        let task = task_for(&h);
        insert_rows(&mut h.core, &task, vec![row(1, 10), row(2, 20)]);

        // PK 99 does not exist: the whole statement must be refused, and the
        // valid row (PK 1) must NOT be updated either.
        let rows = vec![
            (Value::Integer(1), resolved_row(&h.core, 1, 999)),
            (Value::Integer(99), resolved_row(&h.core, 99, 1)),
        ];
        let resp = h.core.execute_columnar_resolved_update(
            &task,
            COLLECTION,
            &rows,
            &RlsWriteCheck::decided_earlier_in_request(),
            None,
        );
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired),
            "missing shipped PK must yield OllpRetryRequired"
        );

        let mut ids = scan_ids(&mut h.core);
        ids.sort();
        assert_eq!(
            ids,
            vec![(1, 10), (2, 20)],
            "collection must be unchanged after a drift-check refusal"
        );
    }

    #[test]
    fn missing_shipped_pk_on_delete_retries_and_mutates_nothing() {
        let mut h = make_core();
        let task = task_for(&h);
        insert_rows(&mut h.core, &task, vec![row(1, 10), row(2, 20)]);

        let pks = vec![Value::Integer(1), Value::Integer(99)];
        let resp = h.core.execute_columnar_resolved_delete(
            &task,
            COLLECTION,
            &pks,
            &RlsWriteCheck::decided_earlier_in_request(),
            None,
        );
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );

        let mut ids = scan_ids(&mut h.core);
        ids.sort();
        assert_eq!(ids, vec![(1, 10), (2, 20)]);
    }

    #[test]
    fn resolved_update_still_consults_the_write_gate() {
        let mut h = make_core();
        let task = task_for(&h);
        insert_rows(&mut h.core, &task, vec![row(1, 10)]);

        let owner_policy = {
            let filter = crate::bridge::scan_filter::ScanFilter {
                field: "v".to_string(),
                op: nodedb_query::scan_filter::FilterOp::Eq,
                value: Value::Integer(1),
                clauses: Vec::new(),
                expr: None,
            };
            zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter")
        };

        let rows = vec![(Value::Integer(1), resolved_row(&h.core, 1, 999))];
        let resp = h.core.execute_columnar_resolved_update(
            &task,
            COLLECTION,
            &rows,
            &RlsWriteCheck::Predicate(owner_policy),
            None,
        );
        assert_eq!(
            resp.status,
            crate::bridge::envelope::Status::Error,
            "a post-image that violates the shipped predicate must be rejected, \
             proving the gate is consulted rather than bypassed"
        );

        let mut ids = scan_ids(&mut h.core);
        ids.sort();
        assert_eq!(
            ids,
            vec![(1, 10)],
            "rejected update must not mutate the row"
        );
    }
}
