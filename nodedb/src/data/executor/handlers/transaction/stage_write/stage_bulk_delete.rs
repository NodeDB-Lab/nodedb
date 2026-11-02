// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for predicate `DELETE ... WHERE <predicate>`
//! (`DocumentOp::BulkDelete`) inside a transaction. A `RETURNING` clause does
//! not change what is staged — the matched rows are tombstoned identically;
//! the reply projects their pre-deletion images per the clause.
//!
//! Mirrors point `DELETE` staging in `dispatch.rs` (`stage_point_delete`):
//! each row in the BASE ∪ OVERLAY matching set (resolved via
//! [`CoreLoop::txn_matching_rows`]) is recorded as a staged tombstone.
//! Nothing is written durably here — cascades (inverted index, secondary
//! indexes, graph edges) run only at COMMIT replay through the real apply
//! path, exactly as a staged point delete defers its cascade today.

use nodedb_types::StorageKey;

use super::body::stored_row_identity;
use super::stage_returning::{StageReturning, StagedRowsOf};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::bulk_dml::{TxnPrediction, decode_bulk_filters};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Routing identity + payload for one staged `BulkDelete`, bundled to keep
/// the entry point within the argument-count budget.
pub(in crate::data::executor) struct StageBulkDeleteParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub filter_bytes: &'a [u8],
    /// Compiled RLS write policy gating each matched row's removal, decided
    /// against its pre-deletion image.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Declared `PRIMARY KEY` column of the collection, `None` otherwise.
    /// Names the column each removed row's identity is read from.
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
    /// Stage a predicate `DELETE` at statement time: resolve the current
    /// BASE ∪ OVERLAY matching set and tombstone each match. Returns
    /// `{"affected": N}` in the same shape `execute_bulk_delete` returns for
    /// the autocommit path.
    pub(in crate::data::executor) fn stage_bulk_delete(
        &mut self,
        params: StageBulkDeleteParams<'_>,
    ) -> Response {
        let StageBulkDeleteParams {
            task,
            tid,
            txn_id,
            collection,
            filter_bytes,
            rls_write_check,
            declared_primary_key,
            returning,
            prediction,
            system_as_of,
        } = params;
        let database_id = task.request.database_id;
        let coll_key: (DatabaseId, TenantId, String) =
            (database_id, TenantId::new(tid), collection.to_string());

        // The BASE ∪ OVERLAY matching set, BASE read at the match's cut: the
        // block's snapshot, or each row's current version.
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

        // Gate every matched row on the collection's write policy BEFORE any
        // tombstone is staged, so a rejected row cannot leave the rows ahead of
        // it already hidden from the rest of the transaction. Each row's
        // current BASE ∪ OVERLAY body is the pre-deletion image the policy
        // decides — the only image a delete has.
        // Each row's identity is read from the pre-image the delete removes,
        // by the same rule INSERT minted it with.
        let strict_schema = self.resolve_strict_schema(database_id.as_u64(), tid, collection);
        let identified: Vec<(StorageKey, nodedb_types::RowIdentity, &Vec<u8>)> = rows
            .iter()
            .map(|(row_key, body)| {
                let identity = stored_row_identity(
                    body,
                    strict_schema.as_ref(),
                    declared_primary_key,
                    *row_key,
                );
                (*row_key, identity, body)
            })
            .collect();

        if !matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            for (_, identity, body) in &identified {
                if let Err(e) = self.stage_admit_write(
                    rls_write_check,
                    body,
                    identity,
                    database_id.as_u64(),
                    tid,
                    collection,
                ) {
                    return self.response_error(task, e);
                }
            }
        }

        let mut affected = 0u64;
        for (row_key, identity, _body) in &identified {
            let surrogate = row_key.surrogate().as_u32();
            self.txn_overlay_mut(txn_id)
                .insert_tombstone(coll_key.clone(), surrogate, identity);
            affected += 1;
        }

        if let Some(returning) = returning {
            let removed: Vec<(&nodedb_types::RowIdentity, &[u8])> = identified
                .iter()
                .map(|(_, identity, body)| (identity, body.as_slice()))
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
                &removed,
            );
        }

        match response_codec::encode_json_as_msgpack(&serde_json::json!({ "affected": affected })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate};

    use crate::bridge::envelope::{ErrorCode, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;
    use crate::data::executor::handlers::transaction::overlay::Staged;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{TenantId, TxnId};

    fn delete_all(predicted: Option<Vec<u32>>) -> PhysicalPlan {
        delete_all_at(predicted, nodedb_types::SystemTimeScope::Current)
    }

    fn delete_all_at(
        predicted: Option<Vec<u32>>,
        system_time: nodedb_types::SystemTimeScope,
    ) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
            filters: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: predicted,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
            system_time,
        })
    }

    fn seed(core: &mut CoreLoop, task: &ExecutionTask, surrogate: u32) {
        let ordinal = core.bitemporal_ordinal();
        seed_at(core, task, surrogate, ordinal);
    }

    fn seed_at(core: &mut CoreLoop, task: &ExecutionTask, surrogate: u32, ordinal: i64) {
        let body = doc_format::encode_to_msgpack(&serde_json::json!({"a": "1"}));
        core.sparse
            .put(
                task.request.database_id.as_u64(),
                task.request.tenant_id.as_u64(),
                "orders",
                &nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate)),
                &body,
                ordinal,
            )
            .expect("seed row");
    }

    /// Inside a block a predicate delete matches the rows visible at the
    /// block's snapshot plus the block's staged writes. A row committed after
    /// the snapshot is not matched, and the current-state match sees it.
    #[test]
    fn a_snapshot_delete_matches_the_snapshot_and_the_staged_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let txn_id = TxnId::new(8);
        let mut request = make_default_task().request;
        request.txn_id = Some(txn_id);
        let task = ExecutionTask::new(request);
        let tid = task.request.tenant_id.as_u64();
        seed_at(&mut core, &task, 1, 100);
        // A concurrent commit after the snapshot at 150.
        seed_at(&mut core, &task, 2, 200);
        let coll_key = (
            task.request.database_id,
            TenantId::new(tid),
            "orders".to_string(),
        );
        let staged = doc_format::encode_to_msgpack(&serde_json::json!({"a": "2"}));
        let identity = nodedb_types::StorageKey::for_surrogate(Surrogate::new(3)).to_identity();
        core.txn_overlay_mut(txn_id)
            .insert_put(coll_key.clone(), 3, &identity, staged);

        let snapshot = nodedb_types::SystemTimeScope::TxnSnapshot {
            ordinal: 150,
            cut_marker: 0,
        };
        let staged_delete = core.execute_stage_write(&task, tid, &delete_all_at(None, snapshot));
        assert_eq!(
            staged_delete.status,
            Status::Ok,
            "{:?}",
            staged_delete.error_code
        );
        let overlay = core.txn_overlays.get(&txn_id).expect("overlay");
        assert_eq!(overlay.get(&coll_key, 1), Some(&Staged::Tombstone));
        assert_eq!(overlay.get(&coll_key, 3), Some(&Staged::Tombstone));
        assert_eq!(
            overlay.get(&coll_key, 2),
            None,
            "a row committed after the snapshot is not matched"
        );

        // The prediction a block reads at its snapshot holds at the stage.
        let other = TxnId::new(9);
        let mut request = make_default_task().request;
        request.txn_id = Some(other);
        let other_task = ExecutionTask::new(request);
        let held =
            core.execute_stage_write(&other_task, tid, &delete_all_at(Some(vec![1]), snapshot));
        assert_eq!(held.status, Status::Ok, "{:?}", held.error_code);
        let mut request = make_default_task().request;
        request.txn_id = Some(TxnId::new(11));
        let fresh_task = ExecutionTask::new(request);
        let current = core.execute_stage_write(&fresh_task, tid, &delete_all(Some(vec![1])));
        assert_eq!(
            current.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired),
            "the current state holds the later commit too"
        );
    }

    /// A predicate write never matches committed history.
    #[test]
    fn a_historical_scope_refuses_the_stage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let mut request = make_default_task().request;
        request.txn_id = Some(TxnId::new(10));
        let task = ExecutionTask::new(request);
        let tid = task.request.tenant_id.as_u64();
        let pinned = nodedb_types::SystemTimeScope::Pinned {
            ordinal: 150,
            cut_marker: 0,
        };
        let refused = core.execute_stage_write(&task, tid, &delete_all_at(None, pinned));
        assert_eq!(refused.status, Status::Error);
    }

    /// A predicate delete whose carried prediction misses a matching row
    /// refuses with `OllpRetryRequired` and stages no tombstone.
    #[test]
    fn stale_prediction_refuses_the_stage_and_stages_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let txn_id = TxnId::new(7);
        let mut request = make_default_task().request;
        request.txn_id = Some(txn_id);
        let task = ExecutionTask::new(request);
        seed(&mut core, &task, 1);
        seed(&mut core, &task, 2);
        let tid = task.request.tenant_id.as_u64();

        let plan = delete_all(Some(vec![1]));
        let refused = core.execute_stage_write(&task, tid, &plan);
        assert_eq!(refused.status, Status::Error);
        assert_eq!(
            refused.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );
        let coll_key = (
            task.request.database_id,
            TenantId::new(tid),
            "orders".to_string(),
        );
        let staged = core
            .txn_overlays
            .get(&txn_id)
            .map(|overlay| overlay.iter_for_collection(&coll_key).count())
            .unwrap_or(0);
        assert_eq!(staged, 0, "a refused stage stages no tombstone");

        let plan = delete_all(Some(vec![1, 2]));
        let accepted = core.execute_stage_write(&task, tid, &plan);
        assert_eq!(accepted.status, Status::Ok, "{:?}", accepted.error_code);
    }
}
