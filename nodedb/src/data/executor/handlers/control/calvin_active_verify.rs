// SPDX-License-Identifier: BUSL-1.1

//! Stage-time OLLP predicate verification for the dependent-read ACTIVE Calvin
//! path, which carries no versioned read-set. The leader-only `actual !=
//! predicted` re-check must run HERE, before staging — at flush time a
//! mismatch is swallowed as a degraded shard instead. Mirrors the
//! `BulkDelete`/`BulkUpdate` arms of [`CoreLoop::stage_calvin_overlay`].

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::bulk_dml::scan::{
    ollp_edges_match, ollp_predicted_doc_ids, ollp_surrogates_match,
};
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Leader-only OLLP verification of every predicate-DML plan in a
    /// dependent-read ACTIVE Calvin txn, before staging. `Ok(false)` means a
    /// prediction drifted; caller returns `OllpRetryRequired`, stages nothing.
    pub(in crate::data::executor) fn verify_calvin_active_ollp(
        &self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
    ) -> crate::Result<bool> {
        if !self.calvin.ollp_is_group_leader {
            return Ok(true);
        }
        let database_id = task.request.database_id.as_u64();
        for plan in plans {
            // Only the document engine carries an OLLP prediction; exhaustive
            // so a new `PhysicalPlan` variant forces a decision here.
            let document_op = match plan {
                PhysicalPlan::Document(op) => op,
                PhysicalPlan::Vector(_)
                | PhysicalPlan::Graph(_)
                | PhysicalPlan::Kv(_)
                | PhysicalPlan::Text(_)
                | PhysicalPlan::Columnar(_)
                | PhysicalPlan::Timeseries(_)
                | PhysicalPlan::Spatial(_)
                | PhysicalPlan::Crdt(_)
                | PhysicalPlan::Query(_)
                | PhysicalPlan::Meta(_)
                | PhysicalPlan::Array(_)
                | PhysicalPlan::ClusterArray(_)
                | PhysicalPlan::ClusterEvent(_) => continue,
            };
            let (collection, filter_bytes, predicted_surrogates, predicted_edges) =
                match document_op {
                    DocumentOp::BulkDelete {
                        collection,
                        filters,
                        ollp_predicted_surrogates,
                        ollp_predicted_edges,
                        ..
                    }
                    | DocumentOp::BulkUpdate {
                        collection,
                        filters,
                        ollp_predicted_surrogates,
                        ollp_predicted_edges,
                        ..
                    } => (
                        collection,
                        filters,
                        ollp_predicted_surrogates,
                        ollp_predicted_edges,
                    ),
                    // Mirrors `stage_calvin_overlay`'s non-bulk arms: no OLLP
                    // prediction to verify.
                    DocumentOp::PointGet { .. }
                    | DocumentOp::PointPut { .. }
                    | DocumentOp::PointInsert { .. }
                    | DocumentOp::PointDelete { .. }
                    | DocumentOp::PointUpdate { .. }
                    | DocumentOp::Upsert { .. }
                    | DocumentOp::BatchInsert { .. }
                    | DocumentOp::Scan { .. }
                    | DocumentOp::RangeScan { .. }
                    | DocumentOp::Register { .. }
                    | DocumentOp::IndexLookup { .. }
                    | DocumentOp::IndexedFetch { .. }
                    | DocumentOp::DropIndex { .. }
                    | DocumentOp::BackfillIndex { .. }
                    | DocumentOp::Truncate { .. }
                    | DocumentOp::EstimateCount { .. }
                    | DocumentOp::InsertSelect { .. }
                    | DocumentOp::UpdateFromJoin { .. }
                    | DocumentOp::Merge { .. }
                    | DocumentOp::ResolveWrite(_)
                    | DocumentOp::MaterializeScan { .. }
                    // Carries decided rows, not a predicate to re-verify: each
                    // mutation's own content precondition is the drift check.
                    | DocumentOp::ResolvedWrite { .. }
                    | DocumentOp::ApplyBalanceDelta { .. } => continue,
                };
            let Some(predicted) = predicted_surrogates.as_deref() else {
                // A bulk op with no prediction is the non-OLLP (static-set)
                // shape; `stage_calvin_overlay` rejects it loudly at staging.
                continue;
            };

            let filters: Vec<ScanFilter> = if filter_bytes.is_empty() {
                Vec::new()
            } else {
                zerompk::from_msgpack(filter_bytes).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".to_string(),
                    detail: format!("calvin active ollp verify: deserialize filters: {e}"),
                })?
            };
            let matching_ids =
                self.scan_matching_documents(database_id, tid, collection.as_str(), &filters)?;

            if !ollp_surrogates_match(&matching_ids, predicted) {
                return Ok(false);
            }
            if let Some(predicted_edges) = predicted_edges.as_deref() {
                let apply_ids = ollp_predicted_doc_ids(predicted);
                let actual =
                    self.ollp_actual_edges(database_id, tid, collection.as_str(), &apply_ids);
                if !ollp_edges_match(actual, predicted_edges) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}
