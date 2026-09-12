// SPDX-License-Identifier: BUSL-1.1

//! `StageWrite` dispatch: route a point-write plan to the matching staging
//! path, compute its real affected-row count, and record it in the overlay.

use nodedb_physical::physical_plan::{ColumnarOp, DocumentOp, GraphOp, SpatialOp, TimeseriesOp};
use nodedb_types::RowIdentity;

use super::constraint::OverlayPk;
use super::context::StageCtx;
use super::{
    StageBulkDeleteParams, StageBulkUpdateParams, StageColumnarDeleteParams,
    StageColumnarInsertParams, StageColumnarResolvedDeleteParams,
    StageColumnarResolvedUpdateParams, StageColumnarUpdateParams, StageSpatialInsertParams,
    StageTimeseriesInsertParams,
};
use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{MAX_TXN_OVERLAY_BYTES, Staged};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute a `MetaOp::StageWrite` for an in-transaction point write. Only
    /// point-write `DocumentOp`s are valid here; anything else is internal.
    pub(in crate::data::executor) fn execute_stage_write(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
    ) -> Response {
        let Some(txn_id) = task.request.txn_id else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "StageWrite dispatched without a txn_id".into(),
                },
            );
        };

        let doc_op = match plan {
            PhysicalPlan::Document(op) => op,
            PhysicalPlan::Kv(op) => return self.execute_stage_kv(task, tid, txn_id, op),
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection,
                payload,
                surrogates,
                schema_bytes,
                on_conflict_updates,
                rls_write_check,
                ..
            }) => {
                return self.stage_columnar_insert(StageColumnarInsertParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    payload,
                    surrogates,
                    schema_bytes,
                    on_conflict_updates,
                    rls_write_check,
                });
            }
            // Predicate DELETE/UPDATE staged at statement time: the affected
            // set resolves against base ∪ overlay; commit replay durably applies.
            PhysicalPlan::Columnar(ColumnarOp::Delete {
                collection,
                filters,
                rls_write_check,
            }) => {
                return self.stage_columnar_delete(StageColumnarDeleteParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    filter_bytes: filters,
                    rls_write_check,
                });
            }
            PhysicalPlan::Columnar(ColumnarOp::Update {
                collection,
                filters,
                updates,
                rls_write_check,
            }) => {
                return self.stage_columnar_update(StageColumnarUpdateParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    filter_bytes: filters,
                    updates,
                    rls_write_check,
                });
            }
            PhysicalPlan::Columnar(
                ColumnarOp::Scan { .. }
                | ColumnarOp::MaterializeScan { .. }
                | ColumnarOp::ResolveDml { .. },
            ) => return self.stage_not_point_write(task),
            // Resolved-row-set UPDATE/DELETE: staging locates each shipped PK
            // in base ∪ overlay rather than re-evaluating a predicate.
            PhysicalPlan::Columnar(ColumnarOp::ResolvedUpdate {
                collection,
                rows,
                rls_write_check,
            }) => {
                return self.stage_columnar_resolved_update(StageColumnarResolvedUpdateParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    rows,
                    rls_write_check,
                });
            }
            PhysicalPlan::Columnar(ColumnarOp::ResolvedDelete {
                collection,
                pks,
                rls_write_check: _,
            }) => {
                return self.stage_columnar_resolved_delete(StageColumnarResolvedDeleteParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    pks,
                });
            }
            PhysicalPlan::Spatial(SpatialOp::Insert {
                collection,
                field,
                surrogate,
                geometry,
                provenance: _,
            }) => {
                return self.stage_spatial_insert(StageSpatialInsertParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    field,
                    surrogate: *surrogate,
                    geometry,
                });
            }
            PhysicalPlan::Spatial(SpatialOp::Delete {
                collection,
                surrogate,
                field: _,
                provenance: _,
            }) => return self.stage_spatial_delete(task, tid, txn_id, collection.as_str(), *surrogate),
            PhysicalPlan::Spatial(SpatialOp::Scan { .. }) => {
                return self.stage_not_point_write(task);
            }
            PhysicalPlan::Graph(
                op @ (GraphOp::EdgePut { .. }
                | GraphOp::EdgeDelete { .. }
                | GraphOp::EdgePutBatch { .. }
                | GraphOp::EdgeDeleteBatch { .. }
                | GraphOp::SetNodeLabels { .. }
                | GraphOp::RemoveNodeLabels { .. }),
            ) => return self.execute_stage_graph(task, tid, txn_id, op),
            PhysicalPlan::Graph(
                GraphOp::Hop { .. }
                | GraphOp::Neighbors { .. }
                | GraphOp::NeighborsMulti { .. }
                | GraphOp::Path { .. }
                | GraphOp::Subgraph { .. }
                | GraphOp::RagFusion { .. }
                | GraphOp::Algo { .. }
                | GraphOp::Match { .. }
                | GraphOp::MatchContinuation { .. }
                | GraphOp::MatchVarLenResume { .. }
                | GraphOp::BspSuperstep(_)
                | GraphOp::WccSuperstep(_)
                | GraphOp::TemporalNeighbors { .. }
                | GraphOp::TemporalAlgorithm { .. }
                | GraphOp::Stats { .. }
                // Autocommit-then-Raft, never staged in a transaction.
                | GraphOp::ResolveEdgeDelete(_),
            ) => return self.stage_not_point_write(task),
            PhysicalPlan::Vector(_) => return self.stage_not_point_write(task),
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                surrogates,
                rls_write_check,
                ..
            }) => {
                return self.stage_timeseries_insert(StageTimeseriesInsertParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    payload,
                    format,
                    surrogates,
                    rls_write_check,
                });
            }
            PhysicalPlan::Timeseries(
                TimeseriesOp::Scan { .. } | TimeseriesOp::ResolveIngest(_),
            ) => {
                return self.stage_not_point_write(task);
            }
            PhysicalPlan::Text(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => return self.stage_not_point_write(task),
        };

        // The plan's `document_id` is the row's resolved client identity.
        match doc_op {
            DocumentOp::PointInsert {
                collection,
                document_id,
                value,
                if_absent,
                surrogate,
                ..
            } => {
                let ctx = StageCtx::new(
                    task,
                    tid,
                    txn_id,
                    collection.as_str(),
                    RowIdentity::from_user_key(document_id.as_str()),
                    *surrogate,
                );
                self.stage_point_insert(&ctx, value, *if_absent)
            }
            DocumentOp::PointPut {
                collection,
                document_id,
                value,
                surrogate,
                ..
            } => {
                let ctx = StageCtx::new(
                    task,
                    tid,
                    txn_id,
                    collection.as_str(),
                    RowIdentity::from_user_key(document_id.as_str()),
                    *surrogate,
                );
                self.stage_point_put(&ctx, value)
            }
            DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                rls_write_check,
                ..
            } => {
                let ctx = StageCtx::new(
                    task,
                    tid,
                    txn_id,
                    collection.as_str(),
                    RowIdentity::from_user_key(document_id.as_str()),
                    *surrogate,
                );
                self.stage_point_delete(&ctx, rls_write_check)
            }
            DocumentOp::PointUpdate {
                collection,
                document_id,
                surrogate,
                updates,
                rls_write_check,
                declared_primary_key,
                ..
            } => {
                let ctx = StageCtx::new(
                    task,
                    tid,
                    txn_id,
                    collection.as_str(),
                    RowIdentity::from_user_key(document_id.as_str()),
                    *surrogate,
                );
                self.stage_point_update(&ctx, updates, rls_write_check, declared_primary_key.as_deref())
            }
            // Predicate UPDATE staged like a point update, resolved against
            // base ∪ overlay. RETURNING doesn't change staging.
            DocumentOp::BulkUpdate {
                collection,
                filters,
                updates,
                returning: _,
                ollp_predicted_surrogates: _,
                ollp_predicted_edges: _,
                rls_filters: _,
                rls_write_check,
                // Staged post-images become concrete point ops at commit.
                resolved_sum_targets: _,
                declared_primary_key,
            } => self.stage_bulk_update(StageBulkUpdateParams {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                filter_bytes: filters,
                updates,
                rls_write_check,
                declared_primary_key: declared_primary_key.as_deref(),
            }),

            // Predicate DELETE staged like a point delete, resolved against
            // base ∪ overlay. Same RETURNING behavior as `BulkUpdate`.
            DocumentOp::BulkDelete {
                collection,
                filters,
                returning: _,
                ollp_predicted_surrogates: _,
                ollp_predicted_edges: _,
                rls_filters: _,
                rls_write_check,
                // See the `BulkUpdate` arm: staging carries no delta.
                resolved_sum_targets: _,
                declared_primary_key,
            } => self.stage_bulk_delete(StageBulkDeleteParams {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                filter_bytes: filters,
                rls_write_check,
                declared_primary_key: declared_primary_key.as_deref(),
            }),

            // `UPSERT INTO`: resolve the current body under base ∪ overlay,
            // mirroring `execute_upsert`. No `RETURNING` variant exists, so
            // it is always stageable.
            DocumentOp::Upsert {
                collection,
                document_id,
                value,
                on_conflict_updates,
                surrogate,
                rls_write_check,
                ..
            } => {
                let ctx = StageCtx::new(
                    task,
                    tid,
                    txn_id,
                    collection.as_str(),
                    RowIdentity::from_user_key(document_id.as_str()),
                    *surrogate,
                );
                self.stage_document_upsert(&ctx, value, on_conflict_updates, rls_write_check)
            }

            // `INSERT ... SELECT` is resolved into concrete `PointInsert` ops
            // at statement time; a raw `InsertSelect` never reaches here.
            DocumentOp::InsertSelect { .. }
            | DocumentOp::ResolveWrite(_)
            | DocumentOp::PointGet { .. }
            | DocumentOp::Scan { .. }
            | DocumentOp::BatchInsert { .. }
            | DocumentOp::RangeScan { .. }
            | DocumentOp::Register { .. }
            | DocumentOp::IndexLookup { .. }
            | DocumentOp::IndexedFetch { .. }
            | DocumentOp::DropIndex { .. }
            | DocumentOp::BackfillIndex { .. }
            | DocumentOp::Truncate { .. }
            | DocumentOp::EstimateCount { .. }
            | DocumentOp::UpdateFromJoin { .. }
            | DocumentOp::Merge { .. }
            | DocumentOp::MaterializeScan { .. }
            // Autocommit-then-Raft by construction: a governed write inside a
            // transaction is decided by the Data Plane gate, never resolved.
            | DocumentOp::ResolvedWrite { .. }
            | DocumentOp::ApplyBalanceDelta { .. } => self.stage_not_point_write(task),
        }
    }

    pub(super) fn stage_not_point_write(&self, task: &ExecutionTask) -> Response {
        self.response_error(
            task,
            ErrorCode::Internal {
                detail: "StageWrite is only valid for point-write document operations".into(),
            },
        )
    }

    // ── Shared helpers ──
    // The Document point-write staging methods live in `stage_point_document`
    // and call the `pub(super)` helpers below.

    pub(super) fn stage_overlay_pk(&self, ctx: &StageCtx<'_>) -> OverlayPk {
        match self
            .txn_overlays
            .get(&ctx.txn_id)
            .and_then(|o| o.get(&ctx.coll_key, ctx.surrogate.0))
        {
            Some(Staged::Put(_)) => OverlayPk::Present,
            Some(Staged::Tombstone) => OverlayPk::Absent,
            None => OverlayPk::Unstaged,
        }
    }

    /// Decide one staged row body against the compiled RLS write policy.
    /// Staging must decide here since commit install writes the overlay's
    /// bodies as-is. `body` is the stored form, so decode must resolve the
    /// collection's storage mode — reading a strict Binary Tuple as
    /// MessagePack would yield a columnless document.
    pub(in crate::data::executor) fn stage_admit_write(
        &self,
        rls_write_check: &nodedb_types::RlsWriteCheck,
        body: &[u8],
        identity: &crate::engine::document::store::RowIdentity,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> crate::Result<()> {
        if matches!(
            rls_write_check.decision(),
            nodedb_types::WriteGateDecision::AdmitAll
        ) {
            return Ok(());
        }
        let schema = self.resolve_strict_schema(database_id, tid, collection);
        crate::data::executor::handlers::rls_write_gate::admit_stored_row(
            rls_write_check,
            body,
            identity,
            schema.as_ref(),
            tid,
            collection,
        )
    }

    /// Stage a put after enforcing the per-transaction overlay memory cap.
    pub(super) fn stage_put_capped(
        &mut self,
        ctx: &StageCtx<'_>,
        body: Vec<u8>,
    ) -> crate::Result<()> {
        let current = self
            .txn_overlays
            .get(&ctx.txn_id)
            .map(|o| o.memory_size_estimate())
            .unwrap_or(0);
        if current.saturating_add(body.len()) > MAX_TXN_OVERLAY_BYTES {
            return Err(crate::Error::TxnOverlayMemoryExceeded {
                limit: MAX_TXN_OVERLAY_BYTES,
            });
        }
        self.txn_overlay_mut(ctx.txn_id).insert_put(
            ctx.coll_key.clone(),
            ctx.surrogate.0,
            &ctx.document_id,
            body,
        );
        Ok(())
    }

    pub(super) fn stage_count_response(&self, task: &ExecutionTask, affected: usize) -> Response {
        match response_codec::encode_count("affected", affected) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }
}
