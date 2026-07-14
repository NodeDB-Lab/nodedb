// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for `SpatialOp::Insert` / `SpatialOp::Delete`.
//!
//! These are the sync-path spatial variants (`nodedb-physical`'s
//! `SpatialOp::Insert { surrogate, geometry, .. }` /
//! `SpatialOp::Delete { surrogate, .. }`), used to replicate a Lite client's
//! spatial write to Origin. Staging them here inside a `BEGIN..COMMIT` block
//! gives them the same read-your-own-writes parity as every other
//! stageable write: a later same-transaction spatial `SELECT ... WHERE
//! ST_*(...)` observes them via
//! `overlay::merge_overlay_into_spatial_scan` before COMMIT.
//!
//! This is NOT the path a mainstream SQL `INSERT INTO <spatial_collection>
//! VALUES(...)` takes -- that routes to `ColumnarOp::Insert` and is staged
//! by `stage_columnar_insert` instead (see that module's doc comment).
//!
//! Row body encoding mirrors the durable sync-apply path exactly: the same
//! `{field: geometry, "id": surrogate_hex}` document shape
//! `execute_spatial_insert` (`handlers/spatial_sync.rs`) writes to the
//! sparse store, built via the same `geometry_to_value` helper and encoded
//! with `nodedb_types::value_to_msgpack` -- decoded the same way by
//! `merge_overlay_into_spatial_scan` (the `Value::Object` staged-body
//! branch). COMMIT durable replay is unchanged: the buffered `SpatialOp`
//! plan is still replayed through `execute_spatial_insert` /
//! `execute_spatial_delete` inside the COMMIT `TransactionBatch`.

use nodedb_types::Surrogate;
use nodedb_types::geometry::Geometry;

use super::context::StageCtx;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::spatial_sync::geometry_to_value;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use crate::types::TxnId;

/// Inputs for [`CoreLoop::stage_spatial_insert`].
pub(in crate::data::executor) struct StageSpatialInsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub field: &'a str,
    pub surrogate: Surrogate,
    pub geometry: &'a Geometry,
}

impl CoreLoop {
    /// Stage a `SpatialOp::Insert`: encode the geometry as the sync-path's
    /// `{field: geometry, "id": hex}` document body and stage one overlay
    /// `Put` keyed by the surrogate. Returns the shared `stage_count_response`
    /// shape (`{"affected": 1}`).
    pub(in crate::data::executor) fn stage_spatial_insert(
        &mut self,
        params: StageSpatialInsertParams<'_>,
    ) -> Response {
        let StageSpatialInsertParams {
            task,
            tid,
            txn_id,
            collection,
            field,
            surrogate,
            geometry,
        } = params;

        let doc_id = surrogate_to_doc_id(surrogate);

        let mut doc_map = std::collections::HashMap::new();
        doc_map.insert(field.to_string(), geometry_to_value(geometry));
        doc_map.insert(
            "id".to_string(),
            nodedb_types::Value::String(doc_id.clone()),
        );
        let doc_value = nodedb_types::Value::Object(doc_map);

        let body = match nodedb_types::value_to_msgpack(&doc_value) {
            Ok(b) => b,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("spatial insert: serialise geometry document: {e}"),
                    },
                );
            }
        };

        let ctx = StageCtx::new(task, tid, txn_id, collection, doc_id, surrogate);
        if let Err(e) = self.stage_put_capped(&ctx, body) {
            return self.response_error(task, e);
        }
        self.stage_count_response(task, 1)
    }

    /// Stage a `SpatialOp::Delete`: record a tombstone keyed by the
    /// surrogate. Returns `{"affected": 1}`.
    pub(in crate::data::executor) fn stage_spatial_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
        surrogate: Surrogate,
    ) -> Response {
        let doc_id = surrogate_to_doc_id(surrogate);
        let ctx = StageCtx::new(task, tid, txn_id, collection, doc_id, surrogate);
        self.txn_overlay_mut(ctx.txn_id).insert_tombstone(
            ctx.coll_key.clone(),
            ctx.surrogate.0,
            &ctx.document_id,
        );
        self.stage_count_response(task, 1)
    }
}
