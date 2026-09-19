// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for CRDT row writes (`CrdtOp::DocUpsert`,
//! `CrdtOp::DocDelete`) issued inside a `BEGIN..COMMIT` block.
//!
//! A staged CRDT write lives only in the per-transaction overlay: the row
//! body a same-transaction read sees (point get, scan, indexed fetch), or a
//! tombstone. The collection's Loro state is untouched until COMMIT replays
//! the buffered plan through the live handler, which stays the sole durable
//! apply. CRDT constraint checks stay at COMMIT and replication.
//!
//! The staged body is built the way the live handler materializes one: the
//! row's fields normalized through Loro's value model and encoded as
//! MessagePack (`crdt_row_body`), then through the put-body encoder a
//! document point write uses. A partial write merges the incoming fields
//! over the row's current body under BASE ∪ OVERLAY. A partial write on a
//! missing row creates it from the incoming fields alone and counts one row,
//! as the live `set_fields` does.
//!
//! `returning` and `rls_filters` shape a `RETURNING` row set, which the
//! dispatch loop refuses inside a transaction; the count is the whole reply.
//! Write-policy admission for a CRDT collection is decided at planning.

use nodedb_physical::physical_plan::CrdtOp;
use nodedb_types::{RowIdentity, Surrogate};

use super::context::StageCtx;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::control::convert::{crdt_row_body, json_to_loro_value};
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;

type JsonMap = serde_json::Map<String, serde_json::Value>;

impl CoreLoop {
    /// Route a `MetaOp::StageWrite` wrapping a `CrdtOp` to its staging path.
    pub(in crate::data::executor) fn execute_stage_crdt(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &CrdtOp,
    ) -> Response {
        match op {
            CrdtOp::DocUpsert {
                collection,
                document_id,
                fields_json,
                surrogate,
                partial,
                // The verb decides the command tag on the Control Plane.
                verb: _,
                // A row-returning write inside a transaction is refused by the
                // dispatch loop before staging; the count is the whole reply.
                returning: _,
                rls_filters: _,
            } => {
                let ctx = StageCtx::new(
                    task,
                    tid,
                    txn_id,
                    collection.as_str(),
                    RowIdentity::from_user_key(document_id),
                    *surrogate,
                );
                self.stage_crdt_doc_upsert(&ctx, fields_json, *partial)
            }
            CrdtOp::DocDelete {
                collection,
                document_id,
                surrogate,
                returning: _,
                rls_filters: _,
            } => {
                let ctx = StageCtx::new(
                    task,
                    tid,
                    txn_id,
                    collection.as_str(),
                    RowIdentity::from_user_key(document_id),
                    *surrogate,
                );
                self.stage_crdt_doc_delete(&ctx)
            }
            CrdtOp::Read { .. }
            | CrdtOp::Apply { .. }
            | CrdtOp::ApplyAuthenticated { .. }
            | CrdtOp::ImportSnapshot { .. }
            | CrdtOp::SetConstraints { .. }
            | CrdtOp::DropConstraints { .. }
            | CrdtOp::ReadConstraints { .. }
            | CrdtOp::SetPolicy { .. }
            | CrdtOp::GetPolicy { .. }
            | CrdtOp::ReadAtVersion { .. }
            | CrdtOp::GetVersionVector { .. }
            | CrdtOp::ExportDelta { .. }
            | CrdtOp::RestoreToVersion { .. }
            | CrdtOp::CompactAtVersion { .. }
            | CrdtOp::ListInsert { .. }
            | CrdtOp::ListDelete { .. }
            | CrdtOp::ListMove { .. }
            | CrdtOp::PreviewApply { .. } => self.stage_not_point_write(task),
        }
    }

    /// Stage a full replace (`partial == false`) or a per-field merge
    /// (`partial == true`) of a CRDT row's scalar fields.
    fn stage_crdt_doc_upsert(
        &mut self,
        ctx: &StageCtx<'_>,
        fields_json: &str,
        partial: bool,
    ) -> Response {
        if ctx.surrogate == Surrogate::ZERO {
            return self.response_error(
                ctx.task,
                ErrorCode::Internal {
                    detail: format!(
                        "staged crdt write for '{}' carries no surrogate",
                        ctx.document_id
                    ),
                },
            );
        }
        let Ok(incoming) = sonic_rs::from_str::<JsonMap>(fields_json) else {
            return self.response_error(
                ctx.task,
                ErrorCode::Internal {
                    detail: format!(
                        "crdt doc upsert: invalid fields_json for {}",
                        ctx.document_id
                    ),
                },
            );
        };

        let row = if partial {
            match self.stage_crdt_current_fields(ctx) {
                Ok(Some(mut current)) => {
                    current.extend(incoming);
                    current
                }
                Ok(None) => incoming,
                Err(e) => return self.response_error(ctx.task, e),
            }
        } else {
            incoming
        };

        let body = match self.stage_crdt_encode_row(ctx, row) {
            Ok(b) => b,
            Err(e) => return self.response_error(ctx.task, e),
        };
        if let Err(e) = self.stage_put_capped(ctx, body) {
            return self.response_error(ctx.task, e);
        }
        self.stage_count_response(ctx.task, 1)
    }

    /// Stage a tombstone for a CRDT row. A row absent under BASE ∪ OVERLAY
    /// removes nothing, matching the live handler's count.
    fn stage_crdt_doc_delete(&mut self, ctx: &StageCtx<'_>) -> Response {
        match self.stage_current_body(ctx) {
            Ok(Some(_)) => {}
            Ok(None) => return self.stage_count_response(ctx.task, 0),
            Err(e) => return self.response_error(ctx.task, e),
        }
        self.txn_overlay_mut(ctx.txn_id).insert_tombstone(
            ctx.coll_key.clone(),
            ctx.surrogate.0,
            &ctx.document_id,
        );
        self.stage_count_response(ctx.task, 1)
    }

    /// The row's current fields under BASE ∪ OVERLAY, decoded through the
    /// collection's storage mode. `Ok(None)` when the row is absent.
    fn stage_crdt_current_fields(&self, ctx: &StageCtx<'_>) -> crate::Result<Option<JsonMap>> {
        let Some(bytes) = self.stage_current_body(ctx)? else {
            return Ok(None);
        };
        let schema = self.resolve_strict_schema(ctx.database_id, ctx.tid, ctx.collection);
        let doc = doc_format::decode_document_or_binary_tuple(
            &bytes,
            schema.as_ref(),
            "staged crdt row",
        )?;
        match doc {
            serde_json::Value::Object(fields) => Ok(Some(fields)),
            other => Err(crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "staged crdt row '{}' is not an object: {other}",
                    ctx.document_id
                ),
            }),
        }
    }

    /// Encode `row` into the stored form the live materialization writes:
    /// Loro-normalized MessagePack, then the document put-body pipeline.
    fn stage_crdt_encode_row(&self, ctx: &StageCtx<'_>, row: JsonMap) -> crate::Result<Vec<u8>> {
        let loro = json_to_loro_value(&serde_json::Value::Object(row));
        let raw = crdt_row_body(&loro).ok_or_else(|| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("staged crdt row '{}' does not encode", ctx.document_id),
        })?;
        self.stage_encode_put_body(
            ctx.database_id,
            ctx.tid,
            ctx.collection,
            ctx.surrogate,
            &raw,
        )
    }
}
