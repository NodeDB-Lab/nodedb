// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for Document point writes — `PointInsert`,
//! `PointPut`, `PointDelete`, `PointUpdate` — issued inside a `BEGIN..COMMIT`
//! block.
//!
//! Each handler resolves the row against BASE ∪ OVERLAY, raises constraint
//! violations immediately (at the statement, not deferred to COMMIT), computes
//! the real affected-row count, and records the resulting encoded body (or a
//! tombstone) in the per-transaction overlay so a later same-transaction
//! read-modify-write observes it. The write is NOT made durable here — the
//! buffered plan is still replayed through the real apply path inside the
//! COMMIT `TransactionBatch`, which remains the sole durable apply.
//!
//! Split out of `dispatch.rs` (which owns the `MetaOp::StageWrite` routing and
//! the shared `stage_overlay_pk` / `stage_put_capped` / `stage_count_response`
//! helpers these methods call) to keep both files within the source-size
//! limit.

use nodedb_physical::physical_plan::UpdateValue;

use super::context::StageCtx;
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::generated;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::engine::document::store::surrogate_to_doc_id;
use crate::types::TenantId;

impl CoreLoop {
    pub(in crate::data::executor) fn stage_point_insert(
        &mut self,
        ctx: &StageCtx<'_>,
        value: &[u8],
        if_absent: bool,
    ) -> Response {
        let row_key = surrogate_to_doc_id(ctx.surrogate);
        let bitemporal = self.is_bitemporal(ctx.database_id, ctx.tid, ctx.collection);

        let overlay_pk = self.stage_overlay_pk(ctx);
        let present = match self.stage_pk_present(
            ctx.database_id,
            ctx.tid,
            ctx.collection,
            row_key.as_str(),
            bitemporal,
            overlay_pk,
        ) {
            Ok(p) => p,
            Err(e) => return self.response_error(ctx.task, e),
        };
        if present {
            if if_absent {
                return self.stage_count_response(ctx.task, 0);
            }
            return self.response_error(
                ctx.task,
                crate::Error::RejectedConstraint {
                    collection: ctx.collection.to_string(),
                    constraint: "unique".to_string(),
                    detail: format!(
                        "duplicate key value '{}' violates primary-key uniqueness on '{}'",
                        ctx.document_id, ctx.collection
                    ),
                },
            );
        }

        if let Err(e) = self.stage_check_unique(ctx, value) {
            return self.response_error(ctx.task, e);
        }
        self.stage_encode_and_commit(ctx, value)
    }

    pub(in crate::data::executor) fn stage_point_put(
        &mut self,
        ctx: &StageCtx<'_>,
        value: &[u8],
    ) -> Response {
        // Upsert semantics: no primary-key existence check (overwrite allowed);
        // UNIQUE indexes still apply against a DIFFERENT row.
        if let Err(e) = self.stage_check_unique(ctx, value) {
            return self.response_error(ctx.task, e);
        }
        self.stage_encode_and_commit(ctx, value)
    }

    pub(in crate::data::executor) fn stage_point_delete(&mut self, ctx: &StageCtx<'_>) -> Response {
        self.txn_overlay_mut(ctx.txn_id).insert_tombstone(
            ctx.coll_key.clone(),
            ctx.surrogate.0,
            &ctx.document_id,
        );
        self.stage_count_response(ctx.task, 1)
    }

    pub(in crate::data::executor) fn stage_point_update(
        &mut self,
        ctx: &StageCtx<'_>,
        updates: &[(String, UpdateValue)],
    ) -> Response {
        let config_key = (
            crate::types::DatabaseId::new(ctx.database_id),
            TenantId::new(ctx.tid),
            ctx.collection.to_string(),
        );
        let row_key = surrogate_to_doc_id(ctx.surrogate);

        // Reject direct updates to generated columns (matches the durable path).
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) =
                generated::check_generated_readonly(updates, &config.enforcement.generated_columns)
        {
            return self.response_error(ctx.task, e);
        }

        // Current body: overlay wins over base; an in-transaction tombstone
        // means the row is gone (0 rows updated).
        let overlay_cur = self
            .txn_overlays
            .get(&ctx.txn_id)
            .and_then(|o| o.get(&ctx.coll_key, ctx.surrogate.0))
            .cloned();
        let current_bytes = match overlay_cur {
            Some(Staged::Put(body)) => body,
            Some(Staged::Tombstone) => return self.stage_count_response(ctx.task, 0),
            None => {
                let bitemporal = self.is_bitemporal(ctx.database_id, ctx.tid, ctx.collection);
                let read = if bitemporal {
                    self.sparse.versioned_get_current(
                        ctx.database_id,
                        ctx.tid,
                        ctx.collection,
                        row_key.as_str(),
                    )
                } else {
                    self.sparse
                        .get(ctx.database_id, ctx.tid, ctx.collection, row_key.as_str())
                };
                match read {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => return self.stage_count_response(ctx.task, 0),
                    Err(e) => return self.response_error(ctx.task, e),
                }
            }
        };

        let body = match self.stage_apply_update(
            ctx.database_id,
            ctx.tid,
            ctx.collection,
            &current_bytes,
            updates,
        ) {
            Ok(b) => b,
            Err(e) => return self.response_error(ctx.task, e),
        };
        if let Err(e) = self.stage_put_capped(ctx, body) {
            return self.response_error(ctx.task, e);
        }
        self.stage_count_response(ctx.task, 1)
    }

    /// Run BASE ∪ OVERLAY UNIQUE checks for an incoming put/insert body.
    fn stage_check_unique(&self, ctx: &StageCtx<'_>, value: &[u8]) -> crate::Result<()> {
        let config_key = (
            crate::types::DatabaseId::new(ctx.database_id),
            TenantId::new(ctx.tid),
            ctx.collection.to_string(),
        );
        let Some(config) = self.doc_configs.get(&config_key).cloned() else {
            return Ok(());
        };
        if config.index_paths.iter().all(|p| !p.unique) {
            return Ok(());
        }
        let Some(incoming_doc) = doc_format::decode_document(value) else {
            return Ok(());
        };
        let staged_others: Vec<Vec<u8>> = self
            .txn_overlays
            .get(&ctx.txn_id)
            .map(|o| {
                o.iter_for_collection(&ctx.coll_key)
                    .filter_map(|(s, st)| match st {
                        Staged::Put(body) if s != ctx.surrogate.0 => Some(body.clone()),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default();
        self.stage_unique_check(ctx, &config, &incoming_doc, &staged_others)
    }

    fn stage_encode_and_commit(&mut self, ctx: &StageCtx<'_>, value: &[u8]) -> Response {
        let body = match self.stage_encode_put_body(
            ctx.database_id,
            ctx.tid,
            ctx.collection,
            ctx.surrogate,
            value,
        ) {
            Ok(b) => b,
            Err(e) => return self.response_error(ctx.task, e),
        };
        if let Err(e) = self.stage_put_capped(ctx, body) {
            return self.response_error(ctx.task, e);
        }
        self.stage_count_response(ctx.task, 1)
    }
}
