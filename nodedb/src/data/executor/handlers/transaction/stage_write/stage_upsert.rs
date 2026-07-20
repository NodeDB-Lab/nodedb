// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for `DocumentOp::Upsert` (`UPSERT INTO`).
//!
//! Mirrors the autocommit handler (`handlers/upsert.rs::execute_upsert`)
//! exactly: absent -> insert `value`; present with an empty
//! `on_conflict_updates` -> merge incoming fields onto the existing
//! document (`merge_values`); present with `on_conflict_updates` -> apply
//! those assignments, with `EXCLUDED.col` support, against the existing row
//! (`apply_on_conflict_updates`). The only difference from autocommit is
//! *where* "current" resolves from -- BASE ∪ OVERLAY instead of BASE only --
//! and that the result lands in the per-transaction overlay instead of
//! durable storage.

use nodedb_physical::physical_plan::{StorageMode, UpdateValue};
use nodedb_types::columnar::StrictSchema;

use super::context::StageCtx;
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::handlers::upsert::{apply_on_conflict_updates, merge_values};
use crate::data::executor::response_codec;
use crate::data::executor::strict_format;
use crate::engine::document::store::surrogate_to_doc_id;
use crate::types::TenantId;

impl CoreLoop {
    /// Stage a `DocumentOp::Upsert` into the per-transaction overlay.
    pub(in crate::data::executor) fn stage_document_upsert(
        &mut self,
        ctx: &StageCtx<'_>,
        value: &[u8],
        on_conflict_updates: &[(String, UpdateValue)],
    ) -> Response {
        let existing_bytes = match self.resolve_doc_current(ctx) {
            Ok(b) => b,
            Err(e) => return self.response_error(ctx.task, e),
        };

        let (stored_bytes, op) = match existing_bytes {
            None => {
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
                (body, "insert")
            }
            Some(current_bytes) => {
                match self.stage_merge_upsert(ctx, &current_bytes, value, on_conflict_updates) {
                    Ok(b) => (b, "update"),
                    Err(e) => return self.response_error(ctx.task, e),
                }
            }
        };

        if let Err(e) = self.stage_put_capped(ctx, stored_bytes) {
            return self.response_error(ctx.task, e);
        }

        let payload = match response_codec::encode_json(&serde_json::json!({
            "affected": 1,
            "op": op,
        })) {
            Ok(p) => p,
            Err(e) => return self.response_error(ctx.task, e),
        };
        self.response_with_payload(ctx.task, payload)
    }

    /// Resolve the current stored body for `ctx` under BASE ∪ OVERLAY: a
    /// staged put wins, a staged tombstone means absent, otherwise fall back
    /// to durable storage (bitemporal-aware, mirroring `execute_upsert`).
    fn resolve_doc_current(&self, ctx: &StageCtx<'_>) -> crate::Result<Option<Vec<u8>>> {
        match self
            .txn_overlays
            .get(&ctx.txn_id)
            .and_then(|o| o.get(&ctx.coll_key, ctx.surrogate.0))
        {
            Some(Staged::Put(body)) => Ok(Some(body.clone())),
            Some(Staged::Tombstone) => Ok(None),
            None => {
                let bitemporal = self.is_bitemporal(ctx.database_id, ctx.tid, ctx.collection);
                let row_key = surrogate_to_doc_id(ctx.surrogate);
                if bitemporal {
                    self.sparse.versioned_get_current(
                        ctx.database_id,
                        ctx.tid,
                        ctx.collection,
                        row_key.as_str(),
                    )
                } else {
                    self.sparse
                        .get(ctx.database_id, ctx.tid, ctx.collection, row_key.as_str())
                }
            }
        }
    }

    /// Merge `value` (or apply `on_conflict_updates`) onto an existing body,
    /// re-encoding via the same strict/schemaless/bitemporal pipeline
    /// `execute_upsert` uses.
    fn stage_merge_upsert(
        &self,
        ctx: &StageCtx<'_>,
        current_bytes: &[u8],
        value: &[u8],
        on_conflict_updates: &[(String, UpdateValue)],
    ) -> crate::Result<Vec<u8>> {
        let config_key = (
            crate::types::DatabaseId::new(ctx.database_id),
            TenantId::new(ctx.tid),
            ctx.collection.to_string(),
        );
        let strict_schema: Option<StrictSchema> =
            self.doc_configs.get(&config_key).and_then(|config| {
                if let StorageMode::Strict { ref schema } = config.storage_mode {
                    Some(schema.clone())
                } else {
                    None
                }
            });

        let existing_val = self.decode_doc_value(current_bytes, strict_schema.as_ref())?;
        let new_val =
            nodedb_types::value_from_msgpack(value).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("staged upsert value: {e}"),
            })?;

        let merged = if on_conflict_updates.is_empty() {
            merge_values(existing_val, new_val)
        } else {
            apply_on_conflict_updates(existing_val, &new_val, on_conflict_updates)
        };

        let bitemporal = self.is_bitemporal(ctx.database_id, ctx.tid, ctx.collection);
        let sys_from_ms = if bitemporal {
            self.bitemporal_now_ms()
        } else {
            0
        };

        if let Some(ref schema) = strict_schema {
            let result = if bitemporal && schema.bitemporal {
                strict_format::value_to_binary_tuple_bitemporal(
                    &merged,
                    schema,
                    sys_from_ms,
                    i64::MIN,
                    i64::MAX,
                )
            } else {
                strict_format::value_to_binary_tuple(&merged, schema)
            };
            result.map_err(|e| crate::Error::Serialization {
                format: "binary_tuple".into(),
                detail: e.to_string(),
            })
        } else {
            nodedb_types::value_to_msgpack(&merged).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("staged upsert merge: {e}"),
            })
        }
    }

    /// Decode a stored body to `nodedb_types::Value`, strict (Binary Tuple,
    /// with a msgpack migration fallback) or schemaless (msgpack) --
    /// mirrors `execute_upsert`'s existing-document decode.
    fn decode_doc_value(
        &self,
        bytes: &[u8],
        strict_schema: Option<&StrictSchema>,
    ) -> crate::Result<nodedb_types::Value> {
        if let Some(schema) = strict_schema
            && let Some(v) = strict_format::binary_tuple_to_value(bytes, schema)
        {
            return Ok(v);
        }
        nodedb_types::value_from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("staged upsert existing document: {e}"),
        })
    }
}
