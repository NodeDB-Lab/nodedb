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
use crate::data::executor::enforcement::typeguard::GuardScope;
use crate::data::executor::handlers::upsert::{apply_on_conflict_updates, merge_values};
use crate::data::executor::handlers::write_body;
use crate::data::executor::strict_format;
use crate::types::TenantId;

impl CoreLoop {
    /// Stage a `DocumentOp::Upsert` into the per-transaction overlay.
    pub(in crate::data::executor) fn stage_document_upsert(
        &mut self,
        ctx: &StageCtx<'_>,
        value: &[u8],
        on_conflict_updates: &[(String, UpdateValue)],
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> Response {
        if let Some(refusal) = crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
            "document",
            ctx.collection,
            ctx.surrogate,
        ) {
            return self.response_error(ctx.task, refusal);
        }
        let existing_bytes = match self.stage_current_body(ctx) {
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

        // Gate whichever body this branch actually resolved — the incoming row
        // when absent, the merge with the stored row when present. The merged
        // image exists only here, which is why the plan cannot admit it.
        if let Err(e) = self.stage_admit_write(
            rls_write_check,
            &stored_bytes,
            &ctx.document_id,
            ctx.database_id,
            ctx.tid,
            ctx.collection,
        ) {
            return self.response_error(ctx.task, e);
        }

        if let Err(e) = self.stage_put_capped(ctx, stored_bytes) {
            return self.response_error(ctx.task, e);
        }

        self.response_affected_with_op(ctx.task, 1, op)
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

        // The fields this merge writes: the `ON CONFLICT DO UPDATE SET`
        // targets, else every field of the incoming body.
        let assigned: Vec<String> = if on_conflict_updates.is_empty() {
            match &new_val {
                nodedb_types::Value::Object(fields) => fields.keys().cloned().collect(),
                _ => Vec::new(),
            }
        } else {
            write_body::assigned_names(on_conflict_updates)
        };

        let merged = if on_conflict_updates.is_empty() {
            merge_values(existing_val, new_val)
        } else {
            apply_on_conflict_updates(
                existing_val,
                &new_val,
                on_conflict_updates,
                &self.crs_context(),
            )?
        };

        // Apply typeguards and generated columns to the merged row, as the
        // durable merge does, judging the assigned and injected fields.
        let merged = match self.doc_configs.get(&config_key) {
            Some(config) if write_body::rewrites_body(config) => {
                let mut doc = serde_json::Value::from(merged);
                write_body::materialize_write_body(
                    config,
                    &mut doc,
                    GuardScope::Assigned(&assigned),
                    &self.crs_context(),
                )
                .map_err(crate::Error::DataPlane)?;
                nodedb_types::Value::from(doc)
            }
            _ => merged,
        };

        let bitemporal = self.is_bitemporal(ctx.database_id, ctx.tid, ctx.collection);
        let sys_from = if bitemporal {
            self.bitemporal_ordinal()
        } else {
            0
        };

        if let Some(ref schema) = strict_schema {
            if bitemporal && schema.bitemporal {
                strict_format::value_to_binary_tuple_bitemporal(
                    &merged,
                    schema,
                    sys_from,
                    i64::MIN,
                    i64::MAX,
                    ctx.collection,
                )
            } else {
                strict_format::value_to_binary_tuple(&merged, schema, ctx.collection)
            }
        } else {
            let body = nodedb_types::value_to_msgpack(&merged).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("staged upsert merge: {e}"),
                }
            })?;
            // A declared numeric column holds the value its type stores, as
            // on the durable path.
            strict_format::coerce_declared_body(body, self.declared_columns(&config_key))
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
