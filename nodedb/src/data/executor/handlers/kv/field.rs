// SPDX-License-Identifier: BUSL-1.1

//! KV field-level operation handlers: FieldGet, FieldSet.

use nodedb_query::msgpack_scan::{KvBodyShape, kv_body_shape};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

/// Arguments for [`CoreLoop::execute_kv_field_get`].
///
/// Bundled so the handler keeps a readable signature while the read carries
/// its row-level-security filters alongside the field selection.
pub(in crate::data::executor) struct KvFieldGetArgs<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
    pub fields: &'a [String],
    pub rls_filters: &'a [u8],
}

/// Arguments for [`CoreLoop::execute_kv_field_set`] beyond the shared
/// single-key identity context.
pub(in crate::data::executor) struct KvFieldSetArgs<'a> {
    /// Field name → new value (msgpack-encoded bytes).
    pub updates: &'a [(String, Vec<u8>)],
    /// See `KvOp::FieldSet::if_present`.
    pub if_present: bool,
    /// When `Some`, project the STORED post-image (the merged row) per spec
    /// instead of reporting the field-count payload.
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_field_get(
        &self,
        task: &ExecutionTask,
        args: KvFieldGetArgs<'_>,
    ) -> Response {
        let KvFieldGetArgs {
            did,
            tid,
            collection,
            key,
            fields,
            rls_filters,
        } = args;
        debug!(core = self.core_id, %collection, field_count = fields.len(), "kv field get");
        let now_ms = current_ms();

        // Read-your-own-writes: consult this transaction's staging overlay
        // before falling back to base storage (see `execute_kv_batch_get`).
        let value = match self.kv_overlay_body(task, tid, collection, key) {
            Some(overlay_result) => overlay_result,
            None => self.kv_engine.get(did, tid, collection, key, now_ms),
        };
        let Some(value) = value else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // A row the read policy excludes is reported exactly as an absent row:
        // returning a distinguishable error would let a caller probe for keys
        // it may not read.
        match self.row_passes_rls(&value, rls_filters) {
            Ok(true) => {}
            Ok(false) => return self.response_error(task, ErrorCode::NotFound),
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        }

        // A raw body (the single-`value` SQL form, RESP `SET`) is not a
        // hash: the verdict Redis gives `HGET` on a string key.
        if kv_body_shape(&value) == KvBodyShape::Raw {
            return self.response_error(
                task,
                ErrorCode::TypeMismatch {
                    collection: collection.to_string(),
                    detail: "key holds a bare value, not a hash".into(),
                },
            );
        }
        // Decode as standard msgpack map.
        let doc = match nodedb_types::json_from_msgpack(&value) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "value is not a msgpack-encoded object".into(),
                    },
                );
            }
        };

        // Extract requested fields.
        let mut result = serde_json::Map::new();
        for f in fields {
            let v = doc.get(f).cloned().unwrap_or(serde_json::Value::Null);
            result.insert(f.clone(), v);
        }

        match response_codec::encode_json_as_msgpack(&serde_json::Value::Object(result)) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_kv_field_set(
        &mut self,
        ctx: super::atomic::KvAtomicCtx<'_>,
        args: KvFieldSetArgs<'_>,
    ) -> Response {
        let super::atomic::KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
        } = ctx;
        let KvFieldSetArgs {
            updates,
            if_present,
            returning,
            rls_filters,
        } = args;
        debug!(core = self.core_id, %collection, field_count = updates.len(), "kv field set");
        let now_ms = current_ms();

        // Read current value.
        let current = self.kv_engine.get(did, tid, collection, key, now_ms);

        // SQL UPDATE against an absent key is `UPDATE 0`, not a create: the
        // RESP hash-set family (`if_present: false`) is the only caller that
        // may create a row from nothing.
        if if_present && current.is_none() {
            if let Some(spec) = returning {
                return self.kv_stored_returning_response(task, spec, rls_filters, &[]);
            }
            return match response_codec::encode_json_as_msgpack(
                &serde_json::json!({ "affected": 0, "fields_added": 0 }),
            ) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
            };
        }

        // Merge field updates via the pure computation shared with the
        // in-transaction staging handler (`stage_kv_transfer.rs`), so a
        // staged value and its COMMIT-time durable replay never diverge.
        let computed = match super::field_compute::merge_field_updates(
            collection,
            current.as_deref(),
            updates,
        ) {
            Ok(c) => c,
            Err(e) => return self.response_error(task, e),
        };

        // The merged body is the row that will exist afterwards, and it exists
        // only now — decided before it is persisted, so a rejected merge leaves
        // the stored row untouched.
        if let Err(e) =
            super::rls::admit_kv_row(rls_write_check, &computed.new_value, key, tid, collection)
        {
            return self.response_error(task, e);
        }

        self.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: did,
            tenant_id: tid,
            collection,
            key,
            value: &computed.new_value,
            ttl_ms: 0,
            now_ms,
            surrogate,
        });
        self.note_kv_write_lsn(task, did, tid, collection, key);
        if let Some(spec) = returning {
            // `computed.new_value` IS the stored body: the merge is persisted
            // verbatim, so projecting it is projecting the post-image.
            return self.kv_stored_returning_response(
                task,
                spec,
                rls_filters,
                &[(key, computed.new_value.as_slice())],
            );
        }
        // `affected` is the row count the SQL `UPDATE` tag reads;
        // `fields_added` is what RESP `HSET` reports. The merge always
        // persists exactly one row.
        match response_codec::encode_json_as_msgpack(
            &serde_json::json!({ "affected": 1, "fields_added": computed.fields_added }),
        ) {
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
