//! Atomic transfer handlers: Transfer (fungible) and TransferItem (non-fungible).
//!
//! These execute entirely within a single TPC core pass — no TOCTOU race.
//! Read + validate + write happens atomically because the TPC core is
//! single-threaded and owns all keys in its hash table.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

/// Parameters for an atomic fungible transfer.
pub(in crate::data::executor) struct TransferParams<'a> {
    pub tid: u32,
    pub collection: &'a str,
    pub source_key: &'a [u8],
    pub dest_key: &'a [u8],
    pub field: &'a str,
    pub amount: f64,
}

impl CoreLoop {
    /// Atomic fungible transfer: source.field -= amount, dest.field += amount.
    ///
    /// Entire read-validate-write is one Data Plane pass. No TOCTOU.
    pub(in crate::data::executor) fn execute_kv_transfer(
        &mut self,
        task: &ExecutionTask,
        params: TransferParams<'_>,
    ) -> Response {
        let TransferParams {
            tid,
            collection,
            source_key,
            dest_key,
            field,
            amount,
        } = params;
        debug!(core = self.core_id, %collection, %field, amount, "kv transfer");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();

        // Step 1: Read both values atomically (same core, no interleaving).
        let source_val = self.kv_engine.get(tid, collection, source_key, now_ms);
        let dest_val = self.kv_engine.get(tid, collection, dest_key, now_ms);

        let Some(source_bytes) = source_val else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // Step 2: Extract and validate source balance.
        let source_balance = match extract_numeric_field(&source_bytes, field) {
            Some(v) => v,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::TypeMismatch {
                        collection: collection.to_string(),
                        detail: format!("field '{field}' is not numeric or missing"),
                    },
                );
            }
        };

        if source_balance < amount {
            return self.response_error(
                task,
                ErrorCode::InsufficientBalance {
                    collection: collection.to_string(),
                    detail: format!("source has {source_balance}, need {amount}"),
                },
            );
        }

        let dest_bytes = dest_val.unwrap_or_default();
        let dest_balance = extract_numeric_field(&dest_bytes, field).unwrap_or(0.0);

        // Step 3: Compute new values.
        let new_source = match update_numeric_field(&source_bytes, field, source_balance - amount) {
            Ok(v) => v,
            Err(e) => return self.response_error(task, e),
        };
        let new_dest = if dest_bytes.is_empty() {
            // Dest key doesn't exist — create with just the field.
            let doc = serde_json::json!({ field: dest_balance + amount });
            match nodedb_types::json_to_msgpack(&doc) {
                Ok(v) => v,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("serialize destination: {e}"),
                        },
                    );
                }
            }
        } else {
            match update_numeric_field(&dest_bytes, field, dest_balance + amount) {
                Ok(v) => v,
                Err(e) => return self.response_error(task, e),
            }
        };

        // Step 4: Write both atomically (deterministic order for consistency).
        // Write lower key first to match the documented lock ordering.
        if source_key <= dest_key {
            self.kv_engine.put(
                tid,
                collection,
                source_key,
                &new_source,
                0,
                now_ms,
                nodedb_types::Surrogate::ZERO,
            );
            self.kv_engine.put(
                tid,
                collection,
                dest_key,
                &new_dest,
                0,
                now_ms,
                nodedb_types::Surrogate::ZERO,
            );
        } else {
            self.kv_engine.put(
                tid,
                collection,
                dest_key,
                &new_dest,
                0,
                now_ms,
                nodedb_types::Surrogate::ZERO,
            );
            self.kv_engine.put(
                tid,
                collection,
                source_key,
                &new_source,
                0,
                now_ms,
                nodedb_types::Surrogate::ZERO,
            );
        }

        if let Some(ref m) = self.metrics {
            m.record_kv_put();
            m.record_kv_put();
        }

        // Emit CDC events.
        let src_str = String::from_utf8_lossy(source_key);
        let dst_str = String::from_utf8_lossy(dest_key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Update,
            &src_str,
            Some(&new_source),
            Some(&source_bytes),
        );
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Update,
            &dst_str,
            Some(&new_dest),
            if dest_bytes.is_empty() {
                None
            } else {
                Some(&dest_bytes)
            },
        );

        match response_codec::encode_json(&serde_json::json!({
            "source_key": src_str,
            "dest_key": dst_str,
            "field": field,
            "amount": amount,
            "source_balance": source_balance - amount,
            "dest_balance": dest_balance + amount,
        })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Atomic non-fungible item transfer: verify + delete + insert in one pass.
    pub(in crate::data::executor) fn execute_kv_transfer_item(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        source_collection: &str,
        dest_collection: &str,
        item_key: &[u8],
        dest_key: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %source_collection, %dest_collection, "kv transfer item");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();

        // Step 1: Verify source owns the item.
        let Some(item_data) = self.kv_engine.get(tid, source_collection, item_key, now_ms) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // Step 2: Delete from source, insert at dest — atomic (single core).
        self.kv_engine
            .delete(tid, source_collection, &[item_key.to_vec()], now_ms);
        self.kv_engine.put(
            tid,
            dest_collection,
            dest_key,
            &item_data,
            0,
            now_ms,
            nodedb_types::Surrogate::ZERO,
        );

        if let Some(ref m) = self.metrics {
            m.record_kv_delete();
            m.record_kv_put();
        }

        // Emit CDC events.
        let item_str = String::from_utf8_lossy(item_key);
        let dest_str = String::from_utf8_lossy(dest_key);
        self.emit_write_event(
            task,
            source_collection,
            crate::event::WriteOp::Delete,
            &item_str,
            None,
            Some(&item_data),
        );
        self.emit_write_event(
            task,
            dest_collection,
            crate::event::WriteOp::Insert,
            &dest_str,
            Some(&item_data),
            None,
        );

        match response_codec::encode_json(&serde_json::json!({
            "item_key": item_str,
            "dest_key": dest_str,
            "source_collection": source_collection,
            "dest_collection": dest_collection,
        })) {
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

/// Extract a numeric field from a MessagePack-encoded KV value.
fn extract_numeric_field(value: &[u8], field: &str) -> Option<f64> {
    let doc: serde_json::Value = nodedb_types::json_from_msgpack(value).ok()?;
    let v = doc.get(field)?;
    v.as_f64().or_else(|| v.as_i64().map(|i| i as f64))
}

/// Update a numeric field in a MessagePack-encoded KV value, preserving other fields.
fn update_numeric_field(value: &[u8], field: &str, new_value: f64) -> Result<Vec<u8>, ErrorCode> {
    let mut doc: serde_json::Value =
        nodedb_types::json_from_msgpack(value).map_err(|e| ErrorCode::Internal {
            detail: format!("deserialize value: {e}"),
        })?;
    if let Some(obj) = doc.as_object_mut() {
        if new_value.fract() == 0.0 && new_value >= i64::MIN as f64 && new_value <= i64::MAX as f64
        {
            obj.insert(field.to_string(), serde_json::json!(new_value as i64));
        } else {
            obj.insert(field.to_string(), serde_json::json!(new_value));
        }
    }
    nodedb_types::json_to_msgpack(&doc).map_err(|e| ErrorCode::Internal {
        detail: format!("serialize value: {e}"),
    })
}
