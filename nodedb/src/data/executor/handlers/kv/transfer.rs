// SPDX-License-Identifier: BUSL-1.1

//! Atomic transfer handlers: Transfer (fungible) and TransferItem (non-fungible).
//!
//! These execute entirely within a single TPC core pass — no TOCTOU race.
//! Read + validate + write happens atomically because the TPC core is
//! single-threaded and owns all keys in its hash table.

use tracing::debug;

use super::transfer_compute::{TransferError, compute_transfer};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

/// Parameters for an atomic fungible transfer.
pub(in crate::data::executor) struct TransferParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub source_key: &'a [u8],
    pub dest_key: &'a [u8],
    pub field: &'a str,
    pub amount: f64,
    /// Cross-engine surrogate of the debit (source) row.
    pub debit_surrogate: nodedb_types::Surrogate,
    /// Cross-engine surrogate of the credit (dest) row.
    pub credit_surrogate: nodedb_types::Surrogate,
    /// Compiled row-level-security WRITE predicate for the collection both
    /// rows live in.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

/// Parameters for an atomic non-fungible item transfer.
pub(in crate::data::executor) struct TransferItemParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub source_collection: &'a str,
    pub dest_collection: &'a str,
    pub item_key: &'a [u8],
    pub dest_key: &'a [u8],
    /// Cross-engine surrogate of the moved row at its destination.
    pub surrogate: nodedb_types::Surrogate,
    /// Compiled row-level-security WRITE predicate of the SOURCE collection,
    /// decided against the row being removed from it.
    pub source_rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Compiled row-level-security WRITE predicate of the DESTINATION
    /// collection, decided against the same bytes being inserted there. The
    /// two collections carry independent policies, so the two checks stay
    /// separate.
    pub dest_rls_write_check: &'a nodedb_types::RlsWriteCheck,
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
            did,
            tid,
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
            rls_write_check,
        } = params;
        debug!(core = self.core_id, %collection, %field, amount, "kv transfer");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();

        // Step 1: Read both values atomically (same core, no interleaving).
        let source_val = self.kv_engine.get(did, tid, collection, source_key, now_ms);
        let dest_val = self.kv_engine.get(did, tid, collection, dest_key, now_ms);

        let Some(source_bytes) = source_val else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // Step 2 + 3: validate + compute new values via the pure computation
        // shared with the in-transaction staging handler
        // (`stage_kv_transfer.rs`), so a staged pair of writes and their
        // COMMIT-time durable replay never diverge.
        let dest_bytes = dest_val.unwrap_or_default();
        let dest_ref = if dest_bytes.is_empty() {
            None
        } else {
            Some(dest_bytes.as_slice())
        };
        let computed = match compute_transfer(&source_bytes, dest_ref, field, amount) {
            Ok(c) => c,
            Err(TransferError::TypeMismatch(detail)) => {
                return self.response_error(
                    task,
                    ErrorCode::TypeMismatch {
                        collection: collection.to_string(),
                        detail,
                    },
                );
            }
            Err(TransferError::InsufficientBalance { have, need }) => {
                return self.response_error(
                    task,
                    ErrorCode::InsufficientBalance {
                        collection: collection.to_string(),
                        detail: format!("source has {have}, need {need}"),
                    },
                );
            }
        };
        let new_source = computed.new_source;
        let new_dest = computed.new_dest;
        let source_balance_after = computed.source_balance_after;
        let dest_balance_after = computed.dest_balance_after;

        // Both post-images are decided before either is persisted: a transfer
        // is one write, so a policy that rejects the credit must not leave the
        // debit applied.
        if let Err(e) =
            super::rls::admit_kv_row(rls_write_check, &new_source, source_key, tid, collection)
        {
            return self.response_error(task, e);
        }
        if let Err(e) =
            super::rls::admit_kv_row(rls_write_check, &new_dest, dest_key, tid, collection)
        {
            return self.response_error(task, e);
        }

        // Step 4: Write both atomically (deterministic order for consistency).
        // Write lower key first to match the documented lock ordering.
        if source_key <= dest_key {
            self.kv_engine.put(crate::engine::kv::KvPutParams {
                database_id: did,
                tenant_id: tid,
                collection,
                key: source_key,
                value: &new_source,
                ttl_ms: 0,
                now_ms,
                surrogate: debit_surrogate,
            });
            self.kv_engine.put(crate::engine::kv::KvPutParams {
                database_id: did,
                tenant_id: tid,
                collection,
                key: dest_key,
                value: &new_dest,
                ttl_ms: 0,
                now_ms,
                surrogate: credit_surrogate,
            });
        } else {
            self.kv_engine.put(crate::engine::kv::KvPutParams {
                database_id: did,
                tenant_id: tid,
                collection,
                key: dest_key,
                value: &new_dest,
                ttl_ms: 0,
                now_ms,
                surrogate: credit_surrogate,
            });
            self.kv_engine.put(crate::engine::kv::KvPutParams {
                database_id: did,
                tenant_id: tid,
                collection,
                key: source_key,
                value: &new_source,
                ttl_ms: 0,
                now_ms,
                surrogate: debit_surrogate,
            });
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
            crate::engine::document::store::RowIdentity::from_user_key(src_str.as_ref()),
            Some(&new_source),
            Some(&source_bytes),
        );
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Update,
            crate::engine::document::store::RowIdentity::from_user_key(dst_str.as_ref()),
            Some(&new_dest),
            if dest_bytes.is_empty() {
                None
            } else {
                Some(&dest_bytes)
            },
        );

        match response_codec::encode_json_as_msgpack(&serde_json::json!({
            "source_key": src_str,
            "dest_key": dst_str,
            "field": field,
            "amount": amount,
            "source_balance": source_balance_after,
            "dest_balance": dest_balance_after,
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
        params: TransferItemParams<'_>,
    ) -> Response {
        let TransferItemParams {
            did,
            tid,
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
            source_rls_write_check,
            dest_rls_write_check,
        } = params;
        debug!(core = self.core_id, %source_collection, %dest_collection, "kv transfer item");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();

        // Step 1: Verify source owns the item.
        let Some(item_data) = self
            .kv_engine
            .get(did, tid, source_collection, item_key, now_ms)
        else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // The same bytes are two different images to two different policies:
        // the row leaving the source, and the row arriving at the destination.
        // Both are decided before either half runs, so a move a policy rejects
        // cannot delete from the source and then fail to insert at the dest.
        if let Err(e) = super::rls::admit_kv_row(
            source_rls_write_check,
            &item_data,
            item_key,
            tid,
            source_collection,
        ) {
            return self.response_error(task, e);
        }
        if let Err(e) = super::rls::admit_kv_row(
            dest_rls_write_check,
            &item_data,
            dest_key,
            tid,
            dest_collection,
        ) {
            return self.response_error(task, e);
        }

        // Step 2: Delete from source, insert at dest — atomic (single core).
        self.kv_engine
            .delete(did, tid, source_collection, &[item_key.to_vec()], now_ms);
        self.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: did,
            tenant_id: tid,
            collection: dest_collection,
            key: dest_key,
            value: &item_data,
            ttl_ms: 0,
            now_ms,
            surrogate,
        });

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
            crate::engine::document::store::RowIdentity::from_user_key(item_str.as_ref()),
            None,
            Some(&item_data),
        );
        self.emit_write_event(
            task,
            dest_collection,
            crate::event::WriteOp::Insert,
            crate::engine::document::store::RowIdentity::from_user_key(dest_str.as_ref()),
            Some(&item_data),
            None,
        );

        match response_codec::encode_json_as_msgpack(&serde_json::json!({
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
