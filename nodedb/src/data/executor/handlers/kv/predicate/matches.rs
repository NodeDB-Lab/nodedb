// SPDX-License-Identifier: BUSL-1.1

//! The one scan a KV predicate `UPDATE` / `DELETE` resolves its row set with.
//!
//! Shared by the live handlers (`apply.rs`) and the resolve-before-propose
//! handlers (`kv/resolve/predicate_ops.rs`), so a governed statement decides
//! the policy over exactly the rows the ungoverned one would write.

use crate::bridge::envelope::ErrorCode;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::kv::KvScanParams;

/// Rows fetched per engine scan round-trip while resolving a predicate.
/// Bounds the transient batch, not the result: the loop pages to the end of
/// the collection, because a DML must see every matching row.
const KV_PREDICATE_SCAN_BATCH: usize = 1024;

/// One matched row: its key and its stored body.
pub(in crate::data::executor) type KvPredicateRow = (Vec<u8>, Vec<u8>);

impl CoreLoop {
    /// Every `(key, stored body)` in `collection` that `filters` matches. A
    /// malformed filter payload is an error, never an empty predicate — a
    /// silent decode failure would turn a `WHERE` into a whole-collection write.
    pub(in crate::data::executor) fn kv_predicate_matches(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        filters: &[u8],
        now_ms: u64,
    ) -> Result<Vec<KvPredicateRow>, ErrorCode> {
        let crs = self.crs_context();
        let predicates: Vec<ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
            zerompk::from_msgpack(filters).map_err(|e| ErrorCode::Internal {
                detail: format!("kv predicate dml on '{collection}': filter decode: {e}"),
            })?
        };
        // Same single-equality pushdown the read scan uses, so an indexed
        // predicate narrows the candidate set instead of walking the table.
        let probe = crate::data::executor::handlers::kv::scan::eq_index_probe(&predicates);

        let mut cursor: Vec<u8> = Vec::new();
        let mut matched: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        loop {
            let (entries, next_cursor) = self.kv_engine.scan(KvScanParams {
                database_id: did,
                tenant_id: tid,
                collection,
                cursor: &cursor,
                count: KV_PREDICATE_SCAN_BATCH,
                now_ms,
                match_pattern: None,
                filter_field: probe.as_ref().map(|(field, _)| *field),
                filter_value: probe.as_ref().map(|(_, key)| key.as_slice()),
                surrogate_ceiling: None,
            });
            for (key, value) in entries {
                if !predicates.is_empty() {
                    let (_key_str, row) =
                        crate::data::executor::scan_normalize::kv_row_to_doc(&key, &value);
                    match ScanFilter::all_match_binary(&predicates, &row, &crs) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(e) => return Err(ErrorCode::from(e)),
                    }
                }
                matched.push((key, value));
            }
            if next_cursor.is_empty() {
                return Ok(matched);
            }
            cursor = next_cursor;
        }
    }
}
