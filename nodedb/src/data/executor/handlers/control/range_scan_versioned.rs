// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal branch of the native-protocol RANGE scan.
//!
//! A `bitemporal=true` document collection keeps every write on the versioned
//! redb table (see `versioned_put_in_txn`); its plain INDEXES / DOCUMENTS
//! tables are empty. `execute_range_scan`'s index probe + plain-table fallback
//! therefore return ZERO rows for such a collection. This helper reads the
//! newest live version per `doc_id` from the versioned namespace — mirroring
//! the current-state scan in `execute_document_scan` — filters by the range
//! bounds, then sorts + limits + encodes exactly like the non-bitemporal
//! full-scan fallback.

use tracing::warn;

use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::columnar::StrictSchema;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::sort;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::data::executor::{doc_format, strict_format};

/// Decode a stored versioned body to `serde_json::Value`, strict-safely:
/// strict bodies are Binary Tuples (need the schema), schemaless bodies are
/// MessagePack. Mirrors `CoreLoop::decode_stored_document` but takes the
/// already-resolved schema so the scan predicate closure never borrows `self`.
fn decode_body(body: &[u8], schema: Option<&StrictSchema>) -> Option<serde_json::Value> {
    match schema {
        Some(schema) => strict_format::binary_tuple_to_json(body, schema),
        None => doc_format::decode_document(body),
    }
}

/// Range-bound test matching the secondary-index path in
/// `btree_index::range_scan`: redb scans the half-open key range
/// `[prefix+lower, prefix+upper)`, i.e. **inclusive lower, exclusive upper**,
/// comparing keys **lexically** (byte order) on the field's string form.
/// The field value here is stringified via `extract_index_values`
/// (`json_scalar_to_string`) — the exact same encoding the index keys on — so
/// bitemporal and non-bitemporal scans admit the same rows for the same data.
/// A `None` bound is unbounded on that end.
fn value_in_bounds(value: &str, lower: Option<&[u8]>, upper: Option<&[u8]>) -> bool {
    let v = value.as_bytes();
    if let Some(l) = lower
        && v < l
    {
        return false;
    }
    if let Some(u) = upper
        && v >= u
    {
        return false;
    }
    true
}

impl CoreLoop {
    /// Bitemporal RANGE scan: current-version rows whose `field` falls in
    /// `[lower, upper)`, sorted ascending by `field` and capped at `limit`.
    pub(in crate::data::executor) fn execute_range_scan_bitemporal(
        &self,
        task: &ExecutionTask,
        args: super::snapshot::RangeScanArgs<'_>,
    ) -> Response {
        let super::snapshot::RangeScanArgs {
            tid,
            collection,
            field,
            lower,
            upper,
            limit,
        } = args;
        // Resolve the strict schema (if any) for strict-safe body decode.
        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let StorageMode::Strict { ref schema } = c.storage_mode {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Predicate: decode each current body, extract `field`, keep in-range
        // rows. `extract_index_values(_, field, false)` yields the scalar
        // string form for the path (0 or 1 value for a non-array field).
        let predicate = |body: &[u8]| match decode_body(body, strict_schema.as_ref()) {
            Some(doc) => {
                let values =
                    crate::engine::document::store::extract_index_values(&doc, field, false);
                values.iter().any(|v| value_in_bounds(v, lower, upper))
            }
            None => false,
        };

        // Over-fetch so the sort produces a correct top-`limit`, matching the
        // non-bitemporal fallback's `limit.max(1000)` scan ceiling. The
        // versioned scan applies `limit` only to surviving (live + matching)
        // rows, so the predicate has already excluded out-of-range and
        // tombstoned rows before this bound is counted.
        let scan_limit = limit.max(1000);
        let mut scanned = match self.sparse.versioned_scan_as_of(
            crate::engine::sparse::btree_versioned::VersionedScanParams {
                database_id: task.request.database_id.as_u64(),
                tenant: tid,
                coll: collection,
                sys_cutoff_ms: None,
                valid_at_ms: None,
                limit: scan_limit,
            },
            &predicate,
        ) {
            Ok(rows) => rows,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "versioned range scan failed");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Read-your-own-writes: fold this transaction's staging overlay onto
        // the current-version base result, using the SAME range predicate on
        // the raw stored bodies (staged strict bodies are Binary Tuples, like
        // base — the msgpack normalization below happens after the merge). This
        // range scan is always current-version (no sys/valid cutoff), so the
        // merge is safe.
        if let Some(txn_id) = task.request.txn_id {
            let coll_key = (
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );
            self.merge_overlay_into_scan(txn_id, &coll_key, &mut scanned, &predicate);
        }

        // Normalize bodies to MessagePack so `sort_rows` (msgpack field
        // extraction) and the raw row codec operate on one encoding. Strict
        // Binary Tuples decode to msgpack via the schema; schemaless bodies
        // are already msgpack.
        let mut rows: Vec<(String, Vec<u8>)> = Vec::with_capacity(scanned.len());
        for (id, body) in scanned {
            let mp = match &strict_schema {
                Some(schema) => match strict_format::binary_tuple_to_msgpack(&body, schema) {
                    Some(mp) => mp,
                    None => continue,
                },
                None => body,
            };
            rows.push((id, mp));
        }

        // Sort ascending by `field` and cap at `limit` — the same ordering the
        // secondary-index range path yields (index-key order) and the same
        // truncation the non-bitemporal fallback applies.
        if let Err(e) = sort::sort_rows(&mut rows, &[(field.to_string(), true)]) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("in-memory sort failed: {e}"),
                },
            );
        }
        rows.truncate(limit);

        match response_codec::encode_raw_document_rows(&rows) {
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
