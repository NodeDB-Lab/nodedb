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
fn decode_body(body: &[u8], schema: Option<&StrictSchema>) -> crate::Result<serde_json::Value> {
    doc_format::decode_document_or_binary_tuple(body, schema, "versioned row body")
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
            rls_filters,
        } = args;

        // Decoding the filter set once, before the predicate closure, keeps a
        // malformed filter from being silently treated as "no policy" per row.
        let rls: Option<Vec<crate::bridge::scan_filter::ScanFilter>> = if rls_filters.is_empty() {
            None
        } else {
            match zerompk::from_msgpack(rls_filters) {
                Ok(f) => Some(f),
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("RLS filter deserialization failed: {e}"),
                        },
                    );
                }
            }
        };
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
        // The scan API's predicate is `Fn(&str, &[u8]) -> bool`, so an
        // undecodable body is captured through this `Cell` side-channel and
        // checked once the scan finishes. Returning `false` and moving on
        // would drop the row from the answer with nothing anywhere saying a
        // row was dropped, which reads to the client as a smaller — but
        // correct-looking — result set.
        let decode_err: std::cell::Cell<Option<crate::Error>> = std::cell::Cell::new(None);
        let predicate = |doc_id: &str, body: &[u8]| match decode_body(body, strict_schema.as_ref())
        {
            Err(e) => {
                decode_err.set(Some(e));
                false
            }
            Ok(doc) => {
                let values =
                    crate::engine::document::store::extract_index_values(&doc, field, false);
                if !values.iter().any(|v| value_in_bounds(v, lower, upper)) {
                    return false;
                }
                // Row-level security decides on the same decoded body. A filter
                // that errors excludes the row: this predicate cannot surface an
                // error, and admitting the row would leak it.
                match &rls {
                    None => true,
                    // The filters evaluate against the normalized msgpack form,
                    // the same encoding every other RLS site filters on — a
                    // strict body is a Binary Tuple until it is decoded here.
                    // A schemaless row's identity lives only in its storage
                    // key when its body carries no `id` field, so the
                    // client-visible identity is injected before the RLS
                    // check, matching what a reader of the same row sees.
                    Some(filters) => match nodedb_types::json_msgpack::json_to_msgpack(&doc) {
                        Ok(mp) => {
                            let mp = if strict_schema.is_none() {
                                let identity = crate::engine::document::store::identity_of(doc_id);
                                nodedb_query::msgpack_scan::inject_str_field(
                                    &mp,
                                    "id",
                                    identity.as_str(),
                                )
                            } else {
                                mp
                            };
                            crate::bridge::scan_filter::ScanFilter::all_match_binary(filters, &mp)
                                .unwrap_or(false)
                        }
                        Err(_) => false,
                    },
                }
            }
        };

        // Over-fetch so the sort produces a correct top-`limit`, matching the
        // non-bitemporal fallback's `limit.max(1000)` scan ceiling. The
        // versioned scan applies `limit` only to surviving (live + matching)
        // rows, so the predicate has already excluded out-of-range and
        // tombstoned rows before this bound is counted.
        let scan_limit = limit.max(1000);
        // Safe point: the versioned scan consults this once per scanned
        // version. A statement that goes over its deadline mid-scan stops here
        // and the check below turns the short result into an error.
        let deadline = crate::data::executor::deadline::DeadlineCheck::for_task(task);
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
            &|| deadline.expired(),
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

        // Both passes above run the same predicate; check the side-channel once,
        // after the last of them, so one unreadable row fails the scan rather
        // than quietly shrinking its answer.
        if let Some(e) = decode_err.take() {
            return self.response_error(task, e);
        }

        // A scan the deadline cut short holds an arbitrary prefix of the
        // answer. Returning it would look like a complete result, so the
        // statement fails instead.
        if deadline.tripped() {
            return self.response_error(task, ErrorCode::DeadlineExceeded);
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
        if let Err(e) = sort::sort_rows(
            &mut rows,
            &[nodedb_physical::physical_plan::SortKeySpec::column(
                field, true,
            )],
        ) {
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
