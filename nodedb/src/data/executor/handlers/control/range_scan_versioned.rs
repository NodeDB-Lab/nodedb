// SPDX-License-Identifier: BUSL-1.1

//! The native-protocol RANGE scan.
//!
//! Every document collection keeps its rows and index entries versioned. A
//! field with a plain secondary index narrows the scan through the versioned
//! range lookup: the index names every row whose current version holds a
//! value in range, and only those rows are read. Any other field reads the
//! current version of every row. Both paths run the same range and RLS
//! predicate on each body, then sort by `field`, cap at `limit` and encode,
//! so the path a collection takes is not observable.

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

/// Range-bound test matching the versioned range lookup: **inclusive lower,
/// exclusive upper**, comparing **lexically** (byte order) on the field's
/// string form. The field value here is stringified via
/// `extract_index_values` (`json_scalar_to_string`), the exact encoding the
/// index keys on, so the index-narrowed and full-scan paths admit the same
/// rows. A `None` bound is unbounded on that end.
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
    /// RANGE scan: current-version rows whose `field` falls in
    /// `[lower, upper)`, sorted ascending by `field` and capped at `limit`.
    pub(in crate::data::executor) fn execute_range_scan_versioned(
        &self,
        task: &ExecutionTask,
        args: super::snapshot::RangeScanArgs<'_>,
    ) -> Response {
        let crs = self.crs_context();
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
        let identity_column =
            self.identity_column(task.request.database_id.as_u64(), tid, collection);

        // Predicate: decode each current body, extract `field`, keep in-range
        // rows. `extract_index_values(_, field, false)` yields the scalar
        // string form for the path (0 or 1 value for a non-array field).
        // The scan API's predicate is `Fn(&StorageKey, &[u8]) -> bool`, so an
        // undecodable body is captured through this `Cell` side-channel and
        // checked once the scan finishes. Returning `false` and moving on
        // would drop the row from the answer with nothing anywhere saying a
        // row was dropped, which reads to the client as a smaller — but
        // correct-looking — result set.
        let decode_err: std::cell::Cell<Option<crate::Error>> = std::cell::Cell::new(None);
        let predicate = |doc_id: &nodedb_types::StorageKey, body: &[u8]| match decode_body(
            body,
            strict_schema.as_ref(),
        ) {
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
                // that errors excludes the row and fails the scan through the
                // same side-channel: admitting the row would leak it, and
                // dropping it quietly would shrink the answer.
                match &rls {
                    None => true,
                    // The filters evaluate against the normalized msgpack form,
                    // the same encoding every other RLS site filters on — a
                    // strict body is a Binary Tuple until it is decoded here.
                    // A row that lacks its identity column carries its
                    // identity only in its storage key, so the identity is
                    // injected under that column before the RLS check, the
                    // image a reader of the same row sees.
                    Some(filters) => match nodedb_types::json_msgpack::json_to_msgpack(&doc) {
                        Ok(mp) => {
                            let mp = crate::data::executor::row_shape::inject_row_identity(
                                &mp,
                                doc_id,
                                &identity_column,
                            );
                            match crate::bridge::scan_filter::ScanFilter::all_match_binary(
                                filters, &mp, &crs,
                            ) {
                                Ok(admitted) => admitted,
                                Err(e) => {
                                    decode_err.set(Some(e.into()));
                                    false
                                }
                            }
                        }
                        Err(e) => {
                            decode_err.set(Some(crate::Error::Serialization {
                                format: "msgpack".into(),
                                detail: format!("range scan row for RLS: {e}"),
                            }));
                            false
                        }
                    },
                }
            }
        };

        // The rows come back in storage-key order and are sorted by `field`
        // below, so the scan must return every matching row: a read cut at a
        // row count would sort a prefix and return the wrong top-`limit`. The
        // read is bounded by the scan byte budget alone, checked after it.
        let budget = self.query_tuning.max_scan_result_bytes;
        let scan_limit = super::super::scan_budget::fetch_limit_for(usize::MAX, 0, budget);
        // Safe point: the versioned scan consults this once per scanned
        // version. A statement that goes over its deadline mid-scan stops here
        // and the check below turns the short result into an error.
        let deadline = crate::data::executor::deadline::DeadlineCheck::for_task(task);
        let database_id = task.request.database_id.as_u64();
        let scanned = match self.range_index_candidates(&config_key, field, lower, upper) {
            // The index names the candidates. Each one's current body runs
            // the same predicate the full scan runs.
            Some(index_range) => index_range.and_then(|keys| {
                let mut rows = Vec::new();
                for key in keys {
                    if deadline.expired() {
                        break;
                    }
                    if let Some(body) = self.sparse.get(database_id, tid, collection, &key)?
                        && predicate(&key, &body)
                    {
                        rows.push((key, body));
                    }
                }
                Ok(rows)
            }),
            None => self.sparse.versioned_scan_as_of(
                crate::engine::sparse::btree_versioned::VersionedScanParams {
                    database_id,
                    tenant: tid,
                    coll: collection,
                    sys_cutoff: None,
                    valid_at_ms: None,
                    limit: scan_limit,
                },
                &predicate,
                &|| deadline.expired(),
            ),
        };
        let mut scanned = match scanned {
            Ok(rows) => rows,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "range scan failed");
                return self.response_error(task, ErrorCode::from(e));
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
                    // An unreadable row fails the scan; dropping it would
                    // shrink the answer with nothing saying a row is missing.
                    None => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!(
                                    "range scan of '{collection}': row {id} is not a Binary \
                                     Tuple readable under the collection's strict schema"
                                ),
                            },
                        );
                    }
                },
                None => body,
            };
            rows.push((id.to_string(), mp));
        }
        if super::super::scan_budget::scan_bytes_exceeded(&rows, budget) {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        // Sort ascending by `field` and cap at `limit`, whichever path read
        // the rows.
        let sort_keys = [nodedb_physical::physical_plan::SortKeySpec::column(
            field, true,
        )];
        let decimal_keys = sort::decimal_sort_keys(&sort_keys, strict_schema.as_ref());
        if let Err(e) = sort::sort_rows(&mut rows, &sort_keys, &decimal_keys, &crs) {
            return self.response_error(task, ErrorCode::from(e));
        }
        rows.truncate(limit);

        match response_codec::encode_raw_document_rows(&rows) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// The rows a range scan reads when `field` carries a plain secondary
    /// index: every row whose current version holds a value in
    /// `[lower, upper)`, from the versioned range lookup. `None` when the
    /// index cannot narrow the scan: no index on `field`, an array,
    /// case-folded or partial index (whose entries are not the field's plain
    /// string form for every row), or a bound that is not UTF-8.
    fn range_index_candidates(
        &self,
        config_key: &(crate::types::DatabaseId, crate::types::TenantId, String),
        field: &str,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Option<crate::Result<Vec<nodedb_types::StorageKey>>> {
        let config = self.doc_configs.get(config_key)?;
        let plain_index = config.index_paths.iter().any(|path| {
            path.path == field
                && !path.is_array
                && !path.case_insensitive
                && path.predicate.is_none()
        });
        if !plain_index {
            return None;
        }
        let lower = match lower {
            Some(bytes) => Some(std::str::from_utf8(bytes).ok()?),
            None => None,
        };
        let upper = match upper {
            Some(bytes) => Some(std::str::from_utf8(bytes).ok()?),
            None => None,
        };
        let entries = self.sparse.versioned_index_range_as_of(
            crate::engine::sparse::btree_versioned::VersionedIndexRange {
                database_id: config_key.0.as_u64(),
                tenant: config_key.1.as_u64(),
                coll: &config_key.2,
                field,
                lower,
                upper,
                sys_cutoff: None,
            },
        );
        Some(entries.map(|entries| {
            let mut keys: Vec<nodedb_types::StorageKey> =
                entries.into_iter().map(|(_, key)| key).collect();
            keys.sort_unstable();
            keys.dedup();
            keys
        }))
    }
}
