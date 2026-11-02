// SPDX-License-Identifier: BUSL-1.1

//! Multi-facet aggregation handler.
//!
//! Computes facet counts for multiple fields in a single query execution.
//! The filter predicate is evaluated once to produce a set of matching document
//! IDs, then each facet field is counted against that shared set — either via
//! index-backed counting (if the field has a secondary index) or via HashMap
//! counting over the matching documents.

use std::collections::{HashMap, HashSet};

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_types::SystemTimeScope;

/// Borrowed inputs for [`CoreLoop::execute_facet_counts`].
pub(in crate::data::executor) struct FacetCountsParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    /// Serialized `Vec<ScanFilter>` predicates (MessagePack).
    pub filter_bytes: &'a [u8],
    pub fields: &'a [String],
    /// Values per facet field, `0` for every value.
    pub limit_per_facet: usize,
    /// The scope the matched rows are read at. `TxnSnapshot` and `Current`
    /// merge the reading transaction's staged writes.
    pub system_time: &'a SystemTimeScope,
}

impl CoreLoop {
    /// Execute a multi-facet count query.
    ///
    /// Returns a JSON object: `{ field: [{value, count}, ...], ... }` where
    /// each field has its facet values sorted by count descending.
    ///
    /// A current read outside a transaction counts from the secondary index
    /// when one covers the field, and caches its answer. A read at another
    /// scope, or one that merges staged writes, counts the matched bodies
    /// and caches nothing: the index and the cache hold the current state.
    pub(in crate::data::executor) fn execute_facet_counts(
        &mut self,
        task: &ExecutionTask,
        params: FacetCountsParams<'_>,
    ) -> Response {
        let FacetCountsParams {
            tid,
            collection,
            filter_bytes,
            fields,
            limit_per_facet,
            system_time,
        } = params;
        debug!(core = self.core_id, %collection, facet_fields = fields.len(), "facet counts");
        let Some(system_as_of) = point_cut_of(system_time) else {
            return self.response_error(
                task,
                ErrorCode::Unsupported {
                    detail: "AS OF SYSTEM TIME NULL (all-versions) is not supported on facet \
                             counts"
                        .into(),
                },
            );
        };
        let overlay_txn = task
            .request
            .txn_id
            .filter(|_| system_time.merges_txn_overlay());
        let reads_current_state = system_as_of.is_none() && overlay_txn.is_none();

        // Step 1: Evaluate filter predicate once → set of matching document IDs.
        let filters: Vec<ScanFilter> = if filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize facet filters: {e}"),
                        },
                    );
                }
            }
        };

        // Each matching row at the read's cut, with the reading
        // transaction's staged writes on top when the scope merges them.
        let database_id = task.request.database_id.as_u64();
        let matched = match overlay_txn {
            Some(txn_id) => self.txn_matching_rows(
                txn_id,
                &(
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection.to_string(),
                ),
                &filters,
                system_as_of,
            ),
            None => self.matching_rows_at(database_id, tid, collection, &filters, system_as_of),
        };
        let matching_rows = match matched {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };

        let matching_set: HashSet<nodedb_types::StorageKey> =
            matching_rows.iter().map(|(key, _)| *key).collect();

        // Step 2: For each facet field, count values.
        let mut facet_result = serde_json::Map::new();
        let effective_limit = if limit_per_facet == 0 {
            usize::MAX
        } else {
            limit_per_facet
        };

        for field in fields {
            let counts = match self.count_facet_field(
                FacetFieldSource {
                    database_id,
                    tid,
                    collection,
                    matching_set: &matching_set,
                    matching_rows: &matching_rows,
                    index_reads_state: reads_current_state,
                },
                field,
            ) {
                Ok(counts) => counts,
                Err(e) => return self.response_error(task, e),
            };
            let facet_values: Vec<serde_json::Value> = counts
                .into_iter()
                .take(effective_limit)
                .map(|(value, count)| serde_json::json!({ "value": value, "count": count }))
                .collect();
            facet_result.insert(field.clone(), serde_json::Value::Array(facet_values));
        }

        // Cache the result.
        let cache_key = facet_cache_key(
            task.request.database_id.as_u64(),
            tid,
            collection,
            fields,
            filter_bytes,
        );
        if reads_current_state
            && self.aggregate_cache.len() < 256
            && let Ok(bytes) =
                nodedb_types::json_to_msgpack(&serde_json::Value::Object(facet_result.clone()))
        {
            let kv_epoch =
                self.kv_engine
                    .write_epoch(task.request.database_id.as_u64(), tid, collection);
            self.aggregate_cache.insert(
                cache_key,
                super::aggregate::AggregateCacheEntry {
                    kv_epoch,
                    payload: bytes,
                },
            );
        }

        match super::super::response_codec::encode_json_as_msgpack(&serde_json::Value::Object(
            facet_result,
        )) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, crate::bridge::envelope::ErrorCode::from(e)),
        }
    }

    /// Count distinct values for a single facet field, filtered to matching documents.
    ///
    /// Tries index-backed counting first (O(index_entries)) when the index
    /// holds the read's state, falls back to document-scan counting
    /// (O(matching_docs)). A storage read or value transcoding error fails
    /// the count: a partial count is a wrong count.
    fn count_facet_field(
        &self,
        source: FacetFieldSource<'_>,
        field: &str,
    ) -> crate::Result<Vec<(String, usize)>> {
        let FacetFieldSource {
            database_id,
            tid,
            collection,
            matching_set,
            matching_rows,
            index_reads_state,
        } = source;
        // Fast path: index-backed counting with filtered doc set. The index
        // holds each row's current value, so only a current read uses it.
        if index_reads_state {
            let groups = self.sparse.scan_index_groups_filtered(
                database_id,
                tid,
                collection,
                field,
                matching_set,
            )?;
            if !groups.is_empty() {
                return Ok(groups);
            }
        }

        // Fallback: scan matching documents, extract field from msgpack, count.
        //
        // The bodies are STORED rows, so the encoding comes from the
        // collection's registered kind. Normalizing them as documents leaves a
        // strict collection's Binary Tuple untouched — `extract_field` then
        // finds nothing in every row and the facet reports an empty count
        // rather than an error, so the facet silently disagrees with the scan
        // that produced `matching_rows`.
        let body_format = self.sparse_body_format(
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection,
        );
        let mut counts: HashMap<String, usize> = HashMap::new();
        for (key, bytes) in matching_rows {
            let mp = crate::data::executor::scan_normalize::sparse_body_to_msgpack(
                bytes,
                body_format.as_format_ref(),
            );
            if let Some((start, end)) = nodedb_query::msgpack_scan::extract_field(&mp, 0, field) {
                let value_str = if let Some(s) = nodedb_query::msgpack_scan::read_str(&mp, start) {
                    s.to_string()
                } else if let Some(i) = nodedb_query::msgpack_scan::read_integer(&mp, start) {
                    i.to_string()
                } else if let Some(f) = nodedb_query::msgpack_scan::read_f64(&mp, start) {
                    f.to_string()
                } else if let Some(b) = nodedb_query::msgpack_scan::read_bool(&mp, start) {
                    b.to_string()
                } else if nodedb_query::msgpack_scan::read_null(&mp, start) {
                    continue;
                } else {
                    // Complex value — stringify via transcoder.
                    nodedb_types::msgpack_to_json_string(&mp[start..end]).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".to_string(),
                            detail: format!("facet value of '{field}' in {key}: {e}"),
                        }
                    })?
                };
                *counts.entry(value_str).or_default() += 1;
            }
        }

        let mut result: Vec<(String, usize)> = counts.into_iter().collect();
        result.sort_by_key(|r| std::cmp::Reverse(r.1)); // Count descending.
        Ok(result)
    }
}

/// The matched rows one facet field is counted over.
struct FacetFieldSource<'a> {
    database_id: u64,
    tid: u64,
    collection: &'a str,
    matching_set: &'a HashSet<nodedb_types::StorageKey>,
    matching_rows: &'a [(nodedb_types::StorageKey, Vec<u8>)],
    /// True when the secondary index holds the read's state: a current read
    /// with no staged writes merged.
    index_reads_state: bool,
}

/// The ordinal a facet read reads at: `Some(None)` for the current state,
/// `Some(Some(ordinal))` for a point-in-time scope, `None` for
/// `AllVersions`.
fn point_cut_of(system_time: &SystemTimeScope) -> Option<Option<i64>> {
    match system_time {
        SystemTimeScope::Current => Some(None),
        SystemTimeScope::AsOf(_)
        | SystemTimeScope::Pinned { .. }
        | SystemTimeScope::TxnSnapshot { .. } => Some(system_time.as_of_ordinal()),
        SystemTimeScope::AllVersions => None,
    }
}

/// Build a cache key for facet counts.
fn facet_cache_key(
    database_id: u64,
    tid: u64,
    collection: &str,
    fields: &[String],
    filter_bytes: &[u8],
) -> (crate::types::DatabaseId, crate::types::TenantId, String) {
    use std::fmt::Write;
    let mut rest = format!("{collection}\0facet:");
    let _ = write!(rest, "{}", fields.join(","));
    if !filter_bytes.is_empty() {
        // Hash the filter bytes to avoid bloating the cache key.
        let hash = crc32c::crc32c(filter_bytes);
        let _ = write!(rest, "\0filter:{hash:08x}");
    }
    (
        crate::types::DatabaseId::new(database_id),
        crate::types::TenantId::new(tid),
        rest,
    )
}
