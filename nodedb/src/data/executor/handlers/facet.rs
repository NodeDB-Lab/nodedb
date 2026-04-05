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

impl CoreLoop {
    /// Execute a multi-facet count query.
    ///
    /// Returns a JSON object: `{ field: [{value, count}, ...], ... }` where
    /// each field has its facet values sorted by count descending.
    pub(in crate::data::executor) fn execute_facet_counts(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        filter_bytes: &[u8],
        fields: &[String],
        limit_per_facet: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, facet_fields = fields.len(), "facet counts");

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

        let matching_ids = match self.scan_matching_documents(tid, collection, &filters) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        let matching_set: HashSet<String> = matching_ids.iter().cloned().collect();

        // Step 2: For each facet field, count values.
        let mut facet_result = serde_json::Map::new();
        let effective_limit = if limit_per_facet == 0 {
            usize::MAX
        } else {
            limit_per_facet
        };

        for field in fields {
            let counts =
                self.count_facet_field(tid, collection, field, &matching_set, &matching_ids);
            let facet_values: Vec<serde_json::Value> = counts
                .into_iter()
                .take(effective_limit)
                .map(|(value, count)| serde_json::json!({ "value": value, "count": count }))
                .collect();
            facet_result.insert(field.clone(), serde_json::Value::Array(facet_values));
        }

        // Cache the result.
        let cache_key = facet_cache_key(tid, collection, fields, filter_bytes);
        if self.aggregate_cache.len() < 256
            && let Ok(bytes) =
                nodedb_types::json_to_msgpack(&serde_json::Value::Object(facet_result.clone()))
        {
            self.aggregate_cache.insert(cache_key, bytes);
        }

        match super::super::response_codec::encode_json(&serde_json::Value::Object(facet_result)) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Count distinct values for a single facet field, filtered to matching documents.
    ///
    /// Tries index-backed counting first (O(index_entries)), falls back to
    /// document-scan counting (O(matching_docs)).
    fn count_facet_field(
        &self,
        tid: u32,
        collection: &str,
        field: &str,
        matching_set: &HashSet<String>,
        matching_ids: &[String],
    ) -> Vec<(String, usize)> {
        // Fast path: index-backed counting with filtered doc set.
        if let Ok(groups) =
            self.sparse
                .scan_index_groups_filtered(tid, collection, field, matching_set)
            && !groups.is_empty()
        {
            return groups;
        }

        // Fallback: scan matching documents, extract field, count in HashMap.
        let mut counts: HashMap<String, usize> = HashMap::new();
        for doc_id in matching_ids {
            if let Ok(Some(bytes)) = self.sparse.get(tid, collection, doc_id)
                && let Some(doc) = super::super::doc_format::decode_document(&bytes)
            {
                let value_str = match doc.get(field) {
                    Some(serde_json::Value::String(s)) => s.clone(),
                    Some(serde_json::Value::Number(n)) => n.to_string(),
                    Some(serde_json::Value::Bool(b)) => b.to_string(),
                    Some(serde_json::Value::Null) | None => continue,
                    Some(other) => other.to_string(),
                };
                *counts.entry(value_str).or_default() += 1;
            }
        }

        let mut result: Vec<(String, usize)> = counts.into_iter().collect();
        result.sort_by(|a, b| b.1.cmp(&a.1)); // Count descending.
        result
    }
}

/// Build a cache key for facet counts.
fn facet_cache_key(tid: u32, collection: &str, fields: &[String], filter_bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut key = format!("{tid}:{collection}\0facet:");
    let _ = write!(key, "{}", fields.join(","));
    if !filter_bytes.is_empty() {
        // Hash the filter bytes to avoid bloating the cache key.
        let hash = crc32c::crc32c(filter_bytes);
        let _ = write!(key, "\0filter:{hash:08x}");
    }
    key
}
