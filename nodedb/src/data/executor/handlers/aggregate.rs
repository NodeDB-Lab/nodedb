//! Aggregate handler: GROUP BY, HAVING, and aggregate function execution.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::{ScanFilter, compute_aggregate};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Build a cache key for an aggregate query.
///
/// Format: `"{tid}:{collection}\0{group_fields}\0{agg_ops}"`.
/// Null bytes separate sections to avoid ambiguity with field names.
fn aggregate_cache_key(
    tid: u32,
    collection: &str,
    group_by: &[String],
    aggregates: &[(String, String)],
    sub_group_by: &[String],
    sub_aggregates: &[(String, String)],
) -> String {
    use std::fmt::Write;
    let mut key = format!(
        "{tid}:{collection}\0{}\0{}",
        group_by.join(","),
        aggregates
            .iter()
            .map(|(op, f)| format!("{op}({f})"))
            .collect::<Vec<_>>()
            .join(",")
    );
    if !sub_group_by.is_empty() || !sub_aggregates.is_empty() {
        let _ = write!(
            key,
            "\0sub:{}\0{}",
            sub_group_by.join(","),
            sub_aggregates
                .iter()
                .map(|(op, f)| format!("{op}({f})"))
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    key
}

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_aggregate(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        group_by: &[String],
        aggregates: &[(String, String)],
        filters: &[u8],
        having: &[u8],
        limit: usize,
        sub_group_by: &[String],
        sub_aggregates: &[(String, String)],
    ) -> Response {
        debug!(core = self.core_id, %collection, group_fields = group_by.len(), aggs = aggregates.len(), "aggregate");

        // Fast path: incremental aggregate cache.
        // If we've cached the result for this exact (collection, group_by, aggregates)
        // combination and there are no filters/having, return cached result directly.
        if filters.is_empty() && having.is_empty() {
            let cache_key = aggregate_cache_key(
                tid,
                collection,
                group_by,
                aggregates,
                sub_group_by,
                sub_aggregates,
            );
            if let Some(cached) = self.aggregate_cache.get(&cache_key) {
                debug!(core = self.core_id, %collection, "aggregate cache hit");
                return self.response_with_payload(task, cached.clone());
            }
        }

        // Fast path: index-backed COUNT/GROUP BY.
        // When GROUP BY has a single field, no filters, no HAVING, and the
        // only aggregate is COUNT(*), scan the INDEXES table directly.
        // No document table access — O(index_entries) instead of O(documents).
        if group_by.len() == 1
            && filters.is_empty()
            && having.is_empty()
            && aggregates.len() == 1
            && aggregates[0].0 == "count"
        {
            let field = &group_by[0];
            // Empty index — fall through to full scan (documents may exist
            // without index entries if no secondary indexes are declared).
            if let Ok(groups) = self.sparse.scan_index_groups(tid, collection, field)
                && !groups.is_empty()
            {
                let mut results: Vec<serde_json::Value> = groups
                    .into_iter()
                    .take(limit)
                    .map(|(value, count)| {
                        let mut row = serde_json::Map::new();
                        row.insert(field.clone(), serde_json::Value::String(value));
                        row.insert("count_all".into(), serde_json::json!(count));
                        serde_json::Value::Object(row)
                    })
                    .collect();
                results.truncate(limit);
                return match super::super::response_codec::encode_json_vec(&results) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
        }

        // Aggregates must scan all matching documents for correct results.
        // Cap at aggregate_scan_cap to prevent OOM on unbounded collections.
        let scan_limit = self.query_tuning.aggregate_scan_cap;

        // If collection has columnar memtable data, read from there.
        // Works for all columnar profiles: plain, timeseries, spatial.
        // Spatial inserts write to both sparse (R-tree) and columnar (scans/aggregates).
        let use_columnar = self
            .columnar_memtables
            .get(collection)
            .is_some_and(|mt| !mt.is_empty());

        // Fast path: native columnar aggregation.
        // Groups directly on symbol IDs (u32) instead of JSON-serialized strings.
        // Accumulates in-place without document construction.
        // Falls back to generic path for complex filters (OR, string comparisons).
        if use_columnar && sub_group_by.is_empty() && sub_aggregates.is_empty() {
            let mt = self.columnar_memtables.get(collection).unwrap();
            let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
                Vec::new()
            } else {
                zerompk::from_msgpack(filters).unwrap_or_default()
            };

            if let Some(mut agg_result) = super::columnar_agg::try_columnar_aggregate(
                mt,
                group_by,
                aggregates,
                &filter_predicates,
                limit,
                scan_limit,
            ) {
                // Apply HAVING filters.
                if !having.is_empty() {
                    let having_predicates: Vec<ScanFilter> =
                        zerompk::from_msgpack(having).unwrap_or_default();
                    if !having_predicates.is_empty() {
                        agg_result
                            .rows
                            .retain(|row| having_predicates.iter().all(|f| f.matches(row)));
                    }
                }

                agg_result.rows.truncate(limit);

                return match super::super::response_codec::encode_json_vec(&agg_result.rows) {
                    Ok(payload) => {
                        if filters.is_empty() && having.is_empty() {
                            let cache_key = aggregate_cache_key(
                                tid,
                                collection,
                                group_by,
                                aggregates,
                                sub_group_by,
                                sub_aggregates,
                            );
                            if self.aggregate_cache.len() < 256 {
                                self.aggregate_cache.insert(cache_key, payload.clone());
                            }
                        }
                        self.response_with_payload(task, payload)
                    }
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
            // Native path returned None — fall through to generic path.
        }

        let docs_result = if use_columnar {
            let mt = self.columnar_memtables.get(collection).unwrap();
            let schema = mt.schema();
            let row_count = (mt.row_count() as usize).min(scan_limit);
            let col_meta: Vec<_> = schema
                .columns
                .iter()
                .enumerate()
                .map(|(i, (name, ty))| (i, name.clone(), *ty))
                .collect();
            let mut docs = Vec::with_capacity(row_count);
            for idx in 0..row_count {
                let mut row = serde_json::Map::new();
                for (col_idx, col_name, col_type) in &col_meta {
                    let col_data = mt.column(*col_idx);
                    let val = super::columnar_read::emit_column_value(
                        mt, *col_idx, col_type, col_data, idx,
                    );
                    row.insert(col_name.to_string(), val);
                }
                // Encode as msgpack to match the sparse doc format the aggregate loop expects.
                let bytes = nodedb_types::json_to_msgpack(&serde_json::Value::Object(row))
                    .unwrap_or_default();
                docs.push((idx.to_string(), bytes));
            }
            Ok(docs)
        } else {
            self.sparse.scan_documents(tid, collection, scan_limit)
        };

        match docs_result {
            Ok(docs) => {
                let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
                    Vec::new()
                } else {
                    match zerompk::from_msgpack(filters) {
                        Ok(f) => f,
                        Err(e) => {
                            tracing::warn!(core = self.core_id, error = %e, "filter predicate deserialization failed");
                            Vec::new()
                        }
                    }
                };

                let mut groups: std::collections::HashMap<String, Vec<serde_json::Value>> =
                    std::collections::HashMap::new();

                // Determine which fields we actually need to extract.
                // This avoids full document deserialization when possible.
                let mut needed_fields: Vec<&str> = Vec::new();
                for f in group_by {
                    if !needed_fields.contains(&f.as_str()) {
                        needed_fields.push(f.as_str());
                    }
                }
                for (_, field) in aggregates {
                    if field != "*" && !needed_fields.contains(&field.as_str()) {
                        needed_fields.push(field.as_str());
                    }
                }

                // If filters are present, we need full deserialization for
                // filter evaluation (filters can reference any field).
                let needs_full_deser = !filter_predicates.is_empty();

                for (_, value) in &docs {
                    if needs_full_deser {
                        // Full deserialization path (filters need arbitrary field access).
                        let Some(doc) = super::super::doc_format::decode_document(value) else {
                            continue;
                        };
                        if !filter_predicates.iter().all(|f| f.matches(&doc)) {
                            continue;
                        }
                        let key = if group_by.is_empty() {
                            "__all__".to_string()
                        } else {
                            let key_parts: Vec<serde_json::Value> = group_by
                                .iter()
                                .map(|field| {
                                    doc.get(field.as_str())
                                        .cloned()
                                        .unwrap_or(serde_json::Value::Null)
                                })
                                .collect();
                            serde_json::to_string(&key_parts).unwrap_or_else(|_| "[]".into())
                        };
                        groups.entry(key).or_default().push(doc);
                    } else {
                        // Targeted extraction path: only extract needed fields.
                        let extracted =
                            super::super::doc_format::extract_fields(value, &needed_fields);

                        // Build group key from extracted fields.
                        let key = if group_by.is_empty() {
                            "__all__".to_string()
                        } else {
                            let key_parts: Vec<serde_json::Value> = group_by
                                .iter()
                                .map(|field| {
                                    let idx =
                                        needed_fields.iter().position(|&n| n == field.as_str());
                                    idx.and_then(|i| extracted[i].clone())
                                        .unwrap_or(serde_json::Value::Null)
                                })
                                .collect();
                            serde_json::to_string(&key_parts).unwrap_or_else(|_| "[]".into())
                        };

                        // Build a partial document with only the needed fields.
                        let mut doc_map = serde_json::Map::new();
                        for (i, &field_name) in needed_fields.iter().enumerate() {
                            if let Some(val) = &extracted[i] {
                                doc_map.insert(field_name.to_string(), val.clone());
                            }
                        }
                        groups
                            .entry(key)
                            .or_default()
                            .push(serde_json::Value::Object(doc_map));
                    }
                }

                let mut results: Vec<serde_json::Value> = Vec::new();
                for (group_key, group_docs) in &groups {
                    let mut row = serde_json::Map::new();

                    if !group_by.is_empty()
                        && let Ok(parts) = serde_json::from_str::<Vec<serde_json::Value>>(group_key)
                    {
                        for (i, field) in group_by.iter().enumerate() {
                            let val = parts.get(i).cloned().unwrap_or(serde_json::Value::Null);
                            row.insert(field.clone(), val);
                        }
                    }

                    for (op, field) in aggregates {
                        let agg_key = format!("{op}_{field}").replace('*', "all");
                        let val = compute_aggregate(op, field, group_docs);
                        row.insert(agg_key, val);
                    }

                    // Nested sub-aggregation: within each group, further group
                    // by sub_group_by fields and compute sub_aggregates.
                    if !sub_group_by.is_empty() && !sub_aggregates.is_empty() {
                        let mut sub_groups: std::collections::HashMap<
                            String,
                            Vec<serde_json::Value>,
                        > = std::collections::HashMap::new();
                        for doc in group_docs {
                            let key_parts: Vec<serde_json::Value> = sub_group_by
                                .iter()
                                .map(|f| doc.get(f).cloned().unwrap_or(serde_json::Value::Null))
                                .collect();
                            let sub_key =
                                serde_json::to_string(&key_parts).unwrap_or_else(|_| "[]".into());
                            sub_groups.entry(sub_key).or_default().push(doc.clone());
                        }

                        let mut sub_results = Vec::new();
                        for (sub_key, sub_docs) in &sub_groups {
                            let mut sub_row = serde_json::Map::new();
                            // Parse sub-group key back into fields.
                            if let Ok(parts) =
                                serde_json::from_str::<Vec<serde_json::Value>>(sub_key)
                            {
                                for (i, field) in sub_group_by.iter().enumerate() {
                                    let val =
                                        parts.get(i).cloned().unwrap_or(serde_json::Value::Null);
                                    sub_row.insert(field.clone(), val);
                                }
                            }
                            for (op, field) in sub_aggregates {
                                let agg_key = format!("{op}_{field}").replace('*', "all");
                                let val = compute_aggregate(op, field, sub_docs);
                                sub_row.insert(agg_key, val);
                            }
                            sub_results.push(serde_json::Value::Object(sub_row));
                        }
                        row.insert(
                            "sub_groups".to_string(),
                            serde_json::Value::Array(sub_results),
                        );
                    }

                    results.push(serde_json::Value::Object(row));
                }

                if !having.is_empty() {
                    let having_predicates: Vec<ScanFilter> = match zerompk::from_msgpack(having) {
                        Ok(f) => f,
                        Err(e) => {
                            tracing::warn!(core = self.core_id, error = %e, "HAVING predicate deserialization failed");
                            Vec::new()
                        }
                    };
                    if !having_predicates.is_empty() {
                        results.retain(|row| having_predicates.iter().all(|f| f.matches(row)));
                    }
                }

                results.truncate(limit);

                match super::super::response_codec::encode_json_vec(&results) {
                    Ok(payload) => {
                        // Cache the result for future identical queries.
                        if filters.is_empty() && having.is_empty() {
                            let cache_key = aggregate_cache_key(
                                tid,
                                collection,
                                group_by,
                                aggregates,
                                sub_group_by,
                                sub_aggregates,
                            );
                            // Bounded cache: max 256 entries per core.
                            if self.aggregate_cache.len() < 256 {
                                self.aggregate_cache.insert(cache_key, payload.clone());
                            }
                        }
                        self.response_with_payload(task, payload)
                    }
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
