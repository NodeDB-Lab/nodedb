//! Document mutation, aggregate, and join handlers for the Data Plane CoreLoop.
//!
//! Extracted from `execute.rs` to keep that file under the 500-line limit.
//! Handles `PhysicalPlan::Aggregate`, `PhysicalPlan::PointUpdate`, and
//! `PhysicalPlan::HashJoin`.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};

use super::core_loop::CoreLoop;
use super::scan_filter::{ScanFilter, compute_aggregate};
use super::task::ExecutionTask;

impl CoreLoop {
    /// Execute a GROUP BY aggregate plan.
    ///
    /// Scans all documents in `collection`, applies `filters`, groups by
    /// `group_by`, computes each aggregate in `aggregates`, then truncates
    /// to `limit` rows.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_aggregate(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        group_by: &[String],
        aggregates: &[(String, String)],
        filters: &[u8],
        limit: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, group_fields = group_by.len(), aggs = aggregates.len(), "aggregate");

        // Scan all documents.
        let fetch_limit = limit.max(10000);
        match self.sparse.scan_documents(tid, collection, fetch_limit) {
            Ok(docs) => {
                let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
                    Vec::new()
                } else {
                    serde_json::from_slice(filters).unwrap_or_default()
                };

                let filtered: Vec<_> = if filter_predicates.is_empty() {
                    docs
                } else {
                    docs.into_iter()
                        .filter(|(_, value)| {
                            let doc: serde_json::Value = match serde_json::from_slice(value) {
                                Ok(v) => v,
                                Err(_) => return false,
                            };
                            filter_predicates.iter().all(|f| f.matches(&doc))
                        })
                        .collect()
                };

                // Group documents by composite key.
                let mut groups: std::collections::HashMap<String, Vec<serde_json::Value>> =
                    std::collections::HashMap::new();

                for (_, value) in &filtered {
                    let doc: serde_json::Value = match serde_json::from_slice(value) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let key = if group_by.is_empty() {
                        "__all__".to_string()
                    } else {
                        // Composite group key: serialize field values as a JSON array
                        // for collision-free grouping (avoids separator ambiguity).
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
                }

                // Compute aggregates for each group.
                let mut results: Vec<serde_json::Value> = Vec::new();
                for (group_key, group_docs) in &groups {
                    let mut row = serde_json::Map::new();

                    // Reconstruct GROUP BY fields from the JSON array key.
                    if !group_by.is_empty() {
                        if let Ok(parts) = serde_json::from_str::<Vec<serde_json::Value>>(group_key)
                        {
                            for (i, field) in group_by.iter().enumerate() {
                                let val = parts.get(i).cloned().unwrap_or(serde_json::Value::Null);
                                row.insert(field.clone(), val);
                            }
                        }
                    }

                    for (op, field) in aggregates {
                        let agg_key = format!("{op}_{field}").replace('*', "all");
                        let val = compute_aggregate(op, field, group_docs);
                        row.insert(agg_key, val);
                    }

                    results.push(serde_json::Value::Object(row));
                }

                // Apply limit.
                results.truncate(limit);

                match serde_json::to_vec(&results) {
                    Ok(payload) => self.response_with_payload(task, payload),
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

    /// Execute a HashJoin: scan both collections, build a hash index on the
    /// right side, then probe with the left side, merging matching rows.
    ///
    /// The scan limit is derived from `limit` to avoid over-fetching:
    /// `scan_limit = (limit * 10).min(50000)`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute_hash_join(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        left_collection: &str,
        right_collection: &str,
        on: &[(String, String)],
        join_type: &str,
        limit: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %left_collection,
            %right_collection,
            keys = on.len(),
            %join_type,
            "hash join"
        );

        // Derive a proportional scan limit capped at 50 000.
        let scan_limit = (limit * 10).min(50000);

        // Scan both collections.
        let left_docs = match self.sparse.scan_documents(tid, left_collection, scan_limit) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let right_docs = match self
            .sparse
            .scan_documents(tid, right_collection, scan_limit)
        {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Build composite key extraction closure using ALL join keys.
        let extract_key = |doc: &serde_json::Value, keys: &[&str], doc_id: &str| -> String {
            if keys.len() == 1 {
                doc.get(keys[0])
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_else(|| doc_id.to_string())
            } else {
                let parts: Vec<serde_json::Value> = keys
                    .iter()
                    .map(|k| doc.get(*k).cloned().unwrap_or(serde_json::Value::Null))
                    .collect();
                serde_json::to_string(&parts).unwrap_or_else(|_| "[]".into())
            }
        };

        let right_keys: Vec<&str> = on.iter().map(|(_, r)| r.as_str()).collect();
        let left_keys: Vec<&str> = on.iter().map(|(l, _)| l.as_str()).collect();

        // Build hash map on right side using composite key.
        let mut right_index: std::collections::HashMap<String, Vec<serde_json::Value>> =
            std::collections::HashMap::new();
        let mut right_matched: std::collections::HashSet<String> = std::collections::HashSet::new();

        for (doc_id, value) in &right_docs {
            let doc: serde_json::Value = match serde_json::from_slice(value) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let key_val = extract_key(&doc, &right_keys, doc_id);
            right_index.entry(key_val).or_default().push(doc);
        }

        let merge_docs = |left_doc: &serde_json::Value,
                          right_doc: Option<&serde_json::Value>,
                          left_coll: &str,
                          right_coll: &str|
         -> serde_json::Value {
            let mut merged = serde_json::Map::new();
            if let Some(obj) = left_doc.as_object() {
                for (k, v) in obj {
                    merged.insert(format!("{left_coll}.{k}"), v.clone());
                }
            }
            if let Some(right) = right_doc {
                if let Some(obj) = right.as_object() {
                    for (k, v) in obj {
                        merged.insert(format!("{right_coll}.{k}"), v.clone());
                    }
                }
            }
            serde_json::Value::Object(merged)
        };

        let is_left = join_type == "left" || join_type == "full";
        let is_right = join_type == "right" || join_type == "full";

        // Probe with left side.
        let mut results = Vec::new();
        for (doc_id, value) in &left_docs {
            if results.len() >= limit {
                break;
            }
            let left_doc: serde_json::Value = match serde_json::from_slice(value) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let probe_key = extract_key(&left_doc, &left_keys, doc_id);

            if let Some(right_matches) = right_index.get(&probe_key) {
                if is_right {
                    right_matched.insert(probe_key.clone());
                }
                for right_doc in right_matches {
                    if results.len() >= limit {
                        break;
                    }
                    results.push(merge_docs(
                        &left_doc,
                        Some(right_doc),
                        left_collection,
                        right_collection,
                    ));
                }
            } else if is_left {
                // LEFT/FULL: emit left row with NULL right columns.
                results.push(merge_docs(
                    &left_doc,
                    None,
                    left_collection,
                    right_collection,
                ));
            }
            // INNER: no match = no output (default).
        }

        // RIGHT/FULL: emit unmatched right rows with NULL left columns.
        if is_right {
            for (key, right_docs_group) in &right_index {
                if results.len() >= limit {
                    break;
                }
                if right_matched.contains(key) {
                    continue;
                }
                for right_doc in right_docs_group {
                    if results.len() >= limit {
                        break;
                    }
                    // Swap: right doc is the "present" side, left is NULL.
                    let mut merged = serde_json::Map::new();
                    // Left columns are NULL (omitted — absent keys = NULL in JSON).
                    if let Some(obj) = right_doc.as_object() {
                        for (k, v) in obj {
                            merged.insert(format!("{right_collection}.{k}"), v.clone());
                        }
                    }
                    results.push(serde_json::Value::Object(merged));
                }
            }
        }

        match serde_json::to_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Execute a PointUpdate: read-modify-write on a JSON document.
    pub(super) fn execute_point_update(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        updates: &[(String, Vec<u8>)],
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, fields = updates.len(), "point update");
        match self.sparse.get(tid, collection, document_id) {
            Ok(Some(current_bytes)) => {
                let mut doc: serde_json::Value = match serde_json::from_slice(&current_bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("failed to parse document for update: {e}"),
                            },
                        );
                    }
                };
                if let Some(obj) = doc.as_object_mut() {
                    for (field, value_bytes) in updates {
                        let val: serde_json::Value = match serde_json::from_slice(value_bytes) {
                            Ok(v) => v,
                            Err(_) => serde_json::Value::String(
                                String::from_utf8_lossy(value_bytes).into_owned(),
                            ),
                        };
                        obj.insert(field.clone(), val);
                    }
                }
                match serde_json::to_vec(&doc) {
                    Ok(updated_bytes) => {
                        match self
                            .sparse
                            .put(tid, collection, document_id, &updated_bytes)
                        {
                            Ok(()) => self.response_ok(task),
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
                            detail: format!("failed to serialize updated document: {e}"),
                        },
                    ),
                }
            }
            Ok(None) => self.response_error(task, ErrorCode::NotFound),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
