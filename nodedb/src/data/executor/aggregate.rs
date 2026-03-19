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
        group_by: &str,
        aggregates: &[(String, String)],
        filters: &[u8],
        limit: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, %group_by, aggs = aggregates.len(), "aggregate");

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

                // Group documents.
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
                        doc.get(group_by)
                            .map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .unwrap_or_else(|| "null".to_string())
                    };
                    groups.entry(key).or_default().push(doc);
                }

                // Compute aggregates for each group.
                let mut results: Vec<serde_json::Value> = Vec::new();
                for (group_key, group_docs) in &groups {
                    let mut row = serde_json::Map::new();
                    if !group_by.is_empty() {
                        row.insert(
                            group_by.to_string(),
                            serde_json::Value::String(group_key.clone()),
                        );
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
        limit: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %left_collection,
            %right_collection,
            keys = on.len(),
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

        // Build hash map on right side (first join key).
        let right_key = on.first().map(|(_, r)| r.as_str()).unwrap_or("id");
        let left_key = on.first().map(|(l, _)| l.as_str()).unwrap_or("id");

        let mut right_index: std::collections::HashMap<String, Vec<serde_json::Value>> =
            std::collections::HashMap::new();

        for (doc_id, value) in &right_docs {
            let doc: serde_json::Value = match serde_json::from_slice(value) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let key_val = doc
                .get(right_key)
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_else(|| doc_id.clone());
            right_index.entry(key_val).or_default().push(doc);
        }

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
            let probe_key = left_doc
                .get(left_key)
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_else(|| doc_id.clone());

            if let Some(right_matches) = right_index.get(&probe_key) {
                for right_doc in right_matches {
                    if results.len() >= limit {
                        break;
                    }
                    // Merge left + right documents.
                    let mut merged = serde_json::Map::new();
                    if let Some(obj) = left_doc.as_object() {
                        for (k, v) in obj {
                            merged.insert(format!("{}.{}", left_collection, k), v.clone());
                        }
                    }
                    if let Some(obj) = right_doc.as_object() {
                        for (k, v) in obj {
                            merged.insert(format!("{}.{}", right_collection, k), v.clone());
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
