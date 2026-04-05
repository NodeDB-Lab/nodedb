//! HashJoin and NestedLoopJoin execution handlers.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Merge a left and optional right document into a single JSON object,
/// prefixing each key with its source collection name.
fn merge_join_docs(
    left_doc: &serde_json::Value,
    right_doc: Option<&serde_json::Value>,
    left_collection: &str,
    right_collection: &str,
) -> serde_json::Value {
    let mut merged = serde_json::Map::new();
    if let Some(obj) = left_doc.as_object() {
        for (k, v) in obj {
            merged.insert(format!("{left_collection}.{k}"), v.clone());
        }
    }
    if let Some(right) = right_doc
        && let Some(obj) = right.as_object()
    {
        for (k, v) in obj {
            merged.insert(format!("{right_collection}.{k}"), v.clone());
        }
    }
    serde_json::Value::Object(merged)
}

/// Extract a join key from a document given the key field names.
///
/// For single-field keys, returns the field value as a string.
/// For composite keys, returns a JSON array string of all field values.
fn extract_join_key(doc: &serde_json::Value, keys: &[&str], doc_id: &str) -> String {
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
}

/// Probe a hash index with probe-side documents and produce join results.
///
/// Shared logic for hash join and broadcast join: builds results by probing
/// a pre-built hash index with each probe-side document. Handles inner,
/// left, right, and full outer join types.
fn probe_hash_index(
    probe_docs: &[(String, Vec<u8>)],
    index: &std::collections::HashMap<String, Vec<serde_json::Value>>,
    probe_keys: &[&str],
    join_type: &str,
    limit: usize,
    probe_collection: &str,
    index_collection: &str,
) -> Vec<serde_json::Value> {
    let is_left = join_type == "left" || join_type == "full";
    let is_right = join_type == "right" || join_type == "full";
    let is_semi = join_type == "semi";
    let is_anti = join_type == "anti";

    let mut index_matched: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut results = Vec::new();

    for (doc_id, value) in probe_docs {
        if results.len() >= limit {
            break;
        }
        let Some(probe_doc) = super::super::doc_format::decode_document(value) else {
            continue;
        };
        let key = extract_join_key(&probe_doc, probe_keys, doc_id);

        if let Some(matches) = index.get(&key) {
            if is_semi {
                // Semi: emit probe row (no merge), stop at first match.
                results.push(probe_doc);
            } else if is_anti {
                // Anti: skip — this row HAS a match, so exclude it.
            } else {
                if is_right {
                    index_matched.insert(key.clone());
                }
                for matched_doc in matches {
                    if results.len() >= limit {
                        break;
                    }
                    results.push(merge_join_docs(
                        &probe_doc,
                        Some(matched_doc),
                        probe_collection,
                        index_collection,
                    ));
                }
            }
        } else if is_anti {
            // Anti: emit probe row — this row has NO match.
            results.push(probe_doc);
        } else if is_left {
            results.push(merge_join_docs(
                &probe_doc,
                None,
                probe_collection,
                index_collection,
            ));
        }
    }

    // RIGHT/FULL: emit unmatched index-side rows.
    if is_right {
        for (key, docs_group) in index {
            if results.len() >= limit {
                break;
            }
            if index_matched.contains(key) {
                continue;
            }
            for doc in docs_group {
                if results.len() >= limit {
                    break;
                }
                results.push(merge_right_only(doc, index_collection));
            }
        }
    }

    results
}

/// Merge only the right document (left is NULL for right-outer unmatched rows).
fn merge_right_only(right_doc: &serde_json::Value, right_collection: &str) -> serde_json::Value {
    let mut merged = serde_json::Map::new();
    if let Some(obj) = right_doc.as_object() {
        for (k, v) in obj {
            merged.insert(format!("{right_collection}.{k}"), v.clone());
        }
    }
    serde_json::Value::Object(merged)
}

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_hash_join(
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

        let scan_limit = (limit * 10).min(50000);

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

        let right_keys: Vec<&str> = on.iter().map(|(_, r)| r.as_str()).collect();
        let left_keys: Vec<&str> = on.iter().map(|(l, _)| l.as_str()).collect();

        // Build hash index on the right (build) side.
        let mut right_index: std::collections::HashMap<String, Vec<serde_json::Value>> =
            std::collections::HashMap::new();
        for (doc_id, value) in &right_docs {
            let Some(doc) = super::super::doc_format::decode_document(value) else {
                continue;
            };
            let key_val = extract_join_key(&doc, &right_keys, doc_id);
            right_index.entry(key_val).or_default().push(doc);
        }

        // Probe the hash index with left (probe) side.
        let results = probe_hash_index(
            &left_docs,
            &right_index,
            &left_keys,
            join_type,
            limit,
            left_collection,
            right_collection,
        );

        match super::super::response_codec::encode_json_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Broadcast join: the small side is pre-serialized by the Control Plane
    /// and included directly in the plan (`broadcast_data`). Each core builds
    /// a local hash map from the broadcast data and probes with its local
    /// large-side scan. Avoids a second storage scan for the small side.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_broadcast_join(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        large_collection: &str,
        broadcast_data: &[u8],
        on: &[(String, String)],
        join_type: &str,
        limit: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %large_collection,
            broadcast_bytes = broadcast_data.len(),
            keys = on.len(),
            %join_type,
            "broadcast join"
        );

        // Deserialize broadcast (small) side from MessagePack Vec<(String, Vec<u8>)>.
        let small_docs_raw: Vec<(String, Vec<u8>)> = match zerompk::from_msgpack(broadcast_data) {
            Ok(v) => v,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("broadcast_data deserialization: {e}"),
                    },
                );
            }
        };

        let scan_limit = (limit * 10).min(50000);
        let large_docs = match self
            .sparse
            .scan_documents(tid, large_collection, scan_limit)
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

        // The `on` pairs are `(large_field, small_field)`.
        let large_keys: Vec<&str> = on.iter().map(|(l, _)| l.as_str()).collect();
        let small_keys: Vec<&str> = on.iter().map(|(_, s)| s.as_str()).collect();

        // Build hash index on the small (broadcast) side.
        let mut small_index: std::collections::HashMap<String, Vec<serde_json::Value>> =
            std::collections::HashMap::new();
        for (doc_id, value) in &small_docs_raw {
            let Some(doc) = super::super::doc_format::decode_document(value) else {
                continue;
            };
            let key_val = extract_join_key(&doc, &small_keys, doc_id);
            small_index.entry(key_val).or_default().push(doc);
        }

        // Probe the hash index with large (scanned) side.
        let small_collection = "broadcast";
        let results = probe_hash_index(
            &large_docs,
            &small_index,
            &large_keys,
            join_type,
            limit,
            large_collection,
            small_collection,
        );

        match super::super::response_codec::encode_json_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Nested loop join: O(N×M) fallback for non-equi joins, theta joins,
    /// and cross joins where hash join can't operate.
    ///
    /// For each left row, iterates all right rows and evaluates the join
    /// condition. Supports inner/left/right/full join types.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_nested_loop_join(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        left_collection: &str,
        right_collection: &str,
        condition: &[u8],
        join_type: &str,
        limit: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %left_collection,
            %right_collection,
            %join_type,
            "nested loop join"
        );

        let scan_limit = (limit * 10).min(50000);

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

        // Parse join condition predicates.
        let predicates: Vec<crate::bridge::scan_filter::ScanFilter> = if condition.is_empty() {
            Vec::new() // Cross join — no condition.
        } else {
            match zerompk::from_msgpack(condition) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(core = self.core_id, error = %e, "malformed join condition");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("join condition deserialization: {e}"),
                        },
                    );
                }
            }
        };

        let is_left = join_type == "left" || join_type == "full";
        let is_right = join_type == "right" || join_type == "full";

        // Decode all right docs upfront.
        let right_decoded: Vec<serde_json::Value> = right_docs
            .iter()
            .filter_map(|(_, v)| super::super::doc_format::decode_document(v))
            .collect();

        let mut right_matched: Vec<bool> = vec![false; right_decoded.len()];
        let mut results = Vec::new();

        for (_, left_bytes) in &left_docs {
            if results.len() >= limit {
                break;
            }
            let Some(left_doc) = super::super::doc_format::decode_document(left_bytes) else {
                continue;
            };

            let mut left_matched = false;
            for (ri, right_doc) in right_decoded.iter().enumerate() {
                if results.len() >= limit {
                    break;
                }

                // Evaluate condition against merged row.
                let passes = if predicates.is_empty() {
                    true // Cross join.
                } else {
                    let merged = merge_join_docs(
                        &left_doc,
                        Some(right_doc),
                        left_collection,
                        right_collection,
                    );
                    predicates.iter().all(|p| p.matches(&merged))
                };

                if passes {
                    left_matched = true;
                    right_matched[ri] = true;
                    results.push(merge_join_docs(
                        &left_doc,
                        Some(right_doc),
                        left_collection,
                        right_collection,
                    ));
                }
            }

            // LEFT/FULL: emit unmatched left rows.
            if !left_matched && is_left {
                results.push(merge_join_docs(
                    &left_doc,
                    None,
                    left_collection,
                    right_collection,
                ));
            }
        }

        // RIGHT/FULL: emit unmatched right rows.
        if is_right {
            for (ri, right_doc) in right_decoded.iter().enumerate() {
                if results.len() >= limit {
                    break;
                }
                if !right_matched[ri] {
                    results.push(merge_right_only(right_doc, right_collection));
                }
            }
        }

        match super::super::response_codec::encode_json_vec(&results) {
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
