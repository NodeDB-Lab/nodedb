//! Document read and scan handlers: Scan, PointGet, RangeScan, IndexLookup.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_document_scan(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        limit: usize,
        offset: usize,
        sort_keys: &[(String, bool)],
        filters: &[u8],
        distinct: bool,
        projection: &[String],
        computed_columns_bytes: &[u8],
        window_functions_bytes: &[u8],
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            limit,
            offset,
            sort_fields = sort_keys.len(),
            "document scan"
        );

        // Parse window function specs.
        let window_specs: Vec<crate::bridge::window_func::WindowFuncSpec> =
            if window_functions_bytes.is_empty() {
                Vec::new()
            } else {
                zerompk::from_msgpack(window_functions_bytes).unwrap_or_default()
            };

        // Parse computed column expressions.
        let computed_cols: Vec<crate::bridge::expr_eval::ComputedColumn> =
            if computed_columns_bytes.is_empty() {
                Vec::new()
            } else {
                zerompk::from_msgpack(computed_columns_bytes).unwrap_or_default()
            };

        let fetch_limit = (limit + offset).saturating_mul(2).max(1000);

        // Parse filter predicates upfront.
        let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filters) {
                Ok(f) => f,
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "failed to parse scan filters");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("malformed scan filters: {e}"),
                        },
                    );
                }
            }
        };

        // When filters are present, push predicate evaluation into the
        // storage scan. Non-matching documents are never allocated —
        // only decoded for predicate evaluation, then dropped. This
        // avoids O(N) allocation for large collections with selective filters.
        let scan_result = if filter_predicates.is_empty() {
            self.sparse.scan_documents(tid, collection, fetch_limit)
        } else {
            self.sparse
                .scan_documents_filtered(tid, collection, fetch_limit, &|value: &[u8]| {
                    let Some(doc) = super::super::super::doc_format::decode_document(value) else {
                        return false;
                    };
                    filter_predicates.iter().all(|f| f.matches(&doc))
                })
        };

        match scan_result {
            Ok(filtered) => {
                if let Some(ref m) = self.metrics {
                    m.record_document_read();
                }
                let sorted = if sort_keys.is_empty() {
                    filtered
                } else if filtered.len() <= self.query_tuning.sort_run_size {
                    let mut v = filtered;
                    super::sort::sort_rows(&mut v, sort_keys);
                    v
                } else {
                    match self.external_sort(filtered, sort_keys, limit + offset) {
                        Ok(merged) => merged,
                        Err(e) => {
                            warn!(core = self.core_id, error = %e, "external sort failed");
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!("external sort failed: {e}"),
                                },
                            );
                        }
                    }
                };

                // Evaluate window functions (after sort, before dedup/limit).
                let sorted = if !window_specs.is_empty() {
                    let mut rows: Vec<(String, serde_json::Value)> = sorted
                        .into_iter()
                        .map(|(id, val)| {
                            let doc = super::super::super::doc_format::decode_document(&val)
                                .unwrap_or(serde_json::Value::Null);
                            (id, doc)
                        })
                        .collect();
                    crate::bridge::window_func::evaluate_window_functions(&mut rows, &window_specs);
                    // Re-encode back to bytes for the rest of the pipeline.
                    rows.into_iter()
                        .map(|(id, doc)| {
                            let bytes = super::super::super::doc_format::encode_to_msgpack(&doc);
                            (id, bytes)
                        })
                        .collect()
                } else {
                    sorted
                };

                let deduped = if distinct {
                    let mut seen = std::collections::HashSet::new();
                    sorted
                        .into_iter()
                        .filter(|(_, value)| seen.insert(value.clone()))
                        .collect()
                } else {
                    sorted
                };

                let result: Vec<_> = deduped
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .map(|(doc_id, value)| {
                        let data = super::super::super::doc_format::decode_document(&value)
                            .unwrap_or(serde_json::Value::Null);

                        // Apply projection: computed columns or simple column selection.
                        let projected = if !computed_cols.is_empty() {
                            // Evaluate computed expressions against the document.
                            let mut out = serde_json::Map::with_capacity(computed_cols.len());
                            for cc in &computed_cols {
                                out.insert(cc.alias.clone(), cc.expr.eval(&data));
                            }
                            serde_json::Value::Object(out)
                        } else if !projection.is_empty() {
                            if let serde_json::Value::Object(obj) = &data {
                                let mut out = serde_json::Map::with_capacity(projection.len());
                                for col in projection {
                                    if let Some(val) = obj.get(col) {
                                        out.insert(col.clone(), val.clone());
                                    }
                                }
                                serde_json::Value::Object(out)
                            } else {
                                data
                            }
                        } else {
                            data
                        };

                        super::super::super::response_codec::DocumentRow {
                            id: doc_id,
                            data: projected,
                        }
                    })
                    .collect();

                // Stream results in chunks of `stream_chunk_size` rows.
                // Each chunk is sent as a partial response except the last.
                let stream_chunk_size = self.query_tuning.stream_chunk_size;

                if result.len() <= stream_chunk_size {
                    // Small result: send as single response (no streaming overhead).
                    match super::super::super::response_codec::encode(&result) {
                        Ok(payload) => self.response_with_payload(task, payload),
                        Err(e) => self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        ),
                    }
                } else {
                    // Large result: stream in chunks via partial responses.
                    let chunks: Vec<_> = result.chunks(stream_chunk_size).collect();
                    let last_idx = chunks.len().saturating_sub(1);
                    for (i, chunk) in chunks.iter().enumerate() {
                        let is_last = i == last_idx;
                        match super::super::super::response_codec::encode(&chunk.to_vec()) {
                            Ok(payload) => {
                                if is_last {
                                    // Final chunk: return as the function's response.
                                    return self.response_with_payload(task, payload);
                                }
                                // Partial chunk: push directly to response queue.
                                let partial = self.response_partial(task, payload);
                                let _ = self.response_tx.try_push(
                                    crate::bridge::dispatch::BridgeResponse { inner: partial },
                                );
                            }
                            Err(e) => {
                                return self.response_error(
                                    task,
                                    ErrorCode::Internal {
                                        detail: e.to_string(),
                                    },
                                );
                            }
                        }
                    }
                    // All chunks should have been sent and returned above.
                    // If we reach here, the streaming loop exited unexpectedly.
                    tracing::debug!(
                        core = self.core_id,
                        "streaming loop exited without final response"
                    );
                    self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "streaming response incomplete".into(),
                        },
                    )
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
