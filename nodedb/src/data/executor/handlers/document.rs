//! Document operation handlers: DocumentBatchInsert, DocumentScan,
//! RegisterDocumentCollection, DocumentIndexLookup, and DropDocumentIndex.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{BufReader, BufWriter, Read as _, Write as _};

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::{ScanFilter, compare_json_values};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_document_batch_insert(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        documents: &[(String, Vec<u8>)],
    ) -> Response {
        debug!(core = self.core_id, %collection, count = documents.len(), "document batch insert");
        let converted: Vec<(String, Vec<u8>)> = documents
            .iter()
            .map(|(id, val)| (id.clone(), super::super::doc_format::json_to_msgpack(val)))
            .collect();
        let refs: Vec<(&str, &[u8])> = converted
            .iter()
            .map(|(id, val)| (id.as_str(), val.as_slice()))
            .collect();
        match self.sparse.batch_put(tid, collection, &refs) {
            Ok(()) => {
                // Auto-index text fields for full-text search (same as PointPut).
                // Also extract secondary indexes for any registered collection config.
                let config_key = format!("{tid}:{collection}");
                let index_paths: Vec<crate::engine::document::store::IndexPath> = self
                    .doc_configs
                    .get(&config_key)
                    .map(|c| c.index_paths.clone())
                    .unwrap_or_default();
                for (doc_id, val) in documents {
                    if let Some(doc) = super::super::doc_format::decode_document(val) {
                        // Full-text inverted index.
                        if let Some(obj) = doc.as_object() {
                            let text_content: String = obj
                                .values()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                                .join(" ");
                            if !text_content.is_empty() {
                                let _ =
                                    self.inverted
                                        .index_document(collection, doc_id, &text_content);
                            }
                        }

                        // Secondary index extraction.
                        self.apply_secondary_indexes(tid, collection, &doc, doc_id, &index_paths);
                    }
                }

                if let Some(ref m) = self.metrics {
                    m.record_document_insert();
                }
                match super::super::response_codec::encode_count("inserted", documents.len()) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
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
                rmp_serde::from_slice(window_functions_bytes).unwrap_or_default()
            };

        // Parse computed column expressions.
        let computed_cols: Vec<crate::bridge::expr_eval::ComputedColumn> =
            if computed_columns_bytes.is_empty() {
                Vec::new()
            } else {
                rmp_serde::from_slice(computed_columns_bytes).unwrap_or_default()
            };

        let fetch_limit = (limit + offset).saturating_mul(2).max(1000);

        // Parse filter predicates upfront.
        let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
            match rmp_serde::from_slice(filters) {
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
                    let Some(doc) = super::super::doc_format::decode_document(value) else {
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
                    sort_rows(&mut v, sort_keys);
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
                            let doc = super::super::doc_format::decode_document(&val)
                                .unwrap_or(serde_json::Value::Null);
                            (id, doc)
                        })
                        .collect();
                    crate::bridge::window_func::evaluate_window_functions(&mut rows, &window_specs);
                    // Re-encode back to bytes for the rest of the pipeline.
                    rows.into_iter()
                        .map(|(id, doc)| {
                            let bytes = super::super::doc_format::encode_to_msgpack(&doc);
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
                        let data = super::super::doc_format::decode_document(&value)
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

                        super::super::response_codec::DocumentRow {
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
                    match super::super::response_codec::encode(&result) {
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
                        match super::super::response_codec::encode(chunk) {
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

    /// Register a document collection's secondary index configuration.
    ///
    /// Stores the `CollectionConfig` in `self.doc_configs` so that subsequent
    /// `PointPut` and `DocumentBatchInsert` operations extract and write secondary
    /// index entries automatically.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_register_document_collection(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        index_paths: &[String],
        crdt_enabled: bool,
        storage_mode: &crate::bridge::physical_plan::StorageMode,
        accounting: &crate::bridge::physical_plan::AccountingOptions,
    ) -> Response {
        let mode_label = match storage_mode {
            crate::bridge::physical_plan::StorageMode::Schemaless => "schemaless",
            crate::bridge::physical_plan::StorageMode::Strict { .. } => "strict",
        };
        debug!(
            core = self.core_id,
            %collection,
            index_count = index_paths.len(),
            crdt_enabled,
            storage_mode = mode_label,
            append_only = accounting.append_only,
            hash_chain = accounting.hash_chain,
            balanced = accounting.balanced.is_some(),
            "register document collection"
        );

        let mut config = crate::engine::document::store::CollectionConfig::new(collection);
        config.crdt_enabled = crdt_enabled;
        config.storage_mode = storage_mode.clone();
        config.accounting = accounting.clone();
        for path in index_paths {
            config = config.with_index(path);
        }

        let config_key = format!("{tid}:{collection}");
        self.doc_configs.insert(config_key, config);

        self.response_ok(task)
    }

    /// Execute a secondary index lookup: find all doc IDs where `path = value`.
    ///
    /// Delegates to `SparseEngine::range_scan` via a temporary `DocumentEngine`.
    /// Returns a JSON array of document IDs as the response payload.
    pub(in crate::data::executor) fn execute_document_index_lookup(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        path: &str,
        value: &str,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %path,
            %value,
            "document index lookup"
        );

        let doc_engine = crate::engine::document::store::DocumentEngine::new(&self.sparse, tid);
        match doc_engine.index_lookup(collection, path, value) {
            Ok(doc_ids) => {
                let payload = serde_json::json!(doc_ids);
                match serde_json::to_vec(&payload) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("index lookup encode: {e}"),
                        },
                    ),
                }
            }
            Err(e) => self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Drop all secondary index entries for a field across the entire collection.
    ///
    /// Calls `SparseEngine::delete_index_entries_for_field` directly.
    /// Returns `{"removed": N}` as the response payload.
    pub(in crate::data::executor) fn execute_drop_document_index(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %field,
            "drop document index"
        );

        match self
            .sparse
            .delete_index_entries_for_field(tid, collection, field)
        {
            Ok(removed) => match super::super::response_codec::encode_count("removed", removed) {
                Ok(bytes) => self.response_with_payload(task, bytes),
                Err(e) => self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("drop index encode: {e}"),
                    },
                ),
            },
            Err(e) => self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// External sort: split filtered rows into sorted runs, spill each run
    /// to a temp file, then k-way merge to produce the final sorted output.
    fn external_sort(
        &self,
        rows: Vec<(String, Vec<u8>)>,
        sort_keys: &[(String, bool)],
        output_limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        // Spill directory for temporary sort run files. Temp files are
        // auto-deleted on Drop; the directory persists but is cleaned up
        // on the next external_sort call or server restart.
        let spill_dir = self
            .data_dir
            .join(format!("sort-spill/core-{}", self.core_id));
        std::fs::create_dir_all(&spill_dir).map_err(|e| crate::Error::Storage {
            engine: "sort".into(),
            detail: format!("failed to create sort spill dir: {e}"),
        })?;

        let total_rows = rows.len();

        let mut run_files = Vec::new();
        for chunk in rows.chunks(self.query_tuning.sort_run_size) {
            let mut run: Vec<(String, Vec<u8>)> = chunk.to_vec();
            sort_rows(&mut run, sort_keys);

            let file = tempfile::tempfile_in(&spill_dir).map_err(|e| crate::Error::Storage {
                engine: "sort".into(),
                detail: format!("failed to create sort temp file: {e}"),
            })?;
            let mut writer = BufWriter::new(file);

            let count = run.len() as u32;
            writer
                .write_all(&count.to_le_bytes())
                .map_err(|e| crate::Error::Storage {
                    engine: "sort".into(),
                    detail: format!("sort spill write: {e}"),
                })?;
            for (id, val) in &run {
                let id_bytes = id.as_bytes();
                writer
                    .write_all(&(id_bytes.len() as u32).to_le_bytes())
                    .map_err(|e| crate::Error::Storage {
                        engine: "sort".into(),
                        detail: format!("sort spill write: {e}"),
                    })?;
                writer
                    .write_all(id_bytes)
                    .map_err(|e| crate::Error::Storage {
                        engine: "sort".into(),
                        detail: format!("sort spill write: {e}"),
                    })?;
                writer
                    .write_all(&(val.len() as u32).to_le_bytes())
                    .map_err(|e| crate::Error::Storage {
                        engine: "sort".into(),
                        detail: format!("sort spill write: {e}"),
                    })?;
                writer.write_all(val).map_err(|e| crate::Error::Storage {
                    engine: "sort".into(),
                    detail: format!("sort spill write: {e}"),
                })?;
            }
            writer.flush().map_err(|e| crate::Error::Storage {
                engine: "sort".into(),
                detail: format!("sort spill flush: {e}"),
            })?;

            let mut file = writer.into_inner().map_err(|e| crate::Error::Storage {
                engine: "sort".into(),
                detail: format!("sort spill into_inner: {e}"),
            })?;
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(0))
                .map_err(|e| crate::Error::Storage {
                    engine: "sort".into(),
                    detail: format!("sort spill seek: {e}"),
                })?;

            run_files.push(file);
        }

        debug!(
            core = self.core_id,
            runs = run_files.len(),
            total_rows,
            "external sort: spilled runs"
        );

        let mut readers: Vec<RunReader> = run_files
            .into_iter()
            .enumerate()
            .filter_map(|(idx, file)| RunReader::new(file, idx).ok())
            .collect();

        let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();
        for reader in &mut readers {
            if let Some(row) = reader.next_row() {
                heap.push(Reverse(MergeEntry {
                    row,
                    run_idx: reader.run_idx,
                    sort_keys: sort_keys.to_vec(),
                }));
            }
        }

        let mut result = Vec::with_capacity(output_limit.min(total_rows));
        while let Some(Reverse(entry)) = heap.pop() {
            result.push(entry.row);
            if result.len() >= output_limit {
                break;
            }
            if let Some(next_row) = readers[entry.run_idx].next_row() {
                heap.push(Reverse(MergeEntry {
                    row: next_row,
                    run_idx: entry.run_idx,
                    sort_keys: sort_keys.to_vec(),
                }));
            }
        }

        Ok(result)
    }
}

/// Compare two JSON documents by a list of sort keys.
///
/// Used by both in-memory sort and external merge sort to ensure
/// consistent ordering across all code paths.
fn compare_docs_by_keys(
    a_doc: &serde_json::Value,
    b_doc: &serde_json::Value,
    sort_keys: &[(String, bool)],
) -> std::cmp::Ordering {
    for (field, asc) in sort_keys {
        let a_val = a_doc.get(field.as_str());
        let b_val = b_doc.get(field.as_str());
        let cmp = compare_json_values(a_val, b_val);
        let ordered = if *asc { cmp } else { cmp.reverse() };
        if ordered != std::cmp::Ordering::Equal {
            return ordered;
        }
    }
    std::cmp::Ordering::Equal
}

fn sort_rows(rows: &mut [(String, Vec<u8>)], sort_keys: &[(String, bool)]) {
    rows.sort_by(|(_, a_bytes), (_, b_bytes)| {
        let a_doc =
            super::super::doc_format::decode_document(a_bytes).unwrap_or(serde_json::Value::Null);
        let b_doc =
            super::super::doc_format::decode_document(b_bytes).unwrap_or(serde_json::Value::Null);
        compare_docs_by_keys(&a_doc, &b_doc, sort_keys)
    });
}

struct RunReader {
    reader: BufReader<std::fs::File>,
    remaining: u32,
    run_idx: usize,
}

impl RunReader {
    fn new(file: std::fs::File, run_idx: usize) -> crate::Result<Self> {
        let mut reader = BufReader::new(file);
        let mut buf4 = [0u8; 4];
        reader
            .read_exact(&mut buf4)
            .map_err(|e| crate::Error::Storage {
                engine: "sort".into(),
                detail: format!("run reader init: {e}"),
            })?;
        let count = u32::from_le_bytes(buf4);
        Ok(Self {
            reader,
            remaining: count,
            run_idx,
        })
    }

    fn next_row(&mut self) -> Option<(String, Vec<u8>)> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;

        let mut buf4 = [0u8; 4];

        if self.reader.read_exact(&mut buf4).is_err() {
            return None;
        }
        let id_len = u32::from_le_bytes(buf4) as usize;
        let mut id_buf = vec![0u8; id_len];
        if self.reader.read_exact(&mut id_buf).is_err() {
            return None;
        }
        let id = String::from_utf8(id_buf).unwrap_or_default();

        if self.reader.read_exact(&mut buf4).is_err() {
            return None;
        }
        let val_len = u32::from_le_bytes(buf4) as usize;
        let mut val_buf = vec![0u8; val_len];
        if self.reader.read_exact(&mut val_buf).is_err() {
            return None;
        }

        Some((id, val_buf))
    }
}

struct MergeEntry {
    row: (String, Vec<u8>),
    run_idx: usize,
    sort_keys: Vec<(String, bool)>,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let a_doc = super::super::doc_format::decode_document(&self.row.1)
            .unwrap_or(serde_json::Value::Null);
        let b_doc = super::super::doc_format::decode_document(&other.row.1)
            .unwrap_or(serde_json::Value::Null);
        compare_docs_by_keys(&a_doc, &b_doc, &self.sort_keys)
    }
}
