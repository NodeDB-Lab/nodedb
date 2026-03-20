//! Document operation handlers: DocumentScan, DocumentBatchInsert, Aggregate.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{BufReader, BufWriter, Read as _, Write as _};

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::scan_filter::{ScanFilter, compare_json_values, compute_aggregate};
use crate::data::executor::task::ExecutionTask;

/// Maximum rows to sort in memory per run. If the filtered result set
/// exceeds this, it is split into sorted runs written to temp files,
/// then k-way merged. 100K rows x ~1 KiB avg = ~100 MiB RAM per run.
const SORT_RUN_SIZE: usize = 100_000;

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
                match super::super::response_codec::encode_count("inserted", documents.len()) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(task, ErrorCode::Internal { detail: e }),
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
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            limit,
            offset,
            sort_fields = sort_keys.len(),
            "document scan"
        );

        let fetch_limit = (limit + offset).saturating_mul(2).max(1000);
        match self.sparse.scan_documents(tid, collection, fetch_limit) {
            Ok(docs) => {
                let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
                    Vec::new()
                } else {
                    match serde_json::from_slice(filters) {
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

                let filtered: Vec<_> = if filter_predicates.is_empty() {
                    docs
                } else {
                    docs.into_iter()
                        .filter(|(_, value)| {
                            let Some(doc) = super::super::doc_format::decode_document(value) else {
                                return false;
                            };
                            filter_predicates.iter().all(|f| f.matches(&doc))
                        })
                        .collect()
                };

                let sorted = if sort_keys.is_empty() {
                    filtered
                } else if filtered.len() <= SORT_RUN_SIZE {
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

                        // Apply column projection: return only requested fields.
                        let projected = if projection.is_empty() {
                            data
                        } else if let serde_json::Value::Object(obj) = &data {
                            let mut out = serde_json::Map::with_capacity(projection.len());
                            for col in projection {
                                if let Some(val) = obj.get(col) {
                                    out.insert(col.clone(), val.clone());
                                }
                            }
                            serde_json::Value::Object(out)
                        } else {
                            data
                        };

                        super::super::response_codec::DocumentRow {
                            id: doc_id,
                            data: projected,
                        }
                    })
                    .collect();

                match super::super::response_codec::encode(&result) {
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

    /// External sort: split filtered rows into sorted runs, spill each run
    /// to a temp file, then k-way merge to produce the final sorted output.
    fn external_sort(
        &self,
        rows: Vec<(String, Vec<u8>)>,
        sort_keys: &[(String, bool)],
        output_limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, String> {
        let spill_dir = self
            .data_dir
            .join(format!("sort-spill/core-{}", self.core_id));
        std::fs::create_dir_all(&spill_dir)
            .map_err(|e| format!("failed to create sort spill dir: {e}"))?;

        let total_rows = rows.len();

        let mut run_files = Vec::new();
        for chunk in rows.chunks(SORT_RUN_SIZE) {
            let mut run: Vec<(String, Vec<u8>)> = chunk.to_vec();
            sort_rows(&mut run, sort_keys);

            let file = tempfile::tempfile_in(&spill_dir)
                .map_err(|e| format!("failed to create sort temp file: {e}"))?;
            let mut writer = BufWriter::new(file);

            let count = run.len() as u32;
            writer
                .write_all(&count.to_le_bytes())
                .map_err(|e| format!("sort spill write: {e}"))?;
            for (id, val) in &run {
                let id_bytes = id.as_bytes();
                writer
                    .write_all(&(id_bytes.len() as u32).to_le_bytes())
                    .map_err(|e| format!("sort spill write: {e}"))?;
                writer
                    .write_all(id_bytes)
                    .map_err(|e| format!("sort spill write: {e}"))?;
                writer
                    .write_all(&(val.len() as u32).to_le_bytes())
                    .map_err(|e| format!("sort spill write: {e}"))?;
                writer
                    .write_all(val)
                    .map_err(|e| format!("sort spill write: {e}"))?;
            }
            writer
                .flush()
                .map_err(|e| format!("sort spill flush: {e}"))?;

            let mut file = writer
                .into_inner()
                .map_err(|e| format!("sort spill into_inner: {e}"))?;
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(0))
                .map_err(|e| format!("sort spill seek: {e}"))?;

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
    ) -> Response {
        debug!(core = self.core_id, %collection, group_fields = group_by.len(), aggs = aggregates.len(), "aggregate");

        // Aggregates must scan all matching documents for correct results.
        // Cap at 10M to prevent OOM on unbounded collections.
        const AGGREGATE_SCAN_CAP: usize = 10_000_000;
        let scan_limit = AGGREGATE_SCAN_CAP;
        match self.sparse.scan_documents(tid, collection, scan_limit) {
            Ok(docs) => {
                let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
                    Vec::new()
                } else {
                    serde_json::from_slice(filters).unwrap_or_default()
                };

                let mut groups: std::collections::HashMap<String, Vec<serde_json::Value>> =
                    std::collections::HashMap::new();

                for (_, value) in &docs {
                    let Some(doc) = super::super::doc_format::decode_document(value) else {
                        continue;
                    };

                    if !filter_predicates.is_empty()
                        && !filter_predicates.iter().all(|f| f.matches(&doc))
                    {
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
                }

                let mut results: Vec<serde_json::Value> = Vec::new();
                for (group_key, group_docs) in &groups {
                    let mut row = serde_json::Map::new();

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

                if !having.is_empty() {
                    let having_predicates: Vec<ScanFilter> =
                        serde_json::from_slice(having).unwrap_or_default();
                    if !having_predicates.is_empty() {
                        results.retain(|row| having_predicates.iter().all(|f| f.matches(row)));
                    }
                }

                results.truncate(limit);

                // Aggregate results are serde_json::Value objects — serialize as MessagePack.
                match super::super::response_codec::encode(&results) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, ErrorCode::Internal { detail: e }),
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

fn sort_rows(rows: &mut [(String, Vec<u8>)], sort_keys: &[(String, bool)]) {
    rows.sort_by(|(_, a_bytes), (_, b_bytes)| {
        let a_doc =
            super::super::doc_format::decode_document(a_bytes).unwrap_or(serde_json::Value::Null);
        let b_doc =
            super::super::doc_format::decode_document(b_bytes).unwrap_or(serde_json::Value::Null);

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
    });
}

struct RunReader {
    reader: BufReader<std::fs::File>,
    remaining: u32,
    run_idx: usize,
}

impl RunReader {
    fn new(file: std::fs::File, run_idx: usize) -> Result<Self, String> {
        let mut reader = BufReader::new(file);
        let mut buf4 = [0u8; 4];
        reader
            .read_exact(&mut buf4)
            .map_err(|e| format!("run reader init: {e}"))?;
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

        for (field, asc) in &self.sort_keys {
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
}
