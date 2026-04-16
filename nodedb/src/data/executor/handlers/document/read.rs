//! Document read and scan handlers: Scan, PointGet, RangeScan, IndexLookup.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Apply projection or computed columns to a decoded document.
fn apply_projection(
    data: serde_json::Value,
    computed_cols: &[crate::bridge::expr_eval::ComputedColumn],
    projection: &[String],
) -> serde_json::Value {
    match data {
        serde_json::Value::Object(obj) => {
            if computed_cols.is_empty() && projection.is_empty() {
                return serde_json::Value::Object(obj);
            }

            let doc_val = nodedb_types::Value::from(serde_json::Value::Object(obj.clone()));
            let mut out = if projection.is_empty() {
                serde_json::Map::with_capacity(computed_cols.len())
            } else {
                let mut projected =
                    serde_json::Map::with_capacity(projection.len() + computed_cols.len());
                for col in projection {
                    if let Some(val) = obj.get(col) {
                        projected.insert(col.clone(), val.clone());
                    }
                }
                projected
            };

            for cc in computed_cols {
                if out.contains_key(&cc.alias) {
                    continue;
                }
                out.insert(
                    cc.alias.clone(),
                    serde_json::Value::from(cc.expr.eval(&doc_val)),
                );
            }

            serde_json::Value::Object(out)
        }
        other => other,
    }
}

/// Apply projection and computed columns on raw msgpack bytes.
///
/// For projection-only (no computed columns), uses zero-decode binary field extraction.
/// For computed columns, decodes fields on-demand from msgpack.
fn apply_projection_msgpack(
    data: &[u8],
    computed_cols: &[crate::bridge::expr_eval::ComputedColumn],
    projection: &[String],
) -> Vec<u8> {
    if computed_cols.is_empty() && projection.is_empty() {
        return data.to_vec();
    }

    let field_count = if projection.is_empty() {
        computed_cols.len()
    } else {
        projection.len() + computed_cols.len()
    };

    let mut buf = Vec::with_capacity(data.len());
    nodedb_query::msgpack_scan::write_map_header(&mut buf, field_count);

    // Project requested fields from raw msgpack.
    if !projection.is_empty() {
        for col in projection {
            nodedb_query::msgpack_scan::write_str(&mut buf, col);
            if let Some((start, end)) = nodedb_query::msgpack_scan::extract_field(data, 0, col) {
                buf.extend_from_slice(&data[start..end]);
            } else {
                nodedb_query::msgpack_scan::write_null(&mut buf);
            }
        }
    }

    // Computed columns: eval expression against msgpack doc.
    if !computed_cols.is_empty() {
        // Lazy decode to Value only when we have computed columns.
        let doc_val = nodedb_types::value_from_msgpack(data).unwrap_or(nodedb_types::Value::Null);
        for cc in computed_cols {
            // Skip if projection already included this alias.
            let already_present = projection.iter().any(|p| p == &cc.alias);
            if already_present {
                continue;
            }
            nodedb_query::msgpack_scan::write_str(&mut buf, &cc.alias);
            let result = cc.expr.eval(&doc_val);
            if let Ok(mp) = nodedb_types::value_to_msgpack(&result) {
                buf.extend_from_slice(&mp);
            } else {
                nodedb_query::msgpack_scan::write_null(&mut buf);
            }
        }
    }

    buf
}

/// Decode a scanned document to raw msgpack bytes.
fn decode_scanned_document_msgpack(
    value: &[u8],
    strict_schema: Option<&nodedb_types::columnar::StrictSchema>,
) -> Vec<u8> {
    if let Some(schema) = strict_schema
        && let Some(mp) = super::super::super::strict_format::binary_tuple_to_msgpack(value, schema)
    {
        return mp;
    }
    // Already msgpack (or legacy JSON → convert).
    super::super::super::doc_format::json_to_msgpack(value)
}

/// Decode a scanned document to serde_json::Value (for window functions / legacy paths).
fn decode_scanned_document(
    value: &[u8],
    strict_schema: Option<&nodedb_types::columnar::StrictSchema>,
) -> serde_json::Value {
    strict_schema
        .and_then(|schema| super::super::super::strict_format::binary_tuple_to_json(value, schema))
        .or_else(|| super::super::super::doc_format::decode_document(value))
        .unwrap_or(serde_json::Value::Null)
}

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

        // Check if this collection uses strict (Binary Tuple) encoding.
        let config_key = format!("{tid}:{collection}");
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let crate::bridge::physical_plan::StorageMode::Strict { ref schema } = c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Scan strategy:
        // 1. Try sparse engine first (with optimized push-down filters when present).
        // 2. If sparse returns empty, fall back to scan_collection which routes
        //    to the correct engine (KV → columnar → sparse). This makes
        //    DocumentOp::Scan the universal scan for ALL collection types.
        //
        // Strict (Binary Tuple) collections need JSON-level filter evaluation
        // because `matches_binary` operates on MessagePack, not Binary Tuples.
        let scan_result = if filter_predicates.is_empty() {
            let sparse_result = self.sparse.scan_documents(tid, collection, fetch_limit);
            match &sparse_result {
                Ok(docs) if docs.is_empty() => {
                    // Sparse empty — try KV / columnar engines.
                    let fallback = self.scan_collection(tid, collection, fetch_limit);
                    if let Ok(ref docs) = fallback
                        && !docs.is_empty()
                    {
                        warn!(
                            core = self.core_id,
                            %collection,
                            count = docs.len(),
                            "document scan fallback to scan_collection"
                        );
                    }
                    fallback
                }
                _ => sparse_result,
            }
        } else if let Some(ref schema) = strict_schema {
            // Strict: Binary Tuple → msgpack → matches_binary (no serde_json).
            self.sparse
                .scan_documents_filtered(tid, collection, fetch_limit, &|value: &[u8]| {
                    match super::super::super::strict_format::binary_tuple_to_msgpack(value, schema)
                    {
                        Some(mp) => filter_predicates.iter().all(|f| f.matches_binary(&mp)),
                        None => false,
                    }
                })
        } else {
            let sparse_result = self.sparse.scan_documents_filtered(
                tid,
                collection,
                fetch_limit,
                &|value: &[u8]| filter_predicates.iter().all(|f| f.matches_binary(value)),
            );
            match &sparse_result {
                Ok(docs) if docs.is_empty() => {
                    // Sparse empty — try KV / columnar, apply filters post-scan.
                    self.scan_collection(tid, collection, fetch_limit)
                        .map(|docs| {
                            docs.into_iter()
                                .filter(|(_, data)| {
                                    filter_predicates.iter().all(|f| f.matches_binary(data))
                                })
                                .collect()
                        })
                }
                _ => sparse_result,
            }
        };

        match scan_result {
            Ok(filtered) => {
                if let Some(ref m) = self.metrics {
                    m.record_document_read();
                }

                // Strict collections may store binary tuples. Sort and projection
                // operate on msgpack. Normalize binary tuples to msgpack before
                // sorting. The decoder validates the schema version header and
                // rejects bytes that are already msgpack (returns None → kept as-is).
                let filtered = if !sort_keys.is_empty() || !projection.is_empty() {
                    if let Some(ref schema) = strict_schema {
                        filtered
                            .into_iter()
                            .map(|(id, bytes)| {
                                match super::super::super::strict_format::binary_tuple_to_msgpack(
                                    &bytes, schema,
                                ) {
                                    Some(mp) => (id, mp),
                                    None => (id, bytes),
                                }
                            })
                            .collect()
                    } else {
                        filtered
                    }
                } else {
                    filtered
                };

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

                let stream_chunk_size = self.query_tuning.stream_chunk_size;

                // Strict (Binary Tuple) collections: decode binary tuples inline,
                // after dedup/offset/limit, so we only decode rows that are returned.
                // This avoids double serialization (binary_tuple → JSON bytes → re-parse → MsgPack)
                // and avoids decoding rows that will be skipped by offset/limit.
                if let Some(ref schema) = strict_schema
                    && window_specs.is_empty()
                {
                    let deduped = if distinct {
                        let mut seen = std::collections::HashSet::new();
                        sorted
                            .into_iter()
                            .filter(|(_, value)| seen.insert(value.clone()))
                            .collect::<Vec<_>>()
                    } else {
                        sorted
                    };
                    let result: Vec<_> = deduped
                        .into_iter()
                        .skip(offset)
                        .take(limit)
                        .map(|(doc_id, val)| {
                            let mp = decode_scanned_document_msgpack(&val, Some(schema));
                            let projected =
                                apply_projection_msgpack(&mp, &computed_cols, projection);
                            (doc_id, projected)
                        })
                        .collect();
                    return self.send_document_rows_raw(task, &result, stream_chunk_size);
                }

                // Window functions require decoded values; keep them decoded
                // through dedup/projection to avoid re-encode→re-decode cycle.
                if !window_specs.is_empty() {
                    let mut decoded_rows: Vec<(String, serde_json::Value)> = sorted
                        .into_iter()
                        .map(|(id, val)| {
                            let doc = decode_scanned_document(&val, strict_schema.as_ref());
                            (id, doc)
                        })
                        .collect();
                    crate::bridge::window_func::evaluate_window_functions(
                        &mut decoded_rows,
                        &window_specs,
                    );

                    // Dedup on decoded values.
                    let deduped: Vec<_> = if distinct {
                        let mut seen = std::collections::HashSet::new();
                        decoded_rows
                            .into_iter()
                            .filter(|(_, v)| seen.insert(v.to_string()))
                            .collect()
                    } else {
                        decoded_rows
                    };

                    // Build response directly from decoded values (no re-encode).
                    let result: Vec<_> = deduped
                        .into_iter()
                        .skip(offset)
                        .take(limit)
                        .map(|(doc_id, data)| {
                            let projected = apply_projection(data, &computed_cols, projection);
                            super::super::super::response_codec::DocumentRow {
                                id: doc_id,
                                data: projected,
                            }
                        })
                        .collect();

                    self.send_document_rows_transformed(task, &result, stream_chunk_size)
                } else {
                    // No window functions: work with raw bytes.
                    let deduped = if distinct {
                        let mut seen = std::collections::HashSet::new();
                        sorted
                            .into_iter()
                            .filter(|(_, value)| seen.insert(value.clone()))
                            .collect()
                    } else {
                        sorted
                    };

                    let needs_transform = !computed_cols.is_empty() || !projection.is_empty();

                    if needs_transform {
                        let result: Vec<_> = deduped
                            .into_iter()
                            .skip(offset)
                            .take(limit)
                            .map(|(doc_id, value)| {
                                let mp = super::super::super::doc_format::json_to_msgpack(&value);
                                let projected =
                                    apply_projection_msgpack(&mp, &computed_cols, projection);
                                (doc_id, projected)
                            })
                            .collect();

                        self.send_document_rows_raw(task, &result, stream_chunk_size)
                    } else {
                        // Raw passthrough: skip decode+re-encode entirely.
                        let rows: Vec<_> = deduped.into_iter().skip(offset).take(limit).collect();

                        self.send_document_rows_raw(task, &rows, stream_chunk_size)
                    }
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

    /// Send transformed document rows (decoded → projected → re-encoded).
    fn send_document_rows_transformed(
        &mut self,
        task: &ExecutionTask,
        result: &Vec<super::super::super::response_codec::DocumentRow>,
        chunk_size: usize,
    ) -> Response {
        if result.len() <= chunk_size {
            match super::super::super::response_codec::encode(result) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
            }
        } else {
            self.stream_chunks_transformed(task, result, chunk_size)
        }
    }

    /// Send raw document rows with msgpack passthrough (no decode+re-encode).
    fn send_document_rows_raw(
        &mut self,
        task: &ExecutionTask,
        rows: &[(String, Vec<u8>)],
        chunk_size: usize,
    ) -> Response {
        if rows.len() <= chunk_size {
            match super::super::super::response_codec::encode_raw_document_rows(rows) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
            }
        } else {
            self.stream_chunks_raw(task, rows, chunk_size)
        }
    }

    /// Stream transformed document rows in chunks.
    fn stream_chunks_transformed(
        &mut self,
        task: &ExecutionTask,
        result: &[super::super::super::response_codec::DocumentRow],
        chunk_size: usize,
    ) -> Response {
        let chunks: Vec<_> = result.chunks(chunk_size).collect();
        let last_idx = chunks.len().saturating_sub(1);
        for (i, chunk) in chunks.iter().enumerate() {
            let is_last = i == last_idx;
            match super::super::super::response_codec::encode(&chunk.to_vec()) {
                Ok(payload) => {
                    if is_last {
                        return self.response_with_payload(task, payload);
                    }
                    let partial = self.response_partial(task, payload);
                    let _ = self
                        .response_tx
                        .try_push(crate::bridge::dispatch::BridgeResponse { inner: partial });
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
        self.response_error(
            task,
            ErrorCode::Internal {
                detail: "streaming response incomplete".into(),
            },
        )
    }

    /// Stream raw document rows in chunks with msgpack passthrough.
    fn stream_chunks_raw(
        &mut self,
        task: &ExecutionTask,
        rows: &[(String, Vec<u8>)],
        chunk_size: usize,
    ) -> Response {
        let chunks: Vec<_> = rows.chunks(chunk_size).collect();
        let last_idx = chunks.len().saturating_sub(1);
        for (i, chunk) in chunks.iter().enumerate() {
            let is_last = i == last_idx;
            match super::super::super::response_codec::encode_raw_document_rows(chunk) {
                Ok(payload) => {
                    if is_last {
                        return self.response_with_payload(task, payload);
                    }
                    let partial = self.response_partial(task, payload);
                    let _ = self
                        .response_tx
                        .try_push(crate::bridge::dispatch::BridgeResponse { inner: partial });
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
        self.response_error(
            task,
            ErrorCode::Internal {
                detail: "streaming response incomplete".into(),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_projection, decode_scanned_document};
    use crate::bridge::expr_eval::{ComputedColumn, SqlExpr};
    use crate::data::executor::strict_format;
    use nodedb_types::Value;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    #[test]
    fn apply_projection_keeps_base_fields_when_computed_columns_exist() {
        let data = serde_json::json!({
            "id": "u1",
            "name": "Ada",
            "age": 42
        });
        let computed = vec![ComputedColumn {
            alias: "label".into(),
            expr: SqlExpr::Column("name".into()),
        }];
        let projection = vec!["name".to_string(), "age".to_string()];

        let projected = apply_projection(data, &computed, &projection);

        assert_eq!(
            projected,
            serde_json::json!({
                "name": "Ada",
                "age": 42,
                "label": "Ada"
            })
        );
    }

    #[test]
    fn apply_projection_does_not_overwrite_existing_window_alias() {
        let data = serde_json::json!({
            "name": "Ada",
            "age": 42,
            "rn": 1
        });
        let computed = vec![ComputedColumn {
            alias: "rn".into(),
            expr: SqlExpr::Function {
                name: "row_number".into(),
                args: Vec::new(),
            },
        }];
        let projection = vec!["name".to_string(), "age".to_string(), "rn".to_string()];

        let projected = apply_projection(data, &computed, &projection);

        assert_eq!(
            projected,
            serde_json::json!({
                "name": "Ada",
                "age": 42,
                "rn": 1
            })
        );
    }

    #[test]
    fn decode_scanned_document_uses_strict_schema_for_binary_tuple_rows() {
        let schema = StrictSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::String).with_primary_key(),
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::nullable("age", ColumnType::Int64),
            ],
            version: 1,
            dropped_columns: Vec::new(),
        };
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u1".into()));
        map.insert("name".into(), Value::String("Ada".into()));
        map.insert("age".into(), Value::Integer(42));

        let tuple = strict_format::value_to_binary_tuple(&Value::Object(map), &schema)
            .expect("encode strict tuple");

        let decoded = decode_scanned_document(&tuple, Some(&schema));

        assert_eq!(
            decoded,
            serde_json::json!({
                "id": "u1",
                "name": "Ada",
                "age": 42
            })
        );
    }
}
