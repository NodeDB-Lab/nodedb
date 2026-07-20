// SPDX-License-Identifier: BUSL-1.1

//! Universal document scan: routes to the correct engine and normalizes
//! all results to standard msgpack maps.
//!
//! Every query handler (aggregate, join, sort, filter, subquery) should
//! use `scan_collection` instead of calling engine-specific scan methods.
//! This gives a single place to handle format differences:
//! - Schemaless document → msgpack (already standard or legacy JSON)
//! - Strict document → Binary Tuple → decode → msgpack
//! - Key-Value → zerompk Value → transcode → msgpack
//! - Columnar → memtable/engine rows → JSON → msgpack

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::kv::KvScanParams;
use nodedb_query::msgpack_scan;
use nodedb_types::columnar::StrictSchema;

impl CoreLoop {
    /// Universal scan: reads from the correct engine for `collection` and
    /// returns `(doc_id, msgpack_bytes)` pairs in standard msgpack map format.
    ///
    /// Routing order:
    /// 1. KV engine (if collection has KV entries)
    /// 2. Columnar storage (timeseries memtable or plain/spatial engine)
    /// 3. Sparse/document engine (default)
    ///
    /// All results are normalized to standard msgpack maps so callers
    /// (aggregate, join, sort, filter) never need engine-specific code.
    pub fn scan_collection(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        // 1. KV engine
        let kv_docs = self.scan_kv(did, tid, collection, limit);
        if !kv_docs.is_empty() {
            return Ok(kv_docs);
        }

        // 2. Columnar memtable
        let col_docs = self.scan_columnar(did, tid, collection, limit);
        if !col_docs.is_empty() {
            return Ok(col_docs);
        }

        // 3. Sparse/document engine (schemaless + strict)
        self.scan_sparse(did, tid, collection, limit)
    }

    /// Row-at-a-time scan: invokes `f(id, raw_msgpack_bytes)` for every row
    /// in `collection` without an upper-row cap.
    ///
    /// Routing follows the same priority order as [`scan_collection`]:
    /// KV → Columnar → Sparse/document. All rows are normalized to standard
    /// msgpack maps before being passed to `f`.
    ///
    /// The callback receives shared references to the data; it must copy any
    /// bytes it wants to retain beyond the call. If `f` returns `Err`, iteration
    /// stops immediately and the error is propagated. Scan errors from the
    /// underlying engine are also propagated via `crate::Result`.
    ///
    /// There is intentionally no `limit` parameter — bounding is the caller's
    /// responsibility (e.g. the grace-hash join pipeline).
    // Consumed by the streamed grace-hash join build/probe pipeline
    // (`drive_grace_build`).
    pub(in crate::data::executor) fn scan_collection_for_each<F>(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        mut f: F,
    ) -> crate::Result<()>
    where
        F: FnMut(&str, &[u8]) -> crate::Result<()>,
    {
        // 1. KV engine — TRULY streams row-at-a-time via `scan_for_each`.
        //    Mirrors the materializing path's `if !kv_docs.is_empty()`
        //    early-return: a KV collection with zero live rows falls through.
        let now_ms = crate::engine::kv::current_ms();
        let mut found = false;
        self.kv_engine.scan_for_each(
            KvScanParams {
                database_id: did,
                tenant_id: tid,
                collection,
                cursor: &[],
                count: usize::MAX,
                now_ms,
                match_pattern: None,
                filter_field: None,
                filter_value: None,
                surrogate_ceiling: None,
            },
            |key, value| {
                found = true;
                let (key_str, mp) = kv_row_to_doc(key, value);
                f(&key_str, &mp)
            },
        )?;
        if found {
            return Ok(());
        }

        // 2. Columnar — materializes internally; iterate the batch per-row.
        // columnar stays materialized — per-row segment streaming is a separate
        // follow-up (flushed-segment decode).
        let col_docs = self.scan_columnar(did, tid, collection, usize::MAX);
        if !col_docs.is_empty() {
            for (id, bytes) in &col_docs {
                f(id, bytes)?;
            }
            return Ok(());
        }

        // 3. Sparse/document engine (schemaless + strict) — TRULY streams
        //    row-at-a-time via `scan_documents_for_each`. The strict schema is
        //    resolved once up front, exactly as in `scan_sparse`.
        let config_key = (
            crate::types::DatabaseId::new(did),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });
        self.sparse
            .scan_documents_for_each(did, tid, collection, usize::MAX, |id, raw| {
                let (id_s, mp) = sparse_row_to_doc(id, raw, strict_schema.as_ref());
                f(&id_s, &mp)
            })?;
        Ok(())
    }

    /// Scan KV engine entries → standard msgpack.
    /// Injects the `key` field directly into the msgpack map — no JSON roundtrip.
    fn scan_kv(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        limit: usize,
    ) -> Vec<(String, Vec<u8>)> {
        let now_ms = crate::engine::kv::current_ms();
        let (entries, _next_cursor) = self.kv_engine.scan(KvScanParams {
            database_id: did,
            tenant_id: tid,
            collection,
            cursor: &[],
            count: limit,
            now_ms,
            match_pattern: None,
            filter_field: None,
            filter_value: None,
            surrogate_ceiling: None,
        });
        let mut results = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            results.push(kv_row_to_doc(&key, &value));
        }
        results
    }

    /// Scan columnar rows → standard msgpack.
    fn scan_columnar(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        limit: usize,
    ) -> Vec<(String, Vec<u8>)> {
        let columnar_key = (
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        if let Some(mt) = self.columnar_memtables.get(&columnar_key) {
            let schema = mt.schema();
            let row_count = (mt.row_count() as usize).min(limit);
            let col_meta: Vec<_> = schema
                .columns
                .iter()
                .enumerate()
                .map(|(i, (name, ty))| (i, name.clone(), *ty))
                .collect();

            let mut results = Vec::with_capacity(row_count);
            for idx in 0..row_count {
                // Build msgpack map directly — no serde_json intermediary.
                let mut mp = Vec::with_capacity(col_meta.len() * 32);
                msgpack_scan::write_map_header(&mut mp, col_meta.len());
                let mut id = String::new();
                for (col_idx, col_name, col_type) in &col_meta {
                    msgpack_scan::write_str(&mut mp, col_name);
                    let col_data = mt.column(*col_idx);
                    // Check for "id" column to extract the id string.
                    if col_name == "id"
                        && let crate::engine::timeseries::columnar_memtable::ColumnData::Symbol(ids) =
                            col_data
                    {
                        let sym_id = ids[idx];
                        if let Some(s) = mt.symbol_dict(*col_idx).and_then(|dict| dict.get(sym_id))
                        {
                            id = s.to_string();
                        }
                    }
                    super::handlers::columnar_read::emit_column_value(
                        &mut mp, mt, *col_idx, col_type, col_data, idx,
                    );
                }
                results.push((id, mp));
            }
            return results;
        }

        let Some(engine) = self.columnar_engines.get(&columnar_key) else {
            return Vec::new();
        };

        let schema = engine.schema();
        let mut results = Vec::new();

        // 1. Read from flushed segments (older rows drained from prior memtable flushes).
        if let Some(segments) = self.columnar_flushed_segments.get(&columnar_key) {
            for (seg_idx, seg_bytes) in segments.iter().enumerate() {
                if results.len() >= limit {
                    break;
                }
                let seg_id = format!("{}", seg_idx as u64 + 1);
                let reader = if let Some(ref reg) = self.quarantine_registry {
                    match crate::storage::quarantine::engines::open_segment_with_quarantine(
                        reg, seg_bytes, collection, &seg_id,
                    ) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(error = %e, segment_id = %seg_id, collection, "failed to open flushed columnar segment for scan");
                            continue;
                        }
                    }
                } else {
                    match nodedb_columnar::SegmentReader::open(seg_bytes) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to open flushed columnar segment for scan");
                            continue;
                        }
                    }
                };
                let seg_row_count = reader.row_count() as usize;
                let remaining = limit - results.len();
                let take = seg_row_count.min(remaining);

                // Decode all columns for this segment.
                let col_count = schema.columns.len();
                let mut decoded_cols = Vec::with_capacity(col_count);
                let mut decode_ok = true;
                for col_idx in 0..col_count {
                    match reader.read_column(col_idx) {
                        Ok(dc) => decoded_cols.push(dc),
                        Err(e) => {
                            tracing::warn!(error = %e, col_idx, "failed to decode columnar segment column");
                            decode_ok = false;
                            break;
                        }
                    }
                }
                if !decode_ok {
                    continue;
                }

                for row_idx in 0..take {
                    let mut map = std::collections::HashMap::new();
                    let mut id = String::new();
                    for (col_idx, col_def) in schema.columns.iter().enumerate() {
                        let val = decoded_col_to_value(&decoded_cols[col_idx], row_idx);
                        if col_def.name == "id"
                            && let nodedb_types::value::Value::String(s) = &val
                        {
                            id.clone_from(s);
                        }
                        map.insert(col_def.name.clone(), val);
                    }
                    let ndb_val = nodedb_types::value::Value::Object(map);
                    let mp = nodedb_types::value_to_msgpack(&ndb_val).unwrap_or_default();
                    results.push((id, mp));
                }
            }
        }

        // 2. Read from the live memtable (most-recent rows not yet flushed).
        if results.len() < limit {
            let remaining = limit - results.len();
            let rows: Vec<_> = engine.scan_memtable_rows().take(remaining).collect();
            for row in rows {
                let mut map = std::collections::HashMap::new();
                let mut id = String::new();
                for (i, col_def) in schema.columns.iter().enumerate() {
                    if i < row.len() {
                        if col_def.name == "id"
                            && let nodedb_types::value::Value::String(s) = &row[i]
                        {
                            id.clone_from(s);
                        }
                        map.insert(col_def.name.clone(), row[i].clone());
                    }
                }
                let ndb_val = nodedb_types::value::Value::Object(map);
                let mp = nodedb_types::value_to_msgpack(&ndb_val).unwrap_or_default();
                results.push((id, mp));
            }
        }

        results
    }

    /// Scan sparse/document engine → standard msgpack.
    /// Handles both schemaless (msgpack) and strict (Binary Tuple) formats.
    pub(super) fn scan_sparse(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let docs = self.sparse.scan_documents(did, tid, collection, limit)?;
        let strict_schema = self.strict_schema_for(
            crate::types::DatabaseId::new(did),
            crate::types::TenantId::new(tid),
            collection,
        );

        let mut normalized = Vec::with_capacity(docs.len());
        for (id, raw) in docs {
            normalized.push(sparse_row_to_doc(&id, &raw, strict_schema.as_ref()));
        }
        Ok(normalized)
    }
}

/// Convert a single KV engine entry to a `(key, msgpack)` document.
///
/// Lossy-decodes the key to a UTF-8 string and injects it as the `key`
/// field directly into the msgpack map at the binary level (no JSON
/// roundtrip). Shared by the materializing scan and the streaming scan so
/// both paths produce byte-identical output.
fn kv_row_to_doc(key: &[u8], value: &[u8]) -> (String, Vec<u8>) {
    let key_str = String::from_utf8_lossy(key).to_string();
    let mp = msgpack_scan::inject_str_field(value, "key", &key_str);
    (key_str, mp)
}

/// Convert a single sparse/document row to a `(id, msgpack)` document.
///
/// When `strict_schema` is `Some`, the raw bytes are a Binary Tuple and are
/// decoded via the strict schema (falling back to JSON transcoding if the
/// tuple cannot be decoded). When `None`, the raw bytes are schemaless and
/// are normalised from (possibly legacy JSON) to standard msgpack. In both
/// cases the `id` field is injected identically. Shared by the materializing
/// scan and the streaming scan so both paths produce byte-identical output.
pub(in crate::data::executor) fn sparse_row_to_doc(
    id: &str,
    raw: &[u8],
    strict_schema: Option<&StrictSchema>,
) -> (String, Vec<u8>) {
    let mp = if let Some(schema) = strict_schema {
        super::strict_format::binary_tuple_to_msgpack(raw, schema)
            .unwrap_or_else(|| super::doc_format::json_to_msgpack(raw))
    } else {
        super::doc_format::json_to_msgpack(raw)
    };
    let mp = msgpack_scan::inject_str_field(&mp, "id", id);
    (id.to_string(), mp)
}

/// Convert a single row from a `DecodedColumn` to a `nodedb_types::value::Value`.
///
/// Returns `Value::Null` if the row index is out of range or the validity bit is false.
pub(in crate::data::executor) fn decoded_col_to_value(
    col: &nodedb_columnar::reader::DecodedColumn,
    row_idx: usize,
) -> nodedb_types::value::Value {
    use nodedb_columnar::reader::DecodedColumn;
    use nodedb_types::value::Value;

    match col {
        DecodedColumn::Int64 { values, valid } => {
            if row_idx < valid.len() && valid[row_idx] {
                Value::Integer(values[row_idx])
            } else {
                Value::Null
            }
        }
        DecodedColumn::Float64 { values, valid } => {
            if row_idx < valid.len() && valid[row_idx] {
                Value::Float(values[row_idx])
            } else {
                Value::Null
            }
        }
        DecodedColumn::Timestamp { values, valid } => {
            if row_idx < valid.len() && valid[row_idx] {
                // Represent as integer microseconds (same as Value::Integer for timestamps).
                Value::Integer(values[row_idx])
            } else {
                Value::Null
            }
        }
        DecodedColumn::Bool { values, valid } => {
            if row_idx < valid.len() && valid[row_idx] {
                Value::Bool(values[row_idx])
            } else {
                Value::Null
            }
        }
        DecodedColumn::Binary {
            data,
            offsets,
            valid,
        } => {
            if row_idx < valid.len() && valid[row_idx] && row_idx + 1 < offsets.len() {
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                if start <= end && end <= data.len() {
                    let bytes = &data[start..end];
                    // Best-effort UTF-8 interpretation; fall back to bytes.
                    match std::str::from_utf8(bytes) {
                        Ok(s) => Value::String(s.to_string()),
                        Err(_) => Value::Bytes(bytes.to_vec()),
                    }
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        }
        DecodedColumn::DictEncoded {
            ids,
            dictionary,
            valid,
        } => {
            if row_idx < valid.len() && valid[row_idx] {
                let id = ids[row_idx] as usize;
                if id < dictionary.len() {
                    Value::String(dictionary[id].clone())
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    /// Verify that `scan_collection_for_each` visits exactly the same
    /// `(id, bytes)` set as `scan_collection` for a sparse/document collection.
    ///
    /// Constructing a populated `CoreLoop` is feasible here via the shared
    /// `make_core_with_dir` helper used throughout the executor test suite.
    /// We insert a handful of documents via `core.sparse.put`, then compare
    /// both scan outputs.
    #[test]
    fn for_each_matches_scan_collection_on_sparse_docs() {
        let dir = tempfile::tempdir().unwrap();
        let (core, _req_tx, _resp_rx) =
            crate::data::executor::core_loop::tests::make_core_with_dir(dir.path());

        let tid: u64 = 1;
        let coll = "scan_test";

        // Insert three schemaless documents via the sparse engine.
        // `sparse.put` writes raw bytes (here a minimal JSON blob that
        // `json_to_msgpack` will normalise to msgpack in both paths).
        let raw_a = b"{\"x\":1}";
        let raw_b = b"{\"x\":2}";
        let raw_c = b"{\"x\":3}";
        core.sparse.put(0, tid, coll, "a", raw_a).unwrap();
        core.sparse.put(0, tid, coll, "b", raw_b).unwrap();
        core.sparse.put(0, tid, coll, "c", raw_c).unwrap();

        // Collect via `scan_collection` (the reference output).
        let mut expected = core.scan_collection(0, tid, coll, usize::MAX).unwrap();
        expected.sort_by(|a, b| a.0.cmp(&b.0));

        // Collect via `scan_collection_for_each`.
        let mut actual: Vec<(String, Vec<u8>)> = Vec::new();
        core.scan_collection_for_each(0, tid, coll, |id, bytes| {
            actual.push((id.to_owned(), bytes.to_vec()));
            Ok(())
        })
        .unwrap();
        actual.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(
            expected.len(),
            actual.len(),
            "row counts must match: expected {}, got {}",
            expected.len(),
            actual.len()
        );
        assert_eq!(expected, actual, "id+bytes pairs must be identical");
    }

    /// Verify that `scan_collection_for_each` visits exactly the same
    /// `(key, bytes)` set as `scan_collection` for a KV collection.
    ///
    /// This guards the KV streaming path (`scan_for_each`) against drifting
    /// from the materializing path (`scan`/`scan_kv`): both feed the shared
    /// `kv_row_to_doc` helper, so output must be byte-identical.
    #[test]
    fn for_each_matches_scan_collection_on_kv() {
        use nodedb_types::Surrogate;

        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) =
            crate::data::executor::core_loop::tests::make_core_with_dir(dir.path());

        let tid: u64 = 1;
        let coll = "kv_scan_test";
        let now_ms = crate::engine::kv::current_ms();

        // Insert three KV entries with empty-map msgpack values; the `key`
        // field is injected identically by both scan paths.
        let val = nodedb_types::value_to_msgpack(&nodedb_types::value::Value::Object(
            std::collections::HashMap::new(),
        ))
        .unwrap();
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: 0,
            tenant_id: tid,
            collection: coll,
            key: b"a",
            value: &val,
            ttl_ms: 0,
            now_ms,
            surrogate: Surrogate::ZERO,
        });
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: 0,
            tenant_id: tid,
            collection: coll,
            key: b"b",
            value: &val,
            ttl_ms: 0,
            now_ms,
            surrogate: Surrogate::ZERO,
        });
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: 0,
            tenant_id: tid,
            collection: coll,
            key: b"c",
            value: &val,
            ttl_ms: 0,
            now_ms,
            surrogate: Surrogate::ZERO,
        });

        let mut expected = core.scan_collection(0, tid, coll, usize::MAX).unwrap();
        expected.sort_by(|a, b| a.0.cmp(&b.0));

        let mut actual: Vec<(String, Vec<u8>)> = Vec::new();
        core.scan_collection_for_each(0, tid, coll, |id, bytes| {
            actual.push((id.to_owned(), bytes.to_vec()));
            Ok(())
        })
        .unwrap();
        actual.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(
            expected.len(),
            actual.len(),
            "row counts must match: expected {}, got {}",
            expected.len(),
            actual.len()
        );
        assert_eq!(expected, actual, "key+bytes pairs must be identical");
    }

    /// ORDER contract: `scan_collection_for_each` must yield rows in the exact
    /// same order as `scan_collection`, not merely the same set.
    ///
    /// We insert sparse/document rows in an intentionally non-sorted order
    /// ("d", "a", "c", "b") so that a bug which sorts internally would produce
    /// a different sequence from one that doesn't — making an accidental
    /// coincidence of order impossible to hide.  Neither vector is sorted
    /// before the assertion.
    #[test]
    fn for_each_matches_scan_collection_order_on_sparse_docs() {
        let dir = tempfile::tempdir().unwrap();
        let (core, _req_tx, _resp_rx) =
            crate::data::executor::core_loop::tests::make_core_with_dir(dir.path());

        let tid: u64 = 1;
        let coll = "scan_order_sparse";

        // Insert in non-alphabetical order so insertion order != sorted order.
        // If either scan path sorts internally the assertion will catch the divergence.
        core.sparse.put(0, tid, coll, "d", b"{\"v\":4}").unwrap();
        core.sparse.put(0, tid, coll, "a", b"{\"v\":1}").unwrap();
        core.sparse.put(0, tid, coll, "c", b"{\"v\":3}").unwrap();
        core.sparse.put(0, tid, coll, "b", b"{\"v\":2}").unwrap();

        // Reference output — NOT sorted.
        let expected = core.scan_collection(0, tid, coll, usize::MAX).unwrap();

        // Streaming output — NOT sorted.
        let mut actual: Vec<(String, Vec<u8>)> = Vec::new();
        core.scan_collection_for_each(0, tid, coll, |id, bytes| {
            actual.push((id.to_owned(), bytes.to_vec()));
            Ok(())
        })
        .unwrap();

        assert_eq!(
            expected.len(),
            actual.len(),
            "row counts must match: expected {}, got {}",
            expected.len(),
            actual.len(),
        );
        assert_eq!(
            expected, actual,
            "scan_collection_for_each must yield rows in the identical order \
             as scan_collection (ORDER contract, not merely SET equality)"
        );
    }

    /// ORDER contract: `scan_collection_for_each` must yield KV rows in the
    /// exact same order as `scan_collection`.
    ///
    /// Keys are inserted as "k3", "k1", "k4", "k2" — deliberately non-sorted —
    /// so that a path that sorts keys produces a different sequence from one
    /// that preserves scan order, making an accidental coincidence impossible.
    /// Neither vector is sorted before the assertion.
    #[test]
    fn for_each_matches_scan_collection_order_on_kv() {
        use nodedb_types::Surrogate;

        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) =
            crate::data::executor::core_loop::tests::make_core_with_dir(dir.path());

        let tid: u64 = 1;
        let coll = "scan_order_kv";
        let now_ms = crate::engine::kv::current_ms();

        // Empty-map msgpack value; the `key` field is injected by kv_row_to_doc.
        let val = nodedb_types::value_to_msgpack(&nodedb_types::value::Value::Object(
            std::collections::HashMap::new(),
        ))
        .unwrap();

        // Insert in non-sorted order: "k3", "k1", "k4", "k2".
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: 0,
            tenant_id: tid,
            collection: coll,
            key: b"k3",
            value: &val,
            ttl_ms: 0,
            now_ms,
            surrogate: Surrogate::ZERO,
        });
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: 0,
            tenant_id: tid,
            collection: coll,
            key: b"k1",
            value: &val,
            ttl_ms: 0,
            now_ms,
            surrogate: Surrogate::ZERO,
        });
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: 0,
            tenant_id: tid,
            collection: coll,
            key: b"k4",
            value: &val,
            ttl_ms: 0,
            now_ms,
            surrogate: Surrogate::ZERO,
        });
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: 0,
            tenant_id: tid,
            collection: coll,
            key: b"k2",
            value: &val,
            ttl_ms: 0,
            now_ms,
            surrogate: Surrogate::ZERO,
        });

        // Reference output — NOT sorted.
        let expected = core.scan_collection(0, tid, coll, usize::MAX).unwrap();

        // Streaming output — NOT sorted.
        let mut actual: Vec<(String, Vec<u8>)> = Vec::new();
        core.scan_collection_for_each(0, tid, coll, |id, bytes| {
            actual.push((id.to_owned(), bytes.to_vec()));
            Ok(())
        })
        .unwrap();

        assert_eq!(
            expected.len(),
            actual.len(),
            "row counts must match: expected {}, got {}",
            expected.len(),
            actual.len(),
        );
        assert_eq!(
            expected, actual,
            "scan_collection_for_each must yield KV rows in the identical order \
             as scan_collection (ORDER contract, not merely SET equality)"
        );
    }

    // NOTE: Columnar order-equivalence is not covered by a unit test here.
    //
    // `scan_collection_for_each` for columnar collections materialises the
    // batch via the same `scan_columnar` call used by `scan_collection` (the
    // streamed and materialised paths share one code path for columnar — see
    // the comment in `scan_collection_for_each` step 2), so the ORDER contract
    // is structurally guaranteed for columnar at the source-code level rather
    // than being an independent divergence risk.
    //
    // Adding a columnar ORDER test here would require spinning up a
    // `ColumnarEngine` / `ColumnarMemtable` entry in `CoreLoop`'s internal
    // maps (neither `make_core_with_dir` nor any other helper in this test
    // suite exposes a way to pre-populate `columnar_memtables` or
    // `columnar_engines` without going through the full engine-init path).
    // That setup is exercised by the columnar-specific integration tests in
    // `nodedb/tests/executor_tests/` (e.g. `test_cross_type_join`).
    // A follow-up can add a columnar order unit test once a suitable
    // `make_core_with_columnar_collection` helper exists.

    /// Verify that a callback error from `scan_collection_for_each` is
    /// propagated immediately and stops iteration.
    #[test]
    fn for_each_propagates_callback_error() {
        let dir = tempfile::tempdir().unwrap();
        let (core, _req_tx, _resp_rx) =
            crate::data::executor::core_loop::tests::make_core_with_dir(dir.path());

        let tid: u64 = 1;
        let coll = "err_test";
        core.sparse.put(0, tid, coll, "a", b"{\"v\":1}").unwrap();
        core.sparse.put(0, tid, coll, "b", b"{\"v\":2}").unwrap();

        let mut calls = 0usize;
        let result = core.scan_collection_for_each(0, tid, coll, |_id, _bytes| {
            calls += 1;
            Err(crate::Error::Internal {
                detail: "deliberate test error".into(),
            })
        });

        assert!(result.is_err(), "error from callback must be propagated");
        assert_eq!(calls, 1, "iteration must stop after the first error");
    }
}
