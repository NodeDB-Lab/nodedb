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
//! - Columnar → segment and memtable rows → msgpack
//! - Timeseries → memtable and partition rows → msgpack

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::kv::KvScanParams;

impl CoreLoop {
    /// [`Self::scan_collection`] with row-level-security filters applied.
    ///
    /// `rls_filters` is the MessagePack `Vec<ScanFilter>` the planner injected
    /// for this collection; empty means no policy applies and the scan is
    /// returned unchanged. Callers that scan a collection directly — rather
    /// than through a plan variant with its own filter slot — use this, so a
    /// locally-scanned side of a join is filtered exactly as the same rows
    /// would be if they arrived through a `Scan` plan.
    ///
    /// A filter that fails to deserialize is an error, never an empty filter
    /// set: silently dropping security filters would return the unfiltered
    /// rows.
    pub fn scan_collection_with_rls(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        limit: usize,
        rls_filters: &[u8],
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let docs = self.scan_collection(did, tid, collection, limit)?;
        self.retain_rows_matching(docs, rls_filters, "RLS filter (join side)")
    }

    /// Keep the rows matching a MessagePack `Vec<ScanFilter>`.
    ///
    /// Empty `filter_bytes` keeps every row. A set that fails to deserialize is
    /// an error, never an empty set: `context` names which set failed so the
    /// caller's error says whether a policy or a query predicate was dropped.
    pub(in crate::data::executor) fn retain_rows_matching(
        &self,
        docs: Vec<(String, Vec<u8>)>,
        filter_bytes: &[u8],
        context: &str,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        if filter_bytes.is_empty() {
            return Ok(docs);
        }

        let filters = crate::bridge::scan_filter::decode_scan_filters(filter_bytes, context)?;

        let crs = self.crs_context();
        let mut kept = Vec::with_capacity(docs.len());
        for (id, bytes) in docs {
            if crate::bridge::scan_filter::ScanFilter::all_match_binary(&filters, &bytes, &crs)? {
                kept.push((id, bytes));
            }
        }
        Ok(kept)
    }

    /// Whether a stored row passes the caller's row-level-security filters.
    ///
    /// `rls_filters` is the MessagePack `Vec<ScanFilter>` the planner injected;
    /// empty means no policy applies and every row passes. Used by point and
    /// batch reads, which have no pushdown filter slot in storage and so
    /// evaluate the policy on the fetched bytes.
    pub(in crate::data::executor) fn row_passes_rls(
        &self,
        row: &[u8],
        rls_filters: &[u8],
    ) -> crate::Result<bool> {
        if rls_filters.is_empty() {
            return Ok(true);
        }
        let filters = crate::bridge::scan_filter::decode_scan_filters(rls_filters, "RLS filter")?;
        Ok(crate::bridge::scan_filter::ScanFilter::all_match_binary(
            &filters,
            row,
            &self.crs_context(),
        )?)
    }

    /// Universal scan: reads from the correct engine for `collection` and
    /// returns `(doc_id, msgpack_bytes)` pairs in standard msgpack map format.
    ///
    /// Routing order:
    /// 1. Timeseries (by identity): memtable rows, then flushed partitions
    /// 2. KV engine (if collection has KV entries)
    /// 3. Columnar storage (plain/spatial engine)
    /// 4. Sparse/document engine (default)
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
        // 1. Timeseries
        if self.is_timeseries_collection(did, tid, collection) {
            return self.scan_timeseries_docs(did, tid, collection, limit);
        }

        // 2. KV engine
        let kv_docs = self.scan_kv(did, tid, collection, limit);
        if !kv_docs.is_empty() {
            return Ok(kv_docs);
        }

        // 3. Columnar engine
        let col_docs = self.scan_columnar(did, tid, collection, limit)?;
        if !col_docs.is_empty() {
            return Ok(col_docs);
        }

        // 4. Sparse/document engine (schemaless + strict)
        self.scan_sparse(did, tid, collection, limit)
    }

    /// Row-at-a-time scan: invokes `f(id, raw_msgpack_bytes)` for every row
    /// in `collection` without an upper-row cap.
    ///
    /// Routing follows the same priority order as [`scan_collection`]:
    /// Timeseries → KV → Columnar → Sparse/document. All rows are normalized
    /// to standard msgpack maps before being passed to `f`.
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
        // Timeseries — read by identity from memtable and partitions.
        if self.is_timeseries_collection(did, tid, collection) {
            for (id, bytes) in &self.scan_timeseries_docs(did, tid, collection, usize::MAX)? {
                f(id, bytes)?;
            }
            return Ok(());
        }

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
        let col_docs = self.scan_columnar(did, tid, collection, usize::MAX)?;
        if !col_docs.is_empty() {
            for (id, bytes) in &col_docs {
                f(id, bytes)?;
            }
            return Ok(());
        }

        // 3. Sparse/document engine (schemaless + strict + vector-primary
        //    sidecar) — streams via `for_each_current_row`: row-at-a-time on
        //    the plain table, a page at a time on a bitemporal collection's
        //    current versions. The body encoding is resolved once up
        //    front through the same helper `scan_sparse` uses, so the streaming
        //    and materializing scans cannot disagree about a row's shape.
        let database_id = crate::types::DatabaseId::new(did);
        let tenant_id = crate::types::TenantId::new(tid);
        let format = self.sparse_body_format(database_id, tenant_id, collection);
        let identity_column = self.identity_column(did, tid, collection);
        // Each row at its current version, so a bitemporal collection scans.
        self.for_each_current_row(did, tid, collection, |id, raw| {
            let (id_s, mp) = sparse_row_to_doc(id, raw, format.as_format_ref(), &identity_column);
            f(&id_s, &mp).map(|()| std::ops::ControlFlow::Continue(()))
        })
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

    /// Scan plain/spatial columnar rows → standard msgpack: flushed
    /// segments, then the live memtable. A bitemporal collection scans each
    /// key's current version only.
    fn scan_columnar(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let columnar_key = (
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let Some(engine) = self.columnar_engines.get(&columnar_key) else {
            return Ok(Vec::new());
        };

        let schema = engine.schema();
        let version_gate = self.columnar_version_gate(&columnar_key, None, false)?;
        let current = |row: &[nodedb_types::value::Value]| {
            version_gate.as_ref().is_none_or(|gate| gate.admits(row))
        };
        let mut results = Vec::new();

        // 1. Read from flushed segments (older rows drained from prior memtable flushes).
        if let Some(segments) = self.columnar_flushed_segments.get(&columnar_key) {
            for (seg_idx, seg_bytes) in segments.iter().enumerate() {
                if results.len() >= limit {
                    break;
                }
                let seg_id = seg_idx as u64 + 1;
                // A segment that does not read refuses the scan. Skipping it
                // would answer without the segment's rows.
                let segment = self.decode_flushed_segment(
                    collection,
                    seg_id,
                    seg_bytes,
                    schema.columns.len(),
                    "columnar_normalized_scan",
                )?;
                let delete_bm = engine.delete_bitmap(seg_id);
                for row_idx in 0..segment.row_count() {
                    if results.len() >= limit {
                        break;
                    }
                    // A tombstoned row is not a row any more.
                    if delete_bm.is_some_and(|bm| bm.is_deleted(row_idx as u32)) {
                        continue;
                    }
                    let row = segment.row(schema, row_idx)?;
                    if current(&row) {
                        results.push(columnar_row_to_doc(schema, row)?);
                    }
                }
            }
        }

        // 2. Read from the live memtable (most-recent rows not yet flushed).
        if results.len() < limit {
            for row in engine.scan_memtable_rows() {
                if results.len() >= limit {
                    break;
                }
                let row = row?;
                if current(&row) {
                    results.push(columnar_row_to_doc(schema, row)?);
                }
            }
        }

        Ok(results)
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
        // Each row at its current version, so a bitemporal collection scans.
        let docs = self.current_rows(did, tid, collection, limit)?;
        let database_id = crate::types::DatabaseId::new(did);
        let tenant_id = crate::types::TenantId::new(tid);
        let format = self.sparse_body_format(database_id, tenant_id, collection);
        let identity_column = self.identity_column(did, tid, collection);

        let mut normalized = Vec::with_capacity(docs.len());
        for (id, raw) in docs {
            normalized.push(sparse_row_to_doc(
                &id,
                &raw,
                format.as_format_ref(),
                &identity_column,
            ));
        }
        Ok(normalized)
    }
}

/// A positional columnar row as `(id, msgpack object)`: the `id` column's
/// text, or empty when the row has none, and the row keyed by column name.
fn columnar_row_to_doc(
    schema: &nodedb_types::columnar::ColumnarSchema,
    row: Vec<nodedb_types::value::Value>,
) -> crate::Result<(String, Vec<u8>)> {
    let mut id = String::new();
    let mut map = std::collections::HashMap::with_capacity(schema.columns.len());
    for (col_def, val) in schema.columns.iter().zip(row) {
        if col_def.name == "id"
            && let nodedb_types::value::Value::String(s) = &val
        {
            id.clone_from(s);
        }
        map.insert(col_def.name.clone(), val);
    }
    let mp =
        nodedb_types::value_to_msgpack(&nodedb_types::value::Value::Object(map)).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("encode normalized columnar row: {e}"),
            }
        })?;
    Ok((id, mp))
}

// The per-row shape converters live in `row_shape`, beside each other and
// away from the scan orchestration above. Re-exported here because every
// caller in this directory reaches them through `scan_normalize::` and the
// split must not be visible to them.
pub(in crate::data::executor) use super::row_shape::{
    decoded_col_to_value, kv_row_to_doc, sparse_body_to_msgpack, sparse_row_to_doc,
};

#[cfg(test)]
mod tests {
    /// Storage key for a small test surrogate, keyed by a mnemonic letter so
    /// call sites below read the same as the doc-id literals they replace.
    fn key(surrogate: u32) -> nodedb_types::StorageKey {
        nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(surrogate))
    }

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
        core.sparse.put(0, tid, coll, &key(1), raw_a).unwrap();
        core.sparse.put(0, tid, coll, &key(2), raw_b).unwrap();
        core.sparse.put(0, tid, coll, &key(3), raw_c).unwrap();

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
        use crate::engine::kv::test_support::row_surrogate;

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
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: 0,
                tenant_id: tid,
                collection: coll,
                key: b"a",
                value: &val,
                ttl_ms: 0,
                now_ms,
                surrogate: row_surrogate(b"a"),
            })
            .expect("a bound row writes");
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: 0,
                tenant_id: tid,
                collection: coll,
                key: b"b",
                value: &val,
                ttl_ms: 0,
                now_ms,
                surrogate: row_surrogate(b"b"),
            })
            .expect("a bound row writes");
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: 0,
                tenant_id: tid,
                collection: coll,
                key: b"c",
                value: &val,
                ttl_ms: 0,
                now_ms,
                surrogate: row_surrogate(b"c"),
            })
            .expect("a bound row writes");

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
        core.sparse
            .put(0, tid, coll, &key(4), b"{\"v\":4}")
            .unwrap();
        core.sparse
            .put(0, tid, coll, &key(1), b"{\"v\":1}")
            .unwrap();
        core.sparse
            .put(0, tid, coll, &key(3), b"{\"v\":3}")
            .unwrap();
        core.sparse
            .put(0, tid, coll, &key(2), b"{\"v\":2}")
            .unwrap();

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
        use crate::engine::kv::test_support::row_surrogate;

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
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: 0,
                tenant_id: tid,
                collection: coll,
                key: b"k3",
                value: &val,
                ttl_ms: 0,
                now_ms,
                surrogate: row_surrogate(b"k3"),
            })
            .expect("a bound row writes");
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: 0,
                tenant_id: tid,
                collection: coll,
                key: b"k1",
                value: &val,
                ttl_ms: 0,
                now_ms,
                surrogate: row_surrogate(b"k1"),
            })
            .expect("a bound row writes");
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: 0,
                tenant_id: tid,
                collection: coll,
                key: b"k4",
                value: &val,
                ttl_ms: 0,
                now_ms,
                surrogate: row_surrogate(b"k4"),
            })
            .expect("a bound row writes");
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: 0,
                tenant_id: tid,
                collection: coll,
                key: b"k2",
                value: &val,
                ttl_ms: 0,
                now_ms,
                surrogate: row_surrogate(b"k2"),
            })
            .expect("a bound row writes");

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
        core.sparse
            .put(0, tid, coll, &key(1), b"{\"v\":1}")
            .unwrap();
        core.sparse
            .put(0, tid, coll, &key(2), b"{\"v\":2}")
            .unwrap();

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
