// SPDX-License-Identifier: BUSL-1.1

//! Handler for `DocumentOp::Merge`: implements the MERGE statement execution.
//!
//! Execution model (mirroring SQL MERGE semantics):
//!
//! Phase 1: Build a join map from the source collection:
//!   source_join_value → source_document
//!
//! Phase 2: Walk all target rows.  For each target row:
//!   - If the source map has a matching entry, evaluate WHEN MATCHED arms in
//!     order; apply the first arm whose extra_predicate is satisfied.
//!   - If no source row matches, evaluate WHEN NOT MATCHED BY SOURCE arms.
//!
//! Phase 3: Walk source rows that had no target match.  Evaluate WHEN NOT
//!   MATCHED arms in order; apply the first whose extra_predicate is satisfied.

use tracing::debug;

use super::merge_helpers::{
    ApplyActionParams, ApplyInsertActionParams, apply_action, apply_insert_action, build_merged,
    find_arm, json_to_str,
};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::response_codec::encode_json;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::document::merge_types::{
    MergeClauseKind as MergeClauseKindOp, MergeClauseOp,
};

/// Parameters for `execute_merge`.
pub(in crate::data::executor) struct MergeParams<'a> {
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub clauses: &'a [MergeClauseOp],
    /// RESOLVE-ONLY read pass (orchestrator phase 1): classify without writing
    /// and return the NOT-MATCHED insert rows.
    pub resolve_only: bool,
    /// Control-Plane-pre-assigned surrogates for the NOT-MATCHED insert rows,
    /// keyed by source join value (orchestrator phase 3). `Some` selects the
    /// atomic verify-and-apply path; `None` (with `resolve_only == false`)
    /// selects the legacy per-row apply, retained only as a fallback — the
    /// in-transaction MERGE that once used it is now resolved + staged at
    /// statement time into concrete point ops
    /// (`control::server::shared::session::expander_stage`).
    pub resolved_inserts: Option<&'a [(String, u32)]>,
    /// Control-Plane-shipped source rows for cross-core MERGE. When `Some`, the
    /// source join-map is built from these pre-scanned
    /// `(source_doc_id, raw_stored_source_bytes)` rows instead of a local read
    /// of the source collection (whose vShard may live on a different core).
    /// `None` selects the legacy local-storage read (co-resident / in-txn
    /// buffered replay).
    pub source_rows: Option<&'a [(String, Vec<u8>)]>,
}

impl CoreLoop {
    /// Execute a MERGE statement.
    ///
    /// Three modes, selected by [`MergeParams`]:
    /// - `resolve_only` → [`Self::execute_merge_resolve`]: a read pass that
    ///   returns the NOT-MATCHED insert rows for Control-Plane surrogate
    ///   assignment (no writes).
    /// - `resolved_inserts.is_some()` → [`Self::execute_merge_apply`]: the
    ///   atomic apply with CP-assigned surrogates + resolve→apply drift verify.
    /// - otherwise → `execute_merge_legacy`: the per-row apply retained only as
    ///   a fallback (in-transaction MERGE is now expanded at COMMIT into
    ///   concrete point ops before it could reach this path).
    pub(in crate::data::executor) fn execute_merge(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: MergeParams<'_>,
    ) -> Response {
        if params.resolve_only {
            return self.execute_merge_resolve(task, tid, params);
        }
        if params.resolved_inserts.is_some() {
            return self.execute_merge_apply(task, tid, params);
        }
        self.execute_merge_legacy(task, tid, params)
    }

    /// Legacy per-row MERGE apply, retained only as a fallback. In-transaction
    /// MERGE (which formerly reached this via buffered replay) is now expanded
    /// at COMMIT into concrete point ops before dispatch.
    fn execute_merge_legacy(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: MergeParams<'_>,
    ) -> Response {
        let MergeParams {
            target_collection,
            source_collection,
            source_alias,
            target_join_col,
            source_join_col,
            clauses,
            resolve_only: _,
            resolved_inserts: _,
            source_rows,
        } = params;

        debug!(
            core = self.core_id,
            target = %target_collection,
            source = %source_collection,
            "merge"
        );

        // Phase 1: Build source join map.
        let source_map = match self.build_merge_source_map(
            task.request.database_id.as_u64(),
            tid,
            source_collection,
            source_join_col,
            source_rows,
        ) {
            Ok(m) => m,
            Err(e) => return self.response_error(task, e),
        };

        // Check strict schema for target.
        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            target_collection.to_string(),
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

        // Gate secondary-vector maintenance once for the whole statement so a
        // non-vector target collection pays nothing; the per-row UPDATE / DELETE
        // arms maintain the HNSW index only when this is set.
        let has_vectors =
            self.collection_has_vectors(task.request.database_id.as_u64(), tid, target_collection);

        // Collect all target doc IDs and their documents.
        let target_docs: Vec<(String, Vec<u8>)> = match self.collect_target_docs(
            task.request.database_id.as_u64(),
            tid,
            target_collection,
            task.request.txn_id,
        ) {
            Ok(docs) => docs,
            Err(e) => return self.response_error(task, e),
        };

        let mut affected = 0u64;
        // Track which source keys were matched against a target row.
        let mut matched_source_keys: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // Phase 2: process target rows.
        for (doc_id, bytes) in &target_docs {
            let target_doc = if let Some(ref schema) = strict_schema {
                match super::super::strict_format::binary_tuple_to_json(bytes, schema) {
                    Some(v) => v,
                    None => continue,
                }
            } else {
                match doc_format::decode_document(bytes) {
                    Some(v) => v,
                    None => continue,
                }
            };

            let join_val = target_doc
                .get(target_join_col)
                .map(json_to_str)
                .unwrap_or_default();

            if let Some(source_doc) = source_map.get(&join_val) {
                matched_source_keys.insert(join_val.clone());
                // Build merged document for predicate / expression evaluation.
                let merged = build_merged(&target_doc, source_doc, source_alias);
                // Find first MATCHED arm whose predicate is satisfied.
                if let Some(arm) = find_arm(clauses, MergeClauseKindOp::Matched, &merged) {
                    let db_id = task.request.database_id.as_u64();
                    match apply_action(
                        self,
                        ApplyActionParams {
                            database_id: db_id,
                            tid,
                            collection: target_collection,
                            doc_id,
                            target_doc: &target_doc,
                            source_doc,
                            source_alias,
                            clause: arm,
                            strict_schema: &strict_schema,
                            has_vectors,
                        },
                    ) {
                        Ok(true) => affected += 1,
                        Ok(false) => {}
                        Err(e) => return self.response_error(task, e),
                    }
                }
            } else {
                // No matching source row — check NOT MATCHED BY SOURCE arms.
                let merged = target_doc.clone();
                if let Some(arm) = find_arm(clauses, MergeClauseKindOp::NotMatchedBySource, &merged)
                {
                    let db_id = task.request.database_id.as_u64();
                    match apply_action(
                        self,
                        ApplyActionParams {
                            database_id: db_id,
                            tid,
                            collection: target_collection,
                            doc_id,
                            target_doc: &target_doc,
                            source_doc: &serde_json::Value::Null,
                            source_alias,
                            clause: arm,
                            strict_schema: &strict_schema,
                            has_vectors,
                        },
                    ) {
                        Ok(true) => affected += 1,
                        Ok(false) => {}
                        Err(e) => return self.response_error(task, e),
                    }
                }
            }
        }

        // Phase 3: source rows without a matching target row.
        for (src_key, src_doc) in &source_map {
            if matched_source_keys.contains(src_key.as_str()) {
                continue;
            }
            if let Some(arm) = find_arm(clauses, MergeClauseKindOp::NotMatched, src_doc) {
                match apply_insert_action(
                    self,
                    ApplyInsertActionParams {
                        database_id: task.request.database_id.as_u64(),
                        tid,
                        collection: target_collection,
                        source_doc: src_doc,
                        source_alias,
                        clause: arm,
                        strict_schema: &strict_schema,
                    },
                ) {
                    Ok(true) => affected += 1,
                    Ok(false) => {}
                    Err(e) => return self.response_error(task, e),
                }
            }
        }

        let result = serde_json::json!({ "affected": affected });
        match encode_json(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Resolve a collection's strict Binary-Tuple schema, if it is a strict
    /// document collection. `None` for schemaless collections.
    pub(in crate::data::executor) fn merge_strict_schema(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Option<nodedb_types::columnar::StrictSchema> {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        })
    }

    /// Collect every target row as `(doc_id, stored_bytes)` from a consistent
    /// read snapshot. Shared by the legacy walk and the orchestrated
    /// resolve/apply classification so both see the same target set.
    ///
    /// `txn_id` selects the read view. `None` (autocommit) reads committed base
    /// storage only — byte-identical to the pre-staging behavior. `Some(txn)`
    /// folds the transaction's staging overlay: a staged tombstone hides its base
    /// row, a staged put replaces the base body, and a staged put absent from
    /// base is appended — so an in-transaction MERGE resolved at COMMIT sees rows
    /// staged by earlier statements in the same transaction. The `doc_id` this
    /// produces is the hex surrogate, matching the overlay's surrogate keying, so
    /// staged and base bodies (same canonical stored form — Binary Tuple for a
    /// strict target, MessagePack for a schemaless one) are merged like-for-like
    /// and decoded identically downstream by `decode_target`.
    pub(in crate::data::executor) fn collect_target_docs(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        txn_id: Option<crate::types::TxnId>,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let prefix = crate::engine::sparse::btree::coll_prefix(database_id, tid, collection);
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("read txn: {e}"),
            })?;
        let table = read_txn
            .open_table(crate::engine::sparse::btree::DOCUMENTS)
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("open table: {e}"),
            })?;

        let mut docs = Vec::new();
        if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
            for entry in range.flatten() {
                let key = entry.0.value();
                let bytes = entry.1.value().to_vec();
                if let Some(doc_id) = key.strip_prefix(&prefix) {
                    docs.push((doc_id.to_string(), bytes));
                }
            }
        }

        // Read-your-own-writes: fold the transaction's staging overlay over the
        // base set. Collect-all predicate — MERGE classifies the whole target,
        // there is no scan filter to re-check. No-op when the transaction has no
        // overlay (or `txn_id` is `None`).
        if let Some(txn_id) = txn_id {
            let coll_key: (crate::types::DatabaseId, crate::types::TenantId, String) = (
                crate::types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );
            self.merge_overlay_into_scan(txn_id, &coll_key, &mut docs, &|_| true);
        }
        Ok(docs)
    }

    /// Build the source join map `join_val → document`.
    ///
    /// Two sources of source rows, selected by `source_rows`:
    /// - `Some(rows)` (cross-core): the Control Plane scanned the source on its
    ///   OWN Data-Plane core and shipped the RAW stored bytes here. This core
    ///   does not hold the source's storage, but `Register` is broadcast so it
    ///   DOES hold the source's strict schema — the shipped bytes are decoded
    ///   with the exact same schema-aware logic the local scan uses, so the
    ///   resulting map is byte-for-byte identical to a co-resident local read.
    /// - `None` (legacy co-resident / in-txn buffered replay): read the source
    ///   from this core's local storage.
    pub(in crate::data::executor) fn build_merge_source_map(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        join_col: &str,
        source_rows: Option<&[(String, Vec<u8>)]>,
    ) -> crate::Result<std::collections::HashMap<String, serde_json::Value>> {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
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

        // Decode one raw stored source document and extract its non-empty join
        // key. Shared by the shipped-rows path and the local-scan path so both
        // derive an identical `join_val → document` mapping from identical bytes.
        let decode_and_key = |value_bytes: &[u8]| -> Option<(String, serde_json::Value)> {
            let doc = match strict_schema.as_ref() {
                Some(schema) => {
                    super::super::strict_format::binary_tuple_to_json(value_bytes, schema)?
                }
                None => doc_format::decode_document(value_bytes)?,
            };
            let key = doc.get(join_col).map(json_to_str).unwrap_or_default();
            if key.is_empty() {
                return None;
            }
            Some((key, doc))
        };

        let mut map = std::collections::HashMap::new();

        if let Some(rows) = source_rows {
            for (_source_doc_id, value_bytes) in rows {
                if let Some((key, doc)) = decode_and_key(value_bytes) {
                    map.insert(key, doc);
                }
            }
            return Ok(map);
        }

        let prefix = crate::engine::sparse::btree::coll_prefix(database_id, tid, collection);
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("read txn for merge source: {e}"),
            })?;
        let table = read_txn
            .open_table(crate::engine::sparse::btree::DOCUMENTS)
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("open merge source table: {e}"),
            })?;

        if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
            for entry in range.flatten() {
                if let Some((key, doc)) = decode_and_key(entry.1.value()) {
                    map.insert(key, doc);
                }
            }
        }
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use nodedb_types::Value;

    const DB: u64 = 0;
    const TID: u64 = 1;
    const SRC: &str = "merge_src";
    const JOIN: &str = "id";

    /// Build a schemaless source doc as the RAW stored bytes a plain insert
    /// would write (`nodedb_types::Value` msgpack).
    fn src_doc(id: &str, name: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert("id".to_string(), Value::String(id.into()));
        obj.insert("name".to_string(), Value::String(name.into()));
        nodedb_types::value_to_msgpack(&Value::Object(obj)).unwrap()
    }

    /// Write raw schemaless docs directly into a core's sparse DOCUMENTS table,
    /// mirroring the on-disk shape `build_merge_source_map`'s local scan reads.
    fn seed_source(core: &CoreLoop, rows: &[(&str, Vec<u8>)]) {
        use crate::engine::sparse::btree::{DOCUMENTS, coll_prefix};
        let prefix = coll_prefix(DB, TID, SRC);
        let txn = core.sparse.db().begin_write().unwrap();
        {
            let mut table = txn.open_table(DOCUMENTS).unwrap();
            for (doc_id, bytes) in rows {
                let key = format!("{prefix}{doc_id}");
                table.insert(key.as_str(), bytes.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    /// Cross-core MERGE source-shipping: the join-map the Data Plane builds from
    /// Control-Plane-shipped source rows on a core that does NOT hold the source
    /// locally is IDENTICAL to the map a co-resident local read produces — and
    /// WITHOUT the shipped rows that same non-owning core reads an empty map,
    /// which is exactly the silent-wrong-result the source-ship path fixes.
    #[test]
    fn shipped_source_rows_match_local_join_map() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let (core_a, _tx_a, _rx_a) = make_core_with_dir(dir_a.path());
        let (core_b, _tx_b, _rx_b) = make_core_with_dir(dir_b.path());

        let docs = vec![
            ("d1", src_doc("k1", "alpha")),
            ("d2", src_doc("k2", "bravo")),
            ("d3", src_doc("k3", "charlie")),
        ];
        // The source collection lives ONLY on core A (its owning core).
        seed_source(&core_a, &docs);

        // Co-resident (legacy) path on core A: read the source locally.
        let map_local = core_a
            .build_merge_source_map(DB, TID, SRC, JOIN, None)
            .unwrap();
        assert_eq!(map_local.len(), 3, "local read must see all source rows");

        // Cross-core: core B does NOT hold the source. A local read there is
        // empty — the exact silent-wrong-result the guard used to fail-close on.
        let map_b_local = core_b
            .build_merge_source_map(DB, TID, SRC, JOIN, None)
            .unwrap();
        assert!(
            map_b_local.is_empty(),
            "a non-owning core has no source rows to read locally"
        );

        // Ship core A's raw stored rows into core B's handler: the join-map now
        // matches core A's local map byte-for-byte.
        let shipped: Vec<(String, Vec<u8>)> = docs
            .iter()
            .map(|(id, b)| (id.to_string(), b.clone()))
            .collect();
        let map_b_shipped = core_b
            .build_merge_source_map(DB, TID, SRC, JOIN, Some(&shipped))
            .unwrap();

        assert_eq!(
            map_local, map_b_shipped,
            "shipped-source join-map must equal the co-resident local join-map"
        );
    }
}
