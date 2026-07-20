// SPDX-License-Identifier: BUSL-1.1

//! Shared match-and-resolve pass for `DocumentOp::UpdateFromJoin`.
//!
//! Split out of `update_from_join.rs` to keep each file within the size limit.
//! Scans the target collection, joins each row against the pre-built source
//! join-map, evaluates the `SET` assignments against the merged document,
//! recomputes generated columns, and encodes each matched row's post-image —
//! WITHOUT touching storage. Both the write path and the COMMIT-time RESOLVE
//! pass consume the resulting [`ResolvedUpdateRow`]s, so the two can never
//! diverge on which rows match or what post-image each carries (mirrors how
//! `collect_merge_plan` is shared between the MERGE resolve and apply passes).

use std::collections::HashMap;

use nodedb_types::columnar::StrictSchema;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::update_from_join_source_map::json_value_to_string;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};
use nodedb_physical::physical_plan::UpdateValue;

use super::update_from_join::ResolvedUpdateRow;

/// Borrowed inputs for [`CoreLoop::collect_update_from_join_rows`], bundled to
/// keep the shared classifier's signature within argument limits.
pub(in crate::data::executor) struct CollectUpdateRows<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub target_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub updates: &'a [(String, UpdateValue)],
    pub source_map: &'a HashMap<String, serde_json::Value>,
    pub target_filters: &'a [ScanFilter],
    pub strict_schema: Option<&'a StrictSchema>,
    pub config_key: &'a (DatabaseId, TenantId, String),
}

/// Borrowed inputs for [`CoreLoop::scan_target_rows`], bundled to keep the
/// overlay-aware target scan within argument limits.
struct ScanTargetRows<'a> {
    database_id: u64,
    tid: u64,
    target_collection: &'a str,
    target_filters: &'a [ScanFilter],
    strict_schema: Option<&'a StrictSchema>,
    txn_id: Option<TxnId>,
    target_coll_key: &'a (DatabaseId, TenantId, String),
}

impl CoreLoop {
    /// Resolve every target row matched by the join into its post-image without
    /// writing. Shared by the write path and the RESOLVE pass.
    pub(in crate::data::executor) fn collect_update_from_join_rows(
        &self,
        ctx: CollectUpdateRows<'_>,
    ) -> crate::Result<Vec<ResolvedUpdateRow>> {
        let CollectUpdateRows {
            task,
            tid,
            target_collection,
            source_alias,
            target_join_col,
            updates,
            source_map,
            target_filters,
            strict_schema,
            config_key,
        } = ctx;
        let database_id = task.request.database_id.as_u64();
        // Read the TARGET as the transaction's CURRENT view = base ∪ overlay:
        // `None` (autocommit write path) is base-only; `Some(txn)` (COMMIT-time
        // RESOLVE) folds rows staged earlier in the same transaction.
        let txn_id = task.request.txn_id;
        let target_coll_key: (DatabaseId, TenantId, String) = (
            task.request.database_id,
            TenantId::new(tid),
            target_collection.to_string(),
        );

        // Scan the target collection for rows passing the target-only filters,
        // folded with the transaction's staging overlay. The scan already yields
        // each matched row's CURRENT body (overlay put superseding base, staged
        // insert appended), so the body is used directly — a base `sparse.get`
        // would miss a row this transaction only staged.
        let target_rows = self.scan_target_rows(ScanTargetRows {
            database_id,
            tid,
            target_collection,
            target_filters,
            strict_schema,
            txn_id,
            target_coll_key: &target_coll_key,
        })?;

        let mut rows: Vec<ResolvedUpdateRow> = Vec::new();
        for (doc_id, current_bytes) in target_rows {
            let mut target_doc = if let Some(schema) = strict_schema {
                match super::super::strict_format::binary_tuple_to_json(&current_bytes, schema) {
                    Some(v) => v,
                    None => continue,
                }
            } else {
                match doc_format::decode_document(&current_bytes) {
                    Some(v) => v,
                    None => continue,
                }
            };

            // Extract the join key from the target document.
            let join_val = target_doc
                .get(target_join_col)
                .map(json_value_to_string)
                .unwrap_or_default();

            // Look up the matching source row.
            let source_doc = match source_map.get(&join_val) {
                Some(s) => s,
                None => continue, // No matching source row — skip this target row.
            };

            // Build a merged document for expression evaluation:
            // target fields are bare; source fields are qualified as "alias.field".
            let mut merged = target_doc.clone();
            if let (Some(merged_obj), Some(src_obj)) =
                (merged.as_object_mut(), source_doc.as_object())
            {
                for (k, v) in src_obj {
                    merged_obj.insert(format!("{source_alias}.{k}"), v.clone());
                }
            }
            let merged_ndb: nodedb_types::Value = merged.clone().into();

            // Apply SET assignments evaluated against the merged document.
            if let Some(target_obj) = target_doc.as_object_mut() {
                for (field, update_val) in updates {
                    let val: serde_json::Value = match update_val {
                        UpdateValue::Literal(bytes) => match nodedb_types::json_from_msgpack(bytes)
                        {
                            Ok(v) => v,
                            Err(_) => continue,
                        },
                        UpdateValue::Expr(expr) => expr.eval(&merged_ndb).into(),
                    };
                    target_obj.insert(field.clone(), val);
                }
            }

            // Recompute generated columns if any dependency changed.
            if let Some(config) = self.doc_configs.get(config_key)
                && !config.enforcement.generated_columns.is_empty()
                && super::generated::needs_recomputation(
                    updates,
                    &config.enforcement.generated_columns,
                )
                && let Err(e) = super::generated::evaluate_generated_columns(
                    &mut target_doc,
                    &config.enforcement.generated_columns,
                )
            {
                tracing::warn!(
                    %doc_id,
                    error = ?e,
                    "generated column recomputation failed during UpdateFromJoin, skipping"
                );
                continue;
            }

            // Re-encode the post-image (strict Binary Tuple or MessagePack).
            let updated_bytes = if let Some(schema) = strict_schema {
                let ndb_val: nodedb_types::Value = target_doc.clone().into();
                match super::super::strict_format::value_to_binary_tuple(&ndb_val, schema) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        tracing::warn!(
                            %doc_id,
                            error = %e,
                            "strict re-encode failed during UpdateFromJoin, skipping"
                        );
                        continue;
                    }
                }
            } else {
                doc_format::encode_to_msgpack(&target_doc)
            };

            // The storage key is the hex-encoded surrogate on a surrogate-keyed
            // row; parse it once here for the reindex + write-set (write path)
            // and the expanded `PointPut`'s identity (RESOLVE path).
            let surrogate = crate::engine::document::store::doc_id_to_surrogate(&doc_id);
            rows.push(ResolvedUpdateRow {
                doc_id,
                surrogate,
                body: updated_bytes,
                old_body: current_bytes,
                doc: target_doc,
            });
        }
        Ok(rows)
    }

    /// Range-scan the target collection, returning each row that passes every
    /// target-only filter as `(doc_id, current_stored_body)` — decoding strict
    /// Binary Tuples to JSON for filter evaluation when the target is
    /// strict-mode. The body is the row's CURRENT stored form (strict Binary
    /// Tuple or MessagePack), returned so the caller need not re-fetch it.
    ///
    /// When `txn_id` is `Some`, the transaction's staging overlay is folded over
    /// the base result: a staged tombstone hides its base row, a staged put
    /// replaces the base body (re-checked against the SAME target filters via the
    /// strict-aware matcher), and a staged put absent from base is appended when
    /// it passes the filters. `None` (autocommit) returns the base-filtered rows
    /// unchanged — byte-identical to the pre-staging behavior.
    fn scan_target_rows(&self, args: ScanTargetRows<'_>) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let ScanTargetRows {
            database_id,
            tid,
            target_collection,
            target_filters,
            strict_schema,
            txn_id,
            target_coll_key,
        } = args;
        let prefix = crate::engine::sparse::btree::coll_prefix(database_id, tid, target_collection);
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

        let mut rows: Vec<(String, Vec<u8>)> = Vec::new();
        if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
            for entry in range.flatten() {
                let key = entry.0.value();
                let value_bytes = entry.1.value();
                let matches = if let Some(schema) = strict_schema {
                    match super::super::strict_format::binary_tuple_to_json(value_bytes, schema) {
                        Some(doc) => {
                            let msgpack = doc_format::encode_to_msgpack(&doc);
                            target_filters.iter().all(|f| f.matches_binary(&msgpack))
                        }
                        None => false,
                    }
                } else {
                    target_filters.iter().all(|f| f.matches_binary(value_bytes))
                };
                if matches && let Some(doc_id) = key.strip_prefix(&prefix) {
                    rows.push((doc_id.to_string(), value_bytes.to_vec()));
                }
            }
        }

        // Read-your-own-writes: fold the transaction's staging overlay over the
        // base-filtered rows. The overlay's staged bodies are the same canonical
        // stored form as base bodies, so the strict-aware matcher re-checks a
        // staged put against the same target filters — a staged insert/update
        // that satisfies the predicate is surfaced, one that no longer does is
        // dropped, exactly as for a base row.
        if let Some(txn_id) = txn_id {
            let matches =
                self.strict_aware_matcher(database_id, tid, target_collection, target_filters);
            self.merge_overlay_into_scan(txn_id, target_coll_key, &mut rows, &matches);
        }
        Ok(rows)
    }
}
