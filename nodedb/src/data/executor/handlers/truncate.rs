// SPDX-License-Identifier: BUSL-1.1

//! TRUNCATE and ESTIMATE_COUNT handlers.

use nodedb_physical::physical_plan::{ResolvedSumTarget, StorageMode};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::data::executor::enforcement::materialized_sum::divergence::SumTargetCheck;
use crate::data::executor::handlers::partial_refusal::refusal_after_rows;
use crate::data::executor::handlers::truncate_row::{TruncateProgress, TruncateScope};
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

/// Borrowed arguments for [`CoreLoop::execute_truncate`].
pub(in crate::data::executor) struct TruncateParams<'a> {
    pub collection: &'a str,
    /// Join-key VALUE → target row surrogate for every materialized-sum
    /// target the removed rows contribute to, resolved on the Control Plane.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    /// The collection's declared `PRIMARY KEY` column, when it has one. Names
    /// each removed row in its redo entry and delete event.
    pub declared_primary_key: Option<&'a str>,
}

impl CoreLoop {
    /// TRUNCATE: delete all documents in a collection without filter scanning.
    ///
    /// Removes every row's current version. A plain collection deletes each
    /// row. A bitemporal collection closes each row with a tombstone version
    /// and keeps its history. Cascades to the inverted index, secondary
    /// indexes, and document cache. The
    /// collection's graph edges are cut by `TruncateEdges`. Returns
    /// `{"truncated": N}` payload.
    ///
    /// Every removed row folds its own `RowImages::Delete` through the
    /// enforcement funnel, from inside this loop. There is deliberately NO bulk
    /// aggregate: TRUNCATE must leave the stored totals exactly where N
    /// individual deletes leave them, and a separate aggregate path is a second
    /// implementation of the same arithmetic — free to drift from the per-row
    /// one that every other delete path uses.
    pub(in crate::data::executor) fn execute_truncate(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: TruncateParams<'_>,
    ) -> Response {
        let TruncateParams {
            collection,
            resolved_sum_targets,
            declared_primary_key,
        } = params;
        debug!(core = self.core_id, %collection, "truncate");

        // Every row's current version: the newest version of each row on a
        // bitemporal collection, the plain table on any other.
        let database_id = task.request.database_id.as_u64();
        let all_ids = match self.current_row_keys(database_id, tid, collection) {
            Ok(keys) => keys,
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };

        // Materialized-sum coverage verification (LEADER-ONLY), identical in
        // contract to the bulk-DML paths: the resolution was derived from a
        // Control-Plane recon scan of this collection taken before execution, and
        // a row inserted since then debits a target the plan holds no surrogate
        // for. TRUNCATE must leave every bound total at exactly what N individual
        // deletes leave it at, so a shortfall returns OllpRetryRequired
        // WITHOUT removing anything rather than emptying the collection and
        // leaving a total that still counts its rows.
        if self.sum_targets_diverged_for_ids(
            &SumTargetCheck {
                database_id,
                tid,
                collection,
                // TRUNCATE assigns nothing: every removed row contributes the
                // join value it currently holds and no other.
                updates: &[],
                resolved: resolved_sum_targets,
            },
            &all_ids,
        ) {
            return self.response_error(task, ErrorCode::OllpRetryRequired);
        }

        let has_vectors = self.collection_has_vectors(database_id, tid, collection);

        // The stored pre-image is a Binary Tuple on a strict collection and
        // MessagePack otherwise. Hoisted once so each removed row's identity is
        // read through the matching decoder.
        let strict_schema = self
            .doc_configs
            .get(&(
                crate::types::DatabaseId::new(database_id),
                crate::types::TenantId::new(tid),
                collection.to_string(),
            ))
            .and_then(|c| match &c.storage_mode {
                StorageMode::Strict { schema } => Some(schema.clone()),
                StorageMode::Schemaless => None,
            });

        // BALANCED, decided over every row about to be removed and BEFORE the
        // first removal — each row below commits in its own transaction, so a
        // check after the loop cannot undo what it found. Emptying a
        // collection whose journals all balance nets to zero and proceeds;
        // emptying one that holds an unbalanced group is refused with nothing
        // removed.
        match self.balanced_entries_for_stored_deletes(database_id, tid, collection, &all_ids) {
            Ok(entries) => {
                if let Err(e) = self.settle_balanced_entries(database_id, tid, collection, entries)
                {
                    return self.response_error(task, e);
                }
            }
            Err(e) => return self.response_error(task, e),
        }

        // A bitemporal row is closed, never erased: its history stays readable
        // until the retention purge. One system time closes every row, so the
        // TRUNCATE removes them at one instant, as a `DELETE` with no `WHERE`.
        let bitemporal_sys_from = if self.is_bitemporal(database_id, tid, collection) {
            match self.rows_version_ordinal(database_id, tid, collection, &all_ids) {
                Ok(ordinal) => Some(ordinal),
                Err(e) => return self.response_error(task, ErrorCode::from(e)),
            }
        } else {
            None
        };
        let scope = TruncateScope {
            task,
            database_id,
            tid,
            collection,
            resolved_sum_targets,
            declared_primary_key,
            strict_schema: strict_schema.as_ref(),
            has_vectors,
            bitemporal_sys_from,
        };
        // Each document leaves with full cascade, one transaction per row.
        let mut progress = TruncateProgress::default();
        for storage_key in &all_ids {
            if let Err(code) = self.truncate_row(&scope, storage_key, &mut progress) {
                return self.truncate_refusal(
                    task,
                    tid,
                    collection,
                    code,
                    &progress.removed,
                    progress.write_set,
                );
            }
        }
        let TruncateProgress {
            truncated,
            write_set,
            removed: _,
        } = progress;

        // Every row is removed: empty the collection's inverted index in one
        // purge. Its analyzer, language, and fuzzy configuration stay.
        if let Err(e) = self.inverted.clear_collection(
            database_id,
            crate::types::TenantId::new(tid),
            collection,
        ) {
            // The rows are removed, but the index still holds their text. That
            // cannot be undone here, so the core fail-stops, as in
            // `truncate_refusal`. Restart replay rebuilds the index.
            self.fail_stop_core(
                FailStopCause::PostInstallFailed,
                &format!(
                    "TRUNCATE of '{collection}' removed {truncated} rows, then emptying its \
                     inverted index failed: {e}"
                ),
            );
            let code = refusal_after_rows(truncated, ErrorCode::from(e));
            return self.refusal_with_landed_rows(task, code, write_set);
        }

        // Clear aggregate cache for this collection.
        self.invalidate_aggregate_cache_for_collection(
            task.request.database_id.as_u64(),
            tid,
            collection,
        );

        debug!(core = self.core_id, %collection, truncated, "truncate complete");
        let result = serde_json::json!({ "truncated": truncated });
        // Every row is removed by now, so an encode error answers with the
        // removals' entries as well.
        let mut response = match response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        };
        response.write_set = write_set;
        response
    }

    /// The refusal of a TRUNCATE that stopped part-way. The rows removed so
    /// far stay removed, so their text leaves the inverted index in one batch
    /// before the refusal answers. The refusal keeps `code`, so the client
    /// sees the SQLSTATE of what stopped the TRUNCATE.
    ///
    /// A failure of the text removal leaves the inverted index holding text
    /// of removed rows. That cannot be undone here, so the core fail-stops.
    /// Restart replay applies the journalled removals and rebuilds the index.
    fn truncate_refusal(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        code: ErrorCode,
        removed: &[nodedb_types::Surrogate],
        write_set: Vec<WriteSetEntry>,
    ) -> Response {
        if let Err(e) = self.inverted.remove_documents(
            task.request.database_id.as_u64(),
            crate::types::TenantId::new(tid),
            collection,
            removed,
        ) {
            self.fail_stop_core(
                FailStopCause::PostInstallFailed,
                &format!(
                    "TRUNCATE of '{collection}' stopped after {} rows with {code:?}, then \
                     removing those rows' text from the inverted index failed: {e}",
                    removed.len()
                ),
            );
        }
        self.refusal_with_landed_rows(task, code, write_set)
    }

    /// ESTIMATE_COUNT: return approximate row count from HLL cardinality stats.
    pub(in crate::data::executor) fn execute_estimate_count(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field: &str,
    ) -> Response {
        match self
            .stats_store
            .get(task.request.database_id.as_u64(), tid, collection, field)
        {
            Ok(Some(stats)) => {
                let result = serde_json::json!({
                    "collection": collection,
                    "field": field,
                    "estimate": stats.distinct_count,
                    "row_count": stats.row_count,
                    "null_count": stats.null_count,
                });
                match response_codec::encode_json_as_msgpack(&result) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, ErrorCode::from(e)),
                }
            }
            Ok(None) => {
                let result = serde_json::json!({
                    "collection": collection,
                    "field": field,
                    "estimate": 0,
                    "row_count": 0,
                    "null_count": 0,
                });
                match response_codec::encode_json_as_msgpack(&result) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, ErrorCode::from(e)),
                }
            }
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    //! TRUNCATE on a bitemporal collection closes every current version and
    //! keeps the history before it.

    use super::*;
    use crate::bridge::envelope::{RowEffect, Status};
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;
    use crate::engine::document::store::{CollectionConfig, IndexPath};
    use crate::engine::sparse::btree_versioned::{VersionedIndexEntry, VersionedPut};
    use crate::types::{DatabaseId, TenantId};
    use nodedb_types::{StorageKey, Surrogate};

    const TID: u64 = 1;
    const LEDGER: &str = "ledger";
    /// The system time every seeded version lands at, before the TRUNCATE.
    const SEEDED_AT: i64 = 1_000;

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate(surrogate))
    }

    /// A bitemporal `ledger` with a secondary index on `kind`, holding rows
    /// 1 (`x`), 2 (`y`) and 3 (`x`) at `SEEDED_AT`.
    fn bitemporal_ledger(core: &mut CoreLoop, database_id: u64) {
        let mut config = CollectionConfig::new(LEDGER);
        config.bitemporal = true;
        config.index_paths.push(IndexPath::new("kind"));
        core.doc_configs.insert(
            (
                DatabaseId::new(database_id),
                TenantId::new(TID),
                LEDGER.to_string(),
            ),
            config,
        );
        for (surrogate, kind) in [(1, "x"), (2, "y"), (3, "x")] {
            let body = doc_format::encode_to_msgpack(&serde_json::json!({ "kind": kind }));
            core.sparse
                .versioned_put(VersionedPut {
                    database_id,
                    tenant: TID,
                    coll: LEDGER,
                    doc_id: &key(surrogate),
                    sys_from: SEEDED_AT,
                    valid_from_ms: i64::MIN,
                    valid_until_ms: i64::MAX,
                    body: &body,
                })
                .expect("seed version");
            core.sparse
                .versioned_index_put(VersionedIndexEntry {
                    database_id,
                    tenant: TID,
                    coll: LEDGER,
                    field: "kind",
                    value: kind,
                    doc_id: &key(surrogate),
                    sys_from: SEEDED_AT,
                })
                .expect("seed index entry");
        }
    }

    fn version_at(
        core: &CoreLoop,
        database_id: u64,
        surrogate: u32,
        sys_cutoff: Option<i64>,
    ) -> Option<Vec<u8>> {
        core.sparse
            .versioned_get_as_of(database_id, TID, LEDGER, &key(surrogate), sys_cutoff, None)
            .expect("read version")
    }

    #[test]
    fn truncate_closes_every_current_version_of_a_bitemporal_collection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let database_id = task.request.database_id.as_u64();
        bitemporal_ledger(&mut core, database_id);

        let response = core.execute_truncate(
            &task,
            TID,
            TruncateParams {
                collection: LEDGER,
                resolved_sum_targets: &[],
                declared_primary_key: None,
            },
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        let payload =
            nodedb_types::json_from_msgpack(response.payload.as_bytes()).expect("payload decodes");
        assert_eq!(payload["truncated"], serde_json::json!(3));
        for surrogate in [1, 2, 3] {
            assert!(
                version_at(&core, database_id, surrogate, None).is_none(),
                "row {surrogate} has no current version"
            );
            assert!(
                version_at(&core, database_id, surrogate, Some(SEEDED_AT)).is_some(),
                "row {surrogate} keeps its history"
            );
        }
        let current_x = core
            .sparse
            .versioned_index_lookup_as_of(database_id, TID, LEDGER, "kind", "x", None)
            .expect("index lookup");
        assert!(current_x.is_empty(), "the index tombstones every value");

        // Each removal journals its tombstone at one statement system time.
        let stamps: Vec<Option<i64>> = response
            .write_set
            .iter()
            .filter_map(|entry| match entry.effect {
                RowEffect::Delete { system_from } => Some(system_from),
                _ => None,
            })
            .collect();
        assert_eq!(stamps.len(), 3);
        let first = stamps[0].expect("a versioned tombstone");
        assert!(first > SEEDED_AT);
        assert!(stamps.iter().all(|stamp| *stamp == Some(first)));
    }
}
