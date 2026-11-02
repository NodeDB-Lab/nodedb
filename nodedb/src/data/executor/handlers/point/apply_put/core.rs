// SPDX-License-Identifier: BUSL-1.1

//! Shared "apply a PointPut inside an externally-owned transaction" helper.
//!
//! This is called by PointPut and by any composite path (triggers, UPSERT)
//! that needs document write + index + stats side-effects atomically.

use redb::WriteTransaction;
use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;

use crate::data::executor::enforcement::unique::{
    PostImage, UniqueJudge, UniqueScope, check_unique_post_state,
};
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::handlers::transaction::undo::memory::abort_error;

use super::enforce::PutEnforcement;
use super::types::{PointPutOutcome, PointPutParams};

impl CoreLoop {
    /// Apply a PointPut within an externally-owned WriteTransaction. Stores
    /// the document, auto-indexes text, updates stats, populates the doc
    /// cache. Does NOT commit — on `Err` the caller MUST drop `txn`
    /// uncommitted, or a row publishes with indexes nothing re-derives.
    ///
    /// The R-tree, vector and sparse writes are in memory, so dropping `txn`
    /// does not reverse them. On `Err` this function has
    /// already reversed its own. On `Ok` they come back as
    /// `PointPutOutcome::memory_undo`: a caller that abandons the write after
    /// this returns reverses them with `undo_memory_effects`.
    ///
    /// `value` always arrives WITH the write (never a row read back to
    /// reconcile), so a `decode_document(value)` guard below that fails
    /// quietly is an intentional "no fields to derive from", not a swallowed
    /// error — except once `value` becomes STORED state (`old_value`,
    /// enforcement pre-image), where a decode failure is corruption and
    /// propagates instead.
    pub(in crate::data::executor) fn apply_point_put(
        &mut self,
        txn: &WriteTransaction,
        params: PointPutParams<'_>,
    ) -> crate::Result<PointPutOutcome> {
        let PointPutParams {
            database_id,
            tid,
            collection,
            storage_key,
            surrogate,
            value,
            index_text,
            user_roles,
            enforce,
            unique,
            wal_lsn,
            resolved_targets,
            guards,
        } = params;
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );

        // Every write appends a version of the row. A stamp in
        // `apply_scope.bitemporal_stamps` (WAL redo replay, a committed
        // redo install) fixes the version at the EXACT stamp the live write
        // used, independent of `doc_configs` (empty during replay). Absent a
        // stamp the write takes its system-time ordinal, raised above the
        // row's newest version. A `bitemporal=true` collection carries the
        // stamp into its strict tuple's temporal columns. Any other
        // collection's body carries no temporal column.
        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        let carried = self
            .apply_scope
            .bitemporal_stamps
            .get(&surrogate.as_u32())
            .copied();
        let (stamped, sys_from, valid_from_ms, valid_until_ms) = match carried {
            Some(stamp) => (
                true,
                stamp.sys_from,
                stamp.valid_from_ms,
                stamp.valid_until_ms,
            ),
            None => (
                bitemporal,
                self.row_version_ordinal(database_id, tid, collection, &storage_key)?,
                i64::MIN,
                i64::MAX,
            ),
        };

        // Shared with the governed-write RESOLVE pass.
        let body = self.build_stored_body(super::stored_body::StoredBodyInput {
            config_key: &config_key,
            surrogate,
            value,
            bitemporal: stamped,
            sys_from,
            valid_from_ms,
            valid_until_ms,
            guards,
        })?;
        let stored = body.stored;
        let value: &[u8] = &body.value;

        // A carried stamp below the row's newest version installs history
        // only. A fresh ordinal is raised above the newest version, so only
        // a carried one can land below it.
        if carried.is_some()
            && let Some(slot) =
                self.history_slot_in_txn(txn, database_id, tid, collection, &storage_key, sys_from)?
        {
            self.check_stateless_put_enforcement(
                enforce,
                PutEnforcement {
                    config_key: &config_key,
                    database_id,
                    tid,
                    collection,
                    value,
                    old_value: &slot.predecessor,
                    user_roles,
                    resolved_targets,
                },
            )?;
            return self.apply_history_put(
                txn,
                super::history::HistoryPut {
                    database_id,
                    tid,
                    collection,
                    storage_key: &storage_key,
                    value,
                    stored,
                    sys_from,
                    valid_from_ms,
                    valid_until_ms,
                    slot,
                },
            );
        }

        // The row's current body: the prior value the event reports, the
        // enforcement pre-image, and the old index values the write moves
        // off.
        let old_value = self
            .sparse
            .get_in_txn(txn, database_id, tid, collection, &storage_key)?;
        // Pre-write document for the secondary-index SET diff, routed
        // through the storage-mode-aware helper so a strict row's Binary
        // Tuple decodes too. `None` means "no prior row" or "no index
        // paths". A prior row that exists but fails to decode fails the
        // write instead, or its old index entries stay live forever.
        let old_doc_for_index: Option<serde_json::Value> =
            match (old_value.as_ref(), self.doc_configs.get(&config_key)) {
                (Some(b), Some(config)) if !config.index_paths.is_empty() => {
                    Some(self.decode_stored_document(config, b)?)
                }
                _ => None,
            };

        // Runs before any store or index is touched, so a refusal leaves
        // nothing behind.
        self.check_stateless_put_enforcement(
            enforce,
            PutEnforcement {
                config_key: &config_key,
                database_id,
                tid,
                collection,
                value,
                old_value: &old_value,
                user_roles,
                resolved_targets,
            },
        )?;

        self.sparse.versioned_put_in_txn(
            txn,
            crate::engine::sparse::btree_versioned::VersionedPut {
                database_id,
                tenant: tid,
                coll: collection,
                doc_id: &storage_key,
                sys_from,
                valid_from_ms,
                valid_until_ms,
                body: &stored,
            },
        )?;
        let prior = old_value;

        // Pre-image capture for the column-stats read-modify-write, so a
        // transactional caller can restore the exact prior stats on rollback.
        let mut stats_prior: Vec<crate::engine::sparse::stats::StatsPreImage> = Vec::new();

        // Text indexing and stats use the JSON input, not the stored bytes —
        // Binary Tuple needs a schema to decode. A body with no readable
        // fields contributes nothing here; once it IS readable, an
        // inverted-index write failure is real and rejects the write.
        if let Ok(doc) = doc_format::decode_document(value) {
            // Shared with the DELETE-rollback re-index path.
            let text_content = crate::data::executor::fts_text::extract_fts_fields(&doc);
            // Empty text is NOT skipped — stripping every indexable word
            // must still remove the document from the index.
            if index_text {
                // Lands in the caller's transaction, so propagating an error
                // rolls row + index back together as one unit. Swallowing it
                // would be permanent: no WAL record to replay, so the gap
                // stays invisible to full-text search until a manual reindex.
                if let Err(e) = self.inverted.index_document_in_txn(
                    txn,
                    crate::engine::sparse::inverted::IndexDocScope {
                        database_id,
                        tid: crate::types::TenantId::new(tid),
                        collection,
                        surrogate,
                        ordinal: sys_from,
                    },
                    &text_content,
                ) {
                    // Recorded here, at the detection site — an fsync'd
                    // report survives a restart, unlike a log line.
                    crate::diag::fts_index_update_failed(&e, collection, surrogate.as_u32());
                    warn!(core = self.core_id, %collection, %storage_key, error = %e, "inverted index update failed; rejecting the write");
                    return Err(e);
                }
            }

            match self
                .stats_store
                .observe_document_in_txn(txn, database_id, tid, collection, &doc)
            {
                Ok(pre) => stats_prior = pre,
                Err(e) => {
                    warn!(core = self.core_id, %collection, error = %e, "column stats update failed");
                }
            }

            self.invalidate_aggregate_cache_for_collection(database_id, tid, collection);
        }

        self.doc_cache.put(
            database_id,
            tid,
            collection,
            &storage_key,
            sys_from,
            &stored,
        );

        // Secondary index extraction into the caller's write txn — the
        // non-_in_txn variant would deadlock since `execute_point_put`
        // already owns the only writer. A `Row` UNIQUE probe runs first,
        // through a separate MVCC read txn that sees the committed index only.
        let mut index_tuples: Vec<(String, String)> = Vec::new();
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        // A collection with index paths indexes the body, and judges its
        // UNIQUE claims. A body that does not decode fails the write: skipping
        // it stores a row its indexes do not describe and whose claims
        // no one judged.
        let indexed = match self.doc_configs.get(&config_key) {
            Some(config) if !config.index_paths.is_empty() => Some((
                config.index_paths.clone(),
                doc_format::decode_document(value)?,
            )),
            _ => None,
        };
        if let Some((paths, doc)) = indexed {
            if unique == UniqueJudge::Row {
                check_unique_post_state(
                    &UniqueScope {
                        sparse: &self.sparse,
                        database_id,
                        tid,
                        collection,
                        paths: &paths,
                        base_visible: true,
                        crs: &self.crs_context(),
                    },
                    &[PostImage {
                        surrogate: storage_key.surrogate().as_u32(),
                        doc: Some(&doc),
                        judged: true,
                    }],
                )?;
            }
            // Keyed at the same `sys_from` as the row version, so one
            // `version_sys_from` in the undo entry reverses both. Values the
            // write drops are tombstoned, values it gains are added.
            index_tuples = self.apply_secondary_indexes_in_txn(
                txn,
                crate::data::executor::core_loop::maintenance::SecondaryIndexInputs {
                    database_id,
                    tid,
                    collection,
                    old_doc: old_doc_for_index.as_ref(),
                    new_doc: &doc,
                    doc_id: &storage_key,
                    index_paths: &paths,
                    sys_from,
                },
            )?;
        }

        // Every write below is in memory, where the caller's transaction
        // cannot reach it. A failure part-way reverses what already ran, so
        // an `Err` from this function leaves no in-memory index entry behind.
        let mut memory_undo: Vec<UndoEntry> = Vec::new();
        if let Err(e) = self.apply_point_put_memory_indexes(
            MemoryIndexPut {
                database_id,
                tid,
                collection,
                storage_key,
                value,
                wal_lsn: wal_lsn.map(|l| l.as_u64()).unwrap_or(0),
                sys_from,
                replaces_row: prior.is_some(),
            },
            &mut memory_undo,
        ) {
            let undone = self.undo_memory_effects(database_id, tid, memory_undo);
            return Err(abort_error(e, undone));
        }

        Ok(PointPutOutcome {
            prior_value: prior,
            stored_value: stored,
            version_sys_from: sys_from,
            index_tuples,
            memory_undo,
            stats_prior,
            history: None,
        })
    }

    /// The in-memory index writes of one put: the R-tree, the vector
    /// indexes, and the sparse-vector indexes. Each step pushes an undo entry
    /// onto `undo` for every mutation it makes, also when it fails part-way.
    fn apply_point_put_memory_indexes(
        &mut self,
        put: MemoryIndexPut<'_>,
        undo: &mut Vec<UndoEntry>,
    ) -> crate::Result<()> {
        let MemoryIndexPut {
            database_id,
            tid,
            collection,
            storage_key,
            value,
            wal_lsn,
            sys_from,
            replaces_row,
        } = put;
        self.apply_point_put_spatial(
            super::index::SpatialIndexPut {
                database_id,
                tid,
                collection,
                storage_key,
                value,
                sys_from,
            },
            undo,
        )?;
        self.apply_point_put_vector_indexes(
            crate::data::executor::handlers::point::apply_put::VectorIndexPutParams {
                database_id,
                tid,
                collection,
                storage_key,
                value,
                wal_lsn,
                sys_from,
                replaces_row,
            },
            undo,
        )?;
        // A body carries no multi-vector document, so a write that replaces
        // the row ends the row's documents, as a delete does.
        if replaces_row {
            undo.extend(
                self.end_document_multi_vectors(
                    database_id,
                    tid,
                    collection,
                    storage_key,
                    sys_from,
                )
                .into_iter()
                .map(super::VectorIndexDelta::into_delete_undo),
            );
        }
        // No-op unless the strict schema declares a `SparseVector` column.
        self.apply_point_put_sparse_indexes(
            super::SparseIndexPut {
                database_id,
                tid,
                collection,
                storage_key,
                value,
                sys_from,
            },
            undo,
        );
        Ok(())
    }
}

/// Inputs to `apply_point_put_memory_indexes`. `wal_lsn` is `0` when the
/// write carries no LSN.
struct MemoryIndexPut<'a> {
    database_id: u64,
    tid: u64,
    collection: &'a str,
    storage_key: crate::engine::document::store::StorageKey,
    value: &'a [u8],
    wal_lsn: u64,
    /// The row version's system-time ordinal: the index entries the write
    /// replaces end there, the ones it adds start there.
    sys_from: i64,
    /// Whether the write replaces a stored row: the row's multi-vector
    /// documents end at `sys_from`, as an update ends them.
    replaces_row: bool,
}

#[cfg(test)]
mod tests {
    use redb::TableDefinition;

    use crate::bridge::envelope::{Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::enforcement::unique::UniqueJudge;
    use crate::data::executor::handlers::point::apply_put::PointPutParams;
    use crate::data::executor::handlers::point::put::PointPutExec;
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::document::store::StorageKey;
    use crate::engine::sparse::fts_redb::tables::DOC_LENGTHS;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_types::Surrogate;
    use std::time::{Duration, Instant};

    const TID: u64 = 1;
    const COLL: &str = "articles";
    const SURROGATE: Surrogate = Surrogate(7);
    /// Raw JSON body — `doc_format::decode_document`'s JSON fallback accepts
    /// it, and its single string field is what `extract_fts_fields` feeds the
    /// inverted index, so this document has real text to index.
    const BODY: &[u8] = br#"{"title":"alpha bravo charlie"}"#;

    /// A table sharing `DOC_LENGTHS`'s redb name but incompatible key/value
    /// types, so `index_document_in_txn` fails deterministically with no mock.
    const POISONED_DOC_LENGTHS: TableDefinition<u64, u64> =
        TableDefinition::new("text.doc_lengths");

    /// Swap the real `DOC_LENGTHS` table for the type-mismatched one so every
    /// subsequent inverted-index write fails.
    fn poison_inverted_index(core: &CoreLoop) {
        let db = core.sparse.db().clone();
        let txn = db.begin_write().unwrap();
        txn.delete_table(DOC_LENGTHS).unwrap();
        txn.open_table(POISONED_DOC_LENGTHS).unwrap();
        txn.commit().unwrap();
    }

    fn row_key() -> String {
        StorageKey::for_surrogate(SURROGATE).to_string()
    }

    fn point_put_task(row_key: &str) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
                document_id: row_key.into(),
                value: BODY.to_vec(),
                surrogate: SURROGATE,
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            system_ordinal: None,
            admission: crate::bridge::envelope::Admission::Admitted,
        })
    }

    fn stored_row(core: &CoreLoop, row_key: &str) -> Option<Vec<u8>> {
        let key = crate::engine::document::store::StorageKey::parse(row_key)
            .expect("test row_key is always a rendered storage key");
        core.sparse
            .get(DatabaseId::DEFAULT.as_u64(), TID, COLL, &key)
            .unwrap()
    }

    /// Control: a healthy index commits and counts the document, so a
    /// failure test can tell poison-caused-rejection from never-writable.
    #[test]
    fn healthy_index_commits_the_row_and_indexes_it() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let row_key = row_key();

        let task = point_put_task(&row_key);
        let resp = core.execute_point_put(
            &task,
            PointPutExec {
                tid: TID,
                collection: COLL,
                document_id: &row_key,
                surrogate: SURROGATE,
                value: BODY,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );

        assert_eq!(resp.status, Status::Ok);
        assert!(stored_row(&core, &row_key).is_some(), "row must be stored");
        let (doc_count, _avg_len) = core
            .inverted
            .corpus_stats(DatabaseId::DEFAULT.as_u64(), TenantId::new(TID), COLL)
            .unwrap();
        assert_eq!(doc_count, 1, "the committed row must be in the FTS corpus");
    }

    /// An inverted-index failure must reject the write outright, with no row
    /// left in the store or the document cache.
    #[test]
    fn index_failure_rejects_the_write_and_leaves_no_row() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let row_key = row_key();
        poison_inverted_index(&core);

        let task = point_put_task(&row_key);
        let resp = core.execute_point_put(
            &task,
            PointPutExec {
                tid: TID,
                collection: COLL,
                document_id: &row_key,
                surrogate: SURROGATE,
                value: BODY,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
            },
        );

        assert_eq!(
            resp.status,
            Status::Error,
            "the client must be told the write failed, not receive a silent ack"
        );
        assert!(
            stored_row(&core, &row_key).is_none(),
            "the rejected write must leave no committed row — a stored row whose \
             index update failed is invisible to full-text search forever"
        );
        let key = crate::engine::document::store::StorageKey::parse(&row_key)
            .expect("test row_key is always a rendered storage key");
        assert!(
            core.doc_cache
                .get(DatabaseId::DEFAULT.as_u64(), TID, COLL, &key)
                .is_none(),
            "the rejected write must not populate the document cache either, or \
             reads would serve a row that is not in durable storage"
        );
    }

    /// Every caller of `apply_point_put` depends on the error surfacing
    /// instead of being absorbed, so its transaction drops uncommitted.
    #[test]
    fn apply_point_put_propagates_index_failure_instead_of_absorbing_it() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let row_key = row_key();
        poison_inverted_index(&core);

        let txn = core.sparse.begin_write().unwrap();
        let result = core.apply_point_put(
            &txn,
            PointPutParams {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tid: TID,
                collection: COLL,
                storage_key: crate::engine::document::store::StorageKey::for_surrogate(SURROGATE),
                surrogate: SURROGATE,
                value: BODY,
                index_text: true,
                user_roles: &[],
                enforce: true,
                unique: UniqueJudge::Row,
                wal_lsn: None,
                resolved_targets: &[],
                guards: crate::data::executor::enforcement::typeguard::GuardScope::Document,
            },
        );

        assert!(
            result.is_err(),
            "an inverted-index failure must propagate to the caller"
        );
        // Dropping the transaction un-committed is exactly what every caller
        // does on this error, and it is what makes row + index one unit.
        drop(txn);
        assert!(
            stored_row(&core, &row_key).is_none(),
            "aborting the shared transaction must roll the row body back too"
        );
    }

    /// `index_text: false` (CRDT-sync materialization, which receives its text
    /// through a separate FTS frame) must stay unaffected: it never calls the
    /// index at all, so a broken index cannot block it.
    #[test]
    fn index_text_disabled_is_unaffected_by_a_broken_index() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let row_key = row_key();
        poison_inverted_index(&core);

        let txn = core.sparse.begin_write().unwrap();
        let result = core.apply_point_put(
            &txn,
            PointPutParams {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tid: TID,
                collection: COLL,
                storage_key: crate::engine::document::store::StorageKey::for_surrogate(SURROGATE),
                surrogate: SURROGATE,
                value: BODY,
                index_text: false,
                user_roles: &[],
                enforce: true,
                unique: UniqueJudge::Row,
                wal_lsn: None,
                resolved_targets: &[],
                guards: crate::data::executor::enforcement::typeguard::GuardScope::Document,
            },
        );

        assert!(
            result.is_ok(),
            "a put that does not index must not be gated"
        );
        txn.commit().unwrap();
        assert!(stored_row(&core, &row_key).is_some());
    }

    /// Apply one put of `body` to row `SURROGATE` and commit it. Returns the
    /// version's system-time ordinal.
    fn put_committed(core: &mut CoreLoop, body: &[u8]) -> i64 {
        let txn = core.sparse.begin_write().unwrap();
        let outcome = core
            .apply_point_put(
                &txn,
                PointPutParams {
                    database_id: DatabaseId::DEFAULT.as_u64(),
                    tid: TID,
                    collection: COLL,
                    storage_key: StorageKey::for_surrogate(SURROGATE),
                    surrogate: SURROGATE,
                    value: body,
                    index_text: false,
                    user_roles: &[],
                    enforce: true,
                    unique: UniqueJudge::Row,
                    wal_lsn: None,
                    resolved_targets: &[],
                    guards: crate::data::executor::enforcement::typeguard::GuardScope::Document,
                },
            )
            .unwrap();
        txn.commit().unwrap();
        outcome.version_sys_from
    }

    /// A collection that is not bitemporal keeps every version: a read at
    /// the first version's ordinal sees the first body and the first index
    /// value, and the current read sees the overwrite.
    #[test]
    fn an_overwrite_keeps_the_prior_version_and_its_index_value() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string()),
            crate::engine::document::store::CollectionConfig::new(COLL).with_index("city"),
        );
        let first = put_committed(&mut core, br#"{"city":"oslo"}"#);
        let second = put_committed(&mut core, br#"{"city":"rome"}"#);
        assert!(second > first, "each write stamps a later version");

        let db = DatabaseId::DEFAULT.as_u64();
        let key = StorageKey::for_surrogate(SURROGATE);
        let city = |cut: Option<i64>| -> String {
            let body = core
                .sparse
                .versioned_get_as_of(db, TID, COLL, &key, cut, None)
                .unwrap()
                .expect("a visible version");
            crate::data::executor::doc_format::decode_document(&body).unwrap()["city"]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(city(Some(first)), "oslo");
        assert_eq!(city(None), "rome");

        let owners = |value: &str, cut: Option<i64>| {
            core.sparse
                .versioned_index_lookup_as_of(db, TID, COLL, "city", value, cut)
                .unwrap()
        };
        assert_eq!(owners("oslo", Some(first)), vec![key]);
        assert!(owners("rome", Some(first)).is_empty());
        assert!(
            owners("oslo", None).is_empty(),
            "the overwrite tombstones the old index value"
        );
        assert_eq!(owners("rome", None), vec![key]);
    }
}
