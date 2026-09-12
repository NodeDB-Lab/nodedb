// SPDX-License-Identifier: BUSL-1.1

//! Shared "apply a PointPut inside an externally-owned transaction" helper.
//!
//! This is called by PointPut and by any composite path (triggers, UPSERT)
//! that needs document write + index + stats side-effects atomically.

use redb::WriteTransaction;
use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;

use super::enforce::PutEnforcement;
use super::types::{PointPutOutcome, PointPutParams};
use super::unique::{UniqueCheck, check_unique_constraints};

impl CoreLoop {
    /// Apply a PointPut within an externally-owned WriteTransaction. Stores
    /// the document, auto-indexes text, updates stats, populates the doc
    /// cache. Does NOT commit — on `Err` the caller MUST drop `txn`
    /// uncommitted, or a row publishes with indexes nothing re-derives.
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
            document_id,
            surrogate,
            value,
            index_text,
            user_roles,
            enforce,
            wal_lsn,
            resolved_targets,
        } = params;
        // `surrogate` IS the storage key: no parse, no failure arm.
        let storage_key = crate::engine::document::store::StorageKey::for_surrogate(surrogate);
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );

        // A stamp in `active_bitemporal_stamps` (WAL redo replay) forces the
        // versioned branch at the EXACT redo stamp, independent of
        // `doc_configs` (empty during replay); absent an override, derive
        // bitemporality from config and mint a fresh stamp.
        let (bitemporal, sys_from_ms, valid_from_ms, valid_until_ms) =
            match self.active_bitemporal_stamps.get(&surrogate.as_u32()) {
                Some(stamp) => (
                    true,
                    stamp.sys_from_ms,
                    stamp.valid_from_ms,
                    stamp.valid_until_ms,
                ),
                None => (
                    self.is_bitemporal(database_id, tid, collection),
                    self.bitemporal_now_ms(),
                    i64::MIN,
                    i64::MAX,
                ),
            };

        // Shared with the governed-write RESOLVE pass.
        let body = self.build_stored_body(super::stored_body::StoredBodyInput {
            config_key: &config_key,
            surrogate,
            value,
            bitemporal,
            sys_from_ms,
            valid_from_ms,
            valid_until_ms,
        })?;
        let stored = body.stored;
        let value: &[u8] = &body.value;

        // Read the prior value only when needed: bitemporal (always), an
        // enforcement-configured collection (stateless PUT checks), or a
        // collection with index paths (drop the stale entry on UPDATE).
        let need_old = bitemporal
            || (enforce
                && self
                    .doc_configs
                    .get(&config_key)
                    .is_some_and(|config| config.enforcement.has_put_checks()))
            || self
                .doc_configs
                .get(&config_key)
                .is_some_and(|config| !config.index_paths.is_empty());
        let old_value = if bitemporal {
            self.sparse
                .versioned_get_current(database_id, tid, collection, &storage_key)?
        } else if need_old {
            self.sparse
                .get(database_id, tid, collection, &storage_key)?
        } else {
            None
        };
        // Pre-write doc for the non-bitemporal secondary-index SET diff;
        // bitemporal reverses via versioned index tuples instead. Routed
        // through the storage-mode-aware helper so a strict row's Binary
        // Tuple decodes too. `None` means "no prior row" (INSERT, bitemporal,
        // or no index paths) — a prior row that exists but fails to decode
        // fails the write instead, or its old index entries leak forever.
        let old_doc_for_index: Option<serde_json::Value> = if bitemporal {
            None
        } else {
            match (old_value.as_ref(), self.doc_configs.get(&config_key)) {
                (Some(b), Some(config)) => Some(self.decode_stored_document(config, b)?),
                _ => None,
            }
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

        // Bitemporal appends a new version; non-bitemporal overwrites in place.
        let prior = if bitemporal {
            self.sparse.versioned_put_in_txn(
                txn,
                crate::engine::sparse::btree_versioned::VersionedPut {
                    database_id,
                    tenant: tid,
                    coll: collection,
                    doc_id: &storage_key,
                    sys_from_ms,
                    valid_from_ms,
                    valid_until_ms,
                    body: &stored,
                },
            )?;
            old_value
        } else {
            self.sparse
                .put_in_txn(txn, database_id, tid, collection, &storage_key, &stored)?
        };

        // Pre-image capture for the column-stats read-modify-write, so a
        // transactional caller can restore the exact prior stats on rollback.
        let mut stats_prior: Vec<crate::engine::sparse::stats::StatsPreImage> = Vec::new();

        // Text indexing and stats use the JSON input, not the stored bytes —
        // Binary Tuple needs a schema to decode. A body with no readable
        // fields contributes nothing here; once it IS readable, an
        // inverted-index write failure is real and rejects the write.
        if let Ok(doc) = doc_format::decode_document(value) {
            // Shared with the DELETE-rollback re-index path.
            let text_content = crate::data::executor::fts_text::extract_fts_text(&doc);
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
                    },
                    &text_content,
                ) {
                    // Recorded here, at the detection site — an fsync'd
                    // report survives a restart, unlike a log line.
                    crate::diag::fts_index_update_failed(&e, collection, surrogate.as_u32());
                    warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index update failed; rejecting the write");
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

        self.doc_cache
            .put(database_id, tid, collection, &storage_key, &stored);

        // Secondary index extraction into the caller's write txn — the
        // non-_in_txn variant would deadlock since `execute_point_put`
        // already owns the only writer. UNIQUE enforcement runs first,
        // reading via a separate MVCC read txn that can't see our own writes
        // — exactly the semantics "does another row already hold this value" needs.
        let mut bitemporal_index_tuples: Vec<(String, String)> = Vec::new();
        let mut secondary_index_added: Vec<(String, String)> = Vec::new();
        let mut secondary_index_removed: Vec<(String, String)> = Vec::new();
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        // A body that isn't a document yields no index values, same outcome
        // as running the check and finding none.
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Ok(doc) = doc_format::decode_document(value)
        {
            let paths = config.index_paths.clone();
            // Must run in both autocommit and transactional paths.
            check_unique_constraints(UniqueCheck {
                sparse: &self.sparse,
                database_id,
                tid,
                collection,
                doc: &doc,
                document_id: &storage_key,
                paths: &paths,
                bitemporal,
            })?;
            if bitemporal {
                // Keyed at the same `sys_from_ms` as the primary version row,
                // so one `bitemporal_sys_from_ms` in the undo entry reverses both.
                for path in &paths {
                    if let Some(ref pred) = path.predicate
                        && !pred.evaluate_json(&doc)
                    {
                        continue;
                    }
                    for v in crate::engine::document::store::extract_index_values(
                        &doc,
                        &path.path,
                        path.is_array,
                    ) {
                        let value = if path.case_insensitive {
                            v.to_lowercase()
                        } else {
                            v
                        };
                        self.sparse.versioned_index_put_in_txn(
                            txn,
                            crate::engine::sparse::btree_versioned::VersionedIndexEntry {
                                database_id,
                                tenant: tid,
                                coll: collection,
                                field: &path.path,
                                value: &value,
                                doc_id: &storage_key,
                                sys_from_ms,
                            },
                        )?;
                        bitemporal_index_tuples.push((path.path.clone(), value));
                    }
                }
            } else {
                // SET diff against `old_doc_for_index` inserts new values and
                // removes stale ones; (added, removed) let a transactional
                // caller reverse them on rollback.
                let (added, removed) = self.apply_secondary_indexes_in_txn(
                    txn,
                    crate::data::executor::core_loop::maintenance::SecondaryIndexInputs {
                        database_id,
                        tid,
                        collection,
                        old_doc: old_doc_for_index.as_ref(),
                        new_doc: &doc,
                        doc_id: &storage_key,
                        index_paths: &paths,
                    },
                )?;
                secondary_index_added = added;
                secondary_index_removed = removed;
            }
        }

        let spatial_inserts =
            self.apply_point_put_spatial(database_id, tid, collection, document_id, value);
        let vector_inserts = self.apply_point_put_vector_indexes(
            crate::data::executor::handlers::point::apply_put::VectorIndexPutParams {
                database_id,
                tid,
                collection,
                document_id,
                surrogate,
                value,
                wal_lsn: wal_lsn.map(|l| l.as_u64()).unwrap_or(0),
            },
        )?;
        // No-op unless the strict schema declares a `SparseVector` column.
        self.apply_point_put_sparse_indexes(database_id, tid, collection, document_id, value);

        Ok(PointPutOutcome {
            prior_value: prior,
            stored_value: stored,
            bitemporal_sys_from_ms: if bitemporal { Some(sys_from_ms) } else { None },
            bitemporal_index_tuples,
            secondary_index_added,
            secondary_index_removed,
            vector_inserts,
            spatial_inserts,
            stats_prior,
        })
    }
}

#[cfg(test)]
mod tests {
    use redb::TableDefinition;

    use crate::bridge::envelope::{Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::point::apply_put::PointPutParams;
    use crate::data::executor::handlers::point::put::PointPutExec;
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::document::store::surrogate_to_doc_id;
    use crate::engine::sparse::fts_redb::tables::DOC_LENGTHS;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_types::Surrogate;
    use std::time::{Duration, Instant};

    const TID: u64 = 1;
    const COLL: &str = "articles";
    const SURROGATE: Surrogate = Surrogate(7);
    /// Raw JSON body — `doc_format::decode_document`'s JSON fallback accepts
    /// it, and its single string field is what `extract_fts_text` feeds the
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
        let row_key = surrogate_to_doc_id(SURROGATE);

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
        let row_key = surrogate_to_doc_id(SURROGATE);
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
        let row_key = surrogate_to_doc_id(SURROGATE);
        poison_inverted_index(&core);

        let txn = core.sparse.begin_write().unwrap();
        let result = core.apply_point_put(
            &txn,
            PointPutParams {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tid: TID,
                collection: COLL,
                document_id: &row_key,
                surrogate: SURROGATE,
                value: BODY,
                index_text: true,
                user_roles: &[],
                enforce: true,
                wal_lsn: None,
                resolved_targets: &[],
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
        let row_key = surrogate_to_doc_id(SURROGATE);
        poison_inverted_index(&core);

        let txn = core.sparse.begin_write().unwrap();
        let result = core.apply_point_put(
            &txn,
            PointPutParams {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tid: TID,
                collection: COLL,
                document_id: &row_key,
                surrogate: SURROGATE,
                value: BODY,
                index_text: false,
                user_roles: &[],
                enforce: true,
                wal_lsn: None,
                resolved_targets: &[],
            },
        );

        assert!(
            result.is_ok(),
            "a put that does not index must not be gated"
        );
        txn.commit().unwrap();
        assert!(stored_row(&core, &row_key).is_some());
    }
}
