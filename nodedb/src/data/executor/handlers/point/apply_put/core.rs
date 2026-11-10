// SPDX-License-Identifier: BUSL-1.1

//! Shared "apply a PointPut inside an externally-owned transaction" helper.
//!
//! This is called by PointPut and by any composite path (triggers, UPSERT)
//! that needs document write + index + stats side-effects atomically.

use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;

use crate::data::executor::handlers::transaction::undo::UndoEntry;

use super::{
    PreparedPointPut,
    types::{PointPutOutcome, PointPutParams},
};

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
    /// Canonical document decoding and derived storage complete before the caller commits.
    pub(in crate::data::executor) fn apply_point_put(
        &mut self,
        txn: &WriteTransaction,
        params: PointPutParams<'_>,
    ) -> crate::Result<PointPutOutcome> {
        let prepared: PreparedPointPut<'_> = self.prepare_point_put(txn, params)?;
        self.apply_prepared_point_put(txn, prepared)
    }

    /// The in-memory index writes of one put: the R-tree, the vector
    /// indexes, and the sparse-vector indexes. Each step pushes an undo entry
    /// onto `undo` for every mutation it makes, also when it fails part-way.
    pub(super) fn apply_point_put_memory_indexes(
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
pub(super) struct MemoryIndexPut<'a> {
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: &'a str,
    pub(super) storage_key: crate::engine::document::store::StorageKey,
    pub(super) value: &'a [u8],
    pub(super) wal_lsn: u64,
    /// The row version's system-time ordinal: the index entries the write
    /// replaces end there, the ones it adds start there.
    pub(super) sys_from: i64,
    /// Whether the write replaces a stored row: the row's multi-vector
    /// documents end at `sys_from`, as an update ends them.
    pub(super) replaces_row: bool,
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
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
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
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
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
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
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
        outcome.installed.version.sys_from
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
