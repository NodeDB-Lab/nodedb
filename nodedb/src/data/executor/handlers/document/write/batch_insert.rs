// SPDX-License-Identifier: BUSL-1.1

//! Atomic, fully-indexed document batch insert (`DocumentOp::BatchInsert`).

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::chain_guard::ChainGuard;
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_physical::physical_plan::{ResolvedSumTarget, ReturningSpec};

/// Parameters for [`CoreLoop::execute_document_batch_insert`].
pub(in crate::data::executor) struct DocumentBatchInsertParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub documents: &'a [(String, Vec<u8>)],
    pub surrogates: &'a [nodedb_types::Surrogate],
    /// When `Some`, return one row per inserted document — the STORED
    /// post-image of each, in `documents` order.
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this page may credit — one entry per DISTINCT join value across the
    /// batch. Resolved on the Control Plane at plan time.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    /// Materialized-sum TARGET collections whose delta the Control Plane
    /// settled at plan time and appended as its own `ApplyBalanceDelta` task,
    /// homed on the target's vShard. This page must not apply them as well.
    pub deferred_sum_targets: &'a [String],
}

impl CoreLoop {
    /// Insert a page of documents.
    ///
    /// Every index in the system — FTS, vector, spatial, and the secondary
    /// btree — is keyed by a row's global surrogate, so a batch is only
    /// insertable when it carries one surrogate per document. A batch that does
    /// not is refused here rather than stored: writing those rows would put
    /// documents in the collection that no index can ever return, while
    /// reporting the insert as successful. There is no partial answer to give —
    /// the surrogate list IS the rows' identity, and it is the part that is
    /// missing.
    pub(in crate::data::executor) fn execute_document_batch_insert(
        &mut self,
        task: &ExecutionTask,
        params: DocumentBatchInsertParams<'_>,
    ) -> Response {
        debug!(
            core = self.core_id,
            collection = %params.collection,
            count = params.documents.len(),
            "document batch insert"
        );

        if params.surrogates.len() != params.documents.len() {
            // Recorded here, at the detection site, and never re-emitted as the
            // rejection propagates: the malformation is upstream (a plan
            // builder, or a replicated write record that lost rows), and the
            // error the caller receives names only the symptom.
            crate::diag::batch_insert_without_surrogates(
                params.collection,
                params.documents.len(),
                params.surrogates.len(),
            );
            warn!(
                core = self.core_id,
                collection = %params.collection,
                doc_count = params.documents.len(),
                surrogate_count = params.surrogates.len(),
                "document batch insert without a surrogate per row; rejecting the batch"
            );
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "document batch insert for '{}' carries {} documents but {} \
                         surrogates; every cross-engine index is surrogate-keyed, so these \
                         rows cannot be indexed and are not written",
                        params.collection,
                        params.documents.len(),
                        params.surrogates.len(),
                    ),
                },
            );
        }

        self.execute_document_batch_insert_indexed(task, params)
    }

    /// Atomic, fully-indexed batch insert (surrogates parallel to documents).
    ///
    /// Applies every row through [`CoreLoop::apply_point_put`] under ONE redb
    /// write transaction so the document store, FTS inverted index, HNSW vector
    /// index, spatial R-tree, and secondary indexes are all maintained and keyed
    /// by each row's stable surrogate. Any per-row error (including a UNIQUE
    /// constraint violation) drops the transaction, leaving the whole page
    /// unchanged. On success the transaction commits once and one Insert write
    /// event is emitted per row.
    fn execute_document_batch_insert_indexed(
        &mut self,
        task: &ExecutionTask,
        params: DocumentBatchInsertParams<'_>,
    ) -> Response {
        let DocumentBatchInsertParams {
            tid,
            collection,
            documents,
            surrogates,
            returning,
            rls_filters,
            resolved_sum_targets,
            deferred_sum_targets,
        } = params;
        let database_id = task.request.database_id.as_u64();
        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets,
            wal_lsn: task.wal_lsn(),
        };
        // One guard for the whole page: each row advances the head, and a
        // failure anywhere rolls the page back to the head it started from.
        let mut chain = ChainGuard::begin(self, database_id, tid, collection);
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => return self.response_error(task, e),
        };

        // Gate post-apply write-set accumulation once for the whole batch so a
        // collection with no vector/sparse field pays nothing. Each row's
        // `apply_point_put` above reconciles storage + the btree/FTS/graph/HNSW/
        // sparse overlays, but `wal_append_document_op` mints no redo for
        // `BatchInsert` (row durability is redb-synchronous). On a WAL-only
        // restart the HNSW and sparse indexes are rebuilt only from redo `Put`
        // records, so a vector- or sparse-indexed batch insert that journals
        // nothing would lose its rows' index entries. Carrying the surrogate +
        // post-image back per row lets the Control Plane mint a durable `Put`
        // redo for each (see `plan_post_apply_redo` / `append_write_set_redo`).
        let has_vectors = self.collection_has_vectors(database_id, tid, collection)
            || self.collection_has_sparse(database_id, tid, collection);

        // Row key for post-commit event emission, captured as each row applies
        // successfully; the value bytes are re-borrowed from `documents` after
        // commit rather than cloned here. On any error we return early
        // (dropping `txn`, which rolls back every row applied so far).
        let mut applied: Vec<String> = Vec::with_capacity(documents.len());
        let mut write_set: Vec<WriteSetEntry> = Vec::new();
        // Per-row secondary-index tuples (added ∪ removed ∪ bitemporal),
        // parallel to `applied`. Recorded into the per-index write-value
        // substrate only after `txn.commit()` succeeds below — a row that
        // never commits touched no durable index state.
        let mut row_index_tuples: Vec<Vec<(String, String)>> = Vec::with_capacity(documents.len());
        // The exact bytes each row landed as, parallel to `documents`, kept only
        // when a `RETURNING` projection will read them.
        let mut stored_bodies: Vec<Vec<u8>> = Vec::new();
        // Redo entries for the target rows this page credited, accumulated
        // across rows and attached to the response below.
        let mut target_write_set: Vec<WriteSetEntry> = Vec::new();
        // Signed BALANCED contributions, accumulated across the whole page: a
        // multi-row INSERT is one boundary and one genuine set, so its rows are
        // judged together and a journal written as several rows of one
        // statement balances.
        let mut balanced_entries = Vec::new();
        // The row that failed, plus why. Collected rather than returned inline
        // so the page's in-memory side effects — the advanced chain head and the
        // document-cache entries `apply_point_put` populated — are reversed in
        // one place. Dropping `txn` reverses the durable writes; it does not
        // reverse either of those.
        let mut failure: Option<(String, crate::Error)> = None;
        for (i, (document_id, value)) in documents.iter().enumerate() {
            let surrogate = surrogates[i];
            let row_key = surrogate_to_doc_id(surrogate);
            // Every row of a batch insert is INSERT-shaped, so every row is a
            // chain link. The chain rewrites the BODY, so it runs before the
            // body is encoded and stored. The link covers the user-visible
            // document id — the same identity `VERIFY_HASH_CHAIN` recomputes
            // against — not the storage key.
            let chained = match chain.chain_insert(self, database_id, tid, document_id, value) {
                Ok(chained) => chained,
                Err(e) => {
                    // Cloned, not moved: `row_key` is still borrowed by the
                    // parameters of the call this arm is handling.
                    failure = Some((row_key.clone(), e));
                    break;
                }
            };
            let effective_value: &[u8] = chained.as_deref().unwrap_or(value);
            let outcome = match self.apply_point_put(
                &txn,
                PointPutParams {
                    database_id,
                    tid,
                    collection,
                    document_id: &row_key,
                    surrogate,
                    value: effective_value,
                    index_text: true,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    wal_lsn: task.wal_lsn(),
                },
            ) {
                Ok(o) => o,
                Err(e) => {
                    // Cloned, not moved: `row_key` is still borrowed by the
                    // parameters of the call this arm is handling.
                    failure = Some((row_key.clone(), e));
                    break;
                }
            };
            // Image-folding enforcement per row, in the SAME transaction the
            // page is being applied in, so a derived total lands or rolls back
            // with every row that moved it. The post-image is the SUBMITTED
            // body, never the chained one.
            let enforcement = match write_hook::run(
                self,
                &txn,
                &hook_ctx,
                WriteImages::Insert {
                    new: ImageBody::Submitted(value),
                },
            ) {
                Ok(enforcement) => enforcement,
                Err(e) => {
                    // Cloned, not moved: `row_key` is still borrowed by the
                    // parameters of the call this arm is handling.
                    failure = Some((row_key.clone(), e));
                    break;
                }
            };
            target_write_set.extend(write_hook::target_write_set(&enforcement.target_writes));
            balanced_entries.extend(enforcement.balanced_entries);
            if returning.is_some() {
                stored_bodies.push(outcome.stored_value);
            }
            if has_vectors {
                write_set.push(WriteSetEntry {
                    surrogate: surrogate.as_u32(),
                    is_delete: false,
                    value: value.clone(),
                    collection: None,
                });
            }
            if task.wal_lsn().is_some() {
                let mut tuples = outcome.secondary_index_added;
                tuples.extend(outcome.secondary_index_removed);
                tuples.extend(outcome.bitemporal_index_tuples);
                row_index_tuples.push(tuples);
            }
            applied.push(row_key);
        }

        if let Some((failed_row_key, error)) = failure {
            // The whole page rolls back, so put the chain head back where it
            // started and drop every cache entry the abandoned rows populated —
            // a cached body for a row that never committed is served to readers
            // as though it had.
            chain.restore(self);
            for row_key in applied.iter().chain(std::iter::once(&failed_row_key)) {
                self.doc_cache
                    .invalidate(database_id, tid, collection, row_key);
            }
            return self.response_error(task, error);
        }

        // The whole page is one boundary, so it is judged once here — before
        // the commit, so a page that leaves any journal group unbalanced writes
        // no rows at all.
        if let Err(e) = self.settle_balanced_entries(database_id, tid, collection, balanced_entries)
        {
            chain.restore(self);
            for row_key in &applied {
                self.doc_cache
                    .invalidate(database_id, tid, collection, row_key);
            }
            return self.response_error(task, e);
        }

        // The advanced head lands in the SAME transaction as the rows whose
        // hashes it covers.
        if let Err(e) = chain.persist_head(self, &txn) {
            chain.restore(self);
            for row_key in &applied {
                self.doc_cache
                    .invalidate(database_id, tid, collection, row_key);
            }
            return self.response_error(task, e);
        }

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("batch insert commit: {e}"),
                },
            );
        }

        // Record each committed row's touched secondary-index values into the
        // per-index write-value substrate. This runs only after the batch has
        // durably committed.
        if let Some(lsn) = task.wal_lsn() {
            for tuples in &row_index_tuples {
                self.note_index_write_values(
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection,
                    tuples,
                    lsn,
                );
            }
        }

        self.checkpoint_coordinator
            .mark_dirty("sparse", documents.len());
        if let Some(ref m) = self.metrics {
            m.record_document_insert();
        }

        for (i, row_key) in applied.iter().enumerate() {
            let identity = crate::engine::document::store::identity_of(row_key);
            self.emit_put_event(task, tid, collection, identity, &documents[i].1, None);
        }

        let mut response = if let Some(spec) = returning {
            // One row per inserted document, in `documents` order — the order
            // the rows were applied in, which is the order PostgreSQL returns
            // them in for a multi-row INSERT.
            let strict_schema = self.strict_schema_for(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
            );
            let identities: Vec<crate::engine::document::store::RowIdentity> = documents
                .iter()
                .map(|(document_id, _)| {
                    crate::engine::document::store::RowIdentity::from_user_key(document_id.as_str())
                })
                .collect();
            let rows: Vec<(&crate::engine::document::store::RowIdentity, &[u8])> = identities
                .iter()
                .zip(stored_bodies.iter())
                .map(|(identity, stored)| (identity, stored.as_slice()))
                .collect();
            self.stored_returning_response(task, spec, rls_filters, strict_schema.as_ref(), &rows)
        } else {
            match crate::data::executor::response_codec::encode_count("inserted", documents.len()) {
                Ok(bytes) => self.response_with_payload(task, bytes),
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        };
        if !write_set.is_empty() {
            response.write_set = write_set;
        }
        // Derived target rows live in a DIFFERENT collection than this page's,
        // so each carries its own `Some(collection)` and homes to that
        // collection's vShard.
        response.write_set.extend(target_write_set);
        response
    }
}

#[cfg(test)]
mod tests {
    use redb::TableDefinition;

    use super::DocumentBatchInsertParams;
    use crate::bridge::envelope::{Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::doc_format;
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::document::store::{CollectionConfig, surrogate_to_doc_id};
    use crate::engine::sparse::fts_redb::tables::DOC_LENGTHS;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, ResolvedSumTarget};
    use nodedb_types::Surrogate;
    use std::time::{Duration, Instant};

    const TID: u64 = 1;
    const COLL: &str = "articles";

    /// Raw JSON bodies with real words in them, so each row has text the
    /// inverted index actually has to accept for the write to be searchable.
    fn bodies() -> Vec<(String, Vec<u8>)> {
        vec![
            ("d1".to_string(), br#"{"title":"alpha bravo"}"#.to_vec()),
            ("d2".to_string(), br#"{"title":"charlie delta"}"#.to_vec()),
        ]
    }

    /// A table sharing `DOC_LENGTHS`'s redb name but with incompatible
    /// key/value types, so every inverted-index write fails structurally.
    const POISONED_DOC_LENGTHS: TableDefinition<u64, u64> =
        TableDefinition::new("text.doc_lengths");

    fn poison_inverted_index(core: &CoreLoop) {
        let db = core.sparse.db().clone();
        let txn = db.begin_write().unwrap();
        txn.delete_table(DOC_LENGTHS).unwrap();
        txn.open_table(POISONED_DOC_LENGTHS).unwrap();
        txn.commit().unwrap();
    }

    fn batch_task(documents: &[(String, Vec<u8>)], surrogates: &[Surrogate]) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
                documents: documents.to_vec(),
                surrogates: surrogates.to_vec(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
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

    fn stored(core: &CoreLoop, surrogate: Surrogate) -> Option<Vec<u8>> {
        core.sparse
            .get(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                COLL,
                &surrogate_to_doc_id(surrogate),
            )
            .unwrap()
    }

    fn corpus_size(core: &CoreLoop) -> u32 {
        core.inverted
            .corpus_stats(DatabaseId::DEFAULT.as_u64(), TenantId::new(TID), COLL)
            .unwrap()
            .0
    }

    /// Control: with a healthy index the batch lands AND both rows are counted
    /// into the FTS corpus. Without this, a passing failure test could not tell
    /// "the poison caused the rejection" from "this batch was never insertable".
    #[test]
    fn a_healthy_batch_commits_every_row_and_indexes_it() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let documents = bodies();
        let surrogates = vec![Surrogate(11), Surrogate(12)];

        let task = batch_task(&documents, &surrogates);
        let resp = core.execute_document_batch_insert(
            &task,
            DocumentBatchInsertParams {
                tid: TID,
                collection: COLL,
                documents: &documents,
                surrogates: &surrogates,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
                deferred_sum_targets: &[],
            },
        );

        assert_eq!(resp.status, Status::Ok);
        assert!(stored(&core, Surrogate(11)).is_some());
        assert!(stored(&core, Surrogate(12)).is_some());
        assert_eq!(
            corpus_size(&core),
            2,
            "both committed rows must be in the FTS corpus"
        );
    }

    /// The contract this guards: a row whose indexing fails must not be committed
    /// and counted into `inserted` — that would tell the client the write
    /// succeeded while full-text search could never return the row and nothing,
    /// not replay, not the next write, would re-index it. The batch must
    /// fail as a whole, leaving no row behind and no partial corpus.
    #[test]
    fn a_row_that_cannot_be_indexed_fails_the_whole_batch() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        poison_inverted_index(&core);
        let documents = bodies();
        let surrogates = vec![Surrogate(21), Surrogate(22)];

        let task = batch_task(&documents, &surrogates);
        let resp = core.execute_document_batch_insert(
            &task,
            DocumentBatchInsertParams {
                tid: TID,
                collection: COLL,
                documents: &documents,
                surrogates: &surrogates,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
                deferred_sum_targets: &[],
            },
        );

        assert_eq!(
            resp.status,
            Status::Error,
            "the client must be told the batch failed, not receive a success count \
             for rows full-text search will never return"
        );
        assert!(
            stored(&core, Surrogate(21)).is_none() && stored(&core, Surrogate(22)).is_none(),
            "an indexing failure on any row must roll the whole batch back — a stored \
             row whose index update failed is invisible to search forever"
        );
    }

    /// A batch carrying fewer surrogates than documents has no cross-engine
    /// identity for its rows, so every index would silently omit them. It is
    /// refused outright rather than stored-and-reported-successful.
    #[test]
    fn a_batch_without_a_surrogate_per_row_is_refused_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let documents = bodies();
        let surrogates = vec![Surrogate(31)];

        let task = batch_task(&documents, &surrogates);
        let resp = core.execute_document_batch_insert(
            &task,
            DocumentBatchInsertParams {
                tid: TID,
                collection: COLL,
                documents: &documents,
                surrogates: &surrogates,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &[],
                deferred_sum_targets: &[],
            },
        );

        assert_eq!(resp.status, Status::Error);
        assert!(
            stored(&core, Surrogate(31)).is_none(),
            "a batch with no identity for every row must write no row at all"
        );
        assert_eq!(
            corpus_size(&core),
            0,
            "and must leave nothing in the FTS corpus"
        );
    }

    // ── The batch insert an `INSERT ... SELECT` page ships must credit its
    // materialized-sum targets, exactly like every other write path ─────────

    const SUM_SOURCE: &str = "point_txns";
    const SUM_TARGET: &str = "point_holders";
    const SUM_A1: &str = "a1";
    const SUM_T1: Surrogate = Surrogate(4001);

    fn sum_binding() -> nodedb_physical::physical_plan::MaterializedSumBinding {
        nodedb_physical::physical_plan::MaterializedSumBinding {
            target_collection: SUM_TARGET.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        }
    }

    fn sum_config_key(collection: &str) -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            collection.to_string(),
        )
    }

    /// A source collection bound to the sum, and a target row starting at zero.
    fn sum_seeded_core(dir: &std::path::Path) -> CoreLoop {
        let (mut core, _req, _resp) = make_core_with_dir(dir);

        let mut source = CollectionConfig::new(SUM_SOURCE);
        source.enforcement.materialized_sum_sources = vec![sum_binding()];
        core.doc_configs.insert(sum_config_key(SUM_SOURCE), source);
        core.doc_configs.insert(
            sum_config_key(SUM_TARGET),
            CollectionConfig::new(SUM_TARGET),
        );

        let seed = serde_json::json!({"id": SUM_A1, "balance": "100"});
        core.sparse
            .put(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                SUM_TARGET,
                &surrogate_to_doc_id(SUM_T1),
                &doc_format::encode_to_msgpack(&seed),
            )
            .expect("seed target row");
        core
    }

    /// A source row body, in the MessagePack every handler receives.
    fn sum_entry(account: &str, amount: i64) -> Vec<u8> {
        doc_format::encode_to_msgpack(&serde_json::json!({
            "account_id": account,
            "amount": amount,
        }))
    }

    /// The balance the target row currently holds.
    fn sum_balance(core: &CoreLoop, surrogate: Surrogate) -> String {
        let stored = core
            .sparse
            .get(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                SUM_TARGET,
                &surrogate_to_doc_id(surrogate),
            )
            .expect("read target row")
            .expect("target row must still exist");
        doc_format::decode_document(&stored)
            .expect("target row must decode")
            .get("balance")
            .and_then(|v| v.as_str())
            .expect("target row must carry a balance")
            .to_string()
    }

    /// The orchestrator re-issues the copy through `dispatch_local`, which
    /// never passes through the statement-level resolution pass — so a page
    /// shipping an empty resolution would leave the total short of the rows it
    /// inserted.
    #[test]
    fn insert_select_page_credits_its_targets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = sum_seeded_core(dir.path());

        let documents = vec![
            (surrogate_to_doc_id(Surrogate(1)), sum_entry(SUM_A1, 25)),
            (surrogate_to_doc_id(Surrogate(2)), sum_entry(SUM_A1, 75)),
        ];
        let surrogates = vec![Surrogate(1), Surrogate(2)];
        let resolved = vec![ResolvedSumTarget::new(SUM_TARGET, SUM_A1, SUM_T1)];
        let task = batch_task(&documents, &surrogates);
        let response = core.execute_document_batch_insert(
            &task,
            DocumentBatchInsertParams {
                tid: TID,
                collection: SUM_SOURCE,
                documents: &documents,
                surrogates: &surrogates,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &resolved,
                deferred_sum_targets: &[],
            },
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(sum_balance(&core, SUM_T1), "200");
    }
}
