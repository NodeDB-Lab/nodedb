// SPDX-License-Identifier: BUSL-1.1

//! The `UPDATE ... FROM` write pass: persist each row [`super::update_from_join`]
//! already matched and resolved, one row/transaction at a time, folding each
//! into its materialized-sum target and re-indexing its vectors.

use nodedb_physical::physical_plan::ResolvedSumTarget;
use nodedb_types::columnar::StrictSchema;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::partial_refusal::{
    refusal_after_partial_apply, refusal_after_rows,
};
use crate::data::executor::handlers::point::update_reindex_vector::UpdateVectorReindex;
use crate::data::executor::handlers::returning_doc;
use crate::data::executor::handlers::transaction::stage_write::stored_row_identity;
use crate::data::executor::task::ExecutionTask;

use super::update_from_join_types::ResolvedUpdateRow;

/// What the write pass produced, handed back to the caller for response
/// encoding.
pub(in crate::data::executor) struct UpdateFromJoinWriteOutcome {
    pub affected: u64,
    /// One post-apply `Put` redo entry per updated row on a vector collection
    /// plus every derived materialized-sum target write. Empty when the
    /// target collection has no vector index and no materialized-sum target.
    pub write_set: Vec<WriteSetEntry>,
    /// Post-image JSON per affected row, populated only when the caller asked
    /// for `RETURNING`.
    pub returned_docs: Vec<nodedb_types::Value>,
}

/// Everything the write pass needs about the statement, gathered once by the
/// caller so this pass never re-derives it.
pub(in crate::data::executor) struct WriteResolvedRowsCtx<'a> {
    pub tid: u64,
    pub target_collection: &'a str,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    pub has_vectors: bool,
    /// The target collection's strict schema, when it stores Binary Tuples.
    pub strict_schema: Option<&'a StrictSchema>,
    /// The target collection's declared `PRIMARY KEY` column, when it has one.
    pub declared_primary_key: Option<&'a str>,
    pub want_returning: bool,
}

impl CoreLoop {
    /// Persist every row in `rows`, one transaction each. Returns `Err(resp)`
    /// with a ready-to-return error `Response` the moment any row's write,
    /// enforcement fold, or vector re-index fails — the caller returns it
    /// immediately rather than continuing the loop.
    pub(in crate::data::executor) fn write_resolved_update_from_join_rows(
        &mut self,
        task: &ExecutionTask,
        ctx: WriteResolvedRowsCtx<'_>,
        rows: Vec<ResolvedUpdateRow>,
    ) -> Result<UpdateFromJoinWriteOutcome, Response> {
        let WriteResolvedRowsCtx {
            tid,
            target_collection,
            resolved_sum_targets,
            has_vectors,
            strict_schema,
            declared_primary_key,
            want_returning,
        } = ctx;
        let is_strict = strict_schema.is_some();
        let database_id = task.request.database_id.as_u64();
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            target_collection.to_string(),
        );
        let mut affected = 0u64;
        let mut write_set: Vec<WriteSetEntry> = Vec::new();
        let mut returned_docs: Vec<nodedb_types::Value> = if want_returning {
            Vec::with_capacity(rows.len())
        } else {
            Vec::new()
        };

        // A closed period refuses an edit to a row it holds, and an edit that
        // assigns the period column into it. Both images of every row are
        // judged before the first row commits: each row below commits on its
        // own, so a lock judged there refuses after earlier rows landed.
        if let Some(lock) = self
            .doc_configs
            .get(&config_key)
            .and_then(|config| config.enforcement.period_lock.as_ref())
        {
            for row in &rows {
                for image in [&row.old_body, &row.body] {
                    if let Err(e) =
                        crate::data::executor::enforcement::period_lock::check_period_lock(
                            &self.sparse,
                            database_id,
                            tid,
                            target_collection,
                            image,
                            lock,
                            resolved_sum_targets,
                        )
                    {
                        return Err(self.response_error(task, e));
                    }
                }
            }
        }

        for row in rows {
            let ResolvedUpdateRow {
                key: storage_key,
                body: updated_bytes,
                old_body,
                doc,
            } = row;

            // The row's body and the materialized-sum delta it owes share ONE
            // transaction. `ResolvedUpdateRow` already carries BOTH images —
            // `old_body` as stored and `body` as the post-image — so the fold
            // re-reads nothing; the struct was built to carry them.
            let row_txn = match self.sparse.begin_write() {
                Ok(txn) => txn,
                Err(e) => return Err(self.response_error(task, refusal_after_rows(affected, e))),
            };
            let stored = self.sparse.put_in_txn(
                &row_txn,
                database_id,
                tid,
                target_collection,
                &storage_key,
                &updated_bytes,
            );
            if stored.is_ok() {
                let enforcement = write_hook::run(
                    self,
                    &row_txn,
                    &write_hook::HookCtx {
                        database_id,
                        tid,
                        collection: target_collection,
                        resolved_targets: resolved_sum_targets,
                        deferred_sum_targets: &[],
                        wal_lsn: task.wal_lsn(),
                    },
                    write_hook::WriteImages::Update {
                        old: write_hook::ImageBody::Stored(&old_body),
                        new: write_hook::ImageBody::Stored(&updated_bytes),
                    },
                );
                let target_writes = match enforcement {
                    // Only the target writes are taken: this row's BALANCED
                    // contribution was settled for the whole statement above,
                    // before the first row was rewritten, so taking it again
                    // would count the same update twice.
                    Ok(outcome) => outcome.target_writes,
                    // Dropping `row_txn` un-committed reverses the row and every
                    // target it had already moved.
                    Err(e) => {
                        return Err(self.response_error(task, refusal_after_rows(affected, e)));
                    }
                };
                if let Err(e) = row_txn.commit() {
                    return Err(self.response_error(
                        task,
                        refusal_after_rows(
                            affected,
                            ErrorCode::Internal {
                                detail: format!("update-from-join commit: {e}"),
                            },
                        ),
                    ));
                }
                // One durable redo entry per moved target row, naming the TARGET
                // collection — this statement's redo describes only the rows of
                // `target_collection` it rewrote.
                write_set.extend(write_hook::target_write_set(&target_writes));
                self.doc_cache.put(
                    database_id,
                    tid,
                    target_collection,
                    &storage_key,
                    &updated_bytes,
                );
                // Emit an update event per affected row to the Event Plane, so
                // AFTER-UPDATE triggers and CDC/change-stream consumers see
                // each row `UPDATE ... FROM` touched — mirroring
                // `execute_point_update`/`execute_bulk_update`'s single-row
                // emit. `old_body` is the pre-update stored bytes captured by
                // `collect_update_from_join_rows`; `emit_put_event` derives
                // `WriteOp::Update` from the Some prior + Some new pair and
                // handles strict->msgpack conversion on both sides.
                //
                // The identity is the one INSERT minted: the declared primary
                // key when the collection declares one, else the decimal
                // surrogate. The redo entry below journals the same identity.
                let row_identity = stored_row_identity(
                    &updated_bytes,
                    strict_schema,
                    declared_primary_key,
                    storage_key,
                );
                // `row_identity` is read again below for `RETURNING`'s `id`
                // field, so the event-emit boundary gets a clone rather than
                // the move.
                self.emit_put_event(
                    task,
                    tid,
                    target_collection,
                    row_identity.clone(),
                    &updated_bytes,
                    Some(&old_body),
                );
                // Re-index the row's vectors from the new body (soft-delete the
                // old HNSW node + insert the new one, keyed by the stable
                // surrogate), then carry the surrogate + post-image back for a
                // post-apply `Put` redo (`updated_bytes` is moved as its last
                // use). Both are no-ops unless the collection has a vector
                // field, so a non-vector collection pays nothing.
                if has_vectors {
                    if let Err(e) = self.update_reindex_vector_indexes(UpdateVectorReindex {
                        database_id,
                        tid,
                        collection: target_collection,
                        storage_key,
                        new_body: &updated_bytes,
                        is_strict,
                        has_vectors,
                    }) {
                        // The row's body already committed.
                        return Err(
                            self.response_error(task, refusal_after_partial_apply(e.into()))
                        );
                    }
                    write_set.push(WriteSetEntry {
                        surrogate: storage_key.surrogate().as_u32(),
                        identity: row_identity.clone(),
                        is_delete: false,
                        value: updated_bytes,
                        collection: None,
                    });
                }
                affected += 1;
                if want_returning {
                    // `row_identity` only stands in as `id` for a row that
                    // declares no primary key of its own — overwriting a
                    // declared key would return a value the client never wrote.
                    let mut row = nodedb_types::Value::from(doc);
                    returning_doc::attach_row_id(&mut row, &row_identity);
                    returned_docs.push(row);
                }
            }
        }

        Ok(UpdateFromJoinWriteOutcome {
            affected,
            write_set,
            returned_docs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;
    use crate::engine::document::store::CollectionConfig;
    use crate::types::{DatabaseId, TenantId};
    use nodedb_physical::physical_plan::PeriodLockConfig;
    use nodedb_types::{StorageKey, Surrogate};

    const TID: u64 = 1;
    const COLLECTION: &str = "journal";

    fn resolved_row(surrogate: u32, old: serde_json::Value) -> ResolvedUpdateRow {
        let mut new = old.clone();
        if let Some(fields) = new.as_object_mut() {
            fields.insert("note".into(), serde_json::json!("new"));
        }
        ResolvedUpdateRow {
            key: StorageKey::for_surrogate(Surrogate(surrogate)),
            body: doc_format::encode_to_msgpack(&new),
            old_body: doc_format::encode_to_msgpack(&old),
            doc: new,
        }
    }

    /// A closed period holds one resolved row. The refusal code claims
    /// nothing applied, so no row can be rewritten, including the rows ahead
    /// of it.
    #[test]
    fn a_period_lock_on_any_resolved_row_rewrites_no_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let database_id = task.request.database_id.as_u64();
        let mut config = CollectionConfig::new(COLLECTION);
        config.enforcement.period_lock = Some(PeriodLockConfig {
            period_column: "fiscal_period".into(),
            ref_table: "fiscal_periods".into(),
            ref_pk: "period_key".into(),
            status_column: "status".into(),
            allowed_statuses: vec!["OPEN".into()],
        });
        core.doc_configs.insert(
            (
                DatabaseId::new(database_id),
                TenantId::new(TID),
                COLLECTION.to_string(),
            ),
            config,
        );
        let old_rows = [
            serde_json::json!({"note": "old"}),
            // No reference row resolves this period, so the lock refuses it.
            serde_json::json!({"note": "old", "fiscal_period": "2026-01"}),
            serde_json::json!({"note": "old"}),
        ];
        for (surrogate, row) in (1u32..).zip(old_rows.iter()) {
            core.sparse
                .put(
                    database_id,
                    TID,
                    COLLECTION,
                    &StorageKey::for_surrogate(Surrogate(surrogate)),
                    &doc_format::encode_to_msgpack(row),
                )
                .expect("seed row");
        }
        let rows: Vec<ResolvedUpdateRow> = (1u32..)
            .zip(old_rows.iter())
            .map(|(surrogate, row)| resolved_row(surrogate, row.clone()))
            .collect();

        let outcome = core.write_resolved_update_from_join_rows(
            &task,
            WriteResolvedRowsCtx {
                tid: TID,
                target_collection: COLLECTION,
                resolved_sum_targets: &[],
                has_vectors: false,
                strict_schema: None,
                declared_primary_key: None,
                want_returning: false,
            },
            rows,
        );

        let Err(response) = outcome else {
            panic!("a locked period must refuse the statement");
        };
        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::PeriodLocked { .. })
            ),
            "got {:?}",
            response.error_code
        );
        for surrogate in 1u32..=3 {
            let stored = core
                .sparse
                .get(
                    database_id,
                    TID,
                    COLLECTION,
                    &StorageKey::for_surrogate(Surrogate(surrogate)),
                )
                .expect("read row")
                .expect("row exists");
            let doc = doc_format::decode_document(&stored).expect("row decodes");
            assert_eq!(doc.get("note"), Some(&serde_json::json!("old")));
        }
    }
}
