// SPDX-License-Identifier: BUSL-1.1

//! The `UPDATE ... FROM` write pass: persist each row [`super::update_from_join`]
//! already matched and resolved, one row/transaction at a time, with its
//! secondary-index diff and full-text postings, folding each into its
//! materialized-sum target and re-indexing its vectors.

use nodedb_physical::physical_plan::ResolvedSumTarget;
use nodedb_types::columnar::StrictSchema;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::StoredRow;
use crate::data::executor::enforcement::unique::PostImage;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::partial_refusal::{
    refusal_after_partial_apply, refusal_after_rows,
};
use crate::data::executor::handlers::point::update_reindex::UpdateBody;
use crate::data::executor::handlers::point::update_reindex_text::UpdateTextReindex;
use crate::data::executor::handlers::point::update_reindex_vector::UpdateVectorReindex;
use crate::data::executor::handlers::returning_doc;
use crate::data::executor::handlers::transaction::stage_write::stored_row_identity;
use crate::data::executor::task::ExecutionTask;

use super::update_from_join_types::ResolvedUpdateRow;

/// What the write pass produced, handed back to the caller for response
/// encoding.
pub(in crate::data::executor) struct UpdateFromJoinWriteOutcome {
    pub affected: u64,
    /// One post-apply `Put` redo entry per updated row plus every derived
    /// materialized-sum target write.
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
    /// The system time every new version lands at on a bitemporal target,
    /// `None` on any other. The collect pass encoded the post-images at it.
    pub bitemporal_sys_from_ms: Option<i64>,
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
            bitemporal_sys_from_ms,
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

        // UNIQUE is judged on the statement's post-state before the first row
        // commits: a value one row releases is free for another.
        let post_images: Vec<PostImage<'_>> = rows
            .iter()
            .map(|row| PostImage {
                surrogate: row.key.surrogate().as_u32(),
                doc: Some(&row.doc),
                judged: true,
            })
            .collect();
        if let Err(e) = self.check_unit_unique(database_id, tid, target_collection, &post_images) {
            return Err(self.response_error(task, e));
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
                Err(e) => {
                    let code = refusal_after_rows(affected, e);
                    return Err(self.refusal_with_landed_rows(task, code, write_set));
                }
            };
            // The body, its secondary-index diff, and the row's full-text
            // postings land together, as a point update lands them; a row
            // whose write fails refuses the statement rather than drop out of
            // its affected count.
            let stored = self
                .update_body_in_txn(
                    &row_txn,
                    UpdateBody {
                        config_key: &config_key,
                        database_id,
                        tid,
                        collection: target_collection,
                        storage_key: &storage_key,
                        current_bytes: &old_body,
                        updated_bytes: &updated_bytes,
                        bitemporal_sys_from_ms,
                    },
                )
                .and_then(|touched| {
                    self.update_reindex_text(
                        &row_txn,
                        UpdateTextReindex {
                            database_id,
                            tid,
                            collection: target_collection,
                            surrogate: storage_key.surrogate(),
                            new_doc: &doc,
                        },
                    )
                    .map(|()| touched)
                });
            let touched = match stored {
                Ok(touched) => touched,
                Err(e) => {
                    let code = refusal_after_rows(affected, e);
                    return Err(self.refusal_with_landed_rows(task, code, write_set));
                }
            };
            {
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
                        let code = refusal_after_rows(affected, e);
                        return Err(self.refusal_with_landed_rows(task, code, write_set));
                    }
                };
                if let Err(e) = row_txn.commit() {
                    let code = refusal_after_rows(
                        affected,
                        ErrorCode::Internal {
                            detail: format!("update-from-join commit: {e}"),
                        },
                    );
                    return Err(self.refusal_with_landed_rows(task, code, write_set));
                }
                // Index write-versions are published only once the write they
                // describe is durable.
                if let Some(lsn) = task.wal_lsn()
                    && !touched.is_empty()
                {
                    self.note_index_write_values(
                        crate::types::DatabaseId::new(database_id),
                        crate::types::TenantId::new(tid),
                        target_collection,
                        &touched,
                        lsn,
                    );
                }
                // The identity is the one INSERT minted: the declared primary
                // key when the collection declares one, else the decimal
                // surrogate. The event and the redo entry journal the same
                // identity.
                let row_identity = stored_row_identity(
                    &updated_bytes,
                    strict_schema,
                    declared_primary_key,
                    storage_key,
                );
                // The row's post-image, journalled after apply: this plan
                // carries no pre-dispatch record of it. Then one entry per
                // moved target row, naming the TARGET collection.
                let image = self.stored_row_image(
                    StoredRow {
                        database_id,
                        tid,
                        collection: target_collection,
                        surrogate: storage_key.surrogate().as_u32(),
                        identity: row_identity.clone(),
                    },
                    &updated_bytes,
                    // A versioned row landed at the statement's system time.
                    bitemporal_sys_from_ms,
                );
                match image {
                    Ok(image) => write_set.push(image),
                    // This row committed above, so it counts as landed.
                    Err(e) => {
                        let code = refusal_after_rows(affected + 1, e);
                        return Err(self.refusal_with_landed_rows(task, code, write_set));
                    }
                }
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
                // surrogate). A no-op unless the collection has a vector field.
                if has_vectors
                    && let Err(e) = self.update_reindex_vector_indexes(UpdateVectorReindex {
                        database_id,
                        tid,
                        collection: target_collection,
                        storage_key,
                        new_body: &updated_bytes,
                        is_strict,
                        has_vectors,
                    })
                {
                    // The row's body already committed.
                    let code = refusal_after_partial_apply(e.into());
                    return Err(self.refusal_with_landed_rows(task, code, write_set));
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
    use crate::engine::document::store::{CollectionConfig, DocumentEngine, IndexPath};
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

    /// The surrogates the `note` index names for `value`.
    fn note_owners(core: &CoreLoop, database_id: u64, value: &str) -> Vec<u32> {
        DocumentEngine::new(&core.sparse, database_id, TID)
            .index_lookup(COLLECTION, "note", value, false)
            .expect("index lookup")
            .iter()
            .map(|key| key.surrogate().as_u32())
            .collect()
    }

    /// A rewritten indexed field moves the row's secondary-index entry to the
    /// new value, as a point update does.
    #[test]
    fn a_rewritten_indexed_field_moves_its_secondary_index_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let database_id = task.request.database_id.as_u64();
        let mut config = CollectionConfig::new(COLLECTION);
        config.index_paths.push(IndexPath::new("note"));
        core.doc_configs.insert(
            (
                DatabaseId::new(database_id),
                TenantId::new(TID),
                COLLECTION.to_string(),
            ),
            config.clone(),
        );
        let old = serde_json::json!({"note": "old"});
        {
            let mut engine = DocumentEngine::new(&core.sparse, database_id, TID);
            engine.register_collection(config);
            engine
                .put(COLLECTION, &StorageKey::for_surrogate(Surrogate(1)), &old)
                .expect("seed row");
        }
        assert_eq!(note_owners(&core, database_id, "old"), [1]);

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
                bitemporal_sys_from_ms: None,
            },
            vec![resolved_row(1, old)],
        );

        let Ok(outcome) = outcome else {
            panic!("the update must apply");
        };
        assert_eq!(outcome.affected, 1);
        assert!(
            note_owners(&core, database_id, "old").is_empty(),
            "the old value no longer names the row"
        );
        assert_eq!(note_owners(&core, database_id, "new"), [1]);
    }

    /// UPDATE ... FROM on a bitemporal target reads each row's current
    /// version and lands a new version at the statement's system time, with
    /// the versioned index diff and the version in its write set, as a point
    /// update does. The earlier version stays readable as of a time before
    /// the statement, and the plain row store stays empty.
    #[test]
    fn a_bitemporal_target_gets_a_new_version_per_matched_row() {
        use crate::bridge::envelope::RowEffect;
        use crate::engine::sparse::btree_versioned::{VersionedIndexEntry, VersionedPut};
        use nodedb_physical::physical_plan::UpdateValue;

        const SEED_MS: i64 = 1_000;
        const STATEMENT_MS: i64 = 2_000;
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let database_id = task.request.database_id.as_u64();
        let config_key = (
            DatabaseId::new(database_id),
            TenantId::new(TID),
            COLLECTION.to_string(),
        );
        core.doc_configs.insert(
            config_key.clone(),
            CollectionConfig::new(COLLECTION)
                .with_index("note")
                .with_bitemporal(true),
        );
        let row = StorageKey::for_surrogate(Surrogate(1));
        let old = serde_json::json!({"k": "a", "note": "old"});
        core.sparse
            .versioned_put(VersionedPut {
                database_id,
                tenant: TID,
                coll: COLLECTION,
                doc_id: &row,
                sys_from_ms: SEED_MS,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: &doc_format::encode_to_msgpack(&old),
            })
            .expect("seed version");
        core.sparse
            .versioned_index_put(VersionedIndexEntry {
                database_id,
                tenant: TID,
                coll: COLLECTION,
                field: "note",
                value: "old",
                doc_id: &row,
                sys_from_ms: SEED_MS,
            })
            .expect("seed index");

        let source_map: std::collections::HashMap<String, serde_json::Value> =
            [("a".to_string(), serde_json::json!({"k": "a"}))].into();
        let updates = vec![(
            "note".to_string(),
            UpdateValue::Literal(
                nodedb_types::value_to_msgpack(&nodedb_types::Value::String("new".into()))
                    .expect("encode literal"),
            ),
        )];
        let rows = core
            .collect_update_from_join_rows(
                super::super::update_from_join_collect::CollectUpdateRows {
                    task: &task,
                    tid: TID,
                    target_collection: COLLECTION,
                    source_alias: "s",
                    target_join_col: "k",
                    updates: &updates,
                    source_map: &source_map,
                    target_filters: &[],
                    strict_schema: None,
                    config_key: &config_key,
                    declared_primary_key: None,
                    bitemporal_sys_from_ms: Some(STATEMENT_MS),
                },
            )
            .expect("collect");
        assert_eq!(rows.len(), 1, "the current version matches the join");

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
                bitemporal_sys_from_ms: Some(STATEMENT_MS),
            },
            rows,
        );
        let Ok(outcome) = outcome else {
            panic!("the update must apply");
        };
        assert_eq!(outcome.affected, 1);

        let note =
            |body: Vec<u8>| doc_format::decode_document(&body).expect("decode")["note"].clone();
        let current = core
            .sparse
            .versioned_get_current(database_id, TID, COLLECTION, &row)
            .expect("read current")
            .expect("a current version");
        assert_eq!(note(current), serde_json::json!("new"));
        let before = core
            .sparse
            .versioned_get_as_of(database_id, TID, COLLECTION, &row, Some(SEED_MS + 1), None)
            .expect("read as of")
            .expect("the earlier version");
        assert_eq!(note(before), serde_json::json!("old"));
        assert!(
            core.sparse
                .get(database_id, TID, COLLECTION, &row)
                .expect("read plain store")
                .is_none(),
            "a bitemporal row never lands in the plain store"
        );

        let owners = |value: &str| -> Vec<u32> {
            DocumentEngine::new(&core.sparse, database_id, TID)
                .index_lookup(COLLECTION, "note", value, true)
                .expect("index lookup")
                .iter()
                .map(|key| key.surrogate().as_u32())
                .collect()
        };
        assert!(
            owners("old").is_empty(),
            "the old value no longer names the row"
        );
        assert_eq!(owners("new"), [1]);

        assert!(
            matches!(
                &outcome.write_set[0].effect,
                RowEffect::Put { version: Some(version), .. } if version.sys_from_ms == STATEMENT_MS
            ),
            "the write set carries the new version: {:?}",
            outcome.write_set
        );
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
                bitemporal_sys_from_ms: None,
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
