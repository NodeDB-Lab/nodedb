// SPDX-License-Identifier: BUSL-1.1

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::point::update_reindex::NonbitemporalUpdateReindex;
use crate::data::executor::handlers::point::update_reindex_vector::UpdateVectorReindex;
use crate::data::executor::handlers::returning_doc;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{OllpPredictedEdge, ResolvedSumTarget, ReturningSpec};

use super::update_project::{ProjectUpdateRows, ProjectedUpdateRow};

/// Parameters for a bulk update operation.
pub(in crate::data::executor) struct BulkUpdateParams<'a> {
    pub collection: &'a str,
    pub filter_bytes: &'a [u8],
    pub updates: &'a [(String, nodedb_physical::physical_plan::UpdateValue)],
    pub returning: Option<&'a ReturningSpec>,
    pub ollp_predicted_surrogates: Option<&'a [u32]>,
    /// Predicted OLD (pre-update) implicit-edge set of the matched docs. When
    /// `Some`, the handler recomputes the ACTUAL old edges of the matched docs
    /// and returns [`ErrorCode::OllpRetryRequired`] on any divergence BEFORE
    /// applying writes — closing the recon→execute TOCTOU on `_from`/`_to`/
    /// `_type` so the Control-Plane-derived edge reconciliation stays valid.
    pub ollp_predicted_edges: Option<&'a [OllpPredictedEdge]>,
    /// Compiled RLS read policy gating the `RETURNING` rows. Empty = no policy.
    pub rls_filters: &'a [u8],
    /// Compiled RLS write policy gating the PERSIST, decided per row against
    /// its post-update image. A separate slot from `rls_filters`: that one
    /// bounds what may be shown back, this one bounds what may be written.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// the rows this predicate matches may touch, resolved on the Control Plane
    /// from its recon scan of the same predicate. Both sides of a join-key
    /// change are present, so a row moved between targets is debited and
    /// credited in the same pass.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    /// Declared `PRIMARY KEY` column of a schemaless collection, `None`
    /// otherwise — see `ProjectUpdateRows::declared_primary_key`.
    pub declared_primary_key: Option<&'a str>,
}

impl CoreLoop {
    /// Bulk update: scan documents matching filters, apply field updates.
    ///
    /// When `returning` is `None`, returns affected row count as JSON:
    /// `{"affected": N}`.
    ///
    /// When `returning` is `Some(spec)`, returns a `RowsPayload` with the
    /// post-update documents projected per spec. If 0 rows match, returns
    /// an empty `RowsPayload`.
    pub(in crate::data::executor) fn execute_bulk_update(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: BulkUpdateParams<'_>,
    ) -> Response {
        let BulkUpdateParams {
            collection,
            filter_bytes,
            updates,
            returning,
            ollp_predicted_surrogates,
            ollp_predicted_edges,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
            declared_primary_key,
        } = params;
        debug!(core = self.core_id, %collection, has_returning = returning.is_some(), "bulk update");

        // Reject direct updates to generated columns.
        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) = super::super::generated::check_generated_readonly(
                updates,
                &config.enforcement.generated_columns,
            )
        {
            return self.response_error(task, e);
        }

        // Empty `filter_bytes` means "no WHERE clause" — match every row.
        let filters: Vec<ScanFilter> = if filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize filters: {e}"),
                        },
                    );
                }
            }
        };

        let matching_ids = match self.scan_matching_documents(
            task.request.database_id.as_u64(),
            tid,
            collection,
            &filters,
        ) {
            Ok(ids) => ids,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Settle the apply set and run every leader-only pre-write
        // verification the plan's predictions call for. Any divergence returns
        // here, before the first row is touched.
        let apply_ids = match self.admit_bulk_predicate_write(
            task.request.database_id.as_u64(),
            tid,
            matching_ids,
            &super::BulkAdmission {
                collection,
                predicted_surrogates: ollp_predicted_surrogates,
                predicted_edges: ollp_predicted_edges,
                updates,
                resolved_sum_targets,
            },
        ) {
            Ok(ids) => ids,
            Err(code) => return self.response_error(task, code),
        };

        // Check if this is a strict (Binary Tuple) collection.
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
        // collection with no vector field pays nothing. When a vector field is
        // present, an UPDATE that rewrites an embedding must re-index the row's
        // HNSW vectors — the btree/FTS/graph reconciliation this handler already
        // does never touches the vector index, so KNN search would keep scoring
        // the stale pre-update embedding in the same process.
        let database_id = task.request.database_id.as_u64();
        let has_vectors = self.collection_has_vectors(database_id, tid, collection);

        // The plain `INDEXES` secondary-index paths for this collection, cloned
        // once for the whole statement. Each row's primary write reconciles
        // these atomically via `nonbitemporal_update_reindex` so a value the
        // UPDATE changed can't leave a stale index entry pointing at the old
        // value (which would make a later lookup on the new value miss the row).
        let index_paths = self
            .doc_configs
            .get(&config_key)
            .map(|c| c.index_paths.clone())
            .unwrap_or_default();

        // Apply updates to each matching document.
        let mut affected = 0u64;
        // One post-apply `Put` redo entry per updated row on a vector collection.
        // Each row's `sparse.put` above reconciled storage + the btree/FTS/graph
        // overlays but minted no WAL redo carrying the new body, so a WAL-only
        // restart would rebuild the HNSW from the pre-update `Put` records and
        // resurrect the stale embeddings. Carrying the surrogate + post-image back
        // lets the Control Plane mint a durable `Put` redo per row. Only populated
        // when the collection has a vector index.
        let mut write_set: Vec<WriteSetEntry> = Vec::new();
        let mut returned_docs: Vec<serde_json::Value> = if returning.is_some() {
            Vec::with_capacity(apply_ids.len())
        } else {
            Vec::new()
        };

        // Project every matched row to its post-image BEFORE anything is
        // written. The apply loop below commits one transaction per row, so a
        // statement-wide constraint judged while it iterated could only report a
        // violation the rows ahead of it had already made durable.
        let projected = match self.project_bulk_update_rows(ProjectUpdateRows {
            database_id,
            tid,
            collection,
            doc_ids: &apply_ids,
            updates,
            strict_schema: strict_schema.as_ref(),
            declared_primary_key,
        }) {
            Ok(projected) => projected,
            Err(e) => return self.response_error(task, e),
        };

        // BALANCED over the statement's whole matched set. An update takes the
        // old amount off its group and puts the new one on, so an update that
        // moves one leg's amount is refused here — before any row is rewritten
        // — while one that moves both legs by the same amount nets to zero and
        // proceeds.
        let balanced_entries = {
            let images: Vec<(&serde_json::Value, &serde_json::Value)> = projected
                .iter()
                .map(|row| (&row.old_doc, &row.doc))
                .collect();
            self.balanced_entries_for_json_updates(database_id, tid, collection, &images)
        };
        if let Err(e) = self.settle_balanced_entries(database_id, tid, collection, balanced_entries)
        {
            return self.response_error(task, e);
        }
        for row in projected {
            let ProjectedUpdateRow {
                doc_id,
                current_bytes,
                old_doc: old_doc_json,
                mut doc,
                updated_bytes,
            } = row;
            let doc_id = doc_id.as_str();
            // Gate the persist on the collection's write policy, decided
            // against this row's post-update image — `doc` already has
            // the assignments and any regenerated columns applied, so it
            // is the row that would exist afterwards. A rejected row
            // fails the statement rather than being skipped: a skipped
            // row would be reported as unaffected while the rest of the
            // predicate's matches were rewritten.
            if let Err(e) = rls_write_gate::admit_row(rls_write_check, &doc, tid, collection) {
                return self.response_error(task, e);
            }
            // Both images are already materialized here — `old_doc_json`
            // for the secondary-index diff and `doc` as the post-image —
            // so the row's materialized-sum delta costs no extra read
            // and no extra decode. It is folded inside the SAME
            // transaction the row's body and index diff commit in, so a
            // credited target can never outlive the row that caused it.
            let persisted = self.persist_bulk_update_row(
                NonbitemporalUpdateReindex {
                    database_id,
                    tid,
                    collection,
                    doc_id,
                    new_body: &updated_bytes,
                    index_paths: &index_paths,
                    old_doc: &old_doc_json,
                    new_doc: &doc,
                },
                &write_hook::HookCtx {
                    database_id,
                    tid,
                    collection,
                    resolved_targets: resolved_sum_targets,
                    deferred_sum_targets: &[],
                    wal_lsn: task.wal_lsn(),
                },
            );
            let (touched, target_writes) = match persisted {
                // The row's own reindex failed and it is skipped, as it
                // always was — the helper logged which row and why.
                Ok(None) => continue,
                Ok(Some(persisted)) => (persisted.touched, persisted.target_writes),
                // A rejected materialized sum is NOT a skippable row:
                // skipping it would report a smaller affected count as
                // the truth while the rest of the predicate's matches
                // were rewritten, and leave the stored total short of the
                // `SUM(...)` over the rows that did land.
                Err(e) => return self.response_error(task, e),
            };
            // One durable redo entry per derived target row, naming the
            // TARGET collection: the statement's own redo describes the
            // source row only, so without these a WAL-only restart
            // leaves every total as it stood before the statement.
            write_set.extend(write_hook::target_write_set(&target_writes));
            // Published only after the commit succeeded — the same
            // ordering the reindex helper used when it owned the
            // transaction.
            if let Some(lsn) = task.wal_lsn() {
                self.note_index_write_values(
                    task.request.database_id,
                    crate::types::TenantId::new(tid),
                    collection,
                    &touched,
                    lsn,
                );
            }
            self.doc_cache.put(
                task.request.database_id.as_u64(),
                tid,
                collection,
                doc_id,
                &updated_bytes,
            );
            // Record the committed row's write version against its
            // surrogate + collection. Parsed once and reused below
            // for the write-set entry (the row's doc_id is the
            // hex-encoded surrogate storage key either way).
            let row_surrogate = crate::engine::document::store::doc_id_to_surrogate(doc_id);
            if let Some(surrogate) = row_surrogate {
                self.note_surrogate_write_lsn(task, tid, collection, surrogate.as_u32());
                // Re-index the row's vectors from the new body
                // (soft-delete the old HNSW node + insert the new
                // one, keyed by the stable surrogate). No-op unless
                // the collection has a vector field (gated above).
                if has_vectors
                    && let Err(e) = self.update_reindex_vector_indexes(UpdateVectorReindex {
                        database_id,
                        tid,
                        collection,
                        row_key: doc_id,
                        surrogate,
                        new_body: &updated_bytes,
                        is_strict: strict_schema.is_some(),
                        has_vectors,
                    })
                {
                    return self.response_error(task, e);
                }
            }
            // Emit an update event per affected row to the Event Plane,
            // so AFTER-UPDATE triggers and CDC/change-stream consumers
            // see each row a bulk UPDATE touched — mirroring
            // `execute_point_update`'s single-row emit. `current_bytes`
            // is the pre-update row read above; `emit_put_event` derives
            // `WriteOp::Update` from the Some prior + Some new pair and
            // handles strict->msgpack conversion on both sides. Emitted
            // per row (not a `WriteOp::BulkUpdate` summary) because the
            // Event Plane's WAL-replay bulk variants are aggregate
            // metadata reconstructed only when the live per-row events
            // were lost — the live path always emits per row.
            // `doc_id` is the surrogate hex storage key. A value that fails
            // to parse as a minted key is a legacy or user key, taken
            // verbatim.
            let row_identity = crate::engine::document::store::identity_of(doc_id);
            // `row_identity` is read again below for `RETURNING`'s `id` field,
            // so the event-emit boundary gets a clone rather than the move.
            self.emit_put_event(
                task,
                tid,
                collection,
                row_identity.clone(),
                &updated_bytes,
                Some(&current_bytes),
            );
            affected += 1;
            if returning.is_some() {
                // `row_identity` only stands in as `id` for a row that
                // declares no primary key of its own — overwriting a
                // declared key would return a value the client never wrote.
                returning_doc::attach_row_id(&mut doc, &row_identity);
                returned_docs.push(doc);
            }
            // Carry the surrogate + post-image back for a post-apply
            // `Put` redo. `updated_bytes` is moved as its last use;
            // gated on `has_vectors` so a non-vector collection pays
            // nothing. Keyed by the row's surrogate parsed from its
            // doc_id (the hex-encoded surrogate storage key).
            if has_vectors && let Some(surrogate) = row_surrogate {
                write_set.push(WriteSetEntry {
                    surrogate: surrogate.as_u32(),
                    is_delete: false,
                    value: updated_bytes,
                    collection: None,
                });
            }
        }

        debug!(core = self.core_id, %collection, affected, "bulk update complete");

        let mut response = if let Some(spec) = returning {
            match returning_rows::build_rows_payload(spec, rls_filters, &returned_docs) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("RETURNING encode: {e}"),
                        },
                    );
                }
            }
        } else {
            let result = serde_json::json!({ "affected": affected });
            match response_codec::encode_json_as_msgpack(&result) {
                Ok(payload) => self.response_with_payload(task, payload),
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
        response
    }
}
