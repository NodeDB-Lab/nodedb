// SPDX-License-Identifier: BUSL-1.1

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook;
use crate::data::executor::handlers::transaction::stage_write::stored_row_identity;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{OllpPredictedEdge, ResolvedSumTarget, ReturningSpec};

use super::update_project::{MatchedRows, ProjectUpdateRows};
use crate::data::executor::handlers::point::apply_put::InstalledDocumentRow;
use crate::data::executor::handlers::point::update::prepared::{
    PreparedUpdateRow, PreparedUpdateUndo,
};

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
            && let Err(error) = crate::data::executor::enforcement::append_only::check_point_update(
                collection,
                &config.enforcement,
            )
        {
            return self.response_error(task, error);
        }
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) = super::super::generated::check_generated_readonly(
                updates,
                &config.enforcement.generated_columns,
            )
        {
            return self.response_error(task, e);
        }

        // Empty `filter_bytes` means "no WHERE clause" — match every row.
        // The matched set is each row's current version.
        let database_id = task.request.database_id.as_u64();
        let matching_ids = match super::decode_bulk_filters(filter_bytes)
            .and_then(|filters| self.current_matching_rows(database_id, tid, collection, &filters))
        {
            Ok(rows) => rows.into_iter().map(|(key, _)| key).collect(),
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };

        // Settle the apply set and run every leader-only pre-write
        // verification the plan's predictions call for. Any divergence returns
        // here, before the first row is touched.
        let apply_ids = match self.admit_bulk_predicate_write(
            database_id,
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

        let sys_from = match self.rows_version_ordinal(database_id, tid, collection, &apply_ids) {
            Ok(ordinal) => ordinal,
            Err(error) => return self.response_error(task, error),
        };
        let mut write_set: Vec<WriteSetEntry> = Vec::new();

        // Project and judge the complete statement before its transaction begins.
        let projected = match self.project_bulk_update_rows(ProjectUpdateRows {
            database_id,
            tid,
            collection,
            rows: MatchedRows::Stored(&apply_ids),
            updates,
            strict_schema: strict_schema.as_ref(),
            declared_primary_key,
            bitemporal_sys_from: Some(sys_from),
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
        if let Err(code) = self.gate_bulk_update_rows(
            task,
            tid,
            collection,
            &projected,
            rls_write_check,
            resolved_sum_targets,
        ) {
            return self.response_error(task, code);
        }
        let mut images = Vec::with_capacity(projected.len());
        let mut identities = Vec::with_capacity(projected.len());
        let mut notes = Vec::with_capacity(projected.len());
        for row in &projected {
            let identity = stored_row_identity(
                &row.updated_bytes,
                strict_schema.as_ref(),
                declared_primary_key,
                row.key,
            );
            let image = match self.capture_stored_document(
                &config_key,
                &row.updated_bytes,
                crate::bridge::envelope::RowVersion::open(sys_from),
            ) {
                Ok(image) => image,
                Err(error) => return self.response_error(task, error),
            };
            let note = match self.prepare_surrogate_write(
                task,
                tid,
                collection,
                row.key.surrogate().as_u32(),
            ) {
                Ok(note) => note,
                Err(error) => return self.response_error(task, error),
            };
            images.push(image);
            identities.push(identity);
            notes.push(note);
        }
        let prepared_response = match returning {
            Some(spec) => {
                let rows: Vec<_> = identities
                    .iter()
                    .zip(&projected)
                    .map(|(identity, row)| (identity, row.updated_bytes.as_slice()))
                    .collect();
                self.prepare_stored_returning_response(
                    task,
                    spec,
                    rls_filters,
                    strict_schema.as_ref(),
                    &self.identity_column(database_id, tid, collection),
                    &rows,
                )
            }
            None => Ok(self.response_affected(task, projected.len() as u64)),
        };
        let mut response = match prepared_response {
            Ok(response) => response,
            Err(error) => return self.response_error(task, error),
        };
        let txn = match self.sparse.begin_write() {
            Ok(txn) => txn,
            Err(error) => return self.response_error(task, error),
        };
        let mut undo = PreparedUpdateUndo::default();
        let mut event_images = Vec::with_capacity(projected.len());
        let hook = write_hook::HookCtx {
            target_unique: crate::data::executor::enforcement::unique::UniqueJudge::Unit,
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets: &[],
            wal_lsn: task.wal_lsn(),
        };
        for ((row, image), identity) in projected.iter().zip(images).zip(&identities) {
            let applied = self.apply_prepared_update_row(
                &txn,
                PreparedUpdateRow {
                    row: InstalledDocumentRow {
                        database_id,
                        tid,
                        collection,
                        storage_key: row.key,
                        surrogate: row.key.surrogate(),
                        wal_lsn: task.wal_lsn(),
                    },
                    identity: identity.clone(),
                    image,
                    hook: &hook,
                },
                &mut undo,
            );
            match applied {
                Ok(applied) => {
                    write_set.extend(applied.write_set);
                    event_images.push(applied.event);
                }
                Err(error) => {
                    drop(txn);
                    let error = undo.abort(self, database_id, tid, error);
                    return self.response_error(task, error);
                }
            }
        }
        if let Err(error) = undo.check_unique(self, &txn, database_id, tid) {
            drop(txn);
            let error = undo.abort(self, database_id, tid, error);
            return self.response_error(task, error);
        }
        response.write_set = write_set;
        if let Err(error) = txn.commit() {
            let error = crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("bulk update commit: {error}"),
            };
            self.fail_journalled(&error);
            let error = undo.abort(self, database_id, tid, error);
            return self.response_error(task, error);
        }
        for note in notes {
            self.publish_surrogate_write(note, sys_from);
        }
        for (identity, event) in identities.into_iter().zip(event_images) {
            self.emit_prepared_put_event(task, collection, identity, &event);
        }
        self.checkpoint_coordinator
            .mark_dirty("sparse", projected.len());
        response.watermark_lsn = self.watermark;
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::bridge::scan_filter::ScanFilter;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;
    use nodedb_physical::physical_plan::UpdateValue;
    use nodedb_types::{StorageKey, Surrogate};

    const TID: u64 = 1;
    const COLLECTION: &str = "orders";

    fn seed(core: &mut CoreLoop, database_id: u64, surrogate: u32, owner: &str) {
        let row = serde_json::json!({"owner": owner, "note": "old"});
        core.sparse
            .put(
                database_id,
                TID,
                COLLECTION,
                &StorageKey::for_surrogate(Surrogate(surrogate)),
                &doc_format::encode_to_msgpack(&row),
                core.bitemporal_ordinal(),
            )
            .expect("seed row");
    }

    fn note_of(core: &CoreLoop, database_id: u64, surrogate: u32) -> serde_json::Value {
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
        doc_format::decode_document(&stored)
            .expect("row decodes")
            .get("note")
            .cloned()
            .unwrap_or(serde_json::Value::Null)
    }

    #[test]
    fn bulk_update_preserves_append_only_and_hash_chained_rows() {
        for hash_chain in [false, true] {
            let dir = tempfile::tempdir().expect("tempdir");
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            let task = make_default_task();
            let database_id = task.request.database_id.as_u64();
            seed(&mut core, database_id, 1, "alice");
            let mut config = crate::engine::document::store::CollectionConfig::new(COLLECTION);
            config.enforcement.append_only = !hash_chain;
            config.enforcement.hash_chain = hash_chain;
            core.doc_configs.insert(
                (
                    task.request.database_id,
                    crate::types::TenantId::new(TID),
                    COLLECTION.to_owned(),
                ),
                config,
            );
            let updates = [(
                "note".to_owned(),
                UpdateValue::Literal(
                    nodedb_types::json_to_msgpack(&serde_json::json!("new")).expect("encode"),
                ),
            )];
            let response = core.execute_bulk_update(
                &task,
                TID,
                BulkUpdateParams {
                    collection: COLLECTION,
                    filter_bytes: &[],
                    updates: &updates,
                    returning: None,
                    ollp_predicted_surrogates: None,
                    ollp_predicted_edges: None,
                    rls_filters: &[],
                    rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                    resolved_sum_targets: &[],
                    declared_primary_key: None,
                },
            );
            assert!(matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::AppendOnlyViolation { .. })
            ));
            assert!(response.write_set.is_empty());
            assert_eq!(note_of(&core, database_id, 1), serde_json::json!("old"));
        }
    }

    fn owner_policy(owner: &str) -> Vec<u8> {
        let filter = ScanFilter {
            field: "owner".into(),
            op: crate::bridge::scan_filter::FilterOp::Eq,
            value: nodedb_types::Value::String(owner.into()),
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy")
    }

    /// The write policy refuses one matched row. The refusal code claims
    /// nothing applied, so no matched row can be rewritten, including the
    /// rows the policy admits.
    #[test]
    fn a_policy_refusal_on_any_matched_row_rewrites_no_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let database_id = task.request.database_id.as_u64();
        seed(&mut core, database_id, 1, "alice");
        seed(&mut core, database_id, 2, "bob");
        seed(&mut core, database_id, 3, "alice");

        let updates = vec![(
            "note".to_string(),
            UpdateValue::Literal(
                nodedb_types::json_to_msgpack(&serde_json::json!("new")).expect("encode"),
            ),
        )];
        let policy = nodedb_types::RlsWriteCheck::Predicate(owner_policy("alice"));
        let response = core.execute_bulk_update(
            &task,
            TID,
            BulkUpdateParams {
                collection: COLLECTION,
                filter_bytes: &[],
                updates: &updates,
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
                rls_filters: &[],
                rls_write_check: &policy,
                resolved_sum_targets: &[],
                declared_primary_key: None,
            },
        );

        assert_eq!(response.status, Status::Error);
        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RejectedAuthz { .. })
            ),
            "got {:?}",
            response.error_code
        );
        for surrogate in [1, 2, 3] {
            assert_eq!(
                note_of(&core, database_id, surrogate),
                serde_json::json!("old"),
                "row {surrogate} must be unchanged"
            );
        }
    }
}
