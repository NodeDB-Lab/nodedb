// SPDX-License-Identifier: BUSL-1.1

//! CRDT document-row handlers: field-carrying upsert / delete for SQL DML on
//! `crdt='true'` document collections. The Data Plane builds the Loro mutation
//! server-side, then materializes the merged row into the sparse store with
//! `EventSource::User` + text indexing so scans, secondary/spatial/vector
//! indexes, AFTER triggers, and CDC all observe it.
//!
//! An aborted write leaves the Loro row and the sparse row in agreement. An
//! upsert writes the row's captured scalar state back to Loro. A delete
//! tombstones the Loro row only after its storage delete commits.

use loro::LoroValue;
use tracing::debug;

use nodedb_types::Surrogate;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::chain_guard::{AbandonedWrite, abandon_write};
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::handlers::returning_doc;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::handlers::transaction::undo::document_outcome::{
    DocumentRow, push_delete_undo,
};
use crate::data::executor::handlers::transaction::undo::memory::abort_error;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};
use nodedb_physical::physical_plan::ReturningSpec;

/// Borrowed arguments for [`CoreLoop::execute_crdt_doc_upsert`], grouped so the
/// handler stays within the argument-count limit.
pub(in crate::data::executor) struct CrdtDocUpsert<'a> {
    pub collection: &'a str,
    pub document_id: &'a str,
    pub fields_json: &'a str,
    pub surrogate: Surrogate,
    pub partial: bool,
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled RLS read policy gating the `RETURNING` rows. Empty = no policy.
    pub rls_filters: &'a [u8],
    /// Whether the collection's typeguards judge the merged row. WAL replay
    /// passes `false`: the write's commit already judged it.
    pub judge_guards: bool,
}

/// Borrowed arguments for [`CoreLoop::execute_crdt_doc_delete`], grouped so the
/// handler stays within the argument-count limit.
pub(in crate::data::executor) struct CrdtDocDelete<'a> {
    pub collection: &'a str,
    pub document_id: &'a str,
    /// `None`: the key is unbound, so no row matches.
    pub surrogate: Option<Surrogate>,
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled RLS read policy gating the `RETURNING` rows. Empty = no policy.
    pub rls_filters: &'a [u8],
}

impl CoreLoop {
    /// Insert-or-replace (`partial = false`) or partial-merge (`partial = true`)
    /// a document row's scalar fields, server-built from `fields_json`.
    pub(in crate::data::executor) fn execute_crdt_doc_upsert(
        &mut self,
        task: &ExecutionTask,
        args: CrdtDocUpsert<'_>,
    ) -> Response {
        let CrdtDocUpsert {
            collection,
            document_id,
            fields_json,
            surrogate,
            partial,
            returning,
            rls_filters,
            judge_guards,
        } = args;
        debug!(core = self.core_id, %collection, %document_id, partial, "crdt doc upsert");
        if let Some(refusal) = crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
            "crdt", collection, surrogate,
        ) {
            return self.response_error(task, refusal);
        }
        let tenant_id = task.request.tenant_id;
        let Ok(json_map) =
            sonic_rs::from_str::<serde_json::Map<String, serde_json::Value>>(fields_json)
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("crdt doc upsert: invalid fields_json for {document_id}"),
                },
            );
        };
        let fields: Vec<(&str, LoroValue)> = json_map
            .iter()
            .map(|(k, v)| (k.as_str(), super::convert::json_to_loro_value(v)))
            .collect();

        // The row as stored before this write, for the collection's
        // transition rules. Read before Loro changes.
        let prior_body = if judge_guards {
            match self.crdt_transition_pre_image(task, collection, surrogate) {
                Ok(body) => body,
                Err(e) => return self.response_error(task, e),
            }
        } else {
            None
        };

        // The sparse row is built from the merged Loro row, so Loro changes
        // first. The row's prior scalar state is captured before that, and an
        // abort writes it back.
        let row_undo = match self.capture_crdt_row_undo(
            task.request.database_id,
            tenant_id,
            collection,
            document_id,
        ) {
            Ok(entry) => entry,
            Err(e) => return self.response_error(task, e),
        };
        let mutated = self
            .get_crdt_engine(task.request.database_id, tenant_id)
            .and_then(|engine| {
                if partial {
                    engine.doc_set_fields(collection, document_id, &fields)?;
                } else {
                    engine.doc_upsert(collection, document_id, &fields)?;
                }
                Ok(Self::encode_crdt_row(engine, collection, document_id))
            });
        // A Loro write that fails part-way commits the fields it wrote before
        // the error. Loro cannot roll them back, so the undo writes the
        // captured row image over them.
        let bytes = match mutated {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                let error = crate::Error::Internal {
                    detail: format!(
                        "crdt doc upsert: row {document_id} of '{collection}' has no \
                         document body after its write"
                    ),
                };
                return self.abandon_crdt_row(task, row_undo, error);
            }
            Err(e) => return self.abandon_crdt_row(task, row_undo, e),
        };

        // The merged row is the post-image this write produces, so the
        // collection's typeguards judge it here. A partial UPDATE judges the
        // fields it assigns. A full replace judges the whole row. A refused
        // write puts the Loro row back.
        if judge_guards {
            let assigned: Option<Vec<String>> = partial.then(|| json_map.keys().cloned().collect());
            if let Err(error) =
                self.check_crdt_row_guards(task, collection, &bytes, assigned.as_deref())
            {
                return self.abandon_crdt_row(task, row_undo, error);
            }
        }
        // The state and transition rules judge the stored row → merged row
        // change, as they judge every other update.
        if let Some(prior) = prior_body.as_deref()
            && let Err(error) = self.check_crdt_row_transitions(task, collection, prior, &bytes)
        {
            return self.abandon_crdt_row(task, row_undo, error);
        }

        // The Loro row and its sparse projection answer the same reads, so a
        // projection that did not land fails the write and puts the Loro row
        // back.
        if let Err(error) = self.materialize_document_write(
            task,
            super::crdt_materialize::CrdtMaterializeWrite {
                tid: tenant_id.as_u64(),
                collection,
                document_id,
                surrogate,
                value: &bytes,
                index_text: true,
            },
        ) {
            return self.abandon_crdt_row(task, row_undo, error);
        }
        self.checkpoint_coordinator.mark_dirty("crdt", 1);
        if let Some(spec) = returning {
            // No strict schema: a CRDT row's stored body is whatever
            // `encode_crdt_row` materialized from Loro, which is always
            // MessagePack regardless of the collection's storage mode.
            let doc = match returning_doc::from_stored(
                &bytes,
                &RowIdentity::from_user_key(document_id),
                None,
                &self.identity_column(
                    task.request.database_id.as_u64(),
                    tenant_id.as_u64(),
                    collection,
                ),
            ) {
                Ok(doc) => doc,
                Err(e) => return self.response_error(task, e),
            };
            match returning_rows::build_rows_payload(spec, rls_filters, &[doc], &self.crs_context())
            {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(task, ErrorCode::from(e)),
            }
        } else {
            self.response_affected(task, 1)
        }
    }

    /// Validate a merged CRDT row against the collection's typeguards.
    ///
    /// A CRDT collection declares no guard DEFAULT or VALUE, so nothing is
    /// injected. REQUIRED and type judge the fields in `assigned`, or every
    /// field when it is `None`. A CHECK judges every present field.
    fn check_crdt_row_guards(
        &self,
        task: &ExecutionTask,
        collection: &str,
        merged: &[u8],
        assigned: Option<&[String]>,
    ) -> crate::Result<()> {
        let config_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let Some(config) = self.doc_configs.get(&config_key) else {
            return Ok(());
        };
        if config.type_guards.is_empty() {
            return Ok(());
        }
        let row =
            nodedb_types::value_from_msgpack(merged).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("merged CRDT row of '{collection}' does not decode: {e}"),
            })?;
        crate::data::executor::enforcement::typeguard::check_type_guards(
            &config.type_guards,
            &row,
            assigned,
            &self.crs_context(),
        )
        .map_err(crate::Error::DataPlane)
    }

    /// The stored body of the row `surrogate` names, when the collection
    /// declares a transition rule and the row exists. `None` otherwise.
    fn crdt_transition_pre_image(
        &self,
        task: &ExecutionTask,
        collection: &str,
        surrogate: Surrogate,
    ) -> crate::Result<Option<Vec<u8>>> {
        let config_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let Some(config) = self.doc_configs.get(&config_key) else {
            return Ok(None);
        };
        use crate::data::executor::enforcement::update_transitions::UpdateTransitionRules;
        if UpdateTransitionRules::of(&config.enforcement).is_empty() {
            return Ok(None);
        }
        // The row at its current version, as the materialize writes it.
        self.current_row_body(
            task.request.database_id.as_u64(),
            task.request.tenant_id.as_u64(),
            collection,
            &StorageKey::for_surrogate(surrogate),
        )
    }

    /// Judge the stored `prior` row → `merged` row change against the
    /// collection's state constraints and transition checks, with the
    /// writer's roles.
    fn check_crdt_row_transitions(
        &self,
        task: &ExecutionTask,
        collection: &str,
        prior: &[u8],
        merged: &[u8],
    ) -> crate::Result<()> {
        let config_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        self.check_stored_update_transitions(
            &config_key,
            collection,
            prior,
            merged,
            &task.request.user_roles,
        )
        .map_err(crate::Error::DataPlane)
    }

    /// Write the CRDT row's captured pre-image back after its write is
    /// abandoned. Answers with the error the abort reports: `error`, or
    /// `RollbackFailed` when the row did not go back.
    fn abandon_crdt_row(
        &mut self,
        task: &ExecutionTask,
        row_undo: UndoEntry,
        error: crate::Error,
    ) -> Response {
        let undo = self.undo_memory_effects(
            task.request.database_id.as_u64(),
            task.request.tenant_id.as_u64(),
            vec![row_undo],
        );
        self.response_error(task, abort_error(error, undo))
    }

    /// Delete a document row: remove it from the sparse store with the full
    /// index cascade, then tombstone it in the collection's Loro doc, then
    /// emit the CDC delete event. Mirrors the point-delete apply path with
    /// `enforce = false`, since the write was already admitted on its origin.
    ///
    /// The Loro tombstone runs only after the storage commit. A tombstone
    /// error after the commit reverses the committed delete through the undo
    /// driver.
    pub(in crate::data::executor) fn execute_crdt_doc_delete(
        &mut self,
        task: &ExecutionTask,
        args: CrdtDocDelete<'_>,
    ) -> Response {
        let CrdtDocDelete {
            collection,
            document_id,
            surrogate,
            returning,
            rls_filters,
        } = args;
        debug!(core = self.core_id, %collection, %document_id, "crdt doc delete");
        // An unbound key names no stored row: the delete matches nothing.
        let Some(surrogate) = surrogate else {
            return self.point_delete_matched_nothing(task, returning, rls_filters);
        };
        if let Some(refusal) = crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
            "crdt", collection, surrogate,
        ) {
            return self.response_error(task, refusal);
        }
        let tenant_id = task.request.tenant_id;
        // Opening the engine can fail, so it opens before storage changes.
        if let Err(e) = self.get_crdt_engine(task.request.database_id, tenant_id) {
            return self.response_error(task, e);
        }

        let tid = tenant_id.as_u64();
        let storage_key = StorageKey::for_surrogate(surrogate);
        // The sparse-store removal and its index cascades run in one write txn
        // this handler owns: on any failure it is dropped un-committed and none
        // of them land.
        let txn = match self.sparse.begin_write() {
            Ok(txn) => txn,
            Err(e) => {
                return self.response_error(task, ErrorCode::from(e));
            }
        };
        let outcome = match self.apply_point_delete(
            &txn,
            PointDeleteParams {
                database_id: task.request.database_id.as_u64(),
                tid,
                collection,
                document_id,
                surrogate,
                user_roles: &task.request.user_roles,
                enforce: false,
                resolved_targets: &[],
            },
        ) {
            Ok(outcome) => outcome,
            Err(e) => {
                return self.response_error(task, ErrorCode::from(e));
            }
        };
        if let Err(e) = txn.commit() {
            // The dropped txn reverses the durable writes only. The
            // in-memory cascades are reversed here.
            let e = abandon_write(
                self,
                AbandonedWrite::row(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    &storage_key,
                )
                .undo(outcome.memory_undo),
                crate::Error::DataPlane(ErrorCode::Internal {
                    detail: format!("commit: {e}"),
                }),
            );
            return self.response_error(task, e);
        }
        self.checkpoint_coordinator.mark_dirty("sparse", 1);
        let row = DocumentRow {
            collection,
            storage_key,
        };
        // The storage delete is committed, so the Loro tombstone lands now. A
        // tombstone error reverses the committed delete: Loro and storage
        // then both still hold the row.
        let tombstone = self
            .get_crdt_engine(task.request.database_id, tenant_id)
            .and_then(|engine| engine.doc_delete(collection, document_id));
        if let Err(e) = tombstone {
            let mut undo = Vec::new();
            push_delete_undo(&mut undo, row, outcome);
            let reversed = self.undo_memory_effects(task.request.database_id.as_u64(), tid, undo);
            return self.response_error(task, abort_error(e, reversed));
        }
        self.checkpoint_coordinator.mark_dirty("crdt", 1);
        let prior_value = outcome.prior_value.clone();
        if self.recording_redo_undo() {
            let mut undo = Vec::new();
            push_delete_undo(&mut undo, row, outcome);
            self.record_redo_undo(undo);
        }

        // Emit the delete to the Event Plane only when a row was actually
        // removed, threading the pre-delete bytes through as `old_value` so
        // CDC/change-stream consumers observe the prior state.
        if let Some(prior_bytes) = prior_value.as_deref() {
            self.emit_document_delete_event(
                task,
                tid,
                collection,
                RowIdentity::from_user_key(document_id),
                Some(prior_bytes),
            );
        }

        // Project the pre-deletion row for RETURNING. `prior_value` is
        // only borrowed by the CDC emit above (via `.as_deref()`), so it is
        // still available here; the user-visible `document_id` fills the
        // identity column exactly like PointDelete.
        if let Some(spec) = returning {
            if let Some(prior_bytes) = prior_value.as_deref() {
                // No strict schema — see the upsert path: a CRDT row is
                // materialized as MessagePack in either storage mode.
                let doc = match returning_doc::from_stored(
                    prior_bytes,
                    &RowIdentity::from_user_key(document_id),
                    None,
                    &self.identity_column(
                        task.request.database_id.as_u64(),
                        tenant_id.as_u64(),
                        collection,
                    ),
                ) {
                    Ok(doc) => doc,
                    Err(e) => return self.response_error(task, e),
                };
                match returning_rows::build_rows_payload(
                    spec,
                    rls_filters,
                    &[doc],
                    &self.crs_context(),
                ) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, ErrorCode::from(e)),
                }
            } else {
                match returning_rows::build_rows_payload(
                    spec,
                    rls_filters,
                    &[],
                    &self.crs_context(),
                ) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(task, ErrorCode::from(e)),
                }
            }
        } else {
            // No RETURNING: report what the delete actually removed. A tombstone
            // written over an already-absent document removes nothing.
            self.response_affected(task, u64::from(prior_value.is_some()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::engine::document::store::{CollectionConfig, IndexPath};
    use crate::types::{DatabaseId, TenantId};

    const DB: u64 = 0;
    const TID: u64 = 1;
    const COLL: &str = "notes";
    const DOC: &str = "n1";
    const SURROGATE: Surrogate = Surrogate(7);

    fn upsert(core: &mut CoreLoop, task: &ExecutionTask, fields_json: &str) -> Response {
        core.execute_crdt_doc_upsert(
            task,
            CrdtDocUpsert {
                collection: COLL,
                document_id: DOC,
                fields_json,
                surrogate: SURROGATE,
                partial: false,
                returning: None,
                rls_filters: &[],
                judge_guards: true,
            },
        )
    }

    fn delete(core: &mut CoreLoop, task: &ExecutionTask) -> Response {
        core.execute_crdt_doc_delete(
            task,
            CrdtDocDelete {
                collection: COLL,
                document_id: DOC,
                surrogate: Some(SURROGATE),
                returning: None,
                rls_filters: &[],
            },
        )
    }

    fn crdt_row(core: &mut CoreLoop) -> Option<loro::LoroValue> {
        core.get_crdt_engine(DatabaseId::DEFAULT, TenantId::new(TID))
            .expect("engine")
            .read_row(COLL, DOC)
    }

    fn i64_field(row: &loro::LoroValue, key: &str) -> Option<i64> {
        let loro::LoroValue::Map(map) = row else {
            return None;
        };
        match map.get(key) {
            Some(loro::LoroValue::I64(n)) => Some(*n),
            _ => None,
        }
    }

    /// Make the storage step of the next write fail: the collection gets an
    /// index path, and its stored row an undecodable body. A write that reads
    /// the prior row to diff its index entries then refuses.
    fn break_stored_row(core: &mut CoreLoop) {
        let mut config = CollectionConfig::new(COLL);
        config.index_paths.push(IndexPath::new("a"));
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string()),
            config,
        );
        let key = StorageKey::for_surrogate(SURROGATE);
        core.sparse
            .put(DB, TID, COLL, &key, &[0xc1])
            .expect("overwrite the stored row");
        core.doc_cache.invalidate(DB, TID, COLL, &key);
    }

    /// A CRDT delete whose storage step fails leaves the row readable from
    /// the CRDT state, as storage still holds it.
    #[test]
    fn a_refused_crdt_delete_leaves_the_crdt_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        assert_eq!(
            upsert(&mut core, &task, r#"{"a":1,"b":2}"#).status,
            Status::Ok
        );
        break_stored_row(&mut core);

        let resp = delete(&mut core, &task);
        assert_eq!(resp.status, Status::Error, "the storage step must refuse");
        assert!(
            !matches!(
                resp.error_code.as_deref(),
                Some(ErrorCode::RollbackFailed { .. })
            ),
            "the refusal must reverse cleanly, got {:?}",
            resp.error_code
        );
        let row = crdt_row(&mut core).expect("the CRDT row must survive the refused delete");
        assert_eq!(i64_field(&row, "a"), Some(1));
        assert_eq!(i64_field(&row, "b"), Some(2));
        assert!(
            core.sparse
                .get(DB, TID, COLL, &StorageKey::for_surrogate(SURROGATE))
                .expect("read back")
                .is_some(),
            "the refused delete leaves the stored row"
        );
    }

    /// A committed CRDT delete removes the row from both the CRDT state and
    /// storage.
    #[test]
    fn a_crdt_delete_removes_the_row_from_both_stores() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        assert_eq!(upsert(&mut core, &task, r#"{"a":1}"#).status, Status::Ok);

        assert_eq!(delete(&mut core, &task).status, Status::Ok);
        assert!(crdt_row(&mut core).is_none());
        assert!(
            core.sparse
                .get(DB, TID, COLL, &StorageKey::for_surrogate(SURROGATE))
                .expect("read back")
                .is_none()
        );
    }

    /// A CRDT upsert whose sparse projection fails puts the Loro row back to
    /// its fields before the write.
    #[test]
    fn a_refused_crdt_upsert_puts_the_crdt_row_back() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        assert_eq!(
            upsert(&mut core, &task, r#"{"a":1,"b":2}"#).status,
            Status::Ok
        );
        let before = crdt_row(&mut core);
        break_stored_row(&mut core);

        let resp = upsert(&mut core, &task, r#"{"a":5,"c":3}"#);
        assert_eq!(resp.status, Status::Error, "the projection must refuse");
        assert!(
            !matches!(
                resp.error_code.as_deref(),
                Some(ErrorCode::RollbackFailed { .. })
            ),
            "the refusal must reverse cleanly, got {:?}",
            resp.error_code
        );
        assert_eq!(
            crdt_row(&mut core),
            before,
            "the refused upsert leaves the CRDT row as it was"
        );
    }

    /// Register `notes` as a CRDT collection whose `a` must be a present INT.
    fn guard_a(core: &mut CoreLoop) {
        let mut config = CollectionConfig::new(COLL);
        config.crdt_enabled = true;
        config.type_guards =
            crate::data::executor::enforcement::typeguard::CompiledTypeGuard::compile_all(
                COLL,
                &[nodedb_types::TypeGuardFieldDef {
                    field: "a".to_string(),
                    type_expr: "INT".to_string(),
                    required: true,
                    check_expr: None,
                    default_expr: None,
                    value_expr: None,
                }],
            );
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string()),
            config,
        );
    }

    fn partial_set(core: &mut CoreLoop, task: &ExecutionTask, fields_json: &str) -> Response {
        core.execute_crdt_doc_upsert(
            task,
            CrdtDocUpsert {
                collection: COLL,
                document_id: DOC,
                fields_json,
                surrogate: SURROGATE,
                partial: true,
                returning: None,
                rls_filters: &[],
                judge_guards: true,
            },
        )
    }

    /// The merged row a CRDT write produces is judged by the typeguards, and
    /// a refused write puts the Loro row back.
    #[test]
    fn a_crdt_write_violating_a_typeguard_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        guard_a(&mut core);
        assert_eq!(upsert(&mut core, &task, r#"{"a":1}"#).status, Status::Ok);
        let before = crdt_row(&mut core);

        let resp = upsert(&mut core, &task, r#"{"b":2}"#);
        assert!(
            matches!(
                resp.error_code.as_deref(),
                Some(ErrorCode::TypeGuardViolation { .. })
            ),
            "a full replace without `a` must be refused, got {:?}",
            resp.error_code
        );
        assert_eq!(crdt_row(&mut core), before);

        let resp = partial_set(&mut core, &task, r#"{"a":"text"}"#);
        assert!(
            matches!(
                resp.error_code.as_deref(),
                Some(ErrorCode::TypeGuardViolation { .. })
            ),
            "a partial UPDATE assigning a non-INT must be refused, got {:?}",
            resp.error_code
        );
        assert_eq!(crdt_row(&mut core), before);

        assert_eq!(
            partial_set(&mut core, &task, r#"{"b":2}"#).status,
            Status::Ok,
            "a partial UPDATE that leaves `a` intact lands"
        );
    }
}
