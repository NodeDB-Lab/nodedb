// SPDX-License-Identifier: BUSL-1.1

//! The upsert overwrite branch: an existing row was found, so merge the
//! incoming value into it (or apply `ON CONFLICT DO UPDATE SET`), persist,
//! and respond.

use super::merge_body::{MergedUpsert, decode_stored_row, merge_upsert_body};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::WriteSetRow;
use crate::data::executor::enforcement::chain_guard::{AbandonedWrite, abandon_write};
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};
use nodedb_types::Surrogate;
use nodedb_types::columnar::StrictSchema;

/// Everything the overwrite branch needs, resolved once by the caller
/// (`execute_upsert`) so this branch never re-derives it.
pub(super) struct OverwriteCtx<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    pub on_conflict_updates: &'a [(String, nodedb_physical::physical_plan::UpdateValue)],
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    pub rls_filters: &'a [u8],
    pub database_id: u64,
    pub hook_ctx: &'a HookCtx<'a>,
    pub strict_schema: Option<&'a StrictSchema>,
    /// The row's current stored bytes, already read by the probe in
    /// `execute_upsert` — this branch never re-reads it.
    pub current_bytes: Vec<u8>,
}

impl CoreLoop {
    /// Merge `value` into the existing row named by `ctx`, persist, and
    /// respond. See [`super::dispatch::execute_upsert`] for the probe that
    /// dispatches here.
    pub(super) fn execute_upsert_overwrite(
        &mut self,
        task: &ExecutionTask,
        ctx: OverwriteCtx<'_>,
    ) -> Response {
        let OverwriteCtx {
            tid,
            collection,
            document_id,
            surrogate,
            value,
            on_conflict_updates,
            rls_write_check,
            returning,
            rls_filters,
            database_id,
            hook_ctx,
            strict_schema,
            current_bytes,
        } = ctx;

        let storage_key = StorageKey::for_surrogate(surrogate);
        // The plan's `document_id` is the row's client identity: the write
        // gate, the event, the redo entry, and `RETURNING` all name it.
        let document_identity = RowIdentity::from_user_key(document_id);

        // Conflict branch: the `ON CONFLICT DO UPDATE SET` assignments
        // evaluated against the existing row, else the plain merge
        // `UPSERT INTO` uses. The fields the merge writes are the ones
        // typeguards judge.
        let MergedUpsert {
            body: merged_body,
            assigned,
        } = match decode_stored_row(&current_bytes, strict_schema).and_then(|existing| {
            merge_upsert_body(existing, value, on_conflict_updates, &self.crs_context())
        }) {
            Ok(merged) => merged,
            Err(e) => return self.response_error(task, e),
        };

        let identity_column = self.identity_column(database_id, tid, collection);

        // One transaction for the body, every index that describes it,
        // and every derived write the collection's constraints imply.
        // The bare `sparse.put` this replaces reconciled none of them:
        // the row's FTS postings, secondary indexes, UNIQUE checks and
        // column statistics all kept asserting the values it used to
        // hold, and the write landed outside any transaction a
        // constraint could join.
        let prepared_note =
            match self.prepare_surrogate_write(task, tid, collection, surrogate.as_u32()) {
                Ok(note) => note,
                Err(error) => return self.response_error(task, error),
            };

        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(task, ErrorCode::from(e));
            }
        };

        // The put below ends every prior vector node of the row at the new
        // version's ordinal and inserts the new ones there. Its in-memory
        // undo joins this list, so an abort puts the prior nodes back.
        let mut memory_undo: Vec<UndoEntry> = Vec::new();
        let mut outcome = match self
            .prepare_point_put(
                &txn,
                PointPutParams {
                    database_id,
                    tid,
                    collection,
                    storage_key,
                    surrogate,
                    value: &merged_body,
                    index_text: true,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    unique: crate::data::executor::enforcement::unique::UniqueJudge::Row,
                    wal_lsn: task.wal_lsn(),
                    resolved_targets: hook_ctx.resolved_targets,
                    guards: crate::data::executor::enforcement::typeguard::GuardScope::Assigned(
                        &assigned,
                    ),
                },
            )
            .and_then(|prepared| {
                rls_write_gate::admit_installed_row(
                    rls_write_check,
                    prepared.installed(),
                    &document_identity,
                    &identity_column,
                    tid,
                    collection,
                    &self.crs_context(),
                )?;
                self.apply_prepared_point_put(&txn, prepared)
            }) {
            Ok(o) => o,
            Err(e) => {
                // Some rejections land after the row was cached;
                // dropping `txn` reverses the durable write but not
                // that entry, which would then serve a body that never
                // committed.
                let e = abandon_write(
                    self,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key)
                        .undo(memory_undo),
                    e,
                );
                return self.response_error(task, e);
            }
        };
        memory_undo.append(&mut outcome.memory_undo);

        // An overwrite is an UPDATE, and the pre-image is what tells the
        // fold to take the row's old contribution off a total before
        // adding its new one. `current_bytes` is the STORED pre-merge
        // row read above.
        let enforcement = match write_hook::run(
            self,
            &txn,
            hook_ctx,
            match outcome.prior_value.as_deref() {
                Some(old) => WriteImages::Update {
                    old: ImageBody::Stored(old),
                    new: ImageBody::Stored(&outcome.installed.stored),
                },
                None => WriteImages::Insert {
                    new: ImageBody::Stored(&outcome.installed.stored),
                },
            },
        ) {
            Ok(o) => o,
            Err(e) => {
                let e = abandon_write(
                    self,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key)
                        .undo(memory_undo),
                    e,
                );
                return self.response_error(task, e);
            }
        };
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);
        let target_writes = enforcement.target_writes;

        if let Err(error) = self.check_source_and_targets_unique(
            &txn,
            database_id,
            tid,
            (collection, storage_key),
            &target_writes,
        ) {
            let error = abandon_write(
                self,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(memory_undo)
                    .targets(target_writes),
                error,
            );
            return self.response_error(task, error);
        }

        // Settled before the commit: the merged row's old amount comes
        // off the group and its new one goes on, so an overwrite that
        // leaves the group unbalanced is refused with nothing written.
        if let Err(e) =
            self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)
        {
            let e = abandon_write(
                self,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(memory_undo)
                    .targets(target_writes),
                e,
            );
            return self.response_error(task, e);
        }

        let stored_bytes = &outcome.installed.stored;
        let prepared_response = match returning {
            // The MERGED body, not the caller's: on a conflict the
            // submitted values are only part of what the row now holds,
            // so echoing them would report a row that does not exist.
            Some(spec) => self.prepare_stored_returning_response(
                task,
                spec,
                rls_filters,
                strict_schema,
                &identity_column,
                &[(&document_identity, stored_bytes.as_slice())],
            ),
            None => Ok(self.response_affected(task, 1)),
        };
        let mut response = match prepared_response {
            Ok(response) => response,
            Err(error) => {
                let error = abandon_write(
                    self,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key)
                        .undo(memory_undo)
                        .targets(target_writes),
                    error,
                );
                return self.response_error(task, error);
            }
        };
        // `wal_append_document_op` mints no pre-dispatch record for an
        // upsert, so the merged row is journalled after apply, from the body
        // `apply_point_put` took.
        let event = match self.prepare_installed_put_event(
            &WriteSetRow {
                database_id,
                tid,
                collection,
                identity: document_identity.clone(),
            },
            &outcome.installed,
            outcome.prior_value.as_deref(),
        ) {
            Ok(event) => event,
            Err(error) => {
                let error = abandon_write(
                    self,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key)
                        .undo(memory_undo)
                        .targets(target_writes),
                    error,
                );
                return self.response_error(task, error);
            }
        };
        let image = crate::bridge::envelope::WriteSetEntry::installed_put(
            surrogate.as_u32(),
            document_identity.clone(),
            outcome.installed.clone(),
            event.clone(),
        );
        response.write_set = vec![image];
        // Derived target rows live in a different collection, so each
        // carries its own `Some(collection)` and homes to that
        // collection's vShard.
        response.write_set.extend(target_write_set);

        if let Err(e) = txn.commit() {
            self.seal_journalled();
            self.fail_stop_core(
                crate::data::executor::core_loop::fail_stop::FailStopCause::WriteSetUnpersisted,
                &format!("document commit outcome is unknown: {e}"),
            );
            let e = abandon_write(
                self,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(memory_undo)
                    .targets(target_writes),
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("commit: {e}"),
                },
            );
            return self.response_error(task, e);
        }

        // Record the committed row's version, as a point put does.
        self.publish_surrogate_write(prepared_note, outcome.installed.version.sys_from);

        // `current_bytes` is the pre-merge stored row, already read
        // above — thread it to the Event Plane as `old_value` so the
        // emitted WriteOp resolves to Update.
        self.emit_prepared_put_event(task, collection, document_identity.clone(), &event);

        // An upsert always writes the row: one row affected.
        response.watermark_lsn = self.watermark;
        response
    }
}
