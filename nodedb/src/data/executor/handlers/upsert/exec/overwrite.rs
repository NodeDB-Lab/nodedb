// SPDX-License-Identifier: BUSL-1.1

//! The upsert overwrite branch: an existing row was found, so merge the
//! incoming value into it (or apply `ON CONFLICT DO UPDATE SET`), persist,
//! and respond.

use super::merge_body::{MergedUpsert, decode_stored_row, merge_upsert_body};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::submitted_row_image;
use crate::data::executor::enforcement::chain_guard::{AbandonedWrite, abandon_write};
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_put::{PointPutParams, VectorIndexDelta};
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
    pub has_vectors: bool,
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
            has_vectors,
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

        // Gate the persist on the collection's write policy, decided
        // against the MERGED row — the row that will exist afterwards.
        // The insert body alone would clear a write whose actual
        // post-image the policy never saw, which is why this branch
        // cannot be admitted at plan time. Decided on the MessagePack
        // form, exactly as the insert arm below decides its own body.
        let identity_column = self.identity_column(database_id, tid, collection);
        if let Err(e) = rls_write_gate::admit_stored_row(
            rls_write_check,
            rls_write_gate::GatedRow {
                body: &merged_body,
                identity: &document_identity,
                strict_schema: None,
                identity_column: &identity_column,
            },
            tid,
            collection,
            &self.crs_context(),
        ) {
            return self.response_error(task, e);
        }

        // One transaction for the body, every index that describes it,
        // and every derived write the collection's constraints imply.
        // The bare `sparse.put` this replaces reconciled none of them:
        // the row's FTS postings, secondary indexes, UNIQUE checks and
        // column statistics all kept asserting the values it used to
        // hold, and the write landed outside any transaction a
        // constraint could join.
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(task, ErrorCode::from(e));
            }
        };

        // The surrogate is stable across an overwrite and
        // `insert_with_surrogate` APPENDS an HNSW node rather than
        // replacing one, so the prior embedding has to come out before
        // the write below puts the new one in — otherwise KNN keeps
        // scoring both. No-op when `has_vectors` is false. The removal is
        // in memory, so an abort below puts the prior nodes back.
        let mut memory_undo: Vec<UndoEntry> = Vec::new();
        if has_vectors {
            memory_undo.extend(
                self.remove_document_vector_indexes(database_id, tid, collection, storage_key)
                    .into_iter()
                    .map(VectorIndexDelta::into_delete_undo),
            );
        }
        let mut outcome = match self.apply_point_put(
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
        ) {
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
            WriteImages::Update {
                old: ImageBody::Stored(&current_bytes),
                new: ImageBody::Submitted(&merged_body),
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

        if let Err(e) = txn.commit() {
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

        // Record the committed row's version and its touched index values,
        // as a point put does.
        self.note_surrogate_write(task, tid, collection, surrogate.as_u32());
        if let Some(stamp) = self.task_write_stamp(task) {
            let mut tuples = std::mem::take(&mut outcome.secondary_index_added);
            tuples.append(&mut outcome.secondary_index_removed);
            tuples.append(&mut outcome.bitemporal_index_tuples);
            self.note_index_write_values(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
                &tuples,
                stamp,
            );
        }

        // `current_bytes` is the pre-merge stored row, already read
        // above — thread it to the Event Plane as `old_value` so the
        // emitted WriteOp resolves to Update.
        let stored_bytes = outcome.stored_value;
        self.emit_put_event(
            task,
            tid,
            collection,
            document_identity.clone(),
            &stored_bytes,
            Some(&current_bytes),
        );

        // An upsert always writes the row: one row affected.
        let mut response = match returning {
            // The MERGED body, not the caller's: on a conflict the
            // submitted values are only part of what the row now holds,
            // so echoing them would report a row that does not exist.
            Some(spec) => self.stored_returning_response(
                task,
                spec,
                rls_filters,
                strict_schema,
                &identity_column,
                &[(&document_identity, stored_bytes.as_slice())],
            ),
            None => self.response_affected(task, 1),
        };
        // `wal_append_document_op` mints no pre-dispatch record for an
        // upsert, so the merged row is journalled after apply, from the body
        // `apply_point_put` took.
        response.write_set = vec![submitted_row_image(
            surrogate.as_u32(),
            document_identity,
            merged_body,
            outcome.bitemporal_sys_from,
        )];
        // Derived target rows live in a different collection, so each
        // carries its own `Some(collection)` and homes to that
        // collection's vShard.
        response.write_set.extend(target_write_set);
        response
    }
}
