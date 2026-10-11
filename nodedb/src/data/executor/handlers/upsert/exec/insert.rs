// SPDX-License-Identifier: BUSL-1.1

//! The upsert insert branch: no existing row was found, so insert fresh
//! (identical in shape to a `PointPut`, plus chain + enforcement).

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::submitted_row_image;
use crate::data::executor::enforcement::chain_guard::{self, AbandonedWrite, ChainGuard};
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};
use nodedb_types::Surrogate;
use nodedb_types::columnar::StrictSchema;

/// Everything the insert branch needs, resolved once by the caller
/// (`execute_upsert`) so this branch never re-derives it.
pub(super) struct InsertCtx<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    pub rls_filters: &'a [u8],
    pub database_id: u64,
    pub hook_ctx: &'a HookCtx<'a>,
    pub strict_schema: Option<&'a StrictSchema>,
}

impl CoreLoop {
    /// Insert `value` as a fresh row named by `ctx`, persist, and respond.
    /// See [`super::dispatch::execute_upsert`] for the probe that dispatches
    /// here.
    pub(super) fn execute_upsert_insert(
        &mut self,
        task: &ExecutionTask,
        ctx: InsertCtx<'_>,
    ) -> Response {
        let InsertCtx {
            tid,
            collection,
            document_id,
            surrogate,
            value,
            rls_write_check,
            returning,
            rls_filters,
            database_id,
            hook_ctx,
            strict_schema,
        } = ctx;

        let storage_key = StorageKey::for_surrogate(surrogate);
        // The plan's `document_id` is the row's client identity: the write
        // gate, the event, the redo entry, and `RETURNING` all name it.
        let document_identity = RowIdentity::from_user_key(document_id);
        let identity_column = self.identity_column(database_id, tid, collection);

        // Insert: document doesn't exist, create new (same as PointPut).
        // The incoming body IS the post-image here, and the planner
        // emits it as MessagePack for both storage modes (the strict
        // tuple is encoded on the way to disk), so it is decoded
        // without a schema.
        if let Err(e) = rls_write_gate::admit_stored_row(
            rls_write_check,
            value,
            &document_identity,
            None,
            &identity_column,
            tid,
            collection,
        ) {
            return self.response_error(task, e);
        }

        // This arm is INSERT-shaped by construction — the probe above
        // found no row — so every write it performs is a chain link.
        // The row is marked before the write, so `build_stored_body`
        // writes its link into the stored body.
        let mut chain = ChainGuard::begin(self, database_id, tid, collection);
        if let Err(e) = chain.chain_insert(self, surrogate, value) {
            return self.response_error(task, e);
        }

        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                chain.restore(self);
                return self.response_error(task, e);
            }
        };

        // `apply_point_put` returns prior bytes if any; here the
        // existence probe just above found none, and apply_point_put
        // is the only writer on this core — prior must be None. We
        // pass it straight through so the emit resolves to Insert.
        let mut prior = match self.apply_point_put(
            &txn,
            PointPutParams {
                database_id,
                tid,
                collection,
                storage_key,
                surrogate,
                value,
                index_text: true,
                user_roles: &task.request.user_roles,
                enforce: true,
                unique: crate::data::executor::enforcement::unique::UniqueJudge::Row,
                wal_lsn: task.wal_lsn(),
                resolved_targets: hook_ctx.resolved_targets,
            },
        ) {
            Ok(p) => p,
            Err(e) => {
                let e = chain_guard::abort_after_apply(
                    self,
                    &mut chain,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key),
                    e,
                );
                return self.response_error(task, e);
            }
        };

        // The advanced head lands in the SAME transaction as the row
        // whose hash it is.
        if let Err(e) = chain
            .settle(self, surrogate, &prior.stored_value)
            .and_then(|()| chain.persist_head(self, &txn))
        {
            let e = chain_guard::abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(std::mem::take(&mut prior.memory_undo)),
                e,
            );
            return self.response_error(task, e);
        }

        // The post-image is the SUBMITTED body, never the chained one:
        // `_chain_hash` is a wrapper the chain adds around the row and
        // no constraint is declared over it.
        let enforcement = match write_hook::run(
            self,
            &txn,
            hook_ctx,
            WriteImages::Insert {
                new: ImageBody::Submitted(value),
            },
        ) {
            Ok(o) => o,
            Err(e) => {
                let e = chain_guard::abort_after_apply(
                    self,
                    &mut chain,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key)
                        .undo(std::mem::take(&mut prior.memory_undo)),
                    e,
                );
                return self.response_error(task, e);
            }
        };
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);
        let target_writes = enforcement.target_writes;

        // Settled before the commit, so an insert of one journal leg on
        // its own leaves nothing behind.
        if let Err(e) =
            self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)
        {
            let e = chain_guard::abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(std::mem::take(&mut prior.memory_undo))
                    .targets(target_writes),
                e,
            );
            return self.response_error(task, e);
        }

        if let Err(e) = txn.commit() {
            let e = chain_guard::abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(std::mem::take(&mut prior.memory_undo))
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
            let mut tuples = std::mem::take(&mut prior.secondary_index_added);
            tuples.append(&mut prior.secondary_index_removed);
            tuples.append(&mut prior.bitemporal_index_tuples);
            self.note_index_write_values(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
                &tuples,
                stamp,
            );
        }

        self.emit_put_event(
            task,
            tid,
            collection,
            document_identity.clone(),
            value,
            prior.prior_value.as_deref(),
        );

        // An upsert always writes the row: one row affected.
        let mut response = match returning {
            Some(spec) => self.stored_returning_response(
                task,
                spec,
                rls_filters,
                strict_schema,
                &identity_column,
                &[(&document_identity, prior.stored_value.as_slice())],
            ),
            None => self.response_affected(task, 1),
        };
        // `wal_append_document_op` mints no pre-dispatch record for an
        // upsert, so the row the insert branch stored is journalled after
        // apply, from the body `apply_point_put` took.
        response.write_set = vec![submitted_row_image(
            surrogate.as_u32(),
            document_identity,
            value.to_vec(),
            prior.bitemporal_sys_from_ms,
        )];
        response.write_set.extend(target_write_set);
        response
    }
}
