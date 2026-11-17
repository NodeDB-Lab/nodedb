// SPDX-License-Identifier: BUSL-1.1

//! The upsert insert branch: no existing row was found, so insert fresh
//! (identical in shape to a `PointPut`, plus chain + enforcement).

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::WriteSetRow;
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

        // This arm is INSERT-shaped by construction — the probe above
        // found no row — so every write it performs is a chain link.
        // The row is marked before the write, so `build_stored_body`
        // writes its link into the stored body.
        let prepared_note =
            match self.prepare_surrogate_write(task, tid, collection, surrogate.as_u32()) {
                Ok(note) => note,
                Err(error) => return self.response_error(task, error),
            };

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
        let mut prior = match self
            .prepare_point_put(
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
                    guards: crate::data::executor::enforcement::typeguard::GuardScope::Document,
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
            .settle_installed(self, surrogate, &mut prior.installed)
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

        // Fold the actual stored mutation, including defaults and generated fields.
        let enforcement = match write_hook::run(
            self,
            &txn,
            hook_ctx,
            match prior.prior_value.as_deref() {
                Some(old) => WriteImages::Update {
                    old: ImageBody::Stored(old),
                    new: ImageBody::Stored(&prior.installed.stored),
                },
                None => WriteImages::Insert {
                    new: ImageBody::Stored(&prior.installed.stored),
                },
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

        if let Err(error) = self.check_source_and_targets_unique(
            &txn,
            database_id,
            tid,
            (collection, storage_key),
            &target_writes,
        ) {
            let error = chain_guard::abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::row(database_id, tid, collection, &storage_key)
                    .undo(std::mem::take(&mut prior.memory_undo))
                    .targets(target_writes),
                error,
            );
            return self.response_error(task, error);
        }

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

        let prepared_response = match returning {
            Some(spec) => self.prepare_stored_returning_response(
                task,
                spec,
                rls_filters,
                strict_schema,
                &identity_column,
                &[(&document_identity, prior.installed.stored.as_slice())],
            ),
            None => Ok(self.response_affected(task, 1)),
        };
        let mut response = match prepared_response {
            Ok(response) => response,
            Err(error) => {
                let error = chain_guard::abort_after_apply(
                    self,
                    &mut chain,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key)
                        .undo(std::mem::take(&mut prior.memory_undo))
                        .targets(target_writes),
                    error,
                );
                return self.response_error(task, error);
            }
        };
        // `wal_append_document_op` mints no pre-dispatch record for an
        // upsert, so the row the insert branch stored is journalled after
        // apply, from the body `apply_point_put` took.
        let event = match self.prepare_installed_put_event(
            &WriteSetRow {
                database_id,
                tid,
                collection,
                identity: document_identity.clone(),
            },
            &prior.installed,
            prior.prior_value.as_deref(),
        ) {
            Ok(event) => event,
            Err(error) => {
                let error = chain_guard::abort_after_apply(
                    self,
                    &mut chain,
                    AbandonedWrite::row(database_id, tid, collection, &storage_key)
                        .undo(std::mem::take(&mut prior.memory_undo))
                        .targets(target_writes),
                    error,
                );
                return self.response_error(task, error);
            }
        };
        let image = crate::bridge::envelope::WriteSetEntry::installed_put(
            surrogate.as_u32(),
            document_identity.clone(),
            prior.installed.clone(),
            event.clone(),
        );
        response.write_set = vec![image];
        response.write_set.extend(target_write_set);

        if let Err(e) = txn.commit() {
            self.seal_journalled();
            self.fail_stop_core(
                crate::data::executor::core_loop::fail_stop::FailStopCause::WriteSetUnpersisted,
                &format!("document commit outcome is unknown: {e}"),
            );
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

        // Record the committed row's version, as a point put does.
        self.publish_surrogate_write(prepared_note, prior.installed.version.sys_from);

        self.emit_prepared_put_event(task, collection, document_identity.clone(), &event);

        // An upsert always writes the row: one row affected.
        response.watermark_lsn = self.watermark;
        response
    }
}
