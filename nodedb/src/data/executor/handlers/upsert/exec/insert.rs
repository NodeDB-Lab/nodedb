// SPDX-License-Identifier: BUSL-1.1

//! The upsert insert branch: no existing row was found, so insert fresh
//! (identical in shape to a `PointPut`, plus chain + enforcement).

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::chain_guard::{self, ChainGuard};
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
    pub row_key: &'a str,
    pub value: &'a [u8],
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    pub rls_filters: &'a [u8],
    pub database_id: u64,
    pub hook_ctx: &'a HookCtx<'a>,
    pub has_vectors: bool,
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
            row_key,
            value,
            rls_write_check,
            returning,
            rls_filters,
            database_id,
            hook_ctx,
            has_vectors,
            strict_schema,
        } = ctx;

        let storage_key = StorageKey::for_surrogate(surrogate);
        let row_identity = storage_key.to_identity();
        let document_identity = RowIdentity::from_user_key(document_id);

        // Insert: document doesn't exist, create new (same as PointPut).
        // The incoming body IS the post-image here, and the planner
        // emits it as MessagePack for both storage modes (the strict
        // tuple is encoded on the way to disk), so it is decoded
        // without a schema.
        if let Err(e) = rls_write_gate::admit_stored_row(
            rls_write_check,
            value,
            &row_identity,
            None,
            tid,
            collection,
        ) {
            return self.response_error(task, e);
        }

        // This arm is INSERT-shaped by construction — the probe above
        // found no row — so every write it performs is a chain link.
        // Chaining rewrites the BODY, so it runs before the body is
        // encoded and stored.
        let mut chain = ChainGuard::begin(self, database_id, tid, collection);
        let chained = match chain.chain_insert(self, database_id, tid, document_id, value) {
            Ok(chained) => chained,
            Err(e) => return self.response_error(task, e),
        };
        let effective_value: &[u8] = chained.as_deref().unwrap_or(value);

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
        let prior = match self.apply_point_put(
            &txn,
            PointPutParams {
                database_id,
                tid,
                collection,
                document_id: row_key,
                surrogate,
                value: effective_value,
                index_text: true,
                user_roles: &task.request.user_roles,
                enforce: true,
                wal_lsn: task.wal_lsn(),
                resolved_targets: hook_ctx.resolved_targets,
            },
        ) {
            Ok(p) => p,
            Err(e) => {
                chain_guard::abort_after_apply(
                    self,
                    &chain,
                    database_id,
                    tid,
                    collection,
                    &storage_key,
                );
                return self.response_error(task, e);
            }
        };

        // The advanced head lands in the SAME transaction as the row
        // whose hash it is.
        if let Err(e) = chain.persist_head(self, &txn) {
            chain_guard::abort_after_apply(
                self,
                &chain,
                database_id,
                tid,
                collection,
                &storage_key,
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
                chain_guard::abort_after_apply(
                    self,
                    &chain,
                    database_id,
                    tid,
                    collection,
                    &storage_key,
                );
                return self.response_error(task, e);
            }
        };
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);

        // Settled before the commit, so an insert of one journal leg on
        // its own leaves nothing behind.
        if let Err(e) =
            self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)
        {
            chain_guard::abort_after_apply(
                self,
                &chain,
                database_id,
                tid,
                collection,
                &storage_key,
            );
            return self.response_error(task, e);
        }

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("commit: {e}"),
                },
            );
        }

        self.emit_put_event(
            task,
            tid,
            collection,
            row_identity,
            value,
            prior.prior_value.as_deref(),
        );

        // `apply_point_put` already inserted this row's vectors into the
        // live HNSW, so the insert branch needs no live re-index — only a
        // durable post-apply `Put` redo so a WAL-only restart rebuilds the
        // index with the new embedding. `value` is a borrowed param here,
        // so the post-image is copied. No-op when `has_vectors` is false.
        // An upsert always writes the row: one row affected.
        let mut response = match returning {
            Some(spec) => self.stored_returning_response(
                task,
                spec,
                rls_filters,
                strict_schema,
                &[(&document_identity, prior.stored_value.as_slice())],
            ),
            None => self.response_affected(task, 1),
        };
        if has_vectors {
            response.write_set = vec![WriteSetEntry {
                surrogate: surrogate.as_u32(),
                is_delete: false,
                value: value.to_vec(),
                collection: None,
            }];
        }
        response.write_set.extend(target_write_set);
        response
    }
}
