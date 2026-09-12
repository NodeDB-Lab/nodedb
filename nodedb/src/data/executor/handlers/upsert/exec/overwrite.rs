// SPDX-License-Identifier: BUSL-1.1

//! The upsert overwrite branch: an existing row was found, so merge the
//! incoming value into it (or apply `ON CONFLICT DO UPDATE SET`), persist,
//! and respond.

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::handlers::upsert::merge::{apply_on_conflict_updates, merge_values};
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
        let row_identity = storage_key.to_identity();
        let document_identity = RowIdentity::from_user_key(document_id);

        // Decode existing document to nodedb_types::Value.
        let existing_val = if let Some(schema) = strict_schema {
            // Strict: binary tuple → Value via schema.
            match crate::data::executor::strict_format::binary_tuple_to_value(
                &current_bytes,
                schema,
            ) {
                Some(v) => v,
                None => {
                    // Fallback: try msgpack (migration case).
                    match nodedb_types::value_from_msgpack(&current_bytes) {
                        Ok(v) => v,
                        Err(_) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: "failed to decode document for upsert".into(),
                                },
                            );
                        }
                    }
                }
            }
        } else {
            // Schemaless: stored as msgpack.
            match nodedb_types::value_from_msgpack(&current_bytes) {
                Ok(v) => v,
                Err(_) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "failed to decode document for upsert".into(),
                        },
                    );
                }
            }
        };

        // Decode incoming value (msgpack → Value).
        let new_val = match nodedb_types::value_from_msgpack(value) {
            Ok(v) => v,
            Err(_) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "failed to decode upsert value from msgpack".into(),
                    },
                );
            }
        };

        // Conflict branch: if `ON CONFLICT DO UPDATE SET` assignments
        // are present, evaluate each against the *existing* row and
        // apply only those fields. Otherwise fall back to the plain
        // merge semantics used by `UPSERT INTO` / no-action upserts.
        let merged = if on_conflict_updates.is_empty() {
            merge_values(existing_val, new_val)
        } else {
            match apply_on_conflict_updates(existing_val, &new_val, on_conflict_updates) {
                Ok(v) => v,
                Err(e) => return self.response_error(task, e),
            }
        };

        // The merged row as a MessagePack body — the form
        // `apply_point_put` takes for BOTH storage modes; it encodes the
        // strict Binary Tuple, and stamps a bitemporal version, itself.
        // Encoding storage bytes here as well would leave this handler
        // and the write path each deciding the row's on-disk shape.
        let merged_body = match nodedb_types::value_to_msgpack(&merged) {
            Ok(b) => b,
            Err(_) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "failed to encode merged upsert value".into(),
                    },
                );
            }
        };

        // Gate the persist on the collection's write policy, decided
        // against the MERGED row — the row that will exist afterwards.
        // The insert body alone would clear a write whose actual
        // post-image the policy never saw, which is why this branch
        // cannot be admitted at plan time. Decided on the MessagePack
        // form, exactly as the insert arm below decides its own body.
        if let Err(e) = rls_write_gate::admit_stored_row(
            rls_write_check,
            &merged_body,
            &row_identity,
            None,
            tid,
            collection,
        ) {
            return self.response_error(task, e);
        }

        // The surrogate is stable across an overwrite and
        // `insert_with_surrogate` APPENDS an HNSW node rather than
        // replacing one, so the prior embedding has to come out before
        // the write below puts the new one in — otherwise KNN keeps
        // scoring both. No-op when `has_vectors` is false.
        if has_vectors {
            self.remove_document_vector_indexes(database_id, tid, collection, storage_key);
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
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let outcome = match self.apply_point_put(
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
                wal_lsn: task.wal_lsn(),
                resolved_targets: hook_ctx.resolved_targets,
            },
        ) {
            Ok(o) => o,
            Err(e) => {
                // Some rejections land after the row was cached;
                // dropping `txn` reverses the durable write but not
                // that entry, which would then serve a body that never
                // committed.
                self.doc_cache
                    .invalidate(database_id, tid, collection, &storage_key);
                return self.response_error(task, e);
            }
        };

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
                self.doc_cache
                    .invalidate(database_id, tid, collection, &storage_key);
                return self.response_error(task, e);
            }
        };
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);

        // Settled before the commit: the merged row's old amount comes
        // off the group and its new one goes on, so an overwrite that
        // leaves the group unbalanced is refused with nothing written.
        if let Err(e) =
            self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)
        {
            self.doc_cache
                .invalidate(database_id, tid, collection, &storage_key);
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

        // `current_bytes` is the pre-merge stored row, already read
        // above — thread it to the Event Plane as `old_value` so the
        // emitted WriteOp resolves to Update.
        let stored_bytes = outcome.stored_value;
        self.emit_put_event(
            task,
            tid,
            collection,
            row_identity,
            &stored_bytes,
            Some(&current_bytes),
        );

        // Carry the surrogate + post-image back so the Control Plane
        // can mint a post-apply `Put` redo. The autocommit WAL path
        // mints none for an Upsert overwrite, so without this a WAL-only
        // restart rebuilds the HNSW from the pre-upsert body and
        // resurrects the old embedding.
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
                &[(&document_identity, stored_bytes.as_slice())],
            ),
            None => self.response_affected(task, 1),
        };
        if has_vectors {
            response.write_set = vec![WriteSetEntry {
                surrogate: surrogate.as_u32(),
                is_delete: false,
                value: merged_body,
                collection: None,
            }];
        }
        // Derived target rows live in a different collection, so each
        // carries its own `Some(collection)` and homes to that
        // collection's vShard.
        response.write_set.extend(target_write_set);
        response
    }
}
