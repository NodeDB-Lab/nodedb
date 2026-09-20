// SPDX-License-Identifier: BUSL-1.1

//! Resolvers for the KV writes whose image comes from a merge or the stored
//! row itself: `InsertOnConflictUpdate`, `Delete`, `Expire`, `Persist`,
//! `FieldSet`. Each reads what its live handler reads and computes the
//! post-image via the same function — re-deriving it here is exactly the
//! drift this protocol exists to prevent.

use nodedb_physical::physical_plan::{KvResolveOutcome, KvResolvedMutation};

use super::context::{
    ResolveResult, ResolvedPut, delete_mutation, expiry_from_ttl, one, put_mutation,
};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::atomic::KvAtomicCtx;
use crate::data::executor::handlers::kv::conflict_merge::merge_kv_conflict_body;
use crate::data::executor::handlers::kv::crud::{KvDeleteParams, KvInsertOnConflictUpdateParams};
use crate::data::executor::handlers::kv::field::KvFieldSetArgs;
use crate::data::executor::handlers::kv::rls::admit_kv_row;
use crate::data::executor::handlers::kv::ttl::KvTtlTarget;
use crate::data::executor::handlers::returning_rows::kv_stored_rows_payload;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    /// Resolve `INSERT ... ON CONFLICT (key) DO UPDATE SET ...`. Mirrors
    /// `execute_kv_insert_on_conflict_update`: the gate decides whichever body
    /// would actually persist — incoming row if absent, merge if present.
    pub(super) fn resolve_kv_insert_on_conflict_update(
        &self,
        params: KvInsertOnConflictUpdateParams<'_>,
        task: &ExecutionTask,
    ) -> ResolveResult {
        let KvInsertOnConflictUpdateParams {
            did,
            tid,
            collection,
            key,
            value,
            ttl_ms,
            updates,
            surrogate,
            rls_write_check,
            returning,
            rls_filters,
        } = params;

        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::Internal {
                detail: "KV memory budget exceeded, retry later".into(),
            });
        }

        let now_ms = self.kv_ttl_now_ms(task);
        let existing_bytes = self.kv_resolve_read(did, tid, collection, key, now_ms);

        let stored_bytes: Vec<u8> = match &existing_bytes {
            None => value.to_vec(),
            Some(existing_raw) => merge_kv_conflict_body(existing_raw, value, updates)?,
        };

        admit_kv_row(rls_write_check, &stored_bytes, key, tid, collection)?;

        let response_payload = match returning {
            Some(spec) => kv_stored_rows_payload(spec, rls_filters, &[(key, &stored_bytes)])?,
            // Same `{affected, op}` shape `execute_kv_insert_on_conflict_update`
            // reports, so the tag renders identically on both paths.
            None => response_codec::encode_affected_with_op(
                1,
                if existing_bytes.is_some() {
                    "update"
                } else {
                    "insert"
                },
            ),
        };

        Ok(one(
            put_mutation(ResolvedPut {
                collection,
                key,
                value: stored_bytes,
                ttl_ms,
                expire_at_ms: expiry_from_ttl(ttl_ms, now_ms),
                surrogate,
                precondition: existing_bytes,
            }),
            response_payload,
        ))
    }

    /// Resolve a KV `DELETE`. An absent key contributes no mutation and is
    /// counted as not-deleted, same as `execute_kv_delete`. A `RETURNING`
    /// projects the pre-images the mutations carry, same as the live handler.
    pub(super) fn resolve_kv_delete(&self, params: KvDeleteParams<'_>) -> ResolveResult {
        let KvDeleteParams {
            did,
            tid,
            collection,
            keys,
            rls_write_check,
            returning,
            rls_filters,
        } = params;
        let now_ms = current_ms();
        let mut pre_images: Vec<(&[u8], Vec<u8>)> = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(body) = self.kv_resolve_read(did, tid, collection, key, now_ms) else {
                continue;
            };
            admit_kv_row(rls_write_check, &body, key, tid, collection)?;
            pre_images.push((key.as_slice(), body));
        }

        let response_payload = match returning {
            Some(spec) => {
                let rows: Vec<(&[u8], &[u8])> = pre_images
                    .iter()
                    .map(|(key, body)| (*key, body.as_slice()))
                    .collect();
                kv_stored_rows_payload(spec, rls_filters, &rows)?
            }
            None => response_codec::encode_count("deleted", pre_images.len())?,
        };
        let mutations = pre_images
            .into_iter()
            .map(|(key, body)| delete_mutation(collection, key, Some(body)))
            .collect();
        Ok(KvResolveOutcome {
            mutations,
            response_payload,
        })
    }

    /// Resolve `EXPIRE`. The body doesn't change, so the stored row is both
    /// pre- and post-image; an absent key ships `precondition: None` and the
    /// apply reports `NotFound` if still absent.
    pub(super) fn resolve_kv_expire(
        &self,
        target: KvTtlTarget<'_>,
        ttl_ms: u64,
        task: &ExecutionTask,
    ) -> ResolveResult {
        let now_ms = self.kv_ttl_now_ms(task);
        let precondition = self.resolve_kv_ttl_precondition(&target, now_ms)?;
        Ok(one(
            KvResolvedMutation::Expire {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    target.collection.to_owned(),
                ),
                key: target.key.to_vec(),
                ttl_ms,
                resolved_now_ms: now_ms,
                precondition,
            },
            Vec::new(),
        ))
    }

    /// Resolve `PERSIST`. See [`CoreLoop::resolve_kv_expire`] — same image,
    /// and the same clock `execute_kv_persist` reads for its policy check.
    pub(super) fn resolve_kv_persist(&self, target: KvTtlTarget<'_>) -> ResolveResult {
        let precondition = self.resolve_kv_ttl_precondition(&target, current_ms())?;
        Ok(one(
            KvResolvedMutation::Persist {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    target.collection.to_owned(),
                ),
                key: target.key.to_vec(),
                precondition,
            },
            Vec::new(),
        ))
    }

    /// Read a TTL mutation's target row and decide it against the policy.
    /// Unlike `admit_kv_ttl_target`, the row is read even when the policy
    /// admits everything — it pins the drift precondition, not just the gate.
    fn resolve_kv_ttl_precondition(
        &self,
        target: &KvTtlTarget<'_>,
        now_ms: u64,
    ) -> Result<Option<Vec<u8>>, ErrorCode> {
        let KvTtlTarget {
            did,
            tid,
            collection,
            key,
            rls_write_check,
        } = *target;
        let body = self.kv_resolve_read(did, tid, collection, key, now_ms);
        if let Some(bytes) = &body {
            admit_kv_row(rls_write_check, bytes, key, tid, collection)?;
        }
        Ok(body)
    }

    /// Resolve `FieldSet` (HSET-style field merge), via the same
    /// `field_compute::merge_field_updates` `execute_kv_field_set` calls.
    pub(super) fn resolve_kv_field_set(
        &self,
        ctx: KvAtomicCtx<'_>,
        args: KvFieldSetArgs<'_>,
    ) -> ResolveResult {
        let KvAtomicCtx {
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
            ..
        } = ctx;
        let KvFieldSetArgs {
            updates,
            if_present,
            returning,
            rls_filters,
        } = args;
        let now_ms = current_ms();
        let current = self.kv_resolve_read(did, tid, collection, key, now_ms);

        // SQL UPDATE against an absent key is `UPDATE 0`, not a create —
        // mirrors `execute_kv_field_set`'s decision.
        if if_present && current.is_none() {
            let response_payload = match returning {
                Some(spec) => kv_stored_rows_payload(spec, rls_filters, &[])?,
                None => response_codec::encode_json_as_msgpack(
                    &serde_json::json!({ "affected": 0, "fields_added": 0 }),
                )?,
            };
            return Ok(KvResolveOutcome {
                mutations: Vec::new(),
                response_payload,
            });
        }

        let computed = crate::data::executor::handlers::kv::field_compute::merge_field_updates(
            collection,
            current.as_deref(),
            updates,
        )?;
        admit_kv_row(rls_write_check, &computed.new_value, key, tid, collection)?;

        let response_payload = match returning {
            Some(spec) => kv_stored_rows_payload(spec, rls_filters, &[(key, &computed.new_value)])?,
            // Same shape `execute_kv_field_set` reports.
            None => response_codec::encode_json_as_msgpack(
                &serde_json::json!({ "affected": 1, "fields_added": computed.fields_added }),
            )?,
        };
        Ok(one(
            put_mutation(ResolvedPut {
                collection,
                key,
                value: computed.new_value,
                // `execute_kv_field_set` puts with `ttl_ms: 0`, which clears
                // any TTL the key held. Preserved verbatim here.
                ttl_ms: 0,
                expire_at_ms: 0,
                surrogate,
                precondition: current,
            }),
            response_payload,
        ))
    }
}
