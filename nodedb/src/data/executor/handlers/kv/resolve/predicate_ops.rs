// SPDX-License-Identifier: BUSL-1.1

//! Resolvers for the KV predicate writes: `PredicateUpdate`,
//! `PredicateDelete`. Each reads via the same [`CoreLoop::kv_predicate_matches`]
//! scan the live handler uses, computes each post-image with the same merge,
//! and reports the mutations instead of applying them.

use super::context::{ResolveResult, ResolvedRewrite, delete_mutation, rewrite_mutation};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::field_compute::merge_field_updates;
use crate::data::executor::handlers::kv::predicate::KvPredicateCtx;
use crate::data::executor::handlers::kv::rls::admit_kv_row;
use crate::data::executor::handlers::returning_rows::kv_stored_rows_payload;
use crate::data::executor::response_codec;
use crate::engine::kv::current_ms;
use nodedb_physical::physical_plan::KvResolveOutcome;

impl CoreLoop {
    /// Resolve a predicate `UPDATE`. Each matched row's stored body becomes
    /// its mutation's `precondition`, so a moved-past resolution applies nothing.
    /// A `RETURNING` projects the post-images the mutations carry.
    pub(super) fn resolve_kv_predicate_update(
        &self,
        ctx: KvPredicateCtx<'_>,
        updates: &[(String, Vec<u8>)],
    ) -> ResolveResult {
        let KvPredicateCtx {
            did,
            tid,
            collection,
            filters,
            rls_write_check,
            returning,
            rls_filters,
        } = ctx;
        let now_ms = current_ms();
        let matched = self.kv_predicate_matches(did, tid, collection, filters, now_ms)?;

        let declared = self.declared_columns_of(did, tid, collection);
        let mut writes: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::with_capacity(matched.len());
        for (key, body) in matched {
            let computed =
                merge_field_updates(collection, Some(body.as_slice()), updates, declared)?;
            admit_kv_row(
                rls_write_check,
                &computed.new_value,
                &key,
                tid,
                collection,
                &self.crs_context(),
            )?;
            writes.push((key, body, computed.new_value));
        }

        let response_payload = match returning {
            Some(spec) => {
                let rows: Vec<(&[u8], &[u8])> = writes
                    .iter()
                    .map(|(key, _old_body, new_value)| (key.as_slice(), new_value.as_slice()))
                    .collect();
                kv_stored_rows_payload(spec, rls_filters, &rows, &self.crs_context())?
            }
            None => response_codec::encode_count("affected", writes.len())?,
        };
        let mutations = writes
            .into_iter()
            .map(|(key, body, new_value)| {
                // The row exists and keeps its bound surrogate through the merge.
                rewrite_mutation(ResolvedRewrite {
                    collection,
                    key: &key,
                    value: new_value,
                    // The keyed field merge writes with `ttl_ms: 0`, so the
                    // resolved rewrite does too.
                    ttl_ms: 0,
                    expire_at_ms: 0,
                    precondition: body,
                })
            })
            .collect();
        Ok(KvResolveOutcome {
            mutations,
            response_payload,
        })
    }

    /// Resolve a predicate `DELETE`. Counts and replies exactly as
    /// `resolve_kv_delete` does for a keyed one, pre-image `RETURNING` included.
    pub(super) fn resolve_kv_predicate_delete(&self, ctx: KvPredicateCtx<'_>) -> ResolveResult {
        let KvPredicateCtx {
            did,
            tid,
            collection,
            filters,
            rls_write_check,
            returning,
            rls_filters,
        } = ctx;
        let now_ms = current_ms();
        let matched = self.kv_predicate_matches(did, tid, collection, filters, now_ms)?;

        for (key, body) in &matched {
            admit_kv_row(
                rls_write_check,
                body,
                key,
                tid,
                collection,
                &self.crs_context(),
            )?;
        }

        let response_payload = match returning {
            Some(spec) => {
                let rows: Vec<(&[u8], &[u8])> = matched
                    .iter()
                    .map(|(key, body)| (key.as_slice(), body.as_slice()))
                    .collect();
                kv_stored_rows_payload(spec, rls_filters, &rows, &self.crs_context())?
            }
            None => response_codec::encode_count("deleted", matched.len())?,
        };
        let mutations = matched
            .into_iter()
            .map(|(key, body)| delete_mutation(collection, &key, Some(body)))
            .collect();
        Ok(KvResolveOutcome {
            mutations,
            response_payload,
        })
    }
}
