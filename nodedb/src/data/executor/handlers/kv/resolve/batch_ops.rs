// SPDX-License-Identifier: BUSL-1.1

//! Resolvers for the conditional KV inserts: `INSERT ... ON CONFLICT DO
//! NOTHING` (point and batch) and a multi-row `INSERT ... ON CONFLICT (key)
//! DO UPDATE`. Each judges its rows through the same `plan_kv_batch` the live
//! handler and transaction staging call, and ships one mutation per key it
//! writes. A skipped row ships no mutation, so no replica and no WAL replay
//! stores it.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{
    KvBatchIntent, KvResolveOutcome, KvResolvedMutation, ReturningSpec,
};
use nodedb_types::Surrogate;

use super::context::{ResolveResult, ResolvedPut, expiry_from_ttl, put_mutation};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::batch_plan::{merge_batch_verb, plan_kv_batch};
use crate::data::executor::handlers::kv::batch_put::stored_rows;
use crate::data::executor::handlers::kv::rls::admit_kv_row;
use crate::data::executor::handlers::returning_rows::kv_stored_rows_payload;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

/// A `KvOp::BatchPut` to resolve.
pub(super) struct ResolveKvBatch<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub entries: &'a [(Vec<u8>, Vec<u8>)],
    pub ttl_ms: u64,
    pub intent: &'a KvBatchIntent,
    pub surrogates: &'a [Surrogate],
    pub returning: Option<&'a ReturningSpec>,
    pub rls_filters: &'a [u8],
}

/// A `KvOp::InsertIfAbsent` to resolve.
pub(super) struct ResolveKvPoint<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl_ms: u64,
    pub surrogate: Surrogate,
    pub returning: Option<&'a ReturningSpec>,
    pub rls_filters: &'a [u8],
}

impl CoreLoop {
    /// Resolve a point `INSERT ... ON CONFLICT DO NOTHING`: one `Put`
    /// mutation that requires the key absent, or none when it is stored.
    pub(super) fn resolve_kv_insert_if_absent(
        &self,
        task: &ExecutionTask,
        point: ResolveKvPoint<'_>,
    ) -> ResolveResult {
        let ResolveKvPoint {
            did,
            tid,
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            returning,
            rls_filters,
        } = point;
        let entries = [(key.to_vec(), value.to_vec())];
        self.resolve_kv_batch_put(
            task,
            ResolveKvBatch {
                did,
                tid,
                collection,
                entries: &entries,
                ttl_ms,
                intent: &KvBatchIntent::InsertIfAbsent,
                surrogates: std::slice::from_ref(&surrogate),
                returning,
                rls_filters,
            },
        )
    }

    /// Resolve a conditional `KvOp::BatchPut`. Each key the batch writes
    /// ships one `Put`: the body the key's last row stores, with the stored
    /// body the batch read as its precondition. The reply is the one the
    /// live handler gives for the same rows.
    pub(super) fn resolve_kv_batch_put(
        &self,
        task: &ExecutionTask,
        batch: ResolveKvBatch<'_>,
    ) -> ResolveResult {
        let ResolveKvBatch {
            did,
            tid,
            collection,
            entries,
            ttl_ms,
            intent,
            surrogates,
            returning,
            rls_filters,
        } = batch;

        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::Internal {
                detail: "KV memory budget exceeded, retry later".into(),
            });
        }

        let now_ms = self.kv_ttl_now_ms(task);
        // The stored body each key had before the batch: the precondition
        // its mutation carries.
        let mut stored_before: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        let rows = plan_kv_batch(
            collection,
            entries,
            intent,
            self.declared_columns_of(did, tid, collection),
            &self.crs_context(),
            |key| {
                let body = self.kv_resolve_read(did, tid, collection, key, now_ms);
                stored_before.insert(key.to_vec(), body.clone());
                body
            },
        )?;
        if let KvBatchIntent::Merge {
            rls_write_check, ..
        } = intent
        {
            for row in &rows {
                admit_kv_row(
                    rls_write_check,
                    &row.stored,
                    row.key,
                    tid,
                    collection,
                    &self.crs_context(),
                )?;
            }
        }

        let response_payload = match returning {
            Some(spec) => {
                kv_stored_rows_payload(spec, rls_filters, &stored_rows(&rows), &self.crs_context())?
            }
            None => match intent {
                KvBatchIntent::Merge { .. } => response_codec::encode_affected_with_op(
                    rows.len() as u64,
                    merge_batch_verb(&rows),
                ),
                KvBatchIntent::Put => response_codec::encode_count(
                    "inserted",
                    rows.iter().filter(|row| !row.updated()).count(),
                )?,
                KvBatchIntent::Upsert | KvBatchIntent::Insert | KvBatchIntent::InsertIfAbsent => {
                    response_codec::encode_affected(rows.len() as u64)
                }
            },
        };

        // One mutation per key. A key the batch writes twice stores its last
        // row's body, and its precondition is the body stored before the
        // batch: the apply checks every precondition before it applies any.
        let expire_at_ms = expiry_from_ttl(ttl_ms, now_ms);
        let mut slot_of: HashMap<&[u8], usize> = HashMap::with_capacity(rows.len());
        let mut mutations: Vec<KvResolvedMutation> = Vec::with_capacity(rows.len());
        for row in &rows {
            let surrogate = surrogates.get(row.index).copied().ok_or_else(|| {
                ErrorCode::RejectedPrevalidation {
                    reason: format!(
                        "kv batch write into '{collection}' carries {} surrogates for {} \
                         entries; every row must be bound before it is stored",
                        surrogates.len(),
                        entries.len()
                    ),
                }
            })?;
            let precondition = stored_before.get(row.key).cloned().flatten();
            let mutation = put_mutation(ResolvedPut {
                collection,
                key: row.key,
                value: row.stored.to_vec(),
                ttl_ms,
                expire_at_ms,
                surrogate,
                precondition,
            });
            match slot_of.get(row.key) {
                Some(&slot) => {
                    if let Some(earlier) = mutations.get_mut(slot) {
                        *earlier = mutation;
                    }
                }
                None => {
                    slot_of.insert(row.key, mutations.len());
                    mutations.push(mutation);
                }
            }
        }
        Ok(KvResolveOutcome {
            mutations,
            response_payload,
        })
    }
}
