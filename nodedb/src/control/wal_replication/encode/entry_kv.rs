// SPDX-License-Identifier: BUSL-1.1

//! Classify a `KvOp` into an optional `ReplicatedWrite`.
//!
//! Exhaustive over `KvOp` (not a catch-all): a new variant is a compile error
//! here, so no future KV write is silently left un-replicated.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedWrite;
use super::kv;
use super::kv::{WirePut, WireReturning};
use nodedb_physical::physical_plan::KvOp;

/// Encode a `KvOp` write variant, `Ok(None)` when not a single-shard replicated
/// write, or an error when it can't replicate safely. `PredicateUpdate`/
/// `PredicateDelete` refuse on `rls_write_check.has_predicate()`.
pub(super) fn kv_write(op: &KvOp) -> crate::Result<Option<ReplicatedWrite>> {
    Ok(Some(match op {
        KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            returning,
            rls_filters,
            provenance,
        } => kv::put(
            WirePut {
                collection: collection.as_str(),
                key,
                value,
                ttl_ms: *ttl_ms,
                surrogate: surrogate.as_u32(),
            },
            WireReturning {
                returning,
                rls_filters,
            },
            provenance,
        ),
        // The compiled RLS predicate is absent from the durable record, so a
        // replay re-applies the already-admitted write, not re-deciding it.
        KvOp::Delete {
            collection,
            keys,
            // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
            rls_write_check: _,
            returning,
            rls_filters,
            provenance,
        } => kv::delete(
            collection.as_str(),
            keys,
            WireReturning {
                returning,
                rls_filters,
            },
            provenance,
        ),
        KvOp::Insert {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            returning,
            rls_filters,
        } => kv::insert(
            collection.as_str(),
            key,
            value,
            *ttl_ms,
            surrogate.as_u32(),
            WireReturning {
                returning,
                rls_filters,
            },
        ),
        KvOp::InsertIfAbsent {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            returning,
            rls_filters,
        } => kv::insert_if_absent(
            collection.as_str(),
            key,
            value,
            *ttl_ms,
            surrogate.as_u32(),
            WireReturning {
                returning,
                rls_filters,
            },
        ),
        KvOp::InsertOnConflictUpdate {
            collection,
            key,
            value,
            ttl_ms,
            updates,
            surrogate,
            // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
            rls_write_check: _,
            returning,
            rls_filters,
        } => kv::insert_on_conflict_update(
            collection.as_str(),
            key,
            value,
            *ttl_ms,
            updates,
            surrogate.as_u32(),
            WireReturning {
                returning,
                rls_filters,
            },
        ),
        KvOp::BatchPut {
            collection,
            entries,
            ttl_ms,
            surrogates,
            returning,
            rls_filters,
        } => kv::batch_put(
            collection.as_str(),
            entries,
            *ttl_ms,
            surrogates,
            WireReturning {
                returning,
                rls_filters,
            },
        ),
        // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
        KvOp::Expire {
            collection,
            key,
            ttl_ms,
            rls_write_check: _,
        } => kv::expire(collection.as_str(), key, *ttl_ms),
        // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
        KvOp::Persist {
            collection,
            key,
            rls_write_check: _,
        } => kv::persist(collection.as_str(), key),
        // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
        KvOp::Incr {
            collection,
            key,
            delta,
            ttl_ms,
            surrogate,
            rls_write_check: _,
            shape,
        } => kv::incr(
            collection.as_str(),
            key,
            *delta,
            *ttl_ms,
            surrogate.as_u32(),
            shape,
        ),
        // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
        KvOp::IncrFloat {
            collection,
            key,
            delta,
            surrogate,
            rls_write_check: _,
            shape,
        } => kv::incr_float(collection.as_str(), key, delta, surrogate.as_u32(), shape),
        // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
        KvOp::Cas {
            collection,
            key,
            expected,
            new_value,
            surrogate,
            rls_write_check: _,
        } => kv::cas(
            collection.as_str(),
            key,
            expected,
            new_value,
            surrogate.as_u32(),
        ),
        KvOp::GetSet {
            collection,
            key,
            new_value,
            surrogate,
            rls_filters,
            // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
            rls_write_check: _,
        } => kv::get_set(
            collection.as_str(),
            key,
            new_value,
            surrogate.as_u32(),
            rls_filters,
        ),
        KvOp::RegisterSortedIndex {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        } => kv::register_sorted_index(kv::RegisterSortedIndexFields {
            collection: collection.as_str(),
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms: *window_start_ms,
            window_end_ms: *window_end_ms,
        }),
        KvOp::DropSortedIndex { index_name } => kv::drop_sorted_index(index_name),
        KvOp::RegisterIndex {
            collection,
            field,
            field_position,
            backfill,
        } => kv::register_index(collection.as_str(), field, *field_position, *backfill),
        KvOp::DropIndex { collection, field } => kv::drop_index(collection.as_str(), field),
        // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
        KvOp::FieldSet {
            collection,
            key,
            updates,
            surrogate,
            if_present,
            rls_write_check: _,
            returning,
            rls_filters,
        } => kv::field_set(
            collection.as_str(),
            key,
            updates,
            surrogate.as_u32(),
            *if_present,
            WireReturning {
                returning,
                rls_filters,
            },
        ),
        // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
        KvOp::Transfer {
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
            rls_write_check: _,
        } => kv::transfer(
            collection.as_str(),
            source_key,
            dest_key,
            field,
            *amount,
            debit_surrogate.as_u32(),
            credit_surrogate.as_u32(),
        ),
        // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
            source_rls_write_check: _,
            dest_rls_write_check: _,
        } => kv::transfer_item(
            source_collection.as_str(),
            dest_collection.as_str(),
            item_key,
            dest_key,
            surrogate.as_u32(),
        ),

        KvOp::Truncate {
            collection,
            restart_identity,
        } => kv::truncate(collection.as_str(), *restart_identity),

        // Verdict is already on the plan (`RlsWriteCheck::DecidedEarlierInRequest`),
        // so no predicate to drop here.
        KvOp::ResolvedWrite {
            mutations,
            response_payload,
            // Decode stamps `decided_earlier_in_request()`.
            rls_write_check: _,
        } => kv::resolved_write(mutations, response_payload),

        // With no write policy, each replica re-scans the predicate — deterministic
        // replay. A restricting policy refuses; see `refuse_governed_predicate_dml`.
        KvOp::PredicateUpdate {
            collection,
            filters,
            updates,
            rls_write_check,
            returning,
            rls_filters,
        } => {
            refuse_governed_predicate_dml(collection.as_str(), rls_write_check)?;
            kv::predicate_update(
                collection.as_str(),
                filters,
                updates,
                WireReturning {
                    returning,
                    rls_filters,
                },
            )
        }
        KvOp::PredicateDelete {
            collection,
            filters,
            rls_write_check,
            returning,
            rls_filters,
        } => {
            refuse_governed_predicate_dml(collection.as_str(), rls_write_check)?;
            kv::predicate_delete(
                collection.as_str(),
                filters,
                WireReturning {
                    returning,
                    rls_filters,
                },
            )
        }

        // Not a write — reads/scans/sorted-index queries. `ResolveWrite` mutates
        // nothing, so nothing replicates.
        KvOp::ResolveWrite(_)
        | KvOp::Get { .. }
        | KvOp::Scan { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::FieldGet { .. }
        | KvOp::MaterializeScan { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::SortedIndexTxnRead { .. } => return Ok(None),
    }))
}

/// Refuse a KV predicate UPDATE/DELETE on a governed collection: a follower has
/// no writing identity to evaluate the predicate. Must resolve to
/// `KvOp::ResolvedWrite` before proposing.
fn refuse_governed_predicate_dml(
    collection: &str,
    rls_write_check: &nodedb_types::RlsWriteCheck,
) -> crate::Result<()> {
    if rls_write_check.has_predicate() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "kv predicate UPDATE/DELETE on '{collection}' cannot be replicated as a \
                 predicate because it carries an RLS write policy: a follower has no writing \
                 identity to evaluate the predicate against. It must be resolved to a concrete \
                 mutation list before it is proposed."
            ),
        });
    }
    Ok(())
}
