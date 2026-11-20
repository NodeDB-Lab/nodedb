// SPDX-License-Identifier: BUSL-1.1

//! Classify a `KvOp` into an optional `ReplicatedWrite`.
//!
//! Exhaustive over `KvOp` (not a catch-all): a new variant is a compile error
//! here, so no future KV write is silently left un-replicated.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedWrite;
use super::kv;
use super::kv::{WirePut, WireReturning, WireTarget};
use nodedb_physical::physical_plan::{KvDeclaredColumns, KvOp};
use nodedb_types::QualifiedCollection;

/// The collection `collection` with the columns the proposer resolved for it.
/// A write whose columns are still pending is refused: every replica must
/// re-type under the proposer's columns, never under its own catalog.
fn target<'a>(
    collection: &'a QualifiedCollection,
    declared_columns: &KvDeclaredColumns,
) -> crate::Result<WireTarget<'a>> {
    Ok(WireTarget {
        collection: collection.as_str(),
        declared_columns: declared_columns.shared(collection.as_str())?,
    })
}

/// Encode a `KvOp` write variant, `Ok(None)` when not a single-shard replicated
/// write, or an error when it can't replicate safely. `PredicateUpdate` /
/// `PredicateDelete` always refuse: they resolve to a mutation list first.
pub(super) fn kv_write(op: &KvOp) -> crate::Result<Option<ReplicatedWrite>> {
    Ok(Some(match op {
        KvOp::Put {
            collection,
            declared_columns,
            key,
            value,
            ttl_ms,
            surrogate,
            returning,
            rls_filters,
            provenance,
        } => kv::put(
            WirePut {
                target: target(collection, declared_columns)?,
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
            declared_columns,
            key,
            value,
            ttl_ms,
            surrogate,
            returning,
            rls_filters,
        } => kv::insert(
            WirePut {
                target: target(collection, declared_columns)?,
                key,
                value,
                ttl_ms: *ttl_ms,
                surrogate: surrogate.as_u32(),
            },
            WireReturning {
                returning,
                rls_filters,
            },
        ),
        // Whether the row is written depends on stored state, so the write
        // resolves to a mutation list first. A record of the bare op would
        // make a replica or a replay overwrite a stored row.
        KvOp::InsertIfAbsent { collection, .. } => {
            return Err(refuse_unresolved(
                collection.as_str(),
                "INSERT ... ON CONFLICT DO NOTHING",
            ));
        }
        KvOp::BatchPut {
            collection, intent, ..
        } if intent.depends_on_stored_rows() => {
            return Err(refuse_unresolved(
                collection.as_str(),
                "multi-row conditional INSERT",
            ));
        }
        KvOp::InsertOnConflictUpdate {
            collection,
            declared_columns,
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
            WirePut {
                target: target(collection, declared_columns)?,
                key,
                value,
                ttl_ms: *ttl_ms,
                surrogate: surrogate.as_u32(),
            },
            updates,
            WireReturning {
                returning,
                rls_filters,
            },
        ),
        KvOp::BatchPut {
            collection,
            declared_columns,
            entries,
            ttl_ms,
            intent,
            surrogates,
            returning,
            rls_filters,
        } => kv::batch_put(
            target(collection, declared_columns)?,
            entries,
            *ttl_ms,
            intent.clone(),
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
            declared_columns,
            key,
            delta,
            ttl_ms,
            surrogate,
            rls_write_check: _,
            shape,
        } => kv::incr(
            target(collection, declared_columns)?,
            key,
            *delta,
            *ttl_ms,
            surrogate.as_u32(),
            shape,
        ),
        // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
        KvOp::IncrFloat {
            collection,
            declared_columns,
            key,
            delta,
            surrogate,
            rls_write_check: _,
            shape,
        } => kv::incr_float(
            target(collection, declared_columns)?,
            key,
            delta,
            surrogate.as_u32(),
            shape,
        ),
        // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
        KvOp::Cas {
            collection,
            declared_columns,
            key,
            expected,
            new_value,
            surrogate,
            rls_write_check: _,
        } => kv::cas(
            target(collection, declared_columns)?,
            key,
            expected,
            new_value,
            surrogate.as_u32(),
        ),
        KvOp::GetSet {
            collection,
            declared_columns,
            key,
            new_value,
            surrogate,
            rls_filters,
            // A follower has no writing identity; decode stamps `already_decided_elsewhere()`.
            rls_write_check: _,
        } => kv::get_set(
            target(collection, declared_columns)?,
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
            declared_columns,
            key,
            updates,
            surrogate,
            if_present,
            rls_write_check: _,
            returning,
            rls_filters,
        } => kv::field_set(
            target(collection, declared_columns)?,
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
            declared_columns,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
            rls_write_check: _,
        } => kv::transfer(
            target(collection, declared_columns)?,
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
            declared_columns,
            item_key,
            dest_key,
            surrogate,
            source_rls_write_check: _,
            dest_rls_write_check: _,
        } => kv::transfer_item(
            source_collection.as_str(),
            target(dest_collection, declared_columns)?,
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

        // A row set a replica scans for itself depends on its clock (TTL
        // expiry), so a predicate write resolves to rows first.
        KvOp::PredicateUpdate { collection, .. } | KvOp::PredicateDelete { collection, .. } => {
            return Err(refuse_predicate_dml(collection.as_str()));
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

/// Refuse a KV write whose rows depend on stored state: it resolves to
/// `KvOp::ResolvedWrite` before it is proposed.
fn refuse_unresolved(collection: &str, statement: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "internal invariant break: a kv {statement} on '{collection}' reached the Raft \
             propose path unresolved; write_resolve resolves it to a mutation list first"
        ),
    }
}

/// Refuse a KV predicate UPDATE/DELETE on a governed collection: a follower has
/// no writing identity to evaluate the predicate. Must resolve to
/// `KvOp::ResolvedWrite` before proposing.
fn refuse_predicate_dml(collection: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "internal invariant break: a kv predicate UPDATE/DELETE on '{collection}' reached \
             the Raft propose path unresolved; write_resolve resolves it to a mutation list \
             first"
        ),
    }
}
