// SPDX-License-Identifier: BUSL-1.1

//! Dispatch of `KvOp` variants to WAL append calls.

use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalManager;
use nodedb_physical::physical_plan::KvOp;

use super::encode::{
    KvRegisterSortedIndexFields, KvTransferFields, encode_kv_batch_put, encode_kv_cas,
    encode_kv_delete, encode_kv_drop_index, encode_kv_drop_sorted_index, encode_kv_expire,
    encode_kv_field_set, encode_kv_getset, encode_kv_incr, encode_kv_incr_float,
    encode_kv_insert_on_conflict_update, encode_kv_persist, encode_kv_predicate_delete,
    encode_kv_predicate_update, encode_kv_put, encode_kv_register_index,
    encode_kv_register_sorted_index, encode_kv_transfer, encode_kv_transfer_item,
    encode_kv_truncate,
};

/// Outcome of [`wal_append_kv_op`]: the allocated WAL LSN and, for a TTL-bearing
/// write, the wall-clock instant resolved. `resolved_now_ms` must be the one
/// value the WAL record and the live apply both use — resolving independently risks a crash shifting a TTL's expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KvAppendOutcome {
    /// WAL LSN allocated for this write, or `None` for read-only / non-WAL ops.
    pub lsn: Option<crate::types::Lsn>,
    /// The wall-clock instant (ms since epoch) resolved for a TTL-bearing
    /// write's `expire_at_ms`. `None` for non-TTL writes and for ops that
    /// carry no TTL at all.
    pub resolved_now_ms: Option<u64>,
}

/// Resolve `now_ms` and the absolute expiry for a TTL-bearing write, once.
/// Both `None` when `ttl_ms == 0`. `now_override` supplies the instant when
/// decided elsewhere — see [`wal_append_kv_op`].
fn resolve_expiry(ttl_ms: u64, now_override: Option<u64>) -> (Option<u64>, Option<u64>) {
    if ttl_ms == 0 {
        (None, None)
    } else {
        let now_ms = now_override.unwrap_or_else(crate::engine::kv::current_ms);
        (Some(now_ms), Some(now_ms + ttl_ms))
    }
}

/// Serialize a KV operation and append to the WAL — see [`KvAppendOutcome`].
/// `now_override` pins `expire_at_ms` to an instant decided elsewhere (e.g. a
/// Raft-committed entry), so every replica's redo installs it verbatim.
pub fn wal_append_kv_op(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &KvOp,
    now_override: Option<u64>,
) -> crate::Result<KvAppendOutcome> {
    let mut resolved_now_ms: Option<u64> = None;
    let lsn: Option<crate::types::Lsn> = match op {
        KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            ..
        }
        | KvOp::Insert {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            ..
        }
        | KvOp::InsertIfAbsent {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            ..
        } => {
            let (now_ms, expire_at_ms) = resolve_expiry(*ttl_ms, now_override);
            resolved_now_ms = now_ms;
            let entry = encode_kv_put(
                collection.as_str(),
                key,
                value,
                *ttl_ms,
                expire_at_ms,
                surrogate.as_u32(),
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::InsertOnConflictUpdate {
            collection,
            key,
            value,
            ttl_ms,
            updates,
            surrogate: _,
            // Compiled RLS predicate is a session property, not the row's — stays out.
            rls_write_check: _,
            // Projection is answered from the response, not the journal — stays out.
            returning: _,
            rls_filters: _,
        } => {
            let (now_ms, expire_at_ms) = resolve_expiry(*ttl_ms, now_override);
            resolved_now_ms = now_ms;
            let entry = encode_kv_insert_on_conflict_update(
                collection.as_str(),
                key,
                value,
                *ttl_ms,
                updates,
                expire_at_ms,
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Delete {
            collection, keys, ..
        } => {
            let entry = encode_kv_delete(collection.as_str(), keys)?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::BatchPut {
            collection,
            entries,
            ttl_ms,
            surrogates,
            ..
        } => {
            let (now_ms, expire_at_ms) = resolve_expiry(*ttl_ms, now_override);
            resolved_now_ms = now_ms;
            let raw: Vec<u32> = surrogates.iter().map(|s| s.as_u32()).collect();
            let entry =
                encode_kv_batch_put(collection.as_str(), entries, *ttl_ms, expire_at_ms, &raw)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Expire {
            collection,
            key,
            ttl_ms,
            ..
        } => {
            // `Expire` has no "no TTL" sentinel for `ttl_ms == 0`, so it deliberately
            // skips `resolve_expiry` (which returns `None` for the Put family).
            let now_ms = now_override.unwrap_or_else(crate::engine::kv::current_ms);
            let expire_at_ms = now_ms + *ttl_ms;
            resolved_now_ms = Some(now_ms);
            let entry = encode_kv_expire(collection.as_str(), key, *ttl_ms, expire_at_ms)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Persist {
            collection, key, ..
        } => {
            let entry = encode_kv_persist(collection.as_str(), key)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::RegisterIndex {
            collection,
            field,
            field_position,
            backfill,
        } => {
            let entry =
                encode_kv_register_index(collection.as_str(), field, *field_position, *backfill)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::DropIndex { collection, field } => {
            let entry = encode_kv_drop_index(collection.as_str(), field)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::FieldSet {
            collection,
            key,
            updates,
            surrogate,
            ..
        } => {
            let entry = encode_kv_field_set(collection.as_str(), key, updates, surrogate.as_u32())?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Incr {
            collection,
            key,
            delta,
            ttl_ms,
            surrogate,
            ..
        } => {
            let (now_ms, expire_at_ms) = resolve_expiry(*ttl_ms, now_override);
            resolved_now_ms = now_ms;
            let entry = encode_kv_incr(
                collection.as_str(),
                key,
                *delta,
                *ttl_ms,
                surrogate.as_u32(),
                expire_at_ms,
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::IncrFloat {
            collection,
            key,
            delta,
            surrogate,
            ..
        } => {
            let entry = encode_kv_incr_float(collection.as_str(), key, *delta, surrogate.as_u32())?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Cas {
            collection,
            key,
            expected,
            new_value,
            surrogate,
            ..
        } => {
            let entry = encode_kv_cas(
                collection.as_str(),
                key,
                expected,
                new_value,
                surrogate.as_u32(),
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::GetSet {
            collection,
            key,
            new_value,
            surrogate,
            ..
        } => {
            let entry = encode_kv_getset(collection.as_str(), key, new_value, surrogate.as_u32())?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::RegisterSortedIndex {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        } => {
            let entry = encode_kv_register_sorted_index(KvRegisterSortedIndexFields {
                collection: collection.as_str(),
                index_name,
                sort_columns,
                key_column,
                window_type,
                window_timestamp_column,
                window_start_ms: *window_start_ms,
                window_end_ms: *window_end_ms,
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::DropSortedIndex { index_name } => {
            let entry = encode_kv_drop_sorted_index(index_name)?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Truncate { collection } => {
            let entry = encode_kv_truncate(collection.as_str())?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        // The predicate is the durable record; replay re-executes it via the live handler.
        KvOp::PredicateUpdate {
            collection,
            filters,
            updates,
            // Per-request authorization input, not part of the durable image.
            rls_write_check: _,
            // Per-request reply projection, not part of the durable image.
            returning: _,
            rls_filters: _,
        } => {
            let entry = encode_kv_predicate_update(collection.as_str(), filters, updates)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::PredicateDelete {
            collection,
            filters,
            rls_write_check: _,
            returning: _,
            rls_filters: _,
        } => {
            let entry = encode_kv_predicate_delete(collection.as_str(), filters)?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Transfer {
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
            ..
        } => {
            let entry = encode_kv_transfer(KvTransferFields {
                collection: collection.as_str(),
                source_key,
                dest_key,
                field,
                amount: *amount,
                debit_surrogate: debit_surrogate.as_u32(),
                credit_surrogate: credit_surrogate.as_u32(),
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
            ..
        } => {
            let entry = encode_kv_transfer_item(
                source_collection.as_str(),
                dest_collection.as_str(),
                item_key,
                dest_key,
                surrogate.as_u32(),
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        // Each mutation gets its own record; returned LSN is the last appended.
        KvOp::ResolvedWrite {
            mutations,
            // Reply is decided per request; replay re-applies state, not answers a client.
            response_payload: _,
            // Already decided when proposed; a redo re-applies rather than re-decides.
            rls_write_check: _,
        } => {
            let mut last: Option<crate::types::Lsn> = None;
            for mutation in mutations {
                last = Some(append_kv_resolved_mutation(
                    wal,
                    tenant_id,
                    vshard_id,
                    database_id,
                    mutation,
                )?);
            }
            last
        }

        // Read-only ops. `ResolveWrite` reads rows a governed write depends on.
        KvOp::ResolveWrite(_)
        | KvOp::Get { .. }
        | KvOp::BatchGet { .. }
        | KvOp::Scan { .. }
        | KvOp::FieldGet { .. }
        | KvOp::GetTtl { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::MaterializeScan { .. } => None,
    };
    Ok(KvAppendOutcome {
        lsn,
        resolved_now_ms,
    })
}

/// Append one mutation of a resolved KV write and return its LSN. Uses the
/// absolute expiry already resolved — no clock read here, so redo matches apply.
fn append_kv_resolved_mutation(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    mutation: &nodedb_physical::physical_plan::KvResolvedMutation,
) -> crate::Result<crate::types::Lsn> {
    use nodedb_physical::physical_plan::KvResolvedMutation as M;
    match mutation {
        M::Put {
            collection,
            key,
            value,
            ttl_ms,
            expire_at_ms,
            surrogate,
            precondition: _,
        } => {
            let entry = encode_kv_put(
                collection.as_str(),
                key,
                value,
                *ttl_ms,
                // Always explicit, `0` included: `0` is this write's decided
                // "no expiry", not an absent field to re-derive `ttl_ms` from.
                Some(*expire_at_ms),
                surrogate.as_u32(),
            )?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)
        }
        M::Delete {
            collection,
            key,
            precondition: _,
        } => {
            let entry = encode_kv_delete(collection.as_str(), std::slice::from_ref(key))?;
            wal.append_delete(tenant_id, vshard_id, database_id, &entry)
        }
        M::Expire {
            collection,
            key,
            ttl_ms,
            resolved_now_ms,
            precondition: _,
        } => {
            let entry = encode_kv_expire(
                collection.as_str(),
                key,
                *ttl_ms,
                resolved_now_ms.saturating_add(*ttl_ms),
            )?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)
        }
        M::Persist {
            collection,
            key,
            precondition: _,
        } => {
            let entry = encode_kv_persist(collection.as_str(), key)?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)
        }
    }
}
