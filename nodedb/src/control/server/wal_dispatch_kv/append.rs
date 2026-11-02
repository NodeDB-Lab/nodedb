// SPDX-License-Identifier: BUSL-1.1

//! Dispatch of `KvOp` variants to WAL append calls.

use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalAppender;
use nodedb_physical::physical_plan::KvOp;

use super::encode::{
    KvIncrRecord, KvRegisterSortedIndexFields, KvTransferFields, encode_kv_batch_put,
    encode_kv_cas, encode_kv_delete, encode_kv_drop_index, encode_kv_drop_sorted_index,
    encode_kv_expire, encode_kv_field_set, encode_kv_getset, encode_kv_incr, encode_kv_incr_float,
    encode_kv_insert_on_conflict_update, encode_kv_persist, encode_kv_put,
    encode_kv_register_index, encode_kv_register_sorted_index, encode_kv_transfer,
    encode_kv_transfer_item, encode_kv_truncate,
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

/// Append the `SyncSeqAdvance` record of a Lite KV push, ahead of the
/// write's own record.
///
/// Both records land in the write's outcome-floor window. A frame the gate
/// holds back cancels both, so the mark never moves for a write that did not
/// apply. The write's record is the higher LSN, so the durable-at-ack wait
/// on it covers the mark too.
fn append_sync_mark(
    wal: WalAppender<'_>,
    prov: &nodedb_types::sync::wire::SyncProvenance,
) -> crate::Result<crate::types::Lsn> {
    wal.append_sync_seq_advance(prov.producer_id, prov.epoch, prov.stream_id, prov.seq)
}

/// The refusal of a KV write whose rows depend on stored state that reached
/// the WAL as the bare op.
fn unresolved_conditional_write(collection: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "internal invariant break: a conditional kv insert on '{collection}' reached the \
             WAL unresolved; write_resolve resolves it to the rows it writes first"
        ),
    }
}

/// Serialize a KV operation and append to the WAL — see [`KvAppendOutcome`].
/// `now_override` pins `expire_at_ms` to an instant decided elsewhere (e.g. a
/// Raft-committed entry), so every replica's redo installs it verbatim.
pub fn wal_append_kv_op(
    wal: WalAppender<'_>,
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
            provenance,
            ..
        } => {
            if let Some(prov) = provenance {
                append_sync_mark(wal, prov)?;
            }
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
        // A conditional write that skips a stored row would leave a record
        // that replay applies as a put over that row. It resolves to the rows
        // it writes before it reaches the WAL.
        KvOp::InsertIfAbsent { collection, .. } => {
            return Err(unresolved_conditional_write(collection.as_str()));
        }
        KvOp::BatchPut {
            collection, intent, ..
        } if intent.depends_on_stored_rows() => {
            return Err(unresolved_conditional_write(collection.as_str()));
        }
        // A refused insert writes nothing: its outcome cancels this record.
        KvOp::Insert {
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
            surrogate,
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
                surrogate.as_u32(),
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Delete {
            collection,
            keys,
            provenance,
            ..
        } => {
            if let Some(prov) = provenance {
                append_sync_mark(wal, prov)?;
            }
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
            if_present,
            ..
        } => {
            let entry = encode_kv_field_set(
                collection.as_str(),
                key,
                updates,
                surrogate.as_u32(),
                *if_present,
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Incr {
            collection,
            key,
            delta,
            ttl_ms,
            surrogate,
            shape,
            ..
        } => {
            let (now_ms, expire_at_ms) = resolve_expiry(*ttl_ms, now_override);
            resolved_now_ms = now_ms;
            let entry = encode_kv_incr(KvIncrRecord {
                collection: collection.as_str(),
                key,
                delta: *delta,
                ttl_ms: *ttl_ms,
                surrogate: surrogate.as_u32(),
                shape,
                expire_at_ms,
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::IncrFloat {
            collection,
            key,
            delta,
            surrogate,
            shape,
            ..
        } => {
            let entry =
                encode_kv_incr_float(collection.as_str(), key, delta, surrogate.as_u32(), shape)?;
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
        KvOp::Truncate { collection, .. } => {
            let entry = encode_kv_truncate(collection.as_str())?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        // A row set replay scans for itself depends on its clock (TTL expiry),
        // so a predicate write resolves to a mutation list first.
        KvOp::PredicateUpdate { collection, .. } | KvOp::PredicateDelete { collection, .. } => {
            return Err(crate::Error::Internal {
                detail: format!(
                    "internal invariant break: a kv predicate UPDATE/DELETE on '{}' reached \
                     the WAL unresolved; write_resolve resolves it to a mutation list first",
                    collection.as_str()
                ),
            });
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
                last = Some(super::append_resolved::append_kv_resolved_mutation(
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
        | KvOp::SortedIndexTxnRead { .. }
        | KvOp::MaterializeScan { .. } => None,
    };
    Ok(KvAppendOutcome {
        lsn,
        resolved_now_ms,
    })
}
