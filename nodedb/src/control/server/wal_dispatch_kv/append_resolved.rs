// SPDX-License-Identifier: BUSL-1.1

//! WAL append for the mutations of a resolved KV write.

use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalAppender;

use super::encode::{
    encode_kv_delete, encode_kv_expire, encode_kv_persist, encode_kv_put, encode_kv_reap,
    encode_kv_rewrite,
};

/// Append one mutation of a resolved KV write and return its LSN. Uses the
/// absolute expiry already resolved — no clock read here, so redo matches apply.
pub(super) fn append_kv_resolved_mutation(
    wal: WalAppender<'_>,
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
        M::Rewrite {
            collection,
            key,
            value,
            ttl_ms,
            expire_at_ms,
            precondition: _,
        } => {
            let entry = encode_kv_rewrite(collection.as_str(), key, value, *ttl_ms, *expire_at_ms)?;
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
        M::Reap {
            collection,
            key,
            expire_at_ms,
        } => {
            let entry = encode_kv_reap(collection.as_str(), key, *expire_at_ms)?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)
        }
    }
}
