// SPDX-License-Identifier: Apache-2.0

//! Scoped posting keys preserve database, tenant, and collection ownership.

/// Memtable key format: `"{database_id}:{tid}:{collection}:{term}"`. The memtable is a single in-memory map shared across databases and tenants, so keys must carry the full database + tenant + collection scope.
pub(crate) fn memtable_key(database_id: u64, tid: u64, collection: &str, term: &str) -> String {
    format!("{database_id}:{tid}:{collection}:{term}")
}

/// Prefix used by `drain_collection` to remove all memtable entries for a given `(database_id, tid, collection)`.
pub(crate) fn memtable_collection_prefix(database_id: u64, tid: u64, collection: &str) -> String {
    format!("{database_id}:{tid}:{collection}:")
}

/// Prefix used to remove every memtable entry for a given `(database_id, tenant)`.
pub(crate) fn memtable_tenant_prefix(database_id: u64, tid: u64) -> String {
    format!("{database_id}:{tid}:")
}
