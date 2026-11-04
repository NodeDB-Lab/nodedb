// SPDX-License-Identifier: BUSL-1.1

//! Stored-bytes counters of the versioned document and index tables.
//!
//! Every document collection keeps every row version and index entry
//! version until purge, so the bytes a tenant's documents hold grow with
//! history. Each write that adds, rewrites or removes a stored entry records
//! the change in a [`StorageDelta`]. The writer applies the delta inside the
//! same write transaction, so both counters commit or roll back with the
//! entries they count:
//!
//! - the per-collection `STORAGE_BYTES` counter of the sparse file;
//! - the per-tenant `DOCUMENT_HISTORY_BYTES` counter, which the Data Plane
//!   charges to the document engine's memory budget.

use redb::{ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

use crate::engine::sparse::btree::storage_bytes::SPARSE_COUNTER;
use crate::engine::sparse::btree::{SparseEngine, redb_err};
use crate::engine::storage_bytes::StorageDelta;

/// The per-tenant counter name in underflow reports.
const HISTORY_COUNTER: &str = "sparse.document_history";

/// `"{database_id}:{tenant}"` → bytes the tenant's versioned document and
/// index entries hold, keys included.
pub(crate) const DOCUMENT_HISTORY_BYTES: TableDefinition<&str, u64> =
    TableDefinition::new("document_history_bytes");

/// The counter key of one tenant.
fn tenant_key(database_id: u64, tenant: u64) -> String {
    format!("{database_id}:{tenant}")
}

/// The `(database_id, tenant, collection)` a versioned table key names: its
/// first three `:`-separated fields. A collection name holds no `:`.
pub(in crate::engine::sparse) fn key_scope(key: &str) -> Option<(u64, u64, &str)> {
    let mut parts = key.splitn(4, ':');
    let database_id = parts.next()?.parse().ok()?;
    let tenant = parts.next()?.parse().ok()?;
    let collection = parts.next()?;
    parts.next()?;
    Some((database_id, tenant, collection))
}

/// The `(database_id, tenant)` a versioned table key names: its first two
/// `:`-separated fields.
pub(in crate::engine::sparse) fn key_tenant(key: &str) -> Option<(u64, u64)> {
    let mut parts = key.splitn(3, ':');
    let database_id = parts.next()?.parse().ok()?;
    let tenant = parts.next()?.parse().ok()?;
    Some((database_id, tenant))
}

/// Add `delta` bytes to the counter of `(database_id, tenant)` inside `txn`.
/// A change below zero is reported and clamps at zero.
fn adjust_history_bytes(
    txn: &WriteTransaction,
    database_id: u64,
    tenant: u64,
    delta: i64,
) -> crate::Result<()> {
    if delta == 0 {
        return Ok(());
    }
    let key = tenant_key(database_id, tenant);
    let mut table = txn
        .open_table(DOCUMENT_HISTORY_BYTES)
        .map_err(|e| redb_err("open history bytes", e))?;
    let current = table
        .get(key.as_str())
        .map_err(|e| redb_err("read history bytes", e))?
        .map(|guard| guard.value())
        .unwrap_or(0);
    let next = match u64::try_from(delta) {
        Ok(added) => current.saturating_add(added),
        Err(_) => {
            let removed = delta.unsigned_abs();
            if removed > current {
                crate::diag::storage_bytes_underflow(
                    HISTORY_COUNTER,
                    database_id,
                    tenant,
                    None,
                    current,
                    removed,
                );
            }
            current.saturating_sub(removed)
        }
    };
    if next == 0 {
        table
            .remove(key.as_str())
            .map_err(|e| redb_err("clear history bytes", e))?;
    } else {
        table
            .insert(key.as_str(), next)
            .map_err(|e| redb_err("write history bytes", e))?;
    }
    Ok(())
}

/// Apply the change `delta` records for the versioned tables inside `txn`:
/// each collection's stored-bytes counter and each tenant's history counter.
/// Every writer of the versioned tables calls it before its transaction
/// commits.
pub(in crate::engine::sparse) fn apply_versioned_delta(
    txn: &WriteTransaction,
    delta: StorageDelta,
) -> crate::Result<()> {
    for (database_id, tenant, change) in delta.tenant_sums() {
        adjust_history_bytes(txn, database_id, tenant, change)?;
    }
    delta.apply(txn, SPARSE_COUNTER)
}

/// Insert `value` under `key` into a versioned table, and record the bytes
/// the insert adds in `delta`. A rewrite of an existing key records the
/// difference.
pub(in crate::engine::sparse) fn insert_counted(
    table: &mut redb::Table<'_, &'static str, &'static [u8]>,
    key: &str,
    value: &[u8],
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    let (database_id, tenant, collection) = scope_of(key)?;
    let replaced = table
        .insert(key, value)
        .map_err(|e| redb_err("insert", e))?
        .map(|old| old.value().len());
    delta.replaced(
        database_id,
        tenant,
        collection,
        key.len(),
        replaced,
        value.len(),
    );
    Ok(())
}

/// Remove `key` from a versioned table, and record the bytes it held in
/// `delta`. Removing an absent key records nothing.
pub(in crate::engine::sparse) fn remove_counted(
    table: &mut redb::Table<'_, &'static str, &'static [u8]>,
    key: &str,
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    let (database_id, tenant, collection) = scope_of(key)?;
    let removed = table
        .remove(key)
        .map_err(|e| redb_err("remove", e))?
        .map(|old| old.value().len());
    if let Some(len) = removed {
        delta.removed(database_id, tenant, collection, key.len(), len);
    }
    Ok(())
}

/// [`key_scope`], with a key that names no collection as a storage error.
fn scope_of(key: &str) -> crate::Result<(u64, u64, &str)> {
    key_scope(key).ok_or_else(|| crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("versioned key names no collection: {key:?}"),
    })
}

impl SparseEngine {
    /// Bootstrap: ensure the history-bytes counter table exists, so a read
    /// transaction can open it before the first write.
    pub(in crate::engine::sparse) fn ensure_history_bytes_table(&self) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let _ = txn
                .open_table(DOCUMENT_HISTORY_BYTES)
                .map_err(|e| redb_err("open history bytes", e))?;
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// The bytes the versioned document and index entries of
    /// `(database_id, tenant)` hold: every version, tombstones included.
    pub fn document_history_bytes(&self, database_id: u64, tenant: u64) -> crate::Result<u64> {
        let key = tenant_key(database_id, tenant);
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = txn
            .open_table(DOCUMENT_HISTORY_BYTES)
            .map_err(|e| redb_err("open history bytes", e))?;
        Ok(table
            .get(key.as_str())
            .map_err(|e| redb_err("read history bytes", e))?
            .map(|guard| guard.value())
            .unwrap_or(0))
    }

    /// Every `(database_id, tenant)` whose versioned document and index
    /// entries hold bytes. One row per tenant.
    pub fn document_history_tenants(&self) -> crate::Result<Vec<(u64, u64)>> {
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = txn
            .open_table(DOCUMENT_HISTORY_BYTES)
            .map_err(|e| redb_err("open history bytes", e))?;
        let mut tenants = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| redb_err("scan history bytes", e))?
        {
            let (key, _) = entry.map_err(|e| redb_err("read history bytes", e))?;
            let key = key.value();
            let tenant = key_tenant(key).ok_or_else(|| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("history bytes key names no tenant: {key:?}"),
            })?;
            tenants.push(tenant);
        }
        Ok(tenants)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_names_its_tenant() {
        assert_eq!(
            key_tenant("3:7:users:0000002a\u{0}00000000000000000010"),
            Some((3, 7))
        );
        assert_eq!(key_tenant("x:7:users"), None);
    }

    #[test]
    fn a_key_names_its_collection() {
        assert_eq!(
            key_scope("3:7:users:0000002a\u{0}00000000000000000010"),
            Some((3, 7, "users"))
        );
        assert_eq!(
            key_scope("3:7:users:email:a@b:0000002a\u{0}00000000000000000010"),
            Some((3, 7, "users"))
        );
        assert_eq!(key_scope("3:7:users"), None);
    }
}
