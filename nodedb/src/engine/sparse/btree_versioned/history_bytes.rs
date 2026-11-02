// SPDX-License-Identifier: BUSL-1.1

//! Stored-bytes counter of the versioned document and index tables.
//!
//! Every document collection keeps every row version and index entry
//! version until purge, so the bytes a tenant's documents hold grow with
//! history. Each write that adds, rewrites or removes a stored entry adjusts
//! the tenant's counter inside the same write transaction, so the counter
//! commits or rolls back with the entries it counts. The Data Plane charges
//! the counter to the document engine's memory budget.

use redb::{ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

use crate::engine::sparse::btree::{SparseEngine, redb_err};

/// `"{database_id}:{tenant}"` → bytes the tenant's versioned document and
/// index entries hold, keys included.
pub(crate) const DOCUMENT_HISTORY_BYTES: TableDefinition<&str, u64> =
    TableDefinition::new("document_history_bytes");

/// The counter key of one tenant.
fn tenant_key(database_id: u64, tenant: u64) -> String {
    format!("{database_id}:{tenant}")
}

/// The bytes one stored entry holds: its key and its value.
pub(in crate::engine::sparse) fn entry_bytes(key: &str, value_len: usize) -> i64 {
    i64::try_from(key.len().saturating_add(value_len)).unwrap_or(i64::MAX)
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
/// The counter never drops below zero.
pub(in crate::engine::sparse) fn adjust_history_bytes(
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
    let next = if delta >= 0 {
        current.saturating_add(delta.unsigned_abs())
    } else {
        current.saturating_sub(delta.unsigned_abs())
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

/// Insert `value` under `key` into a versioned table inside `txn`, and move
/// the key's tenant counter by the bytes the insert adds (a rewrite of an
/// existing key counts the difference).
pub(in crate::engine::sparse) fn insert_counted(
    txn: &WriteTransaction,
    table: &mut redb::Table<'_, &'static str, &'static [u8]>,
    key: &str,
    value: &[u8],
) -> crate::Result<()> {
    let (database_id, tenant) = tenant_of(key)?;
    let replaced = table
        .insert(key, value)
        .map_err(|e| redb_err("insert", e))?
        .map(|old| old.value().len());
    let delta = entry_bytes(key, value.len()) - replaced.map_or(0, |len| entry_bytes(key, len));
    adjust_history_bytes(txn, database_id, tenant, delta)
}

/// Remove `key` from a versioned table inside `txn`, and take its bytes off
/// its tenant counter. Removing an absent key changes nothing.
pub(in crate::engine::sparse) fn remove_counted(
    txn: &WriteTransaction,
    table: &mut redb::Table<'_, &'static str, &'static [u8]>,
    key: &str,
) -> crate::Result<()> {
    let (database_id, tenant) = tenant_of(key)?;
    let removed = table
        .remove(key)
        .map_err(|e| redb_err("remove", e))?
        .map(|old| old.value().len());
    match removed {
        Some(len) => adjust_history_bytes(txn, database_id, tenant, -entry_bytes(key, len)),
        None => Ok(()),
    }
}

/// [`key_tenant`], with a key that names no tenant as a storage error.
fn tenant_of(key: &str) -> crate::Result<(u64, u64)> {
    key_tenant(key).ok_or_else(|| crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("versioned key names no tenant: {key:?}"),
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
}
