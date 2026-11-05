// SPDX-License-Identifier: BUSL-1.1

//! Reads of the stored-bytes counter, and the removal of a dropped scope's
//! counter rows.

use nodedb_types::{DatabaseId, TenantId};
use redb::{Database, ReadableDatabase, ReadableTable, WriteTransaction};

use super::table::{STORAGE_BYTES, StorageBytesRow, counter_err};

/// The scope a drop removed every counted row of.
#[derive(Debug, Clone, Copy)]
pub enum DroppedScope<'a> {
    /// One collection of one tenant.
    Collection {
        database_id: u64,
        tenant_id: u64,
        collection: &'a str,
    },
    /// Every collection of one tenant.
    Tenant { database_id: u64, tenant_id: u64 },
}

impl DroppedScope<'_> {
    fn label(&self) -> &'static str {
        match self {
            Self::Collection { .. } => "collection",
            Self::Tenant { .. } => "tenant",
        }
    }

    fn holds(&self, database_id: u64, tenant_id: u64, collection: &str) -> bool {
        match *self {
            Self::Collection {
                database_id: d,
                tenant_id: t,
                collection: c,
            } => d == database_id && t == tenant_id && c == collection,
            Self::Tenant {
                database_id: d,
                tenant_id: t,
            } => d == database_id && t == tenant_id,
        }
    }
}

/// Every counter row of the file `db` opens, one per collection holding
/// stored bytes. Cost is the number of counter rows.
pub fn storage_bytes_snapshot(
    db: &Database,
    engine: &'static str,
) -> crate::Result<Vec<StorageBytesRow>> {
    let txn = db
        .begin_read()
        .map_err(|e| counter_err(engine, "read txn", e))?;
    let table = txn
        .open_table(STORAGE_BYTES)
        .map_err(|e| counter_err(engine, "open", e))?;
    let mut rows = Vec::new();
    for entry in table.iter().map_err(|e| counter_err(engine, "scan", e))? {
        let (key, value) = entry.map_err(|e| counter_err(engine, "read", e))?;
        let (database_id, tenant_id, collection) = key.value();
        rows.push((
            DatabaseId::new(database_id),
            TenantId::new(tenant_id),
            collection.to_owned(),
            value.value(),
        ));
    }
    Ok(rows)
}

/// Remove the counter rows of `scope` inside `txn`, after every counted row
/// of the scope in this file is gone.
///
/// Exact counting leaves no row then. A row that remains held bytes the
/// counter never saw leave: it is reported, then removed. Returns the
/// number of rows removed.
pub fn retire_storage_bytes(
    txn: &WriteTransaction,
    engine: &'static str,
    scope: DroppedScope<'_>,
) -> crate::Result<usize> {
    let (database_id, tenant_id) = match scope {
        DroppedScope::Collection {
            database_id,
            tenant_id,
            ..
        }
        | DroppedScope::Tenant {
            database_id,
            tenant_id,
        } => (database_id, tenant_id),
    };
    let mut table = txn
        .open_table(STORAGE_BYTES)
        .map_err(|e| counter_err(engine, "open", e))?;
    let mut residue: Vec<(String, u64)> = Vec::new();
    for entry in table
        .range((database_id, tenant_id, "")..)
        .map_err(|e| counter_err(engine, "range", e))?
    {
        let (key, value) = entry.map_err(|e| counter_err(engine, "read", e))?;
        let (d, t, collection) = key.value();
        if d != database_id || t != tenant_id {
            break;
        }
        if scope.holds(d, t, collection) {
            residue.push((collection.to_owned(), value.value()));
        }
    }
    for (collection, bytes) in &residue {
        crate::diag::storage_bytes_residue(
            engine,
            scope.label(),
            database_id,
            tenant_id,
            collection,
            *bytes,
        );
        table
            .remove((database_id, tenant_id, collection.as_str()))
            .map_err(|e| counter_err(engine, "remove", e))?;
    }
    Ok(residue.len())
}
