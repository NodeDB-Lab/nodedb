// SPDX-License-Identifier: BUSL-1.1

//! The persisted stored-bytes counter of one redb file.
//!
//! Each Data Plane core owns its redb files. Each file keeps one counter row
//! per `(database_id, tenant_id, collection)` that holds stored rows in the
//! file's counted tables. A row holds the exact sum of the stored bytes of
//! those rows. A writer moves the counter inside the write transaction that
//! changes the rows, so the counter commits and rolls back with them. WAL
//! replay writes through the same paths, so a replayed row is counted once.
//!
//! Stored bytes of one row are its key bytes plus its value bytes. A string
//! key counts its UTF-8 length. A tuple key counts each component: 8 for a
//! `u64` or `i64`, 4 for a `u32`, the UTF-8 length for a `&str`. A `&[u8]`
//! value counts its length, an `i64` value 8, a `u8` value 1. A row counts
//! while it is stored: live rows, retained history versions and tombstones.

use nodedb_types::{DatabaseId, TenantId};
use redb::{TableDefinition, WriteTransaction};

/// `(database_id, tenant_id, collection)` → stored bytes of the collection's
/// rows in the file's counted tables. A collection with no stored bytes
/// has no row. The collection is the one the counted rows' keys carry.
pub const STORAGE_BYTES: TableDefinition<(u64, u64, &str), u64> =
    TableDefinition::new("storage_bytes");

/// One counter row: `(database, tenant, collection, stored bytes)`.
pub type StorageBytesRow = (DatabaseId, TenantId, String, u64);

/// The stored bytes of one row: its key bytes plus its value bytes.
pub fn row_bytes(key_len: usize, value_len: usize) -> i64 {
    i64::try_from(key_len.saturating_add(value_len)).unwrap_or(i64::MAX)
}

/// A storage error of the counter in the file `engine` names.
pub(super) fn counter_err(
    engine: &'static str,
    ctx: &str,
    e: impl std::fmt::Display,
) -> crate::Error {
    crate::Error::Storage {
        engine: engine.into(),
        detail: format!("storage bytes {ctx}: {e}"),
    }
}

/// Create the counter table inside `txn`, so a read transaction can open it
/// before the first counted write.
pub fn ensure_storage_bytes_table(
    txn: &WriteTransaction,
    engine: &'static str,
) -> crate::Result<()> {
    txn.open_table(STORAGE_BYTES)
        .map(drop)
        .map_err(|e| counter_err(engine, "open", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_counts_its_key_and_its_value() {
        assert_eq!(row_bytes(10, 5), 15);
        assert_eq!(row_bytes(0, 0), 0);
        assert_eq!(row_bytes(usize::MAX, 1), i64::MAX);
    }
}
