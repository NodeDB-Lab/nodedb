// SPDX-License-Identifier: BUSL-1.1

//! Walks over every stored version of a collection or of one row, from any
//! readable view of the document table.
//!
//! A full-text rebuild reads a collection's versions from a pinned read
//! snapshot on its own thread. Its cutover reads the versions of each row
//! written during the rebuild inside the cutover's write transaction. Both
//! walks decode keys and values the same way.

use nodedb_types::StorageKey;
use redb::{ReadOnlyTable, ReadTransaction, ReadableTable, WriteTransaction};

use super::doc::DOCUMENTS_VERSIONED;
use super::key::{coll_prefix, coll_prefix_end, doc_prefix, doc_prefix_end};
use super::value::decode_value;
use crate::engine::sparse::btree::{KeyedTable, invalid_storage_key_err, redb_err};

/// One stored version: the row, its system-time ordinal, and its body.
/// The body is `None` for a tombstone or an erasure.
pub type VersionVisit<'v> = dyn FnMut(&StorageKey, i64, Option<&[u8]>) -> crate::Result<()> + 'v;

/// The keys one walk covers.
struct WalkRange<'a> {
    coll: &'a str,
    lo: String,
    hi: String,
    /// Byte offset of the row id in every key: the collection prefix length.
    id_start: usize,
}

/// Visit every version `range` covers in key order: row order, then
/// ascending system time within a row. A visit error ends the walk and is
/// returned.
fn walk<T>(table: &T, range: &WalkRange<'_>, visit: &mut VersionVisit<'_>) -> crate::Result<()>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    for entry in table
        .range(range.lo.as_str()..range.hi.as_str())
        .map_err(|e| redb_err("range", e))?
    {
        let (k, v) = entry.map_err(|e| redb_err("entry", e))?;
        let key_str = k.value();
        let invalid =
            || invalid_storage_key_err(KeyedTable::DocumentsVersioned, range.coll, key_str);
        let (id_part, suffix) = key_str.rsplit_once('\x00').ok_or_else(invalid)?;
        let seg = id_part.get(range.id_start..).ok_or_else(invalid)?;
        let doc_id = StorageKey::parse(seg).ok_or_else(invalid)?;
        let sys_from = suffix.parse::<i64>().map_err(|_| invalid())?;
        let decoded = decode_value(v.value())?;
        visit(&doc_id, sys_from, decoded.is_live().then_some(decoded.body))?;
    }
    Ok(())
}

/// Open the document table of a read snapshot.
pub fn open_documents(
    txn: &ReadTransaction,
) -> crate::Result<ReadOnlyTable<&'static str, &'static [u8]>> {
    txn.open_table(DOCUMENTS_VERSIONED)
        .map_err(|e| redb_err("open documents_versioned", e))
}

/// Visit every version of every row of `coll` that `table` holds. A table
/// opened from a pinned snapshot sees the versions committed before it.
pub fn for_each_collection_version<T>(
    table: &T,
    database_id: u64,
    tenant: u64,
    coll: &str,
    visit: &mut VersionVisit<'_>,
) -> crate::Result<()>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let lo = coll_prefix(database_id, tenant, coll);
    let range = WalkRange {
        coll,
        id_start: lo.len(),
        lo,
        hi: coll_prefix_end(database_id, tenant, coll),
    };
    walk(table, &range, visit)
}

/// Whether `table` holds any version of any row of `coll`.
pub fn collection_has_versions<T>(
    table: &T,
    database_id: u64,
    tenant: u64,
    coll: &str,
) -> crate::Result<bool>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let lo = coll_prefix(database_id, tenant, coll);
    let hi = coll_prefix_end(database_id, tenant, coll);
    Ok(table
        .range(lo.as_str()..hi.as_str())
        .map_err(|e| redb_err("range", e))?
        .next()
        .is_some())
}

/// Visit every version of row `doc_id` of `coll` inside `txn`, in
/// ascending system time. The walk sees the versions `txn` staged.
pub fn for_each_row_version_in_txn(
    txn: &WriteTransaction,
    database_id: u64,
    tenant: u64,
    coll: &str,
    doc_id: &StorageKey,
    visit: &mut VersionVisit<'_>,
) -> crate::Result<()> {
    let table = txn
        .open_table(DOCUMENTS_VERSIONED)
        .map_err(|e| redb_err("open documents_versioned", e))?;
    for_each_row_version(&table, database_id, tenant, coll, doc_id, visit)
}

/// Visit every version of row `doc_id` of `coll` that `table` holds, in
/// ascending system time.
pub fn for_each_row_version<T>(
    table: &T,
    database_id: u64,
    tenant: u64,
    coll: &str,
    doc_id: &StorageKey,
    visit: &mut VersionVisit<'_>,
) -> crate::Result<()>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let range = WalkRange {
        coll,
        id_start: coll_prefix(database_id, tenant, coll).len(),
        lo: doc_prefix(database_id, tenant, coll, doc_id),
        hi: doc_prefix_end(database_id, tenant, coll, doc_id),
    };
    walk(table, &range, visit)
}
