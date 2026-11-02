// SPDX-License-Identifier: BUSL-1.1

//! Version visibility over the versioned document table.
//!
//! A version is visible at ordinal `S` when its `sys_from <= S` and no later
//! version of its row sits at or below `S`. The current state is the
//! visibility at the newest ordinal. A tombstone or an erasure that is
//! visible hides its row.
//!
//! Each row's versions are contiguous in key order and ascend by system
//! time. A point read is one reverse seek. A scan walks the rows in key order,
//! holds the newest version at or below the cut, and decodes only that one.

use std::ops::ControlFlow;

use nodedb_types::StorageKey;
use redb::{AccessGuard, ReadableTable};

use super::key::{doc_prefix, doc_prefix_end, format_sys_from, versioned_doc_key};
use super::value::{DecodedValue, decode_value};
use crate::engine::sparse::btree::{KeyedTable, invalid_storage_key_err, redb_err};

/// The visible live version of one row, handed to a scan visitor.
pub struct VisibleVersion<'a> {
    pub doc_id: StorageKey,
    /// The version's system-time ordinal.
    pub sys_from: i64,
    /// The version's decoded envelope. Always live.
    pub value: DecodedValue<'a>,
}

/// The key range one collection scan walks.
pub(in crate::engine::sparse) struct ScanRange<'a> {
    pub coll: &'a str,
    /// The collection prefix `"{db}:{tenant}:{coll}:"` every key starts with.
    pub prefix: &'a str,
    /// The first key the scan visits.
    pub start: &'a str,
    /// The exclusive end of the scan.
    pub end: &'a str,
}

/// One stored version: its system-time ordinal and its stored value.
pub(in crate::engine::sparse) type StoredVersion<'t> = (i64, AccessGuard<'t, &'static [u8]>);

/// The newest stored version of one row at or below `cutoff` (`None` = the
/// newest version), live or not, with its system-time ordinal. One reverse
/// seek.
pub(in crate::engine::sparse) fn visible_version<'t, T>(
    table: &'t T,
    target: RowRef<'_>,
    cutoff: Option<i64>,
) -> crate::Result<Option<StoredVersion<'t>>>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let RowRef {
        database_id,
        tenant,
        coll,
        doc_id,
    } = target;
    let lo = doc_prefix(database_id, tenant, coll, doc_id);
    let newest = match cutoff {
        Some(cutoff) => {
            let hi = versioned_doc_key(database_id, tenant, coll, doc_id, cutoff);
            table
                .range(lo.as_str()..=hi.as_str())
                .map_err(|e| redb_err("range", e))?
                .next_back()
        }
        None => {
            let hi = doc_prefix_end(database_id, tenant, coll, doc_id);
            table
                .range(lo.as_str()..hi.as_str())
                .map_err(|e| redb_err("range", e))?
                .next_back()
        }
    };
    let Some(entry) = newest else {
        return Ok(None);
    };
    let (key, value) = entry.map_err(|e| redb_err("entry", e))?;
    let sys_from = version_sys_from(key.value(), coll)?;
    Ok(Some((sys_from, value)))
}

/// The body of the version of one row visible at `cutoff` (`None` = the
/// current state). `None` when no version is visible or the visible one is a
/// tombstone or an erasure.
pub(in crate::engine::sparse) fn visible_body<T>(
    table: &T,
    target: RowRef<'_>,
    cutoff: Option<i64>,
) -> crate::Result<Option<(i64, Vec<u8>)>>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let Some((sys_from, guard)) = visible_version(table, target, cutoff)? else {
        return Ok(None);
    };
    let decoded = decode_value(guard.value())?;
    Ok(decoded.is_live().then(|| (sys_from, decoded.body.to_vec())))
}

/// One row of one collection.
#[derive(Clone, Copy)]
pub(in crate::engine::sparse) struct RowRef<'a> {
    pub database_id: u64,
    pub tenant: u64,
    pub coll: &'a str,
    pub doc_id: &'a StorageKey,
}

/// Visit the version of every row in `range` visible at `cutoff` (`None` =
/// the current state), in row order. Rows whose visible version is a
/// tombstone or an erasure are skipped. Only the visible version of a row is
/// decoded. `stop` is consulted once per stored version and ends the scan
/// where it stands. The visitor ends the scan with `ControlFlow::Break`.
pub(in crate::engine::sparse) fn for_each_visible<T, F>(
    table: &T,
    range: ScanRange<'_>,
    cutoff: Option<i64>,
    stop: &dyn Fn() -> bool,
    mut visit: F,
) -> crate::Result<()>
where
    T: ReadableTable<&'static str, &'static [u8]>,
    F: FnMut(VisibleVersion<'_>) -> crate::Result<ControlFlow<()>>,
{
    let cutoff_key = cutoff.map(format_sys_from);
    let entries = table
        .range(range.start..range.end)
        .map_err(|e| redb_err("range", e))?;
    let mut current: Option<StorageKey> = None;
    let mut held: Option<(StorageKey, i64, AccessGuard<'_, &'static [u8]>)> = None;
    for entry in entries {
        if stop() {
            return Ok(());
        }
        let (key, value) = entry.map_err(|e| redb_err("entry", e))?;
        let key = key.value();
        let (id_part, suffix) = key.rsplit_once('\x00').ok_or_else(|| {
            invalid_storage_key_err(KeyedTable::DocumentsVersioned, range.coll, key)
        })?;
        let seg = id_part.strip_prefix(range.prefix).ok_or_else(|| {
            invalid_storage_key_err(KeyedTable::DocumentsVersioned, range.coll, key)
        })?;
        let doc_id = StorageKey::parse(seg).ok_or_else(|| {
            invalid_storage_key_err(KeyedTable::DocumentsVersioned, range.coll, seg)
        })?;
        if current != Some(doc_id) {
            if let Some(row) = held.take()
                && emit(row, &mut visit)?.is_break()
            {
                return Ok(());
            }
            current = Some(doc_id);
        }
        // A version above the cut is skipped by its key alone.
        if cutoff_key.as_deref().is_some_and(|c| suffix > c) {
            continue;
        }
        let sys_from = suffix.parse::<i64>().map_err(|_| {
            invalid_storage_key_err(KeyedTable::DocumentsVersioned, range.coll, key)
        })?;
        held = Some((doc_id, sys_from, value));
    }
    // The last row ends the walk whether or not `visit` stops it.
    if let Some(row) = held {
        let _ = emit(row, &mut visit)?;
    }
    Ok(())
}

/// Hand one row's held version to `visit` when it is live.
fn emit<F>(
    row: (StorageKey, i64, AccessGuard<'_, &'static [u8]>),
    visit: &mut F,
) -> crate::Result<ControlFlow<()>>
where
    F: FnMut(VisibleVersion<'_>) -> crate::Result<ControlFlow<()>>,
{
    let (doc_id, sys_from, guard) = row;
    let value = decode_value(guard.value())?;
    if !value.is_live() {
        return Ok(ControlFlow::Continue(()));
    }
    visit(VisibleVersion {
        doc_id,
        sys_from,
        value,
    })
}

/// The system-time ordinal of a versioned document key.
pub(in crate::engine::sparse) fn version_sys_from(key: &str, coll: &str) -> crate::Result<i64> {
    key.rsplit_once('\x00')
        .and_then(|(_, suffix)| suffix.parse::<i64>().ok())
        .ok_or_else(|| invalid_storage_key_err(KeyedTable::DocumentsVersioned, coll, key))
}
