// SPDX-License-Identifier: BUSL-1.1

//! Redb rows of the synced-text frame versions.
//!
//! `SYNCED_TEXT` holds one row per frame a document received, keyed by the
//! frame's system-time ordinal. A frame version ends where the document's
//! next frame version starts. An empty value is a tombstone. The newest
//! version of a document is live, unless it is a tombstone.
//!
//! The history purge drops the frame versions no cut at or above its
//! horizon reads, by the rule the inverted index's history uses: a version
//! ended at or below the horizon goes. A tombstone at or below the horizon
//! with no later version goes too: no cut it covers reads any text.

use redb::{ReadableTable, StorageError, Table, WriteTransaction};

use super::shared::redb_err;
use crate::engine::sparse::fts_redb::bytes::synced_key_len;
use crate::engine::sparse::fts_redb::keys::{KeyOwner, SyncedKey};
use crate::engine::sparse::fts_redb::tables::{SYNCED_TEXT, SyncedTextKey};
use crate::engine::storage_bytes::StorageDelta;

/// An owned `SYNCED_TEXT` key: `(database_id, tenant_id, collection,
/// surrogate, ordinal)`.
type OwnedSyncedKey = (u64, u64, String, u32, i64);

/// Insert `value` under `key` into `text.synced`, and record the bytes it
/// adds in `delta`.
pub(crate) fn put_synced_row(
    table: &mut Table<'_, SyncedTextKey, &'static [u8]>,
    key: SyncedKey<'_>,
    value: &[u8],
    delta: &mut StorageDelta,
) -> Result<(), StorageError> {
    let old = table.insert(key, value)?.map(|prior| prior.value().len());
    delta.replaced(key.0, key.1, key.2, synced_key_len(key.2), old, value.len());
    Ok(())
}

/// Remove `key` from `text.synced`, and record the bytes it held in
/// `delta`.
pub(crate) fn remove_synced_row(
    table: &mut Table<'_, SyncedTextKey, &'static [u8]>,
    key: SyncedKey<'_>,
    delta: &mut StorageDelta,
) -> Result<(), StorageError> {
    if let Some(len) = table.remove(key)?.map(|prior| prior.value().len()) {
        delta.removed(key.0, key.1, key.2, synced_key_len(key.2), len);
    }
    Ok(())
}

/// Remove every owned key in `doomed` from `text.synced`.
fn remove_all(
    table: &mut Table<'_, SyncedTextKey, &'static [u8]>,
    doomed: &[OwnedSyncedKey],
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    for (d, t, c, s, o) in doomed {
        remove_synced_row(table, (*d, *t, c.as_str(), *s, *o), delta)
            .map_err(|e| redb_err("remove synced text", e))?;
    }
    Ok(())
}

/// Delete every `text.synced` row `owner` covers, recording their bytes in
/// `delta`. Returns the number of rows removed.
pub(crate) fn drop_owned(
    txn: &WriteTransaction,
    owner: KeyOwner<'_>,
    delta: &mut StorageDelta,
) -> crate::Result<usize> {
    let mut table = txn
        .open_table(SYNCED_TEXT)
        .map_err(|e| redb_err("open synced text", e))?;
    let mut doomed: Vec<OwnedSyncedKey> = Vec::new();
    for entry in table
        .range(owner.synced_start()..)
        .map_err(|e| redb_err("synced text range", e))?
    {
        let (key, _) = entry.map_err(|e| redb_err("synced text entry", e))?;
        let (d, t, c, s, o) = key.value();
        if !owner.owns(d, t, c, None) {
            break;
        }
        doomed.push((d, t, c.to_string(), s, o));
    }
    remove_all(&mut table, &doomed, delta)?;
    Ok(doomed.len())
}

/// Drop every frame version no cut at or above its collection's horizon
/// reads. `horizon_of(database, tenant, collection)` names the horizon,
/// `None` to keep the collection's whole history. Returns the number of
/// frame versions dropped.
pub(crate) fn purge_below(
    txn: &WriteTransaction,
    horizon_of: &dyn Fn(u64, u64, &str) -> Option<i64>,
    delta: &mut StorageDelta,
) -> crate::Result<usize> {
    let mut table = txn
        .open_table(SYNCED_TEXT)
        .map_err(|e| redb_err("open synced text", e))?;
    let mut doomed: Vec<OwnedSyncedKey> = Vec::new();
    // The previous row and whether it is a tombstone.
    let mut prev: Option<(OwnedSyncedKey, bool)> = None;
    for entry in table.iter().map_err(|e| redb_err("scan synced text", e))? {
        let (key, value) = entry.map_err(|e| redb_err("read synced text", e))?;
        let (d, t, c, s, o) = key.value();
        if let Some((p, tombstone)) = prev.take() {
            let same_doc = p.0 == d && p.1 == t && p.2 == c && p.3 == s;
            let horizon = horizon_of(p.0, p.1, &p.2);
            let ended_below = same_doc && horizon.is_some_and(|h| o <= h);
            let dead_newest = !same_doc && tombstone && horizon.is_some_and(|h| p.4 <= h);
            if ended_below || dead_newest {
                doomed.push(p);
            }
        }
        prev = Some(((d, t, c.to_string(), s, o), value.value().is_empty()));
    }
    if let Some((p, tombstone)) = prev
        && tombstone
        && horizon_of(p.0, p.1, &p.2).is_some_and(|h| p.4 <= h)
    {
        doomed.push(p);
    }
    remove_all(&mut table, &doomed, delta)?;
    Ok(doomed.len())
}
