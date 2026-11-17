// SPDX-License-Identifier: BUSL-1.1

//! Stored sync-frame versions, as a collection rebuild reads them.
//!
//! The rebuild thread reads one collection's frames from its pinned
//! snapshot. The cutover reads the frames of one document inside its own
//! write transaction, for a document written during the rebuild. A
//! catch-up round reads them from its fresh snapshot. Every walk
//! decode a row the way `for_each_synced_text` does: an empty value is a
//! tombstone.

use redb::{ReadTransaction, ReadableTable, WriteTransaction};

use nodedb_types::Surrogate;

use super::errors::inverted_err;
use super::rebuild_snapshot::FtsRebuildTicket;
use super::synced_text::SyncedDocText;
use crate::engine::sparse::fts_redb::keys::KeyOwner;
use crate::engine::sparse::fts_redb::tables::{SYNCED_TEXT, SyncedTextKey};

/// Decode one stored frame value.
fn decode_frame(value: &[u8]) -> crate::Result<Vec<(String, String)>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(value).map_err(|e| inverted_err("decode synced text", e))
}

/// Visit every frame version in `lo..=hi` of `table` that `owns` accepts,
/// in key order, until the first key it refuses.
fn walk<T>(
    table: &T,
    lo: (u64, u64, &str, u32, i64),
    owns: &dyn Fn(u64, u64, &str, u32) -> bool,
    visit: &mut dyn FnMut(SyncedDocText) -> crate::Result<()>,
) -> crate::Result<()>
where
    T: ReadableTable<SyncedTextKey, &'static [u8]>,
{
    for entry in table
        .range(lo..)
        .map_err(|e| inverted_err("synced text range", e))?
    {
        let (key, value) = entry.map_err(|e| inverted_err("synced text entry", e))?;
        let (d, t, c, s, ordinal) = key.value();
        if !owns(d, t, c, s) {
            break;
        }
        visit(SyncedDocText {
            collection: c.to_string(),
            surrogate: Surrogate::new(s),
            ordinal,
            fields: decode_frame(value.value())?,
        })?;
    }
    Ok(())
}

impl FtsRebuildTicket {
    /// Visit every stored frame version of the collection the snapshot
    /// holds, by surrogate, then ordinal.
    pub fn for_each_snapshot_frame(
        &self,
        visit: &mut dyn FnMut(SyncedDocText) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let txn: &ReadTransaction = self.snapshot();
        let table = txn
            .open_table(SYNCED_TEXT)
            .map_err(|e| inverted_err("open synced text", e))?;
        let owner = KeyOwner::collection(self.database_id(), self.tid(), self.collection());
        walk(
            &table,
            owner.synced_start(),
            &|d, t, c, _| owner.owns(d, t, c, None),
            visit,
        )
    }
}

/// Whether `txn` sees any stored frame version of `collection`.
pub fn collection_has_frames(
    txn: &ReadTransaction,
    (database_id, tid): (u64, u64),
    collection: &str,
) -> crate::Result<bool> {
    let table = txn
        .open_table(SYNCED_TEXT)
        .map_err(|e| inverted_err("open synced text", e))?;
    let owner = KeyOwner::collection(database_id, tid, collection);
    let first = table
        .range(owner.synced_start()..)
        .map_err(|e| inverted_err("synced text range", e))?
        .next()
        .transpose()
        .map_err(|e| inverted_err("synced text entry", e))?;
    Ok(first.is_some_and(|(key, _)| {
        let (d, t, c, _, _) = key.value();
        owner.owns(d, t, c, None)
    }))
}

/// Visit every stored frame version of document `surrogate` inside `txn`,
/// in ordinal order. The walk sees the frames `txn` staged.
pub fn for_each_doc_frame_in_txn(
    txn: &WriteTransaction,
    (database_id, tid): (u64, u64),
    collection: &str,
    surrogate: Surrogate,
    visit: &mut dyn FnMut(SyncedDocText) -> crate::Result<()>,
) -> crate::Result<()> {
    let table = txn
        .open_table(SYNCED_TEXT)
        .map_err(|e| inverted_err("open synced text", e))?;
    walk_doc(&table, (database_id, tid), collection, surrogate, visit)
}

/// Visit every stored frame version of document `surrogate` that the
/// pinned snapshot `txn` holds, in ordinal order.
pub fn for_each_doc_frame_in_snapshot(
    txn: &ReadTransaction,
    (database_id, tid): (u64, u64),
    collection: &str,
    surrogate: Surrogate,
    visit: &mut dyn FnMut(SyncedDocText) -> crate::Result<()>,
) -> crate::Result<()> {
    let table = txn
        .open_table(SYNCED_TEXT)
        .map_err(|e| inverted_err("open synced text", e))?;
    walk_doc(&table, (database_id, tid), collection, surrogate, visit)
}

/// Visit every frame version of one document in `table`, in ordinal order.
fn walk_doc<T>(
    table: &T,
    (database_id, tid): (u64, u64),
    collection: &str,
    surrogate: Surrogate,
    visit: &mut dyn FnMut(SyncedDocText) -> crate::Result<()>,
) -> crate::Result<()>
where
    T: ReadableTable<SyncedTextKey, &'static [u8]>,
{
    let s = surrogate.as_u32();
    walk(
        table,
        (database_id, tid, collection, s, i64::MIN),
        &|d, t, c, row| d == database_id && t == tid && c == collection && row == s,
        visit,
    )
}
