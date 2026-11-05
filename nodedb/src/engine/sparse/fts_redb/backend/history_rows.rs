// SPDX-License-Identifier: BUSL-1.1

//! Rows of the inverted index's system-time history.
//!
//! An update or a delete of a document ends its indexed version at the
//! write's ordinal. The ended version's analyzed tokens move to `TEXT_ENDED`
//! keyed by its end, so a read at an earlier cut still matches and scores it.
//! A live version records its start in `TEXT_LIVE_FROM` (and its ordered
//! twin `TEXT_LIVE_BY_FROM`) while a retained snapshot can read below it. A
//! read at cut `S` hides the live documents indexed after `S` and adds the
//! ended versions visible at `S`: both are ranges over the writes after `S`.
//! A purge at horizon `H` drops every ended version with `sys_to <= H` and
//! every live start at or below `H`: no retained cut reads below them.

use nodedb_fts::{IndexScope, StagedDoc};
use nodedb_types::{Surrogate, SurrogateBitmap};
use redb::{ReadableTable, WriteTransaction};

use super::shared::redb_err;
use crate::engine::sparse::fts_redb::bytes::{
    ended_key_len, live_by_from_row_len, live_from_row_len,
};
use crate::engine::sparse::fts_redb::keys::KeyOwner;
use crate::engine::sparse::fts_redb::tables::{
    TEXT_ENDED, TEXT_LIVE_BY_FROM, TEXT_LIVE_FROM, TextEndedKey,
};
use crate::engine::storage_bytes::StorageDelta;

/// One ended version: its surrogate, start, end, and analyzed tokens.
pub(crate) struct EndedVersion {
    pub surrogate: Surrogate,
    pub sys_from: i64,
    pub sys_to: i64,
    pub tokens: Vec<String>,
}

/// The view of one index at a cut: the live documents indexed after it, and
/// the ended versions visible at it.
#[derive(Debug, Default)]
pub(crate) struct CutRows {
    pub hidden: SurrogateBitmap,
    pub docs: Vec<StagedDoc>,
}

/// The recorded start of `surrogate`'s live version in `index`. `None` when
/// every retained cut reads it.
pub(crate) fn live_from(
    txn: &WriteTransaction,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    surrogate: Surrogate,
) -> crate::Result<Option<i64>> {
    let table = txn
        .open_table(TEXT_LIVE_FROM)
        .map_err(|e| redb_err("open text live_from", e))?;
    let key = (
        database_id,
        tid,
        index.collection(),
        index.field_key(),
        surrogate.as_u32(),
    );
    Ok(table
        .get(key)
        .map_err(|e| redb_err("read text live_from", e))?
        .map(|v| v.value()))
}

/// Record `surrogate`'s live version in `index` as starting at `sys_from`,
/// replacing any recorded start. `delta` records the bytes the rows change.
pub(crate) fn set_live(
    txn: &WriteTransaction,
    (database_id, tid): (u64, u64),
    index: IndexScope<'_>,
    surrogate: Surrogate,
    sys_from: i64,
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    clear_live(txn, database_id, tid, index, surrogate, delta)?;
    let (c, f, s) = (index.collection(), index.field_key(), surrogate.as_u32());
    let start_was_there = txn
        .open_table(TEXT_LIVE_FROM)
        .map_err(|e| redb_err("open text live_from", e))?
        .insert((database_id, tid, c, f, s), sys_from)
        .map_err(|e| redb_err("write text live_from", e))?
        .is_some();
    if !start_was_there {
        delta.stored(database_id, tid, c, live_from_row_len(c, f), 0);
    }
    let twin_was_there = txn
        .open_table(TEXT_LIVE_BY_FROM)
        .map_err(|e| redb_err("open text live_by_from", e))?
        .insert((database_id, tid, c, f, sys_from, s), 0u8)
        .map_err(|e| redb_err("write text live_by_from", e))?
        .is_some();
    if !twin_was_there {
        delta.stored(database_id, tid, c, live_by_from_row_len(c, f), 0);
    }
    Ok(())
}

/// Drop the recorded start of `surrogate`'s live version in `index`.
/// `delta` records the bytes the rows held. Returns the start it held.
pub(crate) fn clear_live(
    txn: &WriteTransaction,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    surrogate: Surrogate,
    delta: &mut StorageDelta,
) -> crate::Result<Option<i64>> {
    let (c, f, s) = (index.collection(), index.field_key(), surrogate.as_u32());
    let prior = txn
        .open_table(TEXT_LIVE_FROM)
        .map_err(|e| redb_err("open text live_from", e))?
        .remove((database_id, tid, c, f, s))
        .map_err(|e| redb_err("remove text live_from", e))?
        .map(|v| v.value());
    if let Some(sys_from) = prior {
        delta.removed(database_id, tid, c, live_from_row_len(c, f), 0);
        let twin = txn
            .open_table(TEXT_LIVE_BY_FROM)
            .map_err(|e| redb_err("open text live_by_from", e))?
            .remove((database_id, tid, c, f, sys_from, s))
            .map_err(|e| redb_err("remove text live_by_from", e))?
            .is_some();
        if twin {
            delta.removed(database_id, tid, c, live_by_from_row_len(c, f), 0);
        }
    }
    Ok(prior)
}

/// Store an ended version of `surrogate` in `index`. `delta` records the
/// bytes the row adds.
pub(crate) fn put_ended(
    txn: &WriteTransaction,
    (database_id, tid): (u64, u64),
    index: IndexScope<'_>,
    version: &EndedVersion,
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    let bytes = zerompk::to_msgpack_vec(&version.tokens).map_err(|e| crate::Error::Codec {
        detail: format!("text history tokens encode: {e}"),
    })?;
    let (c, f) = (index.collection(), index.field_key());
    let old = txn
        .open_table(TEXT_ENDED)
        .map_err(|e| redb_err("open text ended", e))?
        .insert(
            (
                database_id,
                tid,
                c,
                f,
                version.sys_to,
                version.surrogate.as_u32(),
                version.sys_from,
            ),
            bytes.as_slice(),
        )
        .map_err(|e| redb_err("write text ended", e))?
        .map(|prior| prior.value().len());
    delta.replaced(database_id, tid, c, ended_key_len(c, f), old, bytes.len());
    Ok(())
}

/// Remove and return the version of `surrogate` in `index` that ended at
/// `sys_to`: the undo of the write that ended it. `delta` records the bytes
/// the row held.
pub(crate) fn take_ended_at(
    txn: &WriteTransaction,
    (database_id, tid): (u64, u64),
    index: IndexScope<'_>,
    surrogate: Surrogate,
    sys_to: i64,
    delta: &mut StorageDelta,
) -> crate::Result<Option<EndedVersion>> {
    let (c, f, s) = (index.collection(), index.field_key(), surrogate.as_u32());
    let mut table = txn
        .open_table(TEXT_ENDED)
        .map_err(|e| redb_err("open text ended", e))?;
    let lo = (database_id, tid, c, f, sys_to, s, i64::MIN);
    let hi = (database_id, tid, c, f, sys_to, s, i64::MAX);
    let found: Option<(i64, Vec<u8>)> = {
        let mut range = table
            .range(lo..=hi)
            .map_err(|e| redb_err("range text ended", e))?;
        match range.next() {
            Some(entry) => {
                let (key, value) = entry.map_err(|e| redb_err("read text ended", e))?;
                Some((key.value().6, value.value().to_vec()))
            }
            None => None,
        }
    };
    let Some((sys_from, bytes)) = found else {
        return Ok(None);
    };
    table
        .remove((database_id, tid, c, f, sys_to, s, sys_from))
        .map_err(|e| redb_err("remove text ended", e))?;
    delta.removed(database_id, tid, c, ended_key_len(c, f), bytes.len());
    Ok(Some(EndedVersion {
        surrogate,
        sys_from,
        sys_to,
        tokens: decode_tokens(&bytes)?,
    }))
}

/// The view of `index` at cut `at`. Cost is the number of writes after `at`
/// no purge dropped yet.
pub(crate) fn cut_rows<R, E>(
    live_by_from: &R,
    ended: &E,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    at: i64,
) -> crate::Result<CutRows>
where
    R: ReadableTable<(u64, u64, &'static str, &'static str, i64, u32), u8>,
    E: ReadableTable<(u64, u64, &'static str, &'static str, i64, u32, i64), &'static [u8]>,
{
    let (c, f) = (index.collection(), index.field_key());
    let mut rows = CutRows::default();
    let Some(after) = at.checked_add(1) else {
        return Ok(rows);
    };
    for entry in live_by_from
        .range((database_id, tid, c, f, after, 0)..=(database_id, tid, c, f, i64::MAX, u32::MAX))
        .map_err(|e| redb_err("range text live_by_from", e))?
    {
        let (key, _) = entry.map_err(|e| redb_err("read text live_by_from", e))?;
        rows.hidden.insert(Surrogate::new(key.value().5));
    }
    for entry in ended
        .range(
            (database_id, tid, c, f, after, 0, i64::MIN)
                ..=(database_id, tid, c, f, i64::MAX, u32::MAX, i64::MAX),
        )
        .map_err(|e| redb_err("range text ended", e))?
    {
        let (key, value) = entry.map_err(|e| redb_err("read text ended", e))?;
        let (_, _, _, _, _, surrogate, sys_from) = key.value();
        if sys_from <= at {
            let doc_id = Surrogate::new(surrogate);
            rows.hidden.insert(doc_id);
            rows.docs.push(StagedDoc {
                doc_id,
                tokens: decode_tokens(value.value())?,
            });
        }
    }
    Ok(rows)
}

/// Drop every history row a cut at or above its index's horizon cannot read.
/// `horizon_of(database, tenant, collection)` names the horizon, `None` to
/// keep the collection's whole history. `delta` records the bytes the rows
/// held. Returns the number of ended versions dropped.
pub(crate) fn purge_below(
    txn: &WriteTransaction,
    horizon_of: &dyn Fn(u64, u64, &str) -> Option<i64>,
    delta: &mut StorageDelta,
) -> crate::Result<usize> {
    let mut ended = txn
        .open_table(TEXT_ENDED)
        .map_err(|e| redb_err("open text ended", e))?;
    let mut dropped: Vec<EndedKey> = Vec::new();
    for entry in ended.iter().map_err(|e| redb_err("scan text ended", e))? {
        let (key, _) = entry.map_err(|e| redb_err("read text ended", e))?;
        let (d, t, c, f, to, s, from) = key.value();
        if horizon_of(d, t, c).is_some_and(|horizon| to <= horizon) {
            dropped.push((d, t, c.to_string(), f.to_string(), to, s, from));
        }
    }
    remove_ended(&mut ended, &dropped, delta)?;
    drop(ended);

    let mut by_from = txn
        .open_table(TEXT_LIVE_BY_FROM)
        .map_err(|e| redb_err("open text live_by_from", e))?;
    let mut settled: Vec<LiveKey> = Vec::new();
    for entry in by_from
        .iter()
        .map_err(|e| redb_err("scan text live_by_from", e))?
    {
        let (key, _) = entry.map_err(|e| redb_err("read text live_by_from", e))?;
        let (d, t, c, f, from, s) = key.value();
        if horizon_of(d, t, c).is_some_and(|horizon| from <= horizon) {
            settled.push((d, t, c.to_string(), f.to_string(), from, s));
        }
    }
    remove_live(txn, &mut by_from, &settled, delta)?;
    Ok(dropped.len())
}

/// An owned `text.ended` key.
type EndedKey = (u64, u64, String, String, i64, u32, i64);
/// An owned `text.live_by_from` key.
type LiveKey = (u64, u64, String, String, i64, u32);

/// Remove the `keys` rows of `text.ended`, and record the bytes they held.
fn remove_ended(
    ended: &mut redb::Table<'_, TextEndedKey, &'static [u8]>,
    keys: &[EndedKey],
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    for (d, t, c, f, to, s, from) in keys {
        let removed = ended
            .remove((*d, *t, c.as_str(), f.as_str(), *to, *s, *from))
            .map_err(|e| redb_err("remove text ended", e))?
            .map(|prior| prior.value().len());
        if let Some(len) = removed {
            delta.removed(*d, *t, c, ended_key_len(c, f), len);
        }
    }
    Ok(())
}

/// Remove the `keys` rows of `text.live_by_from` and their `text.live_from`
/// twins, and record the bytes they held.
fn remove_live(
    txn: &WriteTransaction,
    by_from: &mut redb::Table<'_, (u64, u64, &'static str, &'static str, i64, u32), u8>,
    keys: &[LiveKey],
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    let mut live = txn
        .open_table(TEXT_LIVE_FROM)
        .map_err(|e| redb_err("open text live_from", e))?;
    for (d, t, c, f, from, s) in keys {
        let twin = by_from
            .remove((*d, *t, c.as_str(), f.as_str(), *from, *s))
            .map_err(|e| redb_err("remove text live_by_from", e))?
            .is_some();
        if twin {
            delta.removed(*d, *t, c, live_by_from_row_len(c, f), 0);
        }
        let start = live
            .remove((*d, *t, c.as_str(), f.as_str(), *s))
            .map_err(|e| redb_err("remove text live_from", e))?
            .is_some();
        if start {
            delta.removed(*d, *t, c, live_from_row_len(c, f), 0);
        }
    }
    Ok(())
}

/// Drop every history row `owner` covers: the indexes are dropped or
/// emptied. `delta` records the bytes the rows held.
pub(crate) fn drop_owned(
    txn: &WriteTransaction,
    owner: KeyOwner<'_>,
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    let mut ended = txn
        .open_table(TEXT_ENDED)
        .map_err(|e| redb_err("open text ended", e))?;
    let mut keys: Vec<EndedKey> = Vec::new();
    for entry in ended.iter().map_err(|e| redb_err("scan text ended", e))? {
        let (key, _) = entry.map_err(|e| redb_err("read text ended", e))?;
        let (d, t, c, f, to, s, from) = key.value();
        if owner.owns(d, t, c, Some(f)) {
            keys.push((d, t, c.to_string(), f.to_string(), to, s, from));
        }
    }
    remove_ended(&mut ended, &keys, delta)?;
    drop(ended);

    let mut by_from = txn
        .open_table(TEXT_LIVE_BY_FROM)
        .map_err(|e| redb_err("open text live_by_from", e))?;
    let mut live_keys: Vec<LiveKey> = Vec::new();
    for entry in by_from
        .iter()
        .map_err(|e| redb_err("scan text live_by_from", e))?
    {
        let (key, _) = entry.map_err(|e| redb_err("read text live_by_from", e))?;
        let (d, t, c, f, from, s) = key.value();
        if owner.owns(d, t, c, Some(f)) {
            live_keys.push((d, t, c.to_string(), f.to_string(), from, s));
        }
    }
    remove_live(txn, &mut by_from, &live_keys, delta)
}

fn decode_tokens(bytes: &[u8]) -> crate::Result<Vec<String>> {
    zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Codec {
        detail: format!("text history tokens decode: {e}"),
    })
}
