// SPDX-License-Identifier: BUSL-1.1

//! Redb rows of the inverted index's system-time history.
//!
//! An ended version's analyzed tokens live in `TEXT_ENDED` keyed by its
//! end, so a read at an earlier cut still matches and scores it. A live
//! version records its start in `TEXT_LIVE_FROM` and its ordered twin
//! `TEXT_LIVE_BY_FROM` while a retained snapshot can read below it. A read
//! at a cut ranges over the rows written after it. The cut, write, and purge
//! logic lives in `nodedb_fts::history`, over the stores in `history_store`.

use nodedb_fts::IndexScope;
use nodedb_fts::history::{EndedKey, EndedRow, EndedVersion, LiveKey, LiveRow, TextCut};
use nodedb_types::Surrogate;
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

/// A `text.live_from` key.
pub(crate) type LiveFromKey = (u64, u64, &'static str, &'static str, u32);
/// A `text.live_by_from` key.
pub(crate) type LiveByFromKey = (u64, u64, &'static str, &'static str, i64, u32);

/// The recorded start of `surrogate`'s live version in `index`, read from a
/// `text.live_from` table. `None` when every retained cut reads it.
pub(crate) fn live_from<T: ReadableTable<LiveFromKey, i64>>(
    table: &T,
    (database_id, tid): (u64, u64),
    index: IndexScope<'_>,
    surrogate: Surrogate,
) -> crate::Result<Option<i64>> {
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

/// The documents of `index` whose live version starts after `cut`, read
/// from a `text.live_by_from` table.
pub(crate) fn live_started_after<T: ReadableTable<LiveByFromKey, u8>>(
    table: &T,
    (database_id, tid): (u64, u64),
    index: IndexScope<'_>,
    cut: TextCut,
) -> crate::Result<Vec<Surrogate>> {
    let Some(after) = cut.first_after() else {
        return Ok(Vec::new());
    };
    let (c, f) = (index.collection(), index.field_key());
    let mut hidden = Vec::new();
    for entry in table
        .range((database_id, tid, c, f, after, 0)..=(database_id, tid, c, f, i64::MAX, u32::MAX))
        .map_err(|e| redb_err("range text live_by_from", e))?
    {
        let (key, _) = entry.map_err(|e| redb_err("read text live_by_from", e))?;
        hidden.push(Surrogate::new(key.value().5));
    }
    Ok(hidden)
}

/// The ended versions of `index` that `cut` sees, read from a `text.ended`
/// table. Decodes only the tokens of the versions it returns. Cost is the
/// number of versions ended after `cut`.
pub(crate) fn ended_seen_at<T: ReadableTable<TextEndedKey, &'static [u8]>>(
    table: &T,
    (database_id, tid): (u64, u64),
    index: IndexScope<'_>,
    cut: TextCut,
) -> crate::Result<Vec<EndedVersion>> {
    let Some(after) = cut.first_after() else {
        return Ok(Vec::new());
    };
    let (c, f) = (index.collection(), index.field_key());
    let mut seen = Vec::new();
    for entry in table
        .range(
            (database_id, tid, c, f, after, 0, i64::MIN)
                ..=(database_id, tid, c, f, i64::MAX, u32::MAX, i64::MAX),
        )
        .map_err(|e| redb_err("range text ended", e))?
    {
        let (key, value) = entry.map_err(|e| redb_err("read text ended", e))?;
        let (_, _, _, _, sys_to, surrogate, sys_from) = key.value();
        if cut.sees(sys_from, sys_to) {
            seen.push(EndedVersion {
                surrogate: Surrogate::new(surrogate),
                sys_from,
                sys_to,
                tokens: decode_tokens(value.value())?,
            });
        }
    }
    Ok(seen)
}

/// Visit every row of a `text.ended` table, without its tokens.
pub(crate) fn scan_ended<T: ReadableTable<TextEndedKey, &'static [u8]>>(
    table: &T,
    visit: &mut dyn FnMut(EndedRow<'_>),
) -> crate::Result<()> {
    for entry in table.iter().map_err(|e| redb_err("scan text ended", e))? {
        let (key, _) = entry.map_err(|e| redb_err("read text ended", e))?;
        let (d, t, c, f, sys_to, s, sys_from) = key.value();
        visit(EndedRow {
            database_id: d,
            tid: t,
            index: IndexScope::from_key(c, f),
            surrogate: Surrogate::new(s),
            sys_from,
            sys_to,
        });
    }
    Ok(())
}

/// Visit every row of a `text.live_by_from` table.
pub(crate) fn scan_live<T: ReadableTable<LiveByFromKey, u8>>(
    table: &T,
    visit: &mut dyn FnMut(LiveRow<'_>),
) -> crate::Result<()> {
    for entry in table
        .iter()
        .map_err(|e| redb_err("scan text live_by_from", e))?
    {
        let (key, _) = entry.map_err(|e| redb_err("read text live_by_from", e))?;
        let (d, t, c, f, sys_from, s) = key.value();
        visit(LiveRow {
            database_id: d,
            tid: t,
            index: IndexScope::from_key(c, f),
            surrogate: Surrogate::new(s),
            sys_from,
        });
    }
    Ok(())
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
    clear_live(txn, (database_id, tid), index, surrogate, delta)?;
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
    (database_id, tid): (u64, u64),
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

/// Remove the `keys` rows of `text.ended`, and record the bytes they held.
pub(crate) fn remove_ended(
    txn: &WriteTransaction,
    keys: &[EndedKey],
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    let mut ended = txn
        .open_table(TEXT_ENDED)
        .map_err(|e| redb_err("open text ended", e))?;
    for k in keys {
        let (c, f) = (k.collection.as_str(), k.field_key.as_str());
        let key = (
            k.database_id,
            k.tid,
            c,
            f,
            k.sys_to,
            k.surrogate.as_u32(),
            k.sys_from,
        );
        let removed = ended
            .remove(key)
            .map_err(|e| redb_err("remove text ended", e))?
            .map(|prior| prior.value().len());
        if let Some(len) = removed {
            delta.removed(k.database_id, k.tid, c, ended_key_len(c, f), len);
        }
    }
    Ok(())
}

/// Remove the `keys` rows of `text.live_by_from` and their `text.live_from`
/// twins, and record the bytes they held.
pub(crate) fn remove_live(
    txn: &WriteTransaction,
    keys: &[LiveKey],
    delta: &mut StorageDelta,
) -> crate::Result<()> {
    let mut by_from = txn
        .open_table(TEXT_LIVE_BY_FROM)
        .map_err(|e| redb_err("open text live_by_from", e))?;
    let mut live = txn
        .open_table(TEXT_LIVE_FROM)
        .map_err(|e| redb_err("open text live_from", e))?;
    for k in keys {
        let (d, t, s) = (k.database_id, k.tid, k.surrogate.as_u32());
        let (c, f) = (k.collection.as_str(), k.field_key.as_str());
        let twin = by_from
            .remove((d, t, c, f, k.sys_from, s))
            .map_err(|e| redb_err("remove text live_by_from", e))?
            .is_some();
        if twin {
            delta.removed(d, t, c, live_by_from_row_len(c, f), 0);
        }
        let start = live
            .remove((d, t, c, f, s))
            .map_err(|e| redb_err("remove text live_from", e))?
            .is_some();
        if start {
            delta.removed(d, t, c, live_from_row_len(c, f), 0);
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
    let mut ended: Vec<EndedKey> = Vec::new();
    {
        let table = txn
            .open_table(TEXT_ENDED)
            .map_err(|e| redb_err("open text ended", e))?;
        scan_ended(&table, &mut |row| {
            let (c, f) = (row.index.collection(), row.index.field_key());
            if owner.owns(row.database_id, row.tid, c, Some(f)) {
                ended.push(row.to_key());
            }
        })?;
    }
    remove_ended(txn, &ended, delta)?;

    let mut live: Vec<LiveKey> = Vec::new();
    {
        let table = txn
            .open_table(TEXT_LIVE_BY_FROM)
            .map_err(|e| redb_err("open text live_by_from", e))?;
        scan_live(&table, &mut |row| {
            let (c, f) = (row.index.collection(), row.index.field_key());
            if owner.owns(row.database_id, row.tid, c, Some(f)) {
                live.push(row.to_key());
            }
        })?;
    }
    remove_live(txn, &live, delta)
}

fn decode_tokens(bytes: &[u8]) -> crate::Result<Vec<String>> {
    zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Codec {
        detail: format!("text history tokens decode: {e}"),
    })
}
