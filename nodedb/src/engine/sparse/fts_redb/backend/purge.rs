//! Structural collection and tenant drops.
//!
//! Each table is scanned by tuple range and every matching entry is
//! removed; no lexical-prefix scans appear here. `purge_tenant` performs
//! the drop in a single write transaction so the teardown is atomic
//! across tables.

use redb::ReadableTable;

use super::core::RedbFtsBackend;
use super::shared::{MAX_COLLECTION, MAX_SUBKEY, redb_err};
use crate::engine::sparse::fts_redb::tables::{DOC_LENGTHS, INDEX_META, POSTINGS, SEGMENTS, STATS};

pub(super) fn collection(backend: &RedbFtsBackend, tid: u32, coll: &str) -> crate::Result<usize> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("purge write txn", e))?;
    let mut removed = 0;

    {
        let mut table = write_txn
            .open_table(POSTINGS)
            .map_err(|e| redb_err("open postings", e))?;
        let keys: Vec<String> = table
            .range((tid, coll, "")..=(tid, coll, MAX_SUBKEY))
            .map_err(|e| redb_err("postings range", e))?
            .filter_map(|r| r.ok().map(|(k, _)| k.value().2.to_string()))
            .collect();
        removed += keys.len();
        for term in &keys {
            let _ = table.remove((tid, coll, term.as_str()));
        }
    }

    {
        // DOC_LENGTHS is keyed by (u32, &str, u32) — use numeric range bounds.
        let mut table = write_txn
            .open_table(DOC_LENGTHS)
            .map_err(|e| redb_err("open doc_lengths", e))?;
        let surrogates: Vec<u32> = table
            .range((tid, coll, 0u32)..=(tid, coll, u32::MAX))
            .map_err(|e| redb_err("doc_lengths range", e))?
            .filter_map(|r| r.ok().map(|(k, _)| k.value().2))
            .collect();
        removed += surrogates.len();
        for s in surrogates {
            let _ = table.remove((tid, coll, s));
        }
    }

    {
        let mut table = write_txn
            .open_table(INDEX_META)
            .map_err(|e| redb_err("open index_meta", e))?;
        let keys: Vec<String> = table
            .range((tid, coll, "")..=(tid, coll, MAX_SUBKEY))
            .map_err(|e| redb_err("meta range", e))?
            .filter_map(|r| r.ok().map(|(k, _)| k.value().2.to_string()))
            .collect();
        for sub in &keys {
            let _ = table.remove((tid, coll, sub.as_str()));
        }
    }

    {
        let mut table = write_txn
            .open_table(STATS)
            .map_err(|e| redb_err("open stats", e))?;
        let _ = table.remove((tid, coll));
    }

    {
        let mut table = write_txn
            .open_table(SEGMENTS)
            .map_err(|e| redb_err("open segments", e))?;
        let ids: Vec<String> = table
            .range((tid, coll, "")..=(tid, coll, MAX_SUBKEY))
            .map_err(|e| redb_err("segments range", e))?
            .filter_map(|r| r.ok().map(|(k, _)| k.value().2.to_string()))
            .collect();
        removed += ids.len();
        for id in &ids {
            let _ = table.remove((tid, coll, id.as_str()));
        }
    }

    write_txn
        .commit()
        .map_err(|e| redb_err("commit purge", e))?;
    Ok(removed)
}

pub(super) fn tenant(backend: &RedbFtsBackend, tid: u32) -> crate::Result<usize> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("purge_tenant write txn", e))?;
    let mut removed = 0;

    removed += drop_str_triple_range(&write_txn, POSTINGS, tid)?;
    removed += drop_doc_lengths_tenant(&write_txn, tid)?;
    let _ = drop_str_triple_range(&write_txn, INDEX_META, tid)?;

    {
        let mut stats = write_txn
            .open_table(STATS)
            .map_err(|e| redb_err("open stats", e))?;
        let colls: Vec<String> = stats
            .range((tid, "")..=(tid, MAX_COLLECTION))
            .map_err(|e| redb_err("stats range", e))?
            .filter_map(|r| r.ok().map(|(k, _)| k.value().1.to_string()))
            .collect();
        for c in &colls {
            let _ = stats.remove((tid, c.as_str()));
        }
    }

    removed += drop_str_triple_range(&write_txn, SEGMENTS, tid)?;

    write_txn
        .commit()
        .map_err(|e| redb_err("commit purge_tenant", e))?;
    Ok(removed)
}

/// Delete every `(tid, *, *)` row from a `TableDefinition<(u32, &str, &str), &[u8]>`.
fn drop_str_triple_range(
    txn: &redb::WriteTransaction,
    def: redb::TableDefinition<(u32, &str, &str), &[u8]>,
    tid: u32,
) -> crate::Result<usize> {
    let mut table = txn
        .open_table(def)
        .map_err(|e| redb_err("open triple table", e))?;
    let keys: Vec<(String, String)> = table
        .range((tid, "", "")..=(tid, MAX_COLLECTION, MAX_SUBKEY))
        .map_err(|e| redb_err("triple range", e))?
        .filter_map(|r| {
            r.ok().map(|(k, _)| {
                let (_, c, s) = k.value();
                (c.to_string(), s.to_string())
            })
        })
        .collect();
    let n = keys.len();
    for (c, s) in &keys {
        let _ = table.remove((tid, c.as_str(), s.as_str()));
    }
    Ok(n)
}

/// Delete every `(tid, *, *)` row from DOC_LENGTHS (keyed by `(u32, &str, u32)`).
fn drop_doc_lengths_tenant(txn: &redb::WriteTransaction, tid: u32) -> crate::Result<usize> {
    let mut table = txn
        .open_table(DOC_LENGTHS)
        .map_err(|e| redb_err("open doc_lengths", e))?;
    let keys: Vec<(String, u32)> = table
        .range((tid, "", 0u32)..=(tid, MAX_COLLECTION, u32::MAX))
        .map_err(|e| redb_err("doc_lengths tenant range", e))?
        .filter_map(|r| {
            r.ok().map(|(k, _)| {
                let (_, c, s) = k.value();
                (c.to_string(), s)
            })
        })
        .collect();
    let n = keys.len();
    for (c, s) in &keys {
        let _ = table.remove((tid, c.as_str(), *s));
    }
    Ok(n)
}
