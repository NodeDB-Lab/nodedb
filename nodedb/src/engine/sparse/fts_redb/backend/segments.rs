// SPDX-License-Identifier: BUSL-1.1

//! LSM segment blobs against `SEGMENTS`
//! keyed by `(database_id, tenant_id, collection, field, segment_id)`.

use nodedb_fts::IndexScope;
use redb::ReadableDatabase;
use redb::ReadableTable as _;

use super::core::RedbFtsBackend;
use super::shared::redb_err;
use crate::engine::sparse::btree::storage_bytes::SPARSE_COUNTER;
use crate::engine::sparse::fts_redb::bytes::{put_segment_row, remove_segment_row};
use crate::engine::sparse::fts_redb::keys::{KeyOwner, segment_key};
use crate::engine::sparse::fts_redb::scan;
use crate::engine::sparse::fts_redb::tables::SEGMENTS;
use crate::engine::storage_bytes::StorageDelta;
use crate::storage::quarantine::CollectionScope;
use crate::storage::quarantine::engines::{FtsSegmentRef, validate_fts_segment_bytes};

pub(super) fn write(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    segment_id: &str,
    data: &[u8],
) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    let mut delta = StorageDelta::default();
    {
        let mut table = write_txn
            .open_table(SEGMENTS)
            .map_err(|e| redb_err("open segments", e))?;
        put_segment_row(
            &mut table,
            segment_key(database_id, tid, index, segment_id),
            data,
            &mut delta,
        )
        .map_err(|e| redb_err("insert segment", e))?;
    }
    delta.apply(&write_txn, SPARSE_COUNTER)?;
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}

pub(super) fn read(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    segment_id: &str,
) -> crate::Result<Option<Vec<u8>>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(SEGMENTS)
        .map_err(|e| redb_err("open segments", e))?;
    let bytes = match table.get(segment_key(database_id, tid, index, segment_id)) {
        Ok(Some(val)) => val.value().to_vec(),
        Ok(None) => return Ok(None),
        Err(e) => return Err(redb_err("get segment", e)),
    };

    if let Some(reg) = &backend.quarantine_registry {
        let scope = CollectionScope::new(database_id, tid, index.collection());
        let segment = FtsSegmentRef {
            scope: &scope,
            field: index.field_name(),
            segment_id,
        };
        let validated = validate_fts_segment_bytes(reg, bytes, &segment).map_err(|e| {
            crate::Error::SegmentCorrupted {
                detail: e.to_string(),
            }
        })?;
        Ok(Some(validated))
    } else {
        Ok(Some(bytes))
    }
}

pub(super) fn list(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
) -> crate::Result<Vec<String>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(SEGMENTS)
        .map_err(|e| redb_err("open segments", e))?;
    let keys = scan::str_keys(&table, KeyOwner::index(database_id, tid, index))
        .map_err(|e| redb_err("segments range", e))?;
    Ok(keys.into_iter().map(|(_, _, id)| id).collect())
}

pub(super) fn remove(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    segment_id: &str,
) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    let mut delta = StorageDelta::default();
    {
        let mut table = write_txn
            .open_table(SEGMENTS)
            .map_err(|e| redb_err("open segments", e))?;
        // The error propagates so a corrupt index or aborted txn surfaces.
        // An absent key is a no-op, not an error.
        remove_segment_row(
            &mut table,
            segment_key(database_id, tid, index, segment_id),
            &mut delta,
        )
        .map_err(|e| redb_err("remove segment", e))?;
    }
    delta.apply(&write_txn, SPARSE_COUNTER)?;
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}

/// Inputs to [`compact_commit`]: the merged-segment write plus the source
/// segment ids to remove, scoped to one `(database, tenant, index)`.
pub struct CompactCommit<'a> {
    /// Owning database id.
    pub database_id: u64,
    /// Owning tenant id (raw storage form).
    pub tid: u64,
    /// Index whose segments are being compacted.
    pub index: IndexScope<'a>,
    /// Id of the new merged segment to write.
    pub new_segment_id: &'a str,
    /// Bytes of the new merged segment.
    pub new_segment_data: &'a [u8],
    /// Ids of the source segments to remove after the merged write.
    pub merged_ids: &'a [String],
}

/// Atomically register a new merged segment and remove the source segments
/// that were merged into it.
///
/// Performing the write and all removals in one redb write transaction
/// ensures the operation is crash-safe: a crash mid-compaction leaves the
/// old segments intact (a subsequent maintenance cycle will retry), and
/// there is never a window where both the old and the new segments coexist
/// on a reader that opens a concurrent read transaction.
pub(super) fn compact_commit(
    backend: &RedbFtsBackend,
    params: CompactCommit<'_>,
) -> crate::Result<()> {
    let CompactCommit {
        database_id,
        tid,
        index,
        new_segment_id,
        new_segment_data,
        merged_ids,
    } = params;
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("compact txn", e))?;
    let mut delta = StorageDelta::default();
    {
        let mut table = write_txn
            .open_table(SEGMENTS)
            .map_err(|e| redb_err("open segments for compact", e))?;
        put_segment_row(
            &mut table,
            segment_key(database_id, tid, index, new_segment_id),
            new_segment_data,
            &mut delta,
        )
        .map_err(|e| redb_err("insert merged segment", e))?;
        for id in merged_ids {
            // Propagate remove errors so a failed source-segment removal aborts
            // the entire transaction; otherwise we would commit a state in
            // which both old and new segments are visible to readers, causing
            // double-counted postings during BM25 scoring.
            remove_segment_row(
                &mut table,
                segment_key(database_id, tid, index, id.as_str()),
                &mut delta,
            )
            .map_err(|e| redb_err("remove merged source segment", e))?;
        }
    }
    delta.apply(&write_txn, SPARSE_COUNTER)?;
    write_txn
        .commit()
        .map_err(|e| redb_err("compact txn commit", e))?;
    Ok(())
}

/// Enumerate every `(database_id, tid, collection, field)` index that has at
/// least one segment stored in the SEGMENTS table.
///
/// Used by maintenance to discover which indexes need FTS LSM compaction
/// without requiring the executor to maintain a separate registry of
/// FTS-indexed collections.
pub(super) fn list_all_indexes(
    backend: &RedbFtsBackend,
) -> crate::Result<Vec<(u64, u64, String, String)>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(SEGMENTS)
        .map_err(|e| redb_err("open segments", e))?;

    let mut indexes: Vec<(u64, u64, String, String)> = Vec::new();
    for entry in table.iter().map_err(|e| redb_err("iter segments", e))? {
        let (key, _) = entry.map_err(|e| redb_err("next segment key", e))?;
        let (database_id, tid, collection, field, _) = key.value();
        let same_as_last = indexes.last().is_some_and(|(d, t, c, f)| {
            *d == database_id && *t == tid && c == collection && f == field
        });
        if !same_as_last {
            indexes.push((database_id, tid, collection.to_string(), field.to_string()));
        }
    }
    Ok(indexes)
}
