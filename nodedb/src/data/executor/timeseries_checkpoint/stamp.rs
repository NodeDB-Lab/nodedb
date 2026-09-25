// SPDX-License-Identifier: BUSL-1.1

//! The replay stamp of a timeseries collection.
//!
//! ## What it names
//!
//! A timeseries ingest is an append, so restart replay must apply a record
//! exactly once. [`TsReplayStamp::rows`] names the records whose rows a
//! partition holds or a truncate removed. Replay skips exactly those. A record
//! still on its way to the core when a flush ran is not named, even when a
//! higher LSN is, so it replays once.
//!
//! [`TsReplayStamp::truncates`] names the truncates that took effect. Replay
//! skips a named truncate: re-applying it would remove rows written after it.
//!
//! ## Where it lives
//!
//! - Every partition directory carries the collection stamp as of its flush,
//!   in `replay.stamp`. It is written before `partition.meta`, the partition's
//!   commit point, so a committed partition always carries the stamp naming
//!   its rows.
//! - The collection directory carries `replay.stamp` too. A truncate writes it
//!   into the fresh directory it leaves behind. Retention writes it before it
//!   removes a partition, so the names of the removed partition survive it.
//!
//! The collection stamp is the union of every copy. Stamps only grow, so the
//! union never names a record no copy names.
//!
//! ## Where a flush's stamp comes from
//!
//! A live flush takes the core's applied-prefix stamp, after the ingest that
//! filled the memtable noted its record. Restart replay applies the WAL in
//! LSN order, and the core's floor is already raised to the WAL end, so the
//! core stamp would name records replay has not reached. A flush during
//! restart replay therefore names the records through the replay cursor
//! instead: every record the timeseries pass has passed.

use std::path::Path;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::partition_registry::PartitionRegistry;
use crate::types::replay_stamp::ReplayStamp;
use crate::types::{DatabaseId, Lsn, TenantId};

/// File name of a stamp, in a partition directory and in a collection
/// directory alike. Neither starts with `ts-`, so the partition scan and the
/// orphan sweep pass over it.
pub(in crate::data::executor) const TS_STAMP_FILE: &str = "replay.stamp";

/// Leading byte of an encoded stamp file.
const TS_STAMP_FORMAT_VERSION: u8 = 1;

/// The key of one timeseries collection on a core.
pub(in crate::data::executor) type TsCollectionKey = (DatabaseId, TenantId, String);

/// What a timeseries collection's durable state holds.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub(in crate::data::executor) struct TsReplayStamp {
    /// Records whose rows a partition holds or a truncate removed.
    pub rows: ReplayStamp,
    /// Truncates of the collection that took effect.
    pub truncates: ReplayStamp,
}

impl TsReplayStamp {
    /// A stamp naming what `self` or `other` names.
    pub(in crate::data::executor) fn union(&self, other: &TsReplayStamp) -> TsReplayStamp {
        TsReplayStamp {
            rows: self.rows.union(&other.rows),
            truncates: self.truncates.union(&other.truncates),
        }
    }
}

fn stamp_error(path: &Path, action: &str, detail: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "timeseries".to_string(),
        detail: format!(
            "replay stamp: failed to {action} {}: {detail}",
            path.display()
        ),
    }
}

/// Write `stamp` into `dir` durably: data fsynced before the rename, `dir`
/// fsynced after it.
pub(in crate::data::executor) fn write_ts_stamp(
    dir: &Path,
    stamp: &TsReplayStamp,
) -> crate::Result<()> {
    let mut bytes = vec![TS_STAMP_FORMAT_VERSION];
    let body = zerompk::to_msgpack_vec(stamp)
        .map_err(|e| stamp_error(&dir.join(TS_STAMP_FILE), "encode", &e))?;
    bytes.extend_from_slice(&body);
    nodedb_wal::segment::atomic_write_fsync(dir, TS_STAMP_FILE, &bytes)
        .map_err(|e| stamp_error(&dir.join(TS_STAMP_FILE), "write", &e))
}

/// Decode a stamp file's bytes. `path` names the file in errors.
pub(in crate::data::executor) fn decode_ts_stamp(
    path: &Path,
    bytes: &[u8],
) -> crate::Result<TsReplayStamp> {
    let Some((&version, body)) = bytes.split_first() else {
        return Err(stamp_error(path, "decode", &"the file is empty"));
    };
    if version != TS_STAMP_FORMAT_VERSION {
        return Err(stamp_error(
            path,
            "decode",
            &format!("format version {version}, expected {TS_STAMP_FORMAT_VERSION}"),
        ));
    }
    let stamp: TsReplayStamp =
        zerompk::from_msgpack(body).map_err(|e| stamp_error(path, "decode", &e))?;
    stamp
        .rows
        .validate()
        .map_err(|e| stamp_error(path, "validate the row stamp of", &e))?;
    stamp
        .truncates
        .validate()
        .map_err(|e| stamp_error(path, "validate the truncate stamp of", &e))?;
    Ok(stamp)
}

/// Read the stamp in `dir`. A missing file names nothing.
pub(in crate::data::executor) fn read_ts_stamp(dir: &Path) -> crate::Result<Option<TsReplayStamp>> {
    let path = dir.join(TS_STAMP_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(stamp_error(&path, "read", &e)),
    };
    decode_ts_stamp(&path, &bytes).map(Some)
}

/// The collection stamp of `ts_dir`: the union of its own file and the file
/// of every committed partition `registry` names.
pub(in crate::data::executor) fn read_collection_ts_stamp(
    ts_dir: &Path,
    registry: &PartitionRegistry,
) -> crate::Result<TsReplayStamp> {
    let mut stamp = read_ts_stamp(ts_dir)?.unwrap_or_default();
    for (_, entry) in registry.iter() {
        if let Some(partition) = read_ts_stamp(&ts_dir.join(&entry.dir_name))? {
            stamp = stamp.union(&partition);
        }
    }
    Ok(stamp)
}

impl CoreLoop {
    /// Whether restart replay skips the timeseries record at `lsn` for `key`:
    /// the collection stamp names it. A committed-redo apply never skips
    /// (`replay_watermark_skips`).
    pub(in crate::data::executor) fn ts_replay_skips(
        &self,
        key: &TsCollectionKey,
        lsn: u64,
    ) -> bool {
        self.replay_watermark_skips(self.ts_rows_named(key, lsn))
    }

    /// Whether `key`'s collection stamp names the record at `lsn` as held by
    /// a partition or removed by a truncate.
    pub(in crate::data::executor) fn ts_rows_named(&self, key: &TsCollectionKey, lsn: u64) -> bool {
        self.ts_replay_stamps
            .get(key)
            .is_some_and(|stamp| stamp.rows.skips(lsn))
    }

    /// Whether `key`'s collection stamp names the truncate at `lsn` as taken
    /// effect.
    pub(in crate::data::executor) fn ts_truncate_named(
        &self,
        key: &TsCollectionKey,
        lsn: u64,
    ) -> bool {
        self.ts_replay_stamps
            .get(key)
            .is_some_and(|stamp| stamp.truncates.skips(lsn))
    }

    /// The records this core applied, as a flush or truncate writing now
    /// states them. See the module docs for the restart-replay cursor.
    pub(in crate::data::executor) fn ts_applied_stamp(&self) -> crate::Result<ReplayStamp> {
        match self.ts_replay_cursor {
            Some(through) => Ok(ReplayStamp::through(through)),
            None => Ok(self.floors.applied_prefix.stamp()?),
        }
    }

    /// The stamp a flush of `key` writing now carries: the collection stamp,
    /// plus every record this core applied.
    pub(in crate::data::executor) fn ts_flush_stamp(
        &self,
        key: &TsCollectionKey,
    ) -> crate::Result<TsReplayStamp> {
        let applied = TsReplayStamp {
            rows: self.ts_applied_stamp()?,
            truncates: ReplayStamp::default(),
        };
        Ok(self
            .ts_replay_stamps
            .get(key)
            .map_or_else(|| applied.clone(), |stamp| stamp.union(&applied)))
    }

    /// Record that a timeseries record at `lsn` applied. A flush after this
    /// names the record. Called once the record's rows landed and before any
    /// flush that can write them.
    pub(in crate::data::executor) fn note_ts_record_applied(&mut self, lsn: u64) {
        self.floors.applied_prefix.note_applied(Lsn::new(lsn));
        if let Some(cursor) = self.ts_replay_cursor.as_mut() {
            *cursor = (*cursor).max(lsn);
        }
    }

    /// Write `key`'s collection stamp into its collection directory. Called
    /// before a partition directory is removed, so the stamp names that
    /// partition's records after it is gone.
    pub(in crate::data::executor) fn persist_ts_collection_stamp(
        &self,
        key: &TsCollectionKey,
    ) -> crate::Result<()> {
        let Some(stamp) = self.ts_replay_stamps.get(key) else {
            return Ok(());
        };
        let (database_id, tenant_id, collection) = key;
        let dir = crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
            &self.data_dir,
            database_id.as_u64(),
            tenant_id.as_u64(),
            collection,
        );
        write_ts_stamp(&dir, stamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(prefix: u64, lsns: &[u64]) -> ReplayStamp {
        let mut stamp = ReplayStamp::through(prefix);
        for &lsn in lsns {
            stamp = stamp.union(&ReplayStamp::naming(lsn));
        }
        stamp
    }

    #[test]
    fn a_stamp_file_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let stamp = TsReplayStamp {
            rows: named(5, &[9, 12]),
            truncates: named(0, &[7]),
        };
        write_ts_stamp(dir.path(), &stamp).expect("write");
        assert_eq!(read_ts_stamp(dir.path()).expect("read"), Some(stamp));
        let empty = tempfile::tempdir().expect("tempdir");
        assert_eq!(read_ts_stamp(empty.path()).expect("read"), None);
    }

    #[test]
    fn a_corrupt_stamp_file_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join(TS_STAMP_FILE), [9u8, 1, 2]).expect("write");
        assert!(read_ts_stamp(dir.path()).is_err());
        std::fs::write(dir.path().join(TS_STAMP_FILE), []).expect("write");
        assert!(read_ts_stamp(dir.path()).is_err());
    }

    #[test]
    fn the_collection_stamp_is_the_union_of_every_copy() {
        use crate::engine::timeseries::partition_registry::PartitionEntry;

        let dir = tempfile::tempdir().expect("tempdir");
        write_ts_stamp(
            dir.path(),
            &TsReplayStamp {
                rows: named(3, &[]),
                truncates: named(0, &[2]),
            },
        )
        .expect("collection stamp");
        let mut registry = PartitionRegistry::new(
            nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
        );
        for (name, stamp) in [("ts-a", named(3, &[8])), ("ts-b", named(3, &[6]))] {
            let partition = dir.path().join(name);
            std::fs::create_dir_all(&partition).expect("partition dir");
            write_ts_stamp(
                &partition,
                &TsReplayStamp {
                    rows: stamp,
                    truncates: ReplayStamp::default(),
                },
            )
            .expect("partition stamp");
            registry.insert_partition(PartitionEntry {
                meta: nodedb_types::timeseries::PartitionMeta {
                    min_ts: 0,
                    max_ts: 0,
                    row_count: 1,
                    size_bytes: 0,
                    schema_version: 1,
                    state: nodedb_types::timeseries::PartitionState::Sealed,
                    interval_ms: 0,
                    last_flushed_wal_lsn: 0,
                    column_stats: std::collections::HashMap::new(),
                    max_system_ts: 0,
                },
                dir_name: name.to_string(),
            });
        }
        let stamp = read_collection_ts_stamp(dir.path(), &registry).expect("union");
        assert_eq!(stamp.rows, named(3, &[6, 8]));
        assert_eq!(stamp.truncates, named(0, &[2]));
    }
}
