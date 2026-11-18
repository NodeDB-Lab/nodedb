// SPDX-License-Identifier: BUSL-1.1

//! Undo of the flush a timeseries `TRUNCATE` runs inside a transaction
//! batch.
//!
//! The truncate flushes the memtable first, so every row written before it
//! sits in a partition it can end. That flush files partitions, writes
//! the schema and series catalog files, raises the in-memory collection
//! stamp, and drops the memtable's reservation. Its undo entry precedes the
//! truncate's, so a rollback runs it after the truncate's undo reopened the
//! partitions. It then takes the flush back whole (`revert_flush`): the
//! partitions it filed, and a commit manifest it left with the partitions
//! that manifest names, which an install that failed after the commit
//! leaves. It puts back the registry, both files and the stamp, charges the
//! reservation the memtable held, and puts the memtable back.

use std::path::Path;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::flush_commit::{
    FlushRevert, has_uninstalled_flush, read_manifest, revert_flush,
};
use crate::data::executor::timeseries_checkpoint::schema::TS_SCHEMA_FILE;
use crate::data::executor::timeseries_checkpoint::series_catalog::TS_SERIES_CATALOG_FILE;
use crate::data::executor::timeseries_checkpoint::stamp::{TsCollectionKey, TsReplayStamp};
use crate::engine::timeseries::partition_registry::{PartitionKey, PartitionRegistry};

use super::{TimeseriesIngestUndo, UndoError};

/// What one flush of a truncate inside a transaction batch changed.
pub(in crate::data::executor) struct TimeseriesFlushUndo {
    pub collection_key: TsCollectionKey,
    /// The collection's memtable, caches, ingest timer and reservation
    /// before the flush.
    pub pre_image: TimeseriesIngestUndo,
    /// The partition registry before the flush. `None` when the collection
    /// had none.
    pub registry: Option<PartitionRegistry>,
    /// The partitions the flush filed.
    pub created: Vec<PartitionKey>,
    /// Whether a committed, uninstalled flush was present before the flush.
    pub commit_before: bool,
    /// The in-memory collection stamp before the flush.
    pub stamp: Option<TsReplayStamp>,
    /// The schema file's bytes before the flush. `None` when it was absent.
    pub schema_file: Option<Vec<u8>>,
    /// The series catalog file's bytes before the flush.
    pub catalog_file: Option<Vec<u8>>,
}

impl TimeseriesFlushUndo {
    /// Bytes the entry holds beyond its own size: the memtable copy, the
    /// two file images and the registry's partitions.
    pub(in crate::data::executor) fn held_bytes(&self) -> usize {
        self.pre_image.held_bytes()
            + self.schema_file.as_ref().map_or(0, Vec::len)
            + self.catalog_file.as_ref().map_or(0, Vec::len)
            + self.registry.as_ref().map_or(0, |registry| {
                registry.partition_count() * std::mem::size_of::<PartitionKey>() * 4
            })
    }
}

/// The bytes of the file at `path`, `None` when it is absent.
pub(in crate::data::executor) fn read_optional(path: &Path) -> crate::Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!("read {} before a flush: {e}", path.display()),
        }),
    }
}

/// Put `bytes` back as the file `name` in `dir`, or remove it when `None`.
pub(super) fn restore_file(dir: &Path, name: &str, bytes: Option<&[u8]>) -> crate::Result<()> {
    let storage = |action: &str, e: &dyn std::fmt::Display| crate::Error::Storage {
        engine: "timeseries".into(),
        detail: format!("{action} {}: {e}", dir.join(name).display()),
    };
    match bytes {
        Some(bytes) => nodedb_wal::segment::atomic_write_fsync(dir, name, bytes)
            .map_err(|e| storage("write back", &e)),
        None => match std::fs::remove_file(dir.join(name)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(storage("remove", &e)),
        },
    }
}

impl CoreLoop {
    /// Capture what a flush of `key` changes, before it runs.
    pub(in crate::data::executor) fn capture_ts_flush_undo(
        &self,
        key: &TsCollectionKey,
    ) -> crate::Result<TimeseriesFlushUndo> {
        let dir = self.ts_dir_of(key);
        Ok(TimeseriesFlushUndo {
            collection_key: key.clone(),
            pre_image: self.capture_timeseries_ingest_undo(key),
            registry: self.ts_registries.get(key).cloned(),
            created: Vec::new(),
            commit_before: has_uninstalled_flush(&dir)?,
            stamp: self.ts_replay.stamps.get(key).cloned(),
            schema_file: read_optional(&dir.join(TS_SCHEMA_FILE))?,
            catalog_file: read_optional(&dir.join(TS_SERIES_CATALOG_FILE))?,
        })
    }

    /// Put the in-memory collection stamp of `key` back to `stamp`, removing
    /// it when `None`.
    pub(super) fn restore_ts_stamp(&mut self, key: &TsCollectionKey, stamp: Option<TsReplayStamp>) {
        match stamp {
            Some(stamp) => {
                self.ts_replay.stamps.insert(key.clone(), stamp);
            }
            None => {
                self.ts_replay.stamps.remove(key);
            }
        }
    }

    /// Put the memtable of `key` back under the reservation it held before a
    /// flush dropped or recharged it. `None` leaves it without one.
    pub(super) fn restore_ts_reservation(&mut self, key: &TsCollectionKey, bytes: Option<usize>) {
        self.columnar_memtable_mem.remove(key);
        if let Some(bytes) = bytes {
            let memory = nodedb_mem::ScopedMemory::new(
                self.governor.clone(),
                key.0,
                key.1,
                nodedb_mem::EngineId::Timeseries,
            );
            self.columnar_memtable_mem
                .insert(key.clone(), memory.charge(bytes));
        }
    }

    /// Name in `undo` the partitions its flush filed: those the registry
    /// holds after the flush and its capture lacks.
    pub(in crate::data::executor) fn note_ts_flush_created(&self, undo: &mut TimeseriesFlushUndo) {
        let Some(registry) = self.ts_registries.get(&undo.collection_key) else {
            return;
        };
        undo.created = registry
            .iter()
            .filter(|(key, _)| {
                undo.registry
                    .as_ref()
                    .is_none_or(|before| before.get(key).is_none())
            })
            .map(|(key, _)| key.clone())
            .collect();
    }

    /// Reverse the flush `undo` captured. Every step is fatal on failure:
    /// the collection would hold the rows the rolled-back batch wrote.
    pub(super) fn apply_undo_ts_flush(
        &mut self,
        entry_index: usize,
        undo: TimeseriesFlushUndo,
    ) -> Result<(), UndoError> {
        let TimeseriesFlushUndo {
            collection_key,
            pre_image,
            registry,
            created,
            commit_before,
            stamp,
            schema_file,
            catalog_file,
        } = undo;
        let collection = collection_key.2.clone();
        let failed = |action: &str, e: crate::Error| {
            UndoError::failed(
                entry_index,
                format!("timeseries truncate flush undo of '{collection}': {action}"),
                e,
            )
        };
        let dir = self.ts_dir_of(&collection_key);
        let mut revert = FlushRevert {
            dirs: created
                .iter()
                .map(|partition| partition.dir_name.clone())
                .collect(),
            drop_commit: false,
        };
        // A commit manifest the capture lacked is the flush's own: its
        // install failed after the commit, and its partitions go with it.
        if !commit_before
            && let Some(manifest) =
                read_manifest(&dir).map_err(|e| failed("read the flush's commit manifest", e))?
        {
            revert.drop_commit = true;
            for movement in manifest.moves {
                revert.dirs.push(movement.pending);
                revert.dirs.push(movement.installed);
            }
        }
        revert_flush(&dir, &revert).map_err(|e| failed("take the flush's partitions back", e))?;
        let installed: Vec<String> = created
            .into_iter()
            .map(|partition| partition.dir_name)
            .collect();
        self.release_ts_partitions(
            collection_key.0,
            collection_key.1,
            &collection_key.2,
            &installed,
        );
        match registry {
            Some(registry) => {
                self.ts_registries.insert(collection_key.clone(), registry);
            }
            None => {
                self.ts_registries.remove(&collection_key);
            }
        }
        if dir.exists() {
            restore_file(&dir, TS_SCHEMA_FILE, schema_file.as_deref())
                .and_then(|()| restore_file(&dir, TS_SERIES_CATALOG_FILE, catalog_file.as_deref()))
                .map_err(|e| failed("put back the schema and catalog files", e))?;
        }
        self.restore_ts_stamp(&collection_key, stamp);
        self.restore_ts_reservation(&collection_key, pre_image.reservation_bytes_before);
        self.restore_timeseries_ingest_preimage(entry_index, pre_image)
    }
}
