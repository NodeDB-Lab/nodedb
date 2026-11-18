// SPDX-License-Identifier: BUSL-1.1

//! A batched install's timeseries flush between two batches.
//!
//! A batched install holds one batch of its record at a time, and its
//! memtables stay under their budget: after each timeseries batch, the
//! driver flushes every collection of the record whose memtable is
//! pressed. The record is not noted applied yet, so the flush's stamp
//! names every record the core applied and, in part, the install's record:
//! its ops below the batch's end (`TsFlushStamp::InstallPartial`). Restart
//! replay then applies the record's other ops, each once.
//!
//! A collection flushes only once the install keeps a memtable snapshot of
//! it: the first flush of a collection the install has not ingested into
//! records one first. Shared memory pressure can press such a collection,
//! and flushing it keeps the core's timeseries memory bounded; the snapshot
//! is one memtable copy, at most the budget.
//!
//! Each flush records its undo (`TsInstallFlushUndo`): the directories it
//! wrote, read off the collection directory before and after the flush, so
//! a flush that failed part way is taken back too. A flush whose effects
//! cannot be read back for the undo leaves nothing a rollback can trust,
//! and the install stops the core instead (`InstallFlushError::Untracked`).

use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::handlers::transaction::undo::timeseries_flush::read_optional;
use crate::data::executor::handlers::transaction::undo::ts_install_flush::{
    TsFilesBefore, TsInstallFlushUndo,
};
use crate::data::executor::timeseries_checkpoint::schema::TS_SCHEMA_FILE;
use crate::data::executor::timeseries_checkpoint::series_catalog::TS_SERIES_CATALOG_FILE;
use crate::types::partial_record::PartialRecord;

use super::flush::TsFlushStamp;
use super::flush_commit::{FlushRevert, PENDING_PREFIX, has_uninstalled_flush};
use super::group_flush::TsKey;
use super::stream_load::TsStreamLoads;

/// Why a flush between two batches stopped the install.
#[derive(Debug)]
pub(in crate::data::executor) enum InstallFlushError {
    /// The flush did not complete. Its undo is recorded, so the install
    /// rolls back.
    Failed(crate::Error),
    /// The flush's effects could not be read back for its undo. No rollback
    /// can take them back: the install stops the core, and restart replay
    /// completes the record forward.
    Untracked(crate::Error),
}

impl CoreLoop {
    /// After a timeseries batch of a batched install: flush every
    /// collection in `loads` whose memtable holds rows and is pressed,
    /// naming the install's record through `partial`. `flushed` holds the
    /// collections the install flushed before. The first flush of each
    /// keeps the collection's files as they were for its undo. Returns the
    /// collections it flushed.
    pub(in crate::data::executor) fn flush_ts_install_batch(
        &mut self,
        loads: &TsStreamLoads,
        partial: PartialRecord,
        flushed: &mut HashSet<TsKey>,
    ) -> Result<usize, InstallFlushError> {
        let pressed: Vec<TsKey> = loads
            .keys()
            .filter(|key| {
                self.columnar_memtables
                    .get(*key)
                    .is_some_and(|mt| !mt.is_empty())
                    && self.ts_memtable_pressed(key)
            })
            .cloned()
            .collect();
        for key in &pressed {
            let first = flushed.insert(key.clone());
            self.flush_ts_install_partial(key, partial, first)?;
        }
        Ok(pressed.len())
    }

    /// Flush `key` named through `partial`, and record the flush's undo.
    fn flush_ts_install_partial(
        &mut self,
        key: &TsKey,
        partial: PartialRecord,
        first: bool,
    ) -> Result<(), InstallFlushError> {
        let dir = self.ts_dir_of(key);
        // Nothing is written before the flush runs: an error here rolls
        // back as any refused batch does.
        let failed = InstallFlushError::Failed;
        if has_uninstalled_flush(&dir).map_err(failed)? {
            // The install rolled every committed flush forward before its
            // timeseries batches, so one here is not the install's.
            return Err(InstallFlushError::Failed(crate::Error::Storage {
                engine: "timeseries".into(),
                detail: format!(
                    "a committed flush of '{}' is not installed between two batches of a \
                     batched install",
                    key.2
                ),
            }));
        }
        // The rollback restores the memtable from the install's oldest
        // snapshot of the collection, after this flush's undo. A collection
        // the install has not snapshotted yet, one it has not ingested into
        // and that shared memory pressure flushes, takes its snapshot here,
        // so the rows the flush moves out come back.
        let snapshotted = self
            .redo_apply
            .scope
            .as_ref()
            .is_some_and(|scope| scope.holds_ts_snapshot(key));
        if !snapshotted {
            self.record_redo_ts_pre_image(key.0, key.1, &key.2);
        }
        let before = partition_dirs(&dir).map_err(failed)?;
        let files_before = if first {
            Some(TsFilesBefore {
                schema: read_optional(&dir.join(TS_SCHEMA_FILE)).map_err(failed)?,
                catalog: read_optional(&dir.join(TS_SERIES_CATALOG_FILE)).map_err(failed)?,
            })
        } else {
            None
        };
        let registry = self.ts_registries.get(key);
        let registry_existed = registry.is_some();
        let adaptive_before = registry.map(|registry| registry.adaptive_state());
        let stamp_before = self.ts_replay.stamps.get(key).cloned();
        let reservation_bytes_before = self
            .columnar_memtable_mem
            .get(key)
            .map(nodedb_mem::ReservationToken::size);

        let now_ms = self.ingest_now_ms();
        let flushed = self.flush_ts_collection_stamped(
            key.1,
            key.0,
            &key.2,
            now_ms,
            TsFlushStamp::InstallPartial(partial),
        );

        let untracked = InstallFlushError::Untracked;
        let after = partition_dirs(&dir).map_err(untracked)?;
        let drop_commit = has_uninstalled_flush(&dir).map_err(untracked)?;
        let dirs: Vec<String> = after.difference(&before).cloned().collect();
        let partition_keys = self
            .ts_registries
            .get(key)
            .map(|registry| {
                registry
                    .iter()
                    .filter(|(_, entry)| dirs.contains(&entry.dir_name))
                    .map(|(key, _)| key.clone())
                    .collect()
            })
            .unwrap_or_default();
        self.record_redo_undo([UndoEntry::TimeseriesInstallFlush(Box::new(
            TsInstallFlushUndo {
                collection_key: key.clone(),
                revert: FlushRevert { dirs, drop_commit },
                partition_keys,
                stamp_before,
                registry_existed,
                adaptive_before,
                reservation_bytes_before,
                files_before,
            },
        ))]);
        flushed.map_err(InstallFlushError::Failed)
    }
}

/// The partition directories under `dir`, committed and staged. An absent
/// directory holds none.
fn partition_dirs(dir: &Path) -> crate::Result<BTreeSet<String>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(e) => return Err(list_error(dir, &e)),
    };
    let mut dirs = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(|e| list_error(dir, &e))?;
        if let Some(name) = entry.file_name().to_str()
            && (name.starts_with("ts-") || name.starts_with(PENDING_PREFIX))
        {
            dirs.insert(name.to_string());
        }
    }
    Ok(dirs)
}

fn list_error(dir: &Path, e: &std::io::Error) -> crate::Error {
    crate::Error::Storage {
        engine: "timeseries".into(),
        detail: format!("list the partitions of {}: {e}", dir.display()),
    }
}
