// SPDX-License-Identifier: BUSL-1.1

//! Undo of a batched install's timeseries flush between two batches.
//!
//! The flush filed partitions that hold the install's rows below the
//! batch's end, stamped with the part of the record they hold. A rollback
//! takes the flush back whole through the flush's revert manifest
//! (`revert_flush`): every partition carries that stamp, so none may stay
//! once another went. From the revert manifest's write on, a crash finishes
//! the revert at the next open, before any partition loads. The revert
//! fsyncs the collection directory.
//!
//! The undo then puts back the registry entries, the stamp and the
//! reservation the flush changed. The memtable is not here: the oldest
//! ingest snapshot of the install restores it whole, and applies after
//! every flush undo. So the entry holds no memtable copy and spills, and
//! the RAM the install's flushes hold does not grow with the record.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::flush_commit::{FlushRevert, revert_flush};
use crate::data::executor::timeseries_checkpoint::schema::TS_SCHEMA_FILE;
use crate::data::executor::timeseries_checkpoint::series_catalog::TS_SERIES_CATALOG_FILE;
use crate::data::executor::timeseries_checkpoint::stamp::{TsCollectionKey, TsReplayStamp};
use crate::engine::timeseries::partition_registry::{AdaptiveState, PartitionKey};

use super::UndoError;
use super::timeseries_flush::restore_file;

/// The schema and series catalog files of a collection before the
/// install's first flush of it. `None` for a file that was absent.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor) struct TsFilesBefore {
    pub schema: Option<Vec<u8>>,
    pub catalog: Option<Vec<u8>>,
}

/// What one flush between two batches of a batched install changed. Plain
/// data: it spills with the install's other plain undo entries.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor) struct TsInstallFlushUndo {
    pub collection_key: TsCollectionKey,
    /// Every directory the flush wrote, and whether a commit manifest it
    /// left goes too.
    pub revert: FlushRevert,
    /// The partitions the flush filed in the registry.
    pub partition_keys: Vec<PartitionKey>,
    /// The in-memory collection stamp before the flush.
    pub stamp_before: Option<TsReplayStamp>,
    /// Whether the collection had a registry before the flush.
    pub registry_existed: bool,
    /// The registry's AUTO interval state before the flush.
    pub adaptive_before: Option<AdaptiveState>,
    /// The memtable's reservation before the flush.
    pub reservation_bytes_before: Option<usize>,
    /// The files before the install's first flush of the collection. A
    /// later flush's entry holds none: the first one's undo applies after
    /// it and puts back the files as they were before the install.
    pub files_before: Option<TsFilesBefore>,
}

impl TsInstallFlushUndo {
    /// Heap bytes the entry holds: the directory names, the partition keys
    /// and the file images.
    pub(in crate::data::executor) fn held_bytes(&self) -> usize {
        let dirs: usize = self.revert.dirs.iter().map(String::len).sum();
        let keys: usize = self
            .partition_keys
            .iter()
            .map(|key| key.dir_name.len() + std::mem::size_of::<PartitionKey>())
            .sum();
        let files = self.files_before.as_ref().map_or(0, |files| {
            files.schema.as_ref().map_or(0, Vec::len) + files.catalog.as_ref().map_or(0, Vec::len)
        });
        self.collection_key.2.len() + dirs + keys + files
    }
}

impl CoreLoop {
    /// Reverse the flush `undo` names. Every durable step is fatal on
    /// failure: a partition that stays holds rows of a rolled-back record.
    pub(super) fn apply_undo_ts_install_flush(
        &mut self,
        entry_index: usize,
        undo: TsInstallFlushUndo,
    ) -> Result<(), UndoError> {
        let TsInstallFlushUndo {
            collection_key,
            revert,
            partition_keys,
            stamp_before,
            registry_existed,
            adaptive_before,
            reservation_bytes_before,
            files_before,
        } = undo;
        let collection = collection_key.2.clone();
        let failed = |action: &str, e: crate::Error| {
            UndoError::failed(
                entry_index,
                format!("timeseries install flush undo of '{collection}': {action}"),
                e,
            )
        };
        let dir = self.ts_dir_of(&collection_key);
        revert_flush(&dir, &revert).map_err(|e| failed("take the flush's partitions back", e))?;
        self.release_ts_partitions(
            collection_key.0,
            collection_key.1,
            &collection_key.2,
            &revert.dirs,
        );
        if registry_existed {
            if let Some(registry) = self.ts_registries.get_mut(&collection_key) {
                for key in &partition_keys {
                    registry.remove(key);
                }
                if let Some(state) = adaptive_before {
                    registry.restore_adaptive_state(state);
                }
            }
        } else {
            self.ts_registries.remove(&collection_key);
        }
        if let Some(files) = files_before
            && dir.exists()
        {
            restore_file(&dir, TS_SCHEMA_FILE, files.schema.as_deref())
                .and_then(|()| restore_file(&dir, TS_SERIES_CATALOG_FILE, files.catalog.as_deref()))
                .map_err(|e| failed("put back the schema and catalog files", e))?;
        }
        self.restore_ts_stamp(&collection_key, stamp_before);
        self.restore_ts_reservation(&collection_key, reservation_bytes_before);
        Ok(())
    }
}
