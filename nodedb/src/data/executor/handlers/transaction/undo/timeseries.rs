// SPDX-License-Identifier: BUSL-1.1

//! Timeseries undo: the pre-image capture of an ingest, and the undo of a
//! `TRUNCATE` inside a transaction batch, which reinstalls the in-memory
//! state `execute_timeseries_truncate` moved out and renames the partition
//! directory back to its live name.
//!
//! The rename is the only durable step. A rename that fails is fatal to the
//! rollback (`Err` → `RollbackFailed`): the memory state would say the rows
//! exist while the partitions sit under the aside name, and the next scan
//! would answer from half a collection.

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

use super::{TimeseriesIngestUndo, TimeseriesTruncateUndo};

impl CoreLoop {
    /// The complete in-memory pre-image of a timeseries collection before an
    /// ingest mutates it: the memtable, its config and resident footprint,
    /// the last-value cache, the ingest watermark, the ingest timer, and the
    /// memtable's reservation.
    pub(in crate::data::executor) fn capture_timeseries_ingest_undo(
        &self,
        collection_key: &(DatabaseId, TenantId, String),
    ) -> TimeseriesIngestUndo {
        let memtable = self.columnar_memtables.get(collection_key);
        TimeseriesIngestUndo {
            collection_key: collection_key.clone(),
            memtable_before: memtable.map(|memtable| memtable.export_snapshot()),
            memtable_config_before: memtable.map(|memtable| memtable.config()),
            memtable_memory_bytes_before: memtable.map(|memtable| memtable.memory_bytes()),
            last_value_cache_before: self.ts_last_value_caches.get(collection_key).cloned(),
            series_catalog_before: self.ts_series_catalogs.get(collection_key).cloned(),
            max_ingested_lsn_before: self.ts_max_ingested_lsn.get(collection_key).copied(),
            last_ts_ingest_before: self.last_ts_ingest,
            reservation_bytes_before: self
                .columnar_memtable_mem
                .get(collection_key)
                .map(nodedb_mem::ReservationToken::size),
        }
    }

    pub(super) fn apply_undo_timeseries_truncate(
        &mut self,
        entry_index: usize,
        undo: TimeseriesTruncateUndo,
    ) -> Result<(), (usize, String)> {
        let TimeseriesTruncateUndo {
            collection_key,
            moved_dir,
            memtable,
            memtable_mem,
            registry,
            max_ingested_lsn,
            last_value_cache,
            series_catalog,
            truncate_floor,
        } = undo;

        if let Some((original, moved)) = moved_dir {
            // A directory a later sub-plan created under the live name holds
            // rows the reverse-order rollback has already withdrawn.
            if original.exists()
                && let Err(e) = std::fs::remove_dir_all(&original)
            {
                return Err((
                    entry_index,
                    format!(
                        "timeseries truncate undo: remove {} before restoring {}: {e}",
                        original.display(),
                        moved.display()
                    ),
                ));
            }
            if let Err(e) = std::fs::rename(&moved, &original) {
                return Err((
                    entry_index,
                    format!(
                        "timeseries truncate undo: rename {} back to {}: {e}",
                        moved.display(),
                        original.display()
                    ),
                ));
            }
        }

        reinstall(&mut self.columnar_memtables, &collection_key, memtable);
        reinstall(
            &mut self.columnar_memtable_mem,
            &collection_key,
            memtable_mem,
        );
        reinstall(&mut self.ts_registries, &collection_key, registry);
        reinstall(
            &mut self.ts_max_ingested_lsn,
            &collection_key,
            max_ingested_lsn,
        );
        reinstall(
            &mut self.ts_last_value_caches,
            &collection_key,
            last_value_cache,
        );
        reinstall(
            &mut self.ts_series_catalogs,
            &collection_key,
            series_catalog,
        );
        reinstall(
            &mut self.ts_truncate_floors,
            &collection_key,
            truncate_floor,
        );
        Ok(())
    }
}

/// Put `value` back under `key`, or clear the slot when the pre-image had
/// none: a later sub-plan in the aborted batch can have created one.
fn reinstall<V>(
    map: &mut std::collections::HashMap<
        (nodedb_types::DatabaseId, crate::types::TenantId, String),
        V,
    >,
    key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
    value: Option<V>,
) {
    match value {
        Some(v) => {
            map.insert(key.clone(), v);
        }
        None => {
            map.remove(key);
        }
    }
}
