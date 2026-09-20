// SPDX-License-Identifier: BUSL-1.1

//! Undo of a timeseries `TRUNCATE` inside a transaction batch: reinstall the
//! in-memory state `execute_timeseries_truncate` moved out and rename the
//! partition directory back to its live name.
//!
//! The rename is the only durable step. A rename that fails is fatal to the
//! rollback (`Err` → `RollbackFailed`): the memory state would say the rows
//! exist while the partitions sit under the aside name, and the next scan
//! would answer from half a collection.

use crate::data::executor::core_loop::CoreLoop;

use super::TimeseriesTruncateUndo;

impl CoreLoop {
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
