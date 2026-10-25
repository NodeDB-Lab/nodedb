// SPDX-License-Identifier: BUSL-1.1

//! Install a committed multi-partition flush while the core runs.
//!
//! A flush that committed (`flush_commit`) is durable. Installing it renames
//! its staged partitions, then takes their rows out of the memtable, records
//! the flush's stamp, registers the partitions and folds them into the
//! continuous aggregates. The install is idempotent, so a commit whose
//! install failed is installed by the next flush of the collection and by
//! the maintenance tick before any new flush. A collection refuses a new
//! flush only while that install keeps failing.
//!
//! The rows a committed flush holds are the first rows of the memtable: the
//! flush read the whole memtable, no other flush ran since, and every later
//! ingest appended after them.

use std::path::Path;

use nodedb_types::timeseries::{PartitionMeta, PartitionState};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::timeseries_checkpoint::stamp::{
    TsCollectionKey, TsReplayStamp, read_ts_stamp,
};
use crate::engine::timeseries::columnar_memtable::ColumnarDrainResult;
use crate::engine::timeseries::columnar_segment::ColumnarSegmentReader;
use crate::engine::timeseries::partition_registry::{PartitionEntry, PartitionRegistry};

use super::flush_commit::{FlushManifest, current_dir, install_flush, read_manifest};
use super::partition_read::{partition_corrupt, read_ts_partition};

/// The read path corruption reports name.
const SITE: &str = "timeseries_flush_rollforward";

/// One partition of a committed flush, with its rows.
pub(in crate::data::executor) struct CommittedPartition {
    /// The installed `ts-` directory name.
    pub name: String,
    pub meta: PartitionMeta,
    pub rows: ColumnarDrainResult,
}

/// A committed flush that is not yet installed.
pub(in crate::data::executor) struct CommittedFlush {
    pub manifest: FlushManifest,
    pub partitions: Vec<CommittedPartition>,
    /// The stamp every partition of the flush carries.
    pub stamp: TsReplayStamp,
}

impl CoreLoop {
    /// Install the committed flush of `key` if one is pending. Returns
    /// whether one was.
    pub(in crate::data::executor) fn roll_forward_ts_flush(
        &mut self,
        key: &TsCollectionKey,
        now_ms: i64,
    ) -> crate::Result<bool> {
        let ts_dir =
            super::paths::ts_collection_dir(&self.data_dir, key.0.as_u64(), key.1.as_u64(), &key.2);
        let Some(manifest) = read_manifest(&ts_dir)? else {
            return Ok(false);
        };
        let committed =
            read_committed_flush(&ts_dir, manifest, self.segment_keks.ts_segment_kek.as_ref())?;
        self.install_committed_flush(key, &ts_dir, committed, now_ms)?;
        Ok(true)
    }

    /// Install every pending committed flush of a collection with rows in
    /// memory. Run from the maintenance tick. A failure is reported where it
    /// is detected, and the next tick retries.
    pub(in crate::data::executor) fn roll_forward_ts_flushes(&mut self, now_ms: i64) {
        let keys: Vec<TsCollectionKey> = self
            .columnar_memtables
            .iter()
            .filter(|(_, mt)| !mt.is_empty())
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            if let Err(e) = self.roll_forward_ts_flush(&key, now_ms) {
                tracing::warn!(
                    collection = %key.2,
                    error = %e,
                    "committed timeseries flush did not install; the next tick retries"
                );
            }
        }
    }

    /// Install `committed` and apply it to this core's state: the memtable
    /// rows it holds leave, its stamp joins the collection stamp, and its
    /// partitions are registered and folded.
    pub(in crate::data::executor) fn install_committed_flush(
        &mut self,
        key: &TsCollectionKey,
        ts_dir: &Path,
        committed: CommittedFlush,
        now_ms: i64,
    ) -> crate::Result<()> {
        let CommittedFlush {
            manifest,
            partitions,
            stamp,
        } = committed;
        if let Err(e) = install_flush(ts_dir, &manifest) {
            crate::diag::timeseries_flush_not_installed(&e, key.0.as_u64(), key.1.as_u64(), &key.2);
            return Err(e);
        }

        let rows: u64 = partitions.iter().map(|p| p.meta.row_count).sum();
        // The series the memtable counts, with their catalog keys: the rows
        // that stay are counted again under them.
        let known: Vec<(
            nodedb_types::timeseries::SeriesId,
            nodedb_types::timeseries::SeriesKey,
        )> = match (
            self.columnar_memtables.get(key),
            self.ts_series_catalogs.get(key),
        ) {
            (Some(memtable), Some(catalog)) => memtable
                .series_ids()
                .into_iter()
                .filter_map(|id| catalog.get(id).map(|series| (id, series.clone())))
                .collect(),
            _ => Vec::new(),
        };
        if let Some(memtable) = self.columnar_memtables.get_mut(key) {
            memtable.drain_front(rows, &known);
        }
        self.recharge_ts_memtable_budget(key.1, key.0, &key.2);
        let merged = match self.ts_replay.stamps.get(key) {
            Some(held) => held.union(&stamp),
            None => stamp,
        };
        self.ts_replay.stamps.insert(key.clone(), merged);

        let registry = self.ts_registries.entry(key.clone()).or_insert_with(|| {
            PartitionRegistry::new(
                nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
            )
        });
        for partition in &partitions {
            let mut meta = partition.meta.clone();
            meta.min_ts = partition.rows.min_ts;
            meta.max_ts = partition.rows.max_ts;
            meta.state = PartitionState::Sealed;
            // `insert_partition`, not `import`: a late row's partition can
            // share a min_ts with a sealed one.
            registry.insert_partition(PartitionEntry {
                meta,
                dir_name: partition.name.clone(),
            });
        }
        registry.record_ingest(rows, now_ms);
        registry.maybe_narrow_interval();

        // Each partition is durable, so an aggregate image that names it
        // never names a partition a crash can lose. The fold is system work
        // and takes no row-level-security gate, as the flush's fold takes
        // none.
        for partition in &partitions {
            self.continuous_agg_mgr.on_flush(
                key.0.as_u64(),
                key.1.as_u64(),
                &key.2,
                &partition.name,
                &partition.rows,
                now_ms,
            );
        }
        tracing::info!(
            collection = %key.2,
            rows,
            partitions = partitions.len(),
            "timeseries flush installed"
        );
        Ok(())
    }
}

/// Read every partition `manifest` names from where it is now, staged or
/// installed. Nothing changes on disk.
fn read_committed_flush(
    ts_dir: &Path,
    manifest: FlushManifest,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<CommittedFlush> {
    let mut partitions = Vec::with_capacity(manifest.moves.len());
    let mut stamp = TsReplayStamp::default();
    for movement in &manifest.moves {
        let dir = current_dir(ts_dir, movement);
        let meta = ColumnarSegmentReader::read_meta(&dir, kek)
            .map_err(|e| partition_corrupt(&dir, "meta", SITE, e))?;
        if let Some(partition_stamp) = read_ts_stamp(&dir)? {
            stamp = stamp.union(&partition_stamp);
        }
        let read = read_ts_partition(&dir, SITE, kek)?;
        let columns = read
            .columns
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                partition_corrupt(&dir, "column", SITE, "a schema column did not read")
            })?;
        let rows = ColumnarDrainResult {
            columns,
            schema: read.schema,
            symbol_dicts: read.sym_dicts,
            row_count: meta.row_count,
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            max_system_ts: meta.max_system_ts,
            series_row_counts: std::collections::HashMap::new(),
        };
        partitions.push(CommittedPartition {
            name: movement.installed.clone(),
            meta,
            rows,
        });
    }
    Ok(CommittedFlush {
        manifest,
        partitions,
        stamp,
    })
}
