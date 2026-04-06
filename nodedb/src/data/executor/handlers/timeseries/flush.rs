//! Timeseries flush and partition registry management.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_segment::ColumnarSegmentWriter;
use crate::engine::timeseries::partition_registry::PartitionRegistry;

impl CoreLoop {
    /// Ensure the partition registry is loaded for a timeseries collection.
    ///
    /// On first access, scans the `ts/{collection}/` directory for existing
    /// partition directories and populates the registry from partition metadata.
    pub(in crate::data::executor) fn ensure_ts_registry(&mut self, collection: &str) {
        if self.ts_registries.contains_key(collection) {
            return;
        }
        let ts_dir = self.data_dir.join("ts").join(collection);
        if !ts_dir.exists() {
            return;
        }

        let mut registry = PartitionRegistry::new(
            nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
        );

        // Scan for existing partition directories.
        if let Ok(entries) = std::fs::read_dir(&ts_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name_str) = name.to_str() else {
                    continue;
                };
                if !name_str.starts_with("ts-") || !entry.path().is_dir() {
                    continue;
                }
                let meta_path = entry.path().join("partition.meta");
                if let Ok(meta_bytes) = std::fs::read(&meta_path)
                    && let Ok(meta) =
                        sonic_rs::from_slice::<nodedb_types::timeseries::PartitionMeta>(&meta_bytes)
                {
                    let pe = crate::engine::timeseries::partition_registry::PartitionEntry {
                        meta,
                        dir_name: name_str.to_string(),
                    };
                    registry.import(vec![(pe.meta.min_ts, pe)]);
                }
            }
        }

        if registry.partition_count() > 0 {
            tracing::info!(
                collection,
                partitions = registry.partition_count(),
                "loaded partition registry from disk"
            );
        }
        self.ts_registries.insert(collection.to_string(), registry);
    }

    /// Flush a timeseries collection's memtable to L1 segments.
    ///
    /// Drains the columnar memtable, writes segments via `ColumnarSegmentWriter`,
    /// registers the new partition in `ts_registries`, and fires the continuous
    /// aggregate hook.
    pub(in crate::data::executor) fn flush_ts_collection(&mut self, collection: &str, now_ms: i64) {
        let Some(mt) = self.columnar_memtables.get_mut(collection) else {
            return;
        };
        if mt.is_empty() {
            return;
        }

        // Track memtable bytes for governor release after drain.
        let memtable_bytes = mt.memory_bytes();

        let drain = mt.drain();

        // Release timeseries budget after drain clears the memtable.
        if let Some(ref gov) = self.governor {
            gov.release(nodedb_mem::EngineId::Timeseries, memtable_bytes);
        }

        // Write to L1 segments.
        let segment_dir = self.data_dir.join(format!("ts/{collection}"));
        let writer = ColumnarSegmentWriter::new(&segment_dir);
        let partition_name = format!("ts-{}_{}", drain.min_ts, drain.max_ts);

        // Use the max ingested WAL LSN for this collection so the partition
        // records which WAL records have been flushed.
        let flush_wal_lsn = self
            .ts_max_ingested_lsn
            .get(collection)
            .copied()
            .unwrap_or(0);
        match writer.write_partition(&partition_name, &drain, 0, flush_wal_lsn) {
            Ok(meta) => {
                tracing::info!(
                    collection,
                    rows = meta.row_count,
                    "timeseries columnar flush complete"
                );

                let registry = self
                    .ts_registries
                    .entry(collection.to_string())
                    .or_insert_with(|| {
                        PartitionRegistry::new(
                            nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
                        )
                    });
                let mut reg_meta = meta;
                reg_meta.min_ts = drain.min_ts;
                reg_meta.max_ts = drain.max_ts;
                reg_meta.state = nodedb_types::timeseries::PartitionState::Sealed;
                let pe = crate::engine::timeseries::partition_registry::PartitionEntry {
                    meta: reg_meta,
                    dir_name: partition_name,
                };
                registry.import(vec![(drain.min_ts, pe)]);
            }
            Err(e) => {
                tracing::error!(
                    collection,
                    error = %e,
                    "timeseries columnar flush failed"
                );
                return;
            }
        }

        // Fire continuous aggregate hook.
        let refreshed = self.continuous_agg_mgr.on_flush(collection, &drain, now_ms);
        if !refreshed.is_empty() {
            tracing::debug!(
                collection,
                aggregates = ?refreshed,
                "continuous aggregates refreshed on flush"
            );
        }
    }
}
