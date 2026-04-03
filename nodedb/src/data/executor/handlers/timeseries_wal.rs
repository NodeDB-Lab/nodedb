//! WAL replay for timeseries records.
//!
//! On startup, replays `TimeseriesBatch` records into the per-core
//! columnar memtable. Only replays records with LSN > `last_flushed_wal_lsn`
//! per partition (not max_ts — safe with out-of-order data).

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_memtable::{
    ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
};
use nodedb_types::timeseries::MetricSample;

/// Default timeseries memtable configuration for replay and auto-creation.
fn default_ts_config() -> ColumnarMemtableConfig {
    ColumnarMemtableConfig {
        max_memory_bytes: 64 * 1024 * 1024,
        hard_memory_limit: 80 * 1024 * 1024,
        max_tag_cardinality: 100_000,
    }
}

impl CoreLoop {
    /// Ensure a timeseries memtable exists for the given collection, creating if needed.
    fn ensure_columnar_memtable(&mut self, collection: &str, schema: ColumnarSchema) {
        if !self.columnar_memtables.contains_key(collection) {
            self.columnar_memtables.insert(
                collection.to_string(),
                ColumnarMemtable::new(schema, default_ts_config()),
            );
        }
    }

    /// Replay WAL timeseries records to rebuild in-memory memtable state after crash.
    ///
    /// Called once during startup, after `open()` but before the event loop.
    /// Processes `TimeseriesBatch` records, ignoring records for other vShards.
    /// Uses LSN-based skip: only replays records with LSN > last flushed LSN.
    pub fn replay_timeseries_wal(&mut self, records: &[nodedb_wal::WalRecord], num_cores: usize) {
        use nodedb_wal::record::RecordType;

        let mut replayed = 0usize;
        let mut skipped = 0usize;

        for record in records {
            let logical_type = record.logical_record_type();
            let record_type = RecordType::from_raw(logical_type);

            let is_ts_batch = record_type == Some(RecordType::TimeseriesBatch);
            if !is_ts_batch {
                continue;
            }

            // Route by vShard to the correct core.
            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                skipped += 1;
                continue;
            }

            // Deserialize: (collection, raw_payload).
            let Ok((raw_collection, payload)): Result<(String, Vec<u8>), _> =
                rmp_serde::from_slice(&record.payload)
            else {
                tracing::warn!(
                    core = self.core_id,
                    lsn = record.header.lsn,
                    "skipping malformed TimeseriesBatch WAL record"
                );
                continue;
            };

            // Scope collection name with tenant ID to match the query path
            // (dispatch.rs scopes scan queries as "{tenant_id}:{collection}").
            let tenant_id = record.header.tenant_id;
            let collection = format!("{tenant_id}:{raw_collection}");

            let record_lsn = record.header.lsn;

            // Check if this record was already flushed (LSN-based skip).
            if let Some(registry) = self.ts_registries.get(&collection) {
                // Find the max flushed LSN across all partitions.
                let max_flushed_lsn = registry
                    .iter()
                    .map(|(_, e)| e.meta.last_flushed_wal_lsn)
                    .max()
                    .unwrap_or(0);
                if record_lsn <= max_flushed_lsn {
                    skipped += 1;
                    continue;
                }
            }

            // Track the max WAL LSN ingested per collection for flush metadata.
            if let Some(entry) = self.ts_max_ingested_lsn.get_mut(&collection) {
                *entry = (*entry).max(record_lsn);
            } else {
                self.ts_max_ingested_lsn
                    .insert(collection.clone(), record_lsn);
            }

            // Re-ingest the ILP payload into the memtable.
            if let Ok(input) = std::str::from_utf8(&payload) {
                let lines: Vec<_> = crate::engine::timeseries::ilp::parse_batch(input)
                    .into_iter()
                    .filter_map(|r| r.ok())
                    .collect();

                if lines.is_empty() {
                    continue;
                }

                // Flush memtable if it's at the soft limit BEFORE ingesting.
                // Without this, the memtable overflows during large WAL replays
                // and excess rows are silently rejected.
                if let Some(mt) = self.columnar_memtables.get(&collection)
                    && mt.memory_bytes() >= 64 * 1024 * 1024
                {
                    self.flush_ts_collection(&collection, 0);
                }

                // Ensure memtable exists.
                let schema = crate::engine::timeseries::ilp_ingest::infer_schema(&lines);
                self.ensure_columnar_memtable(&collection, schema);

                let mt = self.columnar_memtables.get_mut(&collection).unwrap();
                let mut series_keys = std::collections::HashMap::new();
                let now_ms = 0; // Default timestamp not needed for replay (records have timestamps).
                let (accepted, _) = crate::engine::timeseries::ilp_ingest::ingest_batch(
                    mt,
                    &lines,
                    &mut series_keys,
                    now_ms,
                );
                replayed += accepted;
            } else {
                // Binary payload — try msgpack-encoded samples.
                if let Ok(batch) =
                    rmp_serde::from_slice::<nodedb_types::timeseries::TimeseriesWalBatch>(&payload)
                {
                    self.ensure_columnar_memtable(&collection, ColumnarSchema::metric_default());

                    let mt = self.columnar_memtables.get_mut(&collection).unwrap();
                    for (series_id, timestamp_ms, value) in &batch.samples {
                        mt.ingest_metric(
                            *series_id,
                            MetricSample {
                                timestamp_ms: *timestamp_ms,
                                value: *value,
                            },
                        );
                    }
                    replayed += batch.samples.len();
                }
            }
        }

        if replayed > 0 {
            tracing::info!(
                core = self.core_id,
                replayed,
                skipped,
                collections = self.columnar_memtables.len(),
                "WAL timeseries replay complete"
            );
        }
    }
}
