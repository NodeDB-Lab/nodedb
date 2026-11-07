// SPDX-License-Identifier: BUSL-1.1

//! `REWRITE PARTITIONS FOR <collection>` on the core that owns it.
//!
//! Each sealed or merged partition is read whole, projected onto the
//! collection's current schema, and written again under a staging name with
//! the current codecs. A column the schema no longer holds is dropped. The
//! staged partition then replaces the live one in one directory swap.
//!
//! ## Crash safety
//!
//! The staging and backup names do not start with `ts-`, so the boot
//! registry load never lists them as partitions. The steps are:
//!
//! 1. Write `rewrite-{name}`, its stamp first and its `partition.meta` last.
//! 2. Swap: rename `{name}` to `old-{name}`, then `rewrite-{name}` to `{name}`.
//! 3. Remove `old-{name}`.
//!
//! `recover_interrupted_rewrites` settles a crash at any step before the
//! registry loads: a backup whose live partition is missing is renamed back,
//! a backup beside a live partition is removed, and every staging directory
//! is removed. The rows a partition holds never change, so either copy is
//! correct.

use std::collections::HashMap;
use std::path::Path;

use nodedb_types::timeseries::{PartitionMeta, PartitionState, SymbolDictionary};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::timeseries_checkpoint::schema::read_ts_schema;
use crate::data::executor::timeseries_checkpoint::stamp::{read_ts_stamp, write_ts_stamp};
use crate::engine::timeseries::columnar_memtable::{
    ColumnData, ColumnarDrainResult, ColumnarSchema,
};
use crate::engine::timeseries::columnar_segment::{ColumnarSegmentWriter, write_partition_meta};
use crate::engine::timeseries::partition_registry::PartitionEntry;
use crate::types::{DatabaseId, TenantId};

use super::partition_read::{TsPartitionColumns, partition_corrupt, read_ts_partition};

/// Prefix of the directory a rewrite stages a partition under.
pub(in crate::data::executor) const REWRITE_STAGED_PREFIX: &str = "rewrite-";
/// Prefix of the directory a rewrite moves the replaced partition to.
pub(in crate::data::executor) const REWRITE_BACKUP_PREFIX: &str = "old-";

/// The read path name partition reads and corruption reports carry.
const SITE: &str = "rewrite_partitions";

impl CoreLoop {
    /// Rewrite every sealed or merged partition of `collection`. Returns the
    /// number of partitions rewritten.
    pub(in crate::data::executor) fn rewrite_ts_partitions(
        &mut self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
    ) -> crate::Result<u64> {
        self.ensure_ts_registry(tid, database_id, collection)?;
        let key = (database_id, tid, collection.to_string());
        // A committed merge finishes first, so its sources are gone before
        // the partitions are listed.
        self.roll_forward_ts_merge(&key)?;
        let ts_dir = super::paths::ts_collection_dir(
            &self.data_dir,
            database_id.as_u64(),
            tid.as_u64(),
            collection,
        );
        let current = match self.columnar_memtables.get(&key) {
            Some(memtable) => Some(memtable.schema().clone()),
            None => read_ts_schema(&ts_dir)?,
        };
        let targets: Vec<(
            crate::engine::timeseries::partition_registry::PartitionKey,
            PartitionEntry,
        )> = match self.ts_registries.get(&key) {
            Some(registry) => registry
                .iter()
                .filter(|(_, entry)| {
                    matches!(
                        entry.meta.state,
                        PartitionState::Sealed | PartitionState::Merged
                    )
                })
                .map(|(partition_key, entry)| (partition_key.clone(), entry.clone()))
                .collect(),
            None => Vec::new(),
        };

        let mut rewritten = 0u64;
        for (partition_key, entry) in targets {
            let meta = rewrite_partition(
                &ts_dir,
                &entry,
                current.as_ref(),
                self.segment_keks.ts_segment_kek.as_ref(),
            )?;
            if let Some(registry) = self.ts_registries.get_mut(&key) {
                registry.update_meta(&partition_key, meta);
            }
            // The directory holds new bytes under its old name.
            self.release_ts_partitions(key.0, key.1, &key.2, std::slice::from_ref(&entry.dir_name));
            rewritten += 1;
        }
        tracing::info!(
            core = self.core_id,
            collection,
            rewritten,
            "timeseries partitions rewritten"
        );
        Ok(rewritten)
    }
}

/// Rewrite the partition `entry` names under `ts_dir`. Returns the meta the
/// registry holds for it afterwards.
fn rewrite_partition(
    ts_dir: &Path,
    entry: &PartitionEntry,
    current: Option<&ColumnarSchema>,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<PartitionMeta> {
    let live_dir = ts_dir.join(&entry.dir_name);
    // The name is joined onto `ts_dir`, renamed and removed below, so only a
    // plain component is accepted.
    if !nodedb_types::is_plain_path_component(&entry.dir_name) {
        return Err(partition_corrupt(
            &live_dir,
            "directory",
            SITE,
            "the registry names a partition that is not one plain path component",
        ));
    }
    let staged = format!("{REWRITE_STAGED_PREFIX}{}", entry.dir_name);
    let backup = format!("{REWRITE_BACKUP_PREFIX}{}", entry.dir_name);
    let staged_dir = ts_dir.join(&staged);

    let partition = read_ts_partition(&live_dir, SITE, kek)?;
    let stamp = read_ts_stamp(&live_dir)?;
    let (schema, columns, symbol_dicts) = project(partition, current, &live_dir)?;
    let drain = ColumnarDrainResult {
        columns,
        schema,
        symbol_dicts,
        row_count: entry.meta.row_count,
        min_ts: entry.meta.min_ts,
        max_ts: entry.meta.max_ts,
        max_system_ts: entry.meta.max_system_ts,
        series_row_counts: HashMap::new(),
    };

    // A staging directory left by an earlier failed rewrite is replaced.
    super::truncate::remove_dir_tree(&staged_dir)?;
    std::fs::create_dir_all(&staged_dir).map_err(|e| storage(&staged_dir, "create", &e))?;
    // The stamp lands before `partition.meta`, the staged partition's commit
    // point, as a flush writes it.
    if let Some(stamp) = stamp.as_ref() {
        write_ts_stamp(&staged_dir, stamp)?;
    }
    let mut meta = ColumnarSegmentWriter::new(ts_dir)
        .write_partition(
            &staged,
            &drain.view(),
            entry.meta.interval_ms,
            entry.meta.last_flushed_wal_lsn,
            kek,
        )
        .map_err(|e| storage(&staged_dir, "write", &e))?;
    // A partition a TRUNCATE ended stays ended: the end lands in the staged
    // copy's `partition.meta` before the swap publishes it.
    if entry.meta.ended_at.is_some() {
        meta.ended_at = entry.meta.ended_at;
        write_partition_meta(&staged_dir, &meta, kek)
            .map_err(|e| storage(&staged_dir, "write the end of", &e))?;
    }

    nodedb_wal::segment::atomic_swap_dirs_fsync(ts_dir, &entry.dir_name, &backup, &staged)
        .map_err(|e| storage(&live_dir, "swap in the rewritten copy of", &e))?;
    super::truncate::remove_dir_tree(&ts_dir.join(&backup))?;

    // The registry keeps the partition's identity: its state, time range and
    // system-time bound. Size, codecs and column statistics are the new ones.
    meta.state = entry.meta.state;
    meta.min_ts = entry.meta.min_ts;
    meta.max_ts = entry.meta.max_ts;
    meta.max_system_ts = entry.meta.max_system_ts;
    Ok(meta)
}

/// The schema, columns and symbol dictionaries of `partition` projected onto
/// `current`. A column `current` does not hold is dropped, except the
/// timestamp column. A kept column takes `current`'s codec. With no current
/// schema every column is kept with its own codec.
fn project(
    partition: TsPartitionColumns,
    current: Option<&ColumnarSchema>,
    part_dir: &Path,
) -> crate::Result<(
    ColumnarSchema,
    Vec<ColumnData>,
    HashMap<usize, SymbolDictionary>,
)> {
    let TsPartitionColumns {
        schema,
        columns,
        mut sym_dicts,
    } = partition;
    let ts_name = schema
        .columns
        .get(schema.timestamp_idx)
        .map(|(name, _)| name.clone())
        .ok_or_else(|| {
            partition_corrupt(
                part_dir,
                "schema",
                SITE,
                "the timestamp column index names no column",
            )
        })?;

    let mut kept_columns = Vec::new();
    let mut kept_data = Vec::new();
    let mut kept_codecs = Vec::new();
    let mut kept_dicts = HashMap::new();
    let mut timestamp_idx = 0;
    for (i, ((name, ty), data)) in schema.columns.iter().zip(columns).enumerate() {
        let current_idx = current.and_then(|c| c.columns.iter().position(|(n, _)| n == name));
        let is_timestamp = *name == ts_name;
        if current.is_some() && current_idx.is_none() && !is_timestamp {
            continue;
        }
        let data = data.ok_or_else(|| {
            partition_corrupt(
                part_dir,
                "column",
                SITE,
                format!("column '{name}' did not read"),
            )
        })?;
        let codec = match (current, current_idx) {
            (Some(c), Some(ci)) => c.codec(ci),
            _ => schema.codec(i),
        };
        let out = kept_columns.len();
        if is_timestamp {
            timestamp_idx = out;
        }
        if let Some(dict) = sym_dicts.remove(&i) {
            kept_dicts.insert(out, dict);
        }
        kept_columns.push((name.clone(), *ty));
        kept_data.push(data);
        kept_codecs.push(codec);
    }
    Ok((
        ColumnarSchema {
            columns: kept_columns,
            timestamp_idx,
            codecs: kept_codecs,
        },
        kept_data,
        kept_dicts,
    ))
}

fn storage(path: &Path, action: &str, error: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "timeseries".into(),
        detail: format!("rewrite partition: {action} {}: {error}", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::timeseries::columnar_memtable::{ColumnType, TimeKind};

    use super::*;

    fn partition() -> TsPartitionColumns {
        TsPartitionColumns {
            schema: ColumnarSchema {
                columns: vec![
                    ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                    ("dropped".into(), ColumnType::Float64),
                    ("value".into(), ColumnType::Float64),
                ],
                timestamp_idx: 0,
                codecs: Vec::new(),
            },
            columns: vec![
                Some(ColumnData::Timestamp(vec![1, 2])),
                Some(ColumnData::Float64(vec![9.0, 9.0])),
                Some(ColumnData::Float64(vec![1.0, 2.0])),
            ],
            sym_dicts: HashMap::new(),
        }
    }

    #[test]
    fn a_column_the_current_schema_lacks_is_dropped() {
        let current = ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        };
        let (schema, data, _) =
            project(partition(), Some(&current), Path::new("ts-x")).expect("projects");
        let names: Vec<&str> = schema.columns.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["ts", "value"]);
        assert_eq!(schema.timestamp_idx, 0);
        assert_eq!(data.len(), 2);
    }

    #[test]
    fn without_a_current_schema_every_column_is_kept() {
        let (schema, data, _) = project(partition(), None, Path::new("ts-x")).expect("projects");
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(data.len(), 3);
    }

    /// A partition written with the segment key is rewritten with it, and the
    /// rewritten copy reads back with it.
    #[test]
    fn an_encrypted_partition_rewrites_with_the_segment_key() {
        use crate::engine::timeseries::columnar_memtable::{
            ColumnarMemtable, ColumnarMemtableConfig,
        };

        let kek = nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0x42u8; 32]).expect("key");
        let dir = tempfile::tempdir().expect("tempdir");
        let mut memtable = ColumnarMemtable::new_metric(ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 1000,
        });
        for i in 0..10 {
            memtable
                .ingest_row(
                    1,
                    &[
                        crate::engine::timeseries::columnar_memtable::ColumnValue::Timestamp(
                            1_000 + i,
                        ),
                        crate::engine::timeseries::columnar_memtable::ColumnValue::Float64(
                            i as f64,
                        ),
                    ],
                )
                .expect("metric row");
        }
        let drain = memtable.drain();
        let mut meta = ColumnarSegmentWriter::new(dir.path())
            .write_partition("ts-enc", &drain.view(), 0, 1, Some(&kek))
            .expect("write");
        meta.state = PartitionState::Sealed;
        let entry = PartitionEntry {
            meta,
            dir_name: "ts-enc".to_string(),
        };

        let rewritten = rewrite_partition(dir.path(), &entry, None, Some(&kek)).expect("rewrites");
        assert_eq!(rewritten.row_count, 10);
        assert_eq!(rewritten.state, PartitionState::Sealed);
        let read = read_ts_partition(&dir.path().join("ts-enc"), "test", Some(&kek))
            .expect("the rewritten copy reads with the key");
        assert_eq!(read.schema.columns.len(), 2);
        assert!(!dir.path().join("old-ts-enc").exists());
        assert!(!dir.path().join("rewrite-ts-enc").exists());
    }

    #[test]
    fn the_timestamp_column_is_always_kept() {
        let current = ColumnarSchema {
            columns: vec![("value".into(), ColumnType::Float64)],
            timestamp_idx: 0,
            codecs: Vec::new(),
        };
        let (schema, _, _) =
            project(partition(), Some(&current), Path::new("ts-x")).expect("projects");
        let names: Vec<&str> = schema.columns.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["ts", "value"]);
        assert_eq!(schema.timestamp_idx, 0);
    }
}
