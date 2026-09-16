// SPDX-License-Identifier: BUSL-1.1

//! Columnar segment writer.
//!
//! Every file here is a checkpoint-class write: the coordinated checkpoint
//! reports these partitions as the timeseries engine's durability and the WAL
//! segments below that LSN are then unlinked, so a correctly-named file full of
//! zeros after power loss is not a degraded segment — it is the only copy of
//! those rows, gone. All writes therefore route through
//! `nodedb_wal::segment::atomic_write_fsync` (data fsynced before the rename,
//! parent directory fsynced after it) rather than a bare `fs::write`.
//!
//! `partition.meta` is written LAST and is the commit point: the boot-side
//! registry load treats a partition directory without a readable meta as
//! nothing, so a crash part-way through leaves an unreferenced directory rather
//! than a partition missing columns.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use nodedb_types::timeseries::{PartitionMeta, PartitionState};
use nodedb_wal::crypto::WalEncryptionKey;

use super::super::columnar_memtable::{ColumnType, ColumnarFlushView, ColumnarSchema};
use super::codec::encode_column;
use super::encrypt::encrypt_file;
use super::error::SegmentError;
use super::schema::schema_to_json;
use super::util::dir_size;

/// Writes drained columnar memtable data to a partition directory.
pub struct ColumnarSegmentWriter {
    base_dir: PathBuf,
}

impl ColumnarSegmentWriter {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Write a flush payload to a partition directory.
    ///
    /// Takes a BORROWED [`ColumnarFlushView`] rather than an owned drain result
    /// so the caller can land the segment before the rows leave the memtable.
    ///
    /// When `kek` is `Some`, every output file is wrapped in a `SEGT`
    /// AES-256-GCM envelope before being written to disk.
    pub fn write_partition(
        &self,
        partition_name: &str,
        view: &ColumnarFlushView<'_>,
        interval_ms: u64,
        flush_wal_lsn: u64,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<PartitionMeta, SegmentError> {
        let partition_dir = self.base_dir.join(partition_name);
        std::fs::create_dir_all(&partition_dir)
            .map_err(|e| SegmentError::Io(format!("create dir: {e}")))?;

        let mut column_stats = HashMap::new();
        let mut resolved_codecs = Vec::with_capacity(view.schema.columns.len());

        // Column FILES are named from the memtable's schema, and the partition
        // records that same schema as its own — so a partition inherits whatever
        // column names the memtable was built with, permanently. Nothing here
        // re-derives them from the collection's declaration at read time.
        //
        // That is only sound because a memtable is always built from the
        // DECLARED schema: `initial_ts_schema` resolves it from `doc_configs`,
        // which the boot path seeds from the durable catalog BEFORE WAL replay
        // (`seed_catalog_state` -> `replay_wal_and_rebuild_indexes`). For a
        // declared collection to reach the inference fallback and flush a
        // partition under inferred names — `timestamp.col` instead of the
        // declared TIME_KEY — the seed would have to arrive empty, which needs
        // one of: the catalog unreadable at boot (an error, see
        // `CatalogForRead::open`), a core spawned without `doc_config_seed`,
        // or the collection missing from the catalog. All three are
        // boot-integrity failures, not steady state.
        //
        // If that ever regresses, the damage is durable and silent: those
        // partitions keep projecting under the inferred name after the
        // regression is fixed, because each partition is read against its own
        // stored schema. The repair would be a rename at partition load keyed
        // on `schema.timestamp_idx` — the time column's identity is positional,
        // so the mapping is unambiguous and needs no rewrite.
        for (i, (col_name, col_type)) in view.schema.columns.iter().enumerate() {
            let col_data = &view.columns[i];
            let requested_codec = view.schema.codec(i);

            let (encoded, resolved_codec, stats) =
                encode_column(col_data, *col_type, requested_codec)?;

            let file_bytes = maybe_encrypt(kek, &encoded)?;
            durable_write(&partition_dir, &format!("{col_name}.col"), &file_bytes)?;

            // Write symbol dictionary for tag columns.
            if *col_type == ColumnType::Symbol
                && let Some(dict) = view.symbol_dicts.get(&i)
            {
                let dict_json = sonic_rs::to_vec(dict)
                    .map_err(|e| SegmentError::Io(format!("serialize dict: {e}")))?;
                let sym_bytes = maybe_encrypt(kek, &dict_json)?;
                durable_write(&partition_dir, &format!("{col_name}.sym"), &sym_bytes)?;
            }

            column_stats.insert(col_name.clone(), stats);
            resolved_codecs.push(resolved_codec);
        }

        // Write schema with resolved codecs.
        let schema_with_codecs = ColumnarSchema {
            columns: view.schema.columns.clone(),
            timestamp_idx: view.schema.timestamp_idx,
            codecs: resolved_codecs
                .iter()
                .map(|c| c.into_column_codec())
                .collect(),
        };
        let schema_json = sonic_rs::to_vec(&schema_to_json(&schema_with_codecs))
            .map_err(|e| SegmentError::Io(format!("serialize schema: {e}")))?;
        let schema_bytes = maybe_encrypt(kek, &schema_json)?;
        durable_write(&partition_dir, "schema.json", &schema_bytes)?;

        // Build and write sparse index.
        let sparse_idx = super::super::sparse_index::SparseIndex::build(
            view.columns,
            view.schema,
            view.row_count,
            super::super::sparse_index::DEFAULT_BLOCK_SIZE,
        );
        let sparse_bytes = sparse_idx.to_bytes();
        let sparse_file_bytes = maybe_encrypt(kek, &sparse_bytes)?;
        durable_write(&partition_dir, "sparse_index.bin", &sparse_file_bytes)?;

        let size_bytes = dir_size(&partition_dir)?;

        let meta = PartitionMeta {
            min_ts: view.min_ts,
            max_ts: view.max_ts,
            row_count: view.row_count,
            size_bytes,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms,
            last_flushed_wal_lsn: flush_wal_lsn,
            column_stats,
            max_system_ts: view.max_system_ts,
        };

        let meta_json = sonic_rs::to_vec(&meta)
            .map_err(|e| SegmentError::Io(format!("serialize meta: {e}")))?;
        let meta_bytes = maybe_encrypt(kek, &meta_json)?;
        // Last, and the commit point: a reader that cannot read a meta treats
        // the whole directory as absent.
        durable_write(&partition_dir, "partition.meta", &meta_bytes)?;

        // The files above each fsynced `partition_dir`; this fsyncs the
        // directory that NAMES it, without which the whole partition can be
        // missing after power loss even though every file inside it landed.
        nodedb_wal::segment::fsync_directory(&self.base_dir)
            .map_err(|e| SegmentError::Io(format!("fsync {}: {e}", self.base_dir.display())))?;

        Ok(meta)
    }
}

/// Write the segment file `name` inside `dir` durably: data fsynced before the
/// rename, parent directory fsynced after it.
///
/// Routed through the shared `nodedb_wal::segment::atomic_write_fsync` rather
/// than re-implemented, so the ordering cannot drift per call site. That helper
/// also rejects a column name that is not one plain path component.
fn durable_write(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), SegmentError> {
    nodedb_wal::segment::atomic_write_fsync(dir, name, bytes)
        .map_err(|e| SegmentError::Io(format!("write {}: {e}", dir.join(name).display())))
}

/// Encrypt `bytes` with `kek` if present, otherwise return as-is.
fn maybe_encrypt(kek: Option<&WalEncryptionKey>, bytes: &[u8]) -> Result<Vec<u8>, SegmentError> {
    match kek {
        Some(key) => encrypt_file(key, bytes),
        None => Ok(bytes.to_vec()),
    }
}

/// Ensure that encrypted-file detection is accessible from tests.
#[cfg(test)]
pub(super) fn file_is_encrypted(bytes: &[u8]) -> Result<bool, SegmentError> {
    super::encrypt::is_encrypted(bytes)
}

#[cfg(test)]
mod tests {
    use nodedb_codec::{ColumnCodec, ResolvedColumnCodec};
    use nodedb_types::timeseries::MetricSample;
    use tempfile::TempDir;

    use super::super::super::columnar_memtable::{
        ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, TimeKind,
    };
    use super::super::reader::ColumnarSegmentReader;
    use super::*;

    const MILLIS: ColumnType = ColumnType::Timestamp(TimeKind::Millis);

    fn test_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    fn test_kek() -> WalEncryptionKey {
        WalEncryptionKey::from_bytes(&[0x42u8; 32]).unwrap()
    }

    fn build_simple_drain() -> (
        TempDir,
        crate::engine::timeseries::columnar_memtable::ColumnarDrainResult,
    ) {
        let tmp = TempDir::new().unwrap();
        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..100 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_000_000 + i * 1000,
                    value: i as f64 * 2.0,
                },
            );
        }
        (tmp, mt.drain())
    }

    #[test]
    fn write_and_read_simple_partition() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..100 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1000 + i,
                    value: i as f64 * 0.5,
                },
            );
        }
        let drain = mt.drain();

        let meta = writer
            .write_partition("ts-test", &drain.view(), 86_400_000, 42, None)
            .unwrap();
        assert_eq!(meta.row_count, 100);
        assert_eq!(meta.min_ts, 1000);
        assert_eq!(meta.max_ts, 1099);
        assert_eq!(meta.last_flushed_wal_lsn, 42);
        assert!(meta.size_bytes > 0);

        assert!(meta.column_stats.contains_key("timestamp"));
        assert!(meta.column_stats.contains_key("value"));
        let ts_stats = &meta.column_stats["timestamp"];
        assert_eq!(ts_stats.count, 100);
        assert_eq!(ts_stats.codec, ResolvedColumnCodec::DoubleDelta);
        let val_stats = &meta.column_stats["value"];
        assert_eq!(val_stats.count, 100);
        assert_eq!(val_stats.codec, ResolvedColumnCodec::Gorilla);

        let part_dir = tmp.path().join("ts-test");
        let read_meta = ColumnarSegmentReader::read_meta(&part_dir, None).unwrap();
        assert_eq!(read_meta.row_count, 100);
        assert!(read_meta.column_stats.contains_key("timestamp"));

        let schema = ColumnarSegmentReader::read_schema(&part_dir, None).unwrap();
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.codec(0), ColumnCodec::DoubleDelta);
        assert_eq!(schema.codec(1), ColumnCodec::Gorilla);

        let ts_col = ColumnarSegmentReader::read_column_with_codec(
            &part_dir,
            "timestamp",
            MILLIS,
            Some(ResolvedColumnCodec::DoubleDelta),
            None,
        )
        .unwrap();
        let timestamps = ts_col.as_timestamps();
        assert_eq!(timestamps.len(), 100);
        assert_eq!(timestamps[0], 1000);
        assert_eq!(timestamps[99], 1099);

        let val_col = ColumnarSegmentReader::read_column_with_codec(
            &part_dir,
            "value",
            ColumnType::Float64,
            Some(ResolvedColumnCodec::Gorilla),
            None,
        )
        .unwrap();
        let values = val_col.as_f64();
        assert_eq!(values.len(), 100);
        assert!((values[0] - 0.0).abs() < f64::EPSILON);
        assert!((values[99] - 49.5).abs() < f64::EPSILON);
    }

    #[test]
    fn write_and_read_with_tags() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), MILLIS),
                ("cpu".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![ColumnCodec::Auto; 3],
        };
        let mut mt = ColumnarMemtable::new(schema, test_config());

        for i in 0..50 {
            let host = if i % 2 == 0 { "prod-1" } else { "prod-2" };
            mt.ingest_row(
                i % 2,
                &[
                    ColumnValue::Timestamp(5000 + i as i64),
                    ColumnValue::Float64(50.0 + i as f64),
                    ColumnValue::Symbol(host.to_string()),
                ],
            )
            .unwrap();
        }
        let drain = mt.drain();

        let meta = writer
            .write_partition("ts-tags", &drain.view(), 86_400_000, 99, None)
            .unwrap();
        assert_eq!(meta.row_count, 50);
        assert_eq!(
            meta.column_stats["host"].codec,
            ResolvedColumnCodec::FastLanesLz4
        );
        assert_eq!(meta.column_stats["host"].cardinality, Some(2));

        let part_dir = tmp.path().join("ts-tags");
        let host_col = ColumnarSegmentReader::read_column_with_codec(
            &part_dir,
            "host",
            ColumnType::Symbol,
            Some(ResolvedColumnCodec::FastLanesLz4),
            None,
        )
        .unwrap();
        assert_eq!(host_col.as_symbols().len(), 50);

        let host_dict = ColumnarSegmentReader::read_symbol_dict(&part_dir, "host", None).unwrap();
        assert_eq!(host_dict.len(), 2);
    }

    #[test]
    fn explicit_codec_override() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), MILLIS),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![ColumnCodec::Gorilla, ColumnCodec::Gorilla],
        };
        let mut mt = ColumnarMemtable::new(schema, test_config());
        for i in 0..100 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_700_000_000_000 + i * 10_000,
                    value: 42.0 + i as f64 * 0.1,
                },
            );
        }
        let drain = mt.drain();
        let meta = writer
            .write_partition("ts-gorilla", &drain.view(), 86_400_000, 0, None)
            .unwrap();

        assert_eq!(
            meta.column_stats["timestamp"].codec,
            ResolvedColumnCodec::Gorilla
        );
        assert_eq!(
            meta.column_stats["value"].codec,
            ResolvedColumnCodec::Gorilla
        );

        let part_dir = tmp.path().join("ts-gorilla");
        let ts_col = ColumnarSegmentReader::read_column_with_codec(
            &part_dir,
            "timestamp",
            MILLIS,
            Some(ResolvedColumnCodec::Gorilla),
            None,
        )
        .unwrap();
        let timestamps = ts_col.as_timestamps();
        assert_eq!(timestamps.len(), 100);
        assert_eq!(timestamps[0], 1_700_000_000_000);
    }

    #[test]
    fn compression_stats_in_metadata() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..10_000 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_700_000_000_000 + i * 10_000,
                    value: 42.0,
                },
            );
        }
        let drain = mt.drain();
        let meta = writer
            .write_partition("ts-stats", &drain.view(), 86_400_000, 0, None)
            .unwrap();

        let ts_stats = &meta.column_stats["timestamp"];
        assert!(ts_stats.compression_ratio() > 3.0);
        assert_eq!(ts_stats.min, Some(1_700_000_000_000.0));
        assert_eq!(ts_stats.count, 10_000);

        let val_stats = &meta.column_stats["value"];
        assert!(val_stats.compression_ratio() > 1.0);
        assert_eq!(val_stats.min, Some(42.0));
        assert_eq!(val_stats.max, Some(42.0));
        assert_eq!(val_stats.sum, Some(420_000.0));
    }

    #[test]
    fn sparse_index_written_during_flush() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..2048 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_700_000_000_000 + i * 10_000,
                    value: (i % 100) as f64,
                },
            );
        }
        let drain = mt.drain();
        writer
            .write_partition("ts-sparse", &drain.view(), 86_400_000, 0, None)
            .unwrap();

        let part_dir = tmp.path().join("ts-sparse");
        let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir, None)
            .unwrap()
            .expect("sparse index should exist");

        assert_eq!(sparse.total_rows(), 2048);
        assert_eq!(sparse.block_count(), 2);
        assert_eq!(sparse.column_names, vec!["timestamp", "value"]);

        assert_eq!(sparse.blocks[0].min_ts, 1_700_000_000_000);
        assert_eq!(sparse.blocks[0].max_ts, 1_700_000_000_000 + 1023 * 10_000);
        assert_eq!(sparse.blocks[1].min_ts, 1_700_000_000_000 + 1024 * 10_000);
    }

    #[test]
    fn columnar_segment_encrypted_at_rest() {
        let kek = test_kek();
        let (tmp, drain) = build_simple_drain();
        let writer = ColumnarSegmentWriter::new(tmp.path());
        writer
            .write_partition("enc-part", &drain.view(), 86_400_000, 1, Some(&kek))
            .unwrap();

        let part_dir = tmp.path().join("enc-part");

        // Each on-disk file must start with SEGT magic.
        for filename in &[
            "timestamp.col",
            "value.col",
            "schema.json",
            "sparse_index.bin",
            "partition.meta",
        ] {
            let bytes = std::fs::read(part_dir.join(filename)).unwrap();
            assert!(
                file_is_encrypted(&bytes).unwrap(),
                "{filename} must start with SEGT when KEK is set"
            );
        }

        // Read-back via reader must succeed with the KEK.
        let meta = ColumnarSegmentReader::read_meta(&part_dir, Some(&kek)).unwrap();
        assert_eq!(meta.row_count, 100);

        let ts_col =
            ColumnarSegmentReader::read_column(&part_dir, "timestamp", MILLIS, Some(&kek)).unwrap();
        assert_eq!(ts_col.as_timestamps().len(), 100);

        let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir, Some(&kek))
            .unwrap()
            .expect("sparse index must exist");
        assert_eq!(sparse.total_rows(), 100);
    }
}
