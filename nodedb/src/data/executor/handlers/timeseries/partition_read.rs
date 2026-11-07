// SPDX-License-Identifier: BUSL-1.1

//! Whole-partition column reads for the timeseries scan paths.
//!
//! The partition writer writes `schema.json` and one `.col` file per schema
//! column, so a schema or column that does not read is corruption. A `.sym`
//! file is written only for a symbol column that has a dictionary, so an
//! absent one is no dictionary, while a present one that does not read is
//! corruption. A time column that holds no time cells and columns of unequal
//! length are corruption too. Corruption refuses the read and files one
//! report here. No caller skips a partition or a column it cannot read.
//!
//! The paths that serve a query also charge a corrupt partition to the
//! quarantine registry ([`account_partition_read`]). Background merges and
//! rewrites do not: they serve nothing from the partition.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use nodedb_types::timeseries::SymbolDictionary;
use nodedb_wal::crypto::WalEncryptionKey;

use crate::bridge::envelope::Priority;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::io::IoMetrics;
use crate::data::io::uring_reader::UringReader;
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnarSchema};
use crate::engine::timeseries::columnar_segment::{ColumnarSegmentReader, SegmentError};
use crate::storage::quarantine::{CollectionScope, QuarantineRegistry, SegmentKey};
use crate::types::{DatabaseId, TenantId};

/// Every column of one on-disk timeseries partition, read whole.
pub(in crate::data::executor) struct TsPartitionColumns {
    pub schema: ColumnarSchema,
    /// One entry per schema column, in schema order. Every entry is `Some`:
    /// the `Option` matches the partition-row emitters this feeds.
    pub columns: Vec<Option<ColumnData>>,
    /// Dictionary of each symbol column that has one, by column index.
    pub sym_dicts: HashMap<usize, SymbolDictionary>,
}

/// The corruption error for the partition at `part_dir`, with one report
/// filed. Every timeseries path that finds a listed partition unreadable
/// calls this at the site that finds it. `stage` names what failed and
/// `site` names the read path.
pub(crate) fn partition_corrupt(
    part_dir: &Path,
    stage: &'static str,
    site: &'static str,
    detail: impl std::fmt::Display,
) -> crate::Error {
    let err = crate::Error::SegmentCorrupted {
        detail: format!("timeseries partition {}: {detail}", part_dir.display()),
    };
    let partition = part_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    crate::diag::timeseries_partition_unreadable(&err, partition, stage, site);
    err
}

/// The error for a partition the registry lists and the disk lacks, or
/// `Ok` when its directory is present.
pub(crate) fn require_partition_dir(part_dir: &Path, site: &'static str) -> crate::Result<()> {
    if part_dir.is_dir() {
        return Ok(());
    }
    Err(partition_corrupt(
        part_dir,
        "directory",
        site,
        "the partition registry lists it, and its directory is missing",
    ))
}

/// Read the schema, every column, and every symbol dictionary of the
/// partition at `part_dir`. `site` names the read path in the report.
///
/// `Err` when the directory is missing: the caller reads only partitions
/// the registry lists.
///
/// `kek` is the core's timeseries segment key, the key every partition write
/// uses. `None` reads plaintext partitions.
pub(in crate::data::executor) fn read_ts_partition(
    part_dir: &Path,
    site: &'static str,
    kek: Option<&WalEncryptionKey>,
) -> crate::Result<TsPartitionColumns> {
    read_ts_partition_on_core(part_dir, site, kek, None)
}

/// The core's batched reader, and what a read charges to it.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct PartitionIo<'a> {
    pub reader: &'a RefCell<Option<UringReader>>,
    pub priority: Priority,
    pub metrics: &'a IoMetrics,
}

/// [`read_ts_partition`] with the column files batched through the core's
/// io_uring, as the aggregate path reads them. The schema, the meta, and the
/// symbol dictionaries stay single small reads, also as the aggregate path
/// reads them.
pub(in crate::data::executor) fn read_ts_partition_on_core(
    part_dir: &Path,
    site: &'static str,
    kek: Option<&WalEncryptionKey>,
    io: Option<PartitionIo<'_>>,
) -> crate::Result<TsPartitionColumns> {
    require_partition_dir(part_dir, site)?;
    let report =
        |stage: &'static str, err: SegmentError| partition_corrupt(part_dir, stage, site, err);
    let schema =
        ColumnarSegmentReader::read_schema(part_dir, kek).map_err(|e| report("schema", e))?;
    let columns = read_column_files(part_dir, &schema, kek, io).map_err(|e| report("column", e))?;
    if let Some(detail) = partition_shape_error(&schema, &columns) {
        return Err(partition_corrupt(part_dir, "column", site, detail));
    }
    let mut sym_dicts = HashMap::new();
    for (i, (name, ty)) in schema.columns.iter().enumerate() {
        if !ty.is_dictionary() || !part_dir.join(format!("{name}.sym")).exists() {
            continue;
        }
        let dict = ColumnarSegmentReader::read_symbol_dict(part_dir, name, kek)
            .map_err(|e| report("symbol_dict", e))?;
        sym_dicts.insert(i, dict);
    }
    Ok(TsPartitionColumns {
        schema,
        columns,
        sym_dicts,
    })
}

/// Read and decode every column file of `schema`. Files go through the core's
/// batched reader when one is free, else through fadvise and a plain read.
fn read_column_files(
    part_dir: &Path,
    schema: &ColumnarSchema,
    kek: Option<&WalEncryptionKey>,
    io: Option<PartitionIo<'_>>,
) -> Result<Vec<Option<ColumnData>>, SegmentError> {
    let read_alone =
        |name: &str, ty| ColumnarSegmentReader::read_column(part_dir, name, ty, kek).map(Some);
    let Some(buffers) = io.and_then(|io| batch_read_column_files(part_dir, schema, io)) else {
        let names: Vec<String> = schema.columns.iter().map(|(n, _)| n.clone()).collect();
        crate::data::io::fadvise::prefetch_partition_columns(part_dir, &names);
        return schema
            .columns
            .iter()
            .map(|(name, ty)| read_alone(name, *ty))
            .collect();
    };
    let meta = ColumnarSegmentReader::read_meta(part_dir, kek)?;
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, (name, ty))| {
            // The batch returns an empty buffer for a file it did not read.
            // That file reads again on its own, so the error names why.
            let raw = buffers.get(i).map(Vec::as_slice).unwrap_or_default();
            if raw.is_empty() {
                return read_alone(name, *ty);
            }
            let codec = meta.column_stats.get(name).map(|stats| stats.codec);
            ColumnarSegmentReader::decode_column_from_bytes(part_dir, name, *ty, codec, raw, kek)
                .map(Some)
        })
        .collect()
}

/// The bytes of every column file, in schema order. `None` when the core has
/// no reader or it is in use.
fn batch_read_column_files(
    part_dir: &Path,
    schema: &ColumnarSchema,
    io: PartitionIo<'_>,
) -> Option<Vec<Vec<u8>>> {
    let mut slot = io.reader.try_borrow_mut().ok()?;
    let reader = slot.as_mut()?;
    let paths: Vec<std::path::PathBuf> = schema
        .columns
        .iter()
        .map(|(name, _)| part_dir.join(format!("{name}.col")))
        .collect();
    let path_refs: Vec<&Path> = paths.iter().map(|p| p.as_path()).collect();
    Some(reader.read_files_with_priority(&path_refs, io.priority, io.metrics))
}

/// Why `columns` do not form a partition of `schema`. `None` when they do.
///
/// Every reader indexes the columns by a row of the time column, so one
/// shorter column panics a Data Plane core unless it is refused here. A schema
/// with no time column names a column that does not hold time cells.
fn partition_shape_error(
    schema: &ColumnarSchema,
    columns: &[Option<ColumnData>],
) -> Option<String> {
    let idx = schema.timestamp_idx;
    if !schema.columns.get(idx).is_some_and(|(_, ty)| ty.is_time()) {
        return Some(format!("schema column {idx} is not a time column"));
    }
    let row_count = columns.get(idx).and_then(Option::as_ref)?.len();
    schema
        .columns
        .iter()
        .zip(columns)
        .find_map(|((name, _), column)| {
            column
                .as_ref()
                .filter(|column| column.len() != row_count)
                .map(|column| {
                    format!(
                        "column '{name}' holds {} rows, the time column holds {row_count}",
                        column.len()
                    )
                })
        })
}

/// A collection's handle on the quarantine registry, for its partition reads.
pub(in crate::data::executor) struct PartitionQuarantine {
    registry: Arc<QuarantineRegistry>,
    scope: CollectionScope,
}

impl PartitionQuarantine {
    fn key_of(&self, part_dir: &Path) -> SegmentKey {
        SegmentKey::Timeseries {
            scope: self.scope.clone(),
            partition: part_dir
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_string(),
        }
    }
}

impl CoreLoop {
    /// The quarantine handle of `collection`. `None` when no registry is wired.
    pub(in crate::data::executor) fn ts_partition_quarantine(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
    ) -> Option<PartitionQuarantine> {
        let registry = self.quarantine_registry.as_ref()?;
        Some(PartitionQuarantine {
            registry: Arc::clone(registry),
            scope: CollectionScope::new(database_id.as_u64(), tid.as_u64(), collection),
        })
    }

    /// Forget the quarantine records of the partitions `dir_names` of
    /// `collection`. The caller removed those directories or rewrote them.
    pub(in crate::data::executor) fn release_ts_partitions(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        dir_names: &[String],
    ) {
        let Some(registry) = &self.quarantine_registry else {
            return;
        };
        let scope = CollectionScope::new(database_id.as_u64(), tid.as_u64(), collection);
        for partition in dir_names {
            registry.release(&SegmentKey::Timeseries {
                scope: scope.clone(),
                partition: partition.clone(),
            });
        }
    }
}

/// Charge the outcome of a query's read of the partition at `part_dir` to the
/// quarantine registry.
///
/// A success clears pending strikes. The first corruption keeps its error.
/// The second consecutive one quarantines the partition, and its error names
/// the quarantine. Every other error passes through without a strike.
pub(in crate::data::executor) fn account_partition_read<T>(
    quarantine: Option<&PartitionQuarantine>,
    part_dir: &Path,
    read: crate::Result<T>,
) -> crate::Result<T> {
    let Some(quarantine) = quarantine else {
        return read;
    };
    let key = quarantine.key_of(part_dir);
    match read {
        Ok(value) => {
            quarantine.registry.record_success(&key);
            Ok(value)
        }
        Err(crate::Error::SegmentCorrupted { detail }) => {
            let detail = match quarantine.registry.record_failure(key, &detail) {
                Ok(()) => detail,
                Err(quarantined) => format!("{quarantined}: {detail}"),
            };
            Err(crate::Error::SegmentCorrupted { detail })
        }
        Err(other) => Err(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, TimeKind,
    };
    use crate::engine::timeseries::columnar_segment::ColumnarSegmentWriter;
    use crate::engine::timeseries::columnar_segment::test_partition::write_ramp_partition as write_metric_partition;

    /// The column files batch through the core's reader. A batched read gives
    /// the plain read's columns, on an encrypted partition too. A kernel with
    /// no io_uring exercises the fallback instead.
    #[test]
    fn a_batched_read_matches_a_plain_read() {
        let kek = WalEncryptionKey::from_bytes(&[0x42u8; 32]).expect("key");
        let dir = tempfile::tempdir().expect("tempdir");
        let mut memtable = ColumnarMemtable::new_metric(ColumnarMemtableConfig::default());
        for i in 0..10 {
            memtable
                .ingest_row(
                    1,
                    &[
                        ColumnValue::Timestamp(1_000 + i),
                        ColumnValue::Float64(i as f64),
                    ],
                )
                .expect("row");
        }
        ColumnarSegmentWriter::new(dir.path())
            .write_partition("ts-enc", &memtable.drain().view(), 0, 1, Some(&kek))
            .expect("write");
        let part = dir.path().join("ts-enc");
        let reader = RefCell::new(UringReader::new());
        let metrics = IoMetrics::default();
        let io = PartitionIo {
            reader: &reader,
            priority: Priority::Normal,
            metrics: &metrics,
        };

        let batched =
            read_ts_partition_on_core(&part, "test", Some(&kek), Some(io)).expect("batch");
        let plain = read_ts_partition(&part, "test", Some(&kek)).expect("plain");

        for (batched, plain) in batched.columns.iter().zip(&plain.columns) {
            match (batched, plain) {
                (Some(ColumnData::Timestamp(a)), Some(ColumnData::Timestamp(b))) => {
                    assert_eq!(a, b);
                }
                (Some(ColumnData::Float64(a)), Some(ColumnData::Float64(b))) => {
                    assert_eq!(a, b);
                }
                other => panic!("columns differ in shape: {other:?}"),
            }
        }
    }

    /// A schema that names no time column refuses the read. Every scan calls
    /// `as_timestamps` on the column it names, which panics a core.
    #[test]
    fn a_schema_without_a_time_column_is_refused_as_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let part = write_metric_partition(dir.path(), "ts-0001", 5);
        let schema_path = part.join("schema.json");
        let schema = std::fs::read_to_string(&schema_path).expect("read schema");
        assert!(schema.contains("\"type\":\"timestamp\""), "{schema}");
        std::fs::write(
            &schema_path,
            schema.replace("\"type\":\"timestamp\"", "\"type\":\"int64\""),
        )
        .expect("write schema");

        let result = read_ts_partition(&part, "test", None);

        assert!(matches!(
            result,
            Err(crate::Error::SegmentCorrupted { ref detail }) if detail.contains("not a time column")
        ));
    }

    /// A column shorter than the time column refuses the read. Every row
    /// emitter indexes it by a row of the time column.
    #[test]
    fn a_short_column_is_refused_as_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let full = write_metric_partition(dir.path(), "ts-full", 10);
        let short = write_metric_partition(dir.path(), "ts-short", 3);
        std::fs::copy(short.join("value.col"), full.join("value.col")).expect("swap column");

        assert!(matches!(
            read_ts_partition(&full, "test", None),
            Err(crate::Error::SegmentCorrupted { .. })
        ));
    }

    #[test]
    fn unequal_columns_name_the_short_column() {
        let schema = ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("v".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        };
        let columns = vec![
            Some(ColumnData::Timestamp(vec![1, 2])),
            Some(ColumnData::Float64(vec![1.0])),
        ];
        let detail = partition_shape_error(&schema, &columns).expect("unequal");
        assert!(detail.contains("'v'"), "{detail}");
    }

    /// Two consecutive corrupt reads quarantine the partition under its
    /// collection. A success between them clears the strike.
    #[test]
    fn corrupt_reads_charge_the_quarantine_registry() {
        let quarantine = PartitionQuarantine {
            registry: Arc::new(QuarantineRegistry::new()),
            scope: CollectionScope::new(1, 2, "metrics"),
        };
        let part = Path::new("/data/ts/ts-0001");
        let corrupt = || -> crate::Result<()> {
            Err(crate::Error::SegmentCorrupted {
                detail: "bad column".into(),
            })
        };
        let key = quarantine.key_of(part);

        assert!(account_partition_read(Some(&quarantine), part, corrupt()).is_err());
        account_partition_read(Some(&quarantine), part, Ok(())).expect("clean read");
        assert!(account_partition_read(Some(&quarantine), part, corrupt()).is_err());
        assert!(!quarantine.registry.is_quarantined(&key));

        let second = account_partition_read(Some(&quarantine), part, corrupt());
        assert!(quarantine.registry.is_quarantined(&key));
        assert!(matches!(
            second,
            Err(crate::Error::SegmentCorrupted { ref detail })
                if detail.contains("quarantined") && detail.contains("bad column")
        ));
    }

    #[test]
    fn other_errors_charge_no_strike() {
        let quarantine = PartitionQuarantine {
            registry: Arc::new(QuarantineRegistry::new()),
            scope: CollectionScope::new(1, 2, "metrics"),
        };
        let part = Path::new("/data/ts/ts-0001");
        for _ in 0..2 {
            let other = account_partition_read::<()>(
                Some(&quarantine),
                part,
                Err(crate::Error::Internal {
                    detail: "divide by zero".into(),
                }),
            );
            assert!(matches!(other, Err(crate::Error::Internal { .. })));
        }
        assert!(!quarantine.registry.is_quarantined(&quarantine.key_of(part)));
    }

    #[test]
    fn a_partition_without_a_schema_is_refused_as_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = read_ts_partition(dir.path(), "test", None);
        assert!(matches!(result, Err(crate::Error::SegmentCorrupted { .. })));
    }

    /// A partition written with the segment key reads back with the same key,
    /// and does not read without it.
    #[test]
    fn an_encrypted_partition_reads_with_the_segment_key() {
        use crate::engine::timeseries::columnar_memtable::{
            ColumnarMemtable, ColumnarMemtableConfig,
        };
        use crate::engine::timeseries::columnar_segment::ColumnarSegmentWriter;

        let kek = WalEncryptionKey::from_bytes(&[0x42u8; 32]).expect("key");
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
        ColumnarSegmentWriter::new(dir.path())
            .write_partition("ts-enc", &drain.view(), 0, 1, Some(&kek))
            .expect("write");
        let part_dir = dir.path().join("ts-enc");

        let read = read_ts_partition(&part_dir, "test", Some(&kek)).expect("reads with the key");
        assert_eq!(read.columns.len(), read.schema.columns.len());
        let timestamps = read.columns[read.schema.timestamp_idx]
            .as_ref()
            .expect("timestamp column");
        assert_eq!(timestamps.len(), 10);

        assert!(
            read_ts_partition(&part_dir, "test", None).is_err(),
            "an encrypted partition does not read without the key"
        );
    }

    #[test]
    fn a_listed_partition_without_a_directory_is_refused_as_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = read_ts_partition(&dir.path().join("ts-0001"), "test", None);
        assert!(matches!(
            result,
            Err(crate::Error::SegmentCorrupted { ref detail }) if detail.contains("directory is missing")
        ));
    }
}
