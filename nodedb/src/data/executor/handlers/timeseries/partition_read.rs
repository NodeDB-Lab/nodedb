// SPDX-License-Identifier: BUSL-1.1

//! Whole-partition column reads for the timeseries scan paths.
//!
//! The partition writer writes `schema.json` and one `.col` file per schema
//! column, so a schema or column that does not read is corruption. A `.sym`
//! file is written only for a symbol column that has a dictionary, so an
//! absent one is no dictionary, while a present one that does not read is
//! corruption. Corruption refuses the read and files one report here. No
//! caller skips a partition or a column it cannot read.

use std::collections::HashMap;
use std::path::Path;

use nodedb_types::timeseries::SymbolDictionary;
use nodedb_wal::crypto::WalEncryptionKey;

use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnarSchema};
use crate::engine::timeseries::columnar_segment::{ColumnarSegmentReader, SegmentError};

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
    require_partition_dir(part_dir, site)?;
    let report =
        |stage: &'static str, err: SegmentError| partition_corrupt(part_dir, stage, site, err);
    let schema =
        ColumnarSegmentReader::read_schema(part_dir, kek).map_err(|e| report("schema", e))?;
    // Prefetch all column files into page cache before reading.
    let all_col_names: Vec<String> = schema.columns.iter().map(|(n, _)| n.clone()).collect();
    crate::data::io::fadvise::prefetch_partition_columns(part_dir, &all_col_names);
    let columns = schema
        .columns
        .iter()
        .map(|(name, ty)| ColumnarSegmentReader::read_column(part_dir, name, *ty, kek).map(Some))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| report("column", e))?;
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

#[cfg(test)]
mod tests {
    use super::*;

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
