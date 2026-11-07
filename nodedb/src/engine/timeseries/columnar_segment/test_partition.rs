// SPDX-License-Identifier: BUSL-1.1

//! Test fixtures that write small metric partitions to disk.

use std::path::{Path, PathBuf};

use crate::engine::timeseries::columnar_memtable::{
    ColumnValue, ColumnarMemtable, ColumnarMemtableConfig,
};

use super::writer::ColumnarSegmentWriter;

/// Write a plaintext partition `name` under `base` with one row per timestamp
/// and a constant `value` column. Returns the partition directory.
pub(crate) fn write_metric_partition(base: &Path, name: &str, timestamps: &[i64]) -> PathBuf {
    let mut memtable = ColumnarMemtable::new_metric(ColumnarMemtableConfig::default());
    for ts in timestamps {
        memtable
            .ingest_row(1, &[ColumnValue::Timestamp(*ts), ColumnValue::Float64(1.0)])
            .expect("row");
    }
    ColumnarSegmentWriter::new(base)
        .write_partition(name, &memtable.drain().view(), 0, 1, None)
        .expect("write");
    base.join(name)
}

/// [`write_metric_partition`] with `rows` rows at consecutive milliseconds.
pub(crate) fn write_ramp_partition(base: &Path, name: &str, rows: i64) -> PathBuf {
    let timestamps: Vec<i64> = (0..rows).map(|i| 1_000 + i).collect();
    write_metric_partition(base, name, &timestamps)
}
