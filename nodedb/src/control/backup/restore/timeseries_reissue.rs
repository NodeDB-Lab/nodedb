// SPDX-License-Identifier: BUSL-1.1

//! Decode of restored timeseries rows.
//!
//! RESTORE re-issues each collection's current rows as Calvin transactions
//! (`redo_reissue::timeseries`). A row of a partition a TRUNCATE ended is no
//! current row, and never decodes here. Timeseries has no surrogate sidecar:
//! series identity re-derives from the tag columns.

use std::collections::HashMap;

use nodedb_types::columnar::schema::TS_SYSTEM;
use nodedb_types::value::Value;

use crate::Error;
use crate::engine::timeseries::columnar_memtable::{
    ColumnType, ColumnarMemtable, ColumnarMemtableConfig, MemtableSnapshot,
};
use crate::engine::timeseries::row_cells::{insert_non_null, memtable_cell, read_partition_cell};
use crate::types::TsFlushedCollectionBlob;

use super::timeseries_partition::{BackedUpPartition, read_backed_up_partition};

/// Whether a decode keeps each row's `_ts_system` ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SystemCell {
    /// Leave it out: the row as a client wrote it.
    Drop,
    /// Keep it: a RESTORE installs each row at its backed-up ordinal. A
    /// partition with no `_ts_system` column gives each row the partition's
    /// `max_system_ts`, and a memtable with none gives `0`.
    Keep,
}

/// Decode the memtable section plus every flushed partition no TRUNCATE
/// ended of one timeseries collection into live `Value::Object` rows (keyed
/// by column name). A row of an ended partition is no current row.
///
/// `memtable_bytes` is `None` when no resident memtable existed at backup
/// time. `kek` is the segment encryption key, `None` if unconfigured.
pub fn decode_timeseries_live_rows(
    collection: &str,
    memtable_bytes: Option<&[u8]>,
    flushed: &TsFlushedCollectionBlob,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<Vec<Value>> {
    decode_rows(collection, memtable_bytes, flushed, kek, SystemCell::Drop)
}

/// [`decode_timeseries_live_rows`], with each row's `_ts_system` ordinal
/// kept: the rows a RESTORE installs at their backed-up ordinals.
pub fn decode_timeseries_restore_rows(
    collection: &str,
    memtable_bytes: Option<&[u8]>,
    flushed: &TsFlushedCollectionBlob,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<Vec<Value>> {
    decode_rows(collection, memtable_bytes, flushed, kek, SystemCell::Keep)
}

fn decode_rows(
    collection: &str,
    memtable_bytes: Option<&[u8]>,
    flushed: &TsFlushedCollectionBlob,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    system: SystemCell,
) -> crate::Result<Vec<Value>> {
    let mut rows: Vec<Value> = Vec::new();

    if let Some(bytes) = memtable_bytes {
        decode_memtable_rows(collection, bytes, system, &mut rows)?;
    }

    for part in &flushed.partitions {
        let meta = partition_meta(collection, part)?;
        if meta.ended_at.is_some() {
            continue;
        }
        let fallback = (system == SystemCell::Keep).then_some(meta.max_system_ts);
        decode_partition_rows(collection, part, kek, fallback, &mut rows)?;
    }

    Ok(rows)
}

/// The meta of `part`. A partition whose `ended_at` is set holds rows a
/// TRUNCATE ended: no current rows.
fn partition_meta(
    collection: &str,
    part: &crate::types::TsFlushedPartitionBlob,
) -> crate::Result<nodedb_types::timeseries::PartitionMeta> {
    let meta: nodedb_types::timeseries::PartitionMeta = zerompk::from_msgpack(&part.meta_bytes)
        .map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!(
                "restore reissue: decode the meta of partition '{}' of '{collection}': {e}",
                part.dir_name
            ),
        })?;
    Ok(meta)
}

/// Decode the captured memtable snapshot into row objects, appending to `rows`.
fn decode_memtable_rows(
    collection: &str,
    bytes: &[u8],
    system: SystemCell,
    rows: &mut Vec<Value>,
) -> crate::Result<()> {
    let snap: MemtableSnapshot =
        zerompk::from_msgpack(bytes).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("restore reissue: decode timeseries memtable for '{collection}': {e}"),
        })?;
    let mt = ColumnarMemtable::from_snapshot(snap, ColumnarMemtableConfig::default())?;

    let schema = mt.schema();
    let columns: Vec<(usize, String, ColumnType)> = schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, (name, ty))| (i, name.clone(), *ty))
        .collect();

    for idx in 0..mt.row_count() as usize {
        let mut map: HashMap<String, Value> = HashMap::with_capacity(columns.len());
        for (col_idx, name, ty) in &columns {
            if name == TS_SYSTEM && system == SystemCell::Drop {
                continue;
            }
            let cell = memtable_cell(&mt, *col_idx, *ty, idx)
                .map_err(|e| cell_error(collection, name, e))?;
            insert_non_null(&mut map, name, cell);
        }
        if system == SystemCell::Keep {
            map.entry(TS_SYSTEM.to_string())
                .or_insert(Value::Integer(0));
        }
        rows.push(Value::Object(map));
    }
    Ok(())
}

/// A stored cell that does not read: a millisecond count its instant kind
/// cannot carry, or a column that does not match its schema.
fn cell_error(collection: &str, column: &str, e: impl std::fmt::Display) -> Error {
    Error::Storage {
        engine: "timeseries".into(),
        detail: format!("restore reissue: column '{column}' of '{collection}': {e}"),
    }
}

/// Decode one flushed partition directory into row objects, appending to `rows`.
///
/// The partition reads with the same `ColumnarSegmentReader` the live scan
/// path uses (`read_backed_up_partition`), so the decode is byte-faithful to
/// what a query against the segment returns.
fn decode_partition_rows(
    collection: &str,
    part: &crate::types::TsFlushedPartitionBlob,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    system_fallback: Option<i64>,
    rows: &mut Vec<Value>,
) -> crate::Result<()> {
    let BackedUpPartition {
        schema,
        columns: col_data,
        sym_dicts,
    } = read_backed_up_partition(collection, part, kek)?;

    let row_count = col_data.first().map(|c| c.len()).unwrap_or(0);
    for idx in 0..row_count {
        let mut map: HashMap<String, Value> = HashMap::with_capacity(schema.columns.len());
        for (col_i, (name, ty)) in schema.columns.iter().enumerate() {
            if name == TS_SYSTEM && system_fallback.is_none() {
                continue;
            }
            let cell = read_partition_cell(&col_data[col_i], *ty, col_i, &sym_dicts, idx)
                .map_err(|e| cell_error(collection, name, e))?;
            insert_non_null(&mut map, name, cell);
        }
        if let Some(fallback) = system_fallback {
            map.entry(TS_SYSTEM.to_string())
                .or_insert(Value::Integer(fallback));
        }
        rows.push(Value::Object(map));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnData, ColumnValue, ColumnarSchema, TimeKind,
    };
    use nodedb_types::InstantKind;
    use nodedb_types::datetime::NdbDateTime;
    use nodedb_types::timeseries::SeriesId;

    /// `2020-03-05T10:00:00Z` in epoch milliseconds.
    const EARLY_MS: i64 = 1_583_402_400_000;

    const NAIVE: ColumnType = ColumnType::Timestamp(TimeKind::Instant(InstantKind::Naive));
    const UTC: ColumnType = ColumnType::Timestamp(TimeKind::Instant(InstantKind::Utc));
    const MILLIS: ColumnType = ColumnType::Timestamp(TimeKind::Millis);

    fn early() -> NdbDateTime {
        NdbDateTime::from_micros(EARLY_MS * 1_000)
    }

    /// A one-row memtable whose time column has the given type.
    fn memtable_with_time_column(ty: ColumnType) -> ColumnarMemtable {
        let schema = ColumnarSchema {
            columns: vec![
                ("captured_at".into(), ty),
                ("v".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![],
        };
        let mut mt = ColumnarMemtable::new(schema, ColumnarMemtableConfig::default());
        let series: SeriesId = 1;
        mt.ingest_row(
            series,
            &[ColumnValue::Timestamp(EARLY_MS), ColumnValue::Float64(1.5)],
        )
        .expect("ingest one row");
        mt
    }

    /// A memtable time cell is reissued as the value its kind denotes.
    #[test]
    fn a_memtable_time_cell_is_typed_by_its_kind() {
        let mt = memtable_with_time_column(NAIVE);
        assert_eq!(
            memtable_cell(&mt, 0, NAIVE, 0).expect("in range"),
            Value::NaiveDateTime(early())
        );
        let mt = memtable_with_time_column(UTC);
        assert_eq!(
            memtable_cell(&mt, 0, UTC, 0).expect("in range"),
            Value::DateTime(early())
        );
        let mt = memtable_with_time_column(MILLIS);
        assert_eq!(
            memtable_cell(&mt, 0, MILLIS, 0).expect("an integer"),
            Value::Integer(EARLY_MS)
        );
    }

    /// A partition time cell is reissued as the value its kind denotes.
    #[test]
    fn a_partition_time_cell_is_typed_by_its_kind() {
        let data = ColumnData::Timestamp(vec![EARLY_MS]);
        let dicts = HashMap::new();
        assert_eq!(
            read_partition_cell(&data, NAIVE, 0, &dicts, 0).expect("in range"),
            Value::NaiveDateTime(early())
        );
        assert_eq!(
            read_partition_cell(&data, UTC, 0, &dicts, 0).expect("in range"),
            Value::DateTime(early())
        );
        assert_eq!(
            read_partition_cell(&data, MILLIS, 0, &dicts, 0).expect("an integer"),
            Value::Integer(EARLY_MS)
        );
    }

    /// A millisecond count outside the instant range is an error, never a
    /// silently wrong cell.
    #[test]
    fn an_out_of_range_instant_cell_is_an_error() {
        let data = ColumnData::Timestamp(vec![i64::MAX]);
        assert!(read_partition_cell(&data, NAIVE, 0, &HashMap::new(), 0).is_err());
    }
}
