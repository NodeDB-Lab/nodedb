// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of restored timeseries rows.
//!
//! Snapshot-install writes memtable + partition state directly, with no WAL
//! record or Raft entry — on a multi-replica cluster only the restore-target
//! node gets the data. RESTORE re-issues each collection's rows as a durable
//! `TimeseriesOp::Ingest` (Raft on cluster, WAL + dispatch on single-node).
//! Surrogates are empty: timeseries has no surrogate sidecar, only series
//! identity re-derived from tag columns.

use std::collections::HashMap;

use nodedb_types::RlsWriteCheck;
use nodedb_types::columnar::schema::TS_SYSTEM;
use nodedb_types::datetime::NdbDateTimeError;
use nodedb_types::value::Value;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::engine::timeseries::columnar_memtable::{
    ColumnData, ColumnType, ColumnarMemtable, ColumnarMemtableConfig, MemtableSnapshot,
};
use crate::engine::timeseries::columnar_segment::ColumnarSegmentReader;
use crate::types::TsFlushedCollectionBlob;
use nodedb_physical::physical_plan::TimeseriesOp;

/// Server-stamped reserved column — re-derived by the ingest path, so it must
/// NOT be carried back into the re-issued rows (the ingest handler restamps it).
/// `_ts_valid_from` / `_ts_valid_until` ARE client-provided and preserved.
const TS_SYSTEM_COLUMN: &str = TS_SYSTEM;

/// Decode the memtable section plus every flushed partition of one timeseries
/// collection into live `Value::Object` rows (keyed by column name).
///
/// `memtable_bytes` is `None` when no resident memtable existed at backup
/// time. `kek` is the segment encryption key, `None` if unconfigured.
pub fn decode_timeseries_live_rows(
    collection: &str,
    memtable_bytes: Option<&[u8]>,
    flushed: &TsFlushedCollectionBlob,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<Vec<Value>> {
    let mut rows: Vec<Value> = Vec::new();

    if let Some(bytes) = memtable_bytes {
        decode_memtable_rows(collection, bytes, &mut rows)?;
    }

    for part in &flushed.partitions {
        decode_partition_rows(collection, part, kek, &mut rows)?;
    }

    Ok(rows)
}

/// Decode the captured memtable snapshot into row objects, appending to `rows`.
fn decode_memtable_rows(
    collection: &str,
    bytes: &[u8],
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
            if name == TS_SYSTEM_COLUMN {
                continue;
            }
            let cell = memtable_cell(&mt, *col_idx, *ty, idx)
                .map_err(|e| instant_cell_error(collection, name, e))?;
            insert_non_null(&mut map, name, cell);
        }
        rows.push(Value::Object(map));
    }
    Ok(())
}

/// A stored millisecond count that the column's instant kind cannot carry.
fn instant_cell_error(collection: &str, column: &str, e: NdbDateTimeError) -> Error {
    Error::Storage {
        engine: "timeseries".into(),
        detail: format!("restore reissue: time column '{column}' of '{collection}': {e}"),
    }
}

/// Extract one cell from a memtable column as a `Value`.
///
/// A time column yields the value its kind denotes — a typed instant for a
/// declared `TIMESTAMP` / `TIMESTAMPTZ` key, the integer stored for a
/// `BIGINT` key — so the reissued row carries the cell a client INSERT
/// would have sent.
fn memtable_cell(
    mt: &ColumnarMemtable,
    col_idx: usize,
    ty: ColumnType,
    idx: usize,
) -> Result<Value, NdbDateTimeError> {
    let value = match ty {
        ColumnType::Timestamp(kind) => {
            return kind.cell_value(mt.column(col_idx).as_timestamps()[idx]);
        }
        ColumnType::Int64 => Value::Integer(mt.column(col_idx).as_i64()[idx]),
        ColumnType::Float64 => {
            let v = mt.column(col_idx).as_f64()[idx];
            if v.is_nan() {
                Value::Null
            } else {
                Value::Float(v)
            }
        }
        ColumnType::Symbol => match mt.column(col_idx) {
            ColumnData::Symbol(ids) => mt
                .symbol_dict(col_idx)
                .and_then(|dict| dict.get(ids[idx]))
                .map(|s| Value::String(s.to_string()))
                .unwrap_or(Value::Null),
            ColumnData::DictEncoded {
                ids,
                dictionary,
                valid,
                ..
            } => {
                if valid.get(idx).copied().unwrap_or(false) {
                    dictionary
                        .get(ids[idx] as usize)
                        .map(|s| Value::String(s.clone()))
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        },
    };
    Ok(value)
}

/// Decode one flushed partition directory into row objects, appending to `rows`.
///
/// The partition files are materialized to a temporary directory and read with
/// the SAME `ColumnarSegmentReader` the live scan path uses, so the decode is
/// byte-faithful to what a query against the restored segment would return.
fn decode_partition_rows(
    collection: &str,
    part: &crate::types::TsFlushedPartitionBlob,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    rows: &mut Vec<Value>,
) -> crate::Result<()> {
    let tmp = tempfile::Builder::new()
        .prefix("nodedb-ts-reissue-")
        .tempdir()
        .map_err(Error::Io)?;
    let part_dir = tmp.path();
    for (filename, data) in &part.files {
        std::fs::write(part_dir.join(filename), data).map_err(Error::Io)?;
    }

    let schema = ColumnarSegmentReader::read_schema(part_dir, kek).map_err(|e| Error::Storage {
        engine: "timeseries".into(),
        detail: format!(
            "restore reissue: read schema for partition '{}' of '{collection}': {e}",
            part.dir_name
        ),
    })?;

    let requested: Vec<(String, ColumnType)> = schema.columns.clone();
    let col_data = ColumnarSegmentReader::read_columns(part_dir, &requested, kek).map_err(|e| {
        Error::Storage {
            engine: "timeseries".into(),
            detail: format!(
                "restore reissue: read columns for partition '{}' of '{collection}': {e}",
                part.dir_name
            ),
        }
    })?;

    let mut sym_dicts: HashMap<usize, nodedb_types::timeseries::SymbolDictionary> = HashMap::new();
    for (i, (name, ty)) in schema.columns.iter().enumerate() {
        if *ty == ColumnType::Symbol
            && let Ok(dict) = ColumnarSegmentReader::read_symbol_dict(part_dir, name, kek)
        {
            sym_dicts.insert(i, dict);
        }
    }

    let row_count = col_data.first().map(|c| c.len()).unwrap_or(0);
    for idx in 0..row_count {
        let mut map: HashMap<String, Value> = HashMap::with_capacity(schema.columns.len());
        for (col_i, (name, ty)) in schema.columns.iter().enumerate() {
            if name == TS_SYSTEM_COLUMN {
                continue;
            }
            let cell = partition_cell(&col_data[col_i], *ty, col_i, &sym_dicts, idx)
                .map_err(|e| instant_cell_error(collection, name, e))?;
            insert_non_null(&mut map, name, cell);
        }
        rows.push(Value::Object(map));
    }
    Ok(())
}

/// Extract one cell from a flushed-segment column as a `Value`. The partition
/// schema carries each time column's kind, so the cell is typed the same way
/// a memtable cell is.
fn partition_cell(
    data: &ColumnData,
    ty: ColumnType,
    col_idx: usize,
    sym_dicts: &HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
    idx: usize,
) -> Result<Value, NdbDateTimeError> {
    let value = match ty {
        ColumnType::Timestamp(kind) => {
            return kind.cell_value(data.as_timestamps()[idx]);
        }
        ColumnType::Int64 => Value::Integer(data.as_i64()[idx]),
        ColumnType::Float64 => {
            let v = data.as_f64()[idx];
            if v.is_nan() {
                Value::Null
            } else {
                Value::Float(v)
            }
        }
        ColumnType::Symbol => match data {
            ColumnData::Symbol(ids) => sym_dicts
                .get(&col_idx)
                .and_then(|dict| dict.get(ids[idx]))
                .map(|s| Value::String(s.to_string()))
                .unwrap_or(Value::Null),
            ColumnData::DictEncoded {
                ids,
                dictionary,
                valid,
                ..
            } => {
                if valid.get(idx).copied().unwrap_or(false) {
                    dictionary
                        .get(ids[idx] as usize)
                        .map(|s| Value::String(s.clone()))
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        },
    };
    Ok(value)
}

/// Insert a cell, skipping nulls so a re-issued row carries only present fields
/// (mirrors the ILP / msgpack ingest contract: absent field == not written).
fn insert_non_null(map: &mut HashMap<String, Value>, name: &str, value: Value) {
    if !matches!(value, Value::Null) {
        map.insert(name.to_string(), value);
    }
}

/// Build the durable `TimeseriesOp::Ingest` plan from decoded rows.
///
/// The payload is the native-`Value` msgpack encoding of `Value::Array(rows)`
/// (array of per-row field-keyed maps) — the exact shape the `"msgpack"` ingest
/// handler decodes (`decode_msgpack_rows`). Surrogates are empty: timeseries
/// re-derives series identity from the tag columns.
pub fn build_timeseries_ingest_plan(
    collection: &str,
    rows: Vec<Value>,
) -> crate::Result<PhysicalPlan> {
    let payload =
        nodedb_types::value_to_msgpack(&Value::Array(rows)).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("restore reissue: encode timeseries rows for '{collection}': {e}"),
        })?;

    Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
        payload,
        format: "msgpack".into(),
        wal_lsn: None,
        surrogates: Vec::new(),
        provenance: None,
        // No predicate here: a restore re-issues rows that were already
        // admitted before the backup was taken. The identity that admitted
        // them is not available during restore.
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning: None,
        rls_filters: Vec::new(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{ColumnValue, ColumnarSchema, TimeKind};
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
            partition_cell(&data, NAIVE, 0, &dicts, 0).expect("in range"),
            Value::NaiveDateTime(early())
        );
        assert_eq!(
            partition_cell(&data, UTC, 0, &dicts, 0).expect("in range"),
            Value::DateTime(early())
        );
        assert_eq!(
            partition_cell(&data, MILLIS, 0, &dicts, 0).expect("an integer"),
            Value::Integer(EARLY_MS)
        );
    }

    /// A millisecond count outside the instant range is an error, never a
    /// silently wrong cell.
    #[test]
    fn an_out_of_range_instant_cell_is_an_error() {
        let data = ColumnData::Timestamp(vec![i64::MAX]);
        assert!(partition_cell(&data, NAIVE, 0, &HashMap::new(), 0).is_err());
    }
}
