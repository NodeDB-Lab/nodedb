// SPDX-License-Identifier: BUSL-1.1

//! A timeseries ingest resolved to the exact rows it stores.
//!
//! Every timeseries ingest resolves once, before its log record exists: the
//! Data Plane parses the lines, evolves a scratch copy of the collection's
//! schema for them, and keeps each line the memtable accepts. A standalone
//! write resolves before its WAL append. A Raft proposal and a sequenced
//! transaction resolve on the node that proposes or submits them, never on
//! each replica. The record carries the result, and every install stores
//! exactly these rows. Restart replay and WAL catch-up read the same rows, so
//! the stored rows, the write events and the rebuilt events agree.
//!
//! A batch names the schema its rows were resolved against. A gate-admitted
//! live install whose collection schema no longer evolves to that schema
//! refuses the batch under [`TsDriftPolicy::Refuse`]: its record is
//! cancelled, and the writer resolves again. Every other install (a
//! committed log entry, a committed transaction, restart replay) never
//! refuses, and stores each value under its column name.

use super::columnar_memtable::{ColumnType, ColumnValue};

/// The ingest format of a resolved batch.
pub const RESOLVED_INGEST_FORMAT: &str = "ts-resolved";

/// The ingest format of a clone copy: MessagePack rows, as the `msgpack`
/// format reads them, each carrying its source version's system-time ordinal
/// in `_ts_system`. Each row keeps that ordinal: the copy is the same logical
/// version as the source row.
pub const CLONE_COPY_INGEST_FORMAT: &str = "msgpack-clone-copy";

/// What an install does when the collection schema no longer matches the
/// schema the batch was resolved against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum TsDriftPolicy {
    /// Refuse the whole batch with nothing stored. The writer resolves again.
    Refuse = 0,
    /// Store each value under its column name. Columns the batch does not
    /// name take their empty value.
    ApplyByName = 1,
}

/// One resolved row.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct ResolvedTsRow {
    /// Index of the input line the row came from. It names the row's
    /// surrogate in the ingest's `surrogates`.
    pub line: u64,
    /// The row's time-column value, in epoch milliseconds.
    pub timestamp_ms: i64,
    /// The row's values, in the order of the batch's `columns`.
    pub values: Vec<ColumnValue>,
    /// Positions in the batch's `columns` the row's line did not name. Each
    /// holds its column's empty value, which stands for no value, not a
    /// value of that type.
    pub absent: Vec<u32>,
    /// The row as a scan reads it, a MessagePack map: its write-event image
    /// and its `RETURNING` row. Empty when the batch emits no event and the
    /// statement projects nothing.
    pub image: Vec<u8>,
}

/// A timeseries ingest resolved to its rows.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct ResolvedTsBatch {
    /// The measurement every row's series key names.
    pub measurement: String,
    /// The schema the rows were resolved against, in memtable column order.
    pub columns: Vec<(String, ColumnType)>,
    /// Index of the designated time column in `columns`.
    pub timestamp_idx: u64,
    pub drift: TsDriftPolicy,
    /// The statement instant, in epoch milliseconds. Every untimed row and
    /// every bitemporal row's system time took it at resolve, so restart
    /// replay stores the same values.
    pub now_ms: i64,
    /// Resident bytes of the scratch memtable that took the rows. An install
    /// flushes first when the live memtable cannot also hold this many.
    pub resolved_bytes: u64,
    /// Whether some Event Plane consumer read the collection at resolve. The
    /// install emits one Insert per row, and WAL catch-up rebuilds the same
    /// events, only when this is set. Every row then carries its image.
    pub emits_events: bool,
    /// The rows the memtable accepted, in input order. A line the memtable
    /// rejected has no row.
    pub rows: Vec<ResolvedTsRow>,
    /// The number of lines the resolve rejected. Every install reports it as
    /// the ingest's `rejected` count.
    pub rejected: u64,
    /// Why the resolve rejected its first rejected line. `None` when it
    /// rejected none.
    pub first_rejection: Option<String>,
    /// Every row keeps the `_ts_system` ordinal it carries: a clone copy of
    /// source rows. Otherwise the install stamps every row with the batch's
    /// ordinal.
    pub carried_system: bool,
}

/// The schema a resolve resolves against in place of the live one: the
/// schema an earlier ingest of the same transaction into the same collection
/// resolved to. A transaction's ingests chain through it, the chain its
/// COMMIT-time resolve follows.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct ResolveBase {
    /// The resolved schema's columns, in memtable column order.
    pub columns: Vec<(String, ColumnType)>,
    /// Index of the designated time column in `columns`.
    pub timestamp_idx: u64,
}

impl ResolveBase {
    /// The schema `batch` resolved to.
    pub fn of(batch: &ResolvedTsBatch) -> Self {
        Self {
            columns: batch.columns.clone(),
            timestamp_idx: batch.timestamp_idx,
        }
    }

    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("timeseries resolve base encode: {e}"),
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("timeseries resolve base decode: {e}"),
        })
    }

    /// The base `schema` names.
    pub fn from_schema(schema: &super::columnar_memtable::ColumnarSchema) -> Self {
        Self {
            columns: schema.columns.clone(),
            timestamp_idx: schema.timestamp_idx as u64,
        }
    }

    /// The memtable schema of this base. Every column takes the automatic
    /// codec: a resolve reads the schema's columns, not its codecs.
    pub fn schema(&self) -> super::columnar_memtable::ColumnarSchema {
        super::columnar_memtable::ColumnarSchema {
            columns: self.columns.clone(),
            timestamp_idx: usize::try_from(self.timestamp_idx).unwrap_or(0),
            codecs: vec![nodedb_codec::ColumnCodec::Auto; self.columns.len()],
        }
    }
}

impl ResolvedTsBatch {
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("resolved timeseries batch: {e}"),
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("resolved timeseries batch: {e}"),
        })
    }

    /// The same batch under `drift`.
    pub fn with_drift(mut self, drift: TsDriftPolicy) -> Self {
        self.drift = drift;
        self
    }
}

/// The empty value a column the batch does not name takes, as a memtable
/// fills a column added after rows landed. A symbol, text or float column
/// takes SQL NULL, never an empty string. Integer and time columns have no NULL.
pub fn empty_value(column_type: ColumnType) -> ColumnValue {
    match column_type {
        ColumnType::Timestamp(_) => ColumnValue::Timestamp(0),
        ColumnType::Float64 | ColumnType::Symbol | ColumnType::Text => ColumnValue::Null,
        ColumnType::Int64 => ColumnValue::Int64(0),
    }
}

/// Whether `value` can be stored in a column of `column_type`.
pub fn value_fits(value: &ColumnValue, column_type: ColumnType) -> bool {
    matches!(
        (value, column_type),
        (ColumnValue::Timestamp(_), ColumnType::Timestamp(_))
            | (ColumnValue::Float64(_), ColumnType::Float64)
            | (ColumnValue::Int64(_), ColumnType::Int64)
            | (
                ColumnValue::Symbol(_),
                ColumnType::Symbol | ColumnType::Text
            )
            | (
                ColumnValue::Null,
                ColumnType::Symbol | ColumnType::Text | ColumnType::Float64
            )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::TimeKind;

    #[test]
    fn a_batch_round_trips_its_rows_and_schema() {
        let batch = ResolvedTsBatch {
            measurement: "cpu".into(),
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("host".into(), ColumnType::Symbol),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            drift: TsDriftPolicy::Refuse,
            now_ms: 1_000,
            resolved_bytes: 64,
            emits_events: true,
            rows: vec![ResolvedTsRow {
                line: 0,
                timestamp_ms: 1_000,
                values: vec![
                    ColumnValue::Timestamp(1_000),
                    ColumnValue::Symbol("a".into()),
                    ColumnValue::Float64(1.5),
                ],
                absent: vec![2],
                image: vec![0x80],
            }],
            rejected: 1,
            first_rejection: Some("type conflict on 'value'".into()),
            carried_system: false,
        };
        let bytes = batch.to_bytes().expect("encode batch");
        assert_eq!(
            ResolvedTsBatch::from_bytes(&bytes).expect("decode batch"),
            batch
        );
    }

    #[test]
    fn a_value_fits_only_its_own_column_type() {
        assert!(value_fits(&ColumnValue::Float64(1.0), ColumnType::Float64));
        assert!(!value_fits(&ColumnValue::Float64(1.0), ColumnType::Symbol));
        assert!(!value_fits(
            &ColumnValue::Symbol("x".into()),
            ColumnType::Int64
        ));
        assert!(value_fits(&ColumnValue::Null, ColumnType::Symbol));
        assert!(value_fits(&ColumnValue::Null, ColumnType::Float64));
        assert!(!value_fits(&ColumnValue::Null, ColumnType::Int64));
    }

    /// A symbol column a row does not name is NULL, never `''`.
    #[test]
    fn an_unnamed_symbol_is_null() {
        assert_eq!(empty_value(ColumnType::Symbol), ColumnValue::Null);
        assert_eq!(empty_value(ColumnType::Float64), ColumnValue::Null);
    }

    /// A NULL cell survives the batch encoding the WAL carries.
    #[test]
    fn a_null_cell_round_trips() {
        let row = ResolvedTsRow {
            line: 0,
            timestamp_ms: 1_000,
            values: vec![ColumnValue::Timestamp(1_000), ColumnValue::Null],
            absent: vec![1],
            image: Vec::new(),
        };
        let bytes = zerompk::to_msgpack_vec(&row).expect("encode row");
        let back: ResolvedTsRow = zerompk::from_msgpack(&bytes).expect("decode row");
        assert_eq!(back, row);
    }
}
