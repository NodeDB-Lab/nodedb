// SPDX-License-Identifier: BUSL-1.1

//! Data types for the columnar timeseries memtable.

use std::collections::HashMap;

use nodedb_types::InstantKind;
use nodedb_types::columnar::schema::TS_SYSTEM;
use nodedb_types::timeseries::{SeriesId, SymbolDictionary};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// What a millisecond time column denotes when its cells are read.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum TimeKind {
    /// An integer count of milliseconds, read back as the integer stored.
    Millis,
    /// A declared TIMESTAMP (naive) or TIMESTAMPTZ (utc) instant.
    Instant(InstantKind),
}

/// Column data type in a columnar memtable.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ColumnType {
    /// Time column (i64 millis). The kind says how a cell is read back.
    Timestamp(TimeKind),
    /// Floating-point metric value.
    Float64,
    /// Integer metric value.
    Int64,
    /// Tag column: a series dimension, stored as u32 symbol IDs. Every
    /// non-NULL symbol cell is part of the row's series key.
    Symbol,
    /// String field: not a series dimension. It is stored as u32 ids into a
    /// per-column dictionary, like a symbol, without the tag cardinality cap.
    Text,
}

impl ColumnType {
    /// Whether this is a time column of any kind.
    pub fn is_time(&self) -> bool {
        matches!(self, ColumnType::Timestamp(_))
    }

    /// Whether the column stores string cells as ids into a per-column
    /// dictionary: a symbol tag or a text field.
    pub fn is_dictionary(&self) -> bool {
        matches!(self, ColumnType::Symbol | ColumnType::Text)
    }

    /// The SQL DDL type name a client sees for this column (schema-preview
    /// responses, catalog field lists).
    pub fn ddl_type_name(&self) -> &'static str {
        match self {
            ColumnType::Timestamp(TimeKind::Instant(InstantKind::Utc)) => "TIMESTAMPTZ",
            ColumnType::Timestamp(TimeKind::Instant(InstantKind::Naive) | TimeKind::Millis) => {
                "TIMESTAMP"
            }
            ColumnType::Float64 => "FLOAT",
            ColumnType::Int64 => "BIGINT",
            ColumnType::Symbol => "SYMBOL",
            ColumnType::Text => "VARCHAR",
        }
    }
}

/// Schema for a columnar memtable (column names + types, in order).
#[derive(Debug, Clone)]
pub struct ColumnarSchema {
    pub columns: Vec<(String, ColumnType)>,
    /// Index of the designated timestamp column.
    pub timestamp_idx: usize,
    /// Per-column codec selection. When empty or shorter than `columns`,
    /// missing entries default to `Auto`.
    pub codecs: Vec<nodedb_codec::ColumnCodec>,
}

impl ColumnarSchema {
    /// Create a schema with a timestamp + one f64 value column (simplest case).
    pub fn metric_default() -> Self {
        Self {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![
                nodedb_codec::ColumnCodec::Auto,
                nodedb_codec::ColumnCodec::Auto,
            ],
        }
    }

    /// Get the codec for column at index `i`. Returns `Auto` if not specified.
    pub fn codec(&self, i: usize) -> nodedb_codec::ColumnCodec {
        self.codecs
            .get(i)
            .copied()
            .unwrap_or(nodedb_codec::ColumnCodec::Auto)
    }

    /// Index of the reserved `_ts_system` column, or `None` when the schema
    /// carries none.
    pub fn ts_system_idx(&self) -> Option<usize> {
        self.columns.iter().position(|(n, _)| n == TS_SYSTEM)
    }

    /// Position of the `_ts_series` column, which holds each row's series
    /// id. `None` when the schema has none.
    pub fn ts_series_idx(&self) -> Option<usize> {
        self.columns
            .iter()
            .position(|(n, _)| n == nodedb_types::columnar::schema::TS_SERIES)
    }
}

// ---------------------------------------------------------------------------
// Column storage
// ---------------------------------------------------------------------------

/// A single column of data in the memtable.
#[derive(Debug)]
pub enum ColumnData {
    Timestamp(Vec<i64>),
    Float64(Vec<f64>),
    Int64(Vec<i64>),
    Symbol(Vec<u32>),
    /// Dictionary-encoded string column for low-cardinality tags.
    ///
    /// Predicates operate on compact integer IDs rather than string bytes —
    /// an `O(dict_size * string_len + N)` win over `O(N * string_len)`.
    DictEncoded {
        /// Row-level symbol IDs (index into `dictionary`).
        ids: Vec<u32>,
        /// ID → string value.
        dictionary: Vec<String>,
        /// Reverse lookup: string → ID.
        reverse: std::collections::HashMap<String, u32>,
        /// Validity bitmap: false = null row.
        valid: Vec<bool>,
    },
}

impl ColumnData {
    pub(super) fn new(ty: ColumnType) -> Self {
        match ty {
            ColumnType::Timestamp(_) => Self::Timestamp(Vec::with_capacity(4096)),
            ColumnType::Float64 => Self::Float64(Vec::with_capacity(4096)),
            ColumnType::Int64 => Self::Int64(Vec::with_capacity(4096)),
            ColumnType::Symbol | ColumnType::Text => Self::Symbol(Vec::with_capacity(4096)),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Timestamp(v) => v.len(),
            Self::Float64(v) => v.len(),
            Self::Int64(v) => v.len(),
            Self::Symbol(v) => v.len(),
            Self::DictEncoded { ids, .. } => ids.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate memory usage in bytes.
    pub(super) fn memory_bytes(&self) -> usize {
        match self {
            Self::Timestamp(v) => v.capacity() * 8,
            Self::Float64(v) => v.capacity() * 8,
            Self::Int64(v) => v.capacity() * 8,
            Self::Symbol(v) => v.capacity() * 4,
            Self::DictEncoded {
                ids,
                dictionary,
                reverse,
                valid,
            } => {
                ids.capacity() * 4
                    + dictionary.iter().map(|s| s.len() + 24).sum::<usize>()
                    + reverse.len() * 56 // rough HashMap entry cost
                    + valid.capacity()
            }
        }
    }

    /// Deep clone the column data.
    pub fn clone_data(&self) -> Self {
        match self {
            Self::Timestamp(v) => Self::Timestamp(v.clone()),
            Self::Float64(v) => Self::Float64(v.clone()),
            Self::Int64(v) => Self::Int64(v.clone()),
            Self::Symbol(v) => Self::Symbol(v.clone()),
            Self::DictEncoded {
                ids,
                dictionary,
                reverse,
                valid,
            } => Self::DictEncoded {
                ids: ids.clone(),
                dictionary: dictionary.clone(),
                reverse: reverse.clone(),
                valid: valid.clone(),
            },
        }
    }

    /// Get timestamp column as slice. Type mismatch is unreachable in correct usage.
    pub(crate) fn as_timestamps(&self) -> &[i64] {
        match self {
            Self::Timestamp(v) => v,
            _ => unreachable!(
                "invariant: ColumnarMemtable columns are constructed by ColumnData::new(ty) keyed to schema; Timestamp type mismatch is impossible"
            ),
        }
    }

    /// Get f64 column as slice. Type mismatch is unreachable in correct usage.
    pub(crate) fn as_f64(&self) -> &[f64] {
        match self {
            Self::Float64(v) => v,
            _ => unreachable!(
                "invariant: ColumnarMemtable columns are constructed by ColumnData::new(ty) keyed to schema; Float64 type mismatch is impossible"
            ),
        }
    }

    /// Get i64 column as slice. Type mismatch is unreachable in correct usage.
    pub(crate) fn as_i64(&self) -> &[i64] {
        match self {
            Self::Int64(v) => v,
            _ => unreachable!(
                "invariant: ColumnarMemtable columns are constructed by ColumnData::new(ty) keyed to schema; Int64 type mismatch is impossible"
            ),
        }
    }

    /// Get symbol column as slice. Type mismatch is unreachable in correct usage.
    pub(crate) fn as_symbols(&self) -> &[u32] {
        match self {
            Self::Symbol(v) => v,
            _ => unreachable!(
                "invariant: ColumnarMemtable columns are constructed by ColumnData::new(ty) keyed to schema; Symbol type mismatch is impossible"
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Config and drain result
// ---------------------------------------------------------------------------

/// Configuration for the columnar memtable.
#[derive(Debug, Clone)]
pub struct ColumnarMemtableConfig {
    /// Soft budget: reaching it makes `ingest_row` report `FlushNeeded` so the
    /// caller can flush at a record boundary. Rows are still accepted.
    pub max_memory_bytes: usize,
    /// Hard memory ceiling. The memtable itself does NOT enforce this — it is
    /// the ingest path's record-boundary admission gate that reads it and
    /// flushes BEFORE a record rather than partway through one. Enforcing it
    /// per-row here is what produced a partial-record flush, and a partition
    /// holding part of a WAL record cannot be stamped with any LSN truthfully.
    pub hard_memory_limit: usize,
    /// Maximum tag cardinality per symbol column, per memtable generation.
    pub max_tag_cardinality: u32,
}

impl ColumnarMemtableConfig {
    /// Build the memtable's limits from the operator's `[tuning.timeseries]`
    /// section. The single translation point from config to memtable, so the
    /// two cannot describe different budgets.
    pub fn from_tuning(tuning: &nodedb_types::config::tuning::TimeseriesToning) -> Self {
        Self {
            max_memory_bytes: tuning.memtable_budget_bytes,
            hard_memory_limit: tuning.memtable_hard_limit_bytes,
            max_tag_cardinality: tuning.max_tag_cardinality,
        }
    }
}

impl Default for ColumnarMemtableConfig {
    /// Delegates to the tuning defaults (64 MiB / 80 MiB / 100k) rather than
    /// restating them, so an operator-facing default and a code default can
    /// never disagree.
    fn default() -> Self {
        Self::from_tuning(&nodedb_types::config::tuning::TimeseriesToning::default())
    }
}

/// The symbol id a NULL symbol cell stores. No dictionary entry has it.
pub const NULL_SYMBOL_ID: u32 = u32::MAX;

/// Value types for `ingest_row()`. A resolved timeseries ingest records its
/// rows in this form.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum ColumnValue {
    Timestamp(i64),
    Float64(f64),
    Int64(i64),
    Symbol(String),
    /// SQL NULL. A symbol column stores [`NULL_SYMBOL_ID`] and a float
    /// column stores NaN. Integer and time columns have no NULL and refuse
    /// it.
    Null,
}

/// Result of draining the columnar memtable.
pub struct ColumnarDrainResult {
    pub columns: Vec<ColumnData>,
    pub schema: ColumnarSchema,
    pub symbol_dicts: HashMap<usize, SymbolDictionary>,
    pub row_count: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    /// The newest `_ts_system` ordinal across rows (0 when none). Populated
    /// on drain by scanning the `_ts_system` column, so retention and the
    /// serialization check read it without re-reading the segment.
    pub max_system_ts: i64,
    pub series_row_counts: HashMap<SeriesId, u64>,
}

impl ColumnarDrainResult {
    /// Borrow this drained payload as a [`ColumnarFlushView`].
    ///
    /// Lets a caller that already owns a drain result share the one segment
    /// writer with callers that only hold a live memtable.
    pub fn view(&self) -> ColumnarFlushView<'_> {
        ColumnarFlushView {
            columns: &self.columns,
            schema: &self.schema,
            symbol_dicts: &self.symbol_dicts,
            row_count: self.row_count,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            max_system_ts: self.max_system_ts,
        }
    }
}

/// A BORROWED view of the rows a flush is about to encode.
///
/// This type exists so a segment can be written without the rows first leaving
/// the memtable. `ColumnarSegmentWriter::write_partition` needs only to READ the
/// columns, so taking a view rather than an owned `ColumnarDrainResult` lets the
/// flush write the segment first and drain only once the write has committed —
/// a failed write then costs a retry instead of the rows. Draining up front
/// took them out of memory without putting them anywhere, and the coordinated
/// checkpoint calls that flush on a timer.
///
/// It borrows rather than clones because the alternative — copying the payload
/// so the memtable can keep its own — would duplicate up to the memtable's whole
/// hard limit on the ingest path's flush, on a Data Plane core, for every flush.
pub struct ColumnarFlushView<'a> {
    pub columns: &'a [ColumnData],
    pub schema: &'a ColumnarSchema,
    pub symbol_dicts: &'a HashMap<usize, SymbolDictionary>,
    pub row_count: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    /// The newest `_ts_system` ordinal across rows (0 when none).
    pub max_system_ts: i64,
}

/// The newest `_ts_system` ordinal across the rows, or `0` when the schema
/// has no such column or no row. The ordinal is kept exact: two writes of one
/// millisecond carry distinct ordinals.
///
/// Shared by the drain and the borrowed view so the two can never disagree about
/// the system-time bound a partition records for retention.
pub fn max_system_ts_of(schema: &ColumnarSchema, columns: &[ColumnData]) -> i64 {
    schema
        .ts_system_idx()
        .and_then(|idx| columns.get(idx))
        .and_then(|col| match col {
            ColumnData::Timestamp(v) | ColumnData::Int64(v) => v.iter().copied().max(),
            _ => None,
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two rows of one millisecond carry distinct ordinals, and the newest
    /// system time is the larger ordinal itself, not its millisecond.
    #[test]
    fn the_newest_system_time_is_the_exact_ordinal() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                (TS_SYSTEM.into(), ColumnType::Int64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        };
        let first = nodedb_types::ms_to_ordinal_upper(5_000) - 10;
        let second = first + 3;
        let columns = vec![
            ColumnData::Timestamp(vec![1, 2]),
            ColumnData::Int64(vec![second, first]),
        ];
        assert_eq!(max_system_ts_of(&schema, &columns), second);
        assert_eq!(max_system_ts_of(&ColumnarSchema::metric_default(), &[]), 0);
    }
}
