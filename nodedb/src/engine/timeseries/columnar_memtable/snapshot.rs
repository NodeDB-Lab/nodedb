// SPDX-License-Identifier: BUSL-1.1

//! Lossless snapshot wire types for [`super::ColumnarMemtable`].
//!
//! `MemtableSnapshot` is the authoritative on-wire format used by
//! `export_snapshot` / `from_snapshot`. It carries full type metadata and
//! symbol dictionaries so the round-trip is BYTE-FAITHFUL: every column value,
//! every tag symbol, and every stat survives a serialize → deserialize cycle
//! unchanged.

use std::collections::HashMap;

use nodedb_types::timeseries::{SeriesId, SymbolDictionary};
use serde::{Deserialize, Serialize};

use super::types::{ColumnData, ColumnType};

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// Complete, lossless snapshot of a [`super::ColumnarMemtable`].
#[derive(
    Debug, PartialEq, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct MemtableSnapshot {
    /// Ordered list of (column_name, column_type) pairs — the schema.
    pub schema_columns: Vec<(String, ColumnType)>,
    /// Index of the designated timestamp column in `schema_columns`.
    pub timestamp_idx: usize,
    /// Column data, one entry per schema column, same order.
    pub columns: Vec<ColumnSnapshot>,
    /// Symbol dictionaries for tag columns (only columns whose `ColumnType` is
    /// `Symbol` carry a dictionary; keyed by column index in the schema).
    pub symbol_dicts: Vec<(usize, SymbolDictionary)>,
    /// Per-series row counts (`SeriesId` is a `u64` type alias — always serde-able).
    pub series_row_counts: Vec<(SeriesId, u64)>,
    /// Total row count — must equal every column's length.
    pub row_count: u64,
    /// Minimum timestamp observed (i64::MAX for empty memtable).
    pub min_ts: i64,
    /// Maximum timestamp observed (i64::MIN for empty memtable).
    pub max_ts: i64,
}

/// Serializable representation of one column's data.
///
/// The `DictEncoded` variant carries its own embedded dictionary so it is
/// fully self-contained; the `reverse` map is NOT serialized (it is
/// mechanically rebuilt from `dictionary` on import).
#[derive(
    Debug, PartialEq, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum ColumnSnapshot {
    Timestamp(Vec<i64>),
    Float64(Vec<f64>),
    Int64(Vec<i64>),
    Symbol(Vec<u32>),
    /// Dictionary-encoded column: `ids` are row-level symbol indices into
    /// `dictionary`; `valid` is the per-row null bitmap.
    DictEncoded {
        ids: Vec<u32>,
        dictionary: Vec<String>,
        valid: Vec<bool>,
    },
}

// ---------------------------------------------------------------------------
// Column conversion helpers (called from `export_snapshot`)
// ---------------------------------------------------------------------------

/// Convert a single [`ColumnData`] value into its [`ColumnSnapshot`] wire form.
///
/// The `reverse` map in `DictEncoded` is intentionally dropped — it is
/// mechanically rebuilt from `dictionary` by [`rebuild_columns`] on import.
pub(super) fn column_to_snapshot(col: &ColumnData) -> ColumnSnapshot {
    match col {
        ColumnData::Timestamp(v) => ColumnSnapshot::Timestamp(v.clone()),
        ColumnData::Float64(v) => ColumnSnapshot::Float64(v.clone()),
        ColumnData::Int64(v) => ColumnSnapshot::Int64(v.clone()),
        ColumnData::Symbol(v) => ColumnSnapshot::Symbol(v.clone()),
        // `reverse` is rebuildable from `dictionary`; not serialized.
        ColumnData::DictEncoded {
            ids,
            dictionary,
            valid,
            ..
        } => ColumnSnapshot::DictEncoded {
            ids: ids.clone(),
            dictionary: dictionary.clone(),
            valid: valid.clone(),
        },
    }
}

// ---------------------------------------------------------------------------
// Column rebuild helper (called from `from_snapshot`)
// ---------------------------------------------------------------------------

/// Rebuild a `Vec<ColumnData>` from the snapshot columns and validate
/// consistency.
///
/// Returns the rebuilt column vector or a typed error on any mismatch.
pub(super) fn rebuild_columns(
    columns: Vec<ColumnSnapshot>,
    schema_columns: &[(String, ColumnType)],
    row_count: u64,
) -> crate::Result<Vec<ColumnData>> {
    if columns.len() != schema_columns.len() {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "snapshot column count mismatch: schema has {} columns but data has {}",
                schema_columns.len(),
                columns.len(),
            ),
        });
    }

    let mut result = Vec::with_capacity(columns.len());

    for (i, (snap_col, (col_name, _col_type))) in
        columns.into_iter().zip(schema_columns.iter()).enumerate()
    {
        let col_len = match &snap_col {
            ColumnSnapshot::Timestamp(v) => v.len(),
            ColumnSnapshot::Float64(v) => v.len(),
            ColumnSnapshot::Int64(v) => v.len(),
            ColumnSnapshot::Symbol(v) => v.len(),
            ColumnSnapshot::DictEncoded { ids, .. } => ids.len(),
        };

        if col_len as u64 != row_count {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "snapshot column {} ('{}') has {} rows but row_count is {}",
                    i, col_name, col_len, row_count,
                ),
            });
        }

        let col_data = match snap_col {
            ColumnSnapshot::Timestamp(v) => ColumnData::Timestamp(v),
            ColumnSnapshot::Float64(v) => ColumnData::Float64(v),
            ColumnSnapshot::Int64(v) => ColumnData::Int64(v),
            ColumnSnapshot::Symbol(v) => ColumnData::Symbol(v),
            ColumnSnapshot::DictEncoded {
                ids,
                dictionary,
                valid,
            } => {
                // Rebuild reverse map: index in `dictionary` == symbol id.
                let reverse: HashMap<String, u32> = dictionary
                    .iter()
                    .enumerate()
                    .map(|(idx, s)| (s.clone(), idx as u32))
                    .collect();
                ColumnData::DictEncoded {
                    ids,
                    dictionary,
                    reverse,
                    valid,
                }
            }
        };

        result.push(col_data);
    }

    Ok(result)
}
