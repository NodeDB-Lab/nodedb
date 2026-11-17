// SPDX-License-Identifier: BUSL-1.1

//! The two forms a gathered timeseries row takes.
//!
//! The raw scan answers a client, so its rows are `rmpv::Value` maps in
//! schema order, rendered as the response carries them: a decimal and a UUID
//! as their text. The collection scan feeds joins, GROUP BY, partition
//! routing and aggregates, so its rows are [`TypedTsRow`]s: every cell keeps
//! its `nodedb_types::Value` type, a decimal as `Value::Decimal` and a UUID
//! as `Value::Uuid`. Both forms read each cell through [`read_ts_cell`], so
//! the two agree on which rows exist and on every cell's value.

use std::collections::HashMap;
use std::path::Path;

use nodedb_types::Value;
use nodedb_types::columnar::schema::{TS_SERIES, TS_SYSTEM, TS_VALID_FROM};

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::handlers::columnar_read::filter::value_matches_filters;
use crate::data::executor::handlers::timeseries::cell_read::{TsCell, read_ts_cell};
use crate::data::executor::handlers::timeseries::partition_read::partition_corrupt;
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType, ColumnarMemtable};
use crate::engine::timeseries::row_cells::uuid_value;

use super::row_emit::{
    emit_memtable_row, emit_partition_row, extract_timestamp, render_system_time, row_admitted,
};

/// The read path the corruption report names.
const SITE: &str = "timeseries_raw_scan";

/// One memtable column as the row emitters read it: index, name, declared
/// type, data.
pub(in crate::data::executor) type MemtableColumn<'a> =
    (usize, &'a String, &'a ColumnType, &'a ColumnData);

/// A form a gathered timeseries row takes.
pub(in crate::data::executor) trait TsRowForm: Sized {
    /// Row `idx` of the memtable.
    fn from_memtable(
        mt: &ColumnarMemtable,
        columns: &[MemtableColumn<'_>],
        idx: usize,
    ) -> crate::Result<Self>;

    /// Row `idx` of a flushed partition. A column the read did not decode,
    /// and a cell that does not read as its declared type, are corruption.
    fn from_partition(
        part_dir: &Path,
        schema: &[(String, ColumnType)],
        col_data: &[Option<ColumnData>],
        sym_dicts: &HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
        idx: usize,
    ) -> crate::Result<Self>;

    /// Whether the row passes `where_predicates`, then `rls_predicates`.
    fn admitted(
        &self,
        where_predicates: &[ScanFilter],
        rls_predicates: &[ScanFilter],
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<bool>;

    /// Render the stored `_ts_system` field for the reader and drop
    /// `_ts_series` ([`render_system_time`]).
    fn render_system_time(&mut self);

    /// The key rows of several partitions merge on: the first time-shaped
    /// field ([`extract_timestamp`]).
    fn sort_timestamp(&self) -> i64;
}

impl TsRowForm for rmpv::Value {
    fn from_memtable(
        mt: &ColumnarMemtable,
        columns: &[MemtableColumn<'_>],
        idx: usize,
    ) -> crate::Result<Self> {
        emit_memtable_row(mt, columns, idx)
    }

    fn from_partition(
        part_dir: &Path,
        schema: &[(String, ColumnType)],
        col_data: &[Option<ColumnData>],
        sym_dicts: &HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
        idx: usize,
    ) -> crate::Result<Self> {
        emit_partition_row(part_dir, schema, col_data, sym_dicts, idx)
    }

    fn admitted(
        &self,
        where_predicates: &[ScanFilter],
        rls_predicates: &[ScanFilter],
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<bool> {
        row_admitted(self, where_predicates, rls_predicates, crs)
    }

    fn render_system_time(&mut self) {
        render_system_time(self);
    }

    fn sort_timestamp(&self) -> i64 {
        extract_timestamp(self)
    }
}

/// A gathered timeseries row with typed cells, in schema order.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::data::executor) struct TypedTsRow(pub Vec<(String, Value)>);

impl TypedTsRow {
    /// The row as a `Value::Object`.
    pub(in crate::data::executor) fn into_value(self) -> Value {
        Value::Object(self.0.into_iter().collect())
    }
}

/// The typed value of one timeseries cell of column `column`.
///
/// A NaN float is stored NULL and reads [`TsCell::Null`]. A time cell takes
/// its column's kind: an instant column reads a typed instant, a `Millis`
/// column the integer stored.
fn typed_cell(cell: TsCell<'_>, column: &str) -> crate::Result<Value> {
    Ok(match cell {
        TsCell::Null => Value::Null,
        TsCell::Time(kind, millis) => {
            kind.cell_value(millis)
                .map_err(|e| crate::Error::Internal {
                    detail: format!("timeseries column {column}: {e}"),
                })?
        }
        TsCell::Float(f) => Value::Float(f),
        TsCell::Int(n) => Value::Integer(n),
        TsCell::Symbol(s) => Value::String(s.to_string()),
        TsCell::Decimal(d) => Value::Decimal(d),
        TsCell::Uuid(bytes) => uuid_value(Some(bytes)),
        TsCell::Bool(b) => Value::Bool(b),
    })
}

impl TsRowForm for TypedTsRow {
    fn from_memtable(
        mt: &ColumnarMemtable,
        columns: &[MemtableColumn<'_>],
        idx: usize,
    ) -> crate::Result<Self> {
        let mut cells = Vec::with_capacity(columns.len());
        for (col_idx, col_name, col_type, col_data) in columns {
            let cell = read_ts_cell(
                col_data,
                **col_type,
                col_name,
                mt.symbol_dict(*col_idx),
                idx,
            )?;
            cells.push(((*col_name).clone(), typed_cell(cell, col_name)?));
        }
        Ok(Self(cells))
    }

    fn from_partition(
        part_dir: &Path,
        schema: &[(String, ColumnType)],
        col_data: &[Option<ColumnData>],
        sym_dicts: &HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
        idx: usize,
    ) -> crate::Result<Self> {
        let mut cells = Vec::with_capacity(schema.len());
        for (col_i, (col_name, col_type)) in schema.iter().enumerate() {
            let Some(data) = col_data.get(col_i).and_then(Option::as_ref) else {
                return Err(partition_corrupt(
                    part_dir,
                    "column",
                    SITE,
                    format!("column '{col_name}' was not decoded"),
                ));
            };
            let cell = read_ts_cell(data, *col_type, col_name, sym_dicts.get(&col_i), idx)
                .map_err(|e| partition_corrupt(part_dir, "cell", SITE, e))?;
            cells.push((col_name.clone(), typed_cell(cell, col_name)?));
        }
        Ok(Self(cells))
    }

    fn admitted(
        &self,
        where_predicates: &[ScanFilter],
        rls_predicates: &[ScanFilter],
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<bool> {
        if where_predicates.is_empty() && rls_predicates.is_empty() {
            return Ok(true);
        }
        let doc = self.clone().into_value();
        Ok(value_matches_filters(&doc, where_predicates, crs)?
            && value_matches_filters(&doc, rls_predicates, crs)?)
    }

    fn render_system_time(&mut self) {
        self.0.retain(|(name, _)| name != TS_SERIES);
        let bitemporal = self.0.iter().any(|(name, _)| name == TS_VALID_FROM);
        if !bitemporal {
            self.0.retain(|(name, _)| name != TS_SYSTEM);
            return;
        }
        for (name, value) in &mut self.0 {
            if name == TS_SYSTEM
                && let Value::Integer(ordinal) = value
            {
                *value = Value::Integer(nodedb_types::ordinal_to_ms(*ordinal));
            }
        }
    }

    fn sort_timestamp(&self) -> i64 {
        self.0
            .iter()
            .find_map(|(_, value)| match value {
                Value::Integer(n) => Some(*n),
                Value::DateTime(dt) | Value::NaiveDateTime(dt) => Some(dt.micros),
                _ => None,
            })
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(cells: &[(&str, Value)]) -> TypedTsRow {
        TypedTsRow(
            cells
                .iter()
                .map(|(name, value)| ((*name).to_string(), value.clone()))
                .collect(),
        )
    }

    /// A typed row renders its system time as the rmpv form does: a plain
    /// row hides the stored ordinal, a bitemporal row shows its millisecond.
    #[test]
    fn system_time_renders_as_the_rmpv_form_renders_it() {
        let ordinal = 1_700_000_000_123 * 1_000_000 + 42;
        let mut plain = row(&[
            ("v", Value::Integer(1)),
            (TS_SYSTEM, Value::Integer(ordinal)),
            (TS_SERIES, Value::Integer(9)),
        ]);
        plain.render_system_time();
        assert_eq!(plain, row(&[("v", Value::Integer(1))]));
        let mut bitemporal = row(&[
            (TS_SYSTEM, Value::Integer(ordinal)),
            (TS_VALID_FROM, Value::Integer(0)),
        ]);
        bitemporal.render_system_time();
        assert_eq!(
            bitemporal
                .0
                .iter()
                .find(|(name, _)| name == TS_SYSTEM)
                .map(|(_, value)| value),
            Some(&Value::Integer(1_700_000_000_123))
        );
    }
}
