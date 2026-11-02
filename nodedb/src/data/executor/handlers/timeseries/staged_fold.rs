// SPDX-License-Identifier: BUSL-1.1

//! Fold a transaction's staged timeseries rows into a grouped aggregate, so
//! an in-transaction aggregate reads its own writes.
//!
//! The staged rows are ingested into a scratch memtable under the
//! collection's live schema and aggregated by the same grouped scan the
//! committed rows go through. Group keys therefore encode identically and
//! the two results merge group by group.

use nodedb_types::Value;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_memtable::{
    ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
};
use crate::engine::timeseries::grouped_scan::{GroupedAggResult, aggregate_memtable};
use crate::engine::timeseries::resolved_ingest::empty_value;
use crate::types::{DatabaseId, TenantId};

/// Inputs to [`CoreLoop::fold_staged_ts_rows`].
pub(super) struct StagedFold<'a> {
    pub database_id: DatabaseId,
    pub tid: TenantId,
    pub collection: &'a str,
    /// The staged rows, already admitted by the time range, WHERE
    /// predicate and read policy.
    pub rows: &'a [Value],
    pub group_by: &'a [String],
    pub aggregates: &'a [(String, String)],
    pub filter_predicates: &'a [ScanFilter],
    pub time_range: (i64, i64),
    pub bucket_interval_ms: i64,
}

impl CoreLoop {
    /// Aggregate the staged rows into `merged`.
    pub(super) fn fold_staged_ts_rows(
        &self,
        merged: &mut GroupedAggResult,
        p: StagedFold<'_>,
    ) -> crate::Result<()> {
        if p.rows.is_empty() {
            return Ok(());
        }
        let key = (p.database_id, p.tid, p.collection.to_string());
        let schema = match self.columnar_memtables.get(&key) {
            Some(mt) => mt.schema().clone(),
            None => self
                .declared_ts_memtable_schema(p.database_id, p.tid, p.collection)
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!(
                        "timeseries collection '{}' has staged rows but no schema to fold them \
                         under",
                        p.collection
                    ),
                })?,
        };
        let mut scratch = ColumnarMemtable::new(
            schema.clone(),
            ColumnarMemtableConfig::from_tuning(&self.ts_tuning),
        );
        for row in p.rows {
            scratch.ingest_row(0, &row_values(&schema, row)?)?;
        }
        if let Some(result) = aggregate_memtable(
            &scratch,
            p.group_by,
            p.aggregates,
            p.filter_predicates,
            p.time_range,
            p.bucket_interval_ms,
        )
        .map_err(crate::Error::from)?
        {
            merged.merge(&result);
        }
        Ok(())
    }
}

/// A staged row's values in `schema`'s column order. A column the row does
/// not carry, or carries as NULL, takes the column's empty value, as a
/// committed ingest stores it.
fn row_values(schema: &ColumnarSchema, row: &Value) -> crate::Result<Vec<ColumnValue>> {
    let Value::Object(map) = row else {
        return Err(crate::Error::Internal {
            detail: "staged timeseries row is not an object".into(),
        });
    };
    schema
        .columns
        .iter()
        .map(|(name, column_type)| {
            let value = map
                .iter()
                .find(|(column, _)| column.eq_ignore_ascii_case(name))
                .map(|(_, value)| value);
            match value {
                None | Some(Value::Null) => Ok(empty_value(*column_type)),
                Some(value) => column_value(name, *column_type, value),
            }
        })
        .collect()
}

/// One staged cell as the column type stores it.
fn column_value(name: &str, column_type: ColumnType, value: &Value) -> crate::Result<ColumnValue> {
    let mismatch = || crate::Error::BadRequest {
        detail: format!("staged value for timeseries column '{name}' does not fit {column_type:?}"),
    };
    Ok(match column_type {
        ColumnType::Timestamp(kind) => {
            ColumnValue::Timestamp(kind.literal_ms(value).ok_or_else(mismatch)?)
        }
        ColumnType::Float64 => match value {
            Value::Float(f) => ColumnValue::Float64(*f),
            Value::Integer(i) => ColumnValue::Float64(*i as f64),
            _ => return Err(mismatch()),
        },
        ColumnType::Int64 => match value {
            Value::Integer(i) => ColumnValue::Int64(*i),
            _ => return Err(mismatch()),
        },
        ColumnType::Symbol | ColumnType::Text => match value {
            Value::String(s) => ColumnValue::Symbol(s.clone()),
            _ => return Err(mismatch()),
        },
    })
}
