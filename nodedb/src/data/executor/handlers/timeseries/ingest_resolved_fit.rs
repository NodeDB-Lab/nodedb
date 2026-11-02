// SPDX-License-Identifier: BUSL-1.1

//! How a resolved timeseries batch maps onto the live memtable schema.
//!
//! The live schema at an install is the schema in force at the install's
//! log position. Every replica applies the same log in the same order, and a
//! restart seeds the memtable with its flushed schema, so every replica holds
//! the same live schema at that position. A row whose value conflicts with
//! its column's type in that schema is therefore rejected by every replica
//! alike.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_memtable::{ColumnType, ColumnValue, ColumnarSchema};
use crate::engine::timeseries::resolved_ingest::{
    ResolvedTsBatch, ResolvedTsRow, empty_value, value_fits,
};
use crate::types::{DatabaseId, TenantId};

pub(super) type CollKey = (DatabaseId, TenantId, String);

/// How a batch's rows map onto the live schema.
#[derive(Clone, Copy)]
pub(super) enum SchemaFit {
    /// The live schema evolves to the batch's schema: every row's values land
    /// in order.
    Exact,
    /// A concurrent write changed the schema: values land by column name.
    ByName,
}

impl CoreLoop {
    /// How `batch` maps onto the live memtable of `key`. A collection with no
    /// memtable takes the batch's schema whole.
    pub(super) fn schema_fit(&self, key: &CollKey, batch: &ResolvedTsBatch) -> SchemaFit {
        let Some(mt) = self.columnar_memtables.get(key) else {
            return SchemaFit::Exact;
        };
        let live = mt.schema();
        if live.timestamp_idx as u64 != batch.timestamp_idx {
            return SchemaFit::ByName;
        }
        let mut prospective = live.columns.clone();
        for (name, column_type) in &batch.columns {
            if !prospective.iter().any(|(existing, _)| existing == name) {
                prospective.push((name.clone(), *column_type));
            }
        }
        if prospective == batch.columns {
            SchemaFit::Exact
        } else {
            SchemaFit::ByName
        }
    }

    /// The schema a fresh memtable takes for `batch`: its columns, with the
    /// declared column codecs where the declaration names the column.
    pub(super) fn resolved_memtable_schema(
        &self,
        key: &CollKey,
        batch: &ResolvedTsBatch,
    ) -> ColumnarSchema {
        let declared = self.declared_ts_memtable_schema(key.0, key.1, &key.2);
        let codecs = batch
            .columns
            .iter()
            .map(|(name, _)| {
                declared
                    .as_ref()
                    .and_then(|schema| {
                        schema
                            .columns
                            .iter()
                            .position(|(declared_name, _)| declared_name == name)
                            .and_then(|idx| schema.codecs.get(idx).cloned())
                    })
                    .unwrap_or(nodedb_codec::ColumnCodec::Auto)
            })
            .collect();
        ColumnarSchema {
            columns: batch.columns.clone(),
            timestamp_idx: usize::try_from(batch.timestamp_idx).unwrap_or(0),
            codecs,
        }
    }
}

/// Each row's values in `schema`'s column order under `fit`. A row whose
/// value conflicts with its column's type in `schema` has `None`.
pub(super) fn landing_values(
    fit: SchemaFit,
    schema: &ColumnarSchema,
    batch: &ResolvedTsBatch,
) -> Vec<Option<Vec<ColumnValue>>> {
    batch
        .rows
        .iter()
        .map(|row| match fit {
            SchemaFit::Exact => Some(row.values.clone()),
            SchemaFit::ByName => values_by_name(schema, &batch.columns, row),
        })
        .collect()
}

/// `row`'s values rearranged into `schema`'s column order by name. A column
/// the row does not name takes its empty value in `schema`. `None` when a
/// value the row names does not fit its column's type.
fn values_by_name(
    schema: &ColumnarSchema,
    columns: &[(String, ColumnType)],
    row: &ResolvedTsRow,
) -> Option<Vec<ColumnValue>> {
    let mut values = Vec::with_capacity(schema.columns.len());
    for (idx, (name, column_type)) in schema.columns.iter().enumerate() {
        let named = columns
            .iter()
            .position(|(batch_name, _)| batch_name == name)
            .filter(|position| {
                !row.absent
                    .iter()
                    .any(|absent| *absent as usize == *position)
            })
            .and_then(|position| row.values.get(position));
        let value = match named {
            Some(value) if value_fits(value, *column_type) => value.clone(),
            Some(_) => return None,
            None if idx == schema.timestamp_idx => ColumnValue::Timestamp(row.timestamp_ms),
            None => empty_value(*column_type),
        };
        values.push(value);
    }
    Some(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::TimeKind;

    fn live() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
                ("extra".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        }
    }

    fn batch_columns() -> Vec<(String, ColumnType)> {
        vec![
            ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
            ("value".into(), ColumnType::Float64),
            ("extra".into(), ColumnType::Symbol),
        ]
    }

    fn row(extra: &str, absent: Vec<u32>) -> ResolvedTsRow {
        ResolvedTsRow {
            line: 0,
            timestamp_ms: 1_000,
            values: vec![
                ColumnValue::Timestamp(1_000),
                ColumnValue::Float64(1.0),
                ColumnValue::Symbol(extra.into()),
            ],
            absent,
            image: Vec::new(),
        }
    }

    #[test]
    fn a_named_value_of_another_type_conflicts() {
        assert!(values_by_name(&live(), &batch_columns(), &row("text", Vec::new())).is_none());
    }

    #[test]
    fn an_absent_column_takes_the_live_empty_value() {
        let values = values_by_name(&live(), &batch_columns(), &row("", vec![2]))
            .expect("an absent column never conflicts");
        assert_eq!(values.get(2), Some(&ColumnValue::Null));
        assert_eq!(values.get(1), Some(&ColumnValue::Float64(1.0)));
    }
}
