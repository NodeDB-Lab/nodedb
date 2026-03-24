//! Schema evolution for columnar timeseries partitions.
//!
//! Each partition stores its own schema independently. When querying across
//! partitions with different schemas:
//! - Missing columns → NULL
//! - Extra columns → ignored (not read)
//! - Column type widening → cast-on-read (e.g., Int64 → Float64)
//!
//! No data rewrite needed for ADD/DROP/RENAME COLUMN.

use super::columnar_memtable::{ColumnData, ColumnType, ColumnarSchema};

/// A column mapping from query schema to partition schema.
#[derive(Debug)]
pub enum ColumnMapping {
    /// Column exists in partition at the given index.
    Present(usize),
    /// Column missing in this partition — fill with NULL.
    Missing,
    /// Column exists but needs type widening (e.g., Int64 → Float64).
    Widen {
        index: usize,
        from: ColumnType,
        to: ColumnType,
    },
}

/// Build column mappings from a query schema against a partition schema.
///
/// For each column in `query_schema`, determines where it lives in
/// `partition_schema` (by name match).
pub fn build_column_mappings(
    query_schema: &ColumnarSchema,
    partition_schema: &ColumnarSchema,
) -> Vec<ColumnMapping> {
    query_schema
        .columns
        .iter()
        .map(|(q_name, q_type)| {
            match partition_schema
                .columns
                .iter()
                .position(|(p_name, _)| p_name == q_name)
            {
                Some(idx) => {
                    let (_, p_type) = &partition_schema.columns[idx];
                    if p_type == q_type {
                        ColumnMapping::Present(idx)
                    } else if can_widen(*p_type, *q_type) {
                        ColumnMapping::Widen {
                            index: idx,
                            from: *p_type,
                            to: *q_type,
                        }
                    } else {
                        // Incompatible types — treat as missing.
                        ColumnMapping::Missing
                    }
                }
                None => ColumnMapping::Missing,
            }
        })
        .collect()
}

/// Check if a column type can be widened (implicit cast).
fn can_widen(from: ColumnType, to: ColumnType) -> bool {
    matches!((from, to), (ColumnType::Int64, ColumnType::Float64))
}

/// Apply column mappings to produce query-schema-aligned data.
///
/// For each column in the query schema:
/// - `Present(idx)` → copy from partition data
/// - `Missing` → fill with NULL-equivalent (0 for numeric, 0 for symbol)
/// - `Widen` → cast from source type
pub fn apply_mappings(
    mappings: &[ColumnMapping],
    query_schema: &ColumnarSchema,
    partition_data: &[ColumnData],
    row_count: usize,
) -> Vec<ColumnData> {
    mappings
        .iter()
        .zip(query_schema.columns.iter())
        .map(|(mapping, (_, q_type))| match mapping {
            ColumnMapping::Present(idx) => partition_data[*idx].clone_data(),
            ColumnMapping::Missing => null_column(*q_type, row_count),
            ColumnMapping::Widen { index, from, to } => {
                widen_column(&partition_data[*index], *from, *to, row_count)
            }
        })
        .collect()
}

/// Create a NULL-equivalent column of the given type and length.
fn null_column(ty: ColumnType, rows: usize) -> ColumnData {
    match ty {
        ColumnType::Timestamp => ColumnData::Timestamp(vec![0; rows]),
        ColumnType::Float64 => ColumnData::Float64(vec![f64::NAN; rows]),
        ColumnType::Int64 => ColumnData::Int64(vec![0; rows]),
        ColumnType::Symbol => ColumnData::Symbol(vec![u32::MAX; rows]), // sentinel
    }
}

/// Widen a column from one type to another.
fn widen_column(data: &ColumnData, _from: ColumnType, to: ColumnType, _rows: usize) -> ColumnData {
    match (data, to) {
        (ColumnData::Int64(vals), ColumnType::Float64) => {
            ColumnData::Float64(vals.iter().map(|&v| v as f64).collect())
        }
        _ => {
            // Unsupported widening — return NaN column.
            let len = data.len();
            null_column(to, len)
        }
    }
}

/// Schema diff for ALTER COLLECTION operations.
#[derive(Debug)]
pub enum SchemaChange {
    AddColumn { name: String, col_type: ColumnType },
    DropColumn { name: String },
    RenameColumn { old_name: String, new_name: String },
}

/// Apply schema changes to produce a new schema version.
/// Returns the new schema (metadata-only, no data rewrite).
pub fn apply_schema_changes(
    current: &ColumnarSchema,
    changes: &[SchemaChange],
) -> crate::Result<ColumnarSchema> {
    let mut columns = current.columns.clone();
    let mut timestamp_idx = current.timestamp_idx;

    for change in changes {
        match change {
            SchemaChange::AddColumn { name, col_type } => {
                if columns.iter().any(|(n, _)| n == name) {
                    return Err(crate::Error::BadRequest {
                        detail: format!("column '{name}' already exists"),
                    });
                }
                columns.push((name.clone(), *col_type));
            }
            SchemaChange::DropColumn { name } => {
                let idx = columns.iter().position(|(n, _)| n == name).ok_or_else(|| {
                    crate::Error::BadRequest {
                        detail: format!("column '{name}' not found"),
                    }
                })?;
                if idx == timestamp_idx {
                    return Err(crate::Error::BadRequest {
                        detail: "cannot drop the designated timestamp column".into(),
                    });
                }
                columns.remove(idx);
                if idx < timestamp_idx {
                    timestamp_idx -= 1;
                }
            }
            SchemaChange::RenameColumn { old_name, new_name } => {
                let idx = columns
                    .iter()
                    .position(|(n, _)| n == old_name)
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: format!("column '{old_name}' not found"),
                    })?;
                if columns.iter().any(|(n, _)| n == new_name) {
                    return Err(crate::Error::BadRequest {
                        detail: format!("column '{new_name}' already exists"),
                    });
                }
                columns[idx].0 = new_name.clone();
            }
        }
    }

    Ok(ColumnarSchema {
        columns,
        timestamp_idx,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_v1() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("cpu".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
        }
    }

    fn schema_v2_added_column() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("cpu".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
                ("mem".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
        }
    }

    #[test]
    fn same_schema_maps_directly() {
        let s = schema_v1();
        let mappings = build_column_mappings(&s, &s);
        assert_eq!(mappings.len(), 3);
        assert!(matches!(mappings[0], ColumnMapping::Present(0)));
        assert!(matches!(mappings[1], ColumnMapping::Present(1)));
        assert!(matches!(mappings[2], ColumnMapping::Present(2)));
    }

    #[test]
    fn missing_column_maps_to_null() {
        let query = schema_v2_added_column(); // has "mem"
        let partition = schema_v1(); // no "mem"
        let mappings = build_column_mappings(&query, &partition);
        assert_eq!(mappings.len(), 4);
        assert!(matches!(mappings[3], ColumnMapping::Missing)); // "mem" missing
    }

    #[test]
    fn apply_mappings_fills_nulls() {
        let query = schema_v2_added_column();
        let partition = schema_v1();
        let mappings = build_column_mappings(&query, &partition);

        let partition_data = vec![
            ColumnData::Timestamp(vec![1000, 2000]),
            ColumnData::Float64(vec![0.5, 0.7]),
            ColumnData::Symbol(vec![0, 1]),
        ];

        let result = apply_mappings(&mappings, &query, &partition_data, 2);
        assert_eq!(result.len(), 4);
        // "mem" column should be NaN-filled.
        let mem = result[3].as_f64();
        assert!(mem[0].is_nan());
        assert!(mem[1].is_nan());
    }

    #[test]
    fn widen_int64_to_float64() {
        let query = ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp),
                ("val".into(), ColumnType::Float64), // query expects f64
            ],
            timestamp_idx: 0,
        };
        let partition = ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp),
                ("val".into(), ColumnType::Int64), // partition has i64
            ],
            timestamp_idx: 0,
        };
        let mappings = build_column_mappings(&query, &partition);
        assert!(matches!(mappings[1], ColumnMapping::Widen { .. }));

        let partition_data = vec![
            ColumnData::Timestamp(vec![100]),
            ColumnData::Int64(vec![42]),
        ];
        let result = apply_mappings(&mappings, &query, &partition_data, 1);
        let val = result[1].as_f64();
        assert!((val[0] - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn add_column_schema_change() {
        let s = schema_v1();
        let changes = vec![SchemaChange::AddColumn {
            name: "mem".into(),
            col_type: ColumnType::Float64,
        }];
        let new_s = apply_schema_changes(&s, &changes).unwrap();
        assert_eq!(new_s.columns.len(), 4);
        assert_eq!(new_s.columns[3].0, "mem");
    }

    #[test]
    fn drop_column_schema_change() {
        let s = schema_v1();
        let changes = vec![SchemaChange::DropColumn {
            name: "host".into(),
        }];
        let new_s = apply_schema_changes(&s, &changes).unwrap();
        assert_eq!(new_s.columns.len(), 2);
        assert!(new_s.columns.iter().all(|(n, _)| n != "host"));
    }

    #[test]
    fn cannot_drop_timestamp() {
        let s = schema_v1();
        let changes = vec![SchemaChange::DropColumn {
            name: "timestamp".into(),
        }];
        assert!(apply_schema_changes(&s, &changes).is_err());
    }

    #[test]
    fn rename_column_schema_change() {
        let s = schema_v1();
        let changes = vec![SchemaChange::RenameColumn {
            old_name: "cpu".into(),
            new_name: "cpu_pct".into(),
        }];
        let new_s = apply_schema_changes(&s, &changes).unwrap();
        assert_eq!(new_s.columns[1].0, "cpu_pct");
    }

    #[test]
    fn duplicate_add_rejected() {
        let s = schema_v1();
        let changes = vec![SchemaChange::AddColumn {
            name: "cpu".into(), // already exists
            col_type: ColumnType::Float64,
        }];
        assert!(apply_schema_changes(&s, &changes).is_err());
    }
}
