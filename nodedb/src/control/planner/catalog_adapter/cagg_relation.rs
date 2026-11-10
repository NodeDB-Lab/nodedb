// SPDX-License-Identifier: BUSL-1.1

//! A continuous aggregate as a read-only relation.
//!
//! The relation's columns are `bucket`, the GROUP BY columns, and one
//! column per aggregate output, typed from the aggregate's root source
//! collection. Every source column an aggregate names, at any tier, is a
//! column of that root.

use nodedb_sql::SqlCatalogError;
use nodedb_sql::types::{CollectionInfo, ColumnInfo, EngineType, SqlDataType};
use nodedb_types::{CollectionType, DatabaseId};

use crate::control::security::catalog::SystemCatalog;
use crate::engine::timeseries::continuous_agg::{AggFunction, ContinuousAggregateDef};

use super::type_convert::convert_collection_type;

/// The relation of continuous aggregate `name`, or `None` when `name` is
/// not one of `tenant_id`'s aggregates in `database_id`.
pub(super) fn continuous_aggregate_relation(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
) -> Result<Option<CollectionInfo>, SqlCatalogError> {
    let db = database_id.as_u64();
    let read = |agg: &str| {
        catalog
            .get_continuous_aggregate(db, tenant_id, agg)
            .map_err(|e| catalog_error(name, &e))
    };
    let Some(stored) = read(name)? else {
        return Ok(None);
    };
    let def: ContinuousAggregateDef =
        zerompk::from_msgpack(&stored.def_bytes).map_err(|e| SqlCatalogError::Unreadable {
            relation: name.to_string(),
            detail: format!("its continuous aggregate definition does not decode: {e}"),
        })?;
    let root = crate::control::server::shared::ddl::neutral::continuous_agg::refresh::resolve_root(
        name,
        stored.source,
        |agg| {
            catalog
                .get_continuous_aggregate(db, tenant_id, agg)
                .map(|s| s.map(|s| s.source))
        },
    )
    .map_err(|e| catalog_error(name, &e))?;
    let source = catalog
        .get_collection(database_id, tenant_id, &root)
        .map_err(|e| catalog_error(name, &e))?;
    let (source_columns, time_key) = match &source {
        Some(stored) => {
            let (_, columns, _) = convert_collection_type(stored);
            let time_key = match &stored.collection_type {
                CollectionType::Columnar(nodedb_types::columnar::ColumnarProfile::Timeseries {
                    time_key,
                    ..
                }) => Some(time_key.clone()),
                _ => None,
            };
            (columns, time_key)
        }
        None => (Vec::new(), None),
    };
    Ok(Some(CollectionInfo {
        name: name.to_string(),
        engine: EngineType::ContinuousAggregate,
        columns: relation_columns(&def, &source_columns, time_key.as_deref()),
        primary_key: None,
        has_auto_tier: false,
        indexes: Vec::new(),
        bitemporal: false,
        primary: nodedb_types::PrimaryEngine::Document,
        vector_primary: None,
        partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        open_schema: CollectionInfo::open_schema_for(EngineType::ContinuousAggregate),
    }))
}

/// `bucket`, the GROUP BY columns, then one column per aggregate output.
fn relation_columns(
    def: &ContinuousAggregateDef,
    source: &[ColumnInfo],
    time_key: Option<&str>,
) -> Vec<ColumnInfo> {
    let source_column = |name: &str| {
        source
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
            .cloned()
    };
    let mut columns = Vec::with_capacity(1 + def.group_by.len() + def.aggregates.len());
    let bucket_type = time_key
        .and_then(source_column)
        .map_or(SqlDataType::Int64, |c| c.data_type);
    columns.push(ColumnInfo {
        nullable: false,
        ..derived("bucket", bucket_type)
    });
    for group in &def.group_by {
        columns.push(match source_column(group) {
            Some(c) => ColumnInfo {
                name: group.clone(),
                nullable: true,
                is_primary_key: false,
                default: None,
                ..c
            },
            None => derived(group, SqlDataType::Unknown),
        });
    }
    for expr in &def.aggregates {
        let input = source_column(&expr.source_column);
        let column = match &expr.function {
            AggFunction::Count | AggFunction::CountDistinct | AggFunction::TopK(_) => {
                derived(&expr.output_column, SqlDataType::Int64)
            }
            AggFunction::Percentile(_) => derived(&expr.output_column, SqlDataType::Float64),
            // PostgreSQL's `avg` is float8 over a float and numeric over
            // integers and numerics.
            AggFunction::Avg => match input.as_ref().map(|c| &c.data_type) {
                Some(SqlDataType::Float64) => derived(&expr.output_column, SqlDataType::Float64),
                Some(_) | None => derived(&expr.output_column, SqlDataType::Decimal(None)),
            },
            // An integer total can pass the 64-bit range and turns exact
            // decimal there, as PostgreSQL's `sum(bigint)` returns numeric.
            AggFunction::Sum => match input.map(|c| c.data_type) {
                Some(SqlDataType::Float64) => derived(&expr.output_column, SqlDataType::Float64),
                Some(_) | None => derived(&expr.output_column, SqlDataType::Decimal(None)),
            },
            AggFunction::Min | AggFunction::Max | AggFunction::First | AggFunction::Last => {
                match input {
                    Some(c) => ColumnInfo {
                        name: expr.output_column.clone(),
                        nullable: true,
                        is_primary_key: false,
                        default: None,
                        ..c
                    },
                    None => derived(&expr.output_column, SqlDataType::Unknown),
                }
            }
            // `AggFunction` is non-exhaustive: a function the aggregate keeps
            // no state for finalizes to NULL.
            _ => derived(&expr.output_column, SqlDataType::Unknown),
        };
        columns.push(column);
    }
    columns
}

/// A nullable column the aggregate computes.
fn derived(name: &str, data_type: SqlDataType) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable: true,
        is_primary_key: false,
        default: None,
        raw_type: None,
        int_width: None,
        float_width: None,
    }
}

fn catalog_error(name: &str, error: &crate::Error) -> SqlCatalogError {
    SqlCatalogError::Unreadable {
        relation: name.to_string(),
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::continuous_agg::{AggregateExpr, RefreshPolicy};

    fn column(name: &str, data_type: SqlDataType) -> ColumnInfo {
        derived(name, data_type)
    }

    #[test]
    fn columns_follow_bucket_groups_and_outputs() {
        let expr = |function, source: &str, output: &str| AggregateExpr {
            function,
            source_column: source.into(),
            output_column: output.into(),
        };
        let def = ContinuousAggregateDef {
            name: "cpu_1m".into(),
            source: "cpu".into(),
            bucket_interval: "1m".into(),
            bucket_interval_ms: 60_000,
            group_by: vec!["host".into()],
            aggregates: vec![
                expr(AggFunction::Count, "*", "c"),
                expr(AggFunction::Sum, "load", "s"),
                expr(AggFunction::Avg, "load", "a"),
                expr(AggFunction::Max, "load", "m"),
            ],
            refresh_policy: RefreshPolicy::OnFlush,
            ..ContinuousAggregateDef::default()
        };
        let source = [
            column("ts", SqlDataType::Timestamp),
            column("host", SqlDataType::String),
            column("load", SqlDataType::Float64),
        ];
        let columns = relation_columns(&def, &source, Some("ts"));
        let shape: Vec<(&str, &SqlDataType)> = columns
            .iter()
            .map(|c| (c.name.as_str(), &c.data_type))
            .collect();
        assert_eq!(
            shape,
            vec![
                ("bucket", &SqlDataType::Timestamp),
                ("host", &SqlDataType::String),
                ("c", &SqlDataType::Int64),
                ("s", &SqlDataType::Float64),
                ("a", &SqlDataType::Float64),
                ("m", &SqlDataType::Float64),
            ]
        );
        assert!(!columns[0].nullable);
    }
}
