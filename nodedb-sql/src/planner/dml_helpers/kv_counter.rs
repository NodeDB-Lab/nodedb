// SPDX-License-Identifier: Apache-2.0

//! The row a KV counter atomic (`KV_INCR`, `KV_INCR_FLOAT`) creates for an
//! absent key.
//!
//! The KV engine stores the bytes it is handed and does not know the declared
//! columns. So the fresh row is planned here, from the catalog, by the same
//! code a `VALUES` insert runs: `INSERT (key, column) VALUES (key, 0)`. The
//! Control Plane encodes the cells into the template the engine fills in.

use sqlparser::ast::{self, Expr, Value, ValueWithSpan};
use sqlparser::tokenizer::Span;

use super::kv_insert::build_kv_insert_plan;
use super::params::KvInsertParams;
use crate::catalog::SqlCatalog;
use crate::error::{Result, SqlError};
use crate::types::*;

/// The kind of number a counter atomic moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvCounterKind {
    /// `KV_INCR` / `KV_DECR`: an integer column.
    Integer,
    /// `KV_INCR_FLOAT`: an integer or float column, the same columns it moves
    /// in an existing row.
    Float,
}

/// The row a counter atomic creates for an absent key.
#[derive(Debug, Clone, PartialEq)]
pub enum KvCounterFreshRow {
    /// The collection holds a single `value` column, or declares none: the
    /// new value is stored as raw decimal text.
    Raw,
    /// The collection declares typed columns.
    Typed {
        /// The declared column the counter moves: the first column of `kind`
        /// in name order, never the primary key. An existing row picks its
        /// column by the same rule. `None` when there is none.
        column: Option<String>,
        /// The cells the insert stores other than `column`: DEFAULTs, and the
        /// primary key when it is a named column.
        cells: Vec<(String, SqlValue)>,
    },
}

/// Plan the row `INSERT (key, column) VALUES (key, 0)` stores in the KV
/// collection `info`, for a counter of `kind`.
pub fn plan_kv_counter_fresh_row(
    info: &CollectionInfo,
    key: &str,
    kind: KvCounterKind,
    catalog: &dyn SqlCatalog,
) -> Result<KvCounterFreshRow> {
    if info.engine != EngineType::KeyValue {
        return Err(SqlError::Unsupported {
            detail: format!(
                "KV counter on '{}', which is not a key-value collection",
                info.name
            ),
        });
    }
    let pk_col = info.primary_key.as_deref().unwrap_or("key");
    let value_columns: Vec<&ColumnInfo> = info
        .columns
        .iter()
        .filter(|c| c.name != pk_col && c.name != "key" && c.name != "ttl")
        .collect();
    if value_columns.is_empty() || (value_columns.len() == 1 && value_columns[0].name == "value") {
        return Ok(KvCounterFreshRow::Raw);
    }

    let column = value_columns
        .iter()
        .filter(|c| moves_as(kind, &c.data_type))
        .map(|c| c.name.clone())
        .min();
    let Some(column) = column else {
        return Ok(KvCounterFreshRow::Typed {
            column: None,
            cells: Vec::new(),
        });
    };

    let placeholder = match kind {
        KvCounterKind::Integer => "0",
        KvCounterKind::Float => "0.0",
    };
    let columns = [pk_col.to_string(), column.clone()];
    let row = ast::Parens::with_empty_span(vec![
        literal(Value::SingleQuotedString(key.to_string())),
        literal(Value::Number(placeholder.to_string(), false)),
    ]);
    let plans = build_kv_insert_plan(KvInsertParams {
        collection: info.name.clone(),
        columns: &columns,
        rows_ast: std::slice::from_ref(&row),
        intent: KvInsertIntent::Put,
        on_conflict_updates: Vec::new(),
        pk_col: info.primary_key.as_deref(),
        declared_columns: &info.columns,
        catalog,
    })?;
    let cells = match plans.into_iter().next() {
        Some(SqlPlan::KvInsert { mut entries, .. }) if entries.len() == 1 => {
            let (_, cells) = entries.remove(0);
            cells
                .into_iter()
                .filter(|(name, _)| *name != column)
                .collect()
        }
        _ => {
            return Err(SqlError::Unsupported {
                detail: "KV counter fresh row did not plan as one KV insert".into(),
            });
        }
    };
    Ok(KvCounterFreshRow::Typed {
        column: Some(column),
        cells,
    })
}

/// Whether a declared column of `data_type` is one a counter of `kind`
/// moves.
fn moves_as(kind: KvCounterKind, data_type: &SqlDataType) -> bool {
    match kind {
        KvCounterKind::Integer => matches!(data_type, SqlDataType::Int64),
        KvCounterKind::Float => matches!(data_type, SqlDataType::Int64 | SqlDataType::Float64),
    }
}

fn literal(value: Value) -> Expr {
    Expr::Value(ValueWithSpan {
        value,
        span: Span::empty(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A catalog with no collections and no sequence state. The DEFAULTs
    /// these cases declare are literals, so no accessor is reached.
    struct NoCatalog;

    impl SqlCatalog for NoCatalog {
        fn get_collection(
            &self,
            _database_id: nodedb_types::DatabaseId,
            _name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, crate::catalog::SqlCatalogError> {
            Ok(None)
        }
    }

    fn column(name: &str, data_type: SqlDataType, default: Option<&str>) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: true,
            is_primary_key: false,
            default: default.map(str::to_string),
            raw_type: None,
            int_width: None,
            float_width: None,
        }
    }

    fn kv_info(pk: &str, columns: Vec<ColumnInfo>) -> CollectionInfo {
        let mut key = column(pk, SqlDataType::String, None);
        key.is_primary_key = true;
        let mut all = vec![key];
        all.extend(columns);
        CollectionInfo {
            name: "c".into(),
            engine: EngineType::KeyValue,
            columns: all,
            primary_key: Some(pk.into()),
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::KeyValue),
        }
    }

    #[test]
    fn a_single_value_column_is_raw() {
        let info = kv_info("key", vec![column("value", SqlDataType::String, None)]);
        let row = plan_kv_counter_fresh_row(&info, "k", KvCounterKind::Integer, &NoCatalog)
            .expect("plan");
        assert_eq!(row, KvCounterFreshRow::Raw);
    }

    #[test]
    fn a_typed_collection_plans_the_insert_row_with_defaults() {
        let info = kv_info(
            "key",
            vec![
                column("n", SqlDataType::Int64, None),
                column("status", SqlDataType::String, Some("'new'")),
                column("score", SqlDataType::Float64, None),
            ],
        );
        let row = plan_kv_counter_fresh_row(&info, "k", KvCounterKind::Integer, &NoCatalog)
            .expect("plan");
        let KvCounterFreshRow::Typed { column, cells } = row else {
            panic!("expected a typed row");
        };
        assert_eq!(column.as_deref(), Some("n"));
        assert!(
            cells
                .iter()
                .any(|(name, value)| name == "status" && *value == SqlValue::String("new".into())),
            "{cells:?}"
        );
        assert!(
            cells.iter().all(|(name, _)| name != "n" && name != "key"),
            "{cells:?}"
        );
    }

    #[test]
    fn a_named_primary_key_is_kept_in_the_row() {
        let info = kv_info("id", vec![column("n", SqlDataType::Int64, None)]);
        let row = plan_kv_counter_fresh_row(&info, "k1", KvCounterKind::Integer, &NoCatalog)
            .expect("plan");
        let KvCounterFreshRow::Typed { cells, .. } = row else {
            panic!("expected a typed row");
        };
        assert!(
            cells
                .iter()
                .any(|(name, value)| name == "id" && *value == SqlValue::String("k1".into())),
            "{cells:?}"
        );
    }

    #[test]
    fn counters_move_the_first_column_of_their_kind_in_name_order() {
        let info = kv_info(
            "key",
            vec![
                column("b", SqlDataType::Int64, None),
                column("a", SqlDataType::Float64, None),
                column("label", SqlDataType::String, None),
            ],
        );
        let target = |kind| match plan_kv_counter_fresh_row(&info, "k", kind, &NoCatalog) {
            Ok(KvCounterFreshRow::Typed { column, .. }) => column,
            other => panic!("expected a typed row, got {other:?}"),
        };
        assert_eq!(target(KvCounterKind::Integer).as_deref(), Some("b"));
        assert_eq!(target(KvCounterKind::Float).as_deref(), Some("a"));

        let text_only = kv_info("key", vec![column("label", SqlDataType::String, None)]);
        let row = plan_kv_counter_fresh_row(&text_only, "k", KvCounterKind::Integer, &NoCatalog)
            .expect("plan");
        assert_eq!(
            row,
            KvCounterFreshRow::Typed {
                column: None,
                cells: Vec::new(),
            }
        );
    }
}
