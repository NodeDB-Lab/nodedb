// SPDX-License-Identifier: BUSL-1.1

//! Column-level derivation shared by every output-schema rule.
//!
//! One projection entry, one GROUP BY key, or one collection's declared column
//! list maps to [`OutputColumn`]s here. Bare columns carry their real catalog
//! type; computed expressions are typed conservatively via
//! [`output_schema_types`](crate::control::planner::sql_plan_convert::output_schema_types).
//! A wrong non-TEXT OID makes clients fail to parse the text value, so every
//! uncertain case falls back to `DdlColType::Text`, the safe default.

use std::collections::HashMap;

use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::query::Projection;
use nodedb_sql::types_expr::SqlExpr;

use crate::control::planner::sql_plan_convert::group_key_name::computed_group_key_name;
use crate::control::planner::sql_plan_convert::output_schema_types::infer_computed_expr_type;
use crate::control::server::response_shape::schema::{
    OutputColumn, OutputSchema, sql_data_type_to_ddl_col_type_with_width,
};
use crate::control::server::response_shape::types::DdlColType;

/// Maps one `Projection` entry to an `OutputColumn`, given a map of bare
/// column name -> resolved wire type for the collection in scope.
///
/// This is the authoritative derivation rule (see also [`schema_from_projection`]):
/// for a qualified `table.column` reference, `lookup_key` keeps the full
/// dot-joined form (the join executor prefixes every key with its source
/// collection name) while `display_name` is the last segment. For a bare
/// column both are identical.
///
/// `Projection::Star` / `Projection::QualifiedStar` have no single concrete
/// column and return `None`; the caller sets `is_star` instead.
pub(super) fn projection_to_column(
    p: &Projection,
    types: &HashMap<String, DdlColType>,
) -> Option<OutputColumn> {
    match p {
        Projection::Column(qname) => {
            let display_name = qname
                .rsplit('.')
                .next()
                .map(str::to_string)
                .unwrap_or_else(|| qname.clone());
            let ty = types
                .get(&display_name)
                .copied()
                .unwrap_or(DdlColType::Text);
            Some(OutputColumn {
                display_name,
                lookup_key: qname.clone(),
                ty,
            })
        }
        Projection::Computed { expr, alias } => {
            // For an aliased column reference (`o.id AS oid`) the Data Plane
            // keys the value by the underlying column, not the alias — so the
            // lookup_key must be the qualified expression (matching the join
            // executor's prefixed keys) while the alias is only the display
            // name. A genuine computed expression (`price * qty AS total`) is
            // emitted by the executor under its alias, so that stays the key.
            let lookup_key = match expr {
                SqlExpr::Column {
                    table: Some(t),
                    name,
                } => format!("{t}.{name}"),
                SqlExpr::Column { table: None, name } => name.clone(),
                _ => alias.clone(),
            };
            Some(OutputColumn {
                display_name: alias.clone(),
                lookup_key,
                ty: infer_computed_expr_type(expr, types),
            })
        }
        Projection::Star | Projection::QualifiedStar(_) => None,
    }
}

/// Builds a `HashMap` of bare column name -> resolved wire type for
/// `collection`, via a best-effort catalog lookup. Returns an empty map
/// (never an error) when the lookup fails or the collection is unknown —
/// callers fall back to `DdlColType::Text` for every column in that case.
pub(super) fn column_types_for<C: SqlCatalog + ?Sized>(
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    collection: &str,
) -> HashMap<String, DdlColType> {
    match catalog.get_collection(database_id, collection) {
        Ok(Some(info)) => info
            .columns
            .iter()
            .map(|c| {
                (
                    c.name.clone(),
                    sql_data_type_to_ddl_col_type_with_width(
                        &c.data_type,
                        c.int_width,
                        c.float_width,
                    ),
                )
            })
            .collect(),
        _ => HashMap::new(),
    }
}

/// Derives an `OutputColumn` for one GROUP BY key expression.
///
/// The `display_name` is the SELECT-list output name: the explicit alias when
/// the projection aliased the key (`SELECT k AS label ... GROUP BY k` yields
/// output column `label`, matching Postgres), otherwise the key's own column
/// name. The `lookup_key` always stays the raw grouped column name (the key
/// the aggregate executor emits the value under), so `project_row` still finds
/// the value.
///
/// The output type is the grouped column's catalog type when the key is a bare
/// column (resolved from `types`, default `Text`); a computed-expression key is
/// typed conservatively via [`infer_computed_expr_type`], defaulting to `Text`.
///
/// A non-`Column` GROUP BY key (a computed expression) derives its `lookup_key`
/// from the shared index-based `computed_group_key_name` rule — the exact name
/// the aggregate spec emits the evaluated value under, so the two can never
/// diverge. Its `display_name` is the SELECT-list alias when present
/// (`UPPER(label) AS u` shows column `u`), else the same placeholder.
pub(super) fn group_by_key_column(
    expr: &SqlExpr,
    index: usize,
    alias: Option<&str>,
    types: &HashMap<String, DdlColType>,
) -> OutputColumn {
    match expr {
        SqlExpr::Column { table, name } => {
            let lookup_key = match table {
                Some(t) => format!("{t}.{name}"),
                None => name.clone(),
            };
            let display_name = alias.map(str::to_string).unwrap_or_else(|| name.clone());
            let ty = types.get(name).copied().unwrap_or(DdlColType::Text);
            OutputColumn {
                display_name,
                lookup_key,
                ty,
            }
        }
        _ => {
            // The executor emits the evaluated value under the shared
            // index-based name (see `group_by_to_specs`), so `lookup_key` MUST
            // equal it. `display_name` is the SELECT-list alias when present
            // (`UPPER(label) AS u` shows column `u`), else the same placeholder.
            let lookup_key = computed_group_key_name(index);
            let display_name = alias
                .map(str::to_string)
                .unwrap_or_else(|| lookup_key.clone());
            OutputColumn {
                display_name,
                lookup_key,
                ty: infer_computed_expr_type(expr, types),
            }
        }
    }
}

/// Returns the collection's columns in declared catalog order, mapped to
/// `OutputColumn`s (`display_name` = `lookup_key` = column name). Returns an
/// empty `Vec` when the catalog/collection lookup fails or the collection has
/// no declared columns (e.g. a schemaless collection) — so a schemaless
/// `SELECT *` still yields empty columns, deriving its shape from the rows.
pub(super) fn ordered_columns_for<C: SqlCatalog + ?Sized>(
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    collection: &str,
) -> Vec<OutputColumn> {
    match catalog.get_collection(database_id, collection) {
        Ok(Some(info)) => info
            .columns
            .iter()
            .map(|c| OutputColumn {
                display_name: c.name.clone(),
                lookup_key: c.name.clone(),
                ty: sql_data_type_to_ddl_col_type_with_width(
                    &c.data_type,
                    c.int_width,
                    c.float_width,
                ),
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Maps a projection list to an `OutputSchema` fragment using `types`.
///
/// A `Star` / `QualifiedStar` in the projection sets `is_star` and expands
/// into `ordered_cols` (the collection's catalog columns in declared order),
/// appending only entries not already produced by a named projection. When
/// the projection has no star, `ordered_cols` is ignored and behavior is the
/// named-columns-only, `is_star=false` case.
pub(super) fn schema_from_projection(
    projection: &[Projection],
    types: &HashMap<String, DdlColType>,
    ordered_cols: &[OutputColumn],
) -> OutputSchema {
    let mut columns = Vec::with_capacity(projection.len());
    let mut is_star = false;
    for p in projection {
        match projection_to_column(p, types) {
            Some(col) => columns.push(col),
            None => {
                is_star = true;
                for oc in ordered_cols {
                    if !columns.iter().any(|c| c.lookup_key == oc.lookup_key) {
                        columns.push(oc.clone());
                    }
                }
            }
        }
    }
    OutputSchema { columns, is_star }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_column_uses_matching_type_from_map() {
        let mut types = HashMap::new();
        types.insert("foo".to_string(), DdlColType::Int8);
        let p = Projection::Column("foo".to_string());
        let col = projection_to_column(&p, &types).expect("Some for Column");
        assert_eq!(col.lookup_key, "foo");
        assert_eq!(col.display_name, "foo");
        assert_eq!(col.ty, DdlColType::Int8);
    }

    #[test]
    fn qualified_column_display_is_last_segment() {
        let types = HashMap::new();
        let p = Projection::Column("t.bar".to_string());
        let col = projection_to_column(&p, &types).expect("Some for Column");
        assert_eq!(col.lookup_key, "t.bar");
        assert_eq!(col.display_name, "bar");
        assert_eq!(col.ty, DdlColType::Text);
    }

    #[test]
    fn computed_uses_alias_for_both_and_defaults_to_text() {
        let types = HashMap::new();
        let p = Projection::Computed {
            expr: nodedb_sql::types_expr::SqlExpr::Wildcard,
            alias: "total".to_string(),
        };
        let col = projection_to_column(&p, &types).expect("Some for Computed");
        assert_eq!(col.lookup_key, "total");
        assert_eq!(col.display_name, "total");
        assert_eq!(col.ty, DdlColType::Text);
    }

    #[test]
    fn star_returns_none() {
        let types = HashMap::new();
        assert!(projection_to_column(&Projection::Star, &types).is_none());
        assert!(
            projection_to_column(&Projection::QualifiedStar("t".to_string()), &types).is_none()
        );
    }
}
