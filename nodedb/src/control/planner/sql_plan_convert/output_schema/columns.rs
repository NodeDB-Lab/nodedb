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

use nodedb_sql::SqlError;
use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::query::Projection;
use nodedb_sql::types_expr::SqlExpr;

use crate::control::planner::sql_plan_convert::group_key_name::computed_group_key_name;
use crate::control::planner::sql_plan_convert::output_schema_types::infer_computed_expr_type;
use crate::control::server::response_shape::schema::{
    CpComputedColumn, OutputColumn, OutputSchema, sql_data_type_to_ddl_col_type_with_width,
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
            // A join's map is keyed on the qualified name, because two
            // sides can carry the same bare column with different types. A
            // single-collection map holds bare names only, so the qualified
            // lookup misses there and the bare name answers.
            let ty = types
                .get(qname)
                .or_else(|| types.get(&display_name))
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
        // The Control Plane writes the evaluated value under the alias. A
        // bare sequence accessor yields a bigint; any wider expression is
        // typed like a computed entry.
        Projection::CpComputed { expr, alias } => {
            let ty = match expr {
                SqlExpr::Function { name, .. }
                    if nodedb_sql::functions::sequence_accessor::is_sequence_accessor(name) =>
                {
                    DdlColType::Int8
                }
                SqlExpr::Function { .. }
                | SqlExpr::Column { .. }
                | SqlExpr::Literal(_)
                | SqlExpr::BinaryOp { .. }
                | SqlExpr::UnaryOp { .. }
                | SqlExpr::Case { .. }
                | SqlExpr::Cast { .. }
                | SqlExpr::Subquery(_)
                | SqlExpr::Wildcard
                | SqlExpr::IsNull { .. }
                | SqlExpr::InList { .. }
                | SqlExpr::Between { .. }
                | SqlExpr::Like { .. }
                | SqlExpr::ArrayLiteral(_) => infer_computed_expr_type(expr, types),
            };
            Some(OutputColumn {
                display_name: alias.clone(),
                lookup_key: alias.clone(),
                ty,
            })
        }
        Projection::Star | Projection::QualifiedStar(_) => None,
    }
}

/// The key column `collection` declares, or `None` when its rows key by the
/// implicit `id` or the auto-generated `_rowid`, or the catalog holds no such
/// collection.
///
/// A scan row's identity renders under this column when the row lacks it.
/// The lookup is the one [`column_types_for`] makes, against the catalog the
/// statement was just planned with. A catalog error is returned, never read as
/// "no declared key": that reading renders the identity under the wrong column.
pub(super) fn declared_key_for<C: SqlCatalog + ?Sized>(
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    collection: &str,
) -> Result<Option<String>, SqlError> {
    let primary_key = catalog
        .get_collection(database_id, collection)?
        .and_then(|info| info.primary_key);
    Ok(nodedb_types::declared_key(primary_key.as_deref()).map(str::to_string))
}

/// Builds a `HashMap` of bare column name -> resolved wire type for
/// `collection`. An unknown collection yields an empty map, and callers then
/// type every column `DdlColType::Text`. A catalog error is returned.
pub(super) fn column_types_for<C: SqlCatalog + ?Sized>(
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    collection: &str,
) -> Result<HashMap<String, DdlColType>, SqlError> {
    let Some(info) = catalog.get_collection(database_id, collection)? else {
        return Ok(HashMap::new());
    };
    Ok(info
        .columns
        .iter()
        .map(|c| {
            (
                c.name.clone(),
                sql_data_type_to_ddl_col_type_with_width(&c.data_type, c.int_width, c.float_width),
            )
        })
        .collect())
}

/// Derives an `OutputColumn` for one GROUP BY key expression.
///
/// The `display_name` is the SELECT-list output name: the explicit alias when
/// the projection aliased the key (`SELECT k AS label ... GROUP BY k` yields
/// output column `label`, matching Postgres), otherwise the key's own name.
///
/// The aggregate renames an aliased key to its alias in every result row
/// unless another cell of the row ends under that name, so an aliased key
/// reads `final_name`, the key the rename leaves it under. An unaliased key
/// reads its row name: the raw grouped column name, qualified when the key
/// is, or for a computed key the shared index-based `computed_group_key_name`
/// (`group_0`), the exact name the aggregate spec emits the evaluated value
/// under.
///
/// The output type is the grouped column's catalog type when the key is a bare
/// column (resolved from `types`, default `Text`); a computed-expression key is
/// typed conservatively via [`infer_computed_expr_type`], defaulting to `Text`.
pub(super) fn group_by_key_column(
    expr: &SqlExpr,
    index: usize,
    alias: Option<&str>,
    final_name: &str,
    types: &HashMap<String, DdlColType>,
) -> OutputColumn {
    let (row_name, display_name, ty) = match expr {
        SqlExpr::Column { table, name } => (
            match table {
                Some(t) => format!("{t}.{name}"),
                None => name.clone(),
            },
            name.clone(),
            types.get(name).copied().unwrap_or(DdlColType::Text),
        ),
        _ => {
            let row_name = computed_group_key_name(index);
            (
                row_name.clone(),
                row_name,
                infer_computed_expr_type(expr, types),
            )
        }
    };
    match alias {
        Some(alias) => OutputColumn {
            display_name: alias.to_string(),
            lookup_key: final_name.to_string(),
            ty,
        },
        None => OutputColumn {
            display_name,
            lookup_key: row_name,
            ty,
        },
    }
}

/// Returns the collection's columns in declared catalog order, mapped to
/// `OutputColumn`s (`display_name` = `lookup_key` = column name). An unknown
/// collection, or one with no declared columns (a schemaless collection),
/// yields an empty `Vec`, so a schemaless `SELECT *` derives its shape from
/// the rows. A catalog error is returned.
pub(super) fn ordered_columns_for<C: SqlCatalog + ?Sized>(
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    collection: &str,
) -> Result<Vec<OutputColumn>, SqlError> {
    let Some(info) = catalog.get_collection(database_id, collection)? else {
        return Ok(Vec::new());
    };
    Ok(info
        .columns
        .iter()
        .map(|c| OutputColumn {
            display_name: c.name.clone(),
            lookup_key: c.name.clone(),
            ty: sql_data_type_to_ddl_col_type_with_width(&c.data_type, c.int_width, c.float_width),
        })
        .collect())
}

/// Maps a projection list to an `OutputSchema` fragment using `types`.
///
/// A `Star` / `QualifiedStar` in the projection sets `is_star` and expands
/// into `ordered_cols` (the collection's catalog columns in declared order),
/// appending only entries not already produced by a named projection. When
/// the projection has no star, `ordered_cols` is ignored and behavior is the
/// named-columns-only, `is_star=false` case.
///
/// A `CpComputed` entry is announced under its alias like any other column
/// and also recorded in `cp_computed`, in SELECT-list order, so the response
/// shaper evaluates it on the Control Plane before the projection runs.
pub(super) fn schema_from_projection(
    projection: &[Projection],
    types: &HashMap<String, DdlColType>,
    ordered_cols: &[OutputColumn],
) -> OutputSchema {
    let mut columns = Vec::with_capacity(projection.len());
    let mut cp_computed = Vec::new();
    let mut is_star = false;
    for p in projection {
        if let Projection::CpComputed { expr, alias } = p {
            cp_computed.push(CpComputedColumn {
                alias: alias.clone(),
                expr: super::super::expr::sql_expr_to_bridge_expr(expr),
            });
        }
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
    OutputSchema {
        columns,
        is_star,
        cp_computed,
        declared_key: None,
    }
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
    fn cp_computed_accessor_is_int8_under_its_alias() {
        let types = HashMap::new();
        let nextval = SqlExpr::Function {
            name: "nextval".to_string(),
            args: vec![SqlExpr::Literal(nodedb_sql::types_expr::SqlValue::String(
                "s".to_string(),
            ))],
            distinct: false,
        };
        let bare = Projection::CpComputed {
            expr: nextval.clone(),
            alias: "nextval".to_string(),
        };
        let col = projection_to_column(&bare, &types).expect("Some for CpComputed");
        assert_eq!(col.display_name, "nextval");
        assert_eq!(col.lookup_key, "nextval");
        assert_eq!(col.ty, DdlColType::Int8);

        let wider = Projection::CpComputed {
            expr: SqlExpr::BinaryOp {
                left: Box::new(nextval),
                op: nodedb_sql::types_expr::BinaryOp::Gt,
                right: Box::new(SqlExpr::Literal(nodedb_sql::types_expr::SqlValue::Int(0))),
            },
            alias: "positive".to_string(),
        };
        let col = projection_to_column(&wider, &types).expect("Some for CpComputed");
        assert_eq!(col.lookup_key, "positive");
        assert_eq!(col.ty, DdlColType::Bool);
    }

    #[test]
    fn cp_computed_entries_are_collected_in_select_list_order() {
        let types = HashMap::new();
        let accessor = |name: &str| SqlExpr::Function {
            name: name.to_string(),
            args: vec![SqlExpr::Literal(nodedb_sql::types_expr::SqlValue::String(
                "s".to_string(),
            ))],
            distinct: false,
        };
        let projection = vec![
            Projection::Column("id".to_string()),
            Projection::CpComputed {
                expr: accessor("nextval"),
                alias: "n".to_string(),
            },
            Projection::CpComputed {
                expr: accessor("currval"),
                alias: "c".to_string(),
            },
        ];
        let schema = schema_from_projection(&projection, &types, &[]);
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[1].display_name, "n");
        assert_eq!(schema.columns[2].display_name, "c");
        let aliases: Vec<&str> = schema
            .cp_computed
            .iter()
            .map(|c| c.alias.as_str())
            .collect();
        assert_eq!(aliases, ["n", "c"]);
        assert!(matches!(
            &schema.cp_computed[0].expr,
            crate::bridge::expr_eval::SqlExpr::Function { name, .. } if name == "nextval"
        ));
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
