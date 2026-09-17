// SPDX-License-Identifier: Apache-2.0

//! The parts of an INSERT-shaped statement every row-writing planner shares:
//! target resolution, the column namespace, `ON CONFLICT` classification,
//! and the one row-typing pass.

use nodedb_types::DatabaseId;
use sqlparser::ast;

use super::super::dml_helpers::{
    coerce_and_check_rows, convert_value_rows, materialize_defaults_in_rows,
};
use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_insert_column, normalize_object_name_checked};
use crate::resolver::ColumnScope;
use crate::resolver::columns::{ResolvedTable, TableScope};
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// The pseudo-relation `ON CONFLICT DO UPDATE` uses for the proposed row.
const EXCLUDED_RELATION: &str = "excluded";

/// The statement's target collection, resolved through the catalog.
///
/// `verb` names the statement in the refusal a non-collection target gets.
pub(super) fn resolve_target(
    ins: &ast::Insert,
    verb: &str,
    catalog: &dyn SqlCatalog,
) -> Result<(String, CollectionInfo)> {
    let table_name = match &ins.table {
        ast::TableObject::TableName(name) => normalize_object_name_checked(name)?,
        ast::TableObject::TableFunction(_) => {
            return Err(SqlError::Unsupported {
                detail: format!("{verb} INTO a table function is not supported"),
            });
        }
        // Oracle's `INSERT INTO (SELECT ...)`: the target is a subquery, so
        // there is no collection to resolve or route to an engine.
        ast::TableObject::TableQuery(_) => {
            return Err(SqlError::Unsupported {
                detail: format!("{verb} INTO a subquery target is not supported"),
            });
        }
    };
    let info = catalog
        .get_collection(DatabaseId::DEFAULT, &table_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: table_name.clone(),
        })?;
    Ok((table_name, info))
}

/// The statement's `VALUES` rows.
///
/// `verb` names the statement in the refusal a non-`VALUES` source gets.
pub(super) fn values_rows<'a>(
    ins: &'a ast::Insert,
    verb: &str,
) -> Result<&'a [ast::Parens<Vec<ast::Expr>>]> {
    let source = ins.source.as_ref().ok_or_else(|| SqlError::Parse {
        detail: format!("{verb} requires VALUES"),
    })?;
    match &*source.body {
        ast::SetExpr::Values(values) => Ok(&values.rows),
        _ => Err(SqlError::Unsupported {
            detail: format!("{verb} source must be VALUES"),
        }),
    }
}

/// The column namespace of an INSERT target.
pub(super) fn target_scope(table_name: &str, info: &CollectionInfo) -> Result<TableScope> {
    let mut scope = TableScope::single(ResolvedTable {
        name: table_name.to_string(),
        alias: None,
        info: info.clone(),
    })?;
    // `ON CONFLICT DO UPDATE` addresses the proposed row as `excluded`. It
    // carries the target's columns and is qualified-only, so a bare name in
    // the SET clause names the stored row.
    scope.add_qualified_only(ResolvedTable {
        name: EXCLUDED_RELATION.to_string(),
        alias: None,
        info: info.clone(),
    })?;
    Ok(scope)
}

/// Normalize an INSERT column list and reject a name the target does not have.
pub(super) fn insert_columns(
    columns: &[ast::ObjectName],
    scope: &TableScope,
) -> Result<Vec<String>> {
    columns
        .iter()
        .map(|c| {
            let col = normalize_insert_column(c)?;
            scope.check_name(None, &col)?;
            Ok(col)
        })
        .collect()
}

/// Classification of an `ON CONFLICT` clause attached to an INSERT.
pub(super) enum OnConflict {
    /// No `ON CONFLICT` clause — plain INSERT (error on duplicate PK).
    None,
    /// `ON CONFLICT DO NOTHING` — skip rows that would conflict, no error.
    DoNothing,
    /// `ON CONFLICT (...) DO UPDATE SET ...` — apply the assignments against
    /// the existing row on conflict.
    DoUpdate(Vec<(String, SqlExpr)>),
}

pub(super) fn classify_on_conflict(ins: &ast::Insert, scope: &TableScope) -> Result<OnConflict> {
    let Some(on) = ins.on.as_ref() else {
        return Ok(OnConflict::None);
    };
    let ast::OnInsert::OnConflict(oc) = on else {
        return Ok(OnConflict::None);
    };
    match &oc.action {
        ast::OnConflictAction::DoNothing => Ok(OnConflict::DoNothing),
        ast::OnConflictAction::DoUpdate(do_update) => {
            let mut pairs = Vec::with_capacity(do_update.assignments.len());
            for a in &do_update.assignments {
                let name = match &a.target {
                    ast::AssignmentTarget::ColumnName(obj) => normalize_object_name_checked(obj)?,
                    ast::AssignmentTarget::Tuple(_) => {
                        return Err(SqlError::Unsupported {
                            detail: "ON CONFLICT DO UPDATE SET target must be a column name".into(),
                        });
                    }
                };
                scope.check_name(None, &name)?;
                let expr = convert_expr(&a.value, &ColumnScope::Relations(scope))?;
                pairs.push((name, expr));
            }
            Ok(OnConflict::DoUpdate(pairs))
        }
    }
}

/// A `VALUES` row set with every declared DEFAULT materialized and every
/// value coerced to its declared column type.
pub(super) struct TypedRows {
    pub rows: Vec<Vec<(String, SqlValue)>>,
    /// Whether a materialized DEFAULT was volatile. A plan carrying one is
    /// never admitted to the plan cache.
    pub volatile_defaults: bool,
}

/// Resolve `VALUES` literals, fill in declared DEFAULTs, then coerce and
/// range-check every cell against its declared column type.
///
/// This is the one row-typing pass for every engine. Defaults are
/// materialized BEFORE coercion so a defaulted value is checked exactly like
/// a supplied literal: `DEFAULT 999999` on a `SMALLINT` column is refused
/// where `VALUES (999999)` is, and `DEFAULT 1583402400000` on a `TIMESTAMP`
/// column becomes the same instant `VALUES (1583402400000)` does. Nothing
/// downstream reads the catalog's DEFAULT text again.
pub(super) fn typed_rows(
    info: &CollectionInfo,
    columns: &[String],
    rows_ast: &[ast::Parens<Vec<ast::Expr>>],
    catalog: &dyn SqlCatalog,
) -> Result<TypedRows> {
    let mut rows = convert_value_rows(columns, rows_ast)?;
    let volatile_defaults = materialize_defaults_in_rows(&info.columns, &mut rows, catalog)?;
    coerce_and_check_rows(info, &mut rows)?;
    Ok(TypedRows {
        rows,
        volatile_defaults,
    })
}

/// Raw column type strings from the catalog: `(column_name, type_str)`.
///
/// Columnar converters read these to reconstruct the exact `ColumnType` for
/// columns whose `SqlDataType` is ambiguous.
pub(super) fn column_schema(info: &CollectionInfo) -> Vec<(String, String)> {
    info.columns
        .iter()
        .filter_map(|c| c.raw_type.as_ref().map(|t| (c.name.clone(), t.clone())))
        .collect()
}
