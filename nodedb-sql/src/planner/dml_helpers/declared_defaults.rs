// SPDX-License-Identifier: Apache-2.0

//! Materialization of declared column DEFAULTs into a parsed `VALUES` row.

use crate::catalog::SqlCatalog;
use crate::error::Result;
use crate::types::*;

/// Fill in every declared column the statement omitted that carries a DEFAULT.
///
/// The key-value and vector-primary engines store the values they are handed
/// and have no typed write path, so a DEFAULT that is not materialized HERE is
/// materialized nowhere: the catalog would keep the declaration and every read
/// return nothing for it. Documents and columnar rows expand theirs through the
/// same `evaluate_default_expr`, so one expression yields one value on every
/// engine.
///
/// Two rules the ordering encodes:
///
/// - A column the statement SUPPLIED is never touched, and that includes an
///   explicit `NULL`. `NULL` is a value the author chose; overwriting it with
///   the default would make it impossible to store one.
/// - Materialized values are appended BEFORE the caller's declared-type
///   coercion and range checks, so a default is validated exactly like a
///   supplied literal. Filling them in afterwards would make `DEFAULT 999999`
///   on a `SMALLINT` column a way to store a value the same literal is
///   rejected for.
///
/// A DEFAULT the evaluator cannot resolve raises `SqlError::UnevaluableDefault`
/// rather than leaving the column out. `catalog` resolves `nextval` / `currval`.
///
/// Returns whether any materialized default came from a `Volatile`
/// expression, so the caller can keep the plan out of the plan cache.
pub(crate) fn materialize_declared_defaults(
    declared_columns: &[ColumnInfo],
    row: &mut Vec<(String, SqlValue)>,
    catalog: &dyn SqlCatalog,
) -> Result<bool> {
    let mut volatile = false;
    for column in declared_columns {
        let Some(default_expr) = column.default.as_deref() else {
            continue;
        };
        if row.iter().any(|(name, _)| name == &column.name) {
            continue;
        }
        let evaluated =
            crate::planner::defaults::evaluate_default_expr(default_expr, &column.name, catalog)?;
        let value = crate::planner::defaults::default_value_to_sql(&column.name, evaluated)?;
        volatile |= crate::types::plan::default_expr_is_volatile(default_expr);
        row.push((column.name.clone(), value));
    }
    Ok(volatile)
}

/// Materialize declared DEFAULTs across a whole `VALUES` row set.
///
/// Returns whether any materialized default was volatile.
pub(crate) fn materialize_defaults_in_rows(
    declared_columns: &[ColumnInfo],
    rows: &mut [Vec<(String, SqlValue)>],
    catalog: &dyn SqlCatalog,
) -> Result<bool> {
    let mut volatile = false;
    for row in rows.iter_mut() {
        volatile |= materialize_declared_defaults(declared_columns, row, catalog)?;
    }
    Ok(volatile)
}
