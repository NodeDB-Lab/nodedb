// SPDX-License-Identifier: Apache-2.0

//! Materialization of declared column DEFAULTs into a parsed `VALUES` row set.

use crate::catalog::SqlCatalog;
use crate::error::Result;
use crate::planner::defaults::ColumnDefaults;
use crate::types::*;

/// Materialize declared DEFAULTs across a whole `VALUES` row set.
///
/// The key-value and vector-primary engines store the values they are handed
/// and have no typed write path, so a DEFAULT that is not materialized HERE is
/// materialized nowhere: the catalog would keep the declaration and every read
/// return nothing for it. Documents and columnar rows expand theirs through the
/// same [`ColumnDefaults`], so one expression yields one value on every engine.
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
/// Every declaration compiles once, before the row loop, so a multi-row
/// `VALUES` clause parses each DEFAULT expression exactly once.
///
/// A DEFAULT the evaluator cannot resolve raises `SqlError::UnevaluableDefault`
/// rather than leaving the column out. `catalog` resolves `nextval` / `currval`.
///
/// Returns whether any materialized default was volatile, so the caller can
/// keep the plan out of the plan cache.
pub(crate) fn materialize_defaults_in_rows(
    declared_columns: &[ColumnInfo],
    rows: &mut [Vec<(String, SqlValue)>],
    catalog: &dyn SqlCatalog,
) -> Result<bool> {
    let compiled = ColumnDefaults::compile_columns(declared_columns)?;
    if compiled.is_empty() {
        return Ok(false);
    }
    let mut volatile = false;
    for row in rows.iter_mut() {
        volatile |= compiled.materialize_row(row, catalog)?;
    }
    Ok(volatile)
}
