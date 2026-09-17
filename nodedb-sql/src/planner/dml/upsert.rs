// SPDX-License-Identifier: Apache-2.0

//! UPSERT and `INSERT ... ON CONFLICT DO UPDATE` planning.

use sqlparser::ast;

use super::super::dml_helpers::{
    KvInsertParams, build_kv_insert_plan, check_declared_float_ranges_in_assignments,
    check_declared_int_ranges_in_assignments, resolve_insert_columns,
};
use super::target::{
    column_schema, insert_columns, resolve_target, target_scope, typed_rows, values_rows,
};
use crate::engine_rules::{self, UpsertParams};
use crate::error::Result;
use crate::planner::declared_type_coerce::coerce_assignments_to_declared_types;
use crate::types::*;

/// Plan an UPSERT statement (pre-processed from `UPSERT INTO` to `INSERT INTO`).
///
/// Same parsing as INSERT but routes through `engine_rules.plan_upsert()`.
pub fn plan_upsert(ins: &ast::Insert, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    plan_upsert_rows(ins, catalog, "UPSERT", Vec::new())
}

/// Plan an `INSERT ... ON CONFLICT DO UPDATE SET` statement.
pub(super) fn plan_upsert_with_on_conflict(
    ins: &ast::Insert,
    catalog: &dyn SqlCatalog,
    on_conflict_updates: Vec<(String, SqlExpr)>,
) -> Result<Vec<SqlPlan>> {
    plan_upsert_rows(ins, catalog, "INSERT ... ON CONFLICT", on_conflict_updates)
}

/// The shared body of `UPSERT` and `INSERT ... ON CONFLICT DO UPDATE`.
///
/// The two differ only in `verb` (for error messages) and in whether
/// `on_conflict_updates` carries per-row assignments for the Data Plane to
/// apply against the existing row.
fn plan_upsert_rows(
    ins: &ast::Insert,
    catalog: &dyn SqlCatalog,
    verb: &str,
    mut on_conflict_updates: Vec<(String, SqlExpr)>,
) -> Result<Vec<SqlPlan>> {
    let (table_name, info) = resolve_target(ins, verb, catalog)?;
    let columns = insert_columns(&ins.columns, &target_scope(&table_name, &info)?)?;
    let rows_ast = values_rows(ins, verb)?;

    // KV: upsert is a PUT (natural overwrite), and `INSERT ... ON CONFLICT
    // (key) DO UPDATE SET ...` is the same physical write with the optional
    // per-row assignments carried through. Positional column binding
    // (below) does not apply here — see `plan_insert`.
    if info.engine == EngineType::KeyValue {
        return build_kv_insert_plan(KvInsertParams {
            collection: table_name,
            columns: &columns,
            rows_ast,
            intent: KvInsertIntent::Put,
            on_conflict_updates,
            pk_col: info.primary_key.as_deref(),
            declared_columns: &info.columns,
            catalog,
        });
    }

    // Positional UPSERT (no column list): bind to the collection's declared
    // column order — see `plan_insert` for the full rationale.
    let columns = resolve_insert_columns(columns, &info, rows_ast)?;

    let typed = typed_rows(&info, &columns, rows_ast, catalog)?;
    // `DO UPDATE SET col = <literal>` writes through the same path as the
    // inserted row, so its literals carry the same declared-type contract.
    coerce_assignments_to_declared_types(
        &info.columns,
        &mut on_conflict_updates,
        info.primary_key.as_deref(),
    )?;
    check_declared_int_ranges_in_assignments(&info.columns, &on_conflict_updates)?;
    check_declared_float_ranges_in_assignments(&info.columns, &on_conflict_updates)?;
    let column_schema = column_schema(&info);
    let rules = engine_rules::resolve_engine_rules(info.engine);
    rules.plan_upsert(UpsertParams {
        collection: table_name,
        columns,
        rows: typed.rows,
        volatile_defaults: typed.volatile_defaults,
        on_conflict_updates,
        column_schema,
        primary_key: info.primary_key.clone(),
    })
}
