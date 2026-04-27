//! nodedb-sql: SQL parser, planner, and optimizer for NodeDB.
//!
//! Parses SQL via sqlparser-rs, resolves against a catalog, and produces
//! `SqlPlan` — an intermediate representation that both Origin (server)
//! and Lite (embedded) map to their own execution model.
//!
//! ```text
//! SQL → parse → resolve → plan → optimize → SqlPlan
//! ```

pub mod aggregate_walk;
pub mod catalog;
pub mod coerce;
pub mod ddl_ast;
pub mod dsl_bind;
pub mod engine_rules;
pub mod error;
pub mod functions;
pub mod optimizer;
pub mod params;
pub mod parser;
pub mod planner;
pub mod resolver;
pub mod temporal;
pub mod types;
pub mod types_array;
pub mod types_expr;

pub use temporal::{TemporalScope, ValidTime};

pub use catalog::{SqlCatalog, SqlCatalogError};
pub use error::{Result, SqlError};
pub use params::ParamValue;
pub use types::*;

/// Parse a standalone SQL expression string into an `SqlExpr`.
///
/// Used by the DEFAULT expression evaluator to handle arbitrary expressions
/// (e.g. `upper('x')`, `1 + 2`) that don't match the hard-coded keyword list.
pub fn parse_expr_string(expr_text: &str) -> Result<SqlExpr> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let ast_expr = Parser::new(&dialect)
        .try_with_sql(expr_text)
        .map_err(|e| SqlError::Parse {
            detail: e.to_string(),
        })?
        .parse_expr()
        .map_err(|e| SqlError::Parse {
            detail: e.to_string(),
        })?;

    resolver::expr::convert_expr(&ast_expr)
}

use functions::registry::FunctionRegistry;
use parser::array_stmt::{ArrayStatement, try_parse_array_statement};
use parser::preprocess;
use parser::statement::{StatementKind, classify, parse_sql};

/// Plan one or more SQL statements against the given catalog.
///
/// Handles NodeDB-specific syntax (UPSERT, `{ }` object literals) via
/// pre-processing before handing to sqlparser.
pub fn plan_sql(sql: &str, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    // Array DDL/DML uses non-standard syntax that sqlparser-rs cannot
    // accept. Intercept it before the standard preprocess pipeline.
    if let Some(stmt) = try_parse_array_statement(sql)? {
        return plan_array_statement(stmt, catalog);
    }
    let preprocessed = preprocess::preprocess(sql)?;
    let effective_sql = preprocessed.as_ref().map_or(sql, |p| p.sql.as_str());
    let is_upsert = preprocessed.as_ref().is_some_and(|p| p.is_upsert);
    let temporal = preprocessed
        .as_ref()
        .map(|p| p.temporal)
        .unwrap_or_default();

    let statements = parse_sql(effective_sql)?;
    plan_statements(&statements, is_upsert, temporal, catalog)
}

/// Plan SQL with bound parameters (prepared statement execution).
///
/// Parses the SQL (which may contain `$1`, `$2`, ... placeholders), substitutes
/// placeholder AST nodes with concrete literal values from `params`, then plans
/// normally. This avoids SQL text substitution entirely — parameters are bound
/// at the AST level, not the string level.
pub fn plan_sql_with_params(
    sql: &str,
    params: &[ParamValue],
    catalog: &dyn SqlCatalog,
) -> Result<Vec<SqlPlan>> {
    // Array DDL/DML never carries `$N` placeholders, but be defensive:
    // intercept the same way as the no-params path so the array surface
    // is reachable even if a client uses extended-query mode.
    if let Some(stmt) = try_parse_array_statement(sql)? {
        let _ = params; // arrays don't accept bound params today
        return plan_array_statement(stmt, catalog);
    }
    let preprocessed = preprocess::preprocess(sql)?;
    let effective_sql = preprocessed.as_ref().map_or(sql, |p| p.sql.as_str());
    let is_upsert = preprocessed.as_ref().is_some_and(|p| p.is_upsert);
    let temporal = preprocessed
        .as_ref()
        .map(|p| p.temporal)
        .unwrap_or_default();

    let mut statements = parse_sql(effective_sql)?;
    for stmt in &mut statements {
        params::bind_params(stmt, params);
    }
    plan_statements(&statements, is_upsert, temporal, catalog)
}

/// Plan a list of parsed statements.
fn plan_statements(
    statements: &[sqlparser::ast::Statement],
    is_upsert: bool,
    temporal: TemporalScope,
    catalog: &dyn SqlCatalog,
) -> Result<Vec<SqlPlan>> {
    let functions = FunctionRegistry::new();
    let mut plans = Vec::new();

    for stmt in statements {
        match classify(stmt) {
            StatementKind::Select(query) => {
                let plan = planner::select::plan_query(query, catalog, &functions, temporal)?;
                let plan = optimizer::optimize(plan);
                plans.push(plan);
            }
            StatementKind::Insert(ins) => {
                let mut dml_plans = if is_upsert {
                    planner::dml::plan_upsert(ins, catalog)?
                } else {
                    planner::dml::plan_insert(ins, catalog)?
                };
                plans.append(&mut dml_plans);
            }
            StatementKind::Update(stmt) => {
                let mut update_plans = planner::dml::plan_update(stmt, catalog)?;
                plans.append(&mut update_plans);
            }
            StatementKind::Delete(stmt) => {
                let mut delete_plans = planner::dml::plan_delete(stmt, catalog)?;
                plans.append(&mut delete_plans);
            }
            StatementKind::Truncate(stmt) => {
                let mut trunc_plans = planner::dml::plan_truncate_stmt(stmt)?;
                plans.append(&mut trunc_plans);
            }
            StatementKind::Other => {
                return Err(SqlError::Unsupported {
                    detail: format!("statement type: {stmt}"),
                });
            }
        }
    }

    Ok(plans)
}

/// Plan one parsed array statement.
fn plan_array_statement(stmt: ArrayStatement, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    match stmt {
        ArrayStatement::Create(c) => Ok(vec![planner::array_ddl::plan_create_array(&c)?]),
        ArrayStatement::Drop(d) => Ok(vec![planner::array_ddl::plan_drop_array(&d)?]),
        ArrayStatement::Insert(i) => planner::array_dml::plan_insert_array(&i, catalog),
        ArrayStatement::Delete(d) => planner::array_dml::plan_delete_array(&d, catalog),
        ArrayStatement::Alter(a) => Ok(vec![planner::array_ddl::plan_alter_array(&a)?]),
    }
}
