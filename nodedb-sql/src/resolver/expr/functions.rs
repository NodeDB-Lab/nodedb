// SPDX-License-Identifier: Apache-2.0

use std::sync::LazyLock;

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::{SCHEMA_QUALIFIED_MSG, normalize_ident};
use crate::resolver::ColumnScope;
use crate::types::*;

use super::convert::convert_expr_depth;

/// Process-wide function registry backing the undefined-function gate in
/// [`convert_function_depth`]. `FunctionRegistry::new()` rebuilds the full
/// builtin table (aggregates + scalars) from scratch, so a fresh instance
/// per function-call site would redo that work on every call in every
/// statement; a lazily-initialized singleton amortizes it, mirroring
/// `planner::const_fold::DEFAULT_REGISTRY`.
static FUNCTION_REGISTRY: LazyLock<FunctionRegistry> = LazyLock::new(FunctionRegistry::new);

pub(super) fn convert_function_depth(
    func: &ast::Function,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    // Intercept PG FTS surface functions and lower them to pg_* internal names
    // before the generic path runs.
    if func.name.0.len() == 1 {
        let raw_name = match &func.name.0[0] {
            ast::ObjectNamePart::Identifier(ident) => ident.value.to_ascii_lowercase(),
            _ => String::new(),
        };
        if let Some(expr) = intercept_fts_function(&raw_name, func, depth, scope)? {
            return Ok(expr);
        }
    }

    if func.name.0.len() > 1 {
        let qualified: String = func
            .name
            .0
            .iter()
            .map(|p| match p {
                ast::ObjectNamePart::Identifier(ident) => ident.value.clone(),
                _ => String::new(),
            })
            .collect::<Vec<_>>()
            .join(".");
        return Err(SqlError::Unsupported {
            detail: format!("schema-qualified function name '{qualified}': {SCHEMA_QUALIFIED_MSG}"),
        });
    }
    let name = func
        .name
        .0
        .iter()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join(".");

    // `current_schemas(bool)` — PostgreSQL catalog function returning TEXT[].
    // Fold to a literal array at parse time so `= ANY(current_schemas(...))` works
    // without threading session context into the data-plane evaluator.
    if (name == "current_schemas" || name == "current_schema")
        && let Some(expr) = intercept_catalog_function(&name, func, depth, scope)?
    {
        return Ok(expr);
    }

    // Plan-time existence gate: every special-form
    // interception above (FTS surface functions, catalog functions) has
    // already had its shot at `name` and returned early. Anything reaching
    // this point is a plain scalar/aggregate/window call that must be a
    // known builtin — `FunctionRegistry` is the single source of truth used
    // by aggregate/window classification and constant folding elsewhere in
    // this crate, so an unknown name here would otherwise flow through to
    // `SqlExpr::Function` and silently evaluate to `NULL` for every row at
    // runtime (`nodedb_query::functions::eval_function`'s fallback). Reject
    // it here instead, so the whole statement fails at plan time.
    if FUNCTION_REGISTRY.lookup(&name).is_none() {
        return Err(SqlError::UndefinedFunction { name });
    }

    // Per-row gate: a sequence accessor is evaluated at plan time, by
    // `planner::catalog_expr_fold` for a FROM-less SELECT and by
    // `planner::defaults` for a column DEFAULT. A call over a FROM relation
    // reaches the row evaluator instead, which holds no sequence state and
    // answers `NULL` for every row. Refuse it here so the statement fails
    // loudly at plan time.
    if scope.is_row_scope() && crate::functions::sequence_accessor::is_sequence_accessor(&name) {
        return Err(SqlError::SequencePerRowUnsupported { name });
    }

    let args = collect_function_args(func, depth, scope)?;

    let distinct = match &func.args {
        ast::FunctionArguments::List(arg_list) => {
            matches!(
                arg_list.duplicate_treatment,
                Some(ast::DuplicateTreatment::Distinct)
            )
        }
        _ => false,
    };

    Ok(SqlExpr::Function {
        name,
        args,
        distinct,
    })
}

/// Intercept PG FTS surface functions and lower them to `pg_*` internal
/// function-call `SqlExpr` nodes.  Returns `Ok(None)` for non-FTS names.
///
/// `ts_rank_cd` is rejected with `SqlError::Unsupported` as per the
/// approved design.
fn intercept_fts_function(
    name: &str,
    func: &ast::Function,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<Option<SqlExpr>> {
    use crate::functions::fts_ops::pg_fts_funcs;

    if !matches!(
        name,
        "to_tsvector"
            | "to_tsquery"
            | "plainto_tsquery"
            | "phraseto_tsquery"
            | "websearch_to_tsquery"
            | "ts_rank"
            | "ts_rank_cd"
            | "ts_headline"
    ) {
        return Ok(None);
    }
    let args = collect_function_args(func, depth, scope)?;
    match name {
        "to_tsvector" => Ok(Some(SqlExpr::Function {
            name: "pg_to_tsvector".into(),
            args,
            distinct: false,
        })),
        "to_tsquery" => Ok(Some(pg_fts_funcs::lower_pg_to_tsquery(args))),
        "plainto_tsquery" => Ok(Some(pg_fts_funcs::lower_pg_plainto_tsquery(args))),
        "phraseto_tsquery" => Ok(Some(pg_fts_funcs::lower_phraseto_tsquery(args))),
        "websearch_to_tsquery" => Ok(Some(pg_fts_funcs::lower_pg_websearch_to_tsquery(args))),
        "ts_rank" => Ok(Some(SqlExpr::Function {
            name: "pg_ts_rank".into(),
            args,
            distinct: false,
        })),
        "ts_rank_cd" => Err(SqlError::Unsupported {
            detail: "ts_rank_cd is not supported; use ts_rank or bm25_score instead".into(),
        }),
        "ts_headline" => Ok(Some(SqlExpr::Function {
            name: "pg_ts_headline".into(),
            args,
            distinct: false,
        })),
        _ => Ok(None),
    }
}

/// Intercept PostgreSQL catalog functions and fold them to literal arrays.
///
/// `current_schemas(true)` → `ARRAY['pg_catalog', 'public']`
/// `current_schemas(false)` → `ARRAY['public']`
/// `current_schema()` → `'public'`
///
/// Returns `Ok(Some(expr))` when the function is folded, `Ok(None)` otherwise.
fn intercept_catalog_function(
    name: &str,
    func: &ast::Function,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<Option<SqlExpr>> {
    let args = collect_function_args(func, depth, scope)?;
    match name {
        "current_schemas" => {
            // Determine whether to include implicit schemas (pg_catalog).
            let include_implicit = match args.first() {
                Some(SqlExpr::Literal(SqlValue::Bool(b))) => *b,
                // Default to false when arg is missing or non-literal.
                _ => false,
            };
            let mut schemas: Vec<SqlExpr> = Vec::new();
            if include_implicit {
                schemas.push(SqlExpr::Literal(SqlValue::String("pg_catalog".into())));
            }
            schemas.push(SqlExpr::Literal(SqlValue::String("public".into())));
            Ok(Some(SqlExpr::ArrayLiteral(schemas)))
        }
        "current_schema" => Ok(Some(SqlExpr::Literal(SqlValue::String("public".into())))),
        _ => Ok(None),
    }
}

/// Collect function call arguments, converting each `Expr` to `SqlExpr`.
fn collect_function_args(
    func: &ast::Function,
    depth: &mut usize,
    scope: &ColumnScope<'_>,
) -> Result<Vec<SqlExpr>> {
    match &func.args {
        ast::FunctionArguments::None => Ok(Vec::new()),
        ast::FunctionArguments::Subquery(_) => Err(SqlError::Unsupported {
            detail: "subquery in function args".into(),
        }),
        ast::FunctionArguments::List(arg_list) => arg_list
            .args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                    Some(convert_expr_depth(e, depth, scope))
                }
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                    Some(Ok(SqlExpr::Wildcard))
                }
                ast::FunctionArg::Named {
                    arg: ast::FunctionArgExpr::Expr(e),
                    ..
                } => Some(convert_expr_depth(e, depth, scope)),
                _ => None,
            })
            .collect::<Result<Vec<_>>>(),
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::*;

    /// Parse `SELECT <expr>` and return the sole projection expression.
    fn parse_expr(sql: &str) -> ast::Expr {
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            ast::Statement::Query(q) => match *q.body {
                ast::SetExpr::Select(s) => match s.projection.into_iter().next().unwrap() {
                    ast::SelectItem::UnnamedExpr(e) => e,
                    ast::SelectItem::ExprWithAlias { expr, .. } => expr,
                    other => panic!("expected a bare/aliased expr projection, got {other:?}"),
                },
                _ => panic!("expected SELECT"),
            },
            _ => panic!("expected query"),
        }
    }

    /// Parse `SELECT <call>` and return the function-call AST node.
    fn function_ast(sql: &str) -> ast::Function {
        match parse_expr(sql) {
            ast::Expr::Function(f) => f,
            other => panic!("expected a function call expr, got {other:?}"),
        }
    }

    #[test]
    fn unknown_function_name_is_rejected() {
        let func = function_ast("SELECT totally_bogus_fn(1, 2)");
        let mut depth = 0;
        let err = convert_function_depth(&func, &mut depth, &ColumnScope::Unchecked).unwrap_err();
        match err {
            SqlError::UndefinedFunction { name } => assert_eq!(name, "totally_bogus_fn"),
            other => panic!("expected SqlError::UndefinedFunction, got {other:?}"),
        }
    }

    #[test]
    fn known_scalar_function_name_resolves() {
        let func = function_ast("SELECT upper('x')");
        let mut depth = 0;
        let expr = convert_function_depth(&func, &mut depth, &ColumnScope::Unchecked).unwrap();
        match expr {
            SqlExpr::Function { name, .. } => assert_eq!(name, "upper"),
            other => panic!("expected SqlExpr::Function, got {other:?}"),
        }
    }

    /// `func.name` is only lower-cased upstream (`normalize_ident`) for
    /// *unquoted* identifiers, so a quoted `"UPPER"` reaches the registry
    /// gate with its original case intact. The gate must still resolve it —
    /// `FunctionRegistry::lookup` case-folds internally — proving the
    /// existence check itself is case-insensitive, not merely riding on
    /// upstream normalization.
    #[test]
    fn function_lookup_is_case_insensitive() {
        let mut depth = 0;
        let lower = function_ast("SELECT upper('x')");
        let upper_quoted = function_ast(r#"SELECT "UPPER"('x')"#);
        assert!(
            convert_function_depth(&lower, &mut depth, &ColumnScope::Unchecked).is_ok(),
            "lowercase 'upper' must resolve"
        );
        assert!(
            convert_function_depth(&upper_quoted, &mut depth, &ColumnScope::Unchecked).is_ok(),
            "quoted 'UPPER' must still resolve via case-insensitive registry lookup"
        );
    }

    /// A FROM-less SELECT resolves against an empty relation set, where the
    /// planner evaluates the accessor through the catalog.
    #[test]
    fn a_sequence_accessor_resolves_without_a_relation() {
        let func = function_ast("SELECT nextval('s')");
        let mut depth = 0;
        let scope = crate::resolver::columns::TableScope::new();
        let expr = convert_function_depth(&func, &mut depth, &ColumnScope::Relations(&scope))
            .expect("a FROM-less sequence accessor must resolve");
        match expr {
            SqlExpr::Function { name, .. } => assert_eq!(name, "nextval"),
            other => panic!("expected SqlExpr::Function, got {other:?}"),
        }
    }

    /// A relation in scope makes the call per-row, which the row evaluator
    /// cannot serve, so the resolver refuses it.
    #[test]
    fn a_sequence_accessor_over_a_relation_is_refused() {
        let mut depth = 0;
        let outer = crate::resolver::columns::TableScope::new();
        let scope = crate::resolver::columns::TableScope::new().nested_in(outer);
        for call in [
            "SELECT nextval('s')",
            "SELECT currval('s')",
            "SELECT setval('s', 1)",
        ] {
            let func = function_ast(call);
            let err = convert_function_depth(&func, &mut depth, &ColumnScope::Relations(&scope))
                .unwrap_err();
            assert!(
                matches!(err, SqlError::SequencePerRowUnsupported { .. }),
                "expected SqlError::SequencePerRowUnsupported for {call}, got {err:?}"
            );
        }
    }
}
