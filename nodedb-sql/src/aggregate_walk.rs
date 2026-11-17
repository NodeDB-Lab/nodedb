// SPDX-License-Identifier: Apache-2.0

//! AST traversal for aggregate detection and extraction.
//!
//! Uses sqlparser's own `Visitor` so aggregates nested inside any
//! expression position — `CASE`, `CAST`, `COALESCE`, `-SUM(x)`,
//! `SUM(x) BETWEEN ...`, `SUM(x) IN (...)`, window specs, array
//! elements — are found without us maintaining a hand-written walker
//! that has to enumerate every `Expr` variant.
//!
//! # What this replaces
//!
//! Two earlier hand-written walkers only descended through `Function`,
//! `BinaryOp`, and `Nested`. Every other position was a silent
//! fall-through that produced wrong plans for ordinary SQL:
//!
//! - `SELECT CASE WHEN x > 0 THEN SUM(y) ELSE 0 END FROM t`
//! - `SELECT CAST(SUM(x) AS TEXT) FROM t`
//! - `SELECT -SUM(x) FROM t`
//! - `SELECT COALESCE(SUM(x), 0) FROM t`
//!
//! None of those were recognised as aggregates; the planner took the
//! non-aggregate path and produced a plan that executed the inner
//! column scan without grouping.

use core::ops::ControlFlow;

use sqlparser::ast::{self, Expr, Visit, Visitor};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::normalize_ident;
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::{AggregateExpr, SqlExpr, SqlValue};

/// Return `true` if any aggregate function call appears anywhere inside
/// `expr` — at any nesting depth, inside any expression position.
pub fn contains_aggregate(expr: &Expr, functions: &FunctionRegistry) -> bool {
    let mut detector = AggregateDetector {
        functions,
        found: false,
    };
    let _ = expr.visit(&mut detector);
    detector.found
}

/// Return `true` if `expr` is one aggregate call and nothing else, past any
/// parentheses: `SUM(x)`, not `ROUND(SUM(x), 1)` or `SUM(x) / 2`.
pub fn is_bare_aggregate_call(expr: &Expr, functions: &FunctionRegistry) -> bool {
    match expr {
        Expr::Nested(inner) => is_bare_aggregate_call(inner, functions),
        Expr::Function(f) => functions.is_aggregate(&function_name(f)) && f.over.is_none(),
        _ => false,
    }
}

/// Return `true` if `expr` computes over an aggregate: it holds an aggregate
/// call and is not one bare aggregate call. A grouped plan emits the
/// aggregate's value, so such an expression is evaluated over the group row.
pub fn computes_over_aggregate(expr: &Expr, functions: &FunctionRegistry) -> bool {
    contains_aggregate(expr, functions) && !is_bare_aggregate_call(expr, functions)
}

/// Extract every aggregate function call from `expr`, binding each to
/// the given output `alias`. Nested aggregates (e.g. `SUM(AVG(x))`,
/// which is illegal SQL in Postgres and most other systems) are
/// reported as a planner error rather than silently double-extracted.
///
/// `scope` carries the relations the arguments resolve against, so a
/// column no relation declares is rejected at plan time.
pub fn extract_aggregates(
    expr: &Expr,
    alias: &str,
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<Vec<AggregateExpr>> {
    let mut extractor = AggregateExtractor {
        functions,
        alias,
        scope,
        inside_aggregate: 0,
        out: Vec::new(),
        error: None,
    };
    let _ = expr.visit(&mut extractor);
    if let Some(e) = extractor.error {
        return Err(e);
    }
    Ok(extractor.out)
}

// ── Detector ────────────────────────────────────────────────────────

struct AggregateDetector<'a> {
    functions: &'a FunctionRegistry,
    found: bool,
}

impl Visitor for AggregateDetector<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if let Expr::Function(f) = expr
            && self.functions.is_aggregate(&function_name(f))
            // A function with an OVER clause is a window function, not an
            // aggregate. Window functions are handled by the window planner
            // and must not trigger the GROUP BY / aggregate plan path.
            && f.over.is_none()
        {
            self.found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

// ── Extractor ───────────────────────────────────────────────────────

struct AggregateExtractor<'a> {
    functions: &'a FunctionRegistry,
    alias: &'a str,
    scope: &'a TableScope,
    /// Depth counter: >0 means we're currently inside the argument
    /// subtree of an already-extracted aggregate. A second aggregate
    /// found in that subtree is an illegal nested aggregate.
    inside_aggregate: u32,
    out: Vec<AggregateExpr>,
    /// Deferred error — `Visitor::Break` is `()` so we can't carry the
    /// error through the control flow directly without boxing. Storing
    /// it and short-circuiting the traversal on next pre-visit gives
    /// the same observable behavior.
    error: Option<SqlError>,
}

impl Visitor for AggregateExtractor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if self.error.is_some() {
            return ControlFlow::Break(());
        }
        if let Expr::Function(f) = expr
            && self.functions.is_aggregate(&function_name(f))
            // Skip window function calls — those with OVER are handled by the
            // window planner, not the aggregate planner.
            && f.over.is_none()
        {
            if self.inside_aggregate > 0 {
                self.error = Some(SqlError::Unsupported {
                    detail: format!(
                        "nested aggregate functions are not allowed: {}(...{}...)",
                        function_name(f),
                        function_name(f),
                    ),
                });
                return ControlFlow::Break(());
            }
            let (args, distinct) = match function_args_and_distinct(f, self.scope) {
                Ok(parsed) => parsed,
                Err(e) => {
                    self.error = Some(e);
                    return ControlFlow::Break(());
                }
            };
            let filter = match aggregate_filter(f, self.scope) {
                Ok(filter) => filter,
                Err(e) => {
                    self.error = Some(e);
                    return ControlFlow::Break(());
                }
            };
            let mut arg_text = computed_arg_text(f, &args);
            let (args, filter_text) = match filter {
                Some((predicate, text)) => {
                    // The folded argument is a CASE; the key keeps naming
                    // the argument as written.
                    if arg_text.is_none() {
                        arg_text = Some(match args.first() {
                            Some(SqlExpr::Column { name, .. }) => name.clone(),
                            _ => "*".to_string(),
                        });
                    }
                    (filtered_args(args, predicate), Some(text))
                }
                None => (args, None),
            };
            self.out.push(AggregateExpr {
                function: function_name(f),
                args,
                alias: self.alias.into(),
                distinct,
                grouping_col_index: None,
                filter_text,
                arg_text,
            });
            self.inside_aggregate += 1;
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if let Expr::Function(f) = expr
            && self.functions.is_aggregate(&function_name(f))
            && f.over.is_none()
            && self.inside_aggregate > 0
        {
            self.inside_aggregate -= 1;
        }
        ControlFlow::Continue(())
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

/// Return the function name, or a qualified stub that will never match any
/// registered function if the name is schema-qualified.
///
/// Detection (`contains_aggregate`) uses this to look up the function in the
/// registry — a qualified name won't be found, so the aggregate check is a
/// safe no-op. Extraction (`extract_aggregates`) will not reach this path
/// for schema-qualified names because `convert_expr` rejects them first.
fn function_name(f: &ast::Function) -> String {
    if f.name.0.len() > 1 {
        // Return a sentinel that cannot match any registry entry.
        // The actual rejection happens in convert_expr / convert_function_depth.
        let qualified: String = f
            .name
            .0
            .iter()
            .map(|p| match p {
                ast::ObjectNamePart::Identifier(ident) => ident.value.clone(),
                _ => String::new(),
            })
            .collect::<Vec<_>>()
            .join(".");
        return format!("__schema_qualified__{qualified}");
    }
    f.name
        .0
        .iter()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Convert an aggregate call's argument list against `scope`.
///
/// An argument the converter rejects is an error, never a dropped argument:
/// discarding one silently changes the aggregate's arity and hides an unknown
/// column behind a query that reports success.
fn function_args_and_distinct(
    f: &ast::Function,
    scope: &TableScope,
) -> Result<(Vec<SqlExpr>, bool)> {
    let ast::FunctionArguments::List(args) = &f.args else {
        return Ok((Vec::new(), false));
    };
    let mut parsed = Vec::with_capacity(args.args.len());
    for a in &args.args {
        match a {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                parsed.push(convert_expr(e, &ColumnScope::Relations(scope))?);
            }
            // `COUNT(*)` carries no column to resolve.
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                parsed.push(SqlExpr::Wildcard);
            }
            _ => {}
        }
    }
    let distinct = matches!(
        args.duplicate_treatment,
        Some(ast::DuplicateTreatment::Distinct)
    );
    Ok((parsed, distinct))
}

/// Convert an aggregate call's `FILTER (WHERE ...)` clause against `scope`:
/// the predicate and its lowercase source text.
///
/// An aggregate inside the predicate is rejected by the extractor's nested
/// aggregate check, because the visitor descends into the clause while the
/// enclosing aggregate is open.
fn aggregate_filter(f: &ast::Function, scope: &TableScope) -> Result<Option<(SqlExpr, String)>> {
    let Some(predicate) = f.filter.as_deref() else {
        return Ok(None);
    };
    Ok(Some((
        convert_expr(predicate, &ColumnScope::Relations(scope))?,
        format!("{predicate}").to_lowercase(),
    )))
}

/// Fold a `FILTER` predicate into the aggregate's arguments.
///
/// Each argument becomes `CASE WHEN <predicate> THEN <arg> END`, NULL for
/// a row the predicate does not admit. Every aggregate skips a NULL input,
/// so the result equals PostgreSQL's FILTER semantics. `COUNT(*)` counts
/// rows, so its wildcard becomes the constant 1.
fn filtered_args(args: Vec<SqlExpr>, predicate: SqlExpr) -> Vec<SqlExpr> {
    let args = if args.is_empty() {
        vec![SqlExpr::Wildcard]
    } else {
        args
    };
    args.into_iter()
        .map(|arg| {
            let value = match arg {
                SqlExpr::Wildcard => SqlExpr::Literal(SqlValue::Int(1)),
                other => other,
            };
            SqlExpr::Case {
                operand: None,
                when_then: vec![(predicate.clone(), value)],
                else_expr: None,
            }
        })
        .collect()
}

/// Lowercase source text of the first argument when it is computed. A
/// column or wildcard argument is named by the column itself.
fn computed_arg_text(f: &ast::Function, args: &[SqlExpr]) -> Option<String> {
    match args.first() {
        None | Some(SqlExpr::Column { .. }) | Some(SqlExpr::Wildcard) => return None,
        Some(_) => {}
    }
    let ast::FunctionArguments::List(list) = &f.args else {
        return None;
    };
    list.args.iter().find_map(|a| match a {
        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
            Some(format!("{e}").to_lowercase())
        }
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;
    use crate::resolver::columns::ResolvedTable;
    use crate::types::{CollectionInfo, EngineType};

    fn first_select_projection(sql: &str) -> Vec<ast::SelectItem> {
        let stmts = parse_sql(sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            ast::Statement::Query(q) => match *q.body {
                ast::SetExpr::Select(s) => s.projection,
                _ => panic!(),
            },
            _ => panic!(),
        }
    }

    fn first_expr(sql: &str) -> ast::Expr {
        match first_select_projection(sql).into_iter().next().unwrap() {
            ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => panic!(),
        }
    }

    fn functions() -> FunctionRegistry {
        FunctionRegistry::new()
    }

    /// A one-relation scope that accepts any column name.
    fn open_scope() -> TableScope {
        let info = CollectionInfo {
            name: "t".into(),
            engine: EngineType::DocumentSchemaless,
            columns: Vec::new(),
            primary_key: None,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
            text_indexes: None,
        };
        TableScope::single(ResolvedTable {
            name: info.name.clone(),
            alias: None,
            info,
        })
        .expect("single-relation scope")
    }

    // ── detection ──

    #[test]
    fn detect_plain_aggregate() {
        assert!(contains_aggregate(
            &first_expr("SELECT SUM(x) FROM t"),
            &functions()
        ));
    }

    #[test]
    fn detect_aggregate_inside_case() {
        assert!(contains_aggregate(
            &first_expr("SELECT CASE WHEN x > 0 THEN SUM(y) ELSE 0 END FROM t"),
            &functions(),
        ));
    }

    #[test]
    fn detect_aggregate_inside_cast() {
        assert!(contains_aggregate(
            &first_expr("SELECT CAST(SUM(x) AS TEXT) FROM t"),
            &functions(),
        ));
    }

    #[test]
    fn detect_aggregate_inside_unary_op() {
        assert!(contains_aggregate(
            &first_expr("SELECT -SUM(x) FROM t"),
            &functions(),
        ));
    }

    #[test]
    fn detect_aggregate_inside_coalesce() {
        assert!(contains_aggregate(
            &first_expr("SELECT COALESCE(SUM(x), 0) FROM t"),
            &functions(),
        ));
    }

    #[test]
    fn detect_aggregate_inside_between() {
        assert!(contains_aggregate(
            &first_expr("SELECT SUM(x) BETWEEN 1 AND 10 FROM t"),
            &functions(),
        ));
    }

    #[test]
    fn detect_aggregate_inside_in_list() {
        assert!(contains_aggregate(
            &first_expr("SELECT SUM(x) IN (1, 2, 3) FROM t"),
            &functions(),
        ));
    }

    #[test]
    fn no_aggregate_in_plain_select() {
        assert!(!contains_aggregate(
            &first_expr("SELECT x FROM t"),
            &functions()
        ));
        assert!(!contains_aggregate(
            &first_expr("SELECT x + 1 FROM t"),
            &functions()
        ));
        assert!(!contains_aggregate(
            &first_expr("SELECT upper(name) FROM t"),
            &functions(),
        ));
    }

    // ── extraction ──

    #[test]
    fn extract_plain_aggregate() {
        let aggs = extract_aggregates(
            &first_expr("SELECT SUM(x) FROM t"),
            "total",
            &functions(),
            &open_scope(),
        )
        .unwrap();
        assert_eq!(aggs.len(), 1);
        assert_eq!(aggs[0].function, "sum");
        assert_eq!(aggs[0].alias, "total");
    }

    #[test]
    fn extract_aggregate_inside_cast() {
        let aggs = extract_aggregates(
            &first_expr("SELECT CAST(SUM(x) AS TEXT) AS n FROM t"),
            "n",
            &functions(),
            &open_scope(),
        )
        .unwrap();
        assert_eq!(aggs.len(), 1);
        assert_eq!(aggs[0].function, "sum");
    }

    #[test]
    fn extract_aggregate_inside_case() {
        let aggs = extract_aggregates(
            &first_expr("SELECT CASE WHEN x > 0 THEN SUM(y) ELSE 0 END FROM t"),
            "r",
            &functions(),
            &open_scope(),
        )
        .unwrap();
        assert_eq!(aggs.len(), 1);
        assert_eq!(aggs[0].function, "sum");
    }

    #[test]
    fn extract_aggregate_inside_coalesce() {
        let aggs = extract_aggregates(
            &first_expr("SELECT COALESCE(SUM(x), 0) FROM t"),
            "r",
            &functions(),
            &open_scope(),
        )
        .unwrap();
        assert_eq!(aggs.len(), 1);
        assert_eq!(aggs[0].function, "sum");
    }

    #[test]
    fn extract_two_aggregates_under_one_alias() {
        let aggs = extract_aggregates(
            &first_expr("SELECT SUM(x) + COUNT(y) AS total FROM t"),
            "total",
            &functions(),
            &open_scope(),
        )
        .unwrap();
        assert_eq!(aggs.len(), 2);
        let names: Vec<&str> = aggs.iter().map(|a| a.function.as_str()).collect();
        assert!(names.contains(&"sum"));
        assert!(names.contains(&"count"));
    }

    #[test]
    fn nested_aggregate_directly_inside_aggregate_rejected() {
        let err = extract_aggregates(
            &first_expr("SELECT SUM(AVG(x)) FROM t"),
            "r",
            &functions(),
            &open_scope(),
        )
        .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.to_lowercase().contains("nested aggregate"),
            "error must identify the nested-aggregate class: {msg}"
        );
    }

    /// Nested aggregate buried under an intermediate non-aggregate
    /// (`AVG(x)` wrapped in `CAST(...)` inside `SUM(...)`) must also
    /// be caught — the depth counter tracks the ancestor aggregate,
    /// not the direct parent.
    #[test]
    fn nested_aggregate_through_cast_rejected() {
        let err = extract_aggregates(
            &first_expr("SELECT SUM(CAST(AVG(x) AS BIGINT)) FROM t"),
            "r",
            &functions(),
            &open_scope(),
        )
        .unwrap_err();
        assert!(
            format!("{err:?}")
                .to_lowercase()
                .contains("nested aggregate"),
            "got: {err:?}"
        );
    }

    /// Two top-level aggregates separated by a non-aggregate binary
    /// operator are siblings, not nested — both must extract cleanly.
    #[test]
    fn sibling_aggregates_not_treated_as_nested() {
        let aggs = extract_aggregates(
            &first_expr("SELECT CAST(SUM(x) AS TEXT) || CAST(COUNT(y) AS TEXT) FROM t"),
            "r",
            &functions(),
            &open_scope(),
        )
        .unwrap();
        assert_eq!(aggs.len(), 2);
    }

    #[test]
    fn extract_distinct_preserved() {
        let aggs = extract_aggregates(
            &first_expr("SELECT COUNT(DISTINCT x) FROM t"),
            "c",
            &functions(),
            &open_scope(),
        )
        .unwrap();
        assert_eq!(aggs.len(), 1);
        assert!(aggs[0].distinct);
    }
}
