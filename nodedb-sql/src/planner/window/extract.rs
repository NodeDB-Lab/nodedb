// SPDX-License-Identifier: Apache-2.0

//! Window function extraction from a SELECT's projection — the module entry
//! point.
//!
//! Resolves `OVER w` / `OVER (w ORDER BY ...)` references against the query's
//! `WINDOW` clause, validates each `<func>() OVER (...)` against the function
//! registry (PostgreSQL allows aggregates as windows), and produces the
//! `WindowSpec`s the Data-Plane evaluator consumes. Names that are neither
//! registered window functions nor aggregates are rejected here so the
//! evaluator never receives an unrecognised verb.

use std::collections::HashMap;

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionCategory, FunctionRegistry};
use crate::parser::normalize::{SCHEMA_QUALIFIED_MSG, normalize_ident};
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::{SortKey, SqlExpr, WindowSpec};
use nodedb_query::{FrameBound, WindowFrame};

use super::frame::convert_window_frame;
use super::named::{collect_named_windows, flatten_window_spec, resolve_named_def};

/// Extract window function specifications from a SELECT's projection.
pub fn extract_window_functions(
    select: &ast::Select,
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<Vec<WindowSpec>> {
    let named = collect_named_windows(&select.named_window)?;
    let mut specs = Vec::new();
    for item in &select.projection {
        let (expr, alias) = match item {
            ast::SelectItem::UnnamedExpr(e) => (
                e,
                crate::planner::ast_helpers::unaliased_projection_alias(e),
            ),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, normalize_ident(alias)),
            _ => continue,
        };
        if let ast::Expr::Function(func) = expr
            && func.over.is_some()
        {
            specs.push(convert_window_spec(func, &alias, functions, &named, scope)?);
        }
    }
    Ok(specs)
}

fn convert_window_spec(
    func: &ast::Function,
    alias: &str,
    functions: &FunctionRegistry,
    named: &HashMap<String, &ast::NamedWindowExpr>,
    scope: &TableScope,
) -> Result<WindowSpec> {
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
            detail: format!(
                "schema-qualified window function name '{qualified}': {SCHEMA_QUALIFIED_MSG}"
            ),
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

    // Reject unknown names at plan time. PostgreSQL permits aggregates as
    // windows, so accept either Window or Aggregate categories.
    match functions.lookup(&name).map(|m| m.category) {
        Some(FunctionCategory::Window) | Some(FunctionCategory::Aggregate) => {}
        Some(FunctionCategory::Scalar) => {
            return Err(SqlError::InvalidFunction {
                detail: format!(
                    "function '{name}() OVER ()' does not exist as a window function \
                     (it is a scalar function)"
                ),
            });
        }
        None => {
            return Err(SqlError::InvalidFunction {
                detail: format!("function '{name}() OVER ()' does not exist"),
            });
        }
    }

    let args = convert_window_args(func, &name, scope)?;
    validate_constant_args(&name, &args)?;

    // Resolve the OVER target into a flattened partition/order/frame.
    let flat = match &func.over {
        Some(ast::WindowType::WindowSpec(spec)) => {
            Some(flatten_window_spec(spec, named, &mut Vec::new())?)
        }
        Some(ast::WindowType::NamedWindow(ident)) => {
            let n = normalize_ident(ident);
            let mut seen = vec![n.clone()];
            let base = resolve_named_def(&n, named, &mut seen)?;
            Some(flatten_window_spec(base, named, &mut seen)?)
        }
        None => None,
    };

    let (partition_by, order_by, frame) = match flat {
        Some(flat) => {
            let pb = flat
                .partition_by
                .iter()
                .map(|e| convert_expr(e, &ColumnScope::Relations(scope)))
                .collect::<Result<Vec<_>>>()?;
            let ob = flat
                .order_by
                .iter()
                .map(|o| {
                    Ok(SortKey {
                        expr: convert_expr(&o.expr, &ColumnScope::Relations(scope))?,
                        ascending: o.options.asc.unwrap_or(true),
                        nulls_first: o
                            .options
                            .nulls_first
                            .unwrap_or(!o.options.asc.unwrap_or(true)),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let frame = match &flat.frame {
                Some(f) => convert_window_frame(f, &ob)?,
                // PostgreSQL default: when ORDER BY is present, RANGE UNBOUNDED
                // PRECEDING TO CURRENT ROW; when no ORDER BY, the window covers
                // the whole partition (RANGE UNBOUNDED PRECEDING TO UNBOUNDED
                // FOLLOWING).
                None => {
                    if ob.is_empty() {
                        WindowFrame {
                            mode: "range".into(),
                            start: FrameBound::UnboundedPreceding,
                            end: FrameBound::UnboundedFollowing,
                        }
                    } else {
                        WindowFrame::default()
                    }
                }
            };
            (pb, ob, frame)
        }
        // Bare `OVER ()` with no spec — whole input is one window.
        None => (
            Vec::new(),
            Vec::new(),
            WindowFrame {
                mode: "range".into(),
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::UnboundedFollowing,
            },
        ),
    };

    Ok(WindowSpec {
        function: name,
        args,
        partition_by,
        order_by,
        alias: alias.into(),
        frame,
    })
}

/// Convert a window function's argument list.
///
/// Each argument is a full expression that the evaluator evaluates per row.
/// An argument the converter cannot represent is an error, never a dropped
/// argument: discarding one silently turns `SUM(price * qty) OVER (...)` into
/// a windowed column of NULLs that still reports success.
fn convert_window_args(
    func: &ast::Function,
    name: &str,
    scope: &TableScope,
) -> Result<Vec<SqlExpr>> {
    let ast::FunctionArguments::List(list) = &func.args else {
        return Ok(Vec::new());
    };

    let mut args = Vec::with_capacity(list.args.len());
    for arg in &list.args {
        match arg {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                args.push(convert_expr(e, &ColumnScope::Relations(scope))?)
            }
            // `COUNT(*) OVER (...)` — a wildcard carries no value to evaluate.
            // The evaluator counts frame rows when no argument is present.
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {}
            other => {
                return Err(SqlError::Unsupported {
                    detail: format!("argument form in '{name}() OVER (...)': {other:?}"),
                });
            }
        }
    }
    Ok(args)
}

/// Reject a non-constant in the argument positions PostgreSQL requires to be
/// constant: the `LAG`/`LEAD` offset and default, the `NTILE` bucket count,
/// and the `NTH_VALUE` position.
///
/// The evaluator resolves these once per partition rather than per row. Left
/// unvalidated, a non-constant there falls back to the position's default and
/// silently answers a different query than the one asked.
fn validate_constant_args(name: &str, args: &[SqlExpr]) -> Result<()> {
    let constant_positions: &[usize] = match name {
        "lag" | "lead" => &[1, 2],
        "ntile" => &[0],
        "nth_value" => &[1],
        _ => return Ok(()),
    };

    for &pos in constant_positions {
        if let Some(arg) = args.get(pos)
            && !matches!(arg, SqlExpr::Literal(_))
        {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "argument {} of '{name}() OVER (...)' must be a constant",
                    pos + 1
                ),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::registry::FunctionRegistry;
    use crate::parser::statement::parse_sql;
    use crate::resolver::columns::test_support::open_scope;

    fn select_of(sql: &str) -> Box<ast::Select> {
        match parse_sql(sql).unwrap().into_iter().next().unwrap() {
            ast::Statement::Query(q) => match *q.body {
                ast::SetExpr::Select(s) => s,
                _ => panic!("not a SELECT"),
            },
            _ => panic!("not a query"),
        }
    }

    #[test]
    fn named_window_referenced_by_multiple_functions() {
        let reg = FunctionRegistry::new();
        let select = select_of(
            "SELECT first_value(price) OVER w AS o, last_value(price) OVER w AS c, sum(volume) OVER w AS v
             FROM ticks
             WINDOW w AS (PARTITION BY bucket ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
        );
        let specs = extract_window_functions(&select, &reg, &open_scope("ticks")).unwrap();
        assert_eq!(specs.len(), 3);
        for s in &specs {
            assert_eq!(
                s.partition_by.len(),
                1,
                "partition by must be resolved from WINDOW clause"
            );
            assert_eq!(
                s.order_by.len(),
                1,
                "order by must be resolved from WINDOW clause"
            );
            assert_eq!(s.frame.mode, "rows");
            assert!(matches!(s.frame.start, FrameBound::UnboundedPreceding));
            assert!(matches!(s.frame.end, FrameBound::UnboundedFollowing));
        }
    }

    #[test]
    fn undefined_named_window_is_rejected() {
        let reg = FunctionRegistry::new();
        let select = select_of("SELECT row_number() OVER missing AS r FROM t");
        let err = extract_window_functions(&select, &reg, &open_scope("ticks")).unwrap_err();
        assert!(
            format!("{err}").contains("missing"),
            "error must name the missing window: {err}"
        );
    }

    #[test]
    fn window_definition_referencing_another_resolves() {
        let reg = FunctionRegistry::new();
        let select = select_of(
            "SELECT sum(x) OVER w2 AS s FROM t WINDOW w1 AS (PARTITION BY a), w2 AS (w1 ORDER BY ts)",
        );
        let specs = extract_window_functions(&select, &reg, &open_scope("ticks")).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(
            specs[0].partition_by.len(),
            1,
            "PARTITION BY inherited from w1"
        );
        assert_eq!(specs[0].order_by.len(), 1, "ORDER BY added by w2");
    }

    #[test]
    fn circular_named_window_is_rejected() {
        let reg = FunctionRegistry::new();
        let select = select_of("SELECT sum(x) OVER w1 AS s FROM t WINDOW w1 AS (w2), w2 AS (w1)");
        let err = extract_window_functions(&select, &reg, &open_scope("ticks")).unwrap_err();
        assert!(
            format!("{err}").to_lowercase().contains("circular"),
            "got: {err}"
        );
    }

    #[test]
    fn ohlcv_shape_base_window_plus_derived_ordered_window() {
        // Mirrors nodedb-docs/use-cases/fintech-trading.rdx OHLCV bars:
        // a base `w` (PARTITION BY only) used by max/min/sum, and a derived
        // `w_ord` (= w + ORDER BY + frame) used by first_value/last_value.
        let reg = FunctionRegistry::new();
        let select = select_of(
            "SELECT first_value(price) OVER w_ord AS o, max(price) OVER w AS h,
                    min(price) OVER w AS l, last_value(price) OVER w_ord AS c, sum(volume) OVER w AS v
             FROM ticks
             WINDOW w     AS (PARTITION BY time_bucket('1m', ts), symbol),
                    w_ord AS (w ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
        );
        let specs = extract_window_functions(&select, &reg, &open_scope("ticks")).unwrap();
        assert_eq!(specs.len(), 5);
        for s in &specs {
            assert_eq!(
                s.partition_by.len(),
                2,
                "{}: partition inherited from w",
                s.function
            );
        }
        // first_value / last_value carry the ORDER BY + explicit frame from w_ord.
        for f in ["first_value", "last_value"] {
            let s = specs.iter().find(|s| s.function == f).unwrap();
            assert_eq!(s.order_by.len(), 1, "{f}: order by from w_ord");
            assert_eq!(s.frame.mode, "rows", "{f}: frame from w_ord");
            assert!(matches!(s.frame.start, FrameBound::UnboundedPreceding));
            assert!(matches!(s.frame.end, FrameBound::UnboundedFollowing));
        }
        // max / min / sum use the base w: no order, whole-partition frame.
        for f in ["max", "min", "sum"] {
            let s = specs.iter().find(|s| s.function == f).unwrap();
            assert!(s.order_by.is_empty(), "{f}: no order by");
            assert_eq!(s.frame.mode, "range");
            assert!(matches!(s.frame.start, FrameBound::UnboundedPreceding));
            assert!(matches!(s.frame.end, FrameBound::UnboundedFollowing));
        }
    }

    #[test]
    fn inline_window_referencing_named_inherits_partition() {
        let reg = FunctionRegistry::new();
        let select = select_of(
            "SELECT sum(x) OVER (w ORDER BY ts) AS s FROM t WINDOW w AS (PARTITION BY a)",
        );
        let specs = extract_window_functions(&select, &reg, &open_scope("ticks")).unwrap();
        assert_eq!(specs[0].partition_by.len(), 1);
        assert_eq!(specs[0].order_by.len(), 1);
    }
}
