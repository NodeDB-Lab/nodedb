// SPDX-License-Identifier: Apache-2.0

//! Binding aggregate calls in HAVING, post-aggregate ORDER BY, and computed
//! grouped SELECT items.
//!
//! These clauses are evaluated against finalized group rows. An aggregate one
//! mentions must be computed and addressed by the column it lands under. Left
//! as a literal `sum(...)` call, it reaches the executor as a scalar function
//! over a row that has no such column. The predicate matches nothing, or the
//! value resolves to nothing.
//!
//! The clauses differ in *which* name they need, because they run on
//! opposite sides of the rename to user aliases:
//!
//! - HAVING filters before the rename, so it addresses the canonical key
//!   (`sum(amount)`).
//! - ORDER BY and a computed SELECT item run after the rename, so they
//!   address the output name (`total`, when the projection said
//!   `SUM(amount) AS total`).

use std::ops::ControlFlow;

use sqlparser::ast::{self, VisitMut, VisitorMut};

use crate::aggregate_walk::extract_aggregates;
use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionCategory, FunctionRegistry};
use crate::parser::normalize::normalize_ident;
use crate::planner::agg_naming::aggregate_output_key;
use crate::resolver::columns::TableScope;
use crate::types::AggregateExpr;

/// Which name an aggregate call is rewritten to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindName {
    /// The canonical key a group row carries before user aliases are applied.
    Canonical,
    /// The output column name the client sees, after that rename.
    Output,
}

/// Rewrite every aggregate call in `expr` to a reference to its computed
/// column, registering aggregates the projection did not already request.
///
/// `scope` carries the input relations each aggregate argument resolves
/// against.
pub fn bind_aggregate_calls(
    expr: &ast::Expr,
    projection: &[ast::SelectItem],
    aggregates: &mut Vec<AggregateExpr>,
    functions: &FunctionRegistry,
    name: BindName,
    scope: &TableScope,
) -> Result<ast::Expr> {
    let resolved = resolve_select_aliases(expr, projection, functions);
    bind(&resolved, aggregates, functions, name, scope)
}

/// Substitute SELECT-list output aliases referenced by the expression, at any
/// depth: inside a scalar function, a CASE, a CAST, an IN list, and every
/// other expression position.
///
/// `SELECT SUM(amount) AS total ... HAVING total > 0` names an output column
/// that does not exist yet when the clause runs. Replacing it with the
/// underlying expression lets the aggregate binding below address the column
/// that does exist.
fn resolve_select_aliases(
    expr: &ast::Expr,
    projection: &[ast::SelectItem],
    functions: &FunctionRegistry,
) -> ast::Expr {
    let mut resolved = expr.clone();
    let mut resolver = AliasResolver {
        projection,
        functions,
        query_depth: 0,
        aggregate_depth: 0,
    };
    let _ = resolved.visit(&mut resolver);
    resolved
}

/// Replaces each identifier that names a SELECT-list alias with the aliased
/// expression.
///
/// A subquery resolves its own names, and an aggregate argument names an
/// input column. Neither takes an alias.
struct AliasResolver<'a> {
    projection: &'a [ast::SelectItem],
    functions: &'a FunctionRegistry,
    /// How many subqueries enclose the expression being visited.
    query_depth: usize,
    /// How many aggregate calls enclose the expression being visited.
    aggregate_depth: usize,
}

impl AliasResolver<'_> {
    fn aliased(&self, ident: &ast::Ident) -> Option<ast::Expr> {
        let needle = normalize_ident(ident);
        self.projection.iter().find_map(|item| match item {
            ast::SelectItem::ExprWithAlias { expr, alias } if normalize_ident(alias) == needle => {
                Some(expr.clone())
            }
            _ => None,
        })
    }

    fn is_aggregate(&self, expr: &ast::Expr) -> bool {
        matches!(expr, ast::Expr::Function(func) if is_aggregate_call(func, self.functions))
    }
}

impl VisitorMut for AliasResolver<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &mut ast::Query) -> ControlFlow<Self::Break> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut ast::Query) -> ControlFlow<Self::Break> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        if self.is_aggregate(expr) {
            self.aggregate_depth += 1;
        }
        ControlFlow::Continue(())
    }

    /// The walk does not revisit a post-visit replacement, so an alias whose
    /// expression reads a column of the same name (`SUM(x) AS x`) ends here.
    fn post_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        if self.is_aggregate(expr) {
            self.aggregate_depth -= 1;
        } else if self.query_depth == 0
            && self.aggregate_depth == 0
            && let ast::Expr::Identifier(ident) = &*expr
            && let Some(aliased) = self.aliased(ident)
        {
            *expr = aliased;
        }
        ControlFlow::Continue(())
    }
}

/// Rewrite every aggregate call in `expr` at any depth: inside a scalar
/// function (`ROUND(AVG(v), 1)`), a CASE, a CAST, an IN list, and every other
/// expression position. An aggregate inside a subquery belongs to that
/// subquery, so it stays as written.
fn bind(
    expr: &ast::Expr,
    aggregates: &mut Vec<AggregateExpr>,
    functions: &FunctionRegistry,
    name: BindName,
    scope: &TableScope,
) -> Result<ast::Expr> {
    let mut bound = expr.clone();
    let mut binder = AggregateBinder {
        aggregates,
        functions,
        name,
        scope,
        query_depth: 0,
        error: None,
    };
    let _ = bound.visit(&mut binder);
    match binder.error {
        Some(error) => Err(error),
        None => Ok(bound),
    }
}

/// Replaces each aggregate call outside a subquery with a reference to its
/// computed column. The first registration error stops the walk.
struct AggregateBinder<'a> {
    aggregates: &'a mut Vec<AggregateExpr>,
    functions: &'a FunctionRegistry,
    name: BindName,
    scope: &'a TableScope,
    /// How many subqueries enclose the expression being visited.
    query_depth: usize,
    error: Option<SqlError>,
}

impl VisitorMut for AggregateBinder<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &mut ast::Query) -> ControlFlow<Self::Break> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut ast::Query) -> ControlFlow<Self::Break> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    /// The walk visits the children of the replacement, an identifier, so it
    /// does not descend into the aggregate's arguments.
    fn pre_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        if self.query_depth > 0 {
            return ControlFlow::Continue(());
        }
        let ast::Expr::Function(func) = &*expr else {
            return ControlFlow::Continue(());
        };
        if !is_aggregate_call(func, self.functions) {
            return ControlFlow::Continue(());
        }
        match register_aggregate(expr, self.aggregates, self.functions, self.name, self.scope) {
            Ok(column) => {
                *expr = ast::Expr::Identifier(ast::Ident::new(column));
                ControlFlow::Continue(())
            }
            Err(error) => {
                self.error = Some(error);
                ControlFlow::Break(())
            }
        }
    }
}

pub(super) fn is_aggregate_call(func: &ast::Function, functions: &FunctionRegistry) -> bool {
    let name = func
        .name
        .0
        .iter()
        .map(|part| match part {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join(".");
    matches!(
        functions.lookup(&name).map(|m| m.category),
        Some(FunctionCategory::Aggregate)
    )
}

/// Ensure the aggregate `expr` is in the computed list and return the column
/// name it should be addressed by.
fn register_aggregate(
    expr: &ast::Expr,
    aggregates: &mut Vec<AggregateExpr>,
    functions: &FunctionRegistry,
    name: BindName,
    scope: &TableScope,
) -> Result<String> {
    // The alias is replaced below for a newly registered aggregate, so the
    // placeholder is never observable.
    let mut extracted = extract_aggregates(expr, "", functions, scope)?;
    let Some(mut agg) = extracted.pop() else {
        return Err(SqlError::Unsupported {
            detail: format!("aggregate `{expr}` could not be extracted"),
        });
    };
    if !extracted.is_empty() {
        return Err(SqlError::Unsupported {
            detail: format!("nested aggregates are not supported: `{expr}`"),
        });
    }

    let key = aggregate_output_key(&agg);

    // An aggregate the projection already computes needs no second entry. Its
    // output name is its alias, which is what a post-rename caller must use.
    if let Some(existing) = aggregates
        .iter()
        .find(|existing| aggregate_output_key(existing) == key)
    {
        return Ok(match name {
            BindName::Canonical => key,
            BindName::Output => existing.alias.clone(),
        });
    }

    // Carry the canonical key as the alias so no user-facing rename is
    // attached: an aggregate introduced only by HAVING or ORDER BY is an input
    // to filtering or sorting, not an output column the client asked for. Both
    // names therefore coincide.
    agg.alias = key.clone();
    aggregates.push(agg);
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;

    /// The HAVING clause of `sql` with its SELECT-list aliases substituted.
    fn resolved_having(sql: &str) -> String {
        let statements = parse_sql(sql).expect("parses");
        let ast::Statement::Query(query) = &statements[0] else {
            panic!("not a query: {sql}");
        };
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("not a SELECT: {sql}");
        };
        let having = select.having.as_ref().expect("a HAVING clause");
        resolve_select_aliases(having, &select.projection, &FunctionRegistry::new()).to_string()
    }

    #[test]
    fn an_alias_resolves_at_any_expression_depth() {
        for predicate in [
            "total > 0",
            "(total) > 0",
            "-total > 0",
            "ROUND(total, 1) > 0",
            "CASE WHEN total > 0 THEN 1 ELSE total END = 1",
            "CAST(total AS BIGINT) > 0",
            "total IN (1, 2)",
            "total BETWEEN 1 AND 2",
        ] {
            let resolved = resolved_having(&format!(
                "SELECT SUM(a) AS total FROM t GROUP BY k HAVING {predicate}"
            ));
            assert!(resolved.contains("SUM(a)"), "{predicate}: {resolved}");
            assert!(!resolved.contains("total"), "{predicate}: {resolved}");
        }
    }

    /// An alias over a column of its own name does not resolve twice.
    #[test]
    fn an_alias_expression_is_not_walked_again() {
        assert_eq!(
            resolved_having("SELECT SUM(x) AS x FROM t GROUP BY k HAVING ROUND(x) > 0"),
            "ROUND(SUM(x)) > 0"
        );
    }

    #[test]
    fn an_aggregate_argument_keeps_its_column() {
        assert_eq!(
            resolved_having("SELECT k AS b, SUM(b) AS t FROM x GROUP BY k HAVING SUM(b) > 0"),
            "SUM(b) > 0"
        );
    }

    #[test]
    fn a_subquery_keeps_its_own_names() {
        assert_eq!(
            resolved_having(
                "SELECT SUM(a) AS total FROM t GROUP BY k HAVING total IN (SELECT total FROM y)"
            ),
            "SUM(a) IN (SELECT total FROM y)"
        );
    }

    #[test]
    fn a_name_no_alias_holds_stays() {
        assert_eq!(
            resolved_having("SELECT SUM(a) AS total FROM t GROUP BY k HAVING k > 0"),
            "k > 0"
        );
    }
}
