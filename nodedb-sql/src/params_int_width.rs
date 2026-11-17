// SPDX-License-Identifier: Apache-2.0

//! The integer type a bound integer parameter keeps.
//!
//! A bound integer reaches the resolver as a number literal, which takes
//! the literal rule: `integer` when it fits, else `bigint`. A parameter has
//! a type of its own instead: its declared `smallint`, `integer` or
//! `bigint`, or `bigint` for an integer bound with no declared type, as a
//! native value is. That type decides the result width only at a width
//! position:
//!
//! - an operand of integer arithmetic or of unary minus;
//! - an argument of `abs` or `mod`;
//! - a SELECT or RETURNING item.
//!
//! There the parameter binds as a cast of its number to its type, so
//! `$1 + 1` with a `bigint` `$1` of 2147483647 is the `bigint` 2147483648.
//! Every other position keeps the plain literal: a comparison or an
//! assignment reads no width, and a point lookup or an index range reads
//! the literal. The pgwire AST binder and the native text splice apply the
//! same rule set.

use core::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, CastKind, DataType, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    Select, SelectItem, Statement, UnaryOperator, Value, ValueWithSpan, VisitMut, VisitorMut,
};
use sqlparser::tokenizer::Location;

use crate::params::ParamValue;

/// The width positions directly inside `expr`: each operand of integer
/// arithmetic and of unary minus, and each argument of `abs` and `mod`.
///
/// This is the one rule set both binders apply: the AST binder of a pgwire
/// statement and the text splice of a native or DSL statement.
fn width_operands(expr: &mut Expr) -> Vec<&mut Expr> {
    match expr {
        Expr::BinaryOp { left, op, right } if is_arithmetic(op) => {
            vec![left.as_mut(), right.as_mut()]
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => vec![expr.as_mut()],
        Expr::Function(function) if is_width_function(function) => match &mut function.args {
            FunctionArguments::List(list) => list
                .args
                .iter_mut()
                .filter_map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
                    _ => None,
                })
                .collect(),
            FunctionArguments::None | FunctionArguments::Subquery(_) => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// The width positions of a SELECT or RETURNING list: each item's
/// expression.
fn item_operands(items: &mut [SelectItem]) -> Vec<&mut Expr> {
    items
        .iter_mut()
        .filter_map(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => Some(expr),
            SelectItem::ExprWithAliases { .. }
            | SelectItem::QualifiedWildcard(..)
            | SelectItem::Wildcard(_) => None,
        })
        .collect()
}

/// Rewrite each placeholder at a width position of `expr` that is bound to
/// an integer parameter to the cast to its type.
pub(crate) fn type_arithmetic_operands(expr: &mut Expr, params: &[ParamValue]) {
    for operand in width_operands(expr) {
        type_int_placeholder(operand, params);
    }
}

/// Rewrite each SELECT or RETURNING item that is a placeholder bound to an
/// integer parameter to the cast to its type.
pub(crate) fn type_output_items(items: &mut [SelectItem], params: &[ParamValue]) {
    for operand in item_operands(items) {
        type_int_placeholder(operand, params);
    }
}

/// The RETURNING items of a DML `statement`.
pub(crate) fn returning_items(statement: &mut Statement) -> Option<&mut Vec<SelectItem>> {
    match statement {
        Statement::Insert(insert) => insert.returning.as_mut(),
        Statement::Update(update) => update.returning.as_mut(),
        Statement::Delete(delete) => delete.returning.as_mut(),
        _ => None,
    }
}

/// The source positions of the placeholders of `sql` that sit at a width
/// position, by the rule set the AST binder applies. Text sqlparser does
/// not parse, a DSL grammar's, has none.
pub(crate) fn width_placeholder_positions(sql: &str) -> Vec<Location> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let Ok(mut statements) = sqlparser::parser::Parser::parse_sql(&dialect, sql) else {
        return Vec::new();
    };
    let mut finder = WidthPositions { found: Vec::new() };
    for statement in &mut statements {
        let _ = statement.visit(&mut finder);
    }
    finder.found
}

/// Collects the source position of each placeholder at a width position.
struct WidthPositions {
    found: Vec<Location>,
}

impl WidthPositions {
    fn record(&mut self, operands: Vec<&mut Expr>) {
        for operand in operands {
            if let Some(at) = placeholder_location(operand) {
                self.found.push(at);
            }
        }
    }
}

impl VisitorMut for WidthPositions {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let operands = width_operands(expr);
        self.record(operands);
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
        let operands = item_operands(&mut select.projection);
        self.record(operands);
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        if let Some(items) = returning_items(statement) {
            let operands = item_operands(items);
            self.record(operands);
        }
        ControlFlow::Continue(())
    }
}

/// The source position of `expr` when it is a placeholder, parenthesized
/// or not.
fn placeholder_location(expr: &Expr) -> Option<Location> {
    match expr {
        Expr::Nested(inner) => placeholder_location(inner),
        Expr::Value(value) if matches!(value.value, Value::Placeholder(_)) => {
            Some(value.span.start)
        }
        _ => None,
    }
}

/// The SQL text an integer `param` takes at a width position: the cast of
/// its number to its type. `None` for a parameter that is not an integer.
pub(crate) fn int_param_cast_text(param: &ParamValue) -> Option<String> {
    let (value, data_type) = int_param_type(param)?;
    Some(format!("CAST({value} AS {data_type})"))
}

/// `expr`, a placeholder bound to an integer parameter, as the cast of its
/// number to the parameter's type. A parenthesized placeholder is typed
/// inside its parentheses. Any other expression is unchanged.
fn type_int_placeholder(expr: &mut Expr, params: &[ParamValue]) {
    match expr {
        Expr::Nested(inner) => type_int_placeholder(inner, params),
        Expr::Value(value) => {
            let Value::Placeholder(placeholder) = &value.value else {
                return;
            };
            let Some((bound, data_type)) =
                placeholder_param(placeholder, params).and_then(int_param_type)
            else {
                return;
            };
            let span = value.span;
            *expr = Expr::Cast {
                kind: CastKind::Cast,
                expr: Box::new(Expr::Value(ValueWithSpan { value: bound, span })),
                data_type,
                format: None,
            };
        }
        _ => {}
    }
}

/// The parameter `placeholder` (`$N`) names.
fn placeholder_param<'a>(placeholder: &str, params: &'a [ParamValue]) -> Option<&'a ParamValue> {
    let index: usize = placeholder.strip_prefix('$')?.parse().ok()?;
    params.get(index.checked_sub(1)?)
}

/// The bound value and SQL type of an integer `param`: its number, or NULL
/// for a typed NULL. `None` for a parameter that is not an integer.
fn int_param_type(param: &ParamValue) -> Option<(Value, DataType)> {
    use nodedb_query::cast::IntWidth;
    let number = |n: String| Value::Number(n, false);
    Some(match param {
        ParamValue::Int64(n) => (number(n.to_string()), DataType::BigInt(None)),
        ParamValue::Int32(n) => (number(n.to_string()), DataType::Integer(None)),
        ParamValue::Int16(n) => (number(n.to_string()), DataType::SmallInt(None)),
        ParamValue::IntNull(width) => (
            Value::Null,
            match width {
                IntWidth::Int8 => DataType::BigInt(None),
                IntWidth::Int4 => DataType::Integer(None),
                IntWidth::Int2 => DataType::SmallInt(None),
            },
        ),
        _ => return None,
    })
}

fn is_arithmetic(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
    )
}

/// Whether `function` is `abs` or `mod`, whose integer result keeps its
/// arguments' width.
fn is_width_function(function: &sqlparser::ast::Function) -> bool {
    function
        .name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .is_some_and(|ident| {
            ident.value.eq_ignore_ascii_case("abs") || ident.value.eq_ignore_ascii_case("mod")
        })
}

#[cfg(test)]
mod tests {
    use crate::params::{ParamValue, bind_params};
    use crate::parser::statement::parse_sql;

    fn bound(sql: &str, params: &[ParamValue]) -> String {
        let mut statements = parse_sql(sql).expect("parses");
        bind_params(&mut statements[0], params);
        statements[0].to_string()
    }

    /// A typed NULL binds as the NULL of its type at a width position.
    #[test]
    fn a_typed_null_binds_as_a_null_of_its_type() {
        use nodedb_query::cast::IntWidth;
        assert_eq!(
            bound("SELECT $1 + 1 AS n", &[ParamValue::IntNull(IntWidth::Int4)]),
            "SELECT CAST(NULL AS INTEGER) + 1 AS n"
        );
        assert_eq!(
            bound(
                "SELECT * FROM t WHERE id = $1",
                &[ParamValue::IntNull(IntWidth::Int8)]
            ),
            "SELECT * FROM t WHERE id = NULL"
        );
    }

    #[test]
    fn an_arithmetic_operand_keeps_its_parameter_type() {
        assert_eq!(
            bound("SELECT $1 + 1 AS n", &[ParamValue::Int64(5)]),
            "SELECT CAST(5 AS BIGINT) + 1 AS n"
        );
        assert_eq!(
            bound("SELECT 1 * $1 AS n", &[ParamValue::Int32(5)]),
            "SELECT 1 * CAST(5 AS INTEGER) AS n"
        );
        assert_eq!(
            bound("SELECT ($1) - 1 AS n", &[ParamValue::Int16(5)]),
            "SELECT (CAST(5 AS SMALLINT)) - 1 AS n"
        );
        assert_eq!(
            bound("SELECT -$1 AS n", &[ParamValue::Int64(5)]),
            "SELECT -CAST(5 AS BIGINT) AS n"
        );
    }

    #[test]
    fn abs_and_mod_arguments_keep_their_parameter_type() {
        assert_eq!(
            bound("SELECT abs($1) AS n", &[ParamValue::Int64(-5)]),
            "SELECT abs(CAST(-5 AS BIGINT)) AS n"
        );
        assert_eq!(
            bound("SELECT mod($1, 3) AS n", &[ParamValue::Int16(7)]),
            "SELECT mod(CAST(7 AS SMALLINT), 3) AS n"
        );
        assert_eq!(
            bound("SELECT upper($1) AS n", &[ParamValue::Int64(5)]),
            "SELECT upper(5) AS n"
        );
    }

    #[test]
    fn width_positions_follow_the_same_rules() {
        let columns = |sql: &str| -> Vec<u64> {
            let mut columns: Vec<u64> = super::width_placeholder_positions(sql)
                .iter()
                .map(|at| at.column)
                .collect();
            columns.sort_unstable();
            columns
        };
        // `SELECT $1 + 1 FROM t WHERE id = $2`: `$1` at column 8 is an
        // operand, `$2` a comparison.
        assert_eq!(columns("SELECT $1 + 1 FROM t WHERE id = $2"), [8]);
        // `abs(` ends at column 11 and `), ` at 16, so `$2` is at 17.
        assert_eq!(columns("SELECT abs($1), $2 FROM t"), [12, 17]);
        assert_eq!(
            columns("UPDATE t SET n = $1 WHERE id = 'a' RETURNING -$2"),
            [47]
        );
        assert!(columns("GRAPH TRAVERSE FROM $1").is_empty());
    }

    #[test]
    fn an_output_item_keeps_its_parameter_type() {
        assert_eq!(
            bound("SELECT $1 AS n", &[ParamValue::Int64(5)]),
            "SELECT CAST(5 AS BIGINT) AS n"
        );
        assert!(
            bound(
                "UPDATE t SET n = 1 WHERE id = 'a' RETURNING $1 AS tag",
                &[ParamValue::Int32(7)]
            )
            .contains("RETURNING CAST(7 AS INTEGER) AS tag")
        );
    }

    /// A comparison, an assignment and a non-integer parameter keep the
    /// plain literal.
    #[test]
    fn other_positions_keep_the_literal() {
        assert_eq!(
            bound("SELECT * FROM t WHERE id = $1", &[ParamValue::Int64(5)]),
            "SELECT * FROM t WHERE id = 5"
        );
        assert!(
            bound(
                "UPDATE t SET n = $1 WHERE id = 'a'",
                &[ParamValue::Int32(5)]
            )
            .contains("SET n = 5")
        );
        assert_eq!(
            bound("SELECT $1 + 1 AS n", &[ParamValue::Float64(1.5)]),
            "SELECT 1.5e0 + 1 AS n"
        );
    }
}
