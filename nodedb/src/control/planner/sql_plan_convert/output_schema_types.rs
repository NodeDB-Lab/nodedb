// SPDX-License-Identifier: BUSL-1.1

//! Conservative result-type inference for the planner-authoritative
//! [`OutputSchema`](crate::control::server::response_shape::schema::OutputSchema).
//!
//! Bare columns already carry their real catalog type. This module types the
//! three remaining kinds of output column — computed SELECT expressions,
//! GROUP BY keys, and aggregate results — so the pgwire shaper can advertise a
//! correct RowDescription type OID (typed text-format encoding is already in
//! place).
//!
//! Correctness rule: a WRONG non-TEXT OID makes clients fail to parse the
//! text value as that type, whereas `Text` is always safe. Every function here
//! therefore returns `DdlColType::Text` for any case it cannot type with
//! certainty — under-typing is preferred over mis-typing.

use std::collections::HashMap;

use nodedb_sql::types::query::AggregateExpr;
use nodedb_sql::types_expr::{BinaryOp, SqlExpr, SqlValue, UnaryOp};

use crate::control::server::response_shape::types::DdlColType;

/// Resolves a column reference to its catalog type from `types`. Returns
/// `None` for any non-column expression, or for a column absent from the map.
///
/// A join's map is keyed on the qualified name, because two sides can carry
/// the same bare column with different types, so a table-qualified reference
/// tries `<table>.<name>` first. A single-collection map holds bare names
/// only, where that lookup misses and the bare name answers.
fn bare_column_type(expr: &SqlExpr, types: &HashMap<String, DdlColType>) -> Option<DdlColType> {
    match expr {
        SqlExpr::Column {
            table: Some(table),
            name,
        } => types
            .get(&format!("{table}.{name}"))
            .or_else(|| types.get(name))
            .copied(),
        SqlExpr::Column { table: None, name } => types.get(name).copied(),
        _ => None,
    }
}

/// Infers the result type of a computed SELECT expression.
///
/// - A bare column reference resolves to its catalog type (default `Text`).
/// - Comparison / logical / null / membership predicates are `Bool`.
/// - Numeric literals type as their literal kind; a boolean literal is `Bool`.
/// - Everything else — string functions, arithmetic, casts, arbitrary
///   functions — defaults to `Text`, the safe fallback.
pub fn infer_computed_expr_type(expr: &SqlExpr, types: &HashMap<String, DdlColType>) -> DdlColType {
    match expr {
        SqlExpr::Column { .. } => bare_column_type(expr, types).unwrap_or(DdlColType::Text),
        SqlExpr::Literal(v) => match v {
            SqlValue::Int(_) => DdlColType::Int8,
            SqlValue::Float(_) => DdlColType::Float8,
            SqlValue::Bool(_) => DdlColType::Bool,
            SqlValue::Timestamp(_) => DdlColType::Timestamp,
            SqlValue::Timestamptz(_) => DdlColType::Timestamptz,
            // String / Decimal / Null / Bytes / Array: no certain non-TEXT
            // wire type — stay Text.
            _ => DdlColType::Text,
        },
        SqlExpr::BinaryOp { op, .. } => match op {
            // Comparison and logical operators yield a boolean.
            BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::And
            | BinaryOp::Or => DdlColType::Bool,
            // Arithmetic promotion (int vs float vs numeric) is too subtle to
            // type safely here; `||` is string concatenation. All stay Text.
            BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::Concat => DdlColType::Text,
        },
        SqlExpr::UnaryOp { op, .. } => match op {
            UnaryOp::Not => DdlColType::Bool,
            // `-x` preserves a numeric operand's type; anything else is Text.
            UnaryOp::Neg => DdlColType::Text,
        },
        SqlExpr::IsNull { .. }
        | SqlExpr::InList { .. }
        | SqlExpr::Between { .. }
        | SqlExpr::Like { .. } => DdlColType::Bool,
        // String functions, casts, subqueries, wildcards, array literals and
        // any other function call are not certainly non-TEXT — stay Text.
        SqlExpr::Function { .. }
        | SqlExpr::Case { .. }
        | SqlExpr::Cast { .. }
        | SqlExpr::Subquery(_)
        | SqlExpr::Wildcard
        | SqlExpr::ArrayLiteral(_) => DdlColType::Text,
    }
}

/// Infers the result type of an aggregate expression.
///
/// - `COUNT(...)` is always Postgres `bigint` (`Int8`).
/// - `MIN`/`MAX` preserve the input type when the argument is a resolvable
///   bare column; otherwise `Text`.
/// - `SUM`/`AVG` promote a floating-point argument to `Float8`; integer and
///   numeric sums (which map to `Text` in this codebase anyway) stay `Text`.
/// - Any other aggregate, or an unresolvable argument, is `Text`.
pub fn infer_aggregate_type(
    agg: &AggregateExpr,
    types: &HashMap<String, DdlColType>,
) -> DdlColType {
    let arg_bare_type = || agg.args.first().and_then(|a| bare_column_type(a, types));
    match agg.function.as_str() {
        "count" => DdlColType::Int8,
        "min" | "max" => arg_bare_type().unwrap_or(DdlColType::Text),
        "sum" | "avg" => match arg_bare_type() {
            Some(DdlColType::Float8) | Some(DdlColType::Float4) => DdlColType::Float8,
            _ => DdlColType::Text,
        },
        _ => DdlColType::Text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn types_map() -> HashMap<String, DdlColType> {
        let mut m = HashMap::new();
        m.insert("n".to_string(), DdlColType::Int8);
        m.insert("amount".to_string(), DdlColType::Float8);
        m.insert("label".to_string(), DdlColType::Text);
        m
    }

    fn column(name: &str) -> SqlExpr {
        SqlExpr::Column {
            table: None,
            name: name.to_string(),
        }
    }

    #[test]
    fn computed_bare_column_uses_catalog_type() {
        let t = types_map();
        assert_eq!(infer_computed_expr_type(&column("n"), &t), DdlColType::Int8);
        assert_eq!(
            infer_computed_expr_type(&column("label"), &t),
            DdlColType::Text
        );
        // Unknown column falls back to Text.
        assert_eq!(
            infer_computed_expr_type(&column("missing"), &t),
            DdlColType::Text
        );
    }

    #[test]
    fn computed_comparison_is_bool() {
        let t = types_map();
        let expr = SqlExpr::BinaryOp {
            left: Box::new(column("n")),
            op: BinaryOp::Gt,
            right: Box::new(SqlExpr::Literal(SqlValue::Int(5))),
        };
        assert_eq!(infer_computed_expr_type(&expr, &t), DdlColType::Bool);
    }

    #[test]
    fn computed_logical_and_not_are_bool() {
        let t = types_map();
        let and = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(SqlValue::Bool(true))),
            op: BinaryOp::And,
            right: Box::new(SqlExpr::Literal(SqlValue::Bool(false))),
        };
        assert_eq!(infer_computed_expr_type(&and, &t), DdlColType::Bool);
        let not = SqlExpr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(SqlExpr::Literal(SqlValue::Bool(true))),
        };
        assert_eq!(infer_computed_expr_type(&not, &t), DdlColType::Bool);
    }

    #[test]
    fn computed_string_function_and_concat_are_text() {
        let t = types_map();
        let upper = SqlExpr::Function {
            name: "upper".to_string(),
            args: vec![column("label")],
            distinct: false,
        };
        assert_eq!(infer_computed_expr_type(&upper, &t), DdlColType::Text);
        let concat = SqlExpr::BinaryOp {
            left: Box::new(column("label")),
            op: BinaryOp::Concat,
            right: Box::new(column("label")),
        };
        assert_eq!(infer_computed_expr_type(&concat, &t), DdlColType::Text);
    }

    #[test]
    fn computed_numeric_and_bool_literals() {
        let t = types_map();
        assert_eq!(
            infer_computed_expr_type(&SqlExpr::Literal(SqlValue::Int(1)), &t),
            DdlColType::Int8
        );
        assert_eq!(
            infer_computed_expr_type(&SqlExpr::Literal(SqlValue::Float(1.5)), &t),
            DdlColType::Float8
        );
        assert_eq!(
            infer_computed_expr_type(&SqlExpr::Literal(SqlValue::Bool(true)), &t),
            DdlColType::Bool
        );
        // A string literal has no certain non-TEXT wire type.
        assert_eq!(
            infer_computed_expr_type(&SqlExpr::Literal(SqlValue::String("x".to_string())), &t),
            DdlColType::Text
        );
    }

    fn agg(function: &str, arg: Option<SqlExpr>) -> AggregateExpr {
        AggregateExpr {
            function: function.to_string(),
            args: arg.into_iter().collect(),
            alias: function.to_string(),
            distinct: false,
            grouping_col_index: None,
        }
    }

    #[test]
    fn count_star_is_int8() {
        let t = types_map();
        assert_eq!(
            infer_aggregate_type(&agg("count", Some(SqlExpr::Wildcard)), &t),
            DdlColType::Int8
        );
        assert_eq!(
            infer_aggregate_type(&agg("count", Some(column("n"))), &t),
            DdlColType::Int8
        );
    }

    #[test]
    fn min_max_preserve_bare_column_type() {
        let t = types_map();
        assert_eq!(
            infer_aggregate_type(&agg("min", Some(column("n"))), &t),
            DdlColType::Int8
        );
        assert_eq!(
            infer_aggregate_type(&agg("max", Some(column("amount"))), &t),
            DdlColType::Float8
        );
        // Non-column / unresolvable argument stays Text.
        assert_eq!(
            infer_aggregate_type(&agg("min", Some(SqlExpr::Wildcard)), &t),
            DdlColType::Text
        );
    }

    #[test]
    fn sum_of_float_is_float8_sum_of_int_is_text() {
        let t = types_map();
        assert_eq!(
            infer_aggregate_type(&agg("sum", Some(column("amount"))), &t),
            DdlColType::Float8
        );
        assert_eq!(
            infer_aggregate_type(&agg("avg", Some(column("amount"))), &t),
            DdlColType::Float8
        );
        // SUM/AVG over an integer column stays Text (numeric, no regression).
        assert_eq!(
            infer_aggregate_type(&agg("sum", Some(column("n"))), &t),
            DdlColType::Text
        );
    }

    #[test]
    fn unknown_aggregate_is_text() {
        let t = types_map();
        assert_eq!(
            infer_aggregate_type(&agg("string_agg", Some(column("label"))), &t),
            DdlColType::Text
        );
    }
}
