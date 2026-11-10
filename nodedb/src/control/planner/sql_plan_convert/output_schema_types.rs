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
/// - Arithmetic, math functions and casts take the type the evaluator gives
///   their result.
/// - Everything else — string functions, arbitrary functions — defaults to
///   `Text`, the safe fallback.
pub fn infer_computed_expr_type(expr: &SqlExpr, types: &HashMap<String, DdlColType>) -> DdlColType {
    match expr {
        SqlExpr::Column { .. } => bare_column_type(expr, types).unwrap_or(DdlColType::Text),
        SqlExpr::Literal(v) => match v {
            SqlValue::Int(_) => DdlColType::Int8,
            SqlValue::Float(_) => DdlColType::Float8,
            SqlValue::Bool(_) => DdlColType::Bool,
            SqlValue::Timestamp(_) => DdlColType::Timestamp,
            SqlValue::Timestamptz(_) => DdlColType::Timestamptz,
            SqlValue::Decimal(_) => DdlColType::Numeric,
            // String / Null / Bytes / Array: no certain non-TEXT wire type —
            // stay Text.
            _ => DdlColType::Text,
        },
        SqlExpr::BinaryOp { left, op, right } => match op {
            // Comparison and logical operators yield a boolean.
            BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::And
            | BinaryOp::Or => DdlColType::Bool,
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                arithmetic_type(
                    infer_computed_expr_type(left, types),
                    infer_computed_expr_type(right, types),
                )
            }
            // `||` is string concatenation.
            BinaryOp::Concat => DdlColType::Text,
        },
        SqlExpr::UnaryOp { op, expr } => match op {
            UnaryOp::Not => DdlColType::Bool,
            // `-x` keeps an integer, float or numeric operand's class.
            UnaryOp::Neg => match numeric_class(infer_computed_expr_type(expr, types)) {
                Some(NumericClass::Integer) => DdlColType::Int8,
                Some(NumericClass::Float) => DdlColType::Float8,
                Some(NumericClass::Numeric) => DdlColType::Numeric,
                None => DdlColType::Text,
            },
        },
        SqlExpr::IsNull { .. }
        | SqlExpr::InList { .. }
        | SqlExpr::Between { .. }
        | SqlExpr::Like { .. } => DdlColType::Bool,
        // `time_bucket` keeps the type of the column it buckets.
        SqlExpr::Function { name, args, .. } if name.eq_ignore_ascii_case("time_bucket") => {
            let mut columns = args.iter().filter(|a| matches!(a, SqlExpr::Column { .. }));
            match (columns.next(), columns.next()) {
                (Some(column), None) => time_bucket_type(bare_column_type(column, types)),
                _ => DdlColType::Text,
            }
        }
        // A CASE yields one of its branches. When every non-NULL branch has
        // one type, that is the CASE's type.
        SqlExpr::Case {
            when_then,
            else_expr,
            ..
        } => {
            let mut branch_types = when_then
                .iter()
                .map(|(_, then)| then)
                .chain(else_expr.as_deref())
                .filter(|branch| !matches!(branch, SqlExpr::Literal(SqlValue::Null)))
                .map(|branch| infer_computed_expr_type(branch, types));
            match branch_types.next() {
                Some(first) if branch_types.all(|ty| ty == first) => first,
                Some(_) | None => DdlColType::Text,
            }
        }
        // A cast yields its target type, or NULL.
        SqlExpr::Cast { to_type, .. } => cast_type(to_type),
        // A math function is typed as the evaluator types its result. String
        // functions and any other call are not certainly non-TEXT.
        SqlExpr::Function { name, args, .. } => {
            math_function_type(name, args, types).unwrap_or(DdlColType::Text)
        }
        // Subqueries, wildcards and array literals stay Text.
        SqlExpr::Subquery(_) | SqlExpr::Wildcard | SqlExpr::ArrayLiteral(_) => DdlColType::Text,
    }
}

/// Whether a numeric wire type holds integer, float or numeric cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumericClass {
    Integer,
    Float,
    Numeric,
}

/// The class of a numeric wire type. `None` for every other type.
fn numeric_class(ty: DdlColType) -> Option<NumericClass> {
    match ty {
        DdlColType::Int8 | DdlColType::Int4 | DdlColType::Int2 => Some(NumericClass::Integer),
        DdlColType::Float8 | DdlColType::Float4 => Some(NumericClass::Float),
        DdlColType::Numeric => Some(NumericClass::Numeric),
        DdlColType::Text
        | DdlColType::Varchar
        | DdlColType::Bool
        | DdlColType::Bytea
        | DdlColType::Json
        | DdlColType::Jsonb
        | DdlColType::Timestamp
        | DdlColType::Timestamptz
        | DdlColType::Uuid
        | DdlColType::Float4Array
        | DdlColType::Float8Array => None,
    }
}

/// The type of `left op right` for an arithmetic operator.
///
/// - A float operand makes the result `Float8`, whatever the other: the
///   evaluator reads the other operand as a float, or yields NULL.
/// - Two integers stay `Int8`: `/` and `%` truncate toward zero.
/// - A numeric with an integer or a numeric is `Numeric`.
/// - Any other pair is `Text`.
fn arithmetic_type(left: DdlColType, right: DdlColType) -> DdlColType {
    match (numeric_class(left), numeric_class(right)) {
        (Some(NumericClass::Float), _) | (_, Some(NumericClass::Float)) => DdlColType::Float8,
        (Some(NumericClass::Integer), Some(NumericClass::Integer)) => DdlColType::Int8,
        (Some(_), Some(_)) => DdlColType::Numeric,
        _ => DdlColType::Text,
    }
}

/// The type of a math function call, as the evaluator types its result.
/// `None` for a name that is not a math function.
///
/// - `abs` keeps its argument's type.
/// - `ceil`, `floor` and `sign` are `Numeric` over a numeric, else `Float8`.
/// - `round` is `Numeric` over a numeric, and over an integer with places;
///   `Float8` over a float or a lone integer.
/// - `mod` is the `%` operator.
/// - `power`, `sqrt`, `exp` and the logarithms are `Numeric` when an
///   argument is numeric and none is a float, else `Float8`. `log(b, x)` is
///   `Numeric` over integers too. An untyped argument is `Text`.
fn math_function_type(
    name: &str,
    args: &[SqlExpr],
    types: &HashMap<String, DdlColType>,
) -> Option<DdlColType> {
    let arg = |i: usize| {
        args.get(i)
            .map_or(DdlColType::Text, |a| infer_computed_expr_type(a, types))
    };
    let first = || numeric_class(arg(0));
    let exact_or_float = || match first() {
        Some(NumericClass::Numeric) => DdlColType::Numeric,
        Some(_) => DdlColType::Float8,
        None => DdlColType::Text,
    };
    Some(match name.to_ascii_lowercase().as_str() {
        "abs" => match first() {
            Some(NumericClass::Integer) => DdlColType::Int8,
            Some(NumericClass::Float) => DdlColType::Float8,
            Some(NumericClass::Numeric) => DdlColType::Numeric,
            None => DdlColType::Text,
        },
        "ceil" | "ceiling" | "floor" | "sign" => exact_or_float(),
        "round" => match first() {
            Some(NumericClass::Integer) if args.len() >= 2 => DdlColType::Numeric,
            _ => exact_or_float(),
        },
        "mod" => arithmetic_type(arg(0), arg(1)),
        "power" | "pow" | "sqrt" | "exp" | "ln" | "log" | "log10" | "log2" => {
            let lower = name.to_ascii_lowercase();
            let two_arg_log = lower == "log" && args.len() >= 2;
            let arity = if two_arg_log || matches!(lower.as_str(), "power" | "pow") {
                2
            } else {
                1
            };
            let classes: Option<Vec<NumericClass>> =
                (0..arity).map(|i| numeric_class(arg(i))).collect();
            match classes {
                None => DdlColType::Text,
                Some(classes) if classes.contains(&NumericClass::Float) => DdlColType::Float8,
                Some(classes) if two_arg_log || classes.contains(&NumericClass::Numeric) => {
                    DdlColType::Numeric
                }
                Some(_) => DdlColType::Float8,
            }
        }
        _ => return None,
    })
}

/// The type a `CAST` to `to_type` yields. A name no runtime cast covers is
/// `Text`.
pub fn cast_type(to_type: &str) -> DdlColType {
    use nodedb_query::CastType;
    match CastType::from_type_name(to_type) {
        Some(CastType::Int) => DdlColType::Int8,
        Some(CastType::Int4) => DdlColType::Int4,
        Some(CastType::Int2) => DdlColType::Int2,
        Some(CastType::Float) => DdlColType::Float8,
        Some(CastType::Bool) => DdlColType::Bool,
        Some(CastType::Decimal(_)) => DdlColType::Numeric,
        Some(CastType::Uuid) => DdlColType::Uuid,
        Some(CastType::Timestamp) => DdlColType::Timestamp,
        Some(CastType::Timestamptz) => DdlColType::Timestamptz,
        Some(CastType::String | CastType::Duration) | None => DdlColType::Text,
    }
}

/// The result type of a `time_bucket` call over a column of type `source`.
///
/// A bucket of an instant is an instant of the same kind, and a bucket of a
/// `BIGINT` is a `BIGINT`. Any other source, or none, is `Text`.
pub fn time_bucket_type(source: Option<DdlColType>) -> DdlColType {
    match source {
        Some(ty @ (DdlColType::Timestamp | DdlColType::Timestamptz | DdlColType::Int8)) => ty,
        Some(_) | None => DdlColType::Text,
    }
}

/// Infers the result type of an aggregate expression.
///
/// - `COUNT(...)` is always Postgres `bigint` (`Int8`).
/// - `MIN`/`MAX` preserve the input type when the argument is a resolvable
///   bare column; otherwise `Text`.
/// - `SUM` promotes a floating-point argument to `Float8`; integer and
///   numeric sums stay `Text`.
/// - `AVG` is typed from its argument's inferred type, as PostgreSQL types
///   it: `Float8` over a float argument, `Numeric` over an integer or a
///   numeric one. An untyped argument can hold either kind, so its AVG is a
///   float or a numeric by the data: `Text`.
/// - Any other aggregate, or an unresolvable argument, is `Text`.
pub fn infer_aggregate_type(
    agg: &AggregateExpr,
    types: &HashMap<String, DdlColType>,
) -> DdlColType {
    let arg_bare_type = || agg.args.first().and_then(|a| bare_column_type(a, types));
    match agg.function.as_str() {
        // A HyperLogLog estimate finalizes to a rounded integer.
        "count" | "approx_count_distinct" => DdlColType::Int8,
        // A t-digest quantile, an interpolated percentile, and every
        // Welford dispersion finalize to a float, or NULL.
        "approx_percentile" | "percentile_cont" | "stddev" | "stddev_pop" | "stddev_samp"
        | "variance" | "var_pop" | "var_samp" | "ts_stddev" => DdlColType::Float8,
        // The space-saving top-k finalizes to an array of
        // `{item, count, error}` objects.
        "approx_topk" => DdlColType::Json,
        "min" | "max" => arg_bare_type().unwrap_or(DdlColType::Text),
        "sum" => match arg_bare_type() {
            Some(DdlColType::Float8) | Some(DdlColType::Float4) => DdlColType::Float8,
            _ => DdlColType::Text,
        },
        // PostgreSQL's `avg` is float8 over a float and numeric over an
        // integer or a numeric. The run-time AVG is a float when a float
        // cell is fed, else a numeric. `avg_distinct` is the same.
        "avg" | "avg_distinct" => match agg
            .args
            .first()
            .and_then(|arg| numeric_class(infer_computed_expr_type(arg, types)))
        {
            Some(NumericClass::Float) => DdlColType::Float8,
            Some(NumericClass::Integer | NumericClass::Numeric) => DdlColType::Numeric,
            None => DdlColType::Text,
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

    /// `time_bucket` takes the type of the column it buckets when that is an
    /// instant or a `BIGINT`, in either argument order. Otherwise it is Text.
    #[test]
    fn time_bucket_keeps_its_column_type() {
        let mut t = types_map();
        t.insert("ts".to_string(), DdlColType::Timestamptz);
        let bucket = |args: Vec<SqlExpr>| SqlExpr::Function {
            name: "time_bucket".to_string(),
            args,
            distinct: false,
        };
        let interval = || SqlExpr::Literal(SqlValue::String("1 hour".into()));
        assert_eq!(
            infer_computed_expr_type(&bucket(vec![interval(), column("ts")]), &t),
            DdlColType::Timestamptz
        );
        assert_eq!(
            infer_computed_expr_type(&bucket(vec![column("n"), interval()]), &t),
            DdlColType::Int8
        );
        assert_eq!(
            infer_computed_expr_type(&bucket(vec![interval(), column("amount")]), &t),
            DdlColType::Text
        );
    }

    /// Math functions and arithmetic over integer and float operands take a
    /// numeric type. A CASE takes its branches' one type.
    #[test]
    fn numeric_expressions_take_a_numeric_type() {
        let mut t = types_map();
        t.insert("price".to_string(), DdlColType::Numeric);
        let call = |name: &str, args: Vec<SqlExpr>| SqlExpr::Function {
            name: name.to_string(),
            args,
            distinct: false,
        };
        let binary = |left: SqlExpr, op: BinaryOp, right: SqlExpr| SqlExpr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
        let one = || SqlExpr::Literal(SqlValue::Int(1));
        // A float with a numeric is float8, in either order. Two numerics
        // are numeric.
        assert_eq!(
            infer_computed_expr_type(
                &binary(column("amount"), BinaryOp::Mul, column("price")),
                &t
            ),
            DdlColType::Float8
        );
        assert_eq!(
            infer_computed_expr_type(
                &binary(column("price"), BinaryOp::Sub, column("amount")),
                &t
            ),
            DdlColType::Float8
        );
        assert_eq!(
            infer_computed_expr_type(&binary(column("price"), BinaryOp::Add, column("price")), &t),
            DdlColType::Numeric
        );
        assert_eq!(
            infer_computed_expr_type(&binary(column("n"), BinaryOp::Mul, column("price")), &t),
            DdlColType::Numeric
        );
        // A float with an untyped operand is float8: the evaluator reads the
        // other operand as a float, or yields NULL.
        assert_eq!(
            infer_computed_expr_type(
                &binary(column("label"), BinaryOp::Add, column("amount")),
                &t
            ),
            DdlColType::Float8
        );
        assert_eq!(
            infer_computed_expr_type(&call("round", vec![column("amount"), one()]), &t),
            DdlColType::Float8
        );
        assert_eq!(
            infer_computed_expr_type(&call("round", vec![column("label"), one()]), &t),
            DdlColType::Text
        );
        // Math functions take PostgreSQL's result types.
        for (name, args, want) in [
            ("abs", vec![column("n")], DdlColType::Int8),
            ("abs", vec![column("price")], DdlColType::Numeric),
            ("ceil", vec![column("n")], DdlColType::Float8),
            ("floor", vec![column("price")], DdlColType::Numeric),
            ("round", vec![column("n")], DdlColType::Float8),
            ("round", vec![column("n"), one()], DdlColType::Numeric),
            ("round", vec![column("price"), one()], DdlColType::Numeric),
            ("sqrt", vec![column("price")], DdlColType::Numeric),
            ("sqrt", vec![column("n")], DdlColType::Float8),
            (
                "power",
                vec![column("price"), column("n")],
                DdlColType::Numeric,
            ),
            (
                "power",
                vec![column("price"), column("amount")],
                DdlColType::Float8,
            ),
            ("log", vec![column("n"), column("n")], DdlColType::Numeric),
            ("ln", vec![column("label")], DdlColType::Text),
            ("mod", vec![column("n"), column("n")], DdlColType::Int8),
        ] {
            assert_eq!(
                infer_computed_expr_type(&call(name, args), &t),
                want,
                "{name}"
            );
        }
        assert_eq!(
            infer_computed_expr_type(&binary(column("n"), BinaryOp::Add, one()), &t),
            DdlColType::Int8
        );
        assert_eq!(
            infer_computed_expr_type(&binary(column("amount"), BinaryOp::Div, column("n")), &t),
            DdlColType::Float8
        );
        assert_eq!(
            infer_computed_expr_type(&binary(column("n"), BinaryOp::Div, column("n")), &t),
            DdlColType::Int8
        );
        assert_eq!(
            infer_computed_expr_type(&binary(column("n"), BinaryOp::Mod, column("n")), &t),
            DdlColType::Int8
        );
        let case = SqlExpr::Case {
            operand: None,
            when_then: vec![(
                binary(column("n"), BinaryOp::Gt, one()),
                SqlExpr::Literal(SqlValue::String("many".into())),
            )],
            else_expr: Some(Box::new(SqlExpr::Literal(SqlValue::String("one".into())))),
        };
        assert_eq!(infer_computed_expr_type(&case, &t), DdlColType::Text);
        let mixed = SqlExpr::Case {
            operand: None,
            when_then: vec![(binary(column("n"), BinaryOp::Gt, one()), column("amount"))],
            else_expr: Some(Box::new(SqlExpr::Literal(SqlValue::Null))),
        };
        assert_eq!(infer_computed_expr_type(&mixed, &t), DdlColType::Float8);
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
        assert_eq!(
            infer_computed_expr_type(
                &SqlExpr::Literal(SqlValue::Decimal(rust_decimal::Decimal::new(15, 1))),
                &t
            ),
            DdlColType::Numeric
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
            filter_text: None,
            arg_text: None,
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
        // SUM over an integer column stays Text. AVG over it is numeric.
        assert_eq!(
            infer_aggregate_type(&agg("sum", Some(column("n"))), &t),
            DdlColType::Text
        );
        assert_eq!(
            infer_aggregate_type(&agg("avg", Some(column("n"))), &t),
            DdlColType::Numeric
        );
    }

    /// AVG takes its type from the argument's inferred type where the
    /// run-time result type is certain, else `Text`.
    #[test]
    fn avg_type_follows_the_argument_type() {
        let mut t = types_map();
        t.insert("price".to_string(), DdlColType::Numeric);
        t.insert("ratio".to_string(), DdlColType::Float4);
        let avg = |arg: SqlExpr| infer_aggregate_type(&agg("avg", Some(arg)), &t);
        let one = || SqlExpr::Literal(SqlValue::Int(1));
        let add = |left: SqlExpr, right: SqlExpr| SqlExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Add,
            right: Box::new(right),
        };
        assert_eq!(avg(column("ratio")), DdlColType::Float8);
        assert_eq!(avg(column("price")), DdlColType::Numeric);
        assert_eq!(avg(add(column("n"), one())), DdlColType::Numeric);
        // Float arithmetic yields float cells, so its AVG is float8.
        assert_eq!(avg(add(column("amount"), one())), DdlColType::Float8);
        // A schemaless field can hold integers or floats.
        assert_eq!(avg(column("missing")), DdlColType::Text);
        assert_eq!(avg(column("label")), DdlColType::Text);
        let case = SqlExpr::Case {
            operand: None,
            when_then: vec![(SqlExpr::Literal(SqlValue::Bool(true)), column("amount"))],
            else_expr: Some(Box::new(SqlExpr::Literal(SqlValue::Null))),
        };
        assert_eq!(avg(case), DdlColType::Float8);
        assert_eq!(
            infer_aggregate_type(&agg("avg_distinct", Some(column("n"))), &t),
            DdlColType::Numeric
        );
    }

    /// A cast yields its target type.
    #[test]
    fn cast_takes_its_target_type() {
        let t = types_map();
        let cast = |to_type: &str| SqlExpr::Cast {
            expr: Box::new(column("label")),
            to_type: to_type.to_string(),
        };
        for (to_type, want) in [
            ("SMALLINT", DdlColType::Int2),
            ("INTEGER", DdlColType::Int4),
            ("INT", DdlColType::Int4),
            ("BIGINT", DdlColType::Int8),
            ("DOUBLE PRECISION", DdlColType::Float8),
            ("NUMERIC(10,2)", DdlColType::Numeric),
            ("BOOLEAN", DdlColType::Bool),
            ("TEXT", DdlColType::Text),
            ("JSONB", DdlColType::Text),
        ] {
            assert_eq!(
                infer_computed_expr_type(&cast(to_type), &t),
                want,
                "{to_type}"
            );
        }
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
