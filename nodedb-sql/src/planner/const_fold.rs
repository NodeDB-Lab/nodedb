// SPDX-License-Identifier: Apache-2.0

//! Plan-time constant folding for `SqlExpr`.
//!
//! Evaluates literal expressions and registered zero-or-few-arg scalar
//! functions (e.g. `now()`, `current_timestamp`, `date_add(now(), '1h')`)
//! at plan time via the shared `nodedb_query::functions::eval_function`
//! evaluator.
//!
//! This keeps the bare-`SELECT` projection path, the `INSERT`/`UPSERT`
//! `VALUES` path, and any future default-expression paths from drifting
//! apart — they all reach the same evaluator that the Data Plane uses
//! for column-reference evaluation.
//!
//! Semantics: Postgres / SQL-standard compatible. `now()` and
//! `current_timestamp` snapshot once per statement — `CURRENT_TIMESTAMP`
//! is defined to return the same value for every row of a single
//! statement, and Postgres goes further (same value for the whole
//! transaction). Folding at plan time satisfies both contracts and is
//! cheaper than per-row runtime dispatch.

use std::sync::LazyLock;

use nodedb_types::Value;
use sonic_rs;

use crate::functions::registry::{FunctionCategory, FunctionRegistry};
use crate::types::{BinaryOp, SqlExpr, SqlValue, UnaryOp};

/// Process-wide default registry. Used by call sites that don't already
/// thread a `FunctionRegistry` through (e.g. the DML `VALUES` path).
static DEFAULT_REGISTRY: LazyLock<FunctionRegistry> = LazyLock::new(FunctionRegistry::new);

/// Access the shared default registry.
pub fn default_registry() -> &'static FunctionRegistry {
    &DEFAULT_REGISTRY
}

/// Convenience wrapper around [`fold_constant`] using the default registry.
pub fn fold_constant_default(expr: &SqlExpr) -> Option<SqlValue> {
    fold_constant(expr, default_registry())
}

/// Fold a `SqlExpr` to a literal `SqlValue` at plan time, or return
/// `None` if the expression depends on row/runtime state (column refs,
/// subqueries, unknown functions, etc.).
pub fn fold_constant(expr: &SqlExpr, registry: &FunctionRegistry) -> Option<SqlValue> {
    match expr {
        SqlExpr::Literal(v) => Some(v.clone()),
        SqlExpr::ArrayLiteral(items) => items
            .iter()
            .map(|item| fold_constant(item, registry))
            .collect::<Option<Vec<_>>>()
            .map(SqlValue::Array),
        SqlExpr::UnaryOp {
            op: UnaryOp::Neg,
            expr,
        } => match fold_constant(expr, registry)? {
            SqlValue::Int(i) => Some(SqlValue::Int(-i)),
            SqlValue::Float(f) => Some(SqlValue::Float(-f)),
            SqlValue::Decimal(d) => Some(SqlValue::Decimal(-d)),
            _ => None,
        },
        SqlExpr::BinaryOp { left, op, right } => {
            let l = fold_constant(left, registry)?;
            let r = fold_constant(right, registry)?;
            fold_binary(l, *op, r)
        }
        SqlExpr::Function { name, args, .. } => fold_function_call(name, args, registry),
        SqlExpr::Cast { expr, to_type } => fold_cast(fold_constant(expr, registry)?, to_type),
        _ => None,
    }
}

/// Fold a CAST at plan time. Only applies when the inner expression is already
/// a constant. The `to_type` string comes from sqlparser's `format!("{data_type}")`
/// output, so parameterised types like `NUMERIC(5,1)` must be matched by prefix.
fn fold_cast(inner: SqlValue, to_type: &str) -> Option<SqlValue> {
    let upper = to_type.to_uppercase();
    // Strip any precision/scale suffix: "NUMERIC(5,1)" → "NUMERIC".
    let base = upper
        .split('(')
        .next()
        .map(str::trim)
        .unwrap_or(upper.as_str());

    match base {
        "NUMERIC" | "DECIMAL" => match inner {
            SqlValue::Decimal(d) => Some(SqlValue::Decimal(d)),
            SqlValue::Int(i) => Some(SqlValue::Decimal(rust_decimal::Decimal::from(i))),
            SqlValue::Float(f) => rust_decimal::Decimal::try_from(f)
                .ok()
                .map(SqlValue::Decimal),
            SqlValue::String(s) => rust_decimal::Decimal::from_str_exact(&s)
                .ok()
                .map(SqlValue::Decimal),
            _ => None,
        },
        "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "INT2" | "INT4" | "INT8" => match inner {
            SqlValue::Int(i) => Some(SqlValue::Int(i)),
            SqlValue::Decimal(d) => {
                rust_decimal::prelude::ToPrimitive::to_i64(&d).map(SqlValue::Int)
            }
            SqlValue::Float(f) => {
                if f.is_finite() {
                    Some(SqlValue::Int(f as i64))
                } else {
                    None
                }
            }
            SqlValue::String(s) => s.parse::<i64>().ok().map(SqlValue::Int),
            _ => None,
        },
        "FLOAT" | "DOUBLE" | "REAL" | "FLOAT4" | "FLOAT8" | "DOUBLE PRECISION" => match inner {
            SqlValue::Float(f) => Some(SqlValue::Float(f)),
            SqlValue::Int(i) => Some(SqlValue::Float(i as f64)),
            SqlValue::Decimal(d) => {
                rust_decimal::prelude::ToPrimitive::to_f64(&d).map(SqlValue::Float)
            }
            SqlValue::String(s) => s.parse::<f64>().ok().map(SqlValue::Float),
            _ => None,
        },
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" | "CHARACTER" | "BPCHAR" => match inner {
            SqlValue::String(s) => Some(SqlValue::String(s)),
            SqlValue::Int(i) => Some(SqlValue::String(i.to_string())),
            SqlValue::Float(f) => Some(SqlValue::String(f.to_string())),
            SqlValue::Decimal(d) => Some(SqlValue::String(d.to_string())),
            SqlValue::Bool(b) => Some(SqlValue::String(b.to_string())),
            _ => None,
        },
        "BOOL" | "BOOLEAN" => match inner {
            SqlValue::Bool(b) => Some(SqlValue::Bool(b)),
            SqlValue::Int(i) => Some(SqlValue::Bool(i != 0)),
            SqlValue::String(s) => match s.to_lowercase().as_str() {
                "true" | "t" | "yes" | "1" | "on" => Some(SqlValue::Bool(true)),
                "false" | "f" | "no" | "0" | "off" => Some(SqlValue::Bool(false)),
                _ => None,
            },
            _ => None,
        },
        // `JSON` / `JSONB` — JSON values live internally as their text form
        // in `SqlValue::String`; the write path parses JSON-looking strings
        // into document structure. The cast elides to the inner value's JSON
        // text (mirrors the `::tsvector` / `::tsquery` elision in the resolver).
        "JSON" | "JSONB" => match inner {
            SqlValue::String(s) => Some(SqlValue::String(s)),
            SqlValue::Int(i) => Some(SqlValue::String(i.to_string())),
            SqlValue::Float(f) => Some(SqlValue::String(f.to_string())),
            SqlValue::Decimal(d) => Some(SqlValue::String(d.to_string())),
            SqlValue::Bool(b) => Some(SqlValue::String(b.to_string())),
            SqlValue::Null => Some(SqlValue::Null),
            _ => None,
        },
        _ => None,
    }
}

fn fold_binary(l: SqlValue, op: BinaryOp, r: SqlValue) -> Option<SqlValue> {
    Some(match (l, op, r) {
        // Int × Int arithmetic.
        (SqlValue::Int(a), BinaryOp::Add, SqlValue::Int(b)) => SqlValue::Int(a.checked_add(b)?),
        (SqlValue::Int(a), BinaryOp::Sub, SqlValue::Int(b)) => SqlValue::Int(a.checked_sub(b)?),
        (SqlValue::Int(a), BinaryOp::Mul, SqlValue::Int(b)) => SqlValue::Int(a.checked_mul(b)?),
        // Float × Float arithmetic.
        (SqlValue::Float(a), BinaryOp::Add, SqlValue::Float(b)) => SqlValue::Float(a + b),
        (SqlValue::Float(a), BinaryOp::Sub, SqlValue::Float(b)) => SqlValue::Float(a - b),
        (SqlValue::Float(a), BinaryOp::Mul, SqlValue::Float(b)) => SqlValue::Float(a * b),
        // Decimal × Decimal arithmetic.
        (SqlValue::Decimal(a), BinaryOp::Add, SqlValue::Decimal(b)) => {
            SqlValue::Decimal(a.checked_add(b)?)
        }
        (SqlValue::Decimal(a), BinaryOp::Sub, SqlValue::Decimal(b)) => {
            SqlValue::Decimal(a.checked_sub(b)?)
        }
        (SqlValue::Decimal(a), BinaryOp::Mul, SqlValue::Decimal(b)) => {
            SqlValue::Decimal(a.checked_mul(b)?)
        }
        (SqlValue::Decimal(a), BinaryOp::Div, SqlValue::Decimal(b)) => {
            SqlValue::Decimal(a.checked_div(b)?)
        }
        // Decimal × Int widening (Int promotes to Decimal).
        (SqlValue::Decimal(a), BinaryOp::Add, SqlValue::Int(b)) => {
            SqlValue::Decimal(a.checked_add(rust_decimal::Decimal::from(b))?)
        }
        (SqlValue::Int(a), BinaryOp::Add, SqlValue::Decimal(b)) => {
            SqlValue::Decimal(rust_decimal::Decimal::from(a).checked_add(b)?)
        }
        (SqlValue::Decimal(a), BinaryOp::Sub, SqlValue::Int(b)) => {
            SqlValue::Decimal(a.checked_sub(rust_decimal::Decimal::from(b))?)
        }
        (SqlValue::Int(a), BinaryOp::Sub, SqlValue::Decimal(b)) => {
            SqlValue::Decimal(rust_decimal::Decimal::from(a).checked_sub(b)?)
        }
        (SqlValue::Decimal(a), BinaryOp::Mul, SqlValue::Int(b)) => {
            SqlValue::Decimal(a.checked_mul(rust_decimal::Decimal::from(b))?)
        }
        (SqlValue::Int(a), BinaryOp::Mul, SqlValue::Decimal(b)) => {
            SqlValue::Decimal(rust_decimal::Decimal::from(a).checked_mul(b)?)
        }
        (SqlValue::Decimal(a), BinaryOp::Div, SqlValue::Int(b)) => {
            SqlValue::Decimal(a.checked_div(rust_decimal::Decimal::from(b))?)
        }
        (SqlValue::Int(a), BinaryOp::Div, SqlValue::Decimal(b)) => {
            SqlValue::Decimal(rust_decimal::Decimal::from(a).checked_div(b)?)
        }
        // String concat.
        (SqlValue::String(a), BinaryOp::Concat, SqlValue::String(b)) => {
            SqlValue::String(format!("{a}{b}"))
        }
        _ => return None,
    })
}

/// Fold a function call by recursively folding its arguments, dispatching
/// through the shared scalar evaluator, and converting the result back to
/// `SqlValue`. Only folds functions that are present in `registry`, so
/// callers can distinguish "unknown function" from "known function, all
/// args folded".
pub fn fold_function_call(
    name: &str,
    args: &[SqlExpr],
    registry: &FunctionRegistry,
) -> Option<SqlValue> {
    // Gate on registry so unknown-function paths keep their existing
    // fallbacks instead of collapsing to SqlValue::Null. Aggregates and
    // window functions aren't foldable — they need a row stream.
    let meta = registry.lookup(name)?;
    if matches!(
        meta.category,
        FunctionCategory::Aggregate | FunctionCategory::Window
    ) {
        return None;
    }

    let folded_args: Vec<Value> = args
        .iter()
        .map(|a| fold_constant(a, registry).map(sql_to_ndb_value))
        .collect::<Option<_>>()?;

    let result = nodedb_query::functions::eval_function(&name.to_lowercase(), &folded_args);
    Some(ndb_to_sql_value(result))
}

fn sql_to_ndb_value(v: SqlValue) -> Value {
    match v {
        SqlValue::Null => Value::Null,
        SqlValue::Bool(b) => Value::Bool(b),
        SqlValue::Int(i) => Value::Integer(i),
        SqlValue::Float(f) => Value::Float(f),
        SqlValue::Decimal(d) => Value::Decimal(d),
        SqlValue::String(s) => Value::String(s),
        SqlValue::Bytes(b) => Value::Bytes(b),
        SqlValue::Array(a) => Value::Array(a.into_iter().map(sql_to_ndb_value).collect()),
        SqlValue::Timestamp(dt) => Value::NaiveDateTime(dt),
        SqlValue::Timestamptz(dt) => Value::DateTime(dt),
    }
}

fn ndb_to_sql_value(v: Value) -> SqlValue {
    match v {
        Value::Null => SqlValue::Null,
        Value::Bool(b) => SqlValue::Bool(b),
        Value::Integer(i) => SqlValue::Int(i),
        Value::Float(f) => SqlValue::Float(f),
        Value::String(s) => SqlValue::String(s),
        Value::Bytes(b) => SqlValue::Bytes(b),
        Value::Array(a) => SqlValue::Array(a.into_iter().map(ndb_to_sql_value).collect()),
        // TZ-aware DateTime → Timestamptz; naive → Timestamp.
        Value::DateTime(dt) => SqlValue::Timestamptz(dt),
        Value::NaiveDateTime(dt) => SqlValue::Timestamp(dt),
        Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => SqlValue::String(s),
        Value::Duration(d) => SqlValue::String(d.to_human()),
        Value::Decimal(d) => SqlValue::Decimal(d),
        // Geometry and Object values are serialized to JSON strings so that
        // nested function calls like ST_Distance(ST_Point(...), ST_Point(...))
        // survive the SqlValue round-trip. The geo evaluator's geom_arg helper
        // recovers Geometry from a GeoJSON string; Object results (e.g. from
        // ST_GeoHashDecode) reach the client as a JSON-encoded string column.
        Value::Geometry(g) => sonic_rs::to_string(&g)
            .map(SqlValue::String)
            .unwrap_or(SqlValue::Null),
        Value::Object(map) => sonic_rs::to_string(&map)
            .map(SqlValue::String)
            .unwrap_or(SqlValue::Null),
        // Structured and opaque types collapse to Null — callers that
        // need these go through the runtime expression path, not folding.
        Value::Set(_) | Value::Range { .. } | Value::Record { .. } | Value::ArrayCell(_) => {
            SqlValue::Null
        }
        // Value is #[non_exhaustive]; future variants collapse to Null in the
        // constant-folding path — runtime expression evaluation handles them.
        _ => SqlValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_now_produces_timestamptz() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::Function {
            name: "now".into(),
            args: vec![],
            distinct: false,
        };
        let val = fold_constant(&expr, &registry).expect("now() should fold");
        match val {
            SqlValue::Timestamptz(dt) => {
                // Sanity: must not be epoch (year 1970).
                assert!(dt.micros > 0, "expected post-epoch timestamp, got micros=0");
            }
            other => panic!("expected SqlValue::Timestamptz, got {other:?}"),
        }
    }

    #[test]
    fn fold_current_timestamp_produces_timestamptz() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::Function {
            name: "current_timestamp".into(),
            args: vec![],
            distinct: false,
        };
        assert!(matches!(
            fold_constant(&expr, &registry),
            Some(SqlValue::Timestamptz(_))
        ));
    }

    #[test]
    fn fold_unknown_function_returns_none() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::Function {
            name: "definitely_not_a_real_function".into(),
            args: vec![],
            distinct: false,
        };
        assert!(fold_constant(&expr, &registry).is_none());
    }

    #[test]
    fn fold_array_literal_recursively() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::ArrayLiteral(vec![
            SqlExpr::Literal(SqlValue::String("public".into())),
            SqlExpr::Literal(SqlValue::Int(42)),
        ]);
        assert_eq!(
            fold_constant(&expr, &registry),
            Some(SqlValue::Array(vec![
                SqlValue::String("public".into()),
                SqlValue::Int(42),
            ]))
        );
    }

    #[test]
    fn fold_literal_arithmetic_still_works() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(SqlValue::Int(2))),
            op: BinaryOp::Add,
            right: Box::new(SqlExpr::Literal(SqlValue::Int(3))),
        };
        assert_eq!(fold_constant(&expr, &registry), Some(SqlValue::Int(5)));
    }

    #[test]
    fn fold_column_ref_returns_none() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::Column {
            table: None,
            name: "name".into(),
        };
        assert!(fold_constant(&expr, &registry).is_none());
    }

    #[test]
    fn fold_decimal_literal() {
        let registry = FunctionRegistry::new();
        let d = rust_decimal::Decimal::new(12345, 2); // 123.45
        let expr = SqlExpr::Literal(SqlValue::Decimal(d));
        assert_eq!(fold_constant(&expr, &registry), Some(SqlValue::Decimal(d)));
    }

    #[test]
    fn fold_decimal_addition() {
        use rust_decimal::Decimal;
        let registry = FunctionRegistry::new();
        let a = Decimal::new(12345, 2); // 123.45
        let b = Decimal::new(45678, 2); // 456.78
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(SqlValue::Decimal(a))),
            op: BinaryOp::Add,
            right: Box::new(SqlExpr::Literal(SqlValue::Decimal(b))),
        };
        let expected = Decimal::new(58023, 2); // 580.23
        assert_eq!(
            fold_constant(&expr, &registry),
            Some(SqlValue::Decimal(expected))
        );
    }

    #[test]
    fn fold_decimal_negation() {
        use rust_decimal::Decimal;
        let registry = FunctionRegistry::new();
        let d = Decimal::new(100, 0);
        let expr = SqlExpr::UnaryOp {
            op: crate::types::UnaryOp::Neg,
            expr: Box::new(SqlExpr::Literal(SqlValue::Decimal(d))),
        };
        assert_eq!(fold_constant(&expr, &registry), Some(SqlValue::Decimal(-d)));
    }

    #[test]
    fn fold_st_geohash() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::Function {
            name: "st_geohash".into(),
            args: vec![
                SqlExpr::UnaryOp {
                    op: UnaryOp::Neg,
                    expr: Box::new(SqlExpr::Literal(SqlValue::Float(122.4))),
                },
                SqlExpr::Literal(SqlValue::Float(37.8)),
                SqlExpr::Literal(SqlValue::Int(6)),
            ],
            distinct: false,
        };
        let v = fold_constant(&expr, &registry);
        match v {
            Some(SqlValue::String(ref s)) if !s.is_empty() => {}
            other => panic!("expected non-empty SqlValue::String, got {other:?}"),
        }
    }

    #[test]
    fn fold_st_distance_nested_st_point() {
        let registry = FunctionRegistry::new();
        let make_point = |lng: f64, lat: f64| SqlExpr::Function {
            name: "st_point".into(),
            args: vec![
                SqlExpr::Literal(SqlValue::Float(lng)),
                SqlExpr::Literal(SqlValue::Float(lat)),
            ],
            distinct: false,
        };
        let expr = SqlExpr::Function {
            name: "st_distance".into(),
            args: vec![make_point(-122.4, 37.8), make_point(-87.6, 41.8)],
            distinct: false,
        };
        let v = fold_constant(&expr, &registry);
        match v {
            Some(SqlValue::Float(d)) => {
                assert!(d > 0.0, "distance should be positive, got {d}");
            }
            other => panic!("expected SqlValue::Float, got {other:?}"),
        }
    }

    #[test]
    fn fold_decimal_int_widening() {
        use rust_decimal::Decimal;
        let registry = FunctionRegistry::new();
        let d = Decimal::new(100, 0); // 100
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(SqlValue::Decimal(d))),
            op: BinaryOp::Add,
            right: Box::new(SqlExpr::Literal(SqlValue::Int(50))),
        };
        let expected = Decimal::new(150, 0);
        assert_eq!(
            fold_constant(&expr, &registry),
            Some(SqlValue::Decimal(expected))
        );
    }
}
