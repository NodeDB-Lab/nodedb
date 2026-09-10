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

use crate::error::SqlError;
use crate::functions::registry::{FunctionCategory, FunctionRegistry};
use crate::types::{BinaryOp, SqlExpr, SqlValue, UnaryOp};

/// Outcome of folding a constant expression.
///
/// Three states, kept apart on purpose: `Ok(Some)` folded, `Ok(None)` is not
/// a constant and belongs to the row-scope evaluator, `Err` was a constant
/// whose evaluation failed. Collapsing the last two loses the difference
/// between "ask again at runtime" and "this can never succeed" — and a
/// from-less SELECT has no runtime to ask.
pub type FoldResult = Result<Option<SqlValue>, SqlError>;

/// Process-wide default registry. Used by call sites that don't already
/// thread a `FunctionRegistry` through (e.g. the DML `VALUES` path).
static DEFAULT_REGISTRY: LazyLock<FunctionRegistry> = LazyLock::new(FunctionRegistry::new);

/// Access the shared default registry.
pub fn default_registry() -> &'static FunctionRegistry {
    &DEFAULT_REGISTRY
}

/// Convenience wrapper around [`fold_constant`] using the default registry.
pub fn fold_constant_default(expr: &SqlExpr) -> FoldResult {
    fold_constant(expr, default_registry())
}

/// Whether a fold is allowed to evaluate `Volatile` functions.
///
/// `Reuse` is plan-time folding whose result outlives this execution, so a
/// volatile call is never evaluated. `Once` is folding for a plan that is
/// itself marked volatile and never cached, so the call is evaluated here and
/// re-evaluated on the next execution's re-plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldScope {
    /// The folded value can be reused by a later execution.
    Reuse,
    /// The folded value serves this execution only.
    Once,
}

/// Fold a `SqlExpr` to a literal `SqlValue` at plan time. See [`FoldResult`]
/// for what each outcome means. Volatile calls are never folded.
pub fn fold_constant(expr: &SqlExpr, registry: &FunctionRegistry) -> FoldResult {
    fold_constant_scoped(expr, registry, FoldScope::Reuse)
}

/// Fold a `SqlExpr` under an explicit [`FoldScope`].
pub fn fold_constant_scoped(
    expr: &SqlExpr,
    registry: &FunctionRegistry,
    scope: FoldScope,
) -> FoldResult {
    match expr {
        SqlExpr::Literal(v) => Ok(Some(v.clone())),
        SqlExpr::ArrayLiteral(items) => {
            let mut folded = Vec::with_capacity(items.len());
            for item in items {
                match fold_constant_scoped(item, registry, scope)? {
                    Some(v) => folded.push(v),
                    None => return Ok(None),
                }
            }
            Ok(Some(SqlValue::Array(folded)))
        }
        SqlExpr::UnaryOp {
            op: UnaryOp::Neg,
            expr,
        } => Ok(match fold_constant_scoped(expr, registry, scope)? {
            // `checked_neg` so negating `i64::MIN` declines to fold rather
            // than wrapping (release) or panicking (debug).
            Some(SqlValue::Int(i)) => i.checked_neg().map(SqlValue::Int),
            Some(SqlValue::Float(f)) => Some(SqlValue::Float(-f)),
            Some(SqlValue::Decimal(d)) => Some(SqlValue::Decimal(-d)),
            _ => None,
        }),
        SqlExpr::BinaryOp { left, op, right } => {
            let (Some(l), Some(r)) = (
                fold_constant_scoped(left, registry, scope)?,
                fold_constant_scoped(right, registry, scope)?,
            ) else {
                return Ok(None);
            };
            fold_binary(l, *op, r)
        }
        SqlExpr::Function { name, args, .. } => {
            fold_function_call_scoped(name, args, registry, scope)
        }
        SqlExpr::Cast { expr, to_type } => Ok(fold_constant_scoped(expr, registry, scope)?
            .and_then(|inner| fold_cast(inner, to_type))),
        _ => Ok(None),
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
        // REGCLASS needs the session catalog. Keep the cast intact for the
        // contextual catalog-fold pass rather than erasing it to a string.
        "REGCLASS" => None,
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

/// Whether a value is a zero divisor. Applied before any `checked_div` /
/// `checked_rem`, which return `None` for a zero divisor and for genuine
/// overflow alike — folding those together is what let a division by zero
/// pass as "not foldable".
fn is_zero(v: &SqlValue) -> bool {
    match v {
        SqlValue::Int(0) => true,
        SqlValue::Float(f) => *f == 0.0,
        SqlValue::Decimal(d) => d.is_zero(),
        _ => false,
    }
}

fn overflowed(detail: &str) -> SqlError {
    SqlError::ConstantOverflow {
        detail: detail.to_owned(),
    }
}

fn fold_binary(l: SqlValue, op: BinaryOp, r: SqlValue) -> FoldResult {
    if matches!(op, BinaryOp::Div | BinaryOp::Mod) && is_zero(&r) {
        return Err(SqlError::DivisionByZero);
    }
    Ok(Some(match (l, op, r) {
        // Int × Int arithmetic.
        (SqlValue::Int(a), BinaryOp::Add, SqlValue::Int(b)) => {
            SqlValue::Int(a.checked_add(b).ok_or_else(|| overflowed("integer add"))?)
        }
        (SqlValue::Int(a), BinaryOp::Sub, SqlValue::Int(b)) => SqlValue::Int(
            a.checked_sub(b)
                .ok_or_else(|| overflowed("integer subtract"))?,
        ),
        (SqlValue::Int(a), BinaryOp::Mul, SqlValue::Int(b)) => SqlValue::Int(
            a.checked_mul(b)
                .ok_or_else(|| overflowed("integer multiply"))?,
        ),
        // Int division and modulo. The zero divisor is already refused above,
        // so `None` here is `i64::MIN / -1`, which overflows.
        (SqlValue::Int(a), BinaryOp::Div, SqlValue::Int(b)) => SqlValue::Int(
            a.checked_div(b)
                .ok_or_else(|| overflowed("integer divide"))?,
        ),
        (SqlValue::Int(a), BinaryOp::Mod, SqlValue::Int(b)) => SqlValue::Int(
            a.checked_rem(b)
                .ok_or_else(|| overflowed("integer modulo"))?,
        ),
        // Float × Float arithmetic.
        (SqlValue::Float(a), BinaryOp::Add, SqlValue::Float(b)) => SqlValue::Float(a + b),
        (SqlValue::Float(a), BinaryOp::Sub, SqlValue::Float(b)) => SqlValue::Float(a - b),
        (SqlValue::Float(a), BinaryOp::Mul, SqlValue::Float(b)) => SqlValue::Float(a * b),
        (SqlValue::Float(a), BinaryOp::Div, SqlValue::Float(b)) => SqlValue::Float(a / b),
        (SqlValue::Float(a), BinaryOp::Mod, SqlValue::Float(b)) => SqlValue::Float(a % b),
        // Decimal × Decimal arithmetic.
        (SqlValue::Decimal(a), BinaryOp::Add, SqlValue::Decimal(b)) => {
            SqlValue::Decimal(a.checked_add(b).ok_or_else(|| overflowed("decimal add"))?)
        }
        (SqlValue::Decimal(a), BinaryOp::Sub, SqlValue::Decimal(b)) => SqlValue::Decimal(
            a.checked_sub(b)
                .ok_or_else(|| overflowed("decimal subtract"))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Mul, SqlValue::Decimal(b)) => SqlValue::Decimal(
            a.checked_mul(b)
                .ok_or_else(|| overflowed("decimal multiply"))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Div, SqlValue::Decimal(b)) => SqlValue::Decimal(
            a.checked_div(b)
                .ok_or_else(|| overflowed("decimal divide"))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Mod, SqlValue::Decimal(b)) => SqlValue::Decimal(
            a.checked_rem(b)
                .ok_or_else(|| overflowed("decimal modulo"))?,
        ),
        // Decimal × Int widening (Int promotes to Decimal).
        (SqlValue::Decimal(a), BinaryOp::Add, SqlValue::Int(b)) => SqlValue::Decimal(
            a.checked_add(rust_decimal::Decimal::from(b))
                .ok_or_else(|| overflowed("decimal add"))?,
        ),
        (SqlValue::Int(a), BinaryOp::Add, SqlValue::Decimal(b)) => SqlValue::Decimal(
            rust_decimal::Decimal::from(a)
                .checked_add(b)
                .ok_or_else(|| overflowed("decimal add"))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Sub, SqlValue::Int(b)) => SqlValue::Decimal(
            a.checked_sub(rust_decimal::Decimal::from(b))
                .ok_or_else(|| overflowed("decimal subtract"))?,
        ),
        (SqlValue::Int(a), BinaryOp::Sub, SqlValue::Decimal(b)) => SqlValue::Decimal(
            rust_decimal::Decimal::from(a)
                .checked_sub(b)
                .ok_or_else(|| overflowed("decimal subtract"))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Mul, SqlValue::Int(b)) => SqlValue::Decimal(
            a.checked_mul(rust_decimal::Decimal::from(b))
                .ok_or_else(|| overflowed("decimal multiply"))?,
        ),
        (SqlValue::Int(a), BinaryOp::Mul, SqlValue::Decimal(b)) => SqlValue::Decimal(
            rust_decimal::Decimal::from(a)
                .checked_mul(b)
                .ok_or_else(|| overflowed("decimal multiply"))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Div, SqlValue::Int(b)) => SqlValue::Decimal(
            a.checked_div(rust_decimal::Decimal::from(b))
                .ok_or_else(|| overflowed("decimal divide"))?,
        ),
        (SqlValue::Int(a), BinaryOp::Div, SqlValue::Decimal(b)) => SqlValue::Decimal(
            rust_decimal::Decimal::from(a)
                .checked_div(b)
                .ok_or_else(|| overflowed("decimal divide"))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Mod, SqlValue::Int(b)) => SqlValue::Decimal(
            a.checked_rem(rust_decimal::Decimal::from(b))
                .ok_or_else(|| overflowed("decimal modulo"))?,
        ),
        (SqlValue::Int(a), BinaryOp::Mod, SqlValue::Decimal(b)) => SqlValue::Decimal(
            rust_decimal::Decimal::from(a)
                .checked_rem(b)
                .ok_or_else(|| overflowed("decimal modulo"))?,
        ),
        // String concat.
        (SqlValue::String(a), BinaryOp::Concat, SqlValue::String(b)) => {
            SqlValue::String(format!("{a}{b}"))
        }
        _ => return Ok(None),
    }))
}

/// Fold a function call by recursively folding its arguments, dispatching
/// through the shared scalar evaluator, and converting the result back to
/// `SqlValue`. Only folds functions that are present in `registry`, so
/// callers can distinguish "unknown function" from "known function, all
/// args folded". A `Volatile` function is never folded.
pub fn fold_function_call(name: &str, args: &[SqlExpr], registry: &FunctionRegistry) -> FoldResult {
    fold_function_call_scoped(name, args, registry, FoldScope::Reuse)
}

/// Fold a function call under an explicit [`FoldScope`].
pub fn fold_function_call_scoped(
    name: &str,
    args: &[SqlExpr],
    registry: &FunctionRegistry,
    scope: FoldScope,
) -> FoldResult {
    // Gate on registry so unknown-function paths keep their existing
    // fallbacks instead of collapsing to SqlValue::Null. Aggregates and
    // window functions aren't foldable — they need a row stream.
    let name_lower = name.to_lowercase();
    let Some(meta) = registry.lookup(&name_lower) else {
        return Ok(None);
    };
    if matches!(
        meta.category,
        FunctionCategory::Aggregate | FunctionCategory::Window
    ) {
        return Ok(None);
    }
    // A volatile call must run per execution, so folding it into a reusable
    // literal would freeze the first result into every later execution of the
    // same plan. Same "not folded" outcome the category guard above produces.
    if meta.volatility.is_volatile() && scope == FoldScope::Reuse {
        return Ok(None);
    }
    // Sequence accessors read catalog state this folder has no handle on.
    // `planner::catalog_expr_fold::eval_catalog_constant` resolves them
    // through `SqlCatalog` before any fold runs.
    if matches!(name_lower.as_str(), "nextval" | "currval" | "setval") {
        return Ok(None);
    }

    let mut folded_args = Vec::with_capacity(args.len());
    for arg in args {
        match fold_constant_scoped(arg, registry, scope)? {
            Some(v) => folded_args.push(sql_to_ndb_value(v)),
            None => return Ok(None),
        }
    }

    // A registered function whose arguments all folded is fully determined
    // here, so a failure is the answer — not a reason to defer. Deferring it
    // reached the row-scope evaluator only when the statement had a FROM
    // clause; `SELECT mod(5, 0)` has no row scope and became NULL.
    // Exhaustive on purpose: a new `EvalError` variant must be classified
    // here rather than defaulting to "defer to a runtime that may not exist".
    match nodedb_query::functions::eval_function(&name_lower, &folded_args) {
        Ok(result) => Ok(Some(ndb_to_sql_value(result))),
        Err(nodedb_query::EvalError::DivisionByZero) => Err(SqlError::DivisionByZero),
    }
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
    fn fold_now_produces_timestamptz_once_only() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::Function {
            name: "now".into(),
            args: vec![],
            distinct: false,
        };
        // `now` is volatile, so a reusable plan must never carry its value.
        assert!(
            matches!(fold_constant(&expr, &registry), Ok(None)),
            "now() must not fold into a reusable plan"
        );
        let val = fold_constant_scoped(&expr, &registry, FoldScope::Once)
            .expect("fold must not error")
            .expect("now() must fold for a single execution");
        match val {
            SqlValue::Timestamptz(dt) => {
                // Sanity: must not be epoch (year 1970).
                assert!(dt.micros > 0, "expected post-epoch timestamp, got micros=0");
            }
            other => panic!("expected SqlValue::Timestamptz, got {other:?}"),
        }
    }

    #[test]
    fn fold_current_timestamp_produces_timestamptz_once_only() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::Function {
            name: "current_timestamp".into(),
            args: vec![],
            distinct: false,
        };
        assert!(
            matches!(fold_constant(&expr, &registry), Ok(None)),
            "current_timestamp must not fold into a reusable plan"
        );
        assert!(matches!(
            fold_constant_scoped(&expr, &registry, FoldScope::Once),
            Ok(Some(SqlValue::Timestamptz(_)))
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
        assert!(
            fold_constant(&expr, &registry)
                .expect("fold must not error")
                .is_none()
        );
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
            Ok(Some(SqlValue::Array(vec![
                SqlValue::String("public".into()),
                SqlValue::Int(42),
            ])))
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
        assert_eq!(fold_constant(&expr, &registry), Ok(Some(SqlValue::Int(5))));
    }

    #[test]
    fn fold_column_ref_returns_none() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::Column {
            table: None,
            name: "name".into(),
        };
        assert!(
            fold_constant(&expr, &registry)
                .expect("fold must not error")
                .is_none()
        );
    }

    #[test]
    fn fold_decimal_literal() {
        let registry = FunctionRegistry::new();
        let d = rust_decimal::Decimal::new(12345, 2); // 123.45
        let expr = SqlExpr::Literal(SqlValue::Decimal(d));
        assert_eq!(
            fold_constant(&expr, &registry),
            Ok(Some(SqlValue::Decimal(d)))
        );
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
            Ok(Some(SqlValue::Decimal(expected)))
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
        assert_eq!(
            fold_constant(&expr, &registry),
            Ok(Some(SqlValue::Decimal(-d)))
        );
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
        let v = fold_constant(&expr, &registry).expect("fold must not error");
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
        let v = fold_constant(&expr, &registry).expect("fold must not error");
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
            Ok(Some(SqlValue::Decimal(expected)))
        );
    }
}
