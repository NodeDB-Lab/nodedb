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

use nodedb_crs::CrsContext;
use nodedb_query::numeric_div::{numeric_div, numeric_rem};
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
pub fn fold_constant_default(expr: &SqlExpr, crs: &CrsContext<'_>) -> FoldResult {
    fold_constant(expr, default_registry(), crs)
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
/// for what each outcome means. Volatile calls are never folded. Geo
/// functions resolve SRIDs through `crs`.
pub fn fold_constant(
    expr: &SqlExpr,
    registry: &FunctionRegistry,
    crs: &CrsContext<'_>,
) -> FoldResult {
    fold_constant_scoped(expr, registry, FoldScope::Reuse, crs)
}

/// Fold a `SqlExpr` under an explicit [`FoldScope`].
pub fn fold_constant_scoped(
    expr: &SqlExpr,
    registry: &FunctionRegistry,
    scope: FoldScope,
    crs: &CrsContext<'_>,
) -> FoldResult {
    match expr {
        SqlExpr::Literal(v) => Ok(Some(v.clone())),
        SqlExpr::ArrayLiteral(items) => {
            let mut folded = Vec::with_capacity(items.len());
            for item in items {
                match fold_constant_scoped(item, registry, scope, crs)? {
                    Some(v) => folded.push(v),
                    None => return Ok(None),
                }
            }
            Ok(Some(SqlValue::Array(folded)))
        }
        SqlExpr::UnaryOp {
            op: UnaryOp::Neg,
            expr,
        } => Ok(match fold_constant_scoped(expr, registry, scope, crs)? {
            // `-i64::MIN` is past the range, as at run time.
            Some(SqlValue::Int(i)) => Some(SqlValue::Int(
                i.checked_neg()
                    .ok_or_else(|| overflowed(BIGINT_OUT_OF_RANGE))?,
            )),
            Some(SqlValue::Float(f)) => Some(SqlValue::Float(-f)),
            // `9223372036854775808` is past `i64::MAX`, so its literal is a
            // `Decimal`. Negated it is `i64::MIN`, an integer like every
            // other in range.
            Some(SqlValue::Decimal(d))
                if d.scale() == 0 && -d == rust_decimal::Decimal::from(i64::MIN) =>
            {
                Some(SqlValue::Int(i64::MIN))
            }
            Some(SqlValue::Decimal(d)) => Some(SqlValue::Decimal(-d)),
            _ => None,
        }),
        SqlExpr::BinaryOp { left, op, right } => {
            let (Some(l), Some(r)) = (
                fold_constant_scoped(left, registry, scope, crs)?,
                fold_constant_scoped(right, registry, scope, crs)?,
            ) else {
                return Ok(None);
            };
            fold_binary(l, *op, r)
        }
        SqlExpr::Function { name, args, .. } => {
            fold_function_call_scoped(name, args, registry, scope, crs)
        }
        SqlExpr::Cast { expr, to_type } => {
            match fold_constant_scoped(expr, registry, scope, crs)? {
                Some(inner) => fold_cast(inner, to_type),
                None => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

/// Fold a CAST at plan time. Only applies when the inner expression is already
/// a constant. The `to_type` string comes from sqlparser's `format!("{data_type}")`
/// output, so parameterised types like `NUMERIC(5,1)` must be matched by prefix.
///
/// A cast to an integer, float, text, boolean or numeric type runs the
/// row-scope cast ([`nodedb_query::cast::eval_cast`]), so a folded cast and
/// a run-time cast give one answer and fail with one error.
fn fold_cast(inner: SqlValue, to_type: &str) -> FoldResult {
    use nodedb_query::CastType;

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
        "REGCLASS" => Ok(None),
        // `JSON` / `JSONB` — JSON values live internally as their text form
        // in `SqlValue::String`; the write path parses JSON-looking strings
        // into document structure. The cast elides to the inner value's JSON
        // text (mirrors the `::tsvector` / `::tsquery` elision in the resolver).
        "JSON" | "JSONB" => Ok(match inner {
            SqlValue::String(s) => Some(SqlValue::String(s)),
            SqlValue::Int(i) => Some(SqlValue::String(i.to_string())),
            SqlValue::Float(f) => Some(SqlValue::String(f.to_string())),
            SqlValue::Decimal(d) => Some(SqlValue::String(d.to_string())),
            SqlValue::Bool(b) => Some(SqlValue::String(b.to_string())),
            SqlValue::Null => Some(SqlValue::Null),
            _ => None,
        }),
        _ => {
            let cast_type = if base == "BPCHAR" {
                Some(CastType::String)
            } else {
                CastType::from_type_name(to_type)
            };
            match cast_type {
                Some(
                    cast_type @ (CastType::Int
                    | CastType::Int2
                    | CastType::Int4
                    | CastType::Float
                    | CastType::String
                    | CastType::Bool
                    | CastType::Decimal(_)),
                ) => match nodedb_query::cast::eval_cast(&sql_to_ndb_value(inner), &cast_type) {
                    Ok(value) => Ok(Some(ndb_to_sql_value(value))),
                    Err(e) => eval_failure(e),
                },
                _ => Ok(None),
            }
        }
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

/// PostgreSQL's message for a `bigint` result past the `i64` range.
const BIGINT_OUT_OF_RANGE: &str = "bigint out of range";

/// PostgreSQL's message for a numeric result past the `Decimal` range.
const NUMERIC_OUT_OF_RANGE: &str = "value overflows numeric format";

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
        (SqlValue::Int(a), BinaryOp::Add, SqlValue::Int(b)) => SqlValue::Int(
            a.checked_add(b)
                .ok_or_else(|| overflowed(BIGINT_OUT_OF_RANGE))?,
        ),
        (SqlValue::Int(a), BinaryOp::Sub, SqlValue::Int(b)) => SqlValue::Int(
            a.checked_sub(b)
                .ok_or_else(|| overflowed(BIGINT_OUT_OF_RANGE))?,
        ),
        (SqlValue::Int(a), BinaryOp::Mul, SqlValue::Int(b)) => SqlValue::Int(
            a.checked_mul(b)
                .ok_or_else(|| overflowed(BIGINT_OUT_OF_RANGE))?,
        ),
        // Int division and modulo. The zero divisor is already refused above,
        // so `None` here is `i64::MIN / -1`, which overflows, and
        // `i64::MIN % -1`, whose remainder is 0.
        (SqlValue::Int(a), BinaryOp::Div, SqlValue::Int(b)) => SqlValue::Int(
            a.checked_div(b)
                .ok_or_else(|| overflowed(BIGINT_OUT_OF_RANGE))?,
        ),
        (SqlValue::Int(a), BinaryOp::Mod, SqlValue::Int(b)) => {
            SqlValue::Int(a.checked_rem(b).unwrap_or(0))
        }
        // Float × Float arithmetic.
        (SqlValue::Float(a), BinaryOp::Add, SqlValue::Float(b)) => SqlValue::Float(a + b),
        (SqlValue::Float(a), BinaryOp::Sub, SqlValue::Float(b)) => SqlValue::Float(a - b),
        (SqlValue::Float(a), BinaryOp::Mul, SqlValue::Float(b)) => SqlValue::Float(a * b),
        (SqlValue::Float(a), BinaryOp::Div, SqlValue::Float(b)) => SqlValue::Float(a / b),
        (SqlValue::Float(a), BinaryOp::Mod, SqlValue::Float(b)) => SqlValue::Float(a % b),
        // Decimal × Decimal arithmetic.
        (SqlValue::Decimal(a), BinaryOp::Add, SqlValue::Decimal(b)) => SqlValue::Decimal(
            a.checked_add(b)
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Sub, SqlValue::Decimal(b)) => SqlValue::Decimal(
            a.checked_sub(b)
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Mul, SqlValue::Decimal(b)) => SqlValue::Decimal(
            a.checked_mul(b)
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
        ),
        // Numeric division takes PostgreSQL's result scale.
        (SqlValue::Decimal(a), BinaryOp::Div, SqlValue::Decimal(b)) => {
            SqlValue::Decimal(numeric_div(a, b).ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?)
        }
        (SqlValue::Decimal(a), BinaryOp::Mod, SqlValue::Decimal(b)) => {
            SqlValue::Decimal(numeric_rem(a, b).ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?)
        }
        // Decimal × Int widening (Int promotes to Decimal).
        (SqlValue::Decimal(a), BinaryOp::Add, SqlValue::Int(b)) => SqlValue::Decimal(
            a.checked_add(rust_decimal::Decimal::from(b))
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
        ),
        (SqlValue::Int(a), BinaryOp::Add, SqlValue::Decimal(b)) => SqlValue::Decimal(
            rust_decimal::Decimal::from(a)
                .checked_add(b)
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Sub, SqlValue::Int(b)) => SqlValue::Decimal(
            a.checked_sub(rust_decimal::Decimal::from(b))
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
        ),
        (SqlValue::Int(a), BinaryOp::Sub, SqlValue::Decimal(b)) => SqlValue::Decimal(
            rust_decimal::Decimal::from(a)
                .checked_sub(b)
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Mul, SqlValue::Int(b)) => SqlValue::Decimal(
            a.checked_mul(rust_decimal::Decimal::from(b))
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
        ),
        (SqlValue::Int(a), BinaryOp::Mul, SqlValue::Decimal(b)) => SqlValue::Decimal(
            rust_decimal::Decimal::from(a)
                .checked_mul(b)
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Div, SqlValue::Int(b)) => SqlValue::Decimal(
            numeric_div(a, rust_decimal::Decimal::from(b))
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
        ),
        (SqlValue::Int(a), BinaryOp::Div, SqlValue::Decimal(b)) => SqlValue::Decimal(
            numeric_div(rust_decimal::Decimal::from(a), b)
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
        ),
        (SqlValue::Decimal(a), BinaryOp::Mod, SqlValue::Int(b)) => SqlValue::Decimal(
            numeric_rem(a, rust_decimal::Decimal::from(b))
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
        ),
        (SqlValue::Int(a), BinaryOp::Mod, SqlValue::Decimal(b)) => SqlValue::Decimal(
            numeric_rem(rust_decimal::Decimal::from(a), b)
                .ok_or_else(|| overflowed(NUMERIC_OUT_OF_RANGE))?,
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
pub fn fold_function_call(
    name: &str,
    args: &[SqlExpr],
    registry: &FunctionRegistry,
    crs: &CrsContext<'_>,
) -> FoldResult {
    fold_function_call_scoped(name, args, registry, FoldScope::Reuse, crs)
}

/// Fold a function call under an explicit [`FoldScope`].
pub fn fold_function_call_scoped(
    name: &str,
    args: &[SqlExpr],
    registry: &FunctionRegistry,
    scope: FoldScope,
    crs: &CrsContext<'_>,
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
        match fold_constant_scoped(arg, registry, scope, crs)? {
            Some(v) => folded_args.push(sql_to_ndb_value(v)),
            None => return Ok(None),
        }
    }

    // A registered function whose arguments all folded is fully determined
    // here, so a failure is the answer — not a reason to defer. Deferring it
    // reached the row-scope evaluator only when the statement had a FROM
    // clause; `SELECT mod(5, 0)` has no row scope and became NULL.
    match nodedb_query::functions::eval_function(&name_lower, &folded_args, crs) {
        Ok(result) => Ok(Some(ndb_to_sql_value(result))),
        Err(e) => eval_failure(e),
    }
}

/// The fold outcome of a constant evaluation that failed. Exhaustive on
/// purpose: a new `EvalError` variant must be classified here rather than
/// defaulting to "defer to a runtime that may not exist".
fn eval_failure(e: nodedb_query::EvalError) -> FoldResult {
    match e {
        nodedb_query::EvalError::DivisionByZero => Err(SqlError::DivisionByZero),
        e @ (nodedb_query::EvalError::VectorDimensionMismatch { .. }
        | nodedb_query::EvalError::ArgumentType { .. }
        | nodedb_query::EvalError::InvalidJsonPath { .. }
        | nodedb_query::EvalError::NumericOverflow { .. }
        | nodedb_query::EvalError::Crs { .. }
        | nodedb_query::EvalError::MixedSrid { .. }
        | nodedb_query::EvalError::Transform { .. }
        | nodedb_query::EvalError::Geometry { .. }
        | nodedb_query::EvalError::ArgumentValue { .. }) => Err(SqlError::DataException {
            detail: e.to_string(),
        }),
        nodedb_query::EvalError::MathDomain(fault) => Err(SqlError::MathDomain(fault)),
        // A registered name some other evaluator owns (a search score):
        // not foldable here. Its search plan serves it, or the plan-time
        // search-scope pass refuses it.
        nodedb_query::EvalError::UnknownFunction { .. } => Ok(None),
        nodedb_query::EvalError::InvalidTextRepresentation { type_name, text } => {
            Err(SqlError::InvalidTextRepresentation { type_name, text })
        }
        nodedb_query::EvalError::InvalidDatetimeFormat { type_name, text } => {
            Err(SqlError::InvalidDatetimeFormat { type_name, text })
        }
        e @ (nodedb_query::EvalError::NumericFieldOverflow { .. }
        | nodedb_query::EvalError::OutOfRange(_)
        | nodedb_query::EvalError::NumericSpecialValue { .. }) => Err(SqlError::ConstantOverflow {
            detail: e.to_string(),
        }),
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
        SqlValue::Uuid(u) => Value::Uuid(u),
        SqlValue::Array(a) => Value::Array(a.into_iter().map(sql_to_ndb_value).collect()),
        SqlValue::Object(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(key, value)| (key, sql_to_ndb_value(value)))
                .collect(),
        ),
        SqlValue::Timestamp(dt) => Value::NaiveDateTime(dt),
        SqlValue::Timestamptz(dt) => Value::DateTime(dt),
        SqlValue::Geometry(g) => Value::Geometry(g),
        SqlValue::Duration(d) => Value::Duration(d),
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
        Value::Uuid(u) => SqlValue::Uuid(u),
        Value::Ulid(s) | Value::Regex(s) => SqlValue::String(s),
        Value::Duration(d) => SqlValue::String(d.to_human()),
        Value::Decimal(d) => SqlValue::Decimal(d),
        // A geometry keeps its SRID, Z and M through the fold.
        Value::Geometry(g) => SqlValue::Geometry(g),
        // Object results (ST_GeoHashDecode) reach the client as a
        // JSON-encoded string column.
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

    fn crs() -> CrsContext<'static> {
        CrsContext::builtin_only()
    }

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
            matches!(fold_constant(&expr, &registry, &crs()), Ok(None)),
            "now() must not fold into a reusable plan"
        );
        let val = fold_constant_scoped(&expr, &registry, FoldScope::Once, &crs())
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

    /// `-9223372036854775808` folds to the integer `i64::MIN`, and a negated
    /// literal past it stays an exact `Decimal`.
    #[test]
    fn negated_integer_literal_past_i64_max_stays_exact() {
        let registry = FunctionRegistry::new();
        let neg = |digits: &str| SqlExpr::UnaryOp {
            op: UnaryOp::Neg,
            expr: Box::new(SqlExpr::Literal(SqlValue::Decimal(
                rust_decimal::Decimal::from_str_exact(digits).unwrap(),
            ))),
        };
        assert_eq!(
            fold_constant(&neg("9223372036854775808"), &registry, &crs()).unwrap(),
            Some(SqlValue::Int(i64::MIN))
        );
        assert_eq!(
            fold_constant(&neg("18446744073709551615"), &registry, &crs()).unwrap(),
            Some(SqlValue::Decimal(-rust_decimal::Decimal::from(u64::MAX)))
        );
    }

    /// A constant call whose argument the function cannot compute on fails
    /// the statement at plan time instead of folding to NULL.
    #[test]
    fn fold_of_a_malformed_json_path_is_a_data_exception() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::Function {
            name: "doc_get".into(),
            args: vec![
                SqlExpr::Literal(SqlValue::String("{\"a\":1}".into())),
                SqlExpr::Literal(SqlValue::String("$..a".into())),
            ],
            distinct: false,
        };
        let err = fold_constant(&expr, &registry, &crs()).expect_err("malformed path must fail");
        assert!(
            matches!(&err, SqlError::DataException { detail } if detail.contains("invalid JSONPath")),
            "{err:?}"
        );
    }

    /// A constant `sqrt` or `ln` outside its domain keeps the fault, so the
    /// statement fails with the subclass PostgreSQL gives it.
    #[test]
    fn fold_of_a_math_domain_fault_keeps_its_class() {
        let registry = FunctionRegistry::new();
        let call = |name: &str, arg: i64| SqlExpr::Function {
            name: name.into(),
            args: vec![SqlExpr::Literal(SqlValue::Int(arg))],
            distinct: false,
        };
        assert_eq!(
            fold_constant(&call("sqrt", -1), &registry, &crs()),
            Err(SqlError::MathDomain(
                nodedb_query::expr::MathDomain::SqrtOfNegative
            ))
        );
        assert_eq!(
            fold_constant(&call("ln", 0), &registry, &crs()),
            Err(SqlError::MathDomain(
                nodedb_query::expr::MathDomain::LogOfZero
            ))
        );
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
            matches!(fold_constant(&expr, &registry, &crs()), Ok(None)),
            "current_timestamp must not fold into a reusable plan"
        );
        assert!(matches!(
            fold_constant_scoped(&expr, &registry, FoldScope::Once, &crs()),
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
            fold_constant(&expr, &registry, &crs())
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
            fold_constant(&expr, &registry, &crs()),
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
        assert_eq!(
            fold_constant(&expr, &registry, &crs()),
            Ok(Some(SqlValue::Int(5)))
        );
    }

    #[test]
    fn fold_column_ref_returns_none() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::Column {
            table: None,
            name: "name".into(),
        };
        assert!(
            fold_constant(&expr, &registry, &crs())
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
            fold_constant(&expr, &registry, &crs()),
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
            fold_constant(&expr, &registry, &crs()),
            Ok(Some(SqlValue::Decimal(expected)))
        );
    }

    /// A constant numeric division folds at PostgreSQL's result scale.
    #[test]
    fn fold_decimal_division_takes_the_postgres_scale() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(SqlValue::Int(2))),
            op: BinaryOp::Div,
            right: Box::new(SqlExpr::Literal(SqlValue::Decimal(
                rust_decimal::Decimal::new(20, 1),
            ))),
        };
        let Ok(Some(SqlValue::Decimal(quotient))) = fold_constant(&expr, &registry, &crs()) else {
            panic!("2 / 2.0 folds to a Decimal");
        };
        assert_eq!(quotient.to_string(), "1.00000000000000000000");
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
            fold_constant(&expr, &registry, &crs()),
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
        let v = fold_constant(&expr, &registry, &crs()).expect("fold must not error");
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
        let v = fold_constant(&expr, &registry, &crs()).expect("fold must not error");
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
            fold_constant(&expr, &registry, &crs()),
            Ok(Some(SqlValue::Decimal(expected)))
        );
    }

    fn int_op(a: i64, op: BinaryOp, b: i64) -> FoldResult {
        let expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(SqlValue::Int(a))),
            op,
            right: Box::new(SqlExpr::Literal(SqlValue::Int(b))),
        };
        fold_constant(&expr, &FunctionRegistry::new(), &crs())
    }

    /// `i64::MIN % -1` is 0 and `i64::MIN / -1` is out of range, as in
    /// PostgreSQL and at run time.
    #[test]
    fn fold_integer_minimum_by_minus_one() {
        assert_eq!(
            int_op(i64::MIN, BinaryOp::Mod, -1),
            Ok(Some(SqlValue::Int(0)))
        );
        let err = int_op(i64::MIN, BinaryOp::Div, -1).unwrap_err();
        assert!(matches!(err, SqlError::ConstantOverflow { .. }));
        assert_eq!(err.to_string(), "bigint out of range");
        let err = int_op(i64::MAX, BinaryOp::Add, 1).unwrap_err();
        assert_eq!(err.to_string(), "bigint out of range");
        let negate = SqlExpr::UnaryOp {
            op: UnaryOp::Neg,
            expr: Box::new(SqlExpr::Literal(SqlValue::Int(i64::MIN))),
        };
        assert!(matches!(
            fold_constant(&negate, &FunctionRegistry::new(), &crs()),
            Err(SqlError::ConstantOverflow { .. })
        ));
    }

    fn cast(inner: SqlValue, to_type: &str) -> FoldResult {
        let expr = SqlExpr::Cast {
            expr: Box::new(SqlExpr::Literal(inner)),
            to_type: to_type.into(),
        };
        fold_constant(&expr, &FunctionRegistry::new(), &crs())
    }

    /// A folded cast gives the run-time cast's answer and error.
    #[test]
    fn fold_cast_matches_the_runtime_cast() {
        let dec = |t: &str| SqlValue::Decimal(rust_decimal::Decimal::from_str_exact(t).unwrap());
        assert_eq!(cast(dec("2.5"), "BIGINT"), Ok(Some(SqlValue::Int(3))));
        assert_eq!(
            cast(SqlValue::Float(2.5), "INT"),
            Ok(Some(SqlValue::Int(2)))
        );
        assert_eq!(cast(SqlValue::Null, "TEXT"), Ok(Some(SqlValue::Null)));
        assert_eq!(cast(SqlValue::Null, "BOOLEAN"), Ok(Some(SqlValue::Null)));
        assert_eq!(
            cast(SqlValue::String("tr".into()), "BOOL"),
            Ok(Some(SqlValue::Bool(true)))
        );
        assert_eq!(
            cast(SqlValue::String("1e5".into()), "NUMERIC"),
            Ok(Some(dec("100000")))
        );
        assert_eq!(cast(dec("12.345"), "NUMERIC(5,2)"), Ok(Some(dec("12.35"))));
        assert!(matches!(
            cast(SqlValue::String("3.5".into()), "BIGINT"),
            Err(SqlError::InvalidTextRepresentation {
                type_name: "bigint",
                ..
            })
        ));
        assert!(matches!(
            cast(SqlValue::String("maybe".into()), "BOOLEAN"),
            Err(SqlError::InvalidTextRepresentation {
                type_name: "boolean",
                ..
            })
        ));
        let err = cast(SqlValue::Float(f64::NAN), "NUMERIC").unwrap_err();
        assert!(matches!(err, SqlError::ConstantOverflow { .. }));
        assert_eq!(err.to_string(), "cannot convert NaN to numeric");
        assert!(matches!(
            cast(SqlValue::Float(1e300), "BIGINT"),
            Err(SqlError::ConstantOverflow { .. })
        ));
        assert_eq!(
            cast(SqlValue::Int(32767), "SMALLINT"),
            Ok(Some(SqlValue::Int(32767)))
        );
        let err = cast(SqlValue::Int(32768), "SMALLINT").unwrap_err();
        assert_eq!(err.to_string(), "smallint out of range");
        let err = cast(SqlValue::Int(1 << 31), "INTEGER").unwrap_err();
        assert_eq!(err.to_string(), "integer out of range");
    }
}
