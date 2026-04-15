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
        SqlExpr::UnaryOp {
            op: UnaryOp::Neg,
            expr,
        } => match fold_constant(expr, registry)? {
            SqlValue::Int(i) => Some(SqlValue::Int(-i)),
            SqlValue::Float(f) => Some(SqlValue::Float(-f)),
            _ => None,
        },
        SqlExpr::BinaryOp { left, op, right } => {
            let l = fold_constant(left, registry)?;
            let r = fold_constant(right, registry)?;
            fold_binary(l, *op, r)
        }
        SqlExpr::Function { name, args, .. } => fold_function_call(name, args, registry),
        _ => None,
    }
}

fn fold_binary(l: SqlValue, op: BinaryOp, r: SqlValue) -> Option<SqlValue> {
    Some(match (l, op, r) {
        (SqlValue::Int(a), BinaryOp::Add, SqlValue::Int(b)) => SqlValue::Int(a + b),
        (SqlValue::Int(a), BinaryOp::Sub, SqlValue::Int(b)) => SqlValue::Int(a - b),
        (SqlValue::Int(a), BinaryOp::Mul, SqlValue::Int(b)) => SqlValue::Int(a * b),
        (SqlValue::Float(a), BinaryOp::Add, SqlValue::Float(b)) => SqlValue::Float(a + b),
        (SqlValue::Float(a), BinaryOp::Sub, SqlValue::Float(b)) => SqlValue::Float(a - b),
        (SqlValue::Float(a), BinaryOp::Mul, SqlValue::Float(b)) => SqlValue::Float(a * b),
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

    let result = nodedb_query::functions::eval_function(name, &folded_args);
    Some(ndb_to_sql_value(result))
}

fn sql_to_ndb_value(v: SqlValue) -> Value {
    match v {
        SqlValue::Null => Value::Null,
        SqlValue::Bool(b) => Value::Bool(b),
        SqlValue::Int(i) => Value::Integer(i),
        SqlValue::Float(f) => Value::Float(f),
        SqlValue::String(s) => Value::String(s),
        SqlValue::Bytes(b) => Value::Bytes(b),
        SqlValue::Array(a) => Value::Array(a.into_iter().map(sql_to_ndb_value).collect()),
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
        Value::DateTime(dt) => SqlValue::String(dt.to_iso8601()),
        Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => SqlValue::String(s),
        Value::Duration(d) => SqlValue::String(d.to_human()),
        Value::Decimal(d) => SqlValue::String(d.to_string()),
        // Structured and opaque types collapse to Null — callers that
        // need these go through the runtime expression path, not folding.
        Value::Object(_)
        | Value::Geometry(_)
        | Value::Set(_)
        | Value::Range { .. }
        | Value::Record { .. } => SqlValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_now_produces_non_epoch_string() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::Function {
            name: "now".into(),
            args: vec![],
            distinct: false,
        };
        let val = fold_constant(&expr, &registry).expect("now() should fold");
        match val {
            SqlValue::String(s) => {
                assert!(!s.starts_with("1970"), "got {s}");
                assert!(s.contains('T'), "not ISO-8601: {s}");
            }
            other => panic!("expected string, got {other:?}"),
        }
    }

    #[test]
    fn fold_current_timestamp() {
        let registry = FunctionRegistry::new();
        let expr = SqlExpr::Function {
            name: "current_timestamp".into(),
            args: vec![],
            distinct: false,
        };
        assert!(matches!(
            fold_constant(&expr, &registry),
            Some(SqlValue::String(_))
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
}
