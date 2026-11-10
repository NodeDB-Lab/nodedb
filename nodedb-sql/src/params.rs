// SPDX-License-Identifier: Apache-2.0

//! AST-level parameter binding for prepared statements.
//!
//! Every `Value::Placeholder("$N")` in a parsed statement is rewritten to
//! a concrete literal value via sqlparser's `VisitorMut`. The visitor
//! traverses every expression position the AST defines — CTE bodies,
//! window specs, `Update.from/returning/limit`, `Delete.returning`,
//! `Insert.on_conflict`, `Expr::Array` elements, `Expr::AnyOp`/`AllOp`
//! right-hand sides, `Expr::Interval`, and any variant sqlparser adds in
//! the future — without us maintaining a hand-written walker.
//!
//! # Why a visitor, not a hand-written walker
//!
//! The previous implementation was a recursive match on ~20 `Expr` /
//! `Statement` / `Query` variants. Every new sqlparser variant it didn't
//! enumerate was a silent bug: placeholders survived into the planner and
//! surfaced as "unsupported expression: $1" in the resolver. The walker
//! was opt-in where it had to be exhaustive to be correct. `VisitorMut`
//! moves the exhaustiveness burden to sqlparser itself.

use core::ops::ControlFlow;

use sqlparser::ast::{Expr, Statement, Value, ValueWithSpan, VisitMut, VisitorMut};

use crate::params_typed::placeholder_to_typed;

/// Parameter value for AST substitution.
///
/// Converted from pgwire binary parameters + type info in the Control Plane.
#[derive(Debug, Clone)]
pub enum ParamValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    /// Exact arbitrary-precision decimal from a NUMERIC/DECIMAL pgwire parameter.
    Decimal(rust_decimal::Decimal),
    Text(String),
    /// Naive (no-timezone) timestamp from a TIMESTAMP pgwire parameter.
    Timestamp(nodedb_types::datetime::NdbDateTime),
    /// UTC (timezone-aware) timestamp from a TIMESTAMPTZ pgwire parameter.
    Timestamptz(nodedb_types::datetime::NdbDateTime),
    /// UUID from a UUID pgwire parameter, as its canonical text: lowercase,
    /// hyphenated.
    Uuid(String),
    /// Raw bytes from a BYTEA pgwire parameter.
    Bytes(Vec<u8>),
    /// A duration from an INTERVAL pgwire parameter.
    Duration(nodedb_types::NdbDuration),
    /// The elements of an array pgwire parameter, or of a JSON array.
    Array(Vec<ParamValue>),
    /// The fields of a JSON / JSONB object parameter, in written order.
    Object(Vec<(String, ParamValue)>),
}

impl ParamValue {
    /// The typed parameter a value of a type SQL text holds only as a
    /// string binds as: a UUID, an instant, bytes, or a decimal. `None` for
    /// every other value, whose plain SQL literal already resolves to it.
    pub fn typed_from_value(value: &nodedb_types::Value) -> Option<Self> {
        use nodedb_types::Value;
        match value {
            Value::Uuid(u) => nodedb_types::id_gen::canonical_uuid(u).map(Self::Uuid),
            Value::DateTime(at) => Some(Self::Timestamptz(*at)),
            Value::NaiveDateTime(at) => Some(Self::Timestamp(*at)),
            Value::Bytes(b) => Some(Self::Bytes(b.clone())),
            Value::Decimal(d) => Some(Self::Decimal(*d)),
            Value::Duration(d) => Some(Self::Duration(*d)),
            _ => None,
        }
    }
}

/// Substitute all `$N` placeholders in a parsed statement with concrete values.
pub fn bind_params(stmt: &mut Statement, params: &[ParamValue]) {
    if params.is_empty() {
        return;
    }
    let mut binder = ParamBinder { params };
    let _ = stmt.visit(&mut binder);
}

/// Visitor that rewrites every `Value::Placeholder("$N")` it encounters.
///
/// sqlparser's `VisitMut` impls take us into every expression position of
/// every `Expr`, `Statement`, `Query`, `SetExpr`, `TableFactor`, etc. —
/// so we only care about the leaf: the `Value` itself.
struct ParamBinder<'a> {
    params: &'a [ParamValue],
}

impl VisitorMut for ParamBinder<'_> {
    type Break = ();

    /// A placeholder bound to a timestamp, UUID or bytes parameter becomes a
    /// typed string (`UUID '...'`), so the resolver reads the typed value,
    /// never its text. The visitor then descends into the typed string,
    /// whose value holds no placeholder.
    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(value) = expr
            && let Value::Placeholder(p) = &value.value
            && let Some(typed) = placeholder_to_typed(p, self.params, value.span)
        {
            *expr = typed;
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<Self::Break> {
        // Only the literal is rewritten; `value.span` keeps pointing at the
        // placeholder token, so a later diagnostic still names the source
        // position the client wrote rather than a synthetic one.
        if let Value::Placeholder(p) = &value.value
            && let Some(v) = placeholder_to_value(p, self.params)
        {
            value.value = v;
        }
        ControlFlow::Continue(())
    }
}

fn placeholder_to_value(placeholder: &str, params: &[ParamValue]) -> Option<Value> {
    let idx_str = placeholder.strip_prefix('$')?;
    let idx: usize = idx_str.parse().ok()?;
    let param = params.get(idx.checked_sub(1)?)?;
    Some(match param {
        ParamValue::Null => Value::Null,
        ParamValue::Bool(true) => Value::Boolean(true),
        ParamValue::Bool(false) => Value::Boolean(false),
        ParamValue::Int64(n) => Value::Number(n.to_string(), false),
        // An exponent keeps the number a float. A non-finite float is the
        // number text `NaN`, `inf` or `-inf`, which `convert_value` reads as
        // a float. An expression position takes the cast form instead, see
        // `placeholder_to_typed`.
        ParamValue::Float64(f) => Value::Number(format!("{f:e}"), false),
        // A position that holds no expression takes the digits. An
        // expression position takes the typed form, see
        // `placeholder_to_typed`.
        ParamValue::Decimal(d) => Value::Number(d.to_string(), false),
        ParamValue::Text(s) => Value::SingleQuotedString(s.clone()),
        // A typed parameter in an expression position is a typed string, see
        // `placeholder_to_typed`. A position that holds no expression takes
        // its PostgreSQL text.
        ParamValue::Timestamp(dt) | ParamValue::Timestamptz(dt) => {
            Value::SingleQuotedString(dt.to_iso8601())
        }
        ParamValue::Uuid(u) => Value::SingleQuotedString(u.clone()),
        ParamValue::Bytes(b) => {
            Value::SingleQuotedString(nodedb_types::bytea_text::format_bytea_hex(b))
        }
        ParamValue::Duration(d) => {
            Value::SingleQuotedString(crate::params_composite::duration_text(d))
        }
        // A position that holds no expression takes the JSON text.
        ParamValue::Array(_) | ParamValue::Object(_) => {
            Value::SingleQuotedString(crate::params_composite::param_json_text(param))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;

    fn bind_and_format(sql: &str, params: &[ParamValue]) -> String {
        let mut stmts = parse_sql(sql).unwrap();
        for stmt in &mut stmts {
            bind_params(stmt, params);
        }
        stmts
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    }

    #[test]
    fn bind_select_where() {
        let result = bind_and_format(
            "SELECT * FROM users WHERE id = $1",
            &[ParamValue::Int64(42)],
        );
        assert!(result.contains("id = 42"), "got: {result}");
    }

    #[test]
    fn bind_string_param() {
        let result = bind_and_format(
            "SELECT * FROM users WHERE name = $1",
            &[ParamValue::Text("alice".into())],
        );
        assert!(result.contains("name = 'alice'"), "got: {result}");
    }

    #[test]
    fn bind_null_param() {
        let result = bind_and_format("SELECT * FROM users WHERE name = $1", &[ParamValue::Null]);
        assert!(result.contains("name = NULL"), "got: {result}");
    }

    #[test]
    fn bind_multiple_params() {
        let result = bind_and_format(
            "SELECT * FROM users WHERE age > $1 AND name = $2",
            &[ParamValue::Int64(18), ParamValue::Text("bob".into())],
        );
        assert!(result.contains("age > 18"), "got: {result}");
        assert!(result.contains("name = 'bob'"), "got: {result}");
    }

    #[test]
    fn bind_insert_values() {
        let result = bind_and_format(
            "INSERT INTO users (id, name) VALUES ($1, $2)",
            &[ParamValue::Int64(1), ParamValue::Text("eve".into())],
        );
        assert!(result.contains("1, 'eve'"), "got: {result}");
    }

    #[test]
    fn bind_bool_param() {
        let result = bind_and_format(
            "SELECT * FROM users WHERE active = $1",
            &[ParamValue::Bool(true)],
        );
        assert!(result.contains("active = true"), "got: {result}");
    }

    #[test]
    fn no_params_noop() {
        let result = bind_and_format("SELECT 1", &[]);
        assert!(result.contains("SELECT 1"));
    }

    #[test]
    fn bind_cte_body_placeholders() {
        let result = bind_and_format(
            "WITH x AS (SELECT $1 AS v) SELECT v FROM x",
            &[ParamValue::Int64(42)],
        );
        assert!(
            result.contains("SELECT 42"),
            "CTE body placeholder not substituted: {result}"
        );
        assert!(!result.contains("$1"), "placeholder survived: {result}");
    }

    #[test]
    fn bind_recursive_cte_placeholders() {
        let result = bind_and_format(
            "WITH RECURSIVE chain AS (SELECT $1 AS id UNION ALL SELECT chain.id + 1 FROM chain WHERE chain.id < $2) SELECT * FROM chain",
            &[ParamValue::Int64(1), ParamValue::Int64(10)],
        );
        assert!(
            !result.contains("$1") && !result.contains("$2"),
            "got: {result}"
        );
    }

    #[test]
    fn bind_array_elements() {
        let result = bind_and_format(
            "SELECT ARRAY[$1, $2, $3]",
            &[
                ParamValue::Int64(1),
                ParamValue::Int64(2),
                ParamValue::Int64(3),
            ],
        );
        assert!(
            !result.contains("$1"),
            "array placeholder survived: {result}"
        );
        assert!(
            result.contains('1') && result.contains('2') && result.contains('3'),
            "got: {result}"
        );
    }

    #[test]
    fn bind_any_op_rhs() {
        let result = bind_and_format(
            "SELECT * FROM t WHERE id = ANY($1)",
            &[ParamValue::Text("{a,b}".into())],
        );
        assert!(!result.contains("$1"), "got: {result}");
    }

    #[test]
    fn bind_all_op_rhs() {
        let result = bind_and_format(
            "SELECT * FROM t WHERE id = ALL($1)",
            &[ParamValue::Text("{a,b}".into())],
        );
        assert!(!result.contains("$1"), "got: {result}");
    }

    #[test]
    fn bind_update_returning() {
        let result = bind_and_format(
            "UPDATE t SET n = 1 WHERE id = $1 RETURNING $2 AS tag",
            &[ParamValue::Int64(7), ParamValue::Text("note".into())],
        );
        assert!(result.contains("id = 7"), "got: {result}");
        assert!(result.contains("'note'"), "got: {result}");
        assert!(!result.contains("$2"), "got: {result}");
    }

    #[test]
    fn bind_delete_returning() {
        let result = bind_and_format(
            "DELETE FROM t WHERE id = $1 RETURNING $2 AS tag",
            &[ParamValue::Int64(3), ParamValue::Text("gone".into())],
        );
        assert!(
            result.contains("id = 3") && result.contains("'gone'"),
            "got: {result}"
        );
        assert!(!result.contains("$2"), "got: {result}");
    }

    #[test]
    fn bind_update_limit() {
        let result = bind_and_format(
            "UPDATE t SET n = 1 WHERE id > 0 LIMIT $1",
            &[ParamValue::Int64(5)],
        );
        assert!(!result.contains("$1"), "got: {result}");
    }

    #[test]
    fn bind_interval_value_placeholder() {
        let result = bind_and_format(
            "SELECT now() - INTERVAL $1",
            &[ParamValue::Text("1 day".into())],
        );
        assert!(!result.contains("$1"), "got: {result}");
    }

    #[test]
    fn bind_update_from_subquery() {
        let result = bind_and_format(
            "UPDATE t SET n = s.y FROM (SELECT $1 AS y) s WHERE t.id = s.y",
            &[ParamValue::Int64(7)],
        );
        assert!(!result.contains("$1"), "got: {result}");
    }

    #[test]
    fn bind_insert_on_conflict_update_placeholder() {
        let result = bind_and_format(
            "INSERT INTO t (id, n) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET n = $3",
            &[
                ParamValue::Int64(1),
                ParamValue::Int64(2),
                ParamValue::Int64(99),
            ],
        );
        assert!(!result.contains("$3"), "got: {result}");
    }

    #[test]
    fn bind_window_partition_by_placeholder() {
        let result = bind_and_format(
            "SELECT LAG(x) OVER (PARTITION BY $1 ORDER BY b) FROM t",
            &[ParamValue::Text("user_id".into())],
        );
        assert!(!result.contains("$1"), "got: {result}");
    }

    /// A float parameter resolves to a float, whole or fractional, never to
    /// the decimal or integer its plain text reads as.
    #[test]
    fn a_float_param_resolves_to_a_float() {
        use crate::resolver::expr::convert_value;
        use crate::types::SqlValue;
        for f in [1.5, 2.0, 0.1, -3.25, 1e300, 1e-300, f64::INFINITY] {
            let literal = placeholder_to_value("$1", &[ParamValue::Float64(f)])
                .expect("the placeholder names the one parameter");
            assert_eq!(
                convert_value(&literal).expect("a float literal resolves"),
                SqlValue::Float(f),
                "{f}"
            );
        }
        let literal = placeholder_to_value("$1", &[ParamValue::Float64(f64::NAN)])
            .expect("the placeholder names the one parameter");
        assert!(
            matches!(convert_value(&literal), Ok(SqlValue::Float(f)) if f.is_nan()),
            "{literal}"
        );
    }

    #[test]
    fn bind_window_order_by_placeholder() {
        let result = bind_and_format(
            "SELECT LAG(x) OVER (PARTITION BY a ORDER BY $1) FROM t",
            &[ParamValue::Text("ts".into())],
        );
        assert!(!result.contains("$1"), "got: {result}");
    }
}
