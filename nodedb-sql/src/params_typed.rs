// SPDX-License-Identifier: Apache-2.0

//! Typed forms of a bound parameter.
//!
//! A parameter whose plain SQL literal reads as another type (a timestamp,
//! UUID, bytes, a whole decimal, a non-finite float, a duration, an array or
//! an object) binds as a typed expression, and a statement bound as text
//! spells it as a typed literal. The resolver reads both back through the
//! typed-literal rule, so the bound value keeps its type.

use sqlparser::ast::{DataType, Expr, TimezoneInfo, TypedString, Value, ValueWithSpan};
use sqlparser::tokenizer::Span;

use crate::params::ParamValue;

/// The SQL text of a typed literal that resolves to exactly the value
/// `param` carries, for a statement bound as text: `UUID '...'`,
/// `TIMESTAMPTZ '...'`, `TIMESTAMP '...'`, `BYTEA '\x...'`, a decimal by
/// [`decimal_literal_text`], `'<n>us'::DURATION`, and `ARRAY[...]` /
/// `jsonb_build_object(...)` of typed elements. The resolver reads each
/// back through the same typed-literal rule a bound pgwire parameter takes.
/// `None` for every other parameter.
pub fn typed_param_sql_literal(param: &ParamValue) -> Option<String> {
    let typed =
        |type_name: &str, text: String| Some(format!("{type_name} '{}'", text.replace('\'', "''")));
    match param {
        ParamValue::Timestamp(dt) => typed("TIMESTAMP", dt.to_iso8601()),
        ParamValue::Timestamptz(dt) => typed("TIMESTAMPTZ", dt.to_iso8601()),
        ParamValue::Uuid(u) => typed("UUID", u.clone()),
        ParamValue::Bytes(b) => typed("BYTEA", nodedb_types::bytea_text::format_bytea_hex(b)),
        ParamValue::Decimal(d) => Some(decimal_literal_text(d)),
        ParamValue::Duration(d) => Some(crate::params_composite::duration_sql_text(d)),
        ParamValue::Array(items) => Some(crate::params_composite::array_sql_text(items)),
        ParamValue::Object(fields) => Some(crate::params_composite::object_sql_text(fields)),
        ParamValue::Null
        | ParamValue::Bool(_)
        | ParamValue::Int64(_)
        | ParamValue::Int32(_)
        | ParamValue::Int16(_)
        | ParamValue::IntNull(_)
        | ParamValue::Float64(_)
        | ParamValue::Text(_) => None,
    }
}

/// The typed string a placeholder bound to a timestamp, UUID, bytes or
/// whole-decimal parameter becomes, keeping the placeholder's source span.
/// A duration, an array, an object and a non-finite float become the
/// expression their SQL text spells. `None` for every other parameter.
pub(crate) fn placeholder_to_typed(
    placeholder: &str,
    params: &[ParamValue],
    span: Span,
) -> Option<Expr> {
    let idx: usize = placeholder.strip_prefix('$')?.parse().ok()?;
    let param = params.get(idx.checked_sub(1)?)?;
    // A duration, an array and an object are written as the typed
    // expression their SQL text spells, as is a non-finite float
    // (`'NaN'::FLOAT8`).
    let spelled = match param {
        ParamValue::Duration(_) | ParamValue::Array(_) | ParamValue::Object(_) => true,
        ParamValue::Float64(f) => !f.is_finite(),
        _ => false,
    };
    if spelled {
        return parse_expr_text(&crate::params_composite::param_sql_text(param));
    }
    let (data_type, text) = match param {
        ParamValue::Timestamp(dt) => (
            DataType::Timestamp(None, TimezoneInfo::None),
            dt.to_iso8601(),
        ),
        ParamValue::Timestamptz(dt) => {
            (DataType::Timestamp(None, TimezoneInfo::Tz), dt.to_iso8601())
        }
        ParamValue::Uuid(u) => (DataType::Uuid, u.clone()),
        ParamValue::Bytes(b) => (
            DataType::Bytea,
            nodedb_types::bytea_text::format_bytea_hex(b),
        ),
        // A whole decimal is `NUMERIC '5'`: its digits alone read as an
        // integer.
        ParamValue::Decimal(d) if d.scale() == 0 => (
            DataType::Numeric(sqlparser::ast::ExactNumberInfo::None),
            d.to_string(),
        ),
        ParamValue::Null
        | ParamValue::Bool(_)
        | ParamValue::Int64(_)
        | ParamValue::Int32(_)
        | ParamValue::Int16(_)
        | ParamValue::IntNull(_)
        | ParamValue::Float64(_)
        | ParamValue::Decimal(_)
        | ParamValue::Text(_)
        | ParamValue::Duration(_)
        | ParamValue::Array(_)
        | ParamValue::Object(_) => return None,
    };
    Some(Expr::TypedString(TypedString {
        data_type,
        value: ValueWithSpan {
            value: Value::SingleQuotedString(text),
            span,
        },
        uses_odbc_syntax: false,
    }))
}

/// The expression `text` spells, or `None` when it does not parse. The text
/// is produced by [`crate::params_composite::param_sql_text`], so it always
/// parses.
fn parse_expr_text(text: &str) -> Option<Expr> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    sqlparser::parser::Parser::new(&dialect)
        .try_with_sql(text)
        .ok()?
        .parse_expr()
        .ok()
}

/// The SQL text of a float parameter.
///
/// A finite float always carries an exponent: `1.5e0`, `2e0`. The literal
/// resolver reads an exponent as a float, so the value stays a float. The
/// plain form reads `1.5` as a decimal and `2` as an integer. The mantissa
/// is the shortest text that round-trips the `f64`.
///
/// A non-finite float is the cast `'NaN'::FLOAT8`, `'Infinity'::FLOAT8` or
/// `'-Infinity'::FLOAT8`. The resolver and the object-literal parser read
/// each as the float. A bare `NaN` would read as a column name.
pub(crate) fn float_literal_text(f: f64) -> String {
    match nodedb_types::value::non_finite_float_text(f) {
        Some(text) => format!("'{text}'::FLOAT8"),
        None => format!("{f:e}"),
    }
}

/// The float a PostgreSQL `float8` text spells: a number, or `NaN`,
/// `Infinity`, `-Infinity` (also `inf`), case ignored. `None` for text
/// that spells no float.
pub(crate) fn parse_float8_text(text: &str) -> Option<f64> {
    text.trim().parse::<f64>().ok()
}

/// The SQL text of a decimal parameter that resolves back to the decimal.
///
/// A decimal with a fractional part is its exact digits: `12.50`. A whole
/// decimal is the typed literal `NUMERIC '5'`, since the digits `5` alone
/// read as an integer.
pub(crate) fn decimal_literal_text(d: &rust_decimal::Decimal) -> String {
    if d.scale() == 0 {
        format!("NUMERIC '{d}'")
    } else {
        d.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::bind_params;
    use crate::parser::statement::parse_sql;

    /// The first projected expression of `sql` with `params` bound,
    /// resolved.
    fn bound_expr(sql: &str, params: &[ParamValue]) -> crate::types::SqlExpr {
        use crate::resolver::ColumnScope;
        use crate::resolver::expr::convert_expr;
        let mut stmts = parse_sql(sql).unwrap();
        bind_params(&mut stmts[0], params);
        let sqlparser::ast::Statement::Query(query) = &stmts[0] else {
            panic!("a query");
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("a select");
        };
        let (sqlparser::ast::SelectItem::UnnamedExpr(expr)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. }) = &select.projection[0]
        else {
            panic!("an expression item");
        };
        convert_expr(expr, &ColumnScope::Unchecked).expect("resolves")
    }

    /// The first projected expression of `sql` with `params` bound, as the
    /// literal it resolves to.
    fn bound_literal(sql: &str, params: &[ParamValue]) -> crate::types::SqlValue {
        match bound_expr(sql, params) {
            crate::types::SqlExpr::Literal(value) => value,
            other => panic!("a literal, got {other:?}"),
        }
    }

    /// A whole decimal parameter stays a decimal: its digits alone would
    /// read as an integer.
    #[test]
    fn a_whole_decimal_param_resolves_to_a_decimal() {
        use crate::types::SqlValue;
        for text in ["5", "-5", "0", "79228162514264337593543950335"] {
            let d: rust_decimal::Decimal = text.parse().unwrap();
            assert_eq!(
                bound_literal("SELECT $1", &[ParamValue::Decimal(d)]),
                SqlValue::Decimal(d),
                "{text}"
            );
        }
        let five: rust_decimal::Decimal = "5".parse().unwrap();
        assert_eq!(
            typed_param_sql_literal(&ParamValue::Decimal(five)).as_deref(),
            Some("NUMERIC '5'")
        );
        let array = ParamValue::Array(vec![ParamValue::Decimal(five)]);
        assert!(matches!(
            bound_expr("SELECT $1", &[array]),
            crate::types::SqlExpr::ArrayLiteral(items)
                if matches!(&items[..], [crate::types::SqlExpr::Literal(SqlValue::Decimal(d))] if *d == five)
        ));
    }

    /// A non-finite float parameter in an expression is the `FLOAT8` cast,
    /// which resolves to the float, alone and inside an array.
    #[test]
    fn a_non_finite_float_param_resolves_to_the_float() {
        use crate::types::SqlValue;
        assert_eq!(float_literal_text(f64::NAN), "'NaN'::FLOAT8");
        assert_eq!(float_literal_text(f64::INFINITY), "'Infinity'::FLOAT8");
        assert_eq!(float_literal_text(f64::NEG_INFINITY), "'-Infinity'::FLOAT8");
        for f in [f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(
                bound_literal("SELECT $1", &[ParamValue::Float64(f)]),
                SqlValue::Float(f)
            );
        }
        assert!(matches!(
            bound_literal("SELECT $1", &[ParamValue::Float64(f64::NAN)]),
            SqlValue::Float(f) if f.is_nan()
        ));
        let array = ParamValue::Array(vec![ParamValue::Float64(f64::NAN)]);
        assert!(matches!(
            bound_expr("SELECT $1", &[array]),
            crate::types::SqlExpr::ArrayLiteral(items)
                if matches!(&items[..], [crate::types::SqlExpr::Literal(SqlValue::Float(f))] if f.is_nan())
        ));
    }

    /// A timestamp, UUID or bytes parameter resolves to its typed value,
    /// alone and inside a cast, never to its text.
    #[test]
    fn typed_params_resolve_to_typed_values() {
        use crate::resolver::ColumnScope;
        use crate::resolver::expr::convert_expr;
        use crate::types::{SqlExpr, SqlValue};
        let at = nodedb_types::NdbDateTime::from_micros(1_704_164_645_000_000);
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let params = [
            ParamValue::Uuid(uuid.into()),
            ParamValue::Timestamptz(at),
            ParamValue::Timestamp(at),
            ParamValue::Bytes(vec![1, 2, 0xff]),
        ];
        let mut stmts = parse_sql("SELECT $1, $2, $3, $4, $1::uuid, $2::timestamptz").unwrap();
        bind_params(&mut stmts[0], &params);
        let sqlparser::ast::Statement::Query(query) = &stmts[0] else {
            panic!("a query");
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("a select");
        };
        let values: Vec<SqlValue> = select
            .projection
            .iter()
            .map(|item| {
                let (sqlparser::ast::SelectItem::UnnamedExpr(expr)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. }) = item
                else {
                    panic!("an expression item");
                };
                match convert_expr(expr, &ColumnScope::Unchecked).expect("resolves") {
                    SqlExpr::Literal(value) => value,
                    other => panic!("a literal, got {other:?}"),
                }
            })
            .collect();
        assert_eq!(
            values,
            vec![
                SqlValue::Uuid(uuid.into()),
                SqlValue::Timestamptz(at),
                SqlValue::Timestamp(at),
                SqlValue::Bytes(vec![1, 2, 0xff]),
                SqlValue::Uuid(uuid.into()),
                SqlValue::Timestamptz(at),
            ]
        );
    }

    /// The typed literal text of a value resolves back to the same typed
    /// value.
    #[test]
    fn typed_literal_text_resolves_to_the_typed_value() {
        use crate::resolver::ColumnScope;
        use crate::resolver::expr::convert_expr;
        use crate::types::{SqlExpr, SqlValue};
        use nodedb_types::Value;
        let at = nodedb_types::NdbDateTime::from_micros(1_704_164_645_123_456);
        let price: rust_decimal::Decimal = "12.50".parse().unwrap();
        for (value, expected) in [
            (
                Value::Uuid("550E8400-e29b-41d4-a716-446655440000".into()),
                SqlValue::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            (Value::DateTime(at), SqlValue::Timestamptz(at)),
            (Value::NaiveDateTime(at), SqlValue::Timestamp(at)),
            (Value::Bytes(vec![0, 0xff]), SqlValue::Bytes(vec![0, 0xff])),
            (Value::Decimal(price), SqlValue::Decimal(price)),
        ] {
            let param = ParamValue::typed_from_value(&value).expect("a typed kind");
            let text = typed_param_sql_literal(&param).expect("a typed literal");
            let stmts = parse_sql(&format!("SELECT {text}")).unwrap();
            let sqlparser::ast::Statement::Query(query) = &stmts[0] else {
                panic!("a query");
            };
            let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
                panic!("a select");
            };
            let sqlparser::ast::SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
                panic!("an unnamed item");
            };
            match convert_expr(expr, &ColumnScope::Unchecked).expect("resolves") {
                SqlExpr::Literal(resolved) => assert_eq!(resolved, expected, "{text}"),
                other => panic!("a literal, got {other:?}"),
            }
        }
    }
}
