// SPDX-License-Identifier: BUSL-1.1

//! The PostgreSQL text form of an array cell: `{1,2.5,NaN,NULL}`.
//!
//! Each dimension is a `{...}` list of comma-separated elements. SQL NULL is
//! the unquoted word `NULL`. An element whose text is empty, is `NULL` in any
//! case, or holds a brace, comma, double quote, backslash or whitespace is
//! double-quoted, with each `"` and `\` escaped by a backslash. A nested
//! list is one more dimension. Every list of one dimension holds the same
//! number of elements, as PostgreSQL requires.

use std::error::Error;

use bytes::{BufMut, BytesMut};
use nodedb_types::Value;
use nodedb_types::columnar::FloatWidth;
use nodedb_types::error::NodeDbError;
use nodedb_types::value::non_finite_float_text;
use pgwire::api::Type;
use pgwire::error::PgWireResult;
use pgwire::types::ToSqlText;
use pgwire::types::format::FormatOptions;
use postgres_types::{IsNull, ToSql, accepts, to_sql_checked};

use crate::control::server::pgwire::numeric_narrow::checked_narrow_f32;
use crate::control::server::pgwire::types::error_map::shape_error_to_pg;
use crate::control::server::response_shape::cell::shape_mismatch;

/// The text of a `float4[]` (`width` `F32`) or `float8[]` (`width` `F64`)
/// cell of column `column`. The cell is an array of floats, integers, NULLs
/// or nested arrays, or a vector. Any other shape is an error naming the
/// column.
pub(super) fn float_array_text(column: &str, v: &Value, width: FloatWidth) -> PgWireResult<String> {
    let mut out = String::new();
    match v {
        Value::Array(items) => write_list(column, items, width, &mut out)?,
        Value::Vector(floats) => {
            out.push('{');
            for (i, f) in floats.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_element(&mut out, &float_text(f64::from(*f), width)?);
            }
            out.push('}');
        }
        other => {
            return Err(shape_error_to_pg(&shape_mismatch(
                column, "an array", other,
            )));
        }
    }
    Ok(out)
}

/// A rendered `{...}` array literal as pgwire encodes it.
///
/// pgwire double-quotes a string under an array type as one element, so the
/// literal travels through this type, which writes its bytes unchanged. An
/// array column always travels in the text format, and the binary encoding
/// is an error naming that rule.
#[derive(Debug)]
pub(super) struct PgArrayLiteral(pub(super) String);

impl ToSqlText for PgArrayLiteral {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSql for PgArrayLiteral {
    fn to_sql(
        &self,
        ty: &Type,
        _out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        Err(
            format!("a {ty} cell has no binary encoding: array columns travel in the text format")
                .into(),
        )
    }

    accepts!(FLOAT4_ARRAY, FLOAT8_ARRAY);

    to_sql_checked!();
}

/// Append one `{...}` list. A list holds only nested lists of one length,
/// or only scalars and NULLs.
fn write_list(
    column: &str,
    items: &[Value],
    width: FloatWidth,
    out: &mut String,
) -> PgWireResult<()> {
    check_rectangular(column, items)?;
    out.push('{');
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match item {
            Value::Null => out.push_str("NULL"),
            Value::Array(inner) => write_list(column, inner, width, out)?,
            Value::Float(f) => push_element(out, &float_text(*f, width)?),
            Value::Integer(n) => push_element(out, &float_text(*n as f64, width)?),
            other => return Err(shape_error_to_pg(&shape_mismatch(column, "a float", other))),
        }
    }
    out.push('}');
    Ok(())
}

/// A list's items are all nested lists of one length, or none is a list.
fn check_rectangular(column: &str, items: &[Value]) -> PgWireResult<()> {
    let mut lists = items.iter().map(|item| match item {
        Value::Array(inner) => Some(inner.len()),
        _ => None,
    });
    let Some(first) = lists.next() else {
        return Ok(());
    };
    if lists.all(|len| len == first) {
        return Ok(());
    }
    Err(shape_error_to_pg(&NodeDbError::data_exception(format!(
        "column \"{column}\" holds a ragged array: every list of one dimension must hold \
         the same number of elements"
    ))))
}

/// The PostgreSQL text of one float element. A non-finite float is `NaN`,
/// `Infinity` or `-Infinity`. A finite float is its shortest round-trip
/// decimal at `width`. A finite value beyond `f32` range under `F32` is an
/// out-of-range error.
fn float_text(f: f64, width: FloatWidth) -> PgWireResult<String> {
    if let Some(text) = non_finite_float_text(f) {
        return Ok(text.to_owned());
    }
    Ok(match width {
        FloatWidth::F64 => f.to_string(),
        FloatWidth::F32 => checked_narrow_f32(f)?.to_string(),
    })
}

/// Append one element's text, double-quoted when PostgreSQL quotes it.
pub(super) fn push_element(out: &mut String, text: &str) {
    if !needs_quotes(text) {
        out.push_str(text);
        return;
    }
    out.push('"');
    for ch in text.chars() {
        if matches!(ch, '"' | '\\') {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('"');
}

/// Whether an element's text needs double quotes to read back as itself.
fn needs_quotes(text: &str) -> bool {
    text.is_empty()
        || text.eq_ignore_ascii_case("NULL")
        || text.chars().any(|ch| {
            matches!(
                ch,
                '{' | '}' | ',' | '"' | '\\' | ' ' | '\t' | '\n' | '\r' | '\u{b}' | '\u{c}'
            )
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pgwire::error::PgWireError;

    use super::*;

    fn floats(values: &[f64]) -> Value {
        Value::Array(values.iter().copied().map(Value::Float).collect())
    }

    fn text_of(v: &Value, width: FloatWidth) -> String {
        float_array_text("c", v, width).expect("encodes")
    }

    fn message_of(err: PgWireError) -> String {
        let PgWireError::UserError(info) = err else {
            panic!("expected a UserError, got {err:?}");
        };
        info.message.clone()
    }

    #[test]
    fn float_arrays_render_postgres_text() {
        assert_eq!(
            text_of(&floats(&[1.0, 2.5, -0.125]), FloatWidth::F64),
            "{1,2.5,-0.125}"
        );
        assert_eq!(text_of(&floats(&[]), FloatWidth::F64), "{}");
        assert_eq!(
            text_of(&Value::Array(vec![Value::Integer(3)]), FloatWidth::F64),
            "{3}"
        );
        // `float4` elements render the shortest text of the `f32` value.
        assert_eq!(text_of(&floats(&[0.1]), FloatWidth::F32), "{0.1}");
        assert_eq!(
            text_of(&Value::Vector(Arc::from([0.5f32, -2.0])), FloatWidth::F32),
            "{0.5,-2}"
        );
    }

    #[test]
    fn non_finite_and_null_elements_render_postgres_words() {
        let v = Value::Array(vec![
            Value::Float(f64::NAN),
            Value::Null,
            Value::Float(f64::INFINITY),
            Value::Float(f64::NEG_INFINITY),
        ]);
        assert_eq!(
            text_of(&v, FloatWidth::F64),
            "{NaN,NULL,Infinity,-Infinity}"
        );
        assert_eq!(
            text_of(&v, FloatWidth::F32),
            "{NaN,NULL,Infinity,-Infinity}"
        );
    }

    #[test]
    fn nested_arrays_are_extra_dimensions() {
        let v = Value::Array(vec![floats(&[1.0, 2.0]), floats(&[3.0, 4.0])]);
        assert_eq!(text_of(&v, FloatWidth::F64), "{{1,2},{3,4}}");
    }

    #[test]
    fn ragged_and_mixed_arrays_are_refused() {
        let ragged = Value::Array(vec![floats(&[1.0, 2.0]), floats(&[3.0])]);
        let message =
            message_of(float_array_text("emb", &ragged, FloatWidth::F64).expect_err("ragged"));
        assert!(
            message.contains("column \"emb\" holds a ragged array"),
            "{message}"
        );

        let mixed = Value::Array(vec![Value::Float(1.0), floats(&[2.0])]);
        assert!(float_array_text("emb", &mixed, FloatWidth::F64).is_err());
    }

    #[test]
    fn non_float_elements_and_non_arrays_are_refused() {
        let text_element = Value::Array(vec![Value::String("x".into())]);
        let message =
            message_of(float_array_text("emb", &text_element, FloatWidth::F64).expect_err("text"));
        assert!(
            message.contains("holds text where a float is required"),
            "{message}"
        );

        let message = message_of(
            float_array_text("emb", &Value::String("[1]".into()), FloatWidth::F64)
                .expect_err("not an array"),
        );
        assert!(
            message.contains("holds text where an array is required"),
            "{message}"
        );
    }

    #[test]
    fn float4_overflow_is_refused() {
        assert!(float_array_text("c", &floats(&[1e39]), FloatWidth::F32).is_err());
        assert_eq!(
            text_of(&floats(&[1e39]), FloatWidth::F64),
            format!("{{{}}}", 1e39f64)
        );
    }

    #[test]
    fn elements_are_quoted_as_postgres_quotes_them() {
        let quoted = |text: &str| {
            let mut out = String::new();
            push_element(&mut out, text);
            out
        };
        assert_eq!(quoted("1.5"), "1.5");
        assert_eq!(quoted("abc"), "abc");
        assert_eq!(quoted(""), "\"\"");
        assert_eq!(quoted("NULL"), "\"NULL\"");
        assert_eq!(quoted("null"), "\"null\"");
        assert_eq!(quoted("a,b"), "\"a,b\"");
        assert_eq!(quoted("{x}"), "\"{x}\"");
        assert_eq!(quoted("two words"), "\"two words\"");
        assert_eq!(quoted("say \"hi\""), "\"say \\\"hi\\\"\"");
        assert_eq!(quoted("back\\slash"), "\"back\\\\slash\"");
    }
}
