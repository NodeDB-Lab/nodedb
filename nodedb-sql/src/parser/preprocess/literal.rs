// SPDX-License-Identifier: Apache-2.0

//! SQL literal canonicalization for `nodedb_types::Value`.

/// Convert a `nodedb_types::Value` to a SQL literal string.
///
/// Used by pre-processing and by Origin's pgwire handlers to build SQL
/// from parsed field maps. Handles all Value variants.
///
/// A UUID, an instant and bytes render as the typed literal a bound
/// parameter of that type resolves through (`UUID '...'`, `TIMESTAMPTZ
/// '...'`, `TIMESTAMP '...'`, `BYTEA '\x...'`), so the row stores the typed
/// value, never its text. A decimal renders as its digits, or `NUMERIC
/// '5'` when it has no fractional part. A float carries an exponent
/// (`2e0`), and a non-finite float is `'NaN'::FLOAT8`, `'Infinity'::FLOAT8`
/// or `'-Infinity'::FLOAT8`, so it reads back as a float. An object renders
/// as the typed object call `jsonb_build_object('k', v, ...)` of its fields
/// in key order. Every text is quoted.
pub fn value_to_sql_literal(value: &nodedb_types::Value) -> String {
    use nodedb_types::sql_quote::quote_literal;
    if let Some(literal) = crate::params::ParamValue::typed_from_value(value)
        .as_ref()
        .and_then(crate::params_typed::typed_param_sql_literal)
    {
        return literal;
    }
    match value {
        nodedb_types::Value::String(s) => quote_literal(s),
        nodedb_types::Value::Integer(n) => n.to_string(),
        nodedb_types::Value::Float(f) => crate::params_typed::float_literal_text(*f),
        nodedb_types::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        nodedb_types::Value::Null => "NULL".to_string(),
        nodedb_types::Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(value_to_sql_literal).collect();
            format!("ARRAY[{}]", inner.join(", "))
        }
        nodedb_types::Value::Bytes(b) => quote_literal(&format!("\\x{}", hex::encode(b))),
        nodedb_types::Value::Object(map) => {
            // The typed object call, so every field keeps its exact type.
            let mut fields: Vec<_> = map.iter().collect();
            fields.sort_by(|a, b| a.0.cmp(b.0));
            let args: Vec<String> = fields
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}, {}",
                        nodedb_types::sql_quote::quote_literal(key),
                        value_to_sql_literal(value)
                    )
                })
                .collect();
            format!(
                "{}({})",
                crate::planner::dml_helpers::OBJECT_FUNCTION,
                args.join(", ")
            )
        }
        // A UUID whose text is no UUID: its text.
        nodedb_types::Value::Uuid(u) | nodedb_types::Value::Ulid(u) => quote_literal(u),
        nodedb_types::Value::DateTime(dt) | nodedb_types::Value::NaiveDateTime(dt) => {
            quote_literal(&dt.to_string())
        }
        nodedb_types::Value::Duration(d) => quote_literal(&d.to_string()),
        nodedb_types::Value::Decimal(d) => crate::params_typed::decimal_literal_text(d),
        other => quote_literal(&format!("{other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Value;

    use super::*;

    #[test]
    fn floats_keep_their_type_and_non_finite_floats_cast() {
        assert_eq!(value_to_sql_literal(&Value::Float(2.0)), "2e0");
        assert_eq!(value_to_sql_literal(&Value::Float(1.5)), "1.5e0");
        assert_eq!(
            value_to_sql_literal(&Value::Float(f64::NAN)),
            "'NaN'::FLOAT8"
        );
        assert_eq!(
            value_to_sql_literal(&Value::Float(f64::NEG_INFINITY)),
            "'-Infinity'::FLOAT8"
        );
    }

    #[test]
    fn a_whole_decimal_is_a_typed_numeric() {
        assert_eq!(
            value_to_sql_literal(&Value::Decimal("5".parse().unwrap())),
            "NUMERIC '5'"
        );
        assert_eq!(
            value_to_sql_literal(&Value::Decimal("5.50".parse().unwrap())),
            "5.50"
        );
    }

    #[test]
    fn uuid_and_ulid_text_is_quoted() {
        assert_eq!(
            value_to_sql_literal(&Value::Uuid("x'); DROP TABLE t; --".into())),
            "'x''); DROP TABLE t; --'"
        );
        assert_eq!(value_to_sql_literal(&Value::Ulid("a'b".into())), "'a''b'");
    }
}
