// SPDX-License-Identifier: Apache-2.0

//! The literal text a bound parameter or native value splices in as, by
//! the `Form` its position takes.

use super::splice::Form;
use crate::params::ParamValue;

/// The SQL text of `param`, spelled in `form`.
pub(super) fn param_literal(param: &ParamValue, form: Form) -> String {
    let quoted = |text: String| nodedb_types::sql_quote::quote_literal(&text);
    match (form, param) {
        (Form::ObjectLiteral, _) => {
            return crate::params_composite::param_object_literal_text(param);
        }
        (Form::Sql, _) => {
            if let Some(literal) = crate::params_typed::typed_param_sql_literal(param) {
                return literal;
            }
        }
        (Form::Text, _) => {}
    }
    match param {
        ParamValue::Null | ParamValue::IntNull(_) => "NULL".to_owned(),
        ParamValue::Bool(true) => "TRUE".to_owned(),
        ParamValue::Bool(false) => "FALSE".to_owned(),
        ParamValue::Int64(n) => n.to_string(),
        ParamValue::Int32(n) => n.to_string(),
        ParamValue::Int16(n) => n.to_string(),
        ParamValue::Float64(f) => match (form, nodedb_types::value::non_finite_float_text(*f)) {
            // A DSL grammar reads a non-finite float as its quoted text.
            (Form::Text, Some(text)) => quoted(text.to_owned()),
            _ => crate::params_typed::float_literal_text(*f),
        },
        ParamValue::Decimal(d) => d.to_string(),
        ParamValue::Text(s) => quoted(s.clone()),
        ParamValue::Timestamp(dt) | ParamValue::Timestamptz(dt) => quoted(dt.to_iso8601()),
        ParamValue::Uuid(u) => quoted(u.clone()),
        ParamValue::Bytes(b) => quoted(nodedb_types::bytea_text::format_bytea_hex(b)),
        ParamValue::Duration(d) => quoted(crate::params_composite::duration_text(d)),
        // A DSL grammar that reads a quoted string takes the JSON text.
        ParamValue::Array(_) | ParamValue::Object(_) => {
            quoted(crate::params_composite::param_json_text(param))
        }
    }
}

/// The SQL text of a native bound `value`, spelled in `form`. An array
/// renders each element the same way: `ARRAY[...]` in SQL, `[...]` in an
/// object literal.
pub(super) fn value_literal(value: &nodedb_types::Value, form: Form) -> String {
    if let nodedb_types::Value::Array(items) = value
        && !matches!(form, Form::Text)
    {
        let items: Vec<String> = items.iter().map(|item| value_literal(item, form)).collect();
        return match form {
            Form::ObjectLiteral => format!("[{}]", items.join(", ")),
            Form::Sql | Form::Text => format!("ARRAY[{}]", items.join(", ")),
        };
    }
    // A float takes the parameter form, so it carries an exponent and reads
    // back as a float, and a non-finite float is its `FLOAT8` cast.
    if let nodedb_types::Value::Float(f) = value {
        return param_literal(&ParamValue::Float64(*f), form);
    }
    match ParamValue::typed_from_value(value) {
        Some(param) => param_literal(&param, form),
        None => value.to_sql_literal(),
    }
}

#[cfg(test)]
mod tests {
    use crate::dsl_bind::{bind_dsl, bind_values};
    use crate::params::ParamValue;

    /// `UPSERT INTO` is SQL once preprocessed, so a UUID parameter is a typed
    /// literal there. Any other DSL reads it as quoted text.
    #[test]
    fn typed_params_are_typed_literals_only_in_upsert() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let params = [ParamValue::Uuid(uuid.into())];
        let upsert = bind_dsl("UPSERT INTO t (id, u) VALUES ('a', $1)", &params).unwrap();
        assert!(
            upsert.as_str().contains(&format!("UUID '{uuid}'")),
            "{}",
            upsert.as_str()
        );
        let search = bind_dsl("SEARCH t USING FTS($1)", &params).unwrap();
        assert!(
            search.as_str().contains(&format!("FTS('{uuid}')")),
            "{}",
            search.as_str()
        );
    }

    /// A typed parameter inside an object literal takes the object-literal
    /// typed form in every DSL, so `PROPERTIES { k: $1 }` reads typed.
    #[test]
    fn typed_params_inside_braces_are_typed_in_every_dsl() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let bound = bind_dsl(
            "GRAPH INSERT EDGE IN 'e' FROM $1 TO 'b' TYPE 'l' PROPERTIES { u: $2, p: $3 }",
            &[
                ParamValue::Uuid(uuid.into()),
                ParamValue::Uuid(uuid.into()),
                ParamValue::Decimal("12.50".parse().unwrap()),
            ],
        )
        .unwrap();
        let text = bound.as_str();
        assert!(text.contains(&format!("FROM '{uuid}'")), "{text}");
        assert!(text.contains(&format!("u: UUID '{uuid}'")), "{text}");
        assert!(text.contains("p: DECIMAL '12.50'"), "{text}");
        let start = text.find('{').expect("an object literal");
        let fields = crate::parser::object_literal::parse_object_literal(&text[start..])
            .expect("an object literal")
            .expect("parses");
        assert_eq!(fields["u"], nodedb_types::Value::Uuid(uuid.into()));
        assert_eq!(
            fields["p"],
            nodedb_types::Value::Decimal("12.50".parse().unwrap())
        );
    }

    /// A native float keeps its type: a whole float carries an exponent and
    /// a non-finite float is its `FLOAT8` cast.
    #[test]
    fn native_floats_bind_as_floats() {
        assert_eq!(
            bind_values(
                "SELECT $1, $2",
                &[
                    nodedb_types::Value::Float(2.0),
                    nodedb_types::Value::Float(f64::NAN)
                ]
            )
            .unwrap(),
            "SELECT 2e0, 'NaN'::FLOAT8"
        );
        assert_eq!(
            bind_values(
                "SELECT $1",
                &[nodedb_types::Value::Decimal("5".parse().unwrap())]
            )
            .unwrap(),
            "SELECT NUMERIC '5'"
        );
    }
}
