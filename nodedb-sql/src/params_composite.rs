// SPDX-License-Identifier: Apache-2.0

//! The SQL, JSON and object-literal text of a parameter, and the parameter
//! a JSON text spells.
//!
//! An `interval`, an array and a `json` / `jsonb` parameter bind as
//! [`ParamValue::Duration`], [`ParamValue::Array`] and
//! [`ParamValue::Object`] (or the JSON scalar the text holds). Each is
//! written into a statement in the form the resolver reads back to the same
//! typed value: `'<n>us'::DURATION`, `ARRAY[...]`, and
//! `jsonb_build_object('k', v, ...)`, every element in its own typed form.
//!
//! A JSON text keeps the types JSON carries. An integer is an integer, a
//! number with an exponent a float, any other fractional number an exact
//! decimal with its scale, the rule a SQL numeric literal follows.

use sonic_rs::{JsonContainerTrait, JsonType, JsonValueTrait};

use std::collections::HashMap;

use crate::params::ParamValue;
use crate::params_typed::{decimal_literal_text, float_literal_text, typed_param_sql_literal};

/// The SQL expression text that resolves to exactly the value `param`
/// carries.
pub fn param_sql_text(param: &ParamValue) -> String {
    if let Some(typed) = typed_param_sql_literal(param) {
        return typed;
    }
    match param {
        ParamValue::Null => "NULL".to_owned(),
        ParamValue::Bool(true) => "TRUE".to_owned(),
        ParamValue::Bool(false) => "FALSE".to_owned(),
        ParamValue::Int64(n) => n.to_string(),
        ParamValue::Float64(f) => float_literal_text(*f),
        ParamValue::Decimal(d) => decimal_literal_text(d),
        ParamValue::Text(s) => nodedb_types::sql_quote::quote_literal(s),
        ParamValue::Timestamp(at) | ParamValue::Timestamptz(at) => {
            nodedb_types::sql_quote::quote_literal(&at.to_iso8601())
        }
        ParamValue::Uuid(u) => nodedb_types::sql_quote::quote_literal(u),
        ParamValue::Bytes(b) => {
            nodedb_types::sql_quote::quote_literal(&nodedb_types::bytea_text::format_bytea_hex(b))
        }
        ParamValue::Duration(d) => duration_sql_text(d),
        ParamValue::Array(items) => array_sql_text(items),
        ParamValue::Object(fields) => object_sql_text(fields),
    }
}

/// `'<n>us'::DURATION`: the exact microseconds of `d`.
pub(crate) fn duration_sql_text(d: &nodedb_types::NdbDuration) -> String {
    format!("'{}'::DURATION", duration_text(d))
}

/// `<n>us`, the exact duration text `NdbDuration::parse` reads back.
pub(crate) fn duration_text(d: &nodedb_types::NdbDuration) -> String {
    format!("{}us", d.micros)
}

/// `ARRAY[...]` of each element's typed SQL text.
pub(crate) fn array_sql_text(items: &[ParamValue]) -> String {
    let items: Vec<String> = items.iter().map(param_sql_text).collect();
    format!("ARRAY[{}]", items.join(", "))
}

/// `jsonb_build_object('k', v, ...)` of each field's typed SQL text.
pub(crate) fn object_sql_text(fields: &[(String, ParamValue)]) -> String {
    let args: Vec<String> = fields
        .iter()
        .map(|(key, value)| {
            format!(
                "{}, {}",
                nodedb_types::sql_quote::quote_literal(key),
                param_sql_text(value)
            )
        })
        .collect();
    format!(
        "{}({})",
        crate::planner::dml_helpers::OBJECT_FUNCTION,
        args.join(", ")
    )
}

/// The object-literal text of `param`, for a placeholder inside a `{ ... }`
/// object literal: `[...]` for an array, `{ "k": v }` for an object, the
/// typed-string form (`UUID '...'`, `DECIMAL '...'`, `DURATION '...'`) for
/// a typed scalar.
pub fn param_object_literal_text(param: &ParamValue) -> String {
    let quoted = |text: String| nodedb_types::sql_quote::quote_literal(&text);
    match param {
        ParamValue::Decimal(d) => format!("DECIMAL {}", quoted(d.to_string())),
        ParamValue::Duration(d) => format!("DURATION {}", quoted(duration_text(d))),
        ParamValue::Array(items) => {
            let items: Vec<String> = items.iter().map(param_object_literal_text).collect();
            format!("[{}]", items.join(", "))
        }
        ParamValue::Object(fields) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|(key, value)| {
                    format!(
                        "\"{}\": {}",
                        key.replace('"', "\"\""),
                        param_object_literal_text(value)
                    )
                })
                .collect();
            format!("{{ {} }}", fields.join(", "))
        }
        other => param_sql_text(other),
    }
}

/// The JSON text of `param`. A typed scalar JSON holds only as text (a
/// UUID, an instant, bytes, a duration) is its text. A float carries an
/// exponent, so [`param_from_json_text`] reads it back as a float, and a
/// decimal its exact digits. A non-finite float, which a JSON number cannot
/// hold, is its PostgreSQL text in a JSON string (`"NaN"`, `"Infinity"`,
/// `"-Infinity"`), as `to_json` renders it.
pub fn param_json_text(param: &ParamValue) -> String {
    match param {
        ParamValue::Null => "null".to_owned(),
        ParamValue::Bool(b) => b.to_string(),
        ParamValue::Int64(n) => n.to_string(),
        ParamValue::Float64(f) => match nodedb_types::value::non_finite_float_text(*f) {
            Some(text) => json_string(text),
            None => float_literal_text(*f),
        },
        ParamValue::Decimal(d) => d.to_string(),
        ParamValue::Text(s) | ParamValue::Uuid(s) => json_string(s),
        ParamValue::Timestamp(at) | ParamValue::Timestamptz(at) => json_string(&at.to_iso8601()),
        ParamValue::Bytes(b) => json_string(&nodedb_types::bytea_text::format_bytea_hex(b)),
        ParamValue::Duration(d) => json_string(&duration_text(d)),
        ParamValue::Array(items) => {
            let items: Vec<String> = items.iter().map(param_json_text).collect();
            format!("[{}]", items.join(","))
        }
        ParamValue::Object(fields) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|(key, value)| format!("{}:{}", json_string(key), param_json_text(value)))
                .collect();
            format!("{{{}}}", fields.join(","))
        }
    }
}

/// `text` as a JSON string, with quotes, backslashes and control
/// characters escaped.
fn json_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// The parameter a JSON text spells: an object, an array, or a scalar.
///
/// An object keeps its keys in written order. A key written twice keeps
/// its last value. `Err` names why the text is not JSON, or which number no
/// exact value holds.
pub fn param_from_json_text(text: &str) -> Result<ParamValue, String> {
    let value: sonic_rs::Value = sonic_rs::Deserializer::from_str(text)
        .use_rawnumber()
        .deserialize()
        .map_err(|e| format!("invalid JSON: {e}"))?;
    json_param(&value)
}

fn json_param(value: &sonic_rs::Value) -> Result<ParamValue, String> {
    Ok(match value.get_type() {
        JsonType::Null => ParamValue::Null,
        JsonType::Boolean => ParamValue::Bool(value.as_bool().unwrap_or_default()),
        JsonType::String => ParamValue::Text(value.as_str().unwrap_or_default().to_owned()),
        JsonType::Number => {
            let raw = value
                .as_raw_number()
                .ok_or_else(|| "a JSON number has no text".to_owned())?;
            json_number(raw.as_str())?
        }
        JsonType::Array => ParamValue::Array(
            value
                .as_array()
                .map(|items| items.iter().map(json_param).collect::<Result<_, _>>())
                .transpose()?
                .unwrap_or_default(),
        ),
        JsonType::Object => {
            let mut fields: Vec<(String, ParamValue)> = Vec::new();
            // Key to its slot in `fields`: a key written twice keeps its
            // first position and its last value, as `jsonb` does.
            let mut slots: HashMap<String, usize> = HashMap::new();
            if let Some(object) = value.as_object() {
                for (key, item) in object.iter() {
                    let item = json_param(item)?;
                    match slots.get(key) {
                        Some(&slot) => fields[slot].1 = item,
                        None => {
                            slots.insert(key.to_owned(), fields.len());
                            fields.push((key.to_owned(), item));
                        }
                    }
                }
            }
            ParamValue::Object(fields)
        }
    })
}

/// A JSON number by the SQL numeric-literal rule: an integer, a float when
/// it carries an exponent, else an exact decimal.
fn json_number(raw: &str) -> Result<ParamValue, String> {
    if let Ok(n) = raw.parse::<i64>() {
        return Ok(ParamValue::Int64(n));
    }
    if raw.contains(['e', 'E']) {
        return raw
            .parse::<f64>()
            .map(ParamValue::Float64)
            .map_err(|_| format!("JSON number {raw} is not a float"));
    }
    rust_decimal::Decimal::from_str_exact(raw)
        .map(ParamValue::Decimal)
        .map_err(|_| format!("JSON number {raw} is out of the exact numeric range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_text_keeps_every_type_json_carries() {
        let param = param_from_json_text(r#"{"a": 1, "d": 12.50, "f": 1.5e0, "s": "x", "n": null, "t": true, "l": [1, {"k": 2}]}"#)
            .expect("json");
        let ParamValue::Object(fields) = param else {
            panic!("an object");
        };
        let get = |key: &str| {
            fields
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value)
                .expect(key)
        };
        assert!(matches!(get("a"), ParamValue::Int64(1)));
        assert!(matches!(get("d"), ParamValue::Decimal(d) if d.to_string() == "12.50"));
        assert!(matches!(get("f"), ParamValue::Float64(f) if *f == 1.5));
        assert!(matches!(get("s"), ParamValue::Text(s) if s == "x"));
        assert!(matches!(get("n"), ParamValue::Null));
        assert!(matches!(get("t"), ParamValue::Bool(true)));
        assert!(
            matches!(get("l"), ParamValue::Array(items) if matches!(items[1], ParamValue::Object(_)))
        );
    }

    #[test]
    fn a_key_written_twice_keeps_its_last_value() {
        let ParamValue::Object(fields) =
            param_from_json_text(r#"{"a": 1, "b": 2, "a": 3}"#).expect("json")
        else {
            panic!("an object");
        };
        assert_eq!(fields.len(), 2);
        assert!(matches!(&fields[0], (key, ParamValue::Int64(3)) if key == "a"));
        assert!(matches!(&fields[1], (key, ParamValue::Int64(2)) if key == "b"));
    }

    /// A whole decimal and a non-finite float keep their type in every
    /// composite form.
    #[test]
    fn whole_decimals_and_non_finite_floats_render_typed() {
        let param = ParamValue::Object(vec![
            (
                "d".into(),
                ParamValue::Decimal("5".parse().expect("decimal")),
            ),
            ("n".into(), ParamValue::Float64(f64::NAN)),
            (
                "l".into(),
                ParamValue::Array(vec![ParamValue::Float64(f64::NEG_INFINITY)]),
            ),
        ]);
        assert_eq!(
            param_sql_text(&param),
            "jsonb_build_object('d', NUMERIC '5', 'n', 'NaN'::FLOAT8, \
             'l', ARRAY['-Infinity'::FLOAT8])"
        );
        let literal = param_object_literal_text(&param);
        assert_eq!(
            literal,
            "{ \"d\": DECIMAL '5', \"n\": 'NaN'::FLOAT8, \"l\": ['-Infinity'::FLOAT8] }"
        );
        let fields = crate::parser::object_literal::parse_object_literal(&literal)
            .expect("an object literal")
            .expect("parses");
        assert_eq!(
            fields["d"],
            nodedb_types::Value::Decimal("5".parse().expect("decimal"))
        );
        assert!(matches!(fields["n"], nodedb_types::Value::Float(f) if f.is_nan()));
        assert_eq!(
            fields["l"],
            nodedb_types::Value::Array(vec![nodedb_types::Value::Float(f64::NEG_INFINITY)])
        );
    }

    #[test]
    fn invalid_json_is_refused() {
        assert!(param_from_json_text("{").is_err());
    }

    #[test]
    fn composite_params_render_typed_sql() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let param = ParamValue::Object(vec![
            ("u".into(), ParamValue::Uuid(uuid.into())),
            (
                "l".into(),
                ParamValue::Array(vec![ParamValue::Int64(1), ParamValue::Null]),
            ),
            (
                "d".into(),
                ParamValue::Duration(nodedb_types::NdbDuration::from_micros(90)),
            ),
        ]);
        assert_eq!(
            param_sql_text(&param),
            format!(
                "jsonb_build_object('u', UUID '{uuid}', 'l', ARRAY[1, NULL], 'd', '90us'::DURATION)"
            )
        );
        assert_eq!(
            param_object_literal_text(&param),
            format!("{{ \"u\": UUID '{uuid}', \"l\": [1, NULL], \"d\": DURATION '90us' }}")
        );
    }

    #[test]
    fn json_text_round_trips_through_the_reader() {
        let param = ParamValue::Array(vec![
            ParamValue::Float64(2.0),
            ParamValue::Decimal("3.10".parse().expect("decimal")),
            ParamValue::Text("a\"b".into()),
        ]);
        let ParamValue::Array(items) =
            param_from_json_text(&param_json_text(&param)).expect("json")
        else {
            panic!("an array");
        };
        assert!(matches!(items[0], ParamValue::Float64(f) if f == 2.0));
        assert!(matches!(&items[1], ParamValue::Decimal(d) if d.to_string() == "3.10"));
        assert!(matches!(&items[2], ParamValue::Text(s) if s == "a\"b"));
    }
}
