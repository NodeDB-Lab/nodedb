// SPDX-License-Identifier: Apache-2.0

//! The typed parameters of `execute_sql` over pgwire.
//!
//! Each value binds in its PostgreSQL binary form under the OID of its
//! exact type, and the statement is prepared with these types, so the
//! server decodes each parameter to the typed value:
//! - A scalar binds by the rule a document field binds by
//!   ([`scalar_param`]): a Decimal as `numeric`, a UUID as `uuid`, an
//!   instant as `timestamptz` or `timestamp` by its kind, bytes as `bytea`.
//!   A ULID binds as its text.
//! - A duration binds as `interval`, its exact microseconds.
//! - An array whose elements share one scalar kind binds as the array type
//!   of that kind (`int8[]`, `numeric[]`, `uuid[]`, ...), `NULL` elements
//!   included. A vector binds as `float4[]`. Any other array binds as an
//!   object does.
//! - An object, or a mixed array, binds as `jsonb` when JSON holds every
//!   leaf exactly: `NULL`, a boolean, an integer, a finite float (written
//!   with an exponent), a string, a ULID, a decimal with a fractional part
//!   (its digits and scale), and a geometry (its GeoJSON object).
//! - Any other object or mixed array binds as a tagged NodeDB value: type
//!   `any` (OID [`nodedb_types::pg_compat::TAGGED_VALUE_PARAM_OID`]), whose
//!   binary form is the tagged MessagePack of the value. JSON holds a whole
//!   decimal only as an integer, a UUID, an instant, bytes and a duration
//!   only as text, and a non-finite float not at all. The tagged form keeps
//!   each of these typed. Genuine JSON keeps `jsonb`, so the server never
//!   reads user JSON as a tagged value.
//! - A geometry binds as its GeoJSON text, the form the server reads and
//!   renders geometry in. The server has no geometry parameter type, so the
//!   statement wraps the placeholder in `ST_GeomFromGeoJSON($n)` to store a
//!   geometry.
//!
//! A set, a regex, a range, a record and an array cell have no SQL
//! parameter form and are refused, naming the placeholder, at the top level
//! and inside an object or array.

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;
use tokio_postgres::types::{ToSql, Type};

use super::document_bind::{TypedParam, scalar_param};
use super::pg_param::{PgInstant, PgInterval, PgJsonb, PgTagged, PgUuid, TAGGED_TYPE};

/// The typed parameter of each value of `params`, in placeholder order.
pub(super) fn typed_params(params: &[Value]) -> NodeDbResult<Vec<TypedParam>> {
    params
        .iter()
        .enumerate()
        .map(|(index, value)| typed_param(index, value))
        .collect()
}

/// The declared types of `params`, in placeholder order.
pub(super) fn param_types(params: &[TypedParam]) -> Vec<Type> {
    params.iter().map(|(_, ty)| ty.clone()).collect()
}

fn typed_param(index: usize, value: &Value) -> NodeDbResult<TypedParam> {
    let refuse = |why: &str| {
        NodeDbError::bad_request(format!(
            "execute_sql parameter ${}: a {} value {why}; bind it as a scalar, an array, \
             an object, a duration or a geometry, or use NativeClient",
            index + 1,
            value.type_name()
        ))
    };
    let param: TypedParam = match value {
        Value::Ulid(text) => (Box::new(text.clone()), Type::TEXT),
        Value::Duration(d) => (Box::new(PgInterval(d.micros)), Type::INTERVAL),
        Value::Geometry(g) => (
            Box::new(nodedb_types::geometry::geojson_tree(g).to_string()),
            Type::TEXT,
        ),
        Value::Vector(v) => (Box::new(v.to_vec()), Type::FLOAT4_ARRAY),
        Value::Object(_) => composite_param(value).map_err(refuse)?,
        Value::Array(items) => match array_param(items).map_err(refuse)? {
            Some(param) => param,
            None => composite_param(value).map_err(refuse)?,
        },
        Value::Set(_)
        | Value::Regex(_)
        | Value::Range { .. }
        | Value::Record { .. }
        | Value::ArrayCell(_) => return Err(refuse("has no SQL parameter form")),
        scalar => match scalar_param(scalar).map_err(refuse)? {
            Some(param) => param,
            None => return Err(refuse("has no SQL parameter form")),
        },
    };
    Ok(param)
}

/// The parameter of an object or mixed array: `jsonb` when JSON holds every
/// leaf exactly, else the tagged NodeDB value.
fn composite_param(value: &Value) -> Result<TypedParam, &'static str> {
    if json_exact(value)? {
        return Ok((Box::new(PgJsonb(json_text(value)?)), Type::JSONB));
    }
    let tagged = zerompk::to_msgpack_vec(value).map_err(|_| "has no tagged MessagePack form")?;
    Ok((Box::new(PgTagged(tagged)), TAGGED_TYPE))
}

/// Whether the JSON text of `value` reads back on the server as exactly
/// `value`. `Err` for a leaf with no parameter form.
fn json_exact(value: &Value) -> Result<bool, &'static str> {
    Ok(match value {
        Value::Null
        | Value::Bool(_)
        | Value::Integer(_)
        | Value::String(_)
        | Value::Ulid(_)
        | Value::Geometry(_) => true,
        Value::Float(f) => f.is_finite(),
        // Digits without a fractional part read back as an integer.
        Value::Decimal(d) => d.scale() > 0,
        Value::Vector(v) => v.iter().all(|f| f.is_finite()),
        Value::Uuid(u) => {
            PgUuid::parse(u).ok_or("holds a UUID that is not valid")?;
            false
        }
        Value::DateTime(_) | Value::NaiveDateTime(_) | Value::Bytes(_) | Value::Duration(_) => {
            false
        }
        Value::Array(items) => {
            let mut exact = true;
            for item in items {
                exact &= json_exact(item)?;
            }
            exact
        }
        Value::Object(fields) => {
            let mut exact = true;
            for item in fields.values() {
                exact &= json_exact(item)?;
            }
            exact
        }
        Value::Set(_)
        | Value::Regex(_)
        | Value::Range { .. }
        | Value::Record { .. }
        | Value::ArrayCell(_) => return Err("holds a value that has no parameter form"),
        // `Value` is non-exhaustive: a kind added later has no parameter
        // form until one is given here.
        _ => return Err("holds a value that has no parameter form"),
    })
}

/// The scalar kind of an array element, for picking the array type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ElementKind {
    Bool,
    Int,
    Float,
    Decimal,
    Text,
    Uuid,
    Timestamptz,
    Timestamp,
    Bytes,
    Duration,
}

fn element_kind(value: &Value) -> Option<ElementKind> {
    Some(match value {
        Value::Bool(_) => ElementKind::Bool,
        Value::Integer(_) => ElementKind::Int,
        Value::Float(_) => ElementKind::Float,
        Value::Decimal(_) => ElementKind::Decimal,
        Value::String(_) | Value::Ulid(_) => ElementKind::Text,
        Value::Uuid(_) => ElementKind::Uuid,
        Value::DateTime(_) => ElementKind::Timestamptz,
        Value::NaiveDateTime(_) => ElementKind::Timestamp,
        Value::Bytes(_) => ElementKind::Bytes,
        Value::Duration(_) => ElementKind::Duration,
        _ => return None,
    })
}

/// The array-typed parameter of `items` when every non-`NULL` element has
/// one scalar kind, else `Ok(None)`: the array binds as `jsonb`. An array
/// of `NULL`s only, or an empty array, binds as `text[]`.
fn array_param(items: &[Value]) -> Result<Option<TypedParam>, &'static str> {
    let mut kind = None;
    for item in items.iter().filter(|item| !matches!(item, Value::Null)) {
        match (kind, element_kind(item)) {
            (_, None) => return Ok(None),
            (None, Some(found)) => kind = Some(found),
            (Some(seen), Some(found)) if seen != found => return Ok(None),
            (Some(_), Some(_)) => {}
        }
    }
    let Some(kind) = kind else {
        return Ok(Some(boxed(
            items.iter().map(|_| None::<String>).collect::<Vec<_>>(),
            Type::TEXT_ARRAY,
        )));
    };
    Ok(Some(match kind {
        ElementKind::Bool => boxed(
            elements(items, |v| match v {
                Value::Bool(b) => Ok(*b),
                _ => Err("is not a boolean"),
            })?,
            Type::BOOL_ARRAY,
        ),
        ElementKind::Int => boxed(
            elements(items, |v| match v {
                Value::Integer(i) => Ok(*i),
                _ => Err("is not an integer"),
            })?,
            Type::INT8_ARRAY,
        ),
        ElementKind::Float => boxed(
            elements(items, |v| match v {
                Value::Float(f) => Ok(*f),
                _ => Err("is not a float"),
            })?,
            Type::FLOAT8_ARRAY,
        ),
        ElementKind::Decimal => boxed(
            elements(items, |v| match v {
                Value::Decimal(d) => Ok(*d),
                _ => Err("is not a decimal"),
            })?,
            Type::NUMERIC_ARRAY,
        ),
        ElementKind::Text => boxed(
            elements(items, |v| match v {
                Value::String(s) | Value::Ulid(s) => Ok(s.clone()),
                _ => Err("is not text"),
            })?,
            Type::TEXT_ARRAY,
        ),
        ElementKind::Uuid => boxed(
            elements(items, |v| match v {
                Value::Uuid(u) => PgUuid::parse(u).ok_or("holds a UUID that is not valid"),
                _ => Err("is not a UUID"),
            })?,
            Type::UUID_ARRAY,
        ),
        ElementKind::Timestamptz => boxed(
            elements(items, |v| match v {
                Value::DateTime(at) => instant(at.micros),
                _ => Err("is not a timestamptz"),
            })?,
            Type::TIMESTAMPTZ_ARRAY,
        ),
        ElementKind::Timestamp => boxed(
            elements(items, |v| match v {
                Value::NaiveDateTime(at) => instant(at.micros),
                _ => Err("is not a timestamp"),
            })?,
            Type::TIMESTAMP_ARRAY,
        ),
        ElementKind::Bytes => boxed(
            elements(items, |v| match v {
                Value::Bytes(b) => Ok(b.clone()),
                _ => Err("is not bytes"),
            })?,
            Type::BYTEA_ARRAY,
        ),
        ElementKind::Duration => boxed(
            elements(items, |v| match v {
                Value::Duration(d) => Ok(PgInterval(d.micros)),
                _ => Err("is not a duration"),
            })?,
            Type::INTERVAL_ARRAY,
        ),
    }))
}

fn instant(micros: i64) -> Result<PgInstant, &'static str> {
    PgInstant::from_unix_micros(micros).ok_or("holds an instant outside the PostgreSQL range")
}

/// Each element of `items` by `element`, `NULL` as `None`.
fn elements<T>(
    items: &[Value],
    element: impl Fn(&Value) -> Result<T, &'static str>,
) -> Result<Vec<Option<T>>, &'static str> {
    items
        .iter()
        .map(|item| match item {
            Value::Null => Ok(None),
            other => element(other).map(Some),
        })
        .collect()
}

fn boxed<T: ToSql + Sync + Send + 'static>(value: T, ty: Type) -> TypedParam {
    (Box::new(value), ty)
}

/// The JSON text of `value`. See the module doc for what each kind becomes.
fn json_text(value: &Value) -> Result<String, &'static str> {
    Ok(match value {
        Value::Null => "null".to_owned(),
        Value::Bool(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) if f.is_finite() => format!("{f:e}"),
        Value::Float(_) => return Err("holds a non-finite float, which JSON cannot hold"),
        Value::Decimal(d) => d.to_string(),
        Value::String(s) | Value::Ulid(s) | Value::Uuid(s) => json_string(s),
        Value::DateTime(at) | Value::NaiveDateTime(at) => json_string(&at.to_iso8601()),
        Value::Bytes(b) => json_string(&nodedb_types::bytea_text::format_bytea_hex(b)),
        Value::Duration(d) => json_string(&format!("{}us", d.micros)),
        Value::Geometry(g) => nodedb_types::geometry::geojson_tree(g).to_string(),
        Value::Vector(v) => {
            if v.iter().any(|f| !f.is_finite()) {
                return Err("holds a vector with a non-finite element, which JSON cannot hold");
            }
            let items: Vec<String> = v.iter().map(|f| format!("{:e}", f64::from(*f))).collect();
            format!("[{}]", items.join(","))
        }
        Value::Array(items) => {
            let items = items.iter().map(json_text).collect::<Result<Vec<_>, _>>()?;
            format!("[{}]", items.join(","))
        }
        Value::Object(fields) => {
            let mut fields: Vec<_> = fields.iter().collect();
            fields.sort_by(|a, b| a.0.cmp(b.0));
            let fields = fields
                .into_iter()
                .map(|(key, item)| Ok(format!("{}:{}", json_string(key), json_text(item)?)))
                .collect::<Result<Vec<_>, &'static str>>()?;
            format!("{{{}}}", fields.join(","))
        }
        // A set, a regex, a range, a record and an array cell.
        _ => return Err("holds a value that has no JSON form"),
    })
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use nodedb_types::NdbDateTime;

    #[test]
    fn typed_values_bind_under_their_exact_type() {
        let at = NdbDateTime::from_micros(1_700_000_000_000_000);
        let params = typed_params(&[
            Value::Null,
            Value::Bool(true),
            Value::Integer(7),
            Value::Float(2.5),
            Value::String("hi".into()),
            Value::Decimal("12.50".parse().expect("decimal")),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            Value::DateTime(at),
            Value::NaiveDateTime(at),
            Value::Bytes(vec![1, 2, 3]),
            Value::Ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV".into()),
            Value::Duration(nodedb_types::NdbDuration::from_micros(90)),
        ])
        .expect("every scalar binds");
        assert_eq!(
            param_types(&params),
            vec![
                Type::TEXT,
                Type::BOOL,
                Type::INT8,
                Type::FLOAT8,
                Type::TEXT,
                Type::NUMERIC,
                Type::UUID,
                Type::TIMESTAMPTZ,
                Type::TIMESTAMP,
                Type::BYTEA,
                Type::TEXT,
                Type::INTERVAL,
            ]
        );
    }

    #[test]
    fn arrays_bind_as_the_array_type_of_their_element_kind() {
        let types = |values: Vec<Value>| {
            param_types(&typed_params(&[Value::Array(values)]).expect("array binds"))
        };
        assert_eq!(
            types(vec![Value::Integer(1), Value::Null]),
            vec![Type::INT8_ARRAY]
        );
        assert_eq!(
            types(vec![Value::Decimal("1.50".parse().expect("decimal"))]),
            vec![Type::NUMERIC_ARRAY]
        );
        assert_eq!(
            types(vec![Value::Uuid(
                "550e8400-e29b-41d4-a716-446655440000".into()
            )]),
            vec![Type::UUID_ARRAY]
        );
        assert_eq!(types(Vec::new()), vec![Type::TEXT_ARRAY]);
        assert_eq!(
            types(vec![Value::Integer(1), Value::String("x".into())]),
            vec![Type::JSONB]
        );
        assert_eq!(
            types(vec![Value::Array(vec![Value::Integer(1)])]),
            vec![Type::JSONB]
        );
    }

    #[test]
    fn an_object_binds_as_jsonb_keeping_exact_numbers() {
        let object = Value::Object(HashMap::from([
            (
                "d".to_string(),
                Value::Decimal("12.50".parse().expect("decimal")),
            ),
            ("f".to_string(), Value::Float(1.5)),
            ("n".to_string(), Value::Integer(2)),
            ("s".to_string(), Value::String("a\"b".into())),
        ]));
        assert_eq!(
            json_text(&object).expect("json"),
            r#"{"d":12.50,"f":1.5e0,"n":2,"s":"a\"b"}"#
        );
        assert_eq!(
            param_types(&typed_params(&[object]).expect("object binds")),
            vec![Type::JSONB]
        );
    }

    #[test]
    fn a_kind_without_a_parameter_form_is_refused_naming_its_placeholder() {
        let err = typed_params(&[Value::Integer(1), Value::Regex("a.*".into())])
            .expect_err("a regex has no parameter form");
        assert!(err.to_string().contains("$2"), "{err}");
        let err = typed_params(&[Value::Uuid("nope".into())]).expect_err("bad uuid text");
        assert!(err.to_string().contains("$1"), "{err}");
        let err = typed_params(&[
            Value::Null,
            Value::Object(HashMap::from([(
                "r".to_string(),
                Value::Regex("a.*".into()),
            )])),
        ])
        .expect_err("a nested regex has no parameter form");
        assert!(err.to_string().contains("$2"), "{err}");
        let err = typed_params(&[Value::Array(vec![
            Value::Integer(1),
            Value::Uuid("nope".into()),
        ])])
        .expect_err("a nested bad uuid");
        assert!(err.to_string().contains("$1"), "{err}");
    }

    /// An object or mixed array JSON holds exactly stays `jsonb`. One with a
    /// leaf JSON cannot hold exactly binds as the tagged value, and the
    /// tagged bytes decode to the same value.
    #[test]
    fn objects_bind_as_jsonb_only_when_json_holds_every_leaf() {
        let types = |value: Value| param_types(&typed_params(&[value]).expect("binds"));
        let exact = Value::Object(HashMap::from([
            (
                "d".to_string(),
                Value::Decimal("1.50".parse().expect("decimal")),
            ),
            ("f".to_string(), Value::Float(1.5)),
            ("s".to_string(), Value::String("x".into())),
        ]));
        assert_eq!(types(exact), vec![Type::JSONB]);
        for leaf in [
            Value::Decimal("5".parse().expect("decimal")),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            Value::DateTime(NdbDateTime::from_micros(7)),
            Value::NaiveDateTime(NdbDateTime::from_micros(7)),
            Value::Bytes(vec![1]),
            Value::Duration(nodedb_types::NdbDuration::from_micros(90)),
            Value::Float(f64::NAN),
            Value::Vector(std::sync::Arc::from([f32::INFINITY])),
        ] {
            let object = Value::Object(HashMap::from([("k".to_string(), leaf.clone())]));
            assert_eq!(types(object), vec![TAGGED_TYPE], "{leaf:?}");
            let mixed = Value::Array(vec![Value::String("x".into()), leaf.clone()]);
            assert_eq!(types(mixed), vec![TAGGED_TYPE], "{leaf:?}");
        }
        let object = Value::Object(HashMap::from([(
            "d".to_string(),
            Value::Decimal("5".parse().expect("decimal")),
        )]));
        let (param, _) = composite_param(&object).expect("tagged");
        let mut out = bytes::BytesMut::new();
        param
            .to_sql_checked(&TAGGED_TYPE, &mut out)
            .expect("encodes");
        let decoded: Value = zerompk::from_msgpack(&out).expect("tagged value");
        assert_eq!(decoded, object);
    }
}
