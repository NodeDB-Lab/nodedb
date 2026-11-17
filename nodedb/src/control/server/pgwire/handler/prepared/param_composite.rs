// SPDX-License-Identifier: BUSL-1.1

//! Decoders for the composite parameter types: `json` / `jsonb`, `interval`,
//! and every array type, in the wire formats a client sends them in.
//!
//! - A JSON text binds as the object, array or scalar it spells, typed by
//!   the SQL numeric-literal rule (`nodedb_sql::params_composite`).
//! - An `interval` binds as an exact duration. Months have no fixed length,
//!   so an interval that carries months is refused (SQLSTATE 22008).
//! - A binary array binds as its elements (see `param_array`).
//! - A tagged NodeDB value binds as the typed object or array it encodes
//!   (see `nodedb_sql::params_value`).

use pgwire::api::Type;
use pgwire::error::PgWireResult;
use postgres_types::Kind;

use super::param_typed::pg_error;

/// The SQLSTATE of a malformed binary parameter.
const INVALID_BINARY: &str = "22P02";
/// The SQLSTATE of interval text that spells no interval.
const INVALID_DATETIME: &str = "22007";
/// The SQLSTATE of an interval no exact duration holds.
const DATETIME_OVERFLOW: &str = "22008";

/// Microseconds in a day, the unit an interval's day field counts.
const MICROS_PER_DAY: i64 = 86_400_000_000;

pub(super) fn malformed(index: usize, what: &str) -> pgwire::error::PgWireError {
    pg_error(
        INVALID_BINARY,
        format!(
            "invalid binary representation for parameter ${}: {what}",
            index + 1
        ),
    )
}

/// A JSON / JSONB text parameter.
pub(super) fn text_json(text: &str, index: usize) -> PgWireResult<nodedb_sql::ParamValue> {
    nodedb_sql::params_composite::param_from_json_text(text).map_err(|detail| {
        pg_error(
            "22P02",
            format!(
                "invalid input syntax for type json (parameter ${}): {detail}",
                index + 1
            ),
        )
    })
}

/// A binary JSON / JSONB parameter. Binary `jsonb` is a version byte (1)
/// and the JSON text. Binary `json` is the JSON text.
pub(super) fn binary_json(
    bytes: &[u8],
    jsonb: bool,
    index: usize,
) -> PgWireResult<nodedb_sql::ParamValue> {
    let text = if jsonb {
        match bytes.split_first() {
            Some((1, text)) => text,
            Some((version, _)) => {
                return Err(malformed(
                    index,
                    &format!("unknown jsonb version {version}"),
                ));
            }
            None => return Err(malformed(index, "empty jsonb payload")),
        }
    } else {
        bytes
    };
    let text = std::str::from_utf8(text).map_err(|_| malformed(index, "invalid UTF-8 JSON"))?;
    text_json(text, index)
}

/// A binary `interval`: 8-byte microseconds, 4-byte days, 4-byte months,
/// big-endian.
pub(super) fn binary_interval(
    bytes: &[u8],
    index: usize,
) -> PgWireResult<nodedb_types::NdbDuration> {
    let raw: [u8; 16] = bytes
        .try_into()
        .map_err(|_| malformed(index, &format!("expected 16 bytes, got {}", bytes.len())))?;
    let micros = i64::from_be_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]);
    let days = i32::from_be_bytes([raw[8], raw[9], raw[10], raw[11]]);
    let months = i32::from_be_bytes([raw[12], raw[13], raw[14], raw[15]]);
    let overflow = |why: &str| {
        pg_error(
            DATETIME_OVERFLOW,
            format!("interval parameter ${} {why}", index + 1),
        )
    };
    if months != 0 {
        return Err(overflow(
            "carries months, which have no fixed length; give it in days or smaller units",
        ));
    }
    i64::from(days)
        .checked_mul(MICROS_PER_DAY)
        .and_then(|day_micros| day_micros.checked_add(micros))
        .map(nodedb_types::NdbDuration::from_micros)
        .ok_or_else(|| overflow("is out of the duration range"))
}

/// A text `interval`: a shorthand (`90s`, `1h30m`, `250us`) or a spelled
/// form (`1 hour`, `2 days`).
pub(super) fn text_interval(text: &str, index: usize) -> PgWireResult<nodedb_types::NdbDuration> {
    if let Some(duration) = nodedb_types::NdbDuration::parse(text) {
        return Ok(duration);
    }
    nodedb_types::kv_parsing::parse_interval_to_ms(text.trim())
        .ok()
        .and_then(|ms| i64::try_from(ms).ok())
        .and_then(|ms| ms.checked_mul(1_000))
        .map(nodedb_types::NdbDuration::from_micros)
        .ok_or_else(|| {
            pg_error(
                INVALID_DATETIME,
                format!(
                    "invalid input syntax for type interval: \"{text}\" (parameter ${})",
                    index + 1
                ),
            )
        })
}

/// The parameter type of a tagged NodeDB value: PostgreSQL's `any`
/// pseudo-type (OID 2276), which no PostgreSQL client binds a value under.
pub(super) fn is_tagged_type(pg_type: &Type) -> bool {
    pg_type.oid() == nodedb_sql::params_value::TAGGED_PARAM_OID
}

/// A binary tagged parameter: the NodeDB tagged MessagePack of an object
/// or array whose leaves JSON cannot hold exactly. Each leaf keeps its
/// type.
pub(super) fn binary_tagged(bytes: &[u8], index: usize) -> PgWireResult<nodedb_sql::ParamValue> {
    nodedb_sql::params_value::param_from_tagged_msgpack(bytes)
        .map_err(|detail| malformed(index, &detail))
}

/// A text-format tagged parameter, which has no text form.
pub(super) fn text_tagged(index: usize) -> pgwire::error::PgWireError {
    pg_error(
        "0A000",
        format!(
            "parameter ${} of type any takes only the binary tagged form; \
             bind it in binary format",
            index + 1
        ),
    )
}

/// The element type of an array type, or `None` for any other type.
pub(super) fn array_member(pg_type: &Type) -> Option<&Type> {
    match pg_type.kind() {
        Kind::Array(member) => Some(member),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_interval_binds_as_an_exact_duration_and_months_are_refused() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1_500i64.to_be_bytes());
        payload.extend_from_slice(&1i32.to_be_bytes());
        payload.extend_from_slice(&0i32.to_be_bytes());
        assert_eq!(
            binary_interval(&payload, 0).expect("interval").micros,
            MICROS_PER_DAY + 1_500
        );
        payload[12..].copy_from_slice(&1i32.to_be_bytes());
        let error = binary_interval(&payload, 0).expect_err("months");
        assert!(error.to_string().contains("22008"), "{error}");
        assert_eq!(text_interval("90s", 0).expect("text").micros, 90_000_000);
    }

    #[test]
    fn a_tagged_object_keeps_its_leaf_types() {
        use std::collections::HashMap;
        let object = nodedb_types::Value::Object(HashMap::from([(
            "d".to_string(),
            nodedb_types::Value::Decimal("5".parse().expect("decimal")),
        )]));
        let bytes = zerompk::to_msgpack_vec(&object).expect("encode");
        let nodedb_sql::ParamValue::Object(fields) = binary_tagged(&bytes, 0).expect("tagged")
        else {
            panic!("an object");
        };
        assert!(
            matches!(&fields[0], (key, nodedb_sql::ParamValue::Decimal(d)) if key == "d" && d.scale() == 0)
        );
        let error = binary_tagged(&bytes[..bytes.len() - 1], 2).expect_err("truncated");
        assert!(error.to_string().contains("22P02"), "{error}");
        assert!(error.to_string().contains("$3"), "{error}");
        assert!(is_tagged_type(&Type::ANY));
        assert!(!is_tagged_type(&Type::JSONB));
    }

    #[test]
    fn jsonb_binds_as_a_typed_object() {
        let mut payload = vec![1u8];
        payload.extend_from_slice(br#"{"d": 12.50, "n": 1}"#);
        let nodedb_sql::ParamValue::Object(fields) = binary_json(&payload, true, 0).expect("jsonb")
        else {
            panic!("an object");
        };
        assert!(
            matches!(&fields[0], (key, nodedb_sql::ParamValue::Decimal(d)) if key == "d" && d.to_string() == "12.50")
        );
        assert!(matches!(&fields[1], (key, nodedb_sql::ParamValue::Int64(1)) if key == "n"));
    }
}
