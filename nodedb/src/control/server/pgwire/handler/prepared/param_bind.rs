// SPDX-License-Identifier: BUSL-1.1

//! Convert pgwire extended-query portal parameters (text or binary wire
//! format) into typed `nodedb_sql::ParamValue` for AST/DSL binding.

use bytes::Bytes;
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use postgres_types::FromSql;

use super::param_array::binary_array;
use super::param_composite::{
    array_member, binary_interval, binary_json, binary_tagged, is_tagged_type, text_interval,
    text_json, text_tagged,
};
use super::param_typed::{
    binary_timestamp, binary_uuid, pg_error, text_bytea, text_timestamp, text_uuid,
};

/// Convert pgwire portal parameters to typed `ParamValue` for AST-level binding.
///
/// Uses per-parameter format codes from the pgwire 0.38 `Format` API to determine
/// whether each parameter was sent in text or binary format.
///
/// Binary-format `BOOL`/`INT2`/`INT4`/`INT8`/`FLOAT4`/`FLOAT8` are decoded directly
/// via their well-specified `postgres_types::FromSql` binary encodings.
/// Binary-format `TEXT`/`VARCHAR`/`BPCHAR`/`UNKNOWN` fall through to the text path
/// below — the binary wire encoding of these types *is* the text bytes, so
/// reusing `pgwire_text_to_param` is correct, not a workaround.
///
/// Binary-format `NUMERIC` is decoded via `rust_decimal`'s `FromSql` impl
/// (the variable-length base-10000 digit encoding), producing the same
/// `ParamValue::Decimal` as the text path. Overflow, NaN, and +/-Infinity
/// payloads come back as typed errors mapped to SQLSTATE 22P02.
///
/// Binary-format `TIMESTAMP` and `TIMESTAMPTZ` are decoded directly: an
/// 8-byte big-endian microsecond offset from 2000-01-01T00:00:00Z, converted
/// to `NdbDateTime` (which stores microseconds since the Unix epoch).
///
/// Binary-format `UUID` is its 16 bytes, decoded to `ParamValue::Uuid`.
/// Binary-format `BYTEA` is the raw bytes, decoded to `ParamValue::Bytes`.
///
/// Binary-format `JSON` / `JSONB` binds as the typed object, array or
/// scalar its text spells, `INTERVAL` as an exact duration, and an array
/// type as its elements, each by the rule for the element type (see
/// `param_composite` and `param_array`). A parameter of type `any` (OID
/// 2276) is a tagged NodeDB value: its binary form is the tagged
/// MessagePack of an object or array, every leaf typed (see
/// `nodedb_sql::params_value`).
///
/// Every other binary-format type (DATE, TIME, user-defined types, ...) is
/// rejected with SQLSTATE 0A000 rather than silently mis-decoded as UTF-8
/// text — its bytes may happen to be valid UTF-8 without being a valid text
/// representation of the type.
pub(super) fn convert_portal_params(
    params: &[Option<Bytes>],
    param_types: &[Option<Type>],
    param_format: &pgwire::api::portal::Format,
) -> PgWireResult<Vec<nodedb_sql::ParamValue>> {
    let mut result = Vec::with_capacity(params.len());
    for (i, param) in params.iter().enumerate() {
        let pg_type = param_types
            .get(i)
            .and_then(|t| t.as_ref())
            .unwrap_or(&Type::UNKNOWN);

        let pv = match param {
            None => nodedb_sql::ParamValue::Null,
            Some(bytes) => {
                if param_format.is_binary(i) {
                    convert_binary_param(bytes, pg_type, i)?
                } else {
                    let text = decode_utf8_param(bytes, i)?;
                    pgwire_text_to_param(text, pg_type, i)?
                }
            }
        };
        result.push(pv);
    }
    Ok(result)
}

/// Decode a single binary-format parameter.
fn convert_binary_param(
    bytes: &Bytes,
    pg_type: &Type,
    index: usize,
) -> PgWireResult<nodedb_sql::ParamValue> {
    if let Some(member) = array_member(pg_type) {
        return binary_array(bytes, member, index, &convert_binary_param);
    }
    if is_tagged_type(pg_type) {
        return binary_tagged(bytes, index);
    }
    match *pg_type {
        Type::BOOL => {
            decode_binary::<bool>(bytes, pg_type, index).map(nodedb_sql::ParamValue::Bool)
        }
        Type::INT2 => {
            decode_binary::<i16>(bytes, pg_type, index).map(nodedb_sql::ParamValue::Int16)
        }
        Type::INT4 => {
            decode_binary::<i32>(bytes, pg_type, index).map(nodedb_sql::ParamValue::Int32)
        }
        Type::INT8 => {
            decode_binary::<i64>(bytes, pg_type, index).map(nodedb_sql::ParamValue::Int64)
        }
        Type::FLOAT4 => decode_binary::<f32>(bytes, pg_type, index)
            .map(|v| nodedb_sql::ParamValue::Float64(v as f64)),
        Type::FLOAT8 => {
            decode_binary::<f64>(bytes, pg_type, index).map(nodedb_sql::ParamValue::Float64)
        }
        // Binary wire bytes for these types are already the text
        // representation — reuse the text path rather than duplicate it.
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::UNKNOWN => {
            let text = decode_utf8_param(bytes, index)?;
            pgwire_text_to_param(text, pg_type, index)
        }
        Type::NUMERIC => decode_binary::<rust_decimal::Decimal>(bytes, pg_type, index)
            .map(nodedb_sql::ParamValue::Decimal),
        Type::TIMESTAMP => binary_timestamp(bytes, index).map(nodedb_sql::ParamValue::Timestamp),
        Type::TIMESTAMPTZ => {
            binary_timestamp(bytes, index).map(nodedb_sql::ParamValue::Timestamptz)
        }
        Type::UUID => binary_uuid(bytes, index).map(nodedb_sql::ParamValue::Uuid),
        Type::BYTEA => Ok(nodedb_sql::ParamValue::Bytes(bytes.to_vec())),
        Type::JSON => binary_json(bytes, false, index),
        Type::JSONB => binary_json(bytes, true, index),
        Type::INTERVAL => binary_interval(bytes, index).map(nodedb_sql::ParamValue::Duration),
        // Every other binary type: refuse rather than silently mis-decode
        // as UTF-8 text (its bytes may happen to be valid UTF-8 without
        // being a valid text representation of the type).
        _ => Err(binary_unsupported_error(pg_type.name(), index)),
    }
}

/// Decode a parameter's raw bytes as UTF-8 text, mapping a decode failure to
/// a typed pgwire error (SQLSTATE 22021 - character_not_in_repertoire).
/// Shared by the text-format path and the binary TEXT/VARCHAR/BPCHAR/UNKNOWN
/// arm, whose wire bytes are already the text representation.
fn decode_utf8_param(bytes: &[u8], index: usize) -> PgWireResult<&str> {
    std::str::from_utf8(bytes).map_err(|_| {
        pg_error(
            "22021",
            format!("invalid UTF-8 in parameter ${}", index + 1),
        )
    })
}

/// The parameter an integer `n` of an `INT2`, `INT4` or `INT8` parameter
/// binds as, at that width. `n` past the width is 22003.
fn int_param(
    n: i64,
    pg_type: &Type,
    text: &str,
    index: usize,
) -> PgWireResult<nodedb_sql::ParamValue> {
    let out_of_range = |type_name: &str| {
        pg_error(
            "22003",
            format!(
                "value \"{text}\" is out of range for type {type_name} (parameter ${})",
                index + 1
            ),
        )
    };
    Ok(match *pg_type {
        Type::INT2 => {
            nodedb_sql::ParamValue::Int16(i16::try_from(n).map_err(|_| out_of_range("smallint"))?)
        }
        Type::INT4 => {
            nodedb_sql::ParamValue::Int32(i32::try_from(n).map_err(|_| out_of_range("integer"))?)
        }
        _ => nodedb_sql::ParamValue::Int64(n),
    })
}

/// Build a typed pgwire error (SQLSTATE 22P02 - invalid_text_representation)
/// for a text-format NUMERIC parameter that cannot be parsed as an exact
/// `rust_decimal::Decimal` (parse failure or magnitude beyond the 96-bit
/// mantissa). Mirrors `decode_binary`'s 22P02 wording but describes a text,
/// not binary, format failure — the two paths must not share prose.
fn numeric_text_invalid_error(index: usize) -> PgWireError {
    pg_error(
        "22P02",
        format!(
            "invalid NUMERIC text representation for parameter ${}",
            index + 1
        ),
    )
}

fn binary_unsupported_error(type_name: &str, index: usize) -> PgWireError {
    pg_error(
        "0A000",
        format!(
            "binary {type_name} parameter format is not supported for parameter ${n}; \
             use text format",
            n = index + 1
        ),
    )
}

/// Decode a binary parameter payload via `postgres_types::FromSql`, mapping
/// a decode failure to a typed pgwire error (SQLSTATE 22P02 -
/// invalid_binary_representation), never a panic.
fn decode_binary<'a, T: FromSql<'a>>(
    bytes: &'a [u8],
    pg_type: &Type,
    index: usize,
) -> PgWireResult<T> {
    T::from_sql(pg_type, bytes).map_err(|e| {
        pg_error(
            "22P02",
            format!(
                "invalid binary representation for parameter ${}: {e}",
                index + 1
            ),
        )
    })
}

/// Convert a pgwire text parameter + declared type to a typed
/// `ParamValue` for AST/DSL binding.
///
/// # Type coverage
///
/// Natively decoded: `BOOL`, `INT2`/`INT4`/`INT8`, `FLOAT4`/`FLOAT8`/
/// `NUMERIC`, `TIMESTAMP`, `TIMESTAMPTZ`, `UUID`, `BYTEA`, `TEXT`/`VARCHAR`
/// (implicit via fall-through), and `UNKNOWN` (the untyped-driver path).
///
/// # TIMESTAMP / TIMESTAMPTZ / UUID / BYTEA
///
/// Text-format parameters of these types decode to the typed `ParamValue`
/// (`Timestamp`, `Timestamptz`, `Uuid`, `Bytes`). The binder writes each as
/// a typed literal, so a schemaless field stores the typed value, never its
/// text. A BYTEA parameter reads the hex (`\x...`) or the escape form.
///
/// Like NUMERIC, a parse failure errors rather than falling back to
/// `ParamValue::Text`: text bound for a typed parameter that spells no value
/// of the type would otherwise be stored as a string. A timestamp refuses
/// with SQLSTATE 22007 (`invalid_datetime_format`), a UUID or BYTEA with
/// 22P02 (`invalid_text_representation`), PostgreSQL's codes for the same
/// input.
///
/// # NUMERIC
///
/// Text-format NUMERIC is parsed as an exact `rust_decimal::Decimal`, never a
/// lossy `f64`. Unlike every other native-type arm, a parse failure here does
/// NOT fall through to `ParamValue::Text` — `SqlValue::Decimal` is itself
/// `rust_decimal::Decimal`, so a value this parser cannot represent (parse
/// failure, or magnitude beyond the 96-bit mantissa) cannot be represented
/// anywhere downstream either, and silently passing it through as text would
/// be exactly the silent precision loss this path exists to avoid. It errors
/// with SQLSTATE 22P02, matching the binary NUMERIC path's error code (see
/// `convert_binary_param`).
///
/// # Fallback policy (catch-all arm)
///
/// `JSON` / `JSONB` text binds as the typed value it spells, and
/// `INTERVAL` text as a duration; text that spells neither is refused.
///
/// Types the bind layer does not decode natively — `DATE`, `TIME`,
/// text-format arrays, and user-defined types —
/// fall through to `ParamValue::Text(text)`. The pgwire text representation of
/// these types is well-defined and the AST bind emits it as a
/// `SingleQuotedString`. Downstream, the planner/engine type-coerces the text
/// via the same path used for literal strings in simple-query SQL. NUMERIC
/// and the typed types above are the exceptions to this policy.
///
/// Binary-format parameters are handled at a layer above this function
/// (see `convert_portal_params`); only binary TEXT/VARCHAR/BPCHAR/UNKNOWN
/// reach this function with binary-sourced bytes (which are already text).
///
/// # Why not error on unknown types
///
/// Postgres itself accepts text representations of every built-in type through
/// the extended-query protocol; refusing here would break drivers that
/// legitimately send dates/UUIDs/etc. as text.
pub(super) fn pgwire_text_to_param(
    text: &str,
    pg_type: &Type,
    index: usize,
) -> PgWireResult<nodedb_sql::ParamValue> {
    if is_tagged_type(pg_type) {
        return Err(text_tagged(index));
    }
    Ok(match *pg_type {
        Type::BOOL => {
            let lower = text.to_lowercase();
            if lower == "t" || lower == "true" || lower == "1" {
                nodedb_sql::ParamValue::Bool(true)
            } else if lower == "f" || lower == "false" || lower == "0" {
                nodedb_sql::ParamValue::Bool(false)
            } else {
                nodedb_sql::ParamValue::Text(text.to_string())
            }
        }
        // An integer parameter keeps its declared width. Text past that
        // width is 22003, as PostgreSQL's integer input refuses it.
        Type::INT2 | Type::INT4 | Type::INT8 => match text.trim().parse::<i64>() {
            Ok(n) => int_param(n, pg_type, text, index)?,
            Err(_) => nodedb_sql::ParamValue::Text(text.to_string()),
        },
        Type::FLOAT4 | Type::FLOAT8 => {
            if let Ok(f) = text.parse::<f64>() {
                nodedb_sql::ParamValue::Float64(f)
            } else {
                nodedb_sql::ParamValue::Text(text.to_string())
            }
        }
        Type::NUMERIC => {
            // Parse NUMERIC as exact Decimal, not lossy f64. Unlike every
            // other arm here, a parse failure errors rather than falling
            // back to ParamValue::Text — Decimal is the terminal
            // representation, so anything it can't parse can't be
            // represented downstream either.
            let d = rust_decimal::Decimal::from_str_exact(text)
                .map_err(|_| numeric_text_invalid_error(index))?;
            nodedb_sql::ParamValue::Decimal(d)
        }
        Type::TIMESTAMP => {
            nodedb_sql::ParamValue::Timestamp(text_timestamp(text, "timestamp", index)?)
        }
        Type::TIMESTAMPTZ => nodedb_sql::ParamValue::Timestamptz(text_timestamp(
            text,
            "timestamp with time zone",
            index,
        )?),
        Type::UUID => nodedb_sql::ParamValue::Uuid(text_uuid(text, index)?),
        Type::BYTEA => nodedb_sql::ParamValue::Bytes(text_bytea(text, index)?),
        Type::JSON | Type::JSONB => text_json(text, index)?,
        Type::INTERVAL => nodedb_sql::ParamValue::Duration(text_interval(text, index)?),
        // Text-passthrough types: wire-format text is already the
        // canonical representation. Engine performs type coercion.
        _ => nodedb_sql::ParamValue::Text(text.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use pgwire::api::portal::Format;

    use super::*;

    fn text_format() -> Format {
        Format::UnifiedText
    }

    fn binary_format() -> Format {
        Format::UnifiedBinary
    }

    #[test]
    fn convert_null_param() {
        let params = vec![None];
        let types = vec![Some(Type::INT8)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], nodedb_sql::ParamValue::Null));
    }

    #[test]
    fn convert_typed_params() {
        let params = vec![
            Some(Bytes::from_static(b"42")),
            Some(Bytes::from_static(b"hello")),
            Some(Bytes::from_static(b"true")),
        ];
        let types = vec![Some(Type::INT8), Some(Type::TEXT), Some(Type::BOOL)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert!(matches!(result[0], nodedb_sql::ParamValue::Int64(42)));
        assert!(matches!(&result[1], nodedb_sql::ParamValue::Text(s) if s == "hello"));
        assert!(matches!(result[2], nodedb_sql::ParamValue::Bool(true)));
    }

    #[test]
    fn convert_float_param() {
        let params = vec![Some(Bytes::from_static(b"2.78"))];
        let types = vec![Some(Type::FLOAT8)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert!(
            matches!(result[0], nodedb_sql::ParamValue::Float64(f) if (f - 2.78).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn convert_numeric_text_to_decimal() {
        let params = vec![Some(Bytes::from_static(b"123.45"))];
        let types = vec![Some(Type::NUMERIC)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        match &result[0] {
            nodedb_sql::ParamValue::Decimal(decimal) => assert_eq!(decimal.to_string(), "123.45"),
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    fn assert_binary_type_rejected(ty: Type, bytes: &'static [u8], name: &str) {
        let params = vec![Some(Bytes::from_static(bytes))];
        let types = vec![Some(ty)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        let message = error.to_string();
        assert!(message.contains(name) || message.contains("0A000"));
    }

    #[test]
    fn convert_date_binary_returns_error() {
        // Pins the silent-misdecode fix: an unmodelled binary type must be
        // refused with 0A000, never guessed at as UTF-8 text.
        assert_binary_type_rejected(Type::DATE, &[0u8; 4], "0A000");
    }

    /// A binary UUID is its 16 bytes and a binary BYTEA its raw bytes, each
    /// decoded to its typed parameter.
    #[test]
    fn convert_uuid_and_bytea_binary_decode_typed() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let uuid_bytes = nodedb_types::id_gen::uuid_to_bytes(uuid).expect("valid uuid");
        let params = vec![
            Some(Bytes::from(uuid_bytes.to_vec())),
            Some(Bytes::from_static(&[0, 0xff, 7])),
        ];
        let types = vec![Some(Type::UUID), Some(Type::BYTEA)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(matches!(&result[0], nodedb_sql::ParamValue::Uuid(u) if u == uuid));
        assert!(matches!(&result[1], nodedb_sql::ParamValue::Bytes(b) if b == &[0, 0xff, 7]));
    }

    #[test]
    fn convert_uuid_binary_wrong_length_returns_22p02() {
        let params = vec![Some(Bytes::from_static(&[0u8; 15]))];
        let types = vec![Some(Type::UUID)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"), "{error}");
    }

    /// Build a PostgreSQL binary NUMERIC payload: ndigits/weight/sign/dscale
    /// header followed by base-10000 digit groups, all big-endian i16.
    fn numeric_binary(digits: &[i16], weight: i16, sign: u16, dscale: u16) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + digits.len() * 2);
        bytes.extend_from_slice(&(digits.len() as i16).to_be_bytes());
        bytes.extend_from_slice(&weight.to_be_bytes());
        bytes.extend_from_slice(&sign.to_be_bytes());
        bytes.extend_from_slice(&dscale.to_be_bytes());
        for d in digits {
            bytes.extend_from_slice(&d.to_be_bytes());
        }
        bytes
    }

    #[test]
    fn convert_numeric_binary_decodes_to_decimal() {
        // 123.45: digit groups [0123, 4500], weight 0, dscale 2, positive.
        let bytes = numeric_binary(&[123, 4500], 0, 0x0000, 2);
        let params = vec![Some(Bytes::from(bytes))];
        let types = vec![Some(Type::NUMERIC)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        match &result[0] {
            nodedb_sql::ParamValue::Decimal(decimal) => assert_eq!(decimal.to_string(), "123.45"),
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn convert_numeric_binary_nan_returns_22p02() {
        let bytes = numeric_binary(&[], 0, 0xC000, 0);
        let params = vec![Some(Bytes::from(bytes))];
        let types = vec![Some(Type::NUMERIC)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn convert_numeric_binary_positive_infinity_returns_22p02() {
        let bytes = numeric_binary(&[], 0, 0xD000, 0);
        let params = vec![Some(Bytes::from(bytes))];
        let types = vec![Some(Type::NUMERIC)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn convert_numeric_binary_negative_infinity_returns_22p02() {
        let bytes = numeric_binary(&[], 0, 0xF000, 0);
        let params = vec![Some(Bytes::from(bytes))];
        let types = vec![Some(Type::NUMERIC)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn convert_numeric_binary_overflow_returns_22p02_not_panic() {
        // 10 digit groups of 9999 at weight 9 vastly exceeds Decimal's
        // 96-bit mantissa (~29 decimal digits).
        let digits = [9999i16; 10];
        let bytes = numeric_binary(&digits, 9, 0x0000, 0);
        let params = vec![Some(Bytes::from(bytes))];
        let types = vec![Some(Type::NUMERIC)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn convert_timestamp_binary_decodes_epoch() {
        // Raw 0 is the PostgreSQL binary epoch (2000-01-01T00:00:00Z), which
        // is 946_684_800_000_000 microseconds after the Unix epoch.
        let params = vec![Some(Bytes::from(0i64.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::TIMESTAMP)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        match &result[0] {
            nodedb_sql::ParamValue::Timestamp(dt) => {
                assert_eq!(dt.micros, 946_684_800_000_000);
            }
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn convert_timestamp_binary_decodes_unix_epoch() {
        // Pre-2000 value: -946_684_800_000_000 microseconds from the
        // PostgreSQL epoch lands exactly on the Unix epoch (micros = 0).
        let params = vec![Some(Bytes::from(
            (-946_684_800_000_000i64).to_be_bytes().to_vec(),
        ))];
        let types = vec![Some(Type::TIMESTAMP)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        match &result[0] {
            nodedb_sql::ParamValue::Timestamp(dt) => assert_eq!(dt.micros, 0),
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn convert_timestamp_binary_decodes_pre_1970_without_panic() {
        // Well before the Unix epoch: must decode to negative micros, not
        // panic or clamp.
        let params = vec![Some(Bytes::from(
            (-2_000_000_000_000_000i64).to_be_bytes().to_vec(),
        ))];
        let types = vec![Some(Type::TIMESTAMP)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        match &result[0] {
            nodedb_sql::ParamValue::Timestamp(dt) => assert!(dt.micros < 0),
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn convert_timestamptz_binary_produces_timestamptz_variant() {
        // Pins that TIMESTAMPTZ is not conflated with TIMESTAMP.
        let params = vec![Some(Bytes::from(0i64.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::TIMESTAMPTZ)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(matches!(result[0], nodedb_sql::ParamValue::Timestamptz(_)));
    }

    #[test]
    fn convert_timestamp_binary_wrong_length_returns_22p02() {
        for bytes in [vec![0u8; 4], vec![0u8; 9]] {
            let params = vec![Some(Bytes::from(bytes))];
            let types = vec![Some(Type::TIMESTAMP)];
            let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
            assert!(error.to_string().contains("22P02"));
        }
    }

    #[test]
    fn convert_timestamp_binary_i64_max_returns_22p02() {
        // PostgreSQL's +infinity sentinel: rebasing to the Unix epoch would
        // overflow i64, so this must error rather than wrap or panic.
        let params = vec![Some(Bytes::from(i64::MAX.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::TIMESTAMP)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn convert_timestamp_binary_i64_min_returns_22p02() {
        // PostgreSQL's -infinity sentinel: same overflow-must-error rule.
        let params = vec![Some(Bytes::from(i64::MIN.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::TIMESTAMP)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn convert_bool_binary() {
        let params = vec![Some(Bytes::from_static(&[1u8]))];
        let types = vec![Some(Type::BOOL)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(matches!(result[0], nodedb_sql::ParamValue::Bool(true)));
    }

    #[test]
    fn convert_int2_binary_keeps_its_width() {
        let params = vec![Some(Bytes::from((-1i16).to_be_bytes().to_vec()))];
        let types = vec![Some(Type::INT2)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(matches!(result[0], nodedb_sql::ParamValue::Int16(-1)));
    }

    /// Text integer parameters keep their declared width, and text past it
    /// is 22003.
    #[test]
    fn convert_int_text_keeps_its_width() {
        let convert = |text: &'static [u8], ty: Type| {
            convert_portal_params(
                &[Some(Bytes::from_static(text))],
                &[Some(ty)],
                &text_format(),
            )
        };
        assert!(matches!(
            convert(b"7", Type::INT2).unwrap()[0],
            nodedb_sql::ParamValue::Int16(7)
        ));
        assert!(matches!(
            convert(b"7", Type::INT4).unwrap()[0],
            nodedb_sql::ParamValue::Int32(7)
        ));
        assert!(matches!(
            convert(b"3000000000", Type::INT8).unwrap()[0],
            nodedb_sql::ParamValue::Int64(3_000_000_000)
        ));
        assert!(
            convert(b"3000000000", Type::INT4)
                .unwrap_err()
                .to_string()
                .contains("22003")
        );
        assert!(
            convert(b"40000", Type::INT2)
                .unwrap_err()
                .to_string()
                .contains("22003")
        );
    }

    #[test]
    fn convert_int4_binary() {
        let params = vec![Some(Bytes::from(42i32.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::INT4)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(matches!(result[0], nodedb_sql::ParamValue::Int32(42)));
    }

    #[test]
    fn convert_int8_binary() {
        let params = vec![Some(Bytes::from(9_999_999_999i64.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::INT8)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(matches!(
            result[0],
            nodedb_sql::ParamValue::Int64(9_999_999_999)
        ));
    }

    #[test]
    fn convert_float4_binary_widens_to_float64() {
        let params = vec![Some(Bytes::from(2.5f32.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::FLOAT4)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(
            matches!(result[0], nodedb_sql::ParamValue::Float64(f) if (f - 2.5).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn convert_float8_binary() {
        let params = vec![Some(Bytes::from(2.78f64.to_be_bytes().to_vec()))];
        let types = vec![Some(Type::FLOAT8)];
        let result = convert_portal_params(&params, &types, &binary_format()).unwrap();
        assert!(
            matches!(result[0], nodedb_sql::ParamValue::Float64(f) if (f - 2.78).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn convert_int4_binary_wrong_length_returns_22p02() {
        let params = vec![Some(Bytes::from_static(&[0u8, 1, 2]))];
        let types = vec![Some(Type::INT4)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    fn assert_text_param(
        input: &'static [u8],
        ty: Type,
        expected: fn(&nodedb_sql::ParamValue) -> bool,
    ) {
        let params = vec![Some(Bytes::from_static(input))];
        let types = vec![Some(ty)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert!(expected(&result[0]));
    }

    #[test]
    fn convert_timestamp_text_to_typed() {
        assert_text_param(b"2024-01-01 00:00:00", Type::TIMESTAMP, |value| {
            matches!(value, nodedb_sql::ParamValue::Timestamp(_))
        });
    }

    #[test]
    fn convert_timestamptz_text_to_typed() {
        assert_text_param(b"2024-01-01 00:00:00+00", Type::TIMESTAMPTZ, |value| {
            matches!(value, nodedb_sql::ParamValue::Timestamptz(_))
        });
    }

    #[test]
    fn convert_bool_variants() {
        for (input, expected) in [("t", true), ("f", false), ("1", true), ("0", false)] {
            let params = vec![Some(Bytes::from(input))];
            let types = vec![Some(Type::BOOL)];
            let result = convert_portal_params(&params, &types, &text_format()).unwrap();
            assert!(matches!(result[0], nodedb_sql::ParamValue::Bool(value) if value == expected));
        }
    }

    #[test]
    fn passthrough_date_text() {
        let value = pgwire_text_to_param("2026-04-19", &Type::DATE, 0).unwrap();
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "2026-04-19"));
    }

    #[test]
    fn timestamp_text_parses_to_typed() {
        let value = pgwire_text_to_param("2026-04-19 12:00:00", &Type::TIMESTAMP, 0).unwrap();
        assert!(matches!(value, nodedb_sql::ParamValue::Timestamp(_)));
    }

    #[test]
    fn timestamptz_text_parses_to_typed() {
        let value = pgwire_text_to_param("2026-04-19 12:00:00+00", &Type::TIMESTAMPTZ, 0).unwrap();
        assert!(matches!(value, nodedb_sql::ParamValue::Timestamptz(_)));
    }

    #[test]
    fn uuid_text_parses_to_canonical_typed() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let value = pgwire_text_to_param(&uuid.to_ascii_uppercase(), &Type::UUID, 0).unwrap();
        assert!(matches!(&value, nodedb_sql::ParamValue::Uuid(u) if u == uuid));
    }

    /// Text bound for a typed parameter that spells no value of the type is
    /// refused with PostgreSQL's SQLSTATE, never passed on as text.
    #[test]
    fn invalid_typed_text_refuses_with_its_sqlstate() {
        for (text, ty, code) in [
            ("not-a-uuid", Type::UUID, "22P02"),
            ("\\xZZ", Type::BYTEA, "22P02"),
            ("not a time", Type::TIMESTAMP, "22007"),
            ("not a time", Type::TIMESTAMPTZ, "22007"),
        ] {
            let error = pgwire_text_to_param(text, &ty, 0).unwrap_err();
            assert!(error.to_string().contains(code), "{ty}: {error}");
        }
    }

    #[test]
    fn jsonb_text_binds_as_a_typed_object() {
        let value = pgwire_text_to_param(r#"{"a":1}"#, &Type::JSONB, 0).unwrap();
        assert!(matches!(
            &value,
            nodedb_sql::ParamValue::Object(fields)
                if matches!(fields.as_slice(), [(key, nodedb_sql::ParamValue::Int64(1))] if key == "a")
        ));
        assert!(pgwire_text_to_param("{", &Type::JSONB, 0).is_err());
    }

    /// A parameter of type `any` is a tagged NodeDB value in binary
    /// format. Its text format is refused.
    #[test]
    fn a_tagged_param_binds_typed_and_its_text_form_is_refused() {
        let array = nodedb_types::Value::Array(vec![
            nodedb_types::Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            nodedb_types::Value::Integer(1),
        ]);
        let params = vec![Some(Bytes::from(
            zerompk::to_msgpack_vec(&array).expect("encode"),
        ))];
        let result = convert_portal_params(&params, &[Some(Type::ANY)], &binary_format()).unwrap();
        assert!(matches!(
            &result[0],
            nodedb_sql::ParamValue::Array(items)
                if matches!(items.as_slice(), [nodedb_sql::ParamValue::Uuid(_), nodedb_sql::ParamValue::Int64(1)])
        ));
        let params = vec![Some(Bytes::from_static(b"[1]"))];
        let Err(PgWireError::UserError(info)) =
            convert_portal_params(&params, &[Some(Type::ANY)], &text_format())
        else {
            panic!("the text form of a tagged parameter is refused");
        };
        assert_eq!(info.code, "0A000");
    }

    #[test]
    fn a_binary_int8_array_binds_its_elements() {
        let mut payload = Vec::new();
        for word in [1i32, 0, 20, 2, 1] {
            payload.extend_from_slice(&word.to_be_bytes());
        }
        for n in [3i64, 4] {
            payload.extend_from_slice(&8i32.to_be_bytes());
            payload.extend_from_slice(&n.to_be_bytes());
        }
        let params = vec![Some(Bytes::from(payload))];
        let result =
            convert_portal_params(&params, &[Some(Type::INT8_ARRAY)], &binary_format()).unwrap();
        assert!(matches!(
            &result[0],
            nodedb_sql::ParamValue::Array(items)
                if matches!(items.as_slice(), [nodedb_sql::ParamValue::Int64(3), nodedb_sql::ParamValue::Int64(4)])
        ));
    }

    #[test]
    fn bytea_hex_text_parses_to_bytes() {
        let value = pgwire_text_to_param("\\xDEADBEEF", &Type::BYTEA, 0).unwrap();
        assert!(
            matches!(&value, nodedb_sql::ParamValue::Bytes(b) if b == &[0xde, 0xad, 0xbe, 0xef])
        );
    }

    #[test]
    fn int_parse_failure_falls_back_to_text() {
        let value = pgwire_text_to_param("abc", &Type::INT8, 0).unwrap();
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "abc"));
    }

    #[test]
    fn unknown_type_routes_to_text() {
        let value = pgwire_text_to_param("42", &Type::UNKNOWN, 0).unwrap();
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "42"));
    }

    #[test]
    fn numeric_text_parses_to_decimal() {
        let value = pgwire_text_to_param("123.45", &Type::NUMERIC, 0).unwrap();
        match value {
            nodedb_sql::ParamValue::Decimal(decimal) => assert_eq!(decimal.to_string(), "123.45"),
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    #[test]
    fn numeric_text_non_numeric_returns_22p02_not_text_fallback() {
        // Pins the fix: NUMERIC no longer falls back to ParamValue::Text on
        // parse failure — that would silently and permanently lose the
        // value, since SqlValue::Decimal is the only downstream
        // representation for a NUMERIC parameter.
        let error = pgwire_text_to_param("abc", &Type::NUMERIC, 0).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn numeric_text_out_of_range_returns_22p02() {
        // A magnitude beyond Decimal's 96-bit mantissa (~29 decimal digits)
        // cannot be parsed exactly and must error, not fall back to text.
        let huge = "9".repeat(50);
        let error = pgwire_text_to_param(&huge, &Type::NUMERIC, 0).unwrap_err();
        assert!(error.to_string().contains("22P02"));
    }

    #[test]
    fn numeric_text_and_binary_agree_on_out_of_range_rejection() {
        // Pins that the text and binary NUMERIC paths agree: the same
        // unrepresentable value is rejected by both with 22P02, not silently
        // accepted as text on one side and rejected on the other.
        let huge = "9".repeat(50);
        let text_error = pgwire_text_to_param(&huge, &Type::NUMERIC, 0).unwrap_err();
        assert!(text_error.to_string().contains("22P02"));

        // Binary side: 10 digit groups of 9999 at weight 9 vastly exceeds
        // Decimal's mantissa, mirroring convert_numeric_binary_overflow_returns_22p02_not_panic.
        let digits = [9999i16; 10];
        let bytes = numeric_binary(&digits, 9, 0x0000, 0);
        let params = vec![Some(Bytes::from(bytes))];
        let types = vec![Some(Type::NUMERIC)];
        let binary_error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        assert!(binary_error.to_string().contains("22P02"));
    }
}
