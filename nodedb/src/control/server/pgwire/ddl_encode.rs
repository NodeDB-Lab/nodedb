// SPDX-License-Identifier: BUSL-1.1

//! Encode a protocol-neutral [`DdlResult`] / [`DdlError`] into pgwire
//! `Response` values.
//!
//! This is the pgwire entrypoint's consumer of the shared, protocol-neutral
//! DDL dispatch result — the mirror of the native and http encoders. It
//! reproduces the exact wire shape (RowDescription type OIDs, DataRow text
//! bytes, CommandComplete tag) the pgwire DDL router produced directly,
//! because the neutral result captured each column's original OID (as a
//! [`DdlColType`]) and each cell's already-text-rendered value. Values are
//! re-emitted as captured text — never re-typed or re-parsed — so the field
//! bytes are byte-identical.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use serde_json::Value as JsonValue;

use crate::control::server::response_shape::cell::value_to_wire_json;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};

use super::command_tag::dml_tag;
use super::numeric_narrow::checked_narrow_f32;
use super::types::{
    bool_field, bytea_field, float4_array_field, float4_field, float8_array_field, float8_field,
    int2_field, int4_field, int8_field, json_field, jsonb_field, text_field, timestamp_field,
    timestamptz_field, varchar_field,
};

/// Encode a protocol-neutral DDL dispatch result into pgwire responses.
///
/// An `Err(DdlError)` maps to a pgwire `UserError` carrying the SQLSTATE +
/// message; each `DdlResult` maps to exactly one `Response`.
///
/// The PostgreSQL wire `ErrorResponse` has no field for a NodeDB numeric
/// code, so it travels in `routine` as `ErrorCode`'s `NDB-XXXX` display
/// form — the same convention a client-side reader can parse regardless of
/// which entrypoint produced the error.
pub fn ddl_results_to_pgwire(
    result: Result<Vec<DdlResult>, DdlError>,
) -> PgWireResult<Vec<Response>> {
    let results = match result {
        Ok(results) => results,
        Err(DdlError {
            sqlstate,
            code,
            message,
        }) => {
            let mut info = ErrorInfo::new("ERROR".to_owned(), sqlstate, message);
            info.routine = Some(code.to_string());
            return Err(PgWireError::UserError(Box::new(info)));
        }
    };

    let mut responses = Vec::with_capacity(results.len());
    for ddl in results {
        responses.push(ddl_result_to_response(ddl)?);
    }
    Ok(responses)
}

/// Map a single [`DdlResult`] to a pgwire [`Response`].
fn ddl_result_to_response(ddl: DdlResult) -> PgWireResult<Response> {
    match ddl {
        DdlResult::Status {
            command,
            rows_affected,
        } => {
            let tag = match rows_affected {
                Some(n) => dml_tag(&command, n as usize),
                None => Tag::new(&command),
            };
            Ok(Response::Execution(tag))
        }
        DdlResult::Empty => Ok(Response::EmptyQuery),
        DdlResult::Rows(shaped) => rows_to_response(shaped),
    }
}

/// Build a `Response::Query` from a protocol-neutral shaped row set.
///
/// The `notice` field is intentionally ignored: the pgwire DDL router never
/// attached a NOTICE to a `Response::Query` (notices are a separate protocol
/// message), so honouring it here would diverge from the captured wire shape.
fn rows_to_response(shaped: ShapedRows) -> PgWireResult<Response> {
    let ShapedRows {
        columns,
        column_types,
        rows,
        ..
    } = shaped;

    // Build the RowDescription. Each column's OID is reproduced from its
    // captured `DdlColType`; a missing/short `column_types` defaults to text.
    let fields: Vec<FieldInfo> = columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let ct = column_types.get(i).copied().unwrap_or(DdlColType::Text);
            col_type_to_field(name, ct)
        })
        .collect();
    let schema = Arc::new(fields);

    let mut encoded_rows: Vec<PgWireResult<pgwire::messages::data::DataRow>> =
        Vec::with_capacity(rows.len());
    for row in &rows {
        let mut encoder = DataRowEncoder::new(schema.clone());
        for (idx, name) in columns.iter().enumerate() {
            let ct = column_types.get(idx).copied().unwrap_or(DdlColType::Text);
            // Each typed cell converts to JSON at this edge before it renders.
            match row.get(name).map(value_to_wire_json) {
                // Captured text (the transitional wrapper path, and text-typed
                // migrated cells): re-emit verbatim so the DataRow bytes match.
                Some(JsonValue::String(s)) => encoder.encode_field(&s)?,
                // Explicit NULL (or absent key) → -1 length field.
                Some(JsonValue::Null) | None => encoder.encode_field(&None::<&str>)?,
                // A migrated handler may carry a numeric cell typed rather than
                // pre-rendered. Float columns MUST be encoded through pgwire's
                // native float path (ryu + extra_float_digits) so the text
                // bytes match what the original handler's `encode_field(&f64)`
                // produced — string pre-rendering (`f64::to_string`) diverges
                // (e.g. `0.0` → "0" vs "0.0"). Integer/other numerics render to
                // the same decimal text either way.
                Some(ref value @ JsonValue::Number(ref n)) => match ct {
                    DdlColType::Float8 => match n.as_f64() {
                        Some(f) => encoder.encode_field(&f)?,
                        None => encoder.encode_field(&None::<f64>)?,
                    },
                    // Narrowing to `real` goes through the shared guard, so a
                    // finite value beyond f32's range surfaces as SQLSTATE
                    // 22003 here exactly as it does on the SELECT path,
                    // instead of reaching the client as `Infinity`.
                    DdlColType::Float4 => match checked_narrow_f32(value)? {
                        Some(f) => encoder.encode_field(&f)?,
                        None => encoder.encode_field(&None::<f32>)?,
                    },
                    _ => encoder.encode_field(&n.to_string())?,
                },
                // Defensive: any other scalar rendered to its text form.
                Some(other) => encoder.encode_field(&other.to_string())?,
            }
        }
        encoded_rows.push(Ok(encoder.take_row()));
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(encoded_rows),
    )))
}

/// Map a protocol-neutral [`DdlColType`] to the pgwire `FieldInfo` builder
/// that produces the matching type OID (all with `FieldFormat::Text`). This
/// is the inverse of the OID→`DdlColType` mapping the neutral dispatch used
/// when capturing the schema, so the RowDescription round-trips losslessly.
///
/// Visible across the pgwire module so `handler::shape_encode` can reuse the
/// same OID mapping when encoding the canonical `ShapedRows` response shape.
pub(in crate::control::server::pgwire) fn col_type_to_field(
    name: &str,
    ct: DdlColType,
) -> FieldInfo {
    match ct {
        DdlColType::Text => text_field(name),
        DdlColType::Int8 => int8_field(name),
        DdlColType::Int4 => int4_field(name),
        DdlColType::Int2 => int2_field(name),
        DdlColType::Float8 => float8_field(name),
        DdlColType::Float4 => float4_field(name),
        DdlColType::Bool => bool_field(name),
        DdlColType::Bytea => bytea_field(name),
        DdlColType::Json => json_field(name),
        DdlColType::Jsonb => jsonb_field(name),
        DdlColType::Timestamp => timestamp_field(name),
        DdlColType::Timestamptz => timestamptz_field(name),
        DdlColType::Varchar => varchar_field(name),
        DdlColType::Float4Array => float4_array_field(name),
        DdlColType::Float8Array => float8_array_field(name),
    }
}

/// Like [`col_type_to_field`] but with an explicit wire `format`.
///
/// Reuses the `DdlColType` -> type-OID mapping and swaps the format, so a
/// column can advertise (and be encoded in) binary when the client requested
/// it. The `DataRowEncoder` reads this format from the schema to pick binary
/// vs text encoding, so both the RowDescription and the `DataRow` bytes stay
/// in agreement.
pub(in crate::control::server::pgwire) fn col_type_to_field_with_format(
    name: &str,
    ct: DdlColType,
    format: pgwire::api::results::FieldFormat,
) -> FieldInfo {
    let base = col_type_to_field(name, ct);
    FieldInfo::new(name.to_owned(), None, None, base.datatype().clone(), format)
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use pgwire::messages::Message;
    use pgwire::messages::response::ErrorResponse;

    use super::*;

    /// Round-trips through the actual PostgreSQL wire bytes `ErrorResponse`
    /// encodes and a client's `pgwire` codec decodes — proving the code
    /// reaches the wire, not just that the server set it.
    #[test]
    fn ddl_error_code_survives_pgwire_wire_bytes() {
        let result: Result<Vec<DdlResult>, DdlError> =
            Err(DdlError::new("42501", "write permission denied"));

        let err = ddl_results_to_pgwire(result).expect_err("must map to a pgwire error");
        let PgWireError::UserError(info) = err else {
            panic!("expected a UserError carrying ErrorInfo");
        };

        let response = ErrorResponse::from(*info);
        let mut buf = BytesMut::new();
        response.encode(&mut buf).expect("encode ErrorResponse");
        // Strip the wire header (1-byte type tag + 4-byte length) that
        // `encode` writes ahead of `encode_body`'s field bytes, mirroring
        // what a client's frame reader strips before decoding the body.
        bytes::Buf::advance(&mut buf, 5);
        let decoded = ErrorResponse::decode_body(&mut buf, 0, &Default::default())
            .expect("decode ErrorResponse body");
        let decoded_info: pgwire::error::ErrorInfo = decoded.into();

        assert_eq!(decoded_info.code, "42501");
        assert_eq!(decoded_info.message, "write permission denied");
        assert_eq!(
            decoded_info.routine,
            Some(nodedb_types::error::ErrorCode::AUTHORIZATION_DENIED.to_string())
        );
    }
}
