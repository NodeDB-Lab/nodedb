// SPDX-License-Identifier: BUSL-1.1

//! Encode a protocol-neutral [`DdlResult`] / [`DdlError`] into pgwire
//! `Response` values.
//!
//! This is the pgwire entrypoint's consumer of the shared, protocol-neutral
//! DDL dispatch result — the mirror of the native and http encoders. Each
//! column's `RowDescription` type and format come from the [`DdlColType`]
//! the neutral result captured and the client's requested result formats,
//! and each typed cell renders through the one pgwire cell encoder
//! (`handler::shape_encode::encode_cell`) in that format.

use std::sync::Arc;

use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};

use super::command_tag::dml_tag;
use super::handler::shape_encode::encode_cell;
use super::types::wire_type::result_fields;

/// Encode a protocol-neutral DDL dispatch result into pgwire responses.
///
/// An `Err(DdlError)` maps to a pgwire `UserError` carrying the SQLSTATE +
/// message; each `DdlResult` maps to exactly one `Response`. Row results
/// honour `requested`, the client's result formats: the simple-query
/// protocol passes `wire_type::TEXT_RESULTS`, an extended-query Execute
/// passes its portal's formats.
///
/// The PostgreSQL wire `ErrorResponse` has no field for a NodeDB numeric
/// code, so it travels in `routine` as `ErrorCode`'s `NDB-XXXX` display
/// form — the same convention a client-side reader can parse regardless of
/// which entrypoint produced the error.
pub fn ddl_results_to_pgwire(
    result: Result<Vec<DdlResult>, DdlError>,
    requested: &Format,
) -> PgWireResult<Vec<Response>> {
    let results = match result {
        Ok(results) => results,
        Err(DdlError {
            sqlstate,
            code,
            message,
            cause,
            ..
        }) => {
            let mut info = ErrorInfo::new("ERROR".to_owned(), sqlstate, message);
            info.routine = Some(code.to_string());
            // The typed cause travels in `detail`: its SQLSTATE, its numeric
            // code, and its message.
            info.detail = cause.map(|cause| {
                format!(
                    "caused by {} ({}): {}",
                    crate::control::server::pgwire::types::error_map::numeric_code_to_sqlstate(
                        cause.code()
                    ),
                    cause.code(),
                    cause.message()
                )
            });
            return Err(PgWireError::UserError(Box::new(info)));
        }
    };

    let mut responses = Vec::with_capacity(results.len());
    for ddl in results {
        responses.push(ddl_result_to_response(ddl, requested)?);
    }
    Ok(responses)
}

/// Map a single [`DdlResult`] to a pgwire [`Response`].
fn ddl_result_to_response(ddl: DdlResult, requested: &Format) -> PgWireResult<Response> {
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
        DdlResult::Rows(shaped) => rows_to_response(shaped, requested),
    }
}

/// Take each row set's notice out of `results`, for the caller to queue on its session.
pub fn take_notices(results: &mut [DdlResult]) -> Vec<String> {
    results
        .iter_mut()
        .filter_map(|ddl| match ddl {
            DdlResult::Rows(shaped) => shaped.notice.take(),
            _ => None,
        })
        .collect()
}

/// Build a `Response::Query` from a protocol-neutral shaped row set, in the
/// client's `requested` result formats.
///
/// A notice is a separate protocol message, so [`take_notices`] handles it.
fn rows_to_response(shaped: ShapedRows, requested: &Format) -> PgWireResult<Response> {
    let ShapedRows {
        columns,
        column_types,
        rows,
        ..
    } = shaped;

    // Each column's type and format come from its captured `DdlColType` and
    // the requested formats; a missing/short `column_types` defaults to text.
    let column_type = |i: usize| column_types.get(i).copied().unwrap_or(DdlColType::Text);
    let schema = Arc::new(result_fields(
        columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_str(), column_type(i))),
        requested,
    ));

    let mut encoded_rows: Vec<PgWireResult<pgwire::messages::data::DataRow>> =
        Vec::with_capacity(rows.len());
    let mut encoder = DataRowEncoder::new(schema.clone());
    for row in &rows {
        for (idx, (name, field)) in columns.iter().zip(schema.iter()).enumerate() {
            match row.get(name) {
                // Absent key → -1 length field.
                None => encoder.encode_field(&None::<&str>)?,
                Some(v) => encode_cell(&mut encoder, name, column_type(idx), field.format(), v)?,
            }
        }
        encoded_rows.push(Ok(encoder.take_row()));
    }

    Ok(Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(encoded_rows),
    )))
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use pgwire::messages::Message;
    use pgwire::messages::response::ErrorResponse;

    use super::*;

    /// A phase failure keeps its SQLSTATE, and the typed cause travels in
    /// `detail` with its own SQLSTATE and code.
    #[test]
    fn ddl_phase_failure_names_its_cause_in_detail() {
        let phase = nodedb_types::NodeDbError::move_tenant_snapshot_failed("7", "dispatch")
            .with_cause(nodedb_types::NodeDbError::division_by_zero());
        let result: Result<Vec<DdlResult>, DdlError> =
            Err(DdlError::move_tenant_snapshot_failed(phase.message()).with_cause_of(&phase));

        let err = ddl_results_to_pgwire(result, &Format::UnifiedText)
            .expect_err("must map to a pgwire error");
        let PgWireError::UserError(info) = err else {
            panic!("expected a UserError carrying ErrorInfo");
        };
        let info = *info;
        assert_eq!(info.code, "XX000");
        let detail = info.detail.expect("the cause travels in detail");
        assert!(detail.contains("22012"), "{detail}");
        assert!(detail.contains("NDB-1204"), "{detail}");
    }

    /// Round-trips through the actual PostgreSQL wire bytes `ErrorResponse`
    /// encodes and a client's `pgwire` codec decodes — proving the code
    /// reaches the wire, not just that the server set it.
    #[test]
    fn ddl_error_code_survives_pgwire_wire_bytes() {
        let result: Result<Vec<DdlResult>, DdlError> =
            Err(DdlError::new("42501", "write permission denied"));

        let err = ddl_results_to_pgwire(result, &Format::UnifiedText)
            .expect_err("must map to a pgwire error");
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

    /// The first field's raw bytes of the first row of a query response,
    /// with the response's per-column formats.
    async fn first_cell(response: Response) -> (Vec<pgwire::api::results::FieldFormat>, Vec<u8>) {
        use futures::StreamExt;

        let Response::Query(mut qr) = response else {
            panic!("expected a Query response");
        };
        let formats = qr.row_schema.iter().map(|f| f.format()).collect();
        let row = qr
            .data_rows
            .next()
            .await
            .expect("one row")
            .expect("row encodes");
        let data = &row.data;
        let len = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        (formats, data[4..4 + len].to_vec())
    }

    fn show_rows() -> Result<Vec<DdlResult>, DdlError> {
        let row = [
            ("n".to_string(), nodedb_types::Value::Integer(42)),
            ("name".to_string(), nodedb_types::Value::String("x".into())),
        ]
        .into_iter()
        .collect();
        Ok(vec![DdlResult::Rows(ShapedRows::from_rows(
            vec!["n".into(), "name".into()],
            vec![DdlColType::Int8, DdlColType::Text],
            vec![row],
        ))])
    }

    /// A DDL row result honours a binary request: an `int8` column is
    /// advertised binary and its cell is the 8-byte big-endian integer.
    #[tokio::test]
    async fn ddl_rows_honour_a_binary_request() {
        let mut responses =
            ddl_results_to_pgwire(show_rows(), &Format::UnifiedBinary).expect("encodes");
        let (formats, bytes) = first_cell(responses.remove(0)).await;
        assert_eq!(
            formats,
            vec![
                pgwire::api::results::FieldFormat::Binary,
                pgwire::api::results::FieldFormat::Binary
            ]
        );
        assert_eq!(bytes, 42i64.to_be_bytes().to_vec());
    }

    /// The simple-query protocol's text request renders the digits.
    #[tokio::test]
    async fn ddl_rows_render_text_under_a_text_request() {
        let mut responses =
            ddl_results_to_pgwire(show_rows(), &Format::UnifiedText).expect("encodes");
        let (formats, bytes) = first_cell(responses.remove(0)).await;
        assert_eq!(formats[0], pgwire::api::results::FieldFormat::Text);
        assert_eq!(bytes, b"42".to_vec());
    }
}
