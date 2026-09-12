// SPDX-License-Identifier: BUSL-1.1

//! Lazy streaming response shaping for the single-node pgwire SELECT path.
//!
//! Turns a [`ResultStream`] of row batches into a pgwire `QueryResponse` whose
//! `DataRow`s are pulled by the framework AFTER `do_query` returns, so a large
//! scan flows to the client incrementally instead of being materialized and
//! merged on the coordinator first.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::server::response_shape::compose::shape_decoded_rows;
use crate::control::server::response_shape::redaction::{QueryRedaction, redact_envelope_row};
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::DdlColType;
use crate::control::server::result_stream::ResultStream;
use crate::control::server::shared::metering::DetachedMeterGuard;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::decode_payload_to_json;

use super::super::ddl_encode::col_type_to_field_with_format;
use super::super::types::{error_to_sqlstate, text_field};
use super::shape_encode::{encode_shaped_row, shaped_query_response};

/// The per-request plumbing a lazily-streamed pgwire response owns for its
/// whole lifetime.
///
/// These travel together because they share one lifetime rule: each must stay
/// alive until the stream finishes or the client disconnects. The lease scope
/// holds descriptor leases open, the meter guard charges on drop, and the
/// shared state backs both — so handing them to a stream separately invites
/// one being dropped early.
pub(crate) struct StreamResponseContext {
    pub(crate) lease_scope: Arc<crate::control::lease::QueryLeaseScope>,
    pub(crate) redaction: Option<QueryRedaction>,
    pub(crate) state: Arc<SharedState>,
    /// `None` when metering is disabled, or when this request is not billable.
    pub(crate) meter_guard: Option<DetachedMeterGuard>,
}

/// Build a streaming multi-row pgwire `Response` whose `DataRow`s are pulled
/// lazily from a [`ResultStream`].
///
/// Schema is a single TEXT column `result`, matching the non-streaming
/// `MultiRow` shape in `plan::payload_to_response`: each row's JSON object is
/// rendered as one text field.
///
/// A global take-N is enforced when `limit < usize::MAX`: the stream stops
/// emitting after `limit` rows across the whole union. A mid-stream
/// `crate::Error` (over-budget, dispatch failure) is mapped to a pgwire
/// `ErrorResponse` via `error_to_sqlstate`, so the client sees a proper error
/// instead of a silently-truncated result.
pub(crate) fn streaming_multirow_response(
    stream: ResultStream,
    limit: usize,
    context: StreamResponseContext,
) -> Response {
    use futures::StreamExt;

    let StreamResponseContext {
        lease_scope,
        redaction,
        state,
        meter_guard,
    } = context;

    let schema = Arc::new(vec![text_field("result")]);
    let row_schema = schema.clone();

    let row_stream = async_stream::try_stream! {
        // QueryResponse outlives the pgwire handler. Its row stream owns this
        // scope so descriptor leases remain held while the client polls rows,
        // including on a mid-stream error, until completion or disconnect.
        let _lease_scope = lease_scope;
        // Owned by the lazy response for the same reason the lease scope is:
        // it charges on drop, which must be when the stream finishes or the
        // client disconnects — so what gets billed is rows actually written
        // to the client, not rows planned.
        let mut meter_guard = meter_guard;
        let mut emitted: usize = 0;
        let mut batches = stream;
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

            if emitted >= limit {
                break;
            }

            // Each batch payload is a standalone msgpack array of rows; decode
            // to a JSON array and stream each element as its own pgwire row.
            let text = decode_payload_to_json(&batch.payload);
            if let Ok(serde_json::Value::Array(items)) =
                sonic_rs::from_str::<serde_json::Value>(&text)
            {
                for mut item in items {
                    if emitted >= limit {
                        break;
                    }
                    // This shape delivers each row as one opaque JSON text
                    // cell rather than named columns, so it never reaches the
                    // shaping core — redact it here rather than let it be the
                    // one streamed shape that bypasses policy.
                    redact_envelope_row(redaction.as_ref(), &state.redaction, &mut item);
                    let mut encoder = DataRowEncoder::new(row_schema.clone());
                    encoder.encode_field(&item.to_string()).map_err(|e| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            format!("failed to encode streamed row: {e}"),
                        )))
                    })?;
                    emitted += 1;
                    if let Some(guard) = meter_guard.as_mut() {
                        guard.add_rows(1);
                    }
                    yield encoder.take_row();
                }
            }
        }
    };

    Response::Query(QueryResponse::new(schema, row_stream))
}

/// Build a streaming, already-projected pgwire `Response` for a SELECT with a
/// named projection list.
///
/// The `RowDescription` schema (one TEXT column per projected display name) is
/// fixed up front, before the first row is pulled. Each batch is decoded and
/// handed to the neutral [`shape_decoded_rows`] core with the projection, then
/// each shaped row is encoded with one pgwire field per projected column. Rows
/// stream lazily — batches are not collected. A global take-N is enforced when
/// `limit < usize::MAX`, matching `streaming_multirow_response`.
pub(crate) fn streaming_shaped_response(
    stream: ResultStream,
    limit: usize,
    schema_out: OutputSchema,
    formats: &[FieldFormat],
    context: StreamResponseContext,
) -> Response {
    use futures::StreamExt;

    let StreamResponseContext {
        lease_scope,
        redaction,
        state,
        meter_guard,
    } = context;

    let display_columns: Vec<String> = schema_out
        .columns
        .iter()
        .map(|c| c.display_name.clone())
        .collect();
    // Cells live in the shaped row maps under unique per-column keys
    // (display names may repeat across columns, e.g. `SELECT w.id, b.id`);
    // derive the same keys the shaper used so every column reads its own cell.
    let cell_keys = crate::control::server::response_shape::project::cell_keys(&display_columns);
    // Advertise each projected column's real catalog type so the streaming
    // path's RowDescription OIDs match the non-streaming `shaped_query_response`
    // (and the extended-query Describe path); `column_types` also drives the
    // per-cell text rendering in `encode_shaped_row`. Per-column `formats`
    // carry the client's (feature-downgraded) result-format request so binary
    // columns advertise and encode in binary.
    let column_types: Vec<DdlColType> = schema_out.columns.iter().map(|c| c.ty).collect();
    let row_formats: Vec<FieldFormat> = schema_out
        .columns
        .iter()
        .enumerate()
        .map(|(i, _)| formats.get(i).copied().unwrap_or(FieldFormat::Text))
        .collect();
    let fields: Vec<FieldInfo> = schema_out
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| col_type_to_field_with_format(&c.display_name, c.ty, row_formats[i]))
        .collect();
    let schema = Arc::new(fields);
    let row_schema = schema.clone();

    let row_stream = async_stream::try_stream! {
        // The lazy QueryResponse owns the admission scope rather than the
        // handler stack frame; dropping it on wire completion or disconnect
        // releases the descriptor leases.
        let _lease_scope = lease_scope;
        // Owned by the lazy response for the same reason the lease scope is:
        // it charges on drop, which must be when the stream finishes or the
        // client disconnects — so what gets billed is rows actually written
        // to the client, not rows planned.
        let mut meter_guard = meter_guard;
        let mut emitted: usize = 0;
        let mut batches = stream;
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

            if emitted >= limit {
                break;
            }

            let text = decode_payload_to_json(&batch.payload);
            let value = sonic_rs::from_str::<serde_json::Value>(&text).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("failed to decode streamed batch: {e}"),
                )))
            })?;
            // Resolved once before the first batch was pulled; this only
            // re-borrows it, so no batch can slip out ahead of the policy.
            let shaped = shape_decoded_rows(
                &value,
                Some(&schema_out),
                redaction.as_ref().map(|r| r.ctx(&state.redaction)),
            )
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("failed to shape streamed batch: {e}"),
                )))
            })?;
            for row in &shaped.rows {
                if emitted >= limit {
                    break;
                }
                let encoded =
                    encode_shaped_row(&row_schema, &cell_keys, &column_types, &row_formats, row)?;
                emitted += 1;
                if let Some(guard) = meter_guard.as_mut() {
                    guard.add_rows(1);
                }
                yield encoded;
            }
        }
    };

    Response::Query(QueryResponse::new(schema, row_stream))
}

/// Build a single-column, single-row `Response` that immediately yields the
/// given error, used by non-lazy streaming callers that discover a fatal
/// condition before any row schema is otherwise fixed.
fn single_pgwire_error(err: PgWireError) -> Response {
    let schema = Arc::new(vec![text_field("result")]);
    let errored: Vec<PgWireResult<_>> = vec![Err(err)];
    Response::Query(QueryResponse::new(schema, futures::stream::iter(errored)))
}

/// Build a `SELECT *` pgwire `Response` by materializing the stream and
/// deriving the id-first column union across all rows.
///
/// Unlike the named-projection path this is NOT lazy: the id-first column set
/// can only be known once every row is seen, so all batches are drained first,
/// then the neutral shaping core derives the column union. Zero rows yield a
/// single-column `result` empty response.
pub(crate) async fn streaming_star_response(
    stream: ResultStream,
    limit: usize,
    redaction: Option<QueryRedaction>,
    state: &SharedState,
    meter_guard: Option<DetachedMeterGuard>,
) -> Response {
    use futures::StreamExt;

    let mut values: Vec<serde_json::Value> = Vec::new();
    let mut batches = stream;
    while let Some(batch) = batches.next().await {
        let batch = match batch {
            Ok(b) => b,
            Err(e) => {
                let (severity, code, message) = error_to_sqlstate(&e);
                return single_pgwire_error(PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                ))));
            }
        };

        if values.len() >= limit {
            break;
        }

        let text = decode_payload_to_json(&batch.payload);
        match sonic_rs::from_str::<serde_json::Value>(&text) {
            Ok(serde_json::Value::Array(items)) => {
                for item in items {
                    if values.len() >= limit {
                        break;
                    }
                    values.push(item);
                }
            }
            Ok(_) => {
                return single_pgwire_error(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    "streamed batch payload was not a JSON array".to_owned(),
                ))));
            }
            Err(e) => {
                return single_pgwire_error(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("failed to decode streamed batch: {e}"),
                ))));
            }
        }
    }

    // Materialized, so the exact row count is known here rather than
    // accumulated as rows go out; charge it before the guard drops.
    if let Some(mut guard) = meter_guard {
        guard.add_rows(values.len() as u64);
    }

    if values.is_empty() {
        let schema = Arc::new(vec![text_field("result")]);
        return Response::Query(QueryResponse::new(
            schema,
            futures::stream::iter(Vec::<PgWireResult<_>>::new()),
        ));
    }

    let shaped = match shape_decoded_rows(
        &serde_json::Value::Array(values),
        None,
        redaction.as_ref().map(|r| r.ctx(&state.redaction)),
    ) {
        Ok(s) => s,
        Err(e) => {
            return single_pgwire_error(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("failed to shape streamed batch: {e}"),
            ))));
        }
    };
    // `SELECT *` derives its columns from the rows and has no client-requested
    // per-column formats, so it always renders text.
    let (response, _notice) = shaped_query_response(shaped, &[]);
    response
}
