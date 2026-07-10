// SPDX-License-Identifier: BUSL-1.1

//! Lazy NDJSON streaming body for `POST /v1/query/stream`.
//!
//! When a query is an eligible single-task, autocommit-free (HTTP is
//! stateless), unordered multi-row SELECT — i.e. exactly
//! `Query(Exchange(Gather{as_aggregate:false}))` over a streamable scan — its
//! rows flow to the client one NDJSON line at a time straight off a
//! [`ResultStream`], instead of being materialized into a `String` first.
//!
//! A mid-stream error is surfaced **in band** as a final
//! `{"error": "..."}\n` line and the body then ends cleanly: NDJSON clients
//! parse line-by-line, so an in-band error line is the correct UX and matches
//! the shape the materialized path emits for a dispatch failure. The HTTP body
//! itself never errors, so the response stays `Content-Type:
//! application/x-ndjson` and 200.

use bytes::Bytes;
use futures::StreamExt;

use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::server::exchange::gather::gather_all_cores_stream;
use crate::control::server::exchange::streamable::streamable_gather_child;
use crate::control::server::response_shape::compose::shape_decoded_rows;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::result_stream::ResultStream;
use crate::data::executor::response_codec::decode_payload_to_json;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::auth::AppState;

/// Decide whether the single-task list is an eligible streamable SELECT and,
/// if so, open the row stream.
///
/// Mirrors the pgwire `maybe_stream_select` plan-shape predicate via
/// [`streamable_gather_child`] plus the single-task and no-set-op gates. HTTP
/// is stateless, so there is no autocommit / transaction-block check.
///
/// Returns `Ok(Some((stream, limit)))` when eligible, `Ok(None)` when the
/// caller should fall back to the materialized path, or `Err` when the
/// stream could not be opened.
pub(super) async fn try_open_stream(
    state: &AppState,
    tasks: &[PhysicalTask],
    database_id: nodedb_types::DatabaseId,
    trace_id: crate::types::TraceId,
) -> crate::Result<Option<(ResultStream, usize)>> {
    let [task] = tasks else {
        return Ok(None);
    };
    if task.post_set_op != PostSetOp::None {
        return Ok(None);
    }
    let Some((child_plan, limit)) = streamable_gather_child(&task.plan) else {
        return Ok(None);
    };

    let stream = if let Some(gw) = state.shared.gateway.as_ref() {
        let ctx = QueryContext {
            tenant_id: task.tenant_id,
            trace_id,
            database_id,
            // HTTP streaming query endpoint is autocommit.
            txn_id: None,
        };
        gw.execute_stream(&ctx, child_plan).await
    } else {
        gather_all_cores_stream(
            &state.shared,
            task.tenant_id,
            task.database_id,
            child_plan,
            trace_id,
            task.txn_id,
        )
    }?;

    Ok(Some((stream, limit)))
}

/// Build a lazy NDJSON byte stream from a [`ResultStream`].
///
/// Each [`RowBatch`] payload is a standalone msgpack array; it is decoded to a
/// JSON array and each element is emitted as its own `<json>\n` line, with a
/// global take-N enforced at `limit`. A mid-stream `Err` is emitted as a final
/// `{"error": "..."}\n` line and the stream then ends — see the module docs for
/// why the HTTP body never errors.
///
/// [`RowBatch`]: crate::control::server::result_stream::RowBatch
pub(super) fn ndjson_body_stream(
    stream: ResultStream,
    limit: usize,
    projection: Option<OutputSchema>,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> {
    async_stream::stream! {
        let mut emitted: usize = 0;
        let mut batches = stream;
        while emitted < limit {
            let batch = match batches.next().await {
                None => break,
                Some(Ok(b)) => b,
                Some(Err(e)) => {
                    let (_status, msg) = GatewayErrorMap::to_http(&e);
                    let line = format!("{}\n", serde_json::json!({ "error": msg }));
                    yield Ok(Bytes::from(line));
                    return;
                }
            };

            let json_str = decode_payload_to_json(&batch.payload);
            let value = match sonic_rs::from_str::<serde_json::Value>(&json_str) {
                Ok(v) => v,
                Err(e) => {
                    // A malformed batch payload is surfaced as an in-band error
                    // line (matching the mid-stream dispatch-error path above)
                    // rather than silently dropping the batch.
                    let line = format!(
                        "{}\n",
                        serde_json::json!({ "error": format!("malformed response batch: {e}") })
                    );
                    yield Ok(Bytes::from(line));
                    return;
                }
            };
            let shaped = shape_decoded_rows(&value, projection.as_ref());
            for row in shaped.rows {
                if emitted >= limit {
                    break;
                }
                let line = format!("{}\n", serde_json::Value::Object(row));
                emitted += 1;
                yield Ok(Bytes::from(line));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::result_stream::RowBatch;
    use crate::types::Lsn;

    /// A JSON-text array of `n` `{"id": i}` objects. `decode_payload_to_json`
    /// returns a JSON-leading payload as-is, so this exercises the same
    /// array-of-objects → per-line decode path a Data-Plane scan chunk drives.
    fn json_object_batch(start: usize, n: usize) -> Vec<u8> {
        let items: Vec<serde_json::Value> = (start..start + n)
            .map(|i| serde_json::json!({ "id": i }))
            .collect();
        serde_json::Value::Array(items).to_string().into_bytes()
    }

    fn batch(start: usize, n: usize) -> crate::Result<RowBatch> {
        Ok(RowBatch {
            payload: json_object_batch(start, n),
            watermark_lsn: Lsn::ZERO,
        })
    }

    async fn collect_lines(
        stream: impl futures::Stream<Item = Result<Bytes, std::io::Error>>,
    ) -> Vec<String> {
        futures::pin_mut!(stream);
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("body chunk");
            for line in String::from_utf8_lossy(&chunk).lines() {
                out.push(line.to_string());
            }
        }
        out
    }

    #[tokio::test]
    async fn streams_all_rows_across_batches() {
        // 2500 rows across three batches > the native/pgwire chunk size; assert
        // every row arrives as its own NDJSON line.
        let batches: Vec<crate::Result<RowBatch>> =
            vec![batch(0, 1000), batch(1000, 1000), batch(2000, 500)];
        let stream: ResultStream = Box::pin(futures::stream::iter(batches));
        let lines = collect_lines(ndjson_body_stream(stream, usize::MAX, None)).await;
        assert_eq!(lines.len(), 2500, "all rows must stream as NDJSON lines");
    }

    #[tokio::test]
    async fn global_limit_caps_emitted_rows() {
        let batches: Vec<crate::Result<RowBatch>> = vec![batch(0, 1000), batch(1000, 1000)];
        let stream: ResultStream = Box::pin(futures::stream::iter(batches));
        let lines = collect_lines(ndjson_body_stream(stream, 1500, None)).await;
        assert_eq!(lines.len(), 1500, "global take-N must cap the line count");
    }

    #[tokio::test]
    async fn mid_stream_error_becomes_in_band_error_line() {
        let batches: Vec<crate::Result<RowBatch>> = vec![
            batch(0, 10),
            Err(crate::Error::Dispatch {
                detail: "boom".into(),
            }),
        ];
        let stream: ResultStream = Box::pin(futures::stream::iter(batches));
        let lines = collect_lines(ndjson_body_stream(stream, usize::MAX, None)).await;
        assert_eq!(lines.len(), 11, "10 rows + 1 in-band error line");
        let last: serde_json::Value =
            sonic_rs::from_str(lines.last().expect("error line")).expect("json error line");
        assert!(last.get("error").is_some(), "final line is an error object");
    }
}
