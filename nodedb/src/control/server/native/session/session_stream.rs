// SPDX-License-Identifier: BUSL-1.1

//! Multi-frame emission for a lazy native SQL row stream.
//!
//! Drives a [`SqlStream`] to the wire as a sequence of `NativeResponse` frames
//! using the exact shape `chunk_large_response` produces, so existing native
//! clients reassemble a streamed result identically to a chunked one.

use futures::StreamExt;

use nodedb_types::Value;
use nodedb_types::protocol::{NativeResponse, ResponseStatus};

use crate::control::server::conn_stream::ConnStream;
use crate::control::server::response_shape::compose::shape_decoded_rows;
use crate::control::server::response_shape::redaction::RedactionCtx;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::data::executor::response_codec::decode_payload_to_json;

use super::codec::{self, FrameFormat};
use super::dispatch::{self, SqlStream, to_native_columns_rows};

/// Decode one streamed row-batch's JSON text into columns/rows.
///
/// Streamable plans are always plain unordered scans (`streamable_gather_child`
/// only matches `Query(Exchange(Gather{as_aggregate:false}))` over a
/// streamable scan) — never a KV point-get or vector search — so a batch
/// here only needs decode + scan-envelope unwrap + the statement's
/// SELECT-list projection, exactly the pure [`shape_decoded_rows`] core the
/// materialized dispatch loop also uses (via
/// `response_shape::compose::shape_response_materialized`). `apply_kv_wrap`
/// / `translate_search_response` do not apply to a streamed batch and are
/// deliberately not called here, matching pgwire's own streamed responses,
/// which likewise only ever get column projection, never kv_wrap/vector.
///
/// A JSON-parse failure (non-JSON/malformed batch text) falls back to a
/// single "result" text column, matching the shape this decoder has always
/// produced for an undecodable batch.
fn decode_batch_to_columns_rows(
    json_text: &str,
    projection: Option<&OutputSchema>,
    redaction: Option<RedactionCtx<'_>>,
) -> crate::Result<(Vec<String>, Vec<Vec<Value>>)> {
    match sonic_rs::from_str::<serde_json::Value>(json_text) {
        Ok(decoded) => {
            let shaped = shape_decoded_rows(&decoded, projection, redaction)?;
            Ok(to_native_columns_rows(&shaped))
        }
        Err(_) => Ok((
            vec!["result".into()],
            vec![vec![Value::String(json_text.to_string())]],
        )),
    }
}

/// Drive a lazy SQL row stream to `stream` as multiple frames.
///
/// Frame shape mirrors `chunk_large_response`: columns ride the first
/// row-bearing frame only, every frame but the last has `Partial` status, and a
/// terminal `Ok` frame carries `rows_affected` (the total emitted) plus the
/// maximum watermark LSN seen. Each `RowBatch` decodes to one frame's
/// `(columns, rows)`.
///
/// A mid-stream error is written as a terminal `Error` frame (typed via
/// `error_to_native`) — never a silent truncation. An empty result still emits
/// one terminal `Ok` frame so the client sees a well-formed end.
pub(super) async fn emit_sql_stream(
    stream: &mut ConnStream,
    sql_stream: SqlStream,
    format: FrameFormat,
    state: &crate::control::state::SharedState,
) -> crate::Result<()> {
    let SqlStream {
        seq,
        limit,
        stream: mut rows_stream,
        projection,
        redaction,
        lease_scope: _lease_scope,
    } = sql_stream;

    let mut emitted: usize = 0;
    let mut columns_sent = false;
    let mut last_lsn: u64 = 0;

    while emitted < limit {
        let batch = match rows_stream.next().await {
            None => break,
            Some(Ok(b)) => b,
            Some(Err(e)) => {
                // Terminal error frame — surfaced in band, not a silent stop.
                let err = dispatch::error_to_native(seq, &e);
                let bytes = codec::encode_response(&err, format)?;
                codec::write_frame(stream, &bytes).await?;
                return Ok(());
            }
        };

        last_lsn = batch.watermark_lsn.as_u64();

        let json_text = decode_payload_to_json(&batch.payload);
        // The redaction inputs were resolved once when the stream opened; this
        // only re-borrows them, so every batch — including the first — is
        // shaped under the same policy.
        let (cols, mut batch_rows) = decode_batch_to_columns_rows(
            &json_text,
            projection.as_ref(),
            redaction.as_ref().map(|r| r.ctx(&state.redaction)),
        )?;
        if batch_rows.is_empty() {
            continue;
        }

        // Enforce the global take-N across the union.
        if emitted + batch_rows.len() > limit {
            batch_rows.truncate(limit - emitted);
        }
        emitted += batch_rows.len();

        let columns = if columns_sent {
            None
        } else {
            columns_sent = true;
            if cols.is_empty() { None } else { Some(cols) }
        };

        // Intermediate frame: Partial status; the terminal frame carries
        // rows_affected and finalizes the reassembly.
        let frame = NativeResponse {
            seq,
            status: ResponseStatus::Partial,
            columns,
            rows: Some(batch_rows),
            rows_affected: None,
            watermark_lsn: last_lsn,
            error: None,
            auth: None,
            warnings: Vec::new(),
        };
        let bytes = codec::encode_response(&frame, format)?;
        codec::write_frame(stream, &bytes).await?;
    }

    // Terminal frame: Ok status closes the multi-frame reassembly.
    let terminal = NativeResponse {
        seq,
        status: ResponseStatus::Ok,
        columns: None,
        rows: Some(Vec::new()),
        rows_affected: Some(emitted as u64),
        watermark_lsn: last_lsn,
        error: None,
        auth: None,
        warnings: Vec::new(),
    };
    let bytes = codec::encode_response(&terminal, format)?;
    codec::write_frame(stream, &bytes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::redaction::{RedactionMode, RedactionPolicy, RedactionRule};
    use crate::control::server::payload_merge::encode_msgpack_array;
    use crate::control::server::response_shape::redaction::QueryRedaction;
    use crate::control::server::result_stream::{ResultStream, RowBatch};
    use crate::control::state::SharedState;
    use crate::types::Lsn;
    use crate::wal::WalManager;
    use nodedb_types::protocol::{FRAME_HEADER_LEN, ResponseStatus};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// Minimum real shared state: the streaming emitter reads only
    /// `state.redaction` from it.
    fn shared_state() -> Arc<SharedState> {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("session-stream.wal"))
                .expect("test WAL"),
        );
        let (dispatcher, _) = Dispatcher::new(1, 1);
        SharedState::new(dispatcher, wal).expect("test shared state")
    }

    /// A JSON-text array of `n` `{"id": i}` objects — `decode_payload_to_json`
    /// returns JSON-leading bytes as-is, exercising the array → rows decode.
    fn json_batch(start: usize, n: usize) -> Vec<u8> {
        let items: Vec<serde_json::Value> = (start..start + n)
            .map(|i| serde_json::json!({ "id": i }))
            .collect();
        serde_json::Value::Array(items).to_string().into_bytes()
    }

    fn batch(start: usize, n: usize) -> crate::Result<RowBatch> {
        Ok(RowBatch {
            payload: json_batch(start, n),
            watermark_lsn: Lsn::ZERO,
            read_version_lsn: Lsn::ZERO,
        })
    }

    /// A standalone msgpack array (non-JSON-leading) of `n` empty rows — used to
    /// keep the `encode_msgpack_array` import exercised and to assert the
    /// msgpack decode path also yields rows.
    fn msgpack_empty_batch(n: usize) -> Vec<u8> {
        let rows: Vec<Vec<u8>> = (0..n).map(|_| vec![0x80u8]).collect(); // fixmap(0) == {}
        encode_msgpack_array(&rows)
    }

    async fn read_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
        let mut len_buf = [0u8; FRAME_HEADER_LEN];
        if stream.read_exact(&mut len_buf).await.is_err() {
            return None;
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await.ok()?;
        Some(payload)
    }

    /// Drive `emit_sql_stream` over a loopback TCP pair and reassemble every
    /// frame on the client side, returning the decoded responses.
    async fn run_emit(batches: Vec<crate::Result<RowBatch>>, limit: usize) -> Vec<NativeResponse> {
        run_emit_with_redaction(batches, limit, shared_state(), None).await
    }

    /// Drive `emit_sql_stream` over a loopback TCP pair against a given
    /// shared state and redaction resolution.
    async fn run_emit_with_redaction(
        batches: Vec<crate::Result<RowBatch>>,
        limit: usize,
        state: Arc<SharedState>,
        redaction: Option<QueryRedaction>,
    ) -> Vec<NativeResponse> {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.expect("accept");
            let mut conn = ConnStream::plain(sock);
            let stream: ResultStream = Box::pin(futures::stream::iter(batches));
            let sql_stream = SqlStream {
                seq: 7,
                limit,
                stream,
                projection: None,
                redaction,
                lease_scope: None,
            };
            emit_sql_stream(
                &mut conn,
                sql_stream,
                FrameFormat::MessagePack,
                state.as_ref(),
            )
            .await
            .expect("emit");
            // Keep the connection open until the client has drained all frames.
            conn.shutdown().await.ok();
        });

        let mut client = TcpStream::connect(addr).await.expect("connect");
        let mut frames = Vec::new();
        while let Some(payload) = read_frame(&mut client).await {
            let resp: NativeResponse = zerompk::from_msgpack(&payload).expect("decode frame");
            frames.push(resp);
        }
        server.await.expect("server task");
        frames
    }

    #[tokio::test]
    async fn streams_all_rows_across_partial_frames() {
        // 2500 rows over three batches > the chunk size; every row must arrive
        // across Partial frames terminated by one Ok frame.
        let frames = run_emit(
            vec![batch(0, 1000), batch(1000, 1000), batch(2000, 500)],
            usize::MAX,
        )
        .await;

        let total: usize = frames
            .iter()
            .filter_map(|f| f.rows.as_ref())
            .map(|r| r.len())
            .sum();
        assert_eq!(total, 2500, "all rows must arrive across frames");

        // Columns ride the first row-bearing frame only.
        assert!(frames[0].columns.is_some(), "first frame carries columns");
        for f in &frames[1..] {
            assert!(f.columns.is_none(), "only the first frame carries columns");
        }
        // All but the last frame are Partial; the last is the terminal Ok.
        let last = frames.len() - 1;
        for (i, f) in frames.iter().enumerate() {
            if i < last {
                assert_eq!(
                    f.status,
                    ResponseStatus::Partial,
                    "frame {i} must be Partial"
                );
            } else {
                assert_eq!(f.status, ResponseStatus::Ok, "terminal frame must be Ok");
                assert_eq!(
                    f.rows_affected,
                    Some(2500),
                    "terminal carries total emitted"
                );
            }
            assert_eq!(f.seq, 7, "seq echoes on every frame");
        }
    }

    #[tokio::test]
    async fn global_limit_truncates_total() {
        let frames = run_emit(vec![batch(0, 1000), batch(1000, 1000)], 1500).await;
        let total: usize = frames
            .iter()
            .filter_map(|f| f.rows.as_ref())
            .map(|r| r.len())
            .sum();
        assert_eq!(total, 1500, "global take-N caps the total rows emitted");
    }

    #[tokio::test]
    async fn msgpack_payload_rows_are_emitted() {
        let frames = run_emit(
            vec![Ok(RowBatch {
                payload: msgpack_empty_batch(3),
                watermark_lsn: Lsn::ZERO,
                read_version_lsn: Lsn::ZERO,
            })],
            usize::MAX,
        )
        .await;
        let total: usize = frames
            .iter()
            .filter_map(|f| f.rows.as_ref())
            .map(|r| r.len())
            .sum();
        assert_eq!(total, 3, "msgpack-array payloads decode to rows too");
    }

    #[tokio::test]
    async fn mid_stream_error_yields_terminal_error_frame() {
        let frames = run_emit(
            vec![
                batch(0, 10),
                Err(crate::Error::Dispatch {
                    detail: "boom".into(),
                }),
            ],
            usize::MAX,
        )
        .await;
        // 10 rows on a Partial frame, then a terminal Error frame (no Ok).
        let last = frames.last().expect("at least one frame");
        assert_eq!(
            last.status,
            ResponseStatus::Error,
            "stream error → Error frame"
        );
        assert!(last.error.is_some(), "Error frame carries an error payload");
    }

    /// Redaction must be applied to EVERY streamed batch, the first included:
    /// a batch shipped before the policy took effect is a leak that no later
    /// batch can undo.
    #[tokio::test]
    async fn redaction_applies_to_every_streamed_batch() {
        let state = shared_state();
        state.redaction.create_policy(RedactionPolicy {
            name: "mask_id".into(),
            tenant_id: 1,
            collection: "docs".into(),
            display_collection: "docs".into(),
            for_role: "support".into(),
            rules: vec![RedactionRule {
                field: "id".into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });
        let redaction = QueryRedaction::new(
            crate::types::TenantId::new(1),
            vec!["support".to_string()],
            vec![(String::new(), "docs".to_string())],
        );

        let frames = run_emit_with_redaction(
            vec![batch(0, 3), batch(3, 3)],
            usize::MAX,
            state,
            Some(redaction),
        )
        .await;

        let cells: Vec<&Value> = frames
            .iter()
            .filter_map(|f| f.rows.as_ref())
            .flatten()
            .flatten()
            .collect();
        assert_eq!(cells.len(), 6, "every row must arrive");
        for cell in cells {
            assert_eq!(
                cell,
                &Value::String("***".into()),
                "every batch, including the first, must be redacted"
            );
        }
    }

    #[tokio::test]
    async fn empty_result_emits_single_terminal_ok() {
        let frames = run_emit(Vec::new(), usize::MAX).await;
        assert_eq!(frames.len(), 1, "empty result emits exactly one frame");
        assert_eq!(frames[0].status, ResponseStatus::Ok, "terminal Ok");
        assert_eq!(frames[0].rows_affected, Some(0), "zero rows emitted");
    }
}
