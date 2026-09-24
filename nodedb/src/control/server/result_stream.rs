// SPDX-License-Identifier: BUSL-1.1

//! Durable streaming result abstraction.
//!
//! A dispatched scan returns its rows as a sequence of `Response` frames over a
//! `ResponseReceiver` (see `RequestTracker::register`):
//! several `partial: true` frames followed by one terminal (`partial: false`)
//! frame, each carrying a standalone msgpack-array payload of rows
//! (`encode_raw_document_rows`).
//!
//! This module turns that per-request channel into a [`ResultStream`] — a
//! `futures::Stream` of [`RowBatch`]es, one batch per frame — so consumers can
//! pull rows incrementally instead of materializing the whole result. The
//! [`materialize`] helper is the dual: it drains a `ResultStream` back into one
//! merged msgpack array for byte-demanding consumers that still need the
//! fully-collected result.

use crate::bridge::envelope::Status;
use crate::control::server::dispatch_utils::reject_data_plane_error;
use crate::control::server::payload_merge::merge_msgpack_arrays;
use crate::types::Lsn;

/// One streamed frame of rows.
///
/// `payload` is a standalone msgpack array (the exact bytes produced by a single
/// Data-Plane scan chunk); `watermark_lsn` is that frame's read watermark and
/// `read_version_lsn` its per-collection read version.
pub struct RowBatch {
    /// Standalone msgpack array of row elements for this frame.
    pub payload: Vec<u8>,
    /// Watermark LSN reported by the Data Plane for this frame.
    pub watermark_lsn: Lsn,
    /// Per-collection read-version LSN for this frame (the scanned collection's
    /// `coll_write_lsn` at read time) — the sound comparand for cross-shard OCC
    /// read validation, distinct from the core-global `watermark_lsn`.
    pub read_version_lsn: Lsn,
}

/// A pinned, boxed stream of [`RowBatch`]es, fallible per item.
pub type ResultStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = crate::Result<RowBatch>> + Send>>;

/// Adapt a per-request response channel into a [`ResultStream`].
///
/// Each `Response` frame becomes one [`RowBatch`]. A running byte total over all
/// frames is enforced against `max_result_bytes`; exceeding it ends the stream
/// with `ExecutionLimitExceeded`. A terminal `Status::Error` frame ends the
/// stream with the typed error its code maps to — except when
/// `tolerate_not_found` is set and the error code is `NotFound`, in which case
/// the stream simply ends cleanly (the shard had no matching rows). The stream
/// ends after the terminal (`!partial`) frame is yielded, or when the channel
/// closes.
pub(crate) fn stream_response_channel(
    mut rx: crate::control::ResponseReceiver,
    max_result_bytes: usize,
    tolerate_not_found: bool,
) -> ResultStream {
    Box::pin(async_stream::try_stream! {
        let mut total: usize = 0;
        while let Some(resp) = rx.recv().await {
            if resp.status == Status::Error {
                if tolerate_not_found
                    && matches!(
                        resp.error_code.as_deref(),
                        Some(crate::bridge::envelope::ErrorCode::NotFound)
                    )
                {
                    // No rows on this source — end the stream cleanly.
                    return;
                }
                // Every other code becomes the typed error the one
                // Data-Plane-code conversion the crate owns produces, so a
                // deadline crosses this boundary as the statement's own
                // timeout (`57014`) and a division by zero as `22012`. A
                // `{ec:?}` dump into a generic dispatch failure made every
                // condition reach the client as `XX000`.
                let error = match reject_data_plane_error(&resp) {
                    Err(error) => error,
                    // `NotFound` is the one code that conversion reads as an
                    // empty observation rather than an error. This stream
                    // declined to tolerate it, so it stops here.
                    Ok(()) => crate::Error::Dispatch {
                        detail: "data plane error: NotFound".to_string(),
                    },
                };
                Err(error)?;
                return;
            }

            total = total.saturating_add(resp.payload.len());
            if total > max_result_bytes {
                Err(crate::Error::ExecutionLimitExceeded {
                    detail: format!(
                        "query result exceeded max_query_result_bytes \
                         ({total} > {max_result_bytes} bytes)"
                    ),
                })?;
                return;
            }

            let is_terminal = !resp.partial;
            yield RowBatch {
                payload: resp.payload.to_vec(),
                watermark_lsn: resp.watermark_lsn,
                read_version_lsn: resp.read_version_lsn,
            };
            if is_terminal {
                return;
            }
        }
    })
}

/// Drain a [`ResultStream`] into a single merged msgpack array and the maximum
/// watermark LSN seen.
///
/// The dual of [`stream_response_channel`] for consumers that need the full
/// collected result as bytes (native, gateway, internal funnel paths).
pub(crate) async fn materialize(mut stream: ResultStream) -> crate::Result<(Vec<u8>, Lsn)> {
    use futures::StreamExt;

    let mut payloads: Vec<Vec<u8>> = Vec::new();
    let mut max_lsn = Lsn::ZERO;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.watermark_lsn > max_lsn {
            max_lsn = batch.watermark_lsn;
        }
        payloads.push(batch.payload);
    }
    Ok((merge_msgpack_arrays(&payloads), max_lsn))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{ErrorCode, Payload, Response};
    use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};
    use crate::types::RequestId;
    use tokio::sync::mpsc;

    /// A standalone msgpack array of `n` one-byte elements — the shape of a
    /// streamed scan chunk.
    fn array_payload(n: usize) -> Vec<u8> {
        let rows: Vec<Vec<u8>> = (0..n).map(|i| vec![(i % 128) as u8]).collect();
        encode_msgpack_array(&rows)
    }

    fn partial(n: usize) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Partial,
            attempt: 1,
            partial: true,
            payload: Payload::from_vec(array_payload(n)),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    fn final_frame(n: usize) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(array_payload(n)),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    fn raw_partial(bytes: usize) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Partial,
            attempt: 1,
            partial: true,
            payload: Payload::from_vec(vec![0u8; bytes]),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    fn error_frame(code: ErrorCode) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: Some(Box::new(code)),
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    #[tokio::test]
    async fn materialize_merges_all_batches() {
        let (tx, rx) = mpsc::channel(8);
        tx.send(partial(1000)).await.unwrap();
        tx.send(partial(1000)).await.unwrap();
        tx.send(final_frame(500)).await.unwrap();
        drop(tx);
        let stream = stream_response_channel(
            crate::control::ResponseReceiver::from_channel(rx),
            1 << 20,
            false,
        );
        let (merged, _lsn) = materialize(stream).await.unwrap();
        assert_eq!(
            extract_msgpack_elements(&merged).len(),
            2500,
            "three array batches must materialize into one array of all rows"
        );
    }

    #[tokio::test]
    async fn over_budget_errors() {
        let (tx, rx) = mpsc::channel(8);
        tx.send(raw_partial(600)).await.unwrap();
        tx.send(raw_partial(600)).await.unwrap();
        drop(tx);
        let stream = stream_response_channel(
            crate::control::ResponseReceiver::from_channel(rx),
            1000,
            false,
        );
        let err = materialize(stream).await.unwrap_err();
        assert!(matches!(err, crate::Error::ExecutionLimitExceeded { .. }));
    }

    /// A terminal error frame ends the stream with the typed error its code
    /// maps to. Collapsing every code into one generic variant is what made a
    /// client's own timeout arrive as an internal failure.
    #[tokio::test]
    async fn terminal_error_frame_keeps_its_typed_error() {
        let (tx, rx) = mpsc::channel(8);
        tx.send(partial(10)).await.unwrap();
        tx.send(error_frame(ErrorCode::ResourcesExhausted))
            .await
            .unwrap();
        drop(tx);
        let stream = stream_response_channel(
            crate::control::ResponseReceiver::from_channel(rx),
            1 << 20,
            false,
        );
        match materialize(stream).await {
            Err(crate::Error::DataPlane(ErrorCode::ResourcesExhausted)) => {}
            other => panic!("expected the shard's own code, got {other:?}"),
        }
    }

    /// A shard that stopped because the statement ran out of time reports the
    /// deadline through this adapter too, so a streamed SELECT answers the same
    /// SQLSTATE a materialized one does.
    #[tokio::test]
    async fn terminal_deadline_frame_reports_the_deadline() {
        let (tx, rx) = mpsc::channel(8);
        tx.send(partial(10)).await.unwrap();
        tx.send(error_frame(ErrorCode::DeadlineExceeded))
            .await
            .unwrap();
        drop(tx);
        let stream = stream_response_channel(
            crate::control::ResponseReceiver::from_channel(rx),
            1 << 20,
            false,
        );
        match materialize(stream).await {
            Err(crate::Error::DeadlineExceeded { request_id }) => {
                assert_eq!(request_id, RequestId::new(1));
            }
            other => panic!("expected the deadline variant, got {other:?}"),
        }
    }

    /// An untolerated `NotFound` still stops the stream rather than reading as
    /// an empty success.
    #[tokio::test]
    async fn untolerated_not_found_errors() {
        let (tx, rx) = mpsc::channel(8);
        tx.send(error_frame(ErrorCode::NotFound)).await.unwrap();
        drop(tx);
        let stream = stream_response_channel(
            crate::control::ResponseReceiver::from_channel(rx),
            1 << 20,
            false,
        );
        assert!(materialize(stream).await.is_err());
    }

    #[tokio::test]
    async fn not_found_tolerated_ends_cleanly() {
        let (tx, rx) = mpsc::channel(8);
        tx.send(error_frame(ErrorCode::NotFound)).await.unwrap();
        drop(tx);
        let stream = stream_response_channel(
            crate::control::ResponseReceiver::from_channel(rx),
            1 << 20,
            true,
        );
        let (merged, _lsn) = materialize(stream).await.unwrap();
        assert_eq!(
            extract_msgpack_elements(&merged).len(),
            0,
            "tolerated NotFound yields an empty result, not an error"
        );
    }
}
