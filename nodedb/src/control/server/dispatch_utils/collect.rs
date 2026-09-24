// SPDX-License-Identifier: BUSL-1.1

//! Bounded response collection for dispatched requests: draining a streamed
//! response channel with a total-payload byte ceiling.

use std::time::Instant;

use crate::bridge::envelope::{Payload, Response, Status};
use crate::types::RequestId;

#[derive(Debug)]
pub(crate) enum DispatchCollectError {
    OverBudget { bytes: usize },
    ChannelClosed,
}

/// Drain a dispatched request's bounded response channel, enforcing a
/// total-payload byte ceiling across streamed partials.
///
/// Returns the final Response (non-streaming: pass-through; streaming:
/// concatenated payload) or an error if the channel closed without a
/// final chunk or if the accumulated payload would exceed the ceiling.
pub(crate) async fn collect_bounded_response(
    rx: &mut crate::control::ResponseReceiver,
    max_result_bytes: usize,
) -> Result<Response, DispatchCollectError> {
    // Each streamed chunk is its OWN msgpack array (`encode_raw_document_rows`
    // per chunk), so the chunks are accumulated separately and merged into a
    // single msgpack array at the end. Raw byte concatenation would leave every
    // chunk after the first as a trailing array that downstream single-array
    // decoders silently drop — truncating a streamed scan to `stream_chunk_size`
    // rows. The byte budget is enforced on the running total of raw chunk bytes
    // (the memory actually held), which is `>=` the merged-array size.
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let mut total_bytes: usize = 0;
    let mut final_response_meta: Option<Response> = None;

    loop {
        let Some(resp) = rx.recv().await else { break };
        if resp.partial {
            total_bytes = total_bytes.saturating_add(resp.payload.len());
            if total_bytes > max_result_bytes {
                return Err(DispatchCollectError::OverBudget { bytes: total_bytes });
            }
            chunks.push(resp.payload.to_vec());
        } else if chunks.is_empty() {
            // Non-streaming fast path: a single terminal frame is returned
            // unmodified (writes, point reads, DDL, counts, single-chunk scans).
            return Ok(resp);
        } else if resp.status == Status::Error {
            // The producer ended the stream by failing — a deadline, a budget,
            // a decode error. The chunks already collected are an arbitrary
            // prefix of the answer, so they are dropped: an error response
            // that carried rows would let a caller render a truncated result
            // set as if the statement had completed.
            return Ok(Response {
                payload: Payload::empty(),
                ..resp
            });
        } else {
            total_bytes = total_bytes.saturating_add(resp.payload.len());
            if total_bytes > max_result_bytes {
                return Err(DispatchCollectError::OverBudget { bytes: total_bytes });
            }
            chunks.push(resp.payload.to_vec());
            final_response_meta = Some(resp);
            break;
        }
    }

    match final_response_meta {
        Some(meta) => Ok(Response {
            payload: Payload::from_vec(
                crate::control::server::payload_merge::merge_msgpack_arrays(&chunks),
            ),
            ..meta
        }),
        None => Err(DispatchCollectError::ChannelClosed),
    }
}

/// What one deadline-bounded collect needs to name its outcome.
pub(crate) struct DeadlineCollect<'a> {
    /// The dispatched request this channel answers. Rides on the deadline
    /// error so a client can correlate the cancelled statement.
    pub request_id: RequestId,
    /// The statement's deadline — the same instant stamped on the envelope,
    /// so the Control-Plane wait and the Data-Plane execution expire together.
    pub deadline: Instant,
    /// Total payload ceiling across streamed partials.
    pub max_result_bytes: usize,
    /// Names the collecting site in an over-budget message, e.g.
    /// `"gather on core 3"`.
    pub context: &'a str,
}

/// Collect one dispatched request's bounded response, stopping at the
/// statement's deadline.
///
/// EVERY exit that ends because the deadline elapsed reports
/// [`crate::Error::DeadlineExceeded`]. One condition produces one error
/// variant, so a client sees SQLSTATE `57014` for its own timeout whichever
/// half of the race won — the Data Plane refusing an expired task, or this
/// timer firing while a core is still inside a stage with no safe point.
///
/// A producer that stopped after the deadline passed stopped BECAUSE the
/// statement ran out of time. Reporting the closed channel there would report
/// the symptom and hand the client a generic internal error for its own
/// timeout.
pub(crate) async fn collect_under_deadline(
    rx: &mut crate::control::ResponseReceiver,
    params: DeadlineCollect<'_>,
) -> crate::Result<Response> {
    let DeadlineCollect {
        request_id,
        deadline,
        max_result_bytes,
        context,
    } = params;
    let deadline_error = || crate::Error::DeadlineExceeded { request_id };

    match tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        collect_bounded_response(rx, max_result_bytes),
    )
    .await
    {
        Err(_) => Err(deadline_error()),
        Ok(Ok(response)) => Ok(response),
        Ok(Err(DispatchCollectError::OverBudget { bytes })) => {
            Err(crate::Error::ExecutionLimitExceeded {
                detail: format!(
                    "{context} exceeded max_query_result_bytes \
                     ({bytes} > {max_result_bytes} bytes)"
                ),
            })
        }
        Ok(Err(DispatchCollectError::ChannelClosed)) if Instant::now() >= deadline => {
            Err(deadline_error())
        }
        Ok(Err(DispatchCollectError::ChannelClosed)) => Err(crate::Error::Dispatch {
            detail: format!("{context} channel closed"),
        }),
    }
}

#[cfg(test)]
mod collect_budget_tests {
    use super::*;
    use crate::bridge::envelope::{Payload, Status};
    use crate::types::{Lsn, RequestId};
    use tokio::sync::mpsc;

    use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};

    /// A standalone msgpack array of `n` one-byte elements — the shape a streamed
    /// scan chunk has (`encode_raw_document_rows` per chunk).
    fn array_payload(n: usize) -> Vec<u8> {
        let rows: Vec<Vec<u8>> = (0..n).map(|i| vec![(i % 128) as u8]).collect();
        encode_msgpack_array(&rows)
    }

    fn partial_rows(n: usize) -> Response {
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

    fn final_rows(n: usize) -> Response {
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

    /// Raw (non-array) payload, sized in bytes, for the budget-ceiling tests.
    fn partial_bytes(bytes: usize) -> Response {
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

    fn final_bytes(bytes: usize) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(vec![0u8; bytes]),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    #[tokio::test]
    async fn non_streaming_single_response_passes_through() {
        let (tx, rx) = mpsc::channel(4);
        let mut rx = crate::control::ResponseReceiver::from_channel(rx);
        tx.send(final_bytes(100)).await.unwrap();
        drop(tx);
        // Single terminal frame returns unmodified — no merge, exact bytes.
        let resp = collect_bounded_response(&mut rx, 1024).await.unwrap();
        assert_eq!(resp.payload.len(), 100);
    }

    #[tokio::test]
    async fn streaming_merges_all_chunk_arrays() {
        // Three standalone array chunks must merge into ONE array with every
        // element — the regression: raw concatenation kept only the first array.
        let (tx, rx) = mpsc::channel(4);
        let mut rx = crate::control::ResponseReceiver::from_channel(rx);
        tx.send(partial_rows(1000)).await.unwrap();
        tx.send(partial_rows(1000)).await.unwrap();
        tx.send(final_rows(500)).await.unwrap();
        drop(tx);
        let resp = collect_bounded_response(&mut rx, 1 << 20).await.unwrap();
        let elements = extract_msgpack_elements(resp.payload.as_ref());
        assert_eq!(
            elements.len(),
            2500,
            "streamed chunks must merge into one array of all rows, not just the first chunk"
        );
    }

    #[tokio::test]
    async fn streaming_over_budget_on_partial_aborts() {
        let (tx, rx) = mpsc::channel(4);
        let mut rx = crate::control::ResponseReceiver::from_channel(rx);
        tx.send(partial_bytes(600)).await.unwrap();
        tx.send(partial_bytes(600)).await.unwrap();
        drop(tx);
        let err = collect_bounded_response(&mut rx, 1000).await.unwrap_err();
        match err {
            DispatchCollectError::OverBudget { bytes } => assert!(bytes > 1000),
            DispatchCollectError::ChannelClosed => panic!("expected OverBudget, got ChannelClosed"),
        }
    }

    #[tokio::test]
    async fn streaming_over_budget_on_final_chunk_aborts() {
        let (tx, rx) = mpsc::channel(4);
        let mut rx = crate::control::ResponseReceiver::from_channel(rx);
        tx.send(partial_bytes(500)).await.unwrap();
        tx.send(final_bytes(600)).await.unwrap();
        drop(tx);
        let err = collect_bounded_response(&mut rx, 1000).await.unwrap_err();
        assert!(matches!(err, DispatchCollectError::OverBudget { .. }));
    }

    #[tokio::test]
    async fn a_collect_past_the_deadline_reports_the_deadline() {
        let (_tx, rx) = mpsc::channel(4);
        let mut rx = crate::control::ResponseReceiver::from_channel(rx);
        let result = collect_under_deadline(
            &mut rx,
            DeadlineCollect {
                request_id: RequestId::new(4),
                deadline: Instant::now(),
                max_result_bytes: 1024,
                context: "gather on core 0",
            },
        )
        .await;
        match result {
            Err(crate::Error::DeadlineExceeded { request_id }) => {
                assert_eq!(request_id, RequestId::new(4));
            }
            other => panic!("expected the deadline variant, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_producer_that_stopped_after_the_deadline_reports_the_deadline() {
        // The channel closes rather than answering, and the deadline has
        // already passed: the closure follows from the statement running out
        // of time, so reporting it would report the symptom.
        let (tx, rx) = mpsc::channel(4);
        let mut rx = crate::control::ResponseReceiver::from_channel(rx);
        tx.send(partial_bytes(10)).await.unwrap();
        drop(tx);
        let result = collect_under_deadline(
            &mut rx,
            DeadlineCollect {
                request_id: RequestId::new(5),
                deadline: Instant::now(),
                max_result_bytes: 1024,
                context: "gather on core 0",
            },
        )
        .await;
        assert!(
            matches!(result, Err(crate::Error::DeadlineExceeded { .. })),
            "got {result:?}"
        );
    }

    #[tokio::test]
    async fn a_producer_that_stopped_inside_the_budget_reports_the_closure() {
        let (tx, rx) = mpsc::channel(4);
        let mut rx = crate::control::ResponseReceiver::from_channel(rx);
        tx.send(partial_bytes(10)).await.unwrap();
        drop(tx);
        let result = collect_under_deadline(
            &mut rx,
            DeadlineCollect {
                request_id: RequestId::new(6),
                deadline: Instant::now() + std::time::Duration::from_secs(30),
                max_result_bytes: 1024,
                context: "gather on core 0",
            },
        )
        .await;
        assert!(
            matches!(result, Err(crate::Error::Dispatch { .. })),
            "got {result:?}"
        );
    }

    #[tokio::test]
    async fn channel_closed_without_final_is_explicit_error() {
        let (tx, rx) = mpsc::channel(4);
        let mut rx = crate::control::ResponseReceiver::from_channel(rx);
        tx.send(partial_bytes(10)).await.unwrap();
        drop(tx);
        let err = collect_bounded_response(&mut rx, 1024).await.unwrap_err();
        assert!(matches!(err, DispatchCollectError::ChannelClosed));
    }
}
