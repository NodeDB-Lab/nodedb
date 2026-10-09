// SPDX-License-Identifier: BUSL-1.1

//! Synthetic Data-Plane responses for gathered and streamed results.

use crate::bridge::envelope::{Response, Status};
use crate::control::server::result_stream::ResultStream;
use crate::types::{Lsn, RequestId};

/// Materialize a [`ResultStream`] into a synthetic successful Response.
///
/// Used by the non-pgwire `Resolved::Stream` consumers (native, internal
/// funnel, recursive resolve) that need the fully-collected result as one
/// merged-array `Response`, preserving their prior gather-then-return behaviour.
pub(crate) async fn stream_to_response(stream: ResultStream) -> crate::Result<Response> {
    let (merged, watermark_lsn) =
        crate::control::server::result_stream::materialize(stream).await?;
    // A streamed result carries no per-collection read-version: the stream's
    // frames carry only per-batch watermarks, and nothing consumes a read-version
    // from this path — the streaming branch is gated on `txn_id.is_none()`
    // (`resolve/exchange.rs`), so a stream never serves an in-transaction read and
    // never reaches the read-set capture that would compare one.
    Ok(outcome_to_response(merged, watermark_lsn, Lsn::ZERO))
}

/// Build a synthetic successful Response from a gathered merged-array payload.
///
/// `read_version_lsn` is the gathered collection's `coll_write_lsn` at read time
/// (`GatherOutcome::read_version_lsn`) — the comparand cross-shard OCC read
/// validation consumes. It is distinct from `watermark_lsn` (the core-global
/// fence) and must be passed through: dropping it to `Lsn::ZERO` silently strips
/// the read's version and leaves validation to compare against whatever floor the
/// session's own writes supply.
pub(super) fn outcome_to_response(
    merged_array: Vec<u8>,
    watermark_lsn: Lsn,
    read_version_lsn: Lsn,
) -> Response {
    Response {
        request_id: RequestId::new(0),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(merged_array),
        watermark_lsn,
        error_code: None,
        stage_vote: None,
        read_version_lsn,
        write_set: Vec::new(),
    }
}
