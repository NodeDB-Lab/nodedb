// SPDX-License-Identifier: BUSL-1.1

//! Response emission helpers for document scans.
//!
//! Rows go out as msgpack passthrough via `encode_raw_document_rows`, in
//! chunks when the row count exceeds `stream_chunk_size`.
//!
//! A chunk boundary is a deadline safe point. A statement that goes over
//! mid-stream returns a terminal `DeadlineExceeded` frame instead of its last
//! chunk. The Control Plane's collect keeps the frame's error status and drops
//! the accumulated rows, so a timed-out stream reaches the client as SQLSTATE
//! `57014` and never as a short result set. A chunk the response ring refuses
//! ends the stream the same way, with a terminal `ResourcesExhausted` frame.
//!
//! Every stream ends in a terminal frame. The collect drains `Partial` frames
//! until a terminal one arrives.

use tracing::warn;

use crate::bridge::dispatch::BridgeResponse;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Send raw document rows with msgpack passthrough (no decode+re-encode).
    pub(in crate::data::executor) fn send_document_rows_raw(
        &mut self,
        task: &ExecutionTask,
        rows: &[(String, Vec<u8>)],
        chunk_size: usize,
    ) -> Response {
        if rows.len() <= chunk_size {
            match response_codec::encode_raw_document_rows(rows) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(task, ErrorCode::from(e)),
            }
        } else {
            self.stream_chunks_raw(task, rows, chunk_size)
        }
    }

    /// Stream raw document rows in chunks with msgpack passthrough.
    fn stream_chunks_raw(
        &mut self,
        task: &ExecutionTask,
        rows: &[(String, Vec<u8>)],
        chunk_size: usize,
    ) -> Response {
        let deadline = crate::data::executor::deadline::DeadlineCheck::for_task(task);
        let chunks: Vec<_> = rows.chunks(chunk_size).collect();
        let last_idx = chunks.len().saturating_sub(1);
        for (i, chunk) in chunks.iter().enumerate() {
            // Safe point: a chunk boundary. Every partial already pushed stays
            // on the ring, but the terminal frame this returns carries
            // `DeadlineExceeded`, and the Control Plane surfaces the error
            // rather than the rows it collected. See the module docs.
            if deadline.expired_now() {
                return self.response_error(task, ErrorCode::DeadlineExceeded);
            }
            let is_last = i == last_idx;
            match response_codec::encode_raw_document_rows(chunk) {
                Ok(payload) => {
                    if is_last {
                        return self.response_with_payload(task, payload);
                    }
                    let partial = self.response_partial(task, payload);
                    // A refused partial is rows the collector never sees. The
                    // terminal frame reports it, so the collector drops the
                    // rows it holds rather than render a short result.
                    if let Err(e) = self.response_tx.try_push(BridgeResponse { inner: partial }) {
                        warn!(
                            core = self.core_id,
                            error = %e,
                            "streamed scan chunk refused by the response ring"
                        );
                        return self.response_error(task, ErrorCode::ResourcesExhausted);
                    }
                }
                Err(e) => {
                    return self.response_error(task, ErrorCode::from(e));
                }
            }
        }
        self.response_error(
            task,
            ErrorCode::Internal {
                detail: "streaming response incomplete".into(),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
    use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

    use super::*;
    use crate::bridge::dispatch::BridgeRequest;
    use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request, Status};
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

    /// A core whose response ring holds `ring` frames, and the ring's reader.
    struct Harness {
        core: CoreLoop,
        responses: Consumer<BridgeResponse>,
        _requests: Producer<BridgeRequest>,
        _dir: tempfile::TempDir,
    }

    fn harness(ring: usize) -> Harness {
        let dir = tempfile::tempdir().expect("tempdir");
        let (requests, request_rx) = RingBuffer::channel::<BridgeRequest>(8);
        let (response_tx, responses) = RingBuffer::channel::<BridgeResponse>(ring);
        let core = CoreLoop::open(
            0,
            request_rx,
            response_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        Harness {
            core,
            responses,
            _requests: requests,
            _dir: dir,
        }
    }

    fn task() -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Compact),
            deadline: Instant::now() + Duration::from_secs(30),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            system_ordinal: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    /// `n` document rows, each the msgpack map `{"k": i}`.
    fn rows(n: u8) -> Vec<(String, Vec<u8>)> {
        (0..n)
            .map(|i| (format!("d{i}"), vec![0x81, 0xa1, b'k', i]))
            .collect()
    }

    /// A stream of four one-row chunks pushes three partial frames and ends
    /// in a terminal frame that carries the last chunk.
    #[test]
    fn a_streamed_scan_ends_in_a_terminal_frame() {
        let mut h = harness(8);
        let last = h.core.send_document_rows_raw(&task(), &rows(4), 1);
        assert!(!last.partial);
        assert_eq!(last.status, Status::Ok, "{:?}", last.error_code);
        let mut partials = 0;
        while let Ok(frame) = h.responses.try_pop() {
            assert!(frame.inner.partial);
            partials += 1;
        }
        assert_eq!(partials, 3);
    }

    /// A chunk the full response ring refuses ends the stream in a terminal
    /// error frame, never a short success.
    #[test]
    fn a_refused_chunk_ends_the_stream_in_a_terminal_error() {
        let mut h = harness(2);
        let last = h.core.send_document_rows_raw(&task(), &rows(4), 1);
        assert!(!last.partial);
        assert_eq!(last.status, Status::Error);
        assert_eq!(
            last.error_code.as_deref(),
            Some(&ErrorCode::ResourcesExhausted)
        );
    }
}
