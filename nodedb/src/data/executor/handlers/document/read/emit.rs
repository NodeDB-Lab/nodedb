// SPDX-License-Identifier: BUSL-1.1

//! Response emission helpers for document scans.
//!
//! Rows are emitted as MessagePack passthrough (`encode_raw_document_rows`),
//! with the chunked-streaming contract honoured when row count exceeds
//! `stream_chunk_size`.
//!
//! A chunk boundary is a deadline safe point. A statement that goes over
//! mid-stream returns a terminal `DeadlineExceeded` frame instead of its last
//! chunk. The Control Plane's collect keeps the frame's error status and drops
//! the accumulated rows, so a timed-out stream reaches the client as SQLSTATE
//! `57014` and never as a short result set.

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
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
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
            // Safe point: a chunk boundary. See the module docs.
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
                    let _ = self.response_tx.try_push(BridgeResponse { inner: partial });
                }
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
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
