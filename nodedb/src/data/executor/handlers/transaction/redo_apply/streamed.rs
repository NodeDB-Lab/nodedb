// SPDX-License-Identifier: BUSL-1.1

//! Reads of a stream marker's content back from the WAL.
//!
//! A chunked redo's final entry installs a marker record whose only
//! sub-record names the stream's `RedoChunk` records. The core reads those
//! records back through its io_uring cursor, one bounded batch of whole
//! framed records at a time, and checks each chunk against the length and
//! checksum the reference recorded. The validate (`stream_validate`) and the
//! install (`batched`) each walk the content this way.
//!
//! The same reader answers `MetaOp::ReadRedoStreamPage`: one bounded page
//! of the content's identity or row-change section, for the Control Plane.

use nodedb_physical::physical_plan::RedoStreamSection;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::data::io::uring_wal_cursor::UringWalCursor;
use crate::wal::RedoStreamRef;
use crate::wal::redo::read_stream_page;

/// A read of the stream that failed on this node. The failure is no verdict
/// on the record, so the refusal is retryable.
pub(super) fn unreadable(reference: &RedoStreamRef, error: crate::Error) -> ErrorCode {
    ErrorCode::RetryableRefusal {
        reason: format!(
            "the chunk records of redo stream {:?} could not be read back from this node's \
             WAL: {error}",
            reference.stream
        ),
    }
}

impl CoreLoop {
    /// `MetaOp::ReadRedoStreamPage`: one page of a committed chunked redo's
    /// content, read back through this core's io_uring cursor. Nothing is
    /// written. The page answers as a zerompk-encoded `RedoStreamPage`.
    pub(in crate::data::executor) fn execute_read_redo_stream_page(
        &self,
        task: &ExecutionTask,
        stream: &[u8],
        section: RedoStreamSection,
        from: u64,
        max_page_bytes: u64,
    ) -> Response {
        let reference: RedoStreamRef = match zerompk::from_msgpack(stream) {
            Ok(reference) => reference,
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("a redo stream page request names no stream: {error}"),
                    },
                );
            }
        };
        let page = match read_stream_page::<UringWalCursor>(
            &self.wal_source,
            reference,
            section,
            from,
            max_page_bytes,
        ) {
            Ok(page) => page,
            Err(error) => return self.response_error(task, error),
        };
        match zerompk::to_msgpack_vec(&page) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(error) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("a redo stream page does not encode: {error}"),
                },
            ),
        }
    }
}
