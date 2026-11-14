// SPDX-License-Identifier: BUSL-1.1

//! The content of a stream marker, read back from the WAL for its install.
//!
//! A chunked redo's final entry installs a marker record whose only
//! sub-record names the stream's `RedoChunk` records. The core reads those
//! records back through its io_uring cursor, one bounded batch of whole
//! framed records at a time, checks each chunk against the length and
//! checksum the reference recorded, and decodes the content into the record
//! the install runs over. The record takes the marker's Calvin stamp and
//! cross-shard key, its publish position and its stamp floor, exactly as
//! every whole-WAL reader rebuilds the marker.
//!
//! The same reader answers `MetaOp::ReadRedoStreamPage`: one bounded page
//! of the content's identity or row-change section, for the Control Plane.

use nodedb_physical::physical_plan::{RedoStreamSection, RedoSumTargets};
use nodedb_types::calvin::VersionedReadEntry;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::data::io::uring_wal_cursor::UringWalCursor;
use crate::wal::redo::frame::{FrameBatcher, FramedRedoBuilder};
use crate::wal::redo::read_stream_page;
use crate::wal::{RedoRecord, RedoStreamReader, RedoStreamRef};

use super::passes::final_refusal;

/// Bytes of whole framed records one batch of a stream read holds.
pub(super) const STREAM_BATCH_BYTES: usize = 1024 * 1024;

/// What a stream's content carries beside its record.
pub(super) struct StreamCarried {
    /// The record's encoding, at the marker's LSN.
    pub bytes: Vec<u8>,
    /// The materialized-sum resolution the content's header carries.
    pub sum_targets: Vec<RedoSumTargets>,
    /// The read set a snapshot commit validates.
    pub reads: Vec<VersionedReadEntry>,
}

/// A marker's content, decoded.
pub(super) struct StreamedContent {
    pub record: RedoRecord,
    pub carried: StreamCarried,
}

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

impl CoreLoop {
    /// The content the stream marker `redo` names through `reference`.
    pub(super) fn streamed_content(
        &self,
        redo: &RedoRecord,
        reference: RedoStreamRef,
    ) -> Result<StreamedContent, ErrorCode> {
        let reader = RedoStreamReader::<UringWalCursor>::open(&self.wal_source, reference.clone())
            .map_err(|error| unreadable(&reference, error))?;
        let mut batcher = FrameBatcher::new(reader, STREAM_BATCH_BYTES);
        let mut builder = FramedRedoBuilder::default();
        loop {
            let batch = match batcher.next_batch() {
                Ok(Some(batch)) => batch,
                Ok(None) => break,
                Err(error) => return Err(unreadable(&reference, error)),
            };
            // Every replica holds the same bytes: content that does not
            // decode refuses alike everywhere.
            builder
                .push(&batch)
                .map_err(|error| final_refusal(error.into()))?;
        }
        let mut framed = builder
            .finish()
            .map_err(|error| final_refusal(error.into()))?;
        let sum_targets = std::mem::take(&mut framed.header.sum_targets);
        let reads = std::mem::take(&mut framed.reads);
        let record = framed
            .into_installed(redo, &reference)
            .map_err(|error| final_refusal(error.into()))?;
        let bytes = record
            .to_bytes()
            .map_err(|error| final_refusal(error.into()))?;
        Ok(StreamedContent {
            record,
            carried: StreamCarried {
                bytes,
                sum_targets,
                reads,
            },
        })
    }
}
