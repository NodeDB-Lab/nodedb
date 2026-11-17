// SPDX-License-Identifier: BUSL-1.1

//! Streaming read of a sorted run from a block onward. One refill buffer and
//! one record are resident, never the run or its index. The stream skips
//! the bloom and index page frames between records.

use super::block::{FrameKind, frame_kind};
use super::block_reader::BlockReader;
use super::record::RunRecord;
use super::sorted_run::SortedRun;
use crate::data::executor::handlers::join::FrameStreamReader;

/// Refill buffer of one run stream. A merge over many runs holds one per
/// run, so it stays small.
const STREAM_CHUNK_BYTES: usize = 64 * 1024;

/// A forward cursor over a run's records.
pub(in crate::data::executor) struct RunStream {
    reader: FrameStreamReader,
    path: std::path::PathBuf,
}

impl RunStream {
    /// Stream `run` from its first record.
    pub(in crate::data::executor) fn from_start(run: &SortedRun) -> crate::Result<Self> {
        Self::at(run, 0)
    }

    /// Stream `run` from the block a scan from `key` starts at. Finding the
    /// block reads one index page through `reader`. Records before `key`
    /// can come first: the caller skips them.
    pub(in crate::data::executor) fn from_key(
        run: &SortedRun,
        key: &[u8],
        reader: &BlockReader,
    ) -> crate::Result<Self> {
        Self::at(run, run.scan_offset(key, reader)?)
    }

    fn at(run: &SortedRun, offset: u64) -> crate::Result<Self> {
        let reader = FrameStreamReader::open_at(run.path(), offset, STREAM_CHUNK_BYTES)?;
        Ok(Self {
            reader,
            path: run.path().to_path_buf(),
        })
    }

    /// The next record, or `None` at the run's end.
    pub(in crate::data::executor) fn next_record(&mut self) -> crate::Result<Option<RunRecord>> {
        while let Some(body) = self.reader.next_row()? {
            match frame_kind(&body) {
                FrameKind::Record => return Ok(Some(RunRecord::decode(&self.path, &body)?)),
                FrameKind::Bloom | FrameKind::Page => {}
            }
        }
        Ok(None)
    }
}
