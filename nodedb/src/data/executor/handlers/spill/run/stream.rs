// SPDX-License-Identifier: BUSL-1.1

//! Streaming read of a sorted run from a block onward. One refill buffer and
//! one record are resident, never the run.

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
    /// Stream `run` from the block a scan from `from_key` starts at, or from
    /// the first record when `from_key` is `None`. Records before
    /// `from_key` can come first: the caller skips them.
    pub(in crate::data::executor) fn open(
        run: &SortedRun,
        from_key: Option<&[u8]>,
    ) -> crate::Result<Self> {
        let block = from_key.map_or(0, |key| run.start_block(key));
        let reader =
            FrameStreamReader::open_at(run.path(), run.block_offset(block), STREAM_CHUNK_BYTES)?;
        Ok(Self {
            reader,
            path: run.path().to_path_buf(),
        })
    }

    /// The next record, or `None` at the run's end.
    pub(in crate::data::executor) fn next_record(&mut self) -> crate::Result<Option<RunRecord>> {
        match self.reader.next_row()? {
            Some(body) => Ok(Some(RunRecord::decode(&self.path, &body)?)),
            None => Ok(None),
        }
    }
}
