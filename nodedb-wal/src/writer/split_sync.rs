// SPDX-License-Identifier: Apache-2.0

//! A sync split in three steps, so the fsync can run with no lock on the writer.
//!
//! 1. [`WalWriter::begin_sync`] writes the buffer to the file. It needs `&mut self`.
//! 2. [`PendingSync::fsync`] fsyncs a duplicate of the file handle. It needs no writer.
//! 3. [`WalWriter::finish_sync`] records the outcome. It needs `&mut self`.
//!
//! Appends can run between the steps. An fsync covers what the file held when it started.
//! So a record appended after step 1 is not covered, and a caller claims only `through_lsn`.
//!
//! A flush between the steps keeps the fsync marker set, so a later sync fsyncs again.
//! A failed fsync latches the writer, as an inline [`WalWriter::sync`] does.
//! An earlier latch beats a later success, because that success cannot restore lost pages.

use std::fs::File;

use crate::error::{Result, WalError};

use super::core::WalWriter;
use super::durability::fsync_file;

/// A sync whose file writes are done and whose fsync has not run.
pub struct PendingSync {
    file: File,
    flush_epoch: u64,
    anchor: Option<(u64, u64)>,
    through_lsn: u64,
    /// First LSN of the segment this file holds. Set by `SegmentedWal`.
    pub(crate) segment_first_lsn: u64,
}

impl PendingSync {
    /// Fsync the file. The file stays valid after a roll or a truncation.
    pub fn fsync(&self) -> std::io::Result<()> {
        fsync_file(&self.file)
    }

    /// The highest LSN this sync makes durable.
    pub fn through_lsn(&self) -> u64 {
        self.through_lsn
    }
}

impl WalWriter {
    /// Write the buffer to the file and return the fsync still owed.
    ///
    /// Returns `None` when every written record is already durable.
    pub fn begin_sync(&mut self) -> Result<Option<PendingSync>> {
        if !self.prepare_sync()? {
            return Ok(None);
        }
        let file = self.file.try_clone().map_err(WalError::Io)?;
        Ok(Some(PendingSync {
            file,
            flush_epoch: self.flush_epoch,
            anchor: self.pending_anchor.take(),
            through_lsn: self.next_lsn().saturating_sub(1),
            segment_first_lsn: 0,
        }))
    }

    /// Record the outcome of `pending`'s fsync.
    ///
    /// `same_file` is whether `pending` fsynced this writer's file.
    /// A failed fsync of an older segment still latches this writer.
    pub fn finish_sync(
        &mut self,
        pending: PendingSync,
        fsynced: std::io::Result<()>,
        same_file: bool,
    ) -> Result<()> {
        self.durability.check()?;
        if let Err(e) = fsynced {
            return Err(self.durability.poison(format!("fsync failed: {e}")));
        }
        if same_file && self.flush_epoch == pending.flush_epoch {
            self.durability.record_sync_ok();
        }
        if let Some((lsn, hlc_wall_ns)) = pending.anchor {
            self.record_time_anchor(lsn, hlc_wall_ns);
        }
        Ok(())
    }
}
