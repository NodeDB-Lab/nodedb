// SPDX-License-Identifier: Apache-2.0

//! Write and fsync submission for [`UringWriter`].

use std::os::unix::io::AsRawFd;

use io_uring::{IoUring, opcode, types};

use crate::error::{Result, WalError};
use crate::record::one_shot_record_buffer;
use crate::writer::flush::classify_write_error;

use super::core::UringWriter;

impl UringWriter {
    /// Submit the buffered data via io_uring write + fsync, and wait for completion.
    ///
    /// This is the group commit point: all records appended since the last
    /// `submit_and_sync()` become durable after this returns.
    ///
    /// An empty buffer alone does not mean there is nothing to do: the write
    /// CQE clears the buffer before the fsync is even submitted, so a batch
    /// whose fsync failed leaves the writer empty-buffered with its records
    /// still only in the page cache. The fsync is skipped only when nothing
    /// is outstanding.
    pub fn submit_and_sync(&mut self) -> Result<()> {
        self.durability.check()?;

        if self.durability.should_skip_sync(self.buffer.is_empty()) {
            return Ok(());
        }
        self.submit_and_wait_write()?;
        self.submit_and_wait_fsync()?;
        Ok(())
    }

    /// Submit the buffered batch and wait until all of it is written.
    ///
    /// ## O_DIRECT alignment invariant
    ///
    /// When `use_direct_io` is true, the batch is first padded out to the
    /// alignment boundary with a framed `Noop` record (see
    /// [`crate::record::padding`]) and submitted via `as_aligned_slice`.
    /// `file_offset` MUST advance by the padded length, not the unpadded
    /// buffer content length. Advancing by the unpadded length leaves the
    /// next submission's offset unaligned, and the kernel rejects the
    /// write with `-EINVAL`. Mirrors `WalWriter::flush_buffer`.
    pub(super) fn submit_and_wait_write(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let data = if self.config.use_direct_io {
            crate::record::pad_buffer_to_alignment(
                &mut self.buffer,
                self.config.alignment,
                "io_uring write buffer has no room for its alignment padding record",
            )?;
            self.buffer.as_aligned_slice()
        } else {
            self.buffer.as_slice()
        };
        let fd = types::Fd(self.file.as_raw_fd());
        write_all_at(&mut self.ring, fd, data, self.file_offset)?;
        let written = data.len() as u64;
        self.buffer.clear();
        self.note_written(written);
        Ok(())
    }

    /// Write one framed record to the file by itself, after the buffered
    /// batch. See [`crate::record::padding`] for the framing.
    ///
    /// On failure `file_offset` is unchanged, so the next write lands at the
    /// same offset and covers any partial bytes.
    pub(super) fn write_alone(&mut self, header: &[u8], payload: &[u8]) -> Result<()> {
        self.submit_and_wait_write()?;
        let one_shot = one_shot_record_buffer(
            header,
            payload,
            self.config.alignment,
            self.config.use_direct_io,
        )?;
        let fd = types::Fd(self.file.as_raw_fd());
        write_all_at(&mut self.ring, fd, one_shot.as_slice(), self.file_offset)?;
        self.note_written(one_shot.len() as u64);
        Ok(())
    }

    /// Advance the tail past `len` written bytes.
    ///
    /// The bytes are in the file but not yet fsynced, so a later
    /// `submit_and_sync` still owes an fsync.
    fn note_written(&mut self, len: u64) {
        self.file_offset += len;
        self.durability.record_flush();
    }

    /// Submit an fsync SQE and wait for the CQE.
    ///
    /// A reported failure poisons the writer — see [`WalError::DurabilityLost`].
    fn submit_and_wait_fsync(&mut self) -> Result<()> {
        // Crash injection: the kernel reports a writeback error at the fsync.
        #[cfg(feature = "failpoints")]
        if let Some(detail) = nodedb_types::fail_point::eval_fail(
            nodedb_types::fail_point::FailScope::Any,
            "wal::fsync_failure",
        ) {
            return Err(self.durability.poison(format!(
                "io_uring fsync failed: failpoint wal::fsync_failure: {detail}"
            )));
        }

        let fd = types::Fd(self.file.as_raw_fd());
        let fsync_op = opcode::Fsync::new(fd).build().user_data(0x02);

        // SAFETY: the fsync SQE references no memory.
        unsafe {
            self.ring
                .submission()
                .push(&fsync_op)
                .map_err(|_| WalError::Io(std::io::Error::other("io_uring SQ full")))?;
        }

        self.ring.submit_and_wait(1).map_err(WalError::Io)?;

        let cqe =
            self.ring.completion().next().ok_or_else(|| {
                WalError::Io(std::io::Error::other("io_uring: no CQE after fsync"))
            })?;

        // A negative fsync result is the kernel reporting the writeback error
        // it will never report again; the dirty pages are already gone.
        // Submission-side failures above are different — the fsync was never
        // issued, so the outstanding-flush marker stands and a retry re-issues
        // it.
        if cqe.result() < 0 {
            let err = std::io::Error::from_raw_os_error(-cqe.result());
            return Err(self
                .durability
                .poison(format!("io_uring fsync failed: {err}")));
        }

        self.durability.record_sync_ok();
        Ok(())
    }
}

/// The write site named in an out-of-space report.
const WRITE_SITE: &str = "WAL segment append (io_uring)";

/// Write all of `data` at `offset` through `ring`, resubmitting the rest of a
/// short write. A full device is [`WalError::OutOfSpace`].
fn write_all_at(ring: &mut IoUring, fd: types::Fd, data: &[u8], offset: u64) -> Result<()> {
    let mut done = 0usize;
    while done < data.len() {
        let rest = &data[done..];
        let at = offset + done as u64;

        // Fault injection: the kernel reports a full device for this write.
        #[cfg(feature = "failpoints")]
        if nodedb_types::fail_point::eval_fail(
            nodedb_types::fail_point::FailScope::Any,
            "wal::flush_out_of_space",
        )
        .is_some()
        {
            return Err(classify_write_error(
                std::io::Error::from_raw_os_error(libc::ENOSPC),
                WRITE_SITE,
                at,
                rest.len() as u64,
            ));
        }

        let len = u32::try_from(rest.len()).map_err(|_| {
            WalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "io_uring WAL write is longer than u32::MAX bytes",
            ))
        })?;
        let write_op = opcode::Write::new(fd, rest.as_ptr(), len)
            .offset(at)
            .build()
            .user_data(0x01);

        // SAFETY: write_op holds a raw pointer into `data`, which outlives
        // this function. submit_and_wait(1) returns only after the CQE, so the
        // kernel is done with the memory before `data` can be touched again.
        // Do NOT pipeline submissions without keeping the buffer alive.
        unsafe {
            ring.submission()
                .push(&write_op)
                .map_err(|_| WalError::Io(std::io::Error::other("io_uring SQ full")))?;
        }

        ring.submit_and_wait(1).map_err(WalError::Io)?;

        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| WalError::Io(std::io::Error::other("io_uring: no CQE after write")))?;

        let result = cqe.result();
        if result < 0 {
            return Err(classify_write_error(
                std::io::Error::from_raw_os_error(-result),
                WRITE_SITE,
                at,
                rest.len() as u64,
            ));
        }
        if result == 0 {
            // A zero-length write makes no progress; resubmitting would
            // spin forever.
            return Err(WalError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "io_uring WAL write made no progress",
            )));
        }
        done += result as usize;
    }
    Ok(())
}
