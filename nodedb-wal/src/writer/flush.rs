// SPDX-License-Identifier: Apache-2.0

use std::os::unix::io::AsRawFd;

use crate::error::{Result, WalError};

use super::core::WalWriter;

impl WalWriter {
    /// Flush the aligned buffer to the file.
    ///
    /// On failure the buffer and `file_offset` are left untouched, so the
    /// batch is retried byte-for-byte at the same offset. A partial write that
    /// then errors leaves a torn tail on disk, which recovery discards.
    pub(super) fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Fault injection: a full device. The buffer and `file_offset` must
        // survive untouched so the batch can be retried once space is freed.
        nodedb_types::fail_point_err!("wal::flush_out_of_space", |_: String| {
            const SITE: &str = "WAL segment append (failpoint)";
            let err = WalError::OutOfSpace { context: SITE };
            crate::diag::out_of_space(&err, SITE, self.file_offset, self.buffer.len() as u64);
            err
        });

        let data = if self.config.use_direct_io {
            // O_DIRECT requires aligned I/O size. Padding is framed as a
            // record first so the batch boundary stays replayable.
            crate::record::pad_buffer_to_alignment(
                &mut self.buffer,
                self.config.alignment,
                "write buffer has no room for its alignment padding record",
            )?;
            self.buffer.as_aligned_slice()
        } else {
            // Without O_DIRECT, write only the actual data.
            self.buffer.as_slice()
        };

        pwrite_all(&self.file, data, self.file_offset)?;
        let written = data.len() as u64;
        self.buffer.clear();
        self.note_written(written);
        Ok(())
    }

    /// Advance the tail past `len` bytes that reached the file.
    ///
    /// The bytes are in the file but only in the page cache, so a later
    /// `sync` still owes an fsync.
    pub(super) fn note_written(&mut self, len: u64) {
        self.file_offset += len;
        self.durability.record_flush();
        self.flush_epoch += 1;
    }
}

/// Write all of `data` at `offset`, retrying on short writes.
pub(super) fn pwrite_all(file: &std::fs::File, data: &[u8], offset: u64) -> Result<()> {
    let fd = file.as_raw_fd();
    let mut remaining = data;
    let mut write_offset = offset;
    while !remaining.is_empty() {
        // SAFETY: `remaining` is a live slice of `remaining.len()` bytes.
        let written = unsafe {
            libc::pwrite(
                fd,
                remaining.as_ptr() as *const libc::c_void,
                remaining.len(),
                write_offset as libc::off_t,
            )
        };
        if written < 0 {
            return Err(write_error(
                "WAL segment append",
                write_offset,
                remaining.len() as u64,
            ));
        }
        let n = written as usize;
        if n == 0 {
            // A zero-length write makes no progress; retrying would
            // spin forever.
            return Err(WalError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "WAL pwrite made no progress",
            )));
        }
        remaining = &remaining[n..];
        write_offset += n as u64;
    }
    Ok(())
}

/// Classify the current `errno` from a failed WAL write.
fn write_error(context: &'static str, offset: u64, pending: u64) -> WalError {
    classify_write_error(std::io::Error::last_os_error(), context, offset, pending)
}

/// Classify a failed WAL write. Shared by the `pwrite` and io_uring writers.
///
/// A full device is called out separately from generic I/O failure: it is not
/// transient, retrying cannot succeed, and the caller must stop acknowledging
/// writes rather than treat it as a passing error. `offset` and `pending` say
/// where the batch stalled and how much of it never reached the file, which is
/// what a report needs to describe the write that could not complete.
pub(crate) fn classify_write_error(
    err: std::io::Error,
    context: &'static str,
    offset: u64,
    pending: u64,
) -> WalError {
    if err.raw_os_error() == Some(libc::ENOSPC) {
        let out_of_space = WalError::OutOfSpace { context };
        crate::diag::out_of_space(&out_of_space, context, offset, pending);
        return out_of_space;
    }
    WalError::Io(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enospc_is_out_of_space_and_other_errors_are_io() {
        let full = std::io::Error::from_raw_os_error(libc::ENOSPC);
        assert!(matches!(
            classify_write_error(full, "test", 0, 1),
            WalError::OutOfSpace { context: "test" }
        ));
        let eio = std::io::Error::from_raw_os_error(libc::EIO);
        assert!(matches!(
            classify_write_error(eio, "test", 0, 1),
            WalError::Io(_)
        ));
    }
}
