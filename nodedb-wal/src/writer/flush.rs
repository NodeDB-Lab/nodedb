// SPDX-License-Identifier: Apache-2.0

use crate::error::Result;
// The unix `pwrite` path and the wasm seek+write path both construct an error
// value directly; elsewhere failures propagate as `Result` from calls that
// build their own.
#[cfg(any(unix, target_arch = "wasm32"))]
use crate::error::WalError;

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

        // Write at the exact offset. The unix path uses `pwrite` and retries
        // short writes. `cfg(unix)` is false on wasm32-wasip1 (its
        // `target_family` is "wasm", not "unix"), so wasi needs its own arm or
        // this function would advance `file_offset`, clear the buffer and
        // report the flush with nothing written at all.
        #[cfg(all(unix, not(target_arch = "wasm32")))]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();
            let mut remaining = data;
            let mut write_offset = self.file_offset;
            while !remaining.is_empty() {
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
        }

        // wasi has no positional write, and the file is only ever appended to,
        // so seeking to the current end and writing is equivalent. The shared
        // `file_offset` advance below accounts for the bytes; nothing is
        // advanced here.
        #[cfg(target_arch = "wasm32")]
        {
            use std::io::{Seek as _, SeekFrom, Write as _};
            self.file
                .seek(SeekFrom::Start(self.file_offset))
                .map_err(WalError::Io)?;
            self.file.write_all(data).map_err(|_| {
                write_error("WAL segment append", self.file_offset, data.len() as u64)
            })?;
        }

        self.file_offset += data.len() as u64;
        self.buffer.clear();

        // The bytes are in the file but only in the page cache. Recorded
        // before the buffer's contents are forgotten so a later `sync` still
        // knows an fsync is owed.
        self.durability.record_flush();
        Ok(())
    }
}

/// Classify the current `errno` from a failed WAL write.
///
/// A full device is called out separately from generic I/O failure: it is not
/// transient, retrying cannot succeed, and the caller must stop acknowledging
/// writes rather than treat it as a passing error. `offset` and `pending` say
/// where the batch stalled and how much of it never reached the file, which is
/// what a report needs to describe the write that could not complete.
///
/// Gated to match its call sites: the `pwrite` loop is unix-only and the wasm
/// seek+write arm also classifies its failure through here. The ENOSPC branch
/// itself stays unix-only — on wasi `raw_os_error()` is `None`, so the generic
/// `WalError::Io` branch is what wasm takes.
#[cfg(any(unix, target_arch = "wasm32"))]
fn write_error(context: &'static str, offset: u64, pending: u64) -> WalError {
    let err = std::io::Error::last_os_error();
    #[cfg(unix)]
    if err.raw_os_error() == Some(libc::ENOSPC) {
        let out_of_space = WalError::OutOfSpace { context };
        crate::diag::out_of_space(&out_of_space, context, offset, pending);
        return out_of_space;
    }
    let _ = (context, offset, pending);
    WalError::Io(err)
}
