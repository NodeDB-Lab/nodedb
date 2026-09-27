// SPDX-License-Identifier: Apache-2.0

//! Aligned-buffer helpers shared by the double-write write and recovery paths.

use std::fs::File;

use crate::align::AlignedBuf;
use crate::error::{Result, WalError};

/// Fill the unwritten tail of `buf` with zero bytes so an O_DIRECT write of
/// the entire aligned slot does not leak stale buffer contents to disk.
pub(crate) fn zero_tail(buf: &mut AlignedBuf) {
    let written = buf.len();
    let cap = buf.capacity();
    if written < cap {
        // SAFETY: `as_mut_ptr` is valid for `capacity` bytes; we write only
        // the uninitialized tail between `written..capacity`.
        unsafe {
            std::ptr::write_bytes(buf.as_mut_ptr().add(written), 0, cap - written);
        }
    }
}

/// View the entire allocated capacity of `buf` as a byte slice. Requires
/// that the caller has zeroed any unwritten tail (see `zero_tail`).
pub(crate) fn full_capacity_slice(buf: &AlignedBuf) -> &[u8] {
    // SAFETY: AlignedBuf guarantees `as_ptr` points to `capacity()` valid
    // bytes (alloc_zeroed) for the lifetime of the buffer.
    unsafe { std::slice::from_raw_parts(buf.as_ptr(), buf.capacity()) }
}

/// `pwrite`-retry helper that handles short writes.
///
/// Reports [`WalError::Unsupported`] on wasm32 rather than falling back to a
/// seek-and-write. A fallback would mirror the slot successfully and the writer
/// would then report `DwbProtection::Active`, but `recover_record` is a stub on
/// that target (slot recovery needs `pread`), so nothing could ever read the
/// slot back. The protection would be claimed and unusable. Failing here keeps
/// the writer's own degradation signal honest: the append still succeeds
/// (`DoubleWriteBuffer` failures are deliberately not fatal to the WAL), while
/// `dwb_protection()` reports `Degraded(WriteFailed)` and every record is
/// counted as unprotected.
pub(crate) fn pwrite_all(file: &File, data: &[u8], offset: u64) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        use std::os::unix::io::AsRawFd as _;
        let fd = file.as_raw_fd();
        let mut remaining = data;
        let mut write_offset = offset;
        while !remaining.is_empty() {
            // SAFETY: `remaining` is a live slice of `remaining.len()` bytes
            // and `fd` is owned by the borrowed `file`.
            let n = unsafe {
                libc::pwrite(
                    fd,
                    remaining.as_ptr() as *const libc::c_void,
                    remaining.len(),
                    write_offset as libc::off_t,
                )
            };
            if n < 0 {
                return Err(WalError::Io(std::io::Error::last_os_error()));
            }
            let n = n as usize;
            remaining = &remaining[n..];
            write_offset += n as u64;
        }
        Ok(())
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = (file, data, offset);
        Err(WalError::Unsupported {
            detail: "O_DIRECT pwrite not available on wasm32",
        })
    }
}
