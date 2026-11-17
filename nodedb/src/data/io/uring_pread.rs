// SPDX-License-Identifier: BUSL-1.1

//! io_uring positional reader for the Data Plane.
//!
//! One ring serves many reads at arbitrary offsets of many files. The
//! transaction spill tier reads one index block of a sorted run per lookup
//! through it, so a lookup costs one `IORING_OP_READ` and no ring setup.
//!
//! Like its siblings, this is `!Send`: one TPC core owns it, and it drives I/O
//! with the synchronous model (`submit_and_wait` and drain one completion). It
//! never uses tokio and never reads file contents through blocking `std::fs`.
//! Files are buffered (no `O_DIRECT`), so offsets and buffers carry no
//! alignment constraint.

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

/// Queue depth of the reader's ring. Reads are sequential: one in flight.
#[cfg(target_os = "linux")]
const QUEUE_DEPTH: u32 = 4;

/// Positional io_uring reader. Not `Send`.
#[cfg(target_os = "linux")]
pub struct UringPread {
    ring: io_uring::IoUring,
}

#[cfg(not(target_os = "linux"))]
pub struct UringPread;

#[cfg(target_os = "linux")]
impl UringPread {
    /// Open a ring. `None` when io_uring is unavailable (an old kernel, or a
    /// ring or locked-memory limit reached). The caller then reads through
    /// its blocking fallback.
    pub fn new() -> Option<Self> {
        let ring = io_uring::IoUring::new(QUEUE_DEPTH).ok()?;
        Some(Self { ring })
    }

    /// Fill `dst` from `file` at `offset`.
    ///
    /// Returns `Ok(true)` when `dst` is full and `Ok(false)` when the file
    /// ends first. A short read with bytes left resubmits the remainder.
    pub fn read_exact_at(
        &mut self,
        file: &std::fs::File,
        offset: u64,
        dst: &mut [u8],
    ) -> crate::Result<bool> {
        let mut filled = 0usize;
        while filled < dst.len() {
            let remaining = &mut dst[filled..];
            let read_len = remaining.len().min(u32::MAX as usize) as u32;
            let read_op = io_uring::opcode::Read::new(
                io_uring::types::Fd(file.as_raw_fd()),
                remaining.as_mut_ptr(),
                read_len,
            )
            .offset(offset + filled as u64)
            .build()
            .user_data(0);

            // SAFETY: `remaining` borrows `dst`, which outlives the SQE: the
            // loop waits for the CQE before it touches `dst` again. The fd is
            // valid for the borrow of `file`.
            unsafe {
                self.ring
                    .submission()
                    .push(&read_op)
                    .map_err(|e| crate::Error::Storage {
                        engine: "uring_pread".into(),
                        detail: format!("submission queue push failed: {e}"),
                    })?;
            }
            self.ring.submit_and_wait(1).map_err(crate::Error::Io)?;
            let cqe = self
                .ring
                .completion()
                .next()
                .ok_or_else(|| crate::Error::Storage {
                    engine: "uring_pread".into(),
                    detail: "read completion missing after submit_and_wait".into(),
                })?;
            let res = cqe.result();
            if res < 0 {
                return Err(crate::Error::Io(std::io::Error::from_raw_os_error(-res)));
            }
            if res == 0 {
                return Ok(false);
            }
            filled += res as usize;
        }
        Ok(true)
    }
}

#[cfg(not(target_os = "linux"))]
impl UringPread {
    /// io_uring is Linux-only. Callers read through their blocking fallback.
    pub fn new() -> Option<Self> {
        None
    }

    /// Stub so generic call sites compile on non-Linux hosts.
    pub fn read_exact_at(
        &mut self,
        _file: &std::fs::File,
        _offset: u64,
        _dst: &mut [u8],
    ) -> crate::Result<bool> {
        Ok(false)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn reads_ranges_at_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let data: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &data).unwrap();
        let Some(mut reader) = UringPread::new() else {
            return;
        };
        let file = std::fs::File::open(&path).unwrap();
        let mut buf = vec![0u8; 300];
        assert!(reader.read_exact_at(&file, 4_000, &mut buf).unwrap());
        assert_eq!(&buf[..], &data[4_000..4_300]);
        let mut tail = vec![0u8; 50];
        assert!(!reader.read_exact_at(&file, 9_980, &mut tail).unwrap());
    }
}
