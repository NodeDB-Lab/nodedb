// SPDX-License-Identifier: BUSL-1.1

//! io_uring-backed streaming single-file reader for the Data Plane.
//!
//! This is the read-side primitive for the document external-sort k-way merge,
//! which reads each spilled run incrementally — one row at a time advancing
//! through the file — rather than loading the whole run into memory at once.
//!
//! ## Streaming vs. whole-file batch
//!
//! Unlike [`super::uring_reader::UringReader`], which reads an entire file into
//! a single buffer (whole-file batch), `UringSeqReader` refills a *bounded*
//! buffer on demand via `IORING_OP_READ` at an advancing file offset. Peak
//! memory is therefore one refill buffer regardless of file size — the property
//! the streaming merge needs.
//!
//! ## Plane safety
//!
//! Like the writer/reader siblings, this is `!Send` — owned by a single TPC
//! core, owns its own io_uring ring, and drives I/O with the synchronous
//! poll-loop model (`submit_and_wait` + drain the completion). It NEVER uses
//! tokio and NEVER does blocking `std::fs` reads of file *contents* — only
//! `File::open` to obtain the fd.
//!
//! ## Buffered, not O_DIRECT
//!
//! Spill files are opened buffered (no `O_DIRECT`), so there are no alignment
//! constraints on the offset or the destination buffer — a plain `Vec<u8>`
//! refill buffer is used rather than an [`super::aligned_buf::AlignedBuf`].

use std::path::Path;

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

/// Queue depth for the reader's io_uring instance.
///
/// `refill` only ever has a single read in flight at a time (the streaming
/// model is sequential), so a shallow ring is sufficient.
#[cfg(target_os = "linux")]
const QUEUE_DEPTH: u32 = 8;

/// Default refill buffer / chunk size (256 KiB).
#[cfg(target_os = "linux")]
const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

/// Streaming single-file io_uring reader.
///
/// Not `Send` — owned by a single Data Plane core.
#[cfg(target_os = "linux")]
pub struct UringSeqReader {
    ring: io_uring::IoUring,
    /// Backing file (buffered — NOT O_DIRECT). Held open for the reader's
    /// lifetime; the io_uring ops read through its fd.
    file: std::fs::File,
    /// Next file position to read from (pread-style offset).
    file_offset: u64,
    /// Reusable refill buffer. Valid bytes are `buf[pos..filled]`.
    buf: Vec<u8>,
    /// Cursor of consumed bytes within `buf`.
    pos: usize,
    /// Number of valid bytes currently in `buf`.
    filled: usize,
    /// Set once a refill reads zero bytes (clean EOF).
    eof: bool,
}

#[cfg(not(target_os = "linux"))]
pub struct UringSeqReader;

#[cfg(target_os = "linux")]
impl UringSeqReader {
    /// Open `path` for streaming reads with a `chunk_size`-byte refill buffer.
    ///
    /// Returns `None` if io_uring is unavailable (old kernel, WASM) or the file
    /// cannot be opened, mirroring the `Option`-return fallback convention of
    /// the writer/reader siblings.
    pub fn open(path: &Path, chunk_size: usize) -> Option<Self> {
        Self::open_at(path, 0, chunk_size)
    }

    /// Open `path` for streaming reads that start at byte `offset`.
    pub fn open_at(path: &Path, offset: u64, chunk_size: usize) -> Option<Self> {
        let ring = io_uring::IoUring::new(QUEUE_DEPTH).ok()?;
        let file = std::fs::File::open(path).ok()?;

        Some(Self {
            ring,
            file,
            file_offset: offset,
            buf: vec![0u8; chunk_size.max(1)],
            pos: 0,
            filled: 0,
            eof: false,
        })
    }

    /// Open `path` with the default refill chunk size.
    pub fn open_default(path: &Path) -> Option<Self> {
        Self::open(path, DEFAULT_CHUNK_SIZE)
    }

    /// Refill the buffer with one `IORING_OP_READ` at the current file offset.
    ///
    /// Precondition: the buffer is fully drained (`pos == filled`). On a
    /// zero-byte read the `eof` flag is set and `filled` stays `0`.
    fn refill(&mut self) -> crate::Result<()> {
        self.pos = 0;
        self.filled = 0;

        // The io_uring read length is a `u32`; cap each SQE at `u32::MAX`. A
        // short read (n > 0 but < buf len) is handled naturally by the
        // `read_exact` loop, which calls `refill` again.
        let read_len = self.buf.len().min(u32::MAX as usize) as u32;

        let buf_ptr = self.buf.as_mut_ptr();
        let read_op = io_uring::opcode::Read::new(
            io_uring::types::Fd(self.file.as_raw_fd()),
            buf_ptr,
            read_len,
        )
        .offset(self.file_offset)
        .build()
        .user_data(0);

        // SAFETY: `buf_ptr` points into `self.buf`, which outlives the SQE (we
        // wait for the CQE before reading `self.buf`). The fd is valid for
        // `self.file`'s lifetime.
        unsafe {
            self.ring
                .submission()
                .push(&read_op)
                .map_err(|e| crate::Error::Storage {
                    engine: "sort_spill".into(),
                    detail: format!("uring submission queue push failed: {e}"),
                })?;
        }

        self.ring.submit_and_wait(1).map_err(crate::Error::Io)?;

        let cqe = self
            .ring
            .completion()
            .next()
            .ok_or_else(|| crate::Error::Storage {
                engine: "sort_spill".into(),
                detail: "uring read completion missing after submit_and_wait".into(),
            })?;

        let res = cqe.result();
        if res < 0 {
            // CQE result < 0 is a negated errno.
            return Err(crate::Error::Io(std::io::Error::from_raw_os_error(-res)));
        }

        let n = res as usize;
        if n == 0 {
            self.eof = true;
        } else {
            self.filled = n;
            self.file_offset += n as u64;
        }

        Ok(())
    }

    /// Fill `dst` with exactly `dst.len()` bytes from the stream.
    ///
    /// Returns:
    /// - `Ok(true)`  — `dst` was fully filled.
    /// - `Ok(false)` — clean EOF was reached before `dst` could be filled
    ///   (the caller decides whether a partial read is an error).
    /// - `Err(..)`   — an io_uring read failed.
    ///
    /// Short reads with `n > 0` are handled transparently by the refill loop;
    /// only a zero-byte read is treated as EOF. `dst.len() == 0` is `Ok(true)`.
    pub fn read_exact(&mut self, dst: &mut [u8]) -> crate::Result<bool> {
        let mut written = 0;
        while written < dst.len() {
            if self.pos == self.filled {
                if self.eof {
                    return Ok(false);
                }
                self.refill()?;
                if self.pos == self.filled {
                    // Refill hit EOF with no bytes available.
                    return Ok(false);
                }
            }
            let take = (dst.len() - written).min(self.filled - self.pos);
            dst[written..written + take].copy_from_slice(&self.buf[self.pos..self.pos + take]);
            self.pos += take;
            written += take;
        }
        Ok(true)
    }
}

#[cfg(not(target_os = "linux"))]
impl UringSeqReader {
    /// io_uring streaming reads are Linux-only. Non-Linux builds return `None`
    /// so callers fall back to their non-io_uring path.
    pub fn open(_path: &Path, _chunk_size: usize) -> Option<Self> {
        None
    }

    /// io_uring streaming reads are Linux-only. Non-Linux builds return `None`.
    pub fn open_at(_path: &Path, _offset: u64, _chunk_size: usize) -> Option<Self> {
        None
    }

    /// io_uring streaming reads are Linux-only. Non-Linux builds return `None`.
    pub fn open_default(_path: &Path) -> Option<Self> {
        None
    }

    /// Stub so generic call sites compile on non-Linux hosts.
    pub fn read_exact(&mut self, _dst: &mut [u8]) -> crate::Result<bool> {
        Ok(false)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    fn data_of(size: usize, seed: u8) -> Vec<u8> {
        (0..size)
            .map(|i| ((i + seed as usize) % 256) as u8)
            .collect()
    }

    /// Chunk size smaller than the file forces multiple refills. Reading back in
    /// varying-size `read_exact` calls reconstructs the original bytes exactly.
    #[test]
    fn multiple_refills_varying_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.bin");
        let original = data_of(5000, 11);
        std::fs::write(&path, &original).unwrap();

        let mut reader = UringSeqReader::open(&path, 64).unwrap();
        let mut reconstructed = Vec::new();
        // Varying read sizes (some larger than the chunk, some smaller).
        for size in [1usize, 7, 64, 100, 333, 9, 256].iter().cycle() {
            let remaining = original.len() - reconstructed.len();
            if remaining == 0 {
                break;
            }
            let want = (*size).min(remaining);
            let mut dst = vec![0u8; want];
            let ok = reader.read_exact(&mut dst).unwrap();
            assert!(ok, "read_exact must succeed while bytes remain");
            reconstructed.extend_from_slice(&dst);
        }
        assert_eq!(reconstructed, original);
    }

    /// A single `read_exact` spanning many refills returns `Ok(true)` and the
    /// correct bytes.
    #[test]
    fn single_read_spanning_many_refills() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("span.bin");
        let original = data_of(1000, 3);
        std::fs::write(&path, &original).unwrap();

        let mut reader = UringSeqReader::open(&path, 16).unwrap();
        let mut dst = vec![0u8; 1000];
        let ok = reader.read_exact(&mut dst).unwrap();
        assert!(ok);
        assert_eq!(dst, original);
    }

    /// A `read_exact` larger than the remaining file returns `Ok(false)`
    /// (partial at EOF).
    #[test]
    fn partial_at_eof_returns_false() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial.bin");
        std::fs::write(&path, data_of(10, 1)).unwrap();

        let mut reader = UringSeqReader::open(&path, 256).unwrap();
        let mut dst = vec![0u8; 20];
        let ok = reader.read_exact(&mut dst).unwrap();
        assert!(!ok, "partial read at EOF must return Ok(false)");
    }

    /// An empty file yields `Ok(false)` on the first non-empty read.
    #[test]
    fn empty_file_returns_false() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bin");
        std::fs::write(&path, b"").unwrap();

        let mut reader = UringSeqReader::open_default(&path).unwrap();
        let mut dst = [0u8; 1];
        assert!(!reader.read_exact(&mut dst).unwrap());
    }

    /// Reading exactly to EOF succeeds; the next read returns `Ok(false)`.
    #[test]
    fn read_to_eof_then_more_returns_false() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("exact.bin");
        let original = data_of(48, 5);
        std::fs::write(&path, &original).unwrap();

        let mut reader = UringSeqReader::open(&path, 16).unwrap();
        let mut dst = vec![0u8; 48];
        assert!(reader.read_exact(&mut dst).unwrap());
        assert_eq!(dst, original);

        let mut more = [0u8; 1];
        assert!(!reader.read_exact(&mut more).unwrap());
    }

    /// Zero-length destination is trivially `Ok(true)`.
    #[test]
    fn zero_len_read_is_true() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("z.bin");
        std::fs::write(&path, data_of(4, 0)).unwrap();

        let mut reader = UringSeqReader::open_default(&path).unwrap();
        let mut dst: [u8; 0] = [];
        assert!(reader.read_exact(&mut dst).unwrap());
    }
}
