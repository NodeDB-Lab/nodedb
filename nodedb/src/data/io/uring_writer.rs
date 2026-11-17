// SPDX-License-Identifier: BUSL-1.1

//! io_uring-backed sequential spill-file writer for the Data Plane.
//!
//! This is the write-side primitive for grace-hash-join spill (and later the
//! sort / group-by spill retrofit). Like [`super::uring_reader::UringReader`],
//! it is `!Send` — owned by a single TPC core, owns its own io_uring ring, and
//! drives I/O with the synchronous poll-loop model (`submit_and_wait` + drain
//! completions). It NEVER uses tokio and NEVER does blocking `std::fs` reads or
//! writes of file *contents* — only `File::create` to obtain the fd.
//!
//! ## Buffered, not O_DIRECT
//!
//! Spill files are short-lived temporaries, so the backing file is opened
//! `O_CREAT | O_WRONLY | O_TRUNC` **without** `O_DIRECT`. There are therefore no
//! alignment constraints on data or offset (unlike the WAL), so a plain
//! reusable staging `Vec<u8>` is used rather than an [`super::aligned_buf::AlignedBuf`].
//!
//! ## Design
//!
//! - One `UringWriter` per spill file (created on demand, dropped on `finish`).
//! - `append(data)` issues `IORING_OP_WRITE` at the running file offset, looping
//!   until every byte is durably submitted (short writes are resubmitted).
//! - `finish()` closes the file and returns the spill path. It never syncs:
//!   the core reads the file back through the page cache, and core open
//!   removes every spill directory a crash left.
//!
//! ## Short-write correctness (the critical invariant)
//!
//! An `IORING_OP_WRITE` CQE result may be **less** than the requested length (a
//! partial / short write). `append` MUST advance by exactly the bytes the kernel
//! reports written and resubmit the remainder, never silently accepting a short
//! write — a dropped tail means a truncated spill, which is silent data loss.

use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

/// Queue depth for the writer's io_uring instance.
///
/// `append` only ever has a single write in flight at a time (the short-write
/// loop is sequential), so a shallow ring is sufficient.
#[cfg(target_os = "linux")]
const QUEUE_DEPTH: u32 = 8;

/// Initial staging buffer size (64 KiB). Most spill files are small, as a
/// Calvin slice's resolve file on each of many vShards is. A larger
/// `append` grows the buffer to its size.
#[cfg(target_os = "linux")]
const DEFAULT_BUF_SIZE: usize = 64 * 1024;

/// Per-spill-file io_uring sequential writer.
///
/// Not `Send` — owned by a single Data Plane core.
#[cfg(target_os = "linux")]
pub struct UringWriter {
    ring: io_uring::IoUring,
    /// Backing spill file (buffered — NOT O_DIRECT). Held open for the writer's
    /// lifetime; the io_uring ops write through its fd.
    file: std::fs::File,
    /// Path of the spill file (returned by [`finish`](Self::finish)).
    path: PathBuf,
    /// Reusable staging buffer. `append` copies the caller's slice here so the
    /// pointer handed to the SQE stays stable and owned for the submission's
    /// duration.
    staging: Vec<u8>,
    /// Running write offset (pwrite-style). Advanced by bytes actually written.
    offset: u64,
}

#[cfg(not(target_os = "linux"))]
pub struct UringWriter;

#[cfg(target_os = "linux")]
impl UringWriter {
    /// Create a new spill writer at `path`, truncating any existing file.
    ///
    /// `buf_size` seeds the reusable staging buffer capacity. Returns `None` if
    /// io_uring is unavailable (old kernel, WASM) or the file cannot be created,
    /// mirroring [`UringReader::new`](super::uring_reader::UringReader::new)'s
    /// `Option`-return fallback convention.
    pub fn create(path: &Path, buf_size: usize) -> Option<Self> {
        let ring = io_uring::IoUring::new(QUEUE_DEPTH).ok()?;

        // O_CREAT | O_WRONLY | O_TRUNC, buffered (no O_DIRECT). `File::create`
        // is exactly that. Opening the fd via std is fine — the I/O on the fd's
        // contents goes through io_uring, never blocking std reads/writes.
        let file = std::fs::File::create(path).ok()?;

        Some(Self {
            ring,
            file,
            path: path.to_path_buf(),
            staging: Vec::with_capacity(buf_size.max(1)),
            offset: 0,
        })
    }

    /// Create a writer with the default staging buffer size.
    pub fn new(path: &Path) -> Option<Self> {
        Self::create(path, DEFAULT_BUF_SIZE)
    }

    /// Append `data` to the spill file at the current offset.
    ///
    /// Issues `IORING_OP_WRITE` at `self.offset` and loops on short writes:
    /// each CQE advances `self.offset` and the remaining-bytes cursor by the
    /// count the kernel reports, resubmitting the remainder until all bytes are
    /// written. A negative CQE result is an errno and is surfaced as a typed
    /// [`crate::Error::Io`]; it is never silently swallowed and never panics.
    pub fn append(&mut self, data: &[u8]) -> crate::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        // Copy the chunk into the reusable staging buffer so the pointer handed
        // to the SQE is owned by `self` and stable for the submission.
        self.staging.clear();
        self.staging.extend_from_slice(data);

        // Cursor over the staging buffer; written counts off the front.
        let mut written: usize = 0;
        let total = self.staging.len();

        while written < total {
            let remaining = total - written;
            // The io_uring write length is a `u32`. Cap each SQE at `u32::MAX`
            // so a single large append (>= 4 GiB) is written across multiple
            // SQEs via the short-write loop, rather than truncating the length
            // cast (which would silently drop the high bits).
            let chunk = remaining.min(u32::MAX as usize) as u32;
            // SAFETY: `written < total <= staging.len()`, so this offset is in
            // bounds of the staging allocation.
            let buf_ptr = unsafe { self.staging.as_ptr().add(written) };
            let write_op = io_uring::opcode::Write::new(
                io_uring::types::Fd(self.file.as_raw_fd()),
                buf_ptr,
                chunk,
            )
            .offset(self.offset)
            .build()
            .user_data(0);

            // SAFETY: `buf_ptr` points into `self.staging`, which outlives the
            // SQE (we wait for the CQE before mutating/returning). The fd is
            // valid for `self.file`'s lifetime.
            unsafe {
                self.ring
                    .submission()
                    .push(&write_op)
                    .map_err(|e| crate::Error::Storage {
                        engine: "spill".into(),
                        detail: format!("uring submission queue push failed: {e}"),
                    })?;
            }

            self.ring.submit_and_wait(1).map_err(crate::Error::Io)?;

            let cqe = self
                .ring
                .completion()
                .next()
                .ok_or_else(|| crate::Error::Storage {
                    engine: "spill".into(),
                    detail: "uring write completion missing after submit_and_wait".into(),
                })?;

            let res = cqe.result();
            if res < 0 {
                // CQE result < 0 is a negated errno.
                return Err(crate::Error::Io(std::io::Error::from_raw_os_error(-res)));
            }

            let n = res as usize;
            if n == 0 {
                // A zero-length write with bytes still pending makes no forward
                // progress — treat as a write-zero / no-space condition rather
                // than spinning forever.
                return Err(crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "uring write made no progress (0 bytes) with data remaining",
                )));
            }

            written += n;
            self.offset += n as u64;
        }

        Ok(())
    }

    /// Close the writer and return the spill file path.
    ///
    /// Every append completed before this returns, so a reader of the path
    /// sees every byte through the page cache. The file is not synced: a
    /// spill file is never durable state, and core open removes every spill
    /// directory a crash left.
    pub fn finish(self) -> crate::Result<PathBuf> {
        // Dropping `self` closes the fd.
        Ok(self.path)
    }
}

#[cfg(not(target_os = "linux"))]
impl UringWriter {
    /// io_uring spill writing is Linux-only. Non-Linux builds return `None` so
    /// callers fall back to their non-io_uring path.
    pub fn create(_path: &Path, _buf_size: usize) -> Option<Self> {
        None
    }

    /// io_uring spill writing is Linux-only. Non-Linux builds return `None`.
    pub fn new(_path: &Path) -> Option<Self> {
        None
    }

    /// Stub so generic call sites compile on non-Linux hosts.
    pub fn append(&mut self, _data: &[u8]) -> crate::Result<()> {
        Ok(())
    }

    /// Stub so generic call sites compile on non-Linux hosts.
    pub fn finish(self) -> crate::Result<PathBuf> {
        Ok(PathBuf::new())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::data::io::uring_reader::UringReader;

    fn data_of(size: usize, seed: u8) -> Vec<u8> {
        (0..size)
            .map(|i| ((i + seed as usize) % 256) as u8)
            .collect()
    }

    #[test]
    fn round_trip_varied_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill.tmp");

        // Chunks: small, empty (no-op), large (> staging buf), small again.
        let small = data_of(1024, 1);
        let empty: Vec<u8> = Vec::new();
        let large = data_of(3 * 1024 * 1024, 7); // > 1 MiB default staging
        let tail = data_of(777, 200);

        let mut expected = Vec::new();
        {
            let mut w = UringWriter::create(&path, 64 * 1024).unwrap();
            w.append(&small).unwrap();
            expected.extend_from_slice(&small);
            w.append(&empty).unwrap();
            expected.extend_from_slice(&empty);
            w.append(&large).unwrap();
            expected.extend_from_slice(&large);
            w.append(&tail).unwrap();
            expected.extend_from_slice(&tail);
            let returned = w.finish().unwrap();
            assert_eq!(returned, path);
        }

        // Read back via UringReader (the eventual production read path).
        let mut reader = UringReader::with_config(8, 4, 8 * 1024 * 1024).unwrap();
        let results = reader.read_files(&[path.as_path()]);
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].len(),
            expected.len(),
            "spill file length mismatch"
        );
        assert_eq!(results[0], expected, "spill file contents mismatch");
    }

    #[test]
    fn round_trip_matches_std_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill2.tmp");

        let a = data_of(500, 0);
        let b = data_of(50_000, 3);

        let mut w = UringWriter::create(&path, 4096).unwrap();
        w.append(&a).unwrap();
        w.append(&b).unwrap();
        w.finish().unwrap();

        let mut expected = a.clone();
        expected.extend_from_slice(&b);

        let got = std::fs::read(&path).unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn sequential_appends_ordered() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ordered.tmp");

        let mut expected = Vec::new();
        let mut w = UringWriter::create(&path, 256).unwrap();
        for seed in 0..16u8 {
            let chunk = data_of(300, seed);
            w.append(&chunk).unwrap();
            expected.extend_from_slice(&chunk);
        }
        w.finish().unwrap();

        let got = std::fs::read(&path).unwrap();
        assert_eq!(got, expected, "appends must concatenate in order");
    }

    #[test]
    fn empty_file_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.tmp");

        let w = UringWriter::create(&path, 4096).unwrap();
        let returned = w.finish().unwrap();
        assert_eq!(returned, path);

        let got = std::fs::read(&path).unwrap();
        assert!(got.is_empty());
    }
}
