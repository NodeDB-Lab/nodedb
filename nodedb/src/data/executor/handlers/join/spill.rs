// SPDX-License-Identifier: BUSL-1.1

//! Framed partition spill writer for the grace-hash join.
//!
//! Wraps [`UringWriter`] with a simple length-prefixed framing layer so that
//! many discrete msgpack rows can be stored in a single spill file. When no
//! io_uring ring opens (a platform without it, or a ring or locked-memory
//! limit reached under load) the writer appends through a buffered file
//! instead, so a partition always spills and the join keeps its memory bound.
//!
//! ## Framing format
//!
//! Each row is written as:
//!
//! ```text
//! [ 4 bytes: row length as u32 little-endian ][ row_len bytes: msgpack payload ]
//! ```
//!
//! The frame header is always 4 bytes; zero-length rows are legal (the 4-byte
//! header is written but the body is empty).
//!
//! ## Consumer
//!
//! [`super::grace_spill::PartitionedSpiller`] writes partition spill files via
//! [`SpillPartitionWriter`]; they are read back STREAMING (one row resident at a
//! time) by [`super::grace_repartition::FrameStreamReader`], never as a whole
//! buffer.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::data::io::uring_writer::UringWriter;

/// Where a spill writer's bytes go.
enum SpillBackend {
    // Boxed: the writer carries an io_uring ring and a staging buffer, far
    // larger than the file variant.
    Uring(Box<UringWriter>),
    File {
        out: std::io::BufWriter<std::fs::File>,
        path: PathBuf,
    },
}

/// A spill write error, naming the file.
fn spill_io_error(path: &Path, action: &str, error: std::io::Error) -> crate::Error {
    crate::Error::Storage {
        engine: "spill".into(),
        detail: format!("spill file {}: {action}: {error}", path.display()),
    }
}

// ── Writer ────────────────────────────────────────────────────────────────────

/// Framing layer over [`UringWriter`] that stores many discrete msgpack rows
/// in a single spill file.
///
/// Not `Send` — delegates to [`UringWriter`] which is `!Send` / TPC-owned.
///
/// # Usage
///
/// ```ignore
/// let mut w = SpillPartitionWriter::create(&path)?;
/// w.append_row(row_bytes)?;
/// let path = w.finish()?;
/// ```
pub(in crate::data::executor) struct SpillPartitionWriter {
    backend: SpillBackend,
    /// Bytes handed to the backend so far.
    written: u64,
    /// Framed rows not yet handed to the backend. Batching frames keeps one
    /// backend write per [`PENDING_FLUSH_BYTES`], not two per row.
    pending: Vec<u8>,
}

/// Pending framed bytes that trigger one backend write.
const PENDING_FLUSH_BYTES: usize = 64 * 1024;

impl SpillPartitionWriter {
    /// Create a new partition spill file at `path`, through io_uring when a
    /// ring opens, else through a buffered file. A file that cannot be
    /// created is an error.
    pub(in crate::data::executor) fn create(path: &Path) -> crate::Result<Self> {
        if let Some(writer) = UringWriter::new(path) {
            return Ok(Self {
                backend: SpillBackend::Uring(Box::new(writer)),
                written: 0,
                pending: Vec::new(),
            });
        }
        let file = std::fs::File::create(path).map_err(|e| spill_io_error(path, "create", e))?;
        Ok(Self {
            backend: SpillBackend::File {
                out: std::io::BufWriter::new(file),
                path: path.to_path_buf(),
            },
            written: 0,
            pending: Vec::new(),
        })
    }

    /// Hand every pending framed byte to the backend.
    fn flush_pending(&mut self) -> crate::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        match &mut self.backend {
            SpillBackend::Uring(writer) => writer.append(&self.pending)?,
            SpillBackend::File { out, path } => out
                .write_all(&self.pending)
                .map_err(|e| spill_io_error(path, "write", e))?,
        }
        self.written += self.pending.len() as u64;
        self.pending.clear();
        Ok(())
    }

    /// The file offset the next frame starts at.
    pub(in crate::data::executor) fn offset(&self) -> u64 {
        self.written + self.pending.len() as u64
    }

    /// Write one msgpack row into the spill file with a 4-byte LE length prefix.
    ///
    /// Any error leaves the file with a truncated frame. Spill files are
    /// temporary and never reused after an error.
    ///
    /// Returns a typed [`crate::Error`] if `row.len()` exceeds `u32::MAX` or
    /// if a write fails.
    pub(in crate::data::executor) fn append_row(&mut self, row: &[u8]) -> crate::Result<()> {
        // Guard: length must fit in a u32 frame header.
        if row.len() > u32::MAX as usize {
            return Err(crate::Error::Storage {
                engine: "spill".into(),
                detail: format!(
                    "spill row length {} exceeds u32::MAX ({}); row cannot be framed",
                    row.len(),
                    u32::MAX
                ),
            });
        }

        // 4-byte little-endian length prefix, then the body. A zero-length
        // row is legal.
        self.pending
            .extend_from_slice(&(row.len() as u32).to_le_bytes());
        self.pending.extend_from_slice(row);
        if self.pending.len() >= PENDING_FLUSH_BYTES {
            self.flush_pending()?;
        }
        Ok(())
    }

    /// Flush and close the writer, returning the spill file path.
    pub(in crate::data::executor) fn finish(mut self) -> crate::Result<PathBuf> {
        self.flush_pending()?;
        match self.backend {
            SpillBackend::Uring(writer) => (*writer).finish(),
            SpillBackend::File { out, path } => {
                out.into_inner()
                    .map_err(|e| spill_io_error(&path, "flush", e.into_error()))?;
                Ok(path)
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// This writer's framing is exercised end-to-end by the `grace_spill.rs`
// `io_tests`: those push rows through `PartitionedSpiller` (which writes via
// `SpillPartitionWriter`) and read them back through the streaming
// `FrameStreamReader` in `grace_repartition.rs` — the sole reader of these
// spill files. No standalone reader exists to unit-test in isolation here.
