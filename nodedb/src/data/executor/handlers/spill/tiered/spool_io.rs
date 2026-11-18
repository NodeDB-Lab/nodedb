// SPDX-License-Identifier: BUSL-1.1

//! The writes and the frame checksums the append spools share.
//!
//! A spill writes its frames at the file's recorded length: through
//! io_uring when a ring opens, else through positional writes of the file,
//! as every Data Plane spill writer falls back. A spill that fails keeps
//! the recorded length, so the bytes it left past it are never read: the
//! next spill writes over them, and a reader stops at the recorded length.
//! Reads go through a [`BlockReader`], the positional reader every spill
//! file of the Data Plane shares.
//!
//! Every frame carries the CRC32C of its body. A body that does not match,
//! or bytes left over once every frame was read, is corruption.
//!
//! [`BlockReader`]: crate::data::executor::handlers::spill::run::BlockReader

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::data::executor::handlers::spill::error::SpillError;
use crate::data::io::uring_writer::UringWriter;

/// Bytes of a frame's checksum.
pub(super) const CRC_BYTES: usize = 4;

/// Bytes of a frame's length.
pub(super) const LEN_BYTES: usize = 4;

/// Pending bytes that trigger one write.
const WRITE_CHUNK: usize = 64 * 1024;

/// The checksum a frame carries for `body`.
pub(super) fn frame_checksum(body: &[u8]) -> u32 {
    crc32c::crc32c(body)
}

/// `Corrupt` unless `stored` is the checksum of `body`.
pub(super) fn check_frame(path: &Path, body: &[u8], stored: u32) -> Result<(), SpillError> {
    let actual = frame_checksum(body);
    if actual == stored {
        return Ok(());
    }
    Err(SpillError::Corrupt {
        path: path.to_path_buf(),
        detail: format!(
            "a frame of {} bytes has checksum {actual:#010x}, and its header names \
             {stored:#010x}",
            body.len()
        ),
    })
}

/// The length a frame's header or trailer names for its body.
pub(super) fn frame_len(path: &Path, body: &[u8]) -> Result<u32, SpillError> {
    u32::try_from(body.len()).map_err(|_| SpillError::Codec {
        path: path.to_path_buf(),
        detail: format!("spool entry of {} bytes exceeds u32", body.len()),
    })
}

/// Where a [`FrameWriter`]'s bytes go.
enum Sink {
    // Boxed: the writer carries an io_uring ring and a staging buffer.
    Uring(Box<UringWriter>),
    File(std::fs::File),
}

/// Writes one spill's frames into a spool file from its recorded length on.
pub(super) struct FrameWriter {
    path: PathBuf,
    sink: Sink,
    /// The file offset the pending bytes go to.
    offset: u64,
    /// The recorded length the spill started at.
    start: u64,
    pending: Vec<u8>,
}

impl FrameWriter {
    /// A writer of `path` from offset `at` on. Creates the file when absent.
    pub(super) fn open(path: &Path, at: u64) -> crate::Result<Self> {
        let sink = match UringWriter::open_at(path, at) {
            Some(writer) => Sink::Uring(Box::new(writer)),
            None => Sink::File(
                std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(path)
                    .map_err(|source| SpillError::Io {
                        action: "open",
                        path: path.to_path_buf(),
                        source,
                    })?,
            ),
        };
        Ok(Self {
            path: path.to_path_buf(),
            sink,
            offset: at,
            start: at,
            pending: Vec::new(),
        })
    }

    /// Queue `bytes`. Writes once a chunk is pending.
    pub(super) fn put(&mut self, bytes: &[u8]) -> crate::Result<()> {
        self.pending.extend_from_slice(bytes);
        if self.pending.len() >= WRITE_CHUNK {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> crate::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        match &mut self.sink {
            Sink::Uring(writer) => {
                writer
                    .append(&self.pending)
                    .map_err(|e| crate::Error::Storage {
                        engine: "spill".into(),
                        detail: format!("spill file {}: write: {e}", self.path.display()),
                    })?
            }
            Sink::File(file) => {
                file.write_all_at(&self.pending, self.offset)
                    .map_err(|source| SpillError::Io {
                        action: "write",
                        path: self.path.clone(),
                        source,
                    })?
            }
        }
        self.offset += self.pending.len() as u64;
        self.pending.clear();
        Ok(())
    }

    /// Write what is pending. Returns the bytes the spill wrote. On `Err`
    /// the caller keeps its recorded length.
    pub(super) fn finish(mut self) -> crate::Result<u64> {
        self.flush()?;
        crate::fail_point_err!("spill::spool::after_write", |detail: String| {
            crate::Error::Storage {
                engine: "spill".into(),
                detail,
            }
        });
        Ok(self.offset - self.start)
    }
}
