// SPDX-License-Identifier: BUSL-1.1

//! Positional reads of spill-file blocks.
//!
//! One [`BlockReader`] serves every lookup of one owner (a transaction's
//! overlay). It opens its io_uring ring on first use and keeps it. When no
//! ring opens it reads through blocking `std::fs`, the fallback every spill
//! reader in the Data Plane shares: with no ring there is no reactor to
//! stall.
//!
//! Each spill file is a [`RunFile`]: its owner opens the file on its first
//! read and keeps the handle until the owner drops. A block read fills the
//! reader's one reused buffer and hands it to the caller. The reader also
//! keeps one reused buffer for the encoded key of a point lookup.

use std::cell::{Cell, OnceCell, RefCell};
use std::fs::File;
use std::io::{Read as _, Seek as _};
use std::path::{Path, PathBuf};

use crate::data::executor::handlers::spill::cleanup::{SpillKind, remove_spill_file};
use crate::data::executor::handlers::spill::error::SpillError;
use crate::data::io::uring_pread::UringPread;

/// The reader's ring state.
#[derive(Default)]
enum Ring {
    /// No read ran yet.
    #[default]
    Untried,
    Uring(Box<UringPread>),
    /// No ring opened. Reads go through `std::fs`.
    Std,
}

/// One spill file and its read handle. The handle opens on the first read
/// and closes when the `RunFile` drops or [`remove`](Self::remove)s it.
#[derive(Debug)]
pub(in crate::data::executor) struct RunFile {
    path: PathBuf,
    handle: OnceCell<File>,
}

impl RunFile {
    pub(in crate::data::executor) fn new(path: PathBuf) -> Self {
        Self {
            path,
            handle: OnceCell::new(),
        }
    }

    pub(in crate::data::executor) fn path(&self) -> &Path {
        &self.path
    }

    /// The open handle, opened on first use.
    fn handle(&self) -> Result<&File, SpillError> {
        if let Some(file) = self.handle.get() {
            return Ok(file);
        }
        let file = File::open(&self.path).map_err(|source| SpillError::Io {
            action: "open",
            path: self.path.clone(),
            source,
        })?;
        Ok(self.handle.get_or_init(|| file))
    }

    /// Close the handle, then remove the file.
    pub(in crate::data::executor) fn remove(self) -> Result<(), SpillError> {
        let Self { path, handle } = self;
        drop(handle);
        remove_spill_file(SpillKind::Txn, &path)
    }
}

/// Positional block reader. Not `Send`: one core owns it.
#[derive(Default)]
pub(in crate::data::executor) struct BlockReader {
    ring: RefCell<Ring>,
    /// The block buffer every read reuses.
    block: RefCell<Vec<u8>>,
    /// The encoded-key buffer every point lookup reuses.
    key: RefCell<Vec<u8>>,
    /// Blocks read so far.
    reads: Cell<u64>,
}

impl std::fmt::Debug for BlockReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockReader").finish_non_exhaustive()
    }
}

impl BlockReader {
    /// Number of blocks this reader read.
    #[cfg(test)]
    pub(in crate::data::executor) fn reads(&self) -> u64 {
        self.reads.get()
    }

    /// Encode `key` as MessagePack into the reused key buffer and hand the
    /// bytes to `visit`. `visit` must not encode another key.
    pub(in crate::data::executor) fn with_encoded_key<Q, T>(
        &self,
        key: &Q,
        visit: impl FnOnce(&[u8]) -> crate::Result<T>,
    ) -> crate::Result<T>
    where
        Q: zerompk::ToMessagePack,
    {
        let mut buf = self
            .key
            .try_borrow_mut()
            .map_err(|_| SpillError::BufferBusy { buffer: "key" })?;
        buf.clear();
        zerompk::write_msgpack(&mut *buf, key).map_err(|e| SpillError::Codec {
            path: PathBuf::from("hot-tier"),
            detail: e.to_string(),
        })?;
        visit(&buf)
    }

    /// Read `len` bytes of `file` at `offset` into the reused block buffer
    /// and hand them to `visit`. `visit` must not read another block. A
    /// file shorter than the range is corruption: the range comes from the
    /// file's own index.
    pub(in crate::data::executor) fn read_with<T>(
        &self,
        file: &RunFile,
        offset: u64,
        len: usize,
        visit: impl FnOnce(&[u8]) -> crate::Result<T>,
    ) -> crate::Result<T> {
        let mut buf = self
            .block
            .try_borrow_mut()
            .map_err(|_| SpillError::BufferBusy { buffer: "block" })?;
        self.fill(file, offset, len, &mut buf)?;
        visit(&buf)
    }

    fn fill(
        &self,
        file: &RunFile,
        offset: u64,
        len: usize,
        buf: &mut Vec<u8>,
    ) -> Result<(), SpillError> {
        let path = file.path();
        let io_error = |action: &'static str, source: std::io::Error| SpillError::Io {
            action,
            path: path.to_path_buf(),
            source,
        };
        self.reads.set(self.reads.get() + 1);
        let handle = file.handle()?;
        buf.clear();
        buf.resize(len, 0);
        let mut ring = self
            .ring
            .try_borrow_mut()
            .map_err(|_| SpillError::BufferBusy { buffer: "ring" })?;
        if matches!(*ring, Ring::Untried) {
            *ring = match UringPread::new() {
                Some(reader) => Ring::Uring(Box::new(reader)),
                None => Ring::Std,
            };
        }
        let full =
            match &mut *ring {
                Ring::Uring(reader) => reader.read_exact_at(handle, offset, buf).map_err(|e| {
                    SpillError::UringRead {
                        path: path.to_path_buf(),
                        detail: format!("block read at {offset}: {e}"),
                    }
                })?,
                Ring::Std | Ring::Untried => {
                    let mut reader = handle;
                    reader
                        .seek(std::io::SeekFrom::Start(offset))
                        .map_err(|e| io_error("seek", e))?;
                    match reader.read_exact(buf) {
                        Ok(()) => true,
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => false,
                        Err(e) => return Err(io_error("read", e)),
                    }
                }
            };
        if !full {
            return Err(SpillError::Corrupt {
                path: path.to_path_buf(),
                detail: format!("block of {len} bytes at {offset} reads past the file end"),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_a_range_through_one_handle_and_refuses_one_past_the_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("b");
        std::fs::write(&path, (0..=255u8).collect::<Vec<_>>()).unwrap();
        let file = RunFile::new(path.clone());
        let reader = BlockReader::default();
        let read = |offset, len| reader.read_with(&file, offset, len, |b| Ok(b.to_vec()));
        assert_eq!(read(10, 3).unwrap(), vec![10, 11, 12]);
        assert_eq!(read(0, 2).unwrap(), vec![0, 1]);
        assert!(read(250, 10).is_err());
        assert_eq!(reader.reads(), 3);

        // The open handle still reads after the path is unlinked; removing
        // the file closes the handle first.
        std::fs::remove_file(&path).unwrap();
        assert_eq!(read(5, 1).unwrap(), vec![5]);
        file.remove().unwrap();
    }

    #[test]
    fn an_encoded_key_matches_its_owned_form() {
        let reader = BlockReader::default();
        let borrowed = reader
            .with_encoded_key(&("a", "b", "c"), |b| Ok(b.to_vec()))
            .unwrap();
        let owned =
            zerompk::to_msgpack_vec(&("a".to_string(), "b".to_string(), "c".to_string())).unwrap();
        assert_eq!(borrowed, owned);
        let coord: &[u8] = &[1, 2, 3];
        let borrowed = reader.with_encoded_key(&coord, |b| Ok(b.to_vec())).unwrap();
        assert_eq!(borrowed, zerompk::to_msgpack_vec(&vec![1u8, 2, 3]).unwrap());
    }
}
