// SPDX-License-Identifier: BUSL-1.1

//! The two-level block index of a run.
//!
//! The lower level lives on disk as index page frames, interleaved with the
//! data blocks: the writer emits a page once it fills. A page body is
//! `[PAGE_TAG]` then entries `[u32 LE key len][first key][u64 LE block
//! offset][u64 LE block len]`, in block order. A page holds at most
//! [`PAGE_BYTES`] bytes of body, or one entry when that entry alone is
//! larger.
//!
//! The upper level stays in RAM: one [`PageRef`] per page, holding the
//! page's first key and its frame's offset and length.

use std::path::Path;

use super::block::{FRAME_HEADER, PAGE_TAG};
use crate::data::executor::handlers::spill::error::SpillError;

/// Body bytes of an index page.
pub(in crate::data::executor) const PAGE_BYTES: usize = 16 * 1024;

/// Bytes of an entry beside its key.
const ENTRY_FIXED: usize = 4 + 8 + 8;

/// The resident summary of one index page.
#[derive(Debug)]
pub(super) struct PageRef {
    pub first_key: Box<[u8]>,
    /// Offset of the page frame, header included.
    pub offset: u64,
    /// Length of the page frame, header included.
    pub len: u64,
}

/// One index entry: a data block's first key, offset and length.
#[derive(Debug, Clone, Copy)]
pub(in crate::data::executor) struct IndexEntry<'a> {
    pub first_key: &'a [u8],
    pub offset: u64,
    pub len: u64,
}

/// An index page under construction.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct PageBuilder {
    body: Vec<u8>,
    first_key: Vec<u8>,
}

impl PageBuilder {
    pub(in crate::data::executor) fn is_empty(&self) -> bool {
        self.body.is_empty()
    }

    /// Whether an entry for a key of `key_len` bytes fits the page. An
    /// empty page takes any entry.
    pub(in crate::data::executor) fn fits(&self, key_len: usize) -> bool {
        self.is_empty() || self.body.len() + ENTRY_FIXED + key_len <= PAGE_BYTES
    }

    /// Append the entry of one block.
    pub(in crate::data::executor) fn push(
        &mut self,
        path: &Path,
        entry: IndexEntry<'_>,
    ) -> Result<(), SpillError> {
        let key_len = u32::try_from(entry.first_key.len()).map_err(|_| SpillError::Corrupt {
            path: path.to_path_buf(),
            detail: format!("index key of {} bytes exceeds u32", entry.first_key.len()),
        })?;
        if self.body.is_empty() {
            self.body.push(PAGE_TAG);
            self.first_key.clear();
            self.first_key.extend_from_slice(entry.first_key);
        }
        self.body.extend_from_slice(&key_len.to_le_bytes());
        self.body.extend_from_slice(entry.first_key);
        self.body.extend_from_slice(&entry.offset.to_le_bytes());
        self.body.extend_from_slice(&entry.len.to_le_bytes());
        Ok(())
    }

    /// The page body and its first key. Leaves the builder empty.
    pub(in crate::data::executor) fn take(&mut self) -> (Vec<u8>, Box<[u8]>) {
        let first_key = std::mem::take(&mut self.first_key).into_boxed_slice();
        (std::mem::take(&mut self.body), first_key)
    }
}

/// The entries of one index page, read in place.
#[derive(Clone)]
pub(in crate::data::executor) struct PageEntries<'a> {
    path: &'a Path,
    body: &'a [u8],
    at: usize,
}

impl<'a> PageEntries<'a> {
    /// The entries of the page frame `frame`, header included.
    pub(in crate::data::executor) fn parse(
        path: &'a Path,
        frame: &'a [u8],
    ) -> Result<Self, SpillError> {
        let corrupt = || SpillError::Corrupt {
            path: path.to_path_buf(),
            detail: format!("index page frame of {} bytes is malformed", frame.len()),
        };
        let header = frame.get(..FRAME_HEADER).ok_or_else(corrupt)?;
        let mut len = [0u8; FRAME_HEADER];
        len.copy_from_slice(header);
        let body = frame.get(FRAME_HEADER..).ok_or_else(corrupt)?;
        if u32::from_le_bytes(len) as usize != body.len() || body.first() != Some(&PAGE_TAG) {
            return Err(corrupt());
        }
        Ok(Self { path, body, at: 1 })
    }

    /// The entries from the last one whose first key sorts before `key`.
    /// From the first entry when none does.
    pub(super) fn seek_last_before(self, key: &[u8]) -> Result<Self, SpillError> {
        let mut start = self.at;
        let mut probe = self.clone();
        loop {
            let at = probe.at;
            match probe.next().transpose()? {
                Some(entry) if entry.first_key < key => start = at,
                _ => break,
            }
        }
        Ok(Self { at: start, ..self })
    }

    fn read_entry(&mut self) -> Result<IndexEntry<'a>, SpillError> {
        let at = self.at;
        let corrupt = || SpillError::Corrupt {
            path: self.path.to_path_buf(),
            detail: format!("index entry cut short at page byte {at}"),
        };
        let key_len = u32::from_le_bytes(take_array(self.body, at).ok_or_else(corrupt)?) as usize;
        let key_at = at + 4;
        let first_key = self
            .body
            .get(key_at..key_at + key_len)
            .ok_or_else(corrupt)?;
        let offset =
            u64::from_le_bytes(take_array(self.body, key_at + key_len).ok_or_else(corrupt)?);
        let len =
            u64::from_le_bytes(take_array(self.body, key_at + key_len + 8).ok_or_else(corrupt)?);
        self.at = key_at + key_len + 16;
        Ok(IndexEntry {
            first_key,
            offset,
            len,
        })
    }
}

impl<'a> Iterator for PageEntries<'a> {
    type Item = Result<IndexEntry<'a>, SpillError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.at >= self.body.len() {
            return None;
        }
        let entry = self.read_entry();
        if entry.is_err() {
            self.at = self.body.len();
        }
        Some(entry)
    }
}

/// The `N` bytes of `bytes` at `at`.
fn take_array<const N: usize>(bytes: &[u8], at: usize) -> Option<[u8; N]> {
    let mut out = [0u8; N];
    out.copy_from_slice(bytes.get(at..at + N)?);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(builder: &mut PageBuilder) -> Vec<u8> {
        let (body, _) = builder.take();
        let mut frame = (body.len() as u32).to_le_bytes().to_vec();
        frame.extend_from_slice(&body);
        frame
    }

    #[test]
    fn entries_round_trip_and_seek_to_the_last_before_a_key() {
        let path = Path::new("p");
        let mut page = PageBuilder::default();
        for (i, key) in [b"b", b"d", b"f"].iter().enumerate() {
            let entry = IndexEntry {
                first_key: *key,
                offset: i as u64 * 10,
                len: 10,
            };
            page.push(path, entry).unwrap();
        }
        let frame = frame(&mut page);
        assert!(page.is_empty());
        let entries = PageEntries::parse(path, &frame).unwrap();
        let all: Vec<u64> = entries.clone().map(|e| e.unwrap().offset).collect();
        assert_eq!(all, vec![0, 10, 20]);
        let first = |key: &[u8]| {
            let mut from = entries.clone().seek_last_before(key).unwrap();
            from.next().unwrap().unwrap().offset
        };
        assert_eq!(first(b"a"), 0);
        assert_eq!(first(b"b"), 0);
        assert_eq!(first(b"c"), 0);
        assert_eq!(first(b"d"), 0);
        assert_eq!(first(b"e"), 10);
        assert_eq!(first(b"z"), 20);
        assert!(PageEntries::parse(path, &frame[..frame.len() - 1]).is_err());
        let mut cut = PageEntries::parse(path, &frame).unwrap();
        cut.body = &cut.body[..cut.body.len() - 1];
        assert_eq!(cut.filter(Result::is_err).count(), 1);
    }

    #[test]
    fn a_page_fills_to_its_byte_cap_and_takes_one_oversized_entry() {
        let path = Path::new("p");
        let mut page = PageBuilder::default();
        let big = vec![7u8; PAGE_BYTES * 2];
        assert!(page.fits(big.len()));
        page.push(
            path,
            IndexEntry {
                first_key: &big,
                offset: 0,
                len: 1,
            },
        )
        .unwrap();
        assert!(!page.fits(1));
    }
}
