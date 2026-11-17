// SPDX-License-Identifier: BUSL-1.1

//! Writes one sorted run.
//!
//! Records arrive ordered by key bytes ascending, then seq descending. Every
//! [`BLOCK_RECORDS`] records, or [`BLOCK_BYTES`] bytes, closes a data block:
//! the writer appends the block's bloom frame and adds the block's entry to
//! the open index page. A full index page goes to the file between two data
//! blocks, and only its first key and position stay in RAM. Writer memory
//! is one block's key hashes, one index page and the page summaries.

use std::path::PathBuf;

use super::block::{FRAME_HEADER, bloom_body};
use super::block_reader::RunFile;
use super::bloom::{KeyBloom, KeyHash, key_hash};
use super::index_page::{IndexEntry, PageBuilder, PageRef};
use super::record::RunRecordRef;
use super::sorted_run::SortedRun;
use crate::data::executor::handlers::join::SpillPartitionWriter;
use crate::data::executor::handlers::spill::cleanup::{SpillKind, remove_spill_file};
use crate::data::executor::handlers::spill::error::SpillError;

/// Records per data block.
pub(super) const BLOCK_RECORDS: usize = 32;

/// Bytes after which the next record opens a new block.
const BLOCK_BYTES: u64 = 16 * 1024;

/// Writes one sorted run. See the module doc.
pub(in crate::data::executor) struct SortedRunWriter {
    path: PathBuf,
    writer: SpillPartitionWriter,
    /// Key hashes of the open block.
    hashes: Vec<KeyHash>,
    /// First key of the open block.
    block_first_key: Vec<u8>,
    block_start: u64,
    page: PageBuilder,
    pages: Vec<PageRef>,
    min_seq: u64,
    max_seq: u64,
    level: u8,
    last: Option<(Vec<u8>, u64)>,
}

impl SortedRunWriter {
    /// Create the run file at `path` for a run of merge level `level`.
    pub(in crate::data::executor) fn create(path: PathBuf, level: u8) -> crate::Result<Self> {
        let writer = SpillPartitionWriter::create(&path)?;
        Ok(Self {
            path,
            writer,
            hashes: Vec::with_capacity(BLOCK_RECORDS),
            block_first_key: Vec::new(),
            block_start: 0,
            page: PageBuilder::default(),
            pages: Vec::new(),
            min_seq: u64::MAX,
            max_seq: 0,
            level,
            last: None,
        })
    }

    /// Append one record.
    pub(in crate::data::executor) fn push(
        &mut self,
        record: RunRecordRef<'_>,
    ) -> crate::Result<()> {
        if let Some((last_key, last_seq)) = &self.last {
            let ordered = match last_key.as_slice().cmp(record.key) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => *last_seq > record.seq,
                std::cmp::Ordering::Greater => false,
            };
            if !ordered {
                return Err(SpillError::Corrupt {
                    path: self.path.clone(),
                    detail: "run records arrived out of key order".into(),
                }
                .into());
            }
        }
        let open = self.hashes.len();
        if open == BLOCK_RECORDS
            || (open > 0 && self.writer.offset() - self.block_start >= BLOCK_BYTES)
        {
            self.close_block()?;
        }
        if self.hashes.is_empty() {
            self.block_first_key.clear();
            self.block_first_key.extend_from_slice(record.key);
            self.block_start = self.writer.offset();
        }
        let body = record.encode(&self.path)?;
        self.writer.append_row(&body)?;
        self.hashes.push(key_hash(record.key));
        self.min_seq = self.min_seq.min(record.seq);
        self.max_seq = self.max_seq.max(record.seq);
        match &mut self.last {
            Some((last_key, last_seq)) => {
                last_key.clear();
                last_key.extend_from_slice(record.key);
                *last_seq = record.seq;
            }
            None => self.last = Some((record.key.to_vec(), record.seq)),
        }
        Ok(())
    }

    /// Append the open block's bloom and index its block. A block with no
    /// record writes nothing.
    fn close_block(&mut self) -> crate::Result<()> {
        if self.hashes.is_empty() {
            return Ok(());
        }
        let bloom = KeyBloom::from_hashes(&self.hashes);
        self.writer.append_row(&bloom_body(&self.path, &bloom)?)?;
        self.hashes.clear();
        // The block ends here. A page flush below writes past it.
        let len = self.writer.offset() - self.block_start;
        if !self.page.fits(self.block_first_key.len()) {
            self.flush_page()?;
        }
        let entry = IndexEntry {
            first_key: &self.block_first_key,
            offset: self.block_start,
            len,
        };
        self.page.push(&self.path, entry)?;
        Ok(())
    }

    /// Append the open index page and keep its summary. An empty page
    /// writes nothing.
    fn flush_page(&mut self) -> crate::Result<()> {
        if self.page.is_empty() {
            return Ok(());
        }
        let (body, first_key) = self.page.take();
        let offset = self.writer.offset();
        self.writer.append_row(&body)?;
        self.pages.push(PageRef {
            first_key,
            offset,
            len: (FRAME_HEADER + body.len()) as u64,
        });
        Ok(())
    }

    /// Close the file. A run with no record is removed and yields `None`.
    pub(in crate::data::executor) fn finish(mut self) -> crate::Result<Option<SortedRun>> {
        self.close_block()?;
        self.flush_page()?;
        let end = self.writer.offset();
        let path = self.writer.finish()?;
        let Some((last_key, _)) = self.last else {
            remove_spill_file(SpillKind::Txn, &path)?;
            return Ok(None);
        };
        Ok(Some(SortedRun {
            file: RunFile::new(path),
            pages: self.pages.into_boxed_slice(),
            last_key: last_key.into_boxed_slice(),
            end,
            min_seq: self.min_seq,
            max_seq: self.max_seq,
            level: self.level,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn out_of_order_records_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SortedRunWriter::create(dir.path().join("r.spill"), 0).unwrap();
        w.push(RunRecordRef {
            key: &[2],
            seq: 0,
            value: &[],
        })
        .unwrap();
        assert!(
            w.push(RunRecordRef {
                key: &[1],
                seq: 0,
                value: &[],
            })
            .is_err()
        );
        assert!(
            w.push(RunRecordRef {
                key: &[2],
                seq: 0,
                value: &[],
            })
            .is_err()
        );
    }

    #[test]
    fn an_empty_run_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r.spill");
        let w = SortedRunWriter::create(path.clone(), 0).unwrap();
        assert!(w.finish().unwrap().is_none());
        assert!(!path.exists());
    }

    #[test]
    fn a_large_record_closes_its_block_by_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SortedRunWriter::create(dir.path().join("r.spill"), 0).unwrap();
        let value = vec![7u8; BLOCK_BYTES as usize];
        for key in 0..4u8 {
            w.push(RunRecordRef {
                key: &[key],
                seq: 0,
                value: &value,
            })
            .unwrap();
        }
        let run = w.finish().unwrap().unwrap();
        assert_eq!(run.pages.len(), 1);
        let reader = super::super::BlockReader::default();
        for key in 0..4u8 {
            let found = run
                .find(&[key], &reader, |_| true, |_, v| Ok(v.len()))
                .unwrap();
            assert_eq!(found, Some(value.len()));
        }
    }
}
