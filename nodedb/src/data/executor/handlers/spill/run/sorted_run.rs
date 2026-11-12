// SPDX-License-Identifier: BUSL-1.1

//! A sorted spill run: one file of
//! [`RunRecord`](super::record::RunRecord) frames, with its key bloom and
//! sparse block index kept in RAM.
//!
//! The writer appends frames through the shared framed spill writer. Every
//! [`BLOCK_RECORDS`] records, or [`BLOCK_BYTES`] bytes, opens a block. The
//! index keeps each block's first key and file offset, so a point lookup
//! reads one block per run the bloom admits, and a scan from a key starts at
//! that key's block. A run opens its file on its first point lookup and
//! keeps the handle until the run drops or is removed.

use std::path::{Path, PathBuf};

use super::block_reader::{BlockReader, RunFile};
use super::bloom::KeyBloom;
use super::record::{RunRecordRef, RunRecordView, frame_bodies};
use crate::data::executor::handlers::join::SpillPartitionWriter;
use crate::data::executor::handlers::spill::cleanup::{SpillKind, remove_spill_file};
use crate::data::executor::handlers::spill::error::SpillError;

/// Records per index block.
const BLOCK_RECORDS: usize = 32;

/// Bytes after which the next record opens a new block.
const BLOCK_BYTES: u64 = 16 * 1024;

/// One index entry: the first key of a block and the block's file offset.
#[derive(Debug, Clone)]
pub(in crate::data::executor) struct BlockEntry {
    pub first_key: Vec<u8>,
    pub offset: u64,
}

/// What one block of a point lookup decided.
enum Step<T> {
    /// The version the lookup wanted, as `take` returned it.
    Found(T),
    /// The block passed the key: no later block holds it.
    Absent,
    /// The block ended before the key: the next block can hold it.
    NextBlock,
}

/// A finished sorted run.
#[derive(Debug)]
pub(in crate::data::executor) struct SortedRun {
    file: RunFile,
    bloom: KeyBloom,
    blocks: Vec<BlockEntry>,
    /// File length: the end of the last block.
    end: u64,
    records: u64,
    min_seq: u64,
    max_seq: u64,
    /// Merge level. A spill writes level 0. Merging runs of one level writes
    /// the next.
    level: u8,
}

impl SortedRun {
    pub(in crate::data::executor) fn path(&self) -> &Path {
        self.file.path()
    }

    pub(in crate::data::executor) fn records(&self) -> u64 {
        self.records
    }

    pub(in crate::data::executor) fn min_seq(&self) -> u64 {
        self.min_seq
    }

    pub(in crate::data::executor) fn max_seq(&self) -> u64 {
        self.max_seq
    }

    pub(in crate::data::executor) fn level(&self) -> u8 {
        self.level
    }

    /// Heap bytes of the bloom and the index.
    pub(in crate::data::executor) fn heap_bytes(&self) -> usize {
        self.bloom.heap_bytes()
            + self
                .blocks
                .iter()
                .map(|b| b.first_key.len() + std::mem::size_of::<BlockEntry>())
                .sum::<usize>()
    }

    /// The index of the block a scan from `key` starts at: the last block
    /// whose first key sorts before `key`. Versions of one key can span a
    /// block boundary, so the block whose first key equals `key` can miss
    /// the key's newest version.
    pub(in crate::data::executor) fn start_block(&self, key: &[u8]) -> usize {
        self.blocks
            .partition_point(|b| b.first_key.as_slice() < key)
            .saturating_sub(1)
    }

    /// File offset of block `index`. The file end past the last block.
    pub(in crate::data::executor) fn block_offset(&self, index: usize) -> u64 {
        self.blocks.get(index).map_or(self.end, |b| b.offset)
    }

    /// Whether the run's bloom admits `key`.
    pub(in crate::data::executor) fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.may_contain(key)
    }

    /// The newest version of `key` whose seq passes `admit`, handed to
    /// `take` as its seq and value bytes. Reads the run's blocks without
    /// probing the bloom: a point lookup checks
    /// [`may_contain`](Self::may_contain) first, so a run the bloom rejects
    /// costs no read.
    pub(in crate::data::executor) fn find<T>(
        &self,
        key: &[u8],
        reader: &BlockReader,
        admit: impl Fn(u64) -> bool,
        take: impl FnOnce(u64, &[u8]) -> crate::Result<T>,
    ) -> crate::Result<Option<T>> {
        let mut take = Some(take);
        let path = self.file.path();
        let mut block = self.start_block(key);
        while block < self.blocks.len() {
            let offset = self.block_offset(block);
            let len = self.block_offset(block + 1).saturating_sub(offset);
            let step = reader.read_with(&self.file, offset, len as usize, |bytes| {
                for body in frame_bodies(path, bytes)? {
                    let record = RunRecordView::decode(path, body)?;
                    match record.key.as_ref().cmp(key) {
                        std::cmp::Ordering::Less => {}
                        std::cmp::Ordering::Equal => {
                            if admit(record.seq)
                                && let Some(take) = take.take()
                            {
                                return Ok(Step::Found(take(record.seq, record.value.as_ref())?));
                            }
                        }
                        std::cmp::Ordering::Greater => return Ok(Step::Absent),
                    }
                }
                Ok(Step::NextBlock)
            })?;
            match step {
                Step::Found(found) => return Ok(Some(found)),
                Step::Absent => return Ok(None),
                Step::NextBlock => block += 1,
            }
        }
        Ok(None)
    }

    /// Close the run's handle, then remove its file.
    pub(in crate::data::executor) fn remove(self) -> crate::Result<()> {
        self.file.remove()?;
        Ok(())
    }
}

/// Writes one sorted run. Records arrive ordered by key bytes ascending,
/// then seq descending.
pub(in crate::data::executor) struct SortedRunWriter {
    path: PathBuf,
    writer: SpillPartitionWriter,
    bloom: KeyBloom,
    blocks: Vec<BlockEntry>,
    block_records: usize,
    block_start: u64,
    records: u64,
    min_seq: u64,
    max_seq: u64,
    level: u8,
    last: Option<(Vec<u8>, u64)>,
}

impl SortedRunWriter {
    /// Create the run file at `path`. `expected_keys` sizes the bloom.
    pub(in crate::data::executor) fn create(
        path: PathBuf,
        expected_keys: usize,
        level: u8,
    ) -> crate::Result<Self> {
        let writer = SpillPartitionWriter::create(&path)?;
        Ok(Self {
            path,
            writer,
            bloom: KeyBloom::with_capacity(expected_keys),
            blocks: Vec::new(),
            block_records: 0,
            block_start: 0,
            records: 0,
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
        let offset = self.writer.offset();
        if self.block_records == BLOCK_RECORDS
            || (self.block_records > 0 && offset - self.block_start >= BLOCK_BYTES)
        {
            self.block_records = 0;
        }
        if self.block_records == 0 {
            self.blocks.push(BlockEntry {
                first_key: record.key.to_vec(),
                offset,
            });
            self.block_start = offset;
        }
        let body = record.encode(&self.path)?;
        self.writer.append_row(&body)?;
        self.bloom.insert(record.key);
        self.block_records += 1;
        self.records += 1;
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

    /// Close the file. A run with no record is removed and yields `None`.
    pub(in crate::data::executor) fn finish(self) -> crate::Result<Option<SortedRun>> {
        let end = self.writer.offset();
        let path = self.writer.finish()?;
        if self.records == 0 {
            remove_spill_file(SpillKind::Txn, &path)?;
            return Ok(None);
        }
        Ok(Some(SortedRun {
            file: RunFile::new(path),
            bloom: self.bloom,
            blocks: self.blocks,
            end,
            records: self.records,
            min_seq: self.min_seq,
            max_seq: self.max_seq,
            level: self.level,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, records: &[(&[u8], u64, &[u8])]) -> SortedRun {
        let mut w = SortedRunWriter::create(dir.join("r.spill"), records.len(), 0).unwrap();
        for (key, seq, value) in records {
            w.push(RunRecordRef {
                key,
                seq: *seq,
                value,
            })
            .unwrap();
        }
        w.finish().unwrap().unwrap()
    }

    #[test]
    fn find_returns_newest_version_across_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let mut owned: Vec<(Vec<u8>, u64, Vec<u8>)> = Vec::new();
        for i in 0..200u32 {
            owned.push((i.to_be_bytes().to_vec(), 1, vec![i as u8]));
        }
        // Many versions of one key cross block boundaries.
        let hot = 100u32.to_be_bytes().to_vec();
        owned.retain(|(k, _, _)| k != &hot);
        for seq in (0..80u64).rev() {
            owned.push((hot.clone(), seq + 10, vec![seq as u8]));
        }
        owned.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        let refs: Vec<(&[u8], u64, &[u8])> = owned
            .iter()
            .map(|(k, s, v)| (k.as_slice(), *s, v.as_slice()))
            .collect();
        let run = write(dir.path(), &refs);
        let reader = BlockReader::default();

        let seq_of = |seq: u64, _: &[u8]| Ok::<u64, crate::Error>(seq);
        let newest = run.find(&hot, &reader, |_| true, seq_of).unwrap();
        assert_eq!(newest, Some(89));
        let older = run.find(&hot, &reader, |seq| seq < 50, seq_of).unwrap();
        assert_eq!(older, Some(49));
        let key = 7u32.to_be_bytes();
        let value = run
            .find(&key, &reader, |_| true, |_, value| Ok(value.to_vec()))
            .unwrap();
        assert_eq!(value, Some(vec![7]));
        assert!(
            run.find(&1_000u32.to_be_bytes(), &reader, |_| true, seq_of)
                .unwrap()
                .is_none()
        );
        // Every lookup read through the one handle the run opened.
        assert!(reader.reads() >= 4);
    }

    #[test]
    fn out_of_order_records_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SortedRunWriter::create(dir.path().join("r.spill"), 2, 0).unwrap();
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
    }

    #[test]
    fn an_empty_run_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("r.spill");
        let w = SortedRunWriter::create(path.clone(), 0, 0).unwrap();
        assert!(w.finish().unwrap().is_none());
        assert!(!path.exists());
    }
}
