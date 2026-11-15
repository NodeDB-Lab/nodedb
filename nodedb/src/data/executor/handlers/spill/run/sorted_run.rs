// SPDX-License-Identifier: BUSL-1.1

//! A finished sorted spill run.
//!
//! The run file holds data blocks, each its record frames and one bloom
//! frame over the block's keys, with index pages between blocks (see
//! [`index_page`](super::index_page)). Only one [`PageRef`] per index page
//! and the run's last key stay in RAM.
//!
//! A point lookup picks the page from the resident summaries, reads that
//! one page, then reads blocks from the last one whose first key sorts
//! before the key. It decodes a block's records only when the block's bloom
//! admits the key. A run opens its file on its first read and keeps the
//! handle until the run drops or is removed.

use std::path::Path;

use super::block::split_block;
use super::block_reader::{BlockReader, RunFile};
use super::index_page::{IndexEntry, PageEntries, PageRef};
use super::record::{Frames, RunRecordView};
use crate::data::executor::handlers::spill::error::SpillError;

/// What one block, or one page of blocks, of a point lookup decided.
enum Step<T> {
    /// The version the lookup wanted, as `take` returned it.
    Found(T),
    /// The lookup passed the key: no later block holds it.
    Absent,
    /// The block or page ended before the key: the next one can hold it.
    Next,
}

/// A finished sorted run. See the module doc.
#[derive(Debug)]
pub(in crate::data::executor) struct SortedRun {
    pub(super) file: RunFile,
    /// One summary per index page, in file order.
    pub(super) pages: Box<[PageRef]>,
    /// The key of the run's last record.
    pub(super) last_key: Box<[u8]>,
    /// File length: the end of the last frame.
    pub(super) end: u64,
    pub(super) min_seq: u64,
    pub(super) max_seq: u64,
    /// Merge level. A spill writes level 0. Merging runs of one level writes
    /// the next.
    pub(super) level: u8,
}

/// `len` as a read length of the run file at `path`.
fn byte_len(path: &Path, len: u64) -> Result<usize, SpillError> {
    usize::try_from(len).map_err(|_| SpillError::Corrupt {
        path: path.to_path_buf(),
        detail: format!("range of {len} bytes exceeds usize"),
    })
}

impl SortedRun {
    pub(in crate::data::executor) fn path(&self) -> &Path {
        self.file.path()
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

    /// Bytes the run keeps in RAM: its page summaries and its last key.
    pub(in crate::data::executor) fn resident_bytes(&self) -> usize {
        self.last_key.len()
            + self
                .pages
                .iter()
                .map(|page| page.first_key.len() + std::mem::size_of::<PageRef>())
                .sum::<usize>()
    }

    /// The index of the page a lookup of `key` starts at: the last page
    /// whose first key sorts before `key`. Versions of one key can span a
    /// page boundary, so the page whose first key equals `key` can miss the
    /// key's newest version.
    fn start_page(&self, key: &[u8]) -> usize {
        self.pages
            .partition_point(|page| page.first_key.as_ref() < key)
            .saturating_sub(1)
    }

    /// Whether every record of the run sorts before `key`.
    fn ends_before(&self, key: &[u8]) -> bool {
        self.last_key.as_ref() < key
    }

    /// Read page `index` and hand its entries to `visit`. `visit` can read
    /// data blocks through `reader`.
    fn with_page<T>(
        &self,
        index: usize,
        reader: &BlockReader,
        visit: impl FnOnce(PageEntries<'_>) -> crate::Result<T>,
    ) -> crate::Result<T> {
        let path = self.file.path();
        let page = self.pages.get(index).ok_or_else(|| SpillError::Corrupt {
            path: path.to_path_buf(),
            detail: format!("index page {index} of {} is absent", self.pages.len()),
        })?;
        let len = byte_len(path, page.len)?;
        reader.read_page_with(&self.file, page.offset, len, |frame| {
            visit(PageEntries::parse(path, frame)?)
        })
    }

    /// File offset of the block a scan from `key` starts at. Reads one
    /// index page. Records before `key` can come first: the caller skips
    /// them. The file end when every record sorts before `key`.
    pub(in crate::data::executor) fn scan_offset(
        &self,
        key: &[u8],
        reader: &BlockReader,
    ) -> crate::Result<u64> {
        if self.pages.is_empty() || self.ends_before(key) {
            return Ok(self.end);
        }
        let path = self.file.path();
        self.with_page(self.start_page(key), reader, |entries| {
            match entries.seek_last_before(key)?.next() {
                Some(entry) => Ok(entry?.offset),
                None => Err(SpillError::Corrupt {
                    path: path.to_path_buf(),
                    detail: "index page lists no block".into(),
                }
                .into()),
            }
        })
    }

    /// The newest version of `key` whose seq passes `admit`, handed to
    /// `take` as its seq and value bytes. A block whose bloom refuses `key`
    /// costs its read, never a record decode.
    pub(in crate::data::executor) fn find<T>(
        &self,
        key: &[u8],
        reader: &BlockReader,
        admit: impl Fn(u64) -> bool,
        take: impl FnOnce(u64, &[u8]) -> crate::Result<T>,
    ) -> crate::Result<Option<T>> {
        if self.pages.is_empty() || self.ends_before(key) {
            return Ok(None);
        }
        let mut take = Some(take);
        let first = self.start_page(key);
        for index in first..self.pages.len() {
            let step = self.with_page(index, reader, |entries| {
                let entries = if index == first {
                    entries.seek_last_before(key)?
                } else {
                    entries
                };
                for entry in entries {
                    match self.find_in_block(entry?, key, reader, &admit, &mut take)? {
                        Step::Next => {}
                        decided => return Ok(decided),
                    }
                }
                Ok(Step::Next)
            })?;
            match step {
                Step::Found(found) => return Ok(Some(found)),
                Step::Absent => return Ok(None),
                Step::Next => {}
            }
        }
        Ok(None)
    }

    /// What the block `entry` names decides for a lookup of `key`.
    fn find_in_block<T, F>(
        &self,
        entry: IndexEntry<'_>,
        key: &[u8],
        reader: &BlockReader,
        admit: &impl Fn(u64) -> bool,
        take: &mut Option<F>,
    ) -> crate::Result<Step<T>>
    where
        F: FnOnce(u64, &[u8]) -> crate::Result<T>,
    {
        // A block that starts past the key holds no version of it, and
        // neither does any later block.
        if entry.first_key > key {
            return Ok(Step::Absent);
        }
        let path = self.file.path();
        let len = byte_len(path, entry.len)?;
        reader.read_with(&self.file, entry.offset, len, |block| {
            let (bloom, records) = split_block(path, block)?;
            if !bloom.may_contain(key) {
                return Ok(Step::Next);
            }
            for body in Frames::new(path, records) {
                let record = RunRecordView::decode(path, body?)?;
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
            Ok(Step::Next)
        })
    }

    /// Close the run's handle, then remove its file.
    pub(in crate::data::executor) fn remove(self) -> crate::Result<()> {
        self.file.remove()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::record::RunRecordRef;
    use super::super::stream::RunStream;
    use super::super::writer::SortedRunWriter;
    use super::*;

    type Owned = Vec<(Vec<u8>, u64, Vec<u8>)>;

    fn write(dir: &Path, records: &Owned) -> SortedRun {
        let mut w = SortedRunWriter::create(dir.join("r.spill"), 0).unwrap();
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

    fn sorted(mut records: Owned) -> Owned {
        records.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        records
    }

    #[test]
    fn find_returns_newest_version_across_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let mut owned: Owned = (0..200u32)
            .map(|i| (i.to_be_bytes().to_vec(), 1, vec![i as u8]))
            .collect();
        // Many versions of one key cross block boundaries.
        let hot = 100u32.to_be_bytes().to_vec();
        owned.retain(|(k, _, _)| k != &hot);
        for seq in (0..80u64).rev() {
            owned.push((hot.clone(), seq + 10, vec![seq as u8]));
        }
        let run = write(dir.path(), &sorted(owned));
        let reader = BlockReader::default();

        let seq_of = |seq: u64, _: &[u8]| Ok::<u64, crate::Error>(seq);
        assert_eq!(run.find(&hot, &reader, |_| true, seq_of).unwrap(), Some(89));
        let older = run.find(&hot, &reader, |seq| seq < 50, seq_of).unwrap();
        assert_eq!(older, Some(49));
        let value = run
            .find(
                &7u32.to_be_bytes(),
                &reader,
                |_| true,
                |_, v| Ok(v.to_vec()),
            )
            .unwrap();
        assert_eq!(value, Some(vec![7]));
        // A key past the last record costs no read.
        let before = reader.reads();
        let past = run
            .find(&1_000u32.to_be_bytes(), &reader, |_| true, seq_of)
            .unwrap();
        assert!(past.is_none());
        assert_eq!(reader.reads(), before);
    }

    /// Keys long enough that the index spans several pages.
    fn paged_records() -> Owned {
        (0..2_000u32)
            .map(|i| {
                let mut key = vec![0u8; 600];
                key[..4].copy_from_slice(&i.to_be_bytes());
                (key, u64::from(i), i.to_le_bytes().to_vec())
            })
            .collect()
    }

    #[test]
    fn find_and_scan_cross_index_pages() {
        let dir = tempfile::tempdir().unwrap();
        let records = paged_records();
        let run = write(dir.path(), &records);
        assert!(run.pages.len() > 1, "pages {}", run.pages.len());
        let reader = BlockReader::default();
        for (key, seq, value) in records.iter().step_by(97) {
            let found = run
                .find(key, &reader, |_| true, |s, v| Ok((s, v.to_vec())))
                .unwrap();
            assert_eq!(found, Some((*seq, value.clone())));
            // A scan from the key reaches it before any later key.
            let mut stream = RunStream::from_key(&run, key, &reader).unwrap();
            let first_at_or_after = std::iter::from_fn(|| stream.next_record().unwrap())
                .find(|record| record.key.as_slice() >= key.as_slice())
                .unwrap();
            assert_eq!(&first_at_or_after.key, key);
        }
        // Between two keys: absent, and a scan starts at the next key.
        let mut between = records[500].0.clone();
        between.push(0);
        assert!(
            run.find(&between, &reader, |_| true, |s, _| Ok(s))
                .unwrap()
                .is_none()
        );
        let mut stream = RunStream::from_key(&run, &between, &reader).unwrap();
        let next = std::iter::from_fn(|| stream.next_record().unwrap())
            .find(|record| record.key.as_slice() >= between.as_slice())
            .unwrap();
        assert_eq!(next.key, records[501].0);
        // Past the last key, a scan starts at the file end.
        let mut last = records.last().unwrap().0.clone();
        last.push(0);
        assert_eq!(run.scan_offset(&last, &reader).unwrap(), run.end);
    }

    #[test]
    fn resident_bytes_count_page_summaries_not_records() {
        let dir = tempfile::tempdir().unwrap();
        let run = write(dir.path(), &paged_records());
        let summaries: usize = run.pages.iter().map(|p| p.first_key.len()).sum();
        assert!(run.resident_bytes() >= summaries + run.last_key.len());
        // Far below the run's 1.2 MB of keys.
        assert!(run.resident_bytes() < 64 * 1024);
    }
}
