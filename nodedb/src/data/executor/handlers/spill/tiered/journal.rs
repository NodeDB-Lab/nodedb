// SPDX-License-Identifier: BUSL-1.1

//! The spilled part of an overlay's undo journal.
//!
//! Each overlay spill moves the journal entries that grow with the rows it
//! staged into one file of frames. An entry frame body is `[u64 LE seq]`
//! and the entry's MessagePack. Every [`JOURNAL_BLOCK_ENTRIES`] entries
//! form a block.
//!
//! The block index lives on disk as index pages, interleaved with the
//! blocks (see [`index_page`]). An entry's key is its block's first seq as
//! big-endian bytes. Only one [`JournalPage`] per page stays in RAM. Reads
//! go by explicit block and page ranges, never by streaming the file, so
//! entry frames and page frames never mix.
//!
//! A replay reads one page, then its blocks one at a time, forward or
//! newest first. It never loads a whole file or a whole index. A file
//! opens on its first read and keeps the handle until it is removed or the
//! journal drops.
//!
//! [`index_page`]: crate::data::executor::handlers::spill::run::index_page

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use crate::data::executor::handlers::join::SpillPartitionWriter;
use crate::data::executor::handlers::spill::error::SpillError;
use crate::data::executor::handlers::spill::run::block::FRAME_HEADER;
use crate::data::executor::handlers::spill::run::index_page::{
    IndexEntry, PageBuilder, PageEntries,
};
use crate::data::executor::handlers::spill::run::record::{Frames, decode_value, encode_value};
use crate::data::executor::handlers::spill::run::{BlockReader, RunFile};

/// Entries per block.
const JOURNAL_BLOCK_ENTRIES: usize = 256;

/// Bytes of a block key: its first seq, big-endian.
const SEQ_KEY: usize = 8;

/// The resident summary of one index page.
#[derive(Debug, Clone, Copy)]
struct JournalPage {
    /// First seq of the page's first block.
    first_seq: u64,
    /// Offset of the page frame, header included.
    offset: u64,
    /// Length of the page frame, header included.
    len: u64,
}

/// One block, as its index page lists it.
#[derive(Debug, Clone, Copy)]
struct JournalBlock {
    first_seq: u64,
    offset: u64,
    len: u64,
}

#[derive(Debug)]
struct JournalFile {
    file: RunFile,
    pages: Vec<JournalPage>,
    /// Entries at or after this seq are not part of the journal.
    end_seq: u64,
}

fn byte_len(path: &Path, len: u64) -> Result<usize, SpillError> {
    usize::try_from(len).map_err(|_| SpillError::Corrupt {
        path: path.to_path_buf(),
        detail: format!("range of {len} bytes exceeds usize"),
    })
}

impl JournalFile {
    fn first_seq(&self) -> u64 {
        self.pages.first().map_or(self.end_seq, |p| p.first_seq)
    }

    /// The first seq of page `index + 1`, or the end seq past the last
    /// page.
    fn next_page_seq(&self, index: usize) -> u64 {
        self.pages
            .get(index + 1)
            .map_or(self.end_seq, |p| p.first_seq)
            .min(self.end_seq)
    }

    /// The blocks page `index` lists that start before the end seq, in
    /// file order.
    fn read_page(&self, index: usize, reader: &BlockReader) -> crate::Result<Vec<JournalBlock>> {
        let Some(page) = self.pages.get(index) else {
            return Ok(Vec::new());
        };
        let path = self.file.path();
        let len = byte_len(path, page.len)?;
        reader.read_page_with(&self.file, page.offset, len, |frame| {
            let mut blocks = Vec::new();
            for entry in PageEntries::parse(path, frame)? {
                let entry = entry?;
                let key = <[u8; SEQ_KEY]>::try_from(entry.first_key).map_err(|_| {
                    SpillError::Corrupt {
                        path: path.to_path_buf(),
                        detail: format!("journal index key of {} bytes", entry.first_key.len()),
                    }
                })?;
                let first_seq = u64::from_be_bytes(key);
                if first_seq >= self.end_seq {
                    break;
                }
                blocks.push(JournalBlock {
                    first_seq,
                    offset: entry.offset,
                    len: entry.len,
                });
            }
            Ok(blocks)
        })
    }

    /// The entries of `block` that are part of the journal, oldest first.
    fn read_block<E>(
        &self,
        block: JournalBlock,
        reader: &BlockReader,
    ) -> crate::Result<Vec<(u64, E)>>
    where
        E: for<'de> zerompk::FromMessagePack<'de>,
    {
        let path = self.file.path();
        let len = byte_len(path, block.len)?;
        reader.read_with(&self.file, block.offset, len, |bytes| {
            let mut entries = Vec::new();
            for body in Frames::new(path, bytes) {
                let (seq_bytes, entry_bytes) =
                    body?
                        .split_at_checked(8)
                        .ok_or_else(|| SpillError::Corrupt {
                            path: path.to_path_buf(),
                            detail: "journal frame shorter than its seq".into(),
                        })?;
                let mut seq = [0u8; 8];
                seq.copy_from_slice(seq_bytes);
                let seq = u64::from_le_bytes(seq);
                if seq >= self.end_seq {
                    break;
                }
                entries.push((seq, decode_value(path, entry_bytes)?));
            }
            Ok(entries)
        })
    }
}

/// Writes one journal file: entry frames, with an index page written each
/// time one fills.
struct JournalWriter {
    path: PathBuf,
    out: SpillPartitionWriter,
    pages: Vec<JournalPage>,
    page: PageBuilder,
    page_first_seq: u64,
    /// First seq and offset of the open block, and its entry count.
    block: Option<(u64, u64, usize)>,
}

impl JournalWriter {
    fn create(path: PathBuf) -> crate::Result<Self> {
        let out = SpillPartitionWriter::create(&path)?;
        Ok(Self {
            path,
            out,
            pages: Vec::new(),
            page: PageBuilder::default(),
            page_first_seq: 0,
            block: None,
        })
    }

    fn push(&mut self, seq: u64, body: &[u8]) -> crate::Result<()> {
        if self
            .block
            .is_some_and(|(_, _, count)| count == JOURNAL_BLOCK_ENTRIES)
        {
            self.close_block()?;
        }
        let offset = self.out.offset();
        let (_, _, count) = self.block.get_or_insert((seq, offset, 0));
        *count += 1;
        self.out.append_row(body)
    }

    /// Index the open block. No open block indexes nothing.
    fn close_block(&mut self) -> crate::Result<()> {
        let Some((first_seq, offset, _)) = self.block.take() else {
            return Ok(());
        };
        let len = self.out.offset() - offset;
        if !self.page.fits(SEQ_KEY) {
            self.flush_page()?;
        }
        if self.page.is_empty() {
            self.page_first_seq = first_seq;
        }
        let key = first_seq.to_be_bytes();
        let entry = IndexEntry {
            first_key: &key,
            offset,
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
        let (body, _) = self.page.take();
        let offset = self.out.offset();
        self.out.append_row(&body)?;
        self.pages.push(JournalPage {
            first_seq: self.page_first_seq,
            offset,
            len: (FRAME_HEADER + body.len()) as u64,
        });
        Ok(())
    }

    fn finish(mut self, end_seq: u64) -> crate::Result<JournalFile> {
        self.close_block()?;
        self.flush_page()?;
        let path = self.out.finish()?;
        self.pages.shrink_to_fit();
        Ok(JournalFile {
            file: RunFile::new(path),
            pages: self.pages,
            end_seq,
        })
    }
}

/// Spilled journal entries of type `E`, oldest file first.
pub(in crate::data::executor) struct SpilledJournal<E> {
    files: Vec<JournalFile>,
    _entry: PhantomData<fn() -> E>,
}

impl<E> Default for SpilledJournal<E> {
    fn default() -> Self {
        Self {
            files: Vec::new(),
            _entry: PhantomData,
        }
    }
}

impl<E> std::fmt::Debug for SpilledJournal<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpilledJournal")
            .field("files", &self.files.len())
            .finish()
    }
}

impl<E> SpilledJournal<E>
where
    E: zerompk::ToMessagePack + for<'de> zerompk::FromMessagePack<'de>,
{
    /// Bytes the journal keeps in RAM: each file's handle state and path,
    /// and one summary per index page. No file keeps nothing.
    pub(in crate::data::executor) fn heap_bytes(&self) -> usize {
        self.files
            .iter()
            .map(|f| {
                std::mem::size_of::<JournalFile>()
                    + f.file.path().as_os_str().len()
                    + f.pages.capacity() * std::mem::size_of::<JournalPage>()
            })
            .sum()
    }

    /// Write `entries` (ascending seq, every seq above the spilled ones) as
    /// one file at `path`. No entry writes no file.
    pub(in crate::data::executor) fn spill<'e>(
        &mut self,
        path: PathBuf,
        entries: impl IntoIterator<Item = (u64, &'e E)>,
    ) -> crate::Result<()>
    where
        E: 'e,
    {
        let mut writer: Option<JournalWriter> = None;
        let mut last_seq = 0u64;
        let mut body = Vec::new();
        for (seq, entry) in entries {
            let out = match &mut writer {
                Some(out) => out,
                None => writer.insert(JournalWriter::create(path.clone())?),
            };
            body.clear();
            body.extend_from_slice(&seq.to_le_bytes());
            body.extend(encode_value(&path, entry)?);
            out.push(seq, &body)?;
            last_seq = seq;
        }
        if let Some(writer) = writer {
            self.files.push(writer.finish(last_seq + 1)?);
        }
        Ok(())
    }

    /// Call `visit` on every spilled entry at or after `bound`, newest
    /// first.
    pub(in crate::data::executor) fn for_each_rev(
        &self,
        bound: u64,
        reader: &BlockReader,
        mut visit: impl FnMut(u64, E) -> crate::Result<()>,
    ) -> crate::Result<()> {
        for file in self.files.iter().rev() {
            if file.end_seq <= bound {
                return Ok(());
            }
            for page in (0..file.pages.len()).rev() {
                let blocks = file.read_page(page, reader)?;
                for index in (0..blocks.len()).rev() {
                    let Some(block) = blocks.get(index).copied() else {
                        continue;
                    };
                    let next_first = blocks
                        .get(index + 1)
                        .map_or(file.next_page_seq(page), |b| b.first_seq);
                    if next_first <= bound {
                        return Ok(());
                    }
                    for (seq, entry) in file.read_block::<E>(block, reader)?.into_iter().rev() {
                        if seq >= bound {
                            visit(seq, entry)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Call `visit` on every spilled entry at or after `bound`, oldest
    /// first. Each file starts at the page and block that hold `bound`.
    pub(in crate::data::executor) fn for_each_from(
        &self,
        bound: u64,
        reader: &BlockReader,
        mut visit: impl FnMut(u64, E) -> crate::Result<()>,
    ) -> crate::Result<()> {
        for file in &self.files {
            if file.end_seq <= bound {
                continue;
            }
            let start_page = file
                .pages
                .partition_point(|p| p.first_seq <= bound)
                .saturating_sub(1);
            for page in start_page..file.pages.len() {
                let blocks = file.read_page(page, reader)?;
                let start = if page == start_page {
                    blocks
                        .partition_point(|b| b.first_seq <= bound)
                        .saturating_sub(1)
                } else {
                    0
                };
                for block in blocks.get(start..).unwrap_or_default() {
                    for (seq, entry) in file.read_block::<E>(*block, reader)? {
                        if seq >= bound {
                            visit(seq, entry)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Drop every spilled entry at or after `bound`. A file left with no
    /// entry is removed. A file cut inside keeps its bytes past `bound` on
    /// disk: reads stop at its end seq.
    pub(in crate::data::executor) fn truncate(&mut self, bound: u64) -> crate::Result<()> {
        while let Some(file) = self.files.last_mut() {
            if file.first_seq() >= bound {
                if let Some(file) = self.files.pop() {
                    file.file.remove()?;
                }
                continue;
            }
            if file.end_seq > bound {
                file.end_seq = bound;
                let keep = file.pages.partition_point(|p| p.first_seq < bound);
                file.pages.truncate(keep);
                file.pages.shrink_to_fit();
            }
            break;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::handlers::spill::run::index_page::PAGE_BYTES;

    /// Index entry bytes beside the key.
    const ENTRY_FIXED: usize = 20;

    fn rev(journal: &SpilledJournal<u64>, bound: u64, reader: &BlockReader) -> Vec<(u64, u64)> {
        let mut out = Vec::new();
        journal
            .for_each_rev(bound, reader, |seq, entry| {
                out.push((seq, entry));
                Ok(())
            })
            .unwrap();
        out
    }

    fn fwd(journal: &SpilledJournal<u64>, bound: u64, reader: &BlockReader) -> Vec<(u64, u64)> {
        let mut out = Vec::new();
        journal
            .for_each_from(bound, reader, |seq, entry| {
                out.push((seq, entry));
                Ok(())
            })
            .unwrap();
        out
    }

    fn check(
        journal: &SpilledJournal<u64>,
        model: &[(u64, u64)],
        bound: u64,
        reader: &BlockReader,
    ) {
        let want: Vec<(u64, u64)> = model.iter().copied().filter(|(s, _)| *s >= bound).collect();
        assert_eq!(fwd(journal, bound, reader), want, "forward from {bound}");
        let want_rev: Vec<(u64, u64)> = want.into_iter().rev().collect();
        assert_eq!(
            rev(journal, bound, reader),
            want_rev,
            "reverse from {bound}"
        );
    }

    fn spill(journal: &mut SpilledJournal<u64>, path: PathBuf, entries: &[(u64, u64)]) {
        journal
            .spill(path, entries.iter().map(|(s, e)| (*s, e)))
            .unwrap();
    }

    #[test]
    fn reverse_and_forward_reads_honour_the_bound_and_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let reader = BlockReader::default();
        let mut journal: SpilledJournal<String> = SpilledJournal::default();
        let first: Vec<(u64, String)> = (0..600u64).map(|s| (s * 2, format!("e{s}"))).collect();
        journal
            .spill(dir.path().join("j0"), first.iter().map(|(s, e)| (*s, e)))
            .unwrap();
        let second: Vec<(u64, String)> = (600..700u64).map(|s| (s * 2, format!("e{s}"))).collect();
        journal
            .spill(dir.path().join("j1"), second.iter().map(|(s, e)| (*s, e)))
            .unwrap();

        let mut rev = Vec::new();
        journal
            .for_each_rev(1_000, &reader, |seq, _| {
                rev.push(seq);
                Ok(())
            })
            .unwrap();
        assert_eq!(rev.first(), Some(&1_398));
        assert_eq!(rev.last(), Some(&1_000));
        assert!(rev.windows(2).all(|w| w[0] > w[1]));
        assert_eq!(rev.len(), 200);

        let mut fwd = Vec::new();
        journal
            .for_each_from(999, &reader, |seq, _| {
                fwd.push(seq);
                Ok(())
            })
            .unwrap();
        assert_eq!(fwd.first(), Some(&1_000));
        assert_eq!(fwd.len(), 200);

        journal.truncate(1_001).unwrap();
        assert!(!dir.path().join("j1").exists());
        let mut left = Vec::new();
        journal
            .for_each_rev(0, &reader, |seq, _| {
                left.push(seq);
                Ok(())
            })
            .unwrap();
        assert_eq!(left.first(), Some(&1_000));
        assert_eq!(left.len(), 501);
    }

    #[test]
    fn forward_and_reverse_replay_match_a_model_across_many_pages() {
        let dir = tempfile::tempdir().unwrap();
        let reader = BlockReader::default();
        let mut journal: SpilledJournal<u64> = SpilledJournal::default();
        let first: Vec<(u64, u64)> = (0..400_000u64).map(|s| (s * 2, s ^ 0x5555)).collect();
        let second: Vec<(u64, u64)> = (400_000..420_000u64).map(|s| (s * 2, s ^ 0x5555)).collect();
        spill(&mut journal, dir.path().join("j0"), &first);
        spill(&mut journal, dir.path().join("j1"), &second);
        let pages = &journal.files[0].pages;
        assert!(pages.len() >= 3, "pages {}", pages.len());
        let page_seq = pages[1].first_seq;
        let block_seq = pages[2].first_seq + 2 * 256 * 7;
        let mut model: Vec<(u64, u64)> = first.iter().chain(&second).copied().collect();

        for bound in [
            page_seq - 1,
            page_seq,
            page_seq + 1,
            block_seq - 1,
            block_seq,
            799_999,
            800_000,
            839_998,
            839_999,
        ] {
            check(&journal, &model, bound, &reader);
        }

        // Cut inside a page, then append a file as a rollback and a later
        // spill do.
        let cut = block_seq + 10;
        journal.truncate(cut).unwrap();
        assert!(!dir.path().join("j1").exists());
        model.retain(|(s, _)| *s < cut);
        let more: Vec<(u64, u64)> = (cut..cut + 1_000).map(|s| (s, s + 1)).collect();
        spill(&mut journal, dir.path().join("j2"), &more);
        model.extend_from_slice(&more);
        for bound in [0, page_seq, block_seq, cut - 1, cut, cut + 500, cut + 1_000] {
            check(&journal, &model, bound, &reader);
        }
        journal.truncate(0).unwrap();
        assert!(journal.files.is_empty());
        assert_eq!(journal.heap_bytes(), 0);
    }

    #[test]
    fn resident_bytes_stay_within_one_summary_per_index_page() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal: SpilledJournal<u64> = SpilledJournal::default();
        assert_eq!(journal.heap_bytes(), 0);
        let entries: Vec<(u64, u64)> = (0..300_000u64).map(|s| (s, s)).collect();
        spill(&mut journal, dir.path().join("j0"), &entries);

        let blocks = entries.len().div_ceil(JOURNAL_BLOCK_ENTRIES);
        let entries_per_page = (PAGE_BYTES - 1) / (ENTRY_FIXED + SEQ_KEY);
        let pages_bound = blocks.div_ceil(entries_per_page);
        let file = &journal.files[0];
        assert!(
            file.pages.len() <= pages_bound,
            "pages {} > {pages_bound}",
            file.pages.len()
        );
        let bound = std::mem::size_of::<JournalFile>()
            + file.file.path().as_os_str().len()
            + pages_bound * std::mem::size_of::<JournalPage>();
        assert!(journal.heap_bytes() > 0);
        assert!(
            journal.heap_bytes() <= bound,
            "{} > {bound}",
            journal.heap_bytes()
        );
        // One resident summary per block costs far more.
        assert!(journal.heap_bytes() < blocks * std::mem::size_of::<JournalBlock>() / 10);
    }
}
