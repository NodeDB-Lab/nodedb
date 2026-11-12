// SPDX-License-Identifier: BUSL-1.1

//! The spilled part of an overlay's undo journal.
//!
//! Each overlay spill moves the journal entries that grow with the rows it
//! staged into one file of frames. A frame body is `[u64 LE seq]` and the
//! entry's MessagePack. An index of every [`JOURNAL_BLOCK_ENTRIES`]-th
//! frame stays in RAM. A savepoint rollback reads the entries at or after
//! its marker one block at a time, newest first, so it never loads a whole
//! file. A file opens on its first read and keeps the handle until it is
//! removed or the journal drops.

use std::marker::PhantomData;
use std::path::PathBuf;

use crate::data::executor::handlers::join::SpillPartitionWriter;
use crate::data::executor::handlers::spill::error::SpillError;
use crate::data::executor::handlers::spill::run::record::{
    decode_value, encode_value, frame_bodies,
};
use crate::data::executor::handlers::spill::run::{BlockReader, RunFile};

/// Entries per index block.
const JOURNAL_BLOCK_ENTRIES: usize = 256;

#[derive(Debug, Clone, Copy)]
struct JournalBlock {
    first_seq: u64,
    offset: u64,
}

#[derive(Debug)]
struct JournalFile {
    file: RunFile,
    blocks: Vec<JournalBlock>,
    /// End of the last block.
    end: u64,
    /// Entries at or after this seq are not part of the journal.
    end_seq: u64,
}

impl JournalFile {
    fn first_seq(&self) -> u64 {
        self.blocks.first().map_or(self.end_seq, |b| b.first_seq)
    }

    fn block_range(&self, index: usize) -> (u64, usize) {
        let offset = self.blocks.get(index).map_or(self.end, |b| b.offset);
        let next = self.blocks.get(index + 1).map_or(self.end, |b| b.offset);
        (offset, next.saturating_sub(offset) as usize)
    }

    /// The entries of block `index` that are part of the journal, oldest
    /// first.
    fn read_block<E>(&self, index: usize, reader: &BlockReader) -> crate::Result<Vec<(u64, E)>>
    where
        E: for<'de> zerompk::FromMessagePack<'de>,
    {
        let (offset, len) = self.block_range(index);
        let path = self.file.path();
        reader.read_with(&self.file, offset, len, |bytes| {
            let mut entries = Vec::new();
            for body in frame_bodies(path, bytes)? {
                let (seq_bytes, entry_bytes) =
                    body.split_at_checked(8)
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
    /// Heap bytes of the RAM index.
    pub(in crate::data::executor) fn heap_bytes(&self) -> usize {
        self.files
            .iter()
            .map(|f| f.blocks.len() * std::mem::size_of::<JournalBlock>())
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
        let mut writer: Option<SpillPartitionWriter> = None;
        let mut blocks = Vec::new();
        let mut last_seq = 0u64;
        for (count, (seq, entry)) in entries.into_iter().enumerate() {
            let out = match &mut writer {
                Some(out) => out,
                None => writer.insert(SpillPartitionWriter::create(&path)?),
            };
            if count.is_multiple_of(JOURNAL_BLOCK_ENTRIES) {
                blocks.push(JournalBlock {
                    first_seq: seq,
                    offset: out.offset(),
                });
            }
            let mut body = seq.to_le_bytes().to_vec();
            body.extend(encode_value(&path, entry)?);
            out.append_row(&body)?;
            last_seq = seq;
        }
        let Some(out) = writer else {
            return Ok(());
        };
        let end = out.offset();
        let path = out.finish()?;
        self.files.push(JournalFile {
            file: RunFile::new(path),
            blocks,
            end,
            end_seq: last_seq + 1,
        });
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
            for index in (0..file.blocks.len()).rev() {
                let next_first = file
                    .blocks
                    .get(index + 1)
                    .map_or(file.end_seq, |b| b.first_seq);
                if next_first <= bound {
                    return Ok(());
                }
                for (seq, entry) in file.read_block::<E>(index, reader)?.into_iter().rev() {
                    if seq >= bound {
                        visit(seq, entry)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Call `visit` on every spilled entry at or after `bound`, oldest
    /// first.
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
            let start = file
                .blocks
                .partition_point(|b| b.first_seq <= bound)
                .saturating_sub(1);
            for index in start..file.blocks.len() {
                for (seq, entry) in file.read_block::<E>(index, reader)? {
                    if seq >= bound {
                        visit(seq, entry)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Drop every spilled entry at or after `bound`. A file left with no
    /// entry is removed.
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
                let keep = file.blocks.partition_point(|b| b.first_seq < bound);
                if let Some(dropped) = file.blocks.get(keep) {
                    file.end = dropped.offset;
                }
                file.blocks.truncate(keep);
            }
            break;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
