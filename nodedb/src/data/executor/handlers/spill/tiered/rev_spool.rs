// SPDX-License-Identifier: BUSL-1.1

//! An append-only spool read back newest first: entries kept in write
//! order, with a tail in RAM and the rest in one append file.
//!
//! Every spill writes the tail at the file's recorded length as frames of
//! `[entry MessagePack][u32 LE CRC32C][u32 LE length]` (`spool_io`). The
//! trailer follows its body, so a reader walks the file from the recorded
//! length back. A drain hands the tail back newest first, then the file's
//! frames, read backward in chunks of [`READ_CHUNK`] bytes. RAM holds the
//! tail, the file's path and two counters, and while it drains one chunk.
//!
//! A failed spill keeps the recorded length and the tail, so no entry is
//! lost: the next spill writes over what the failed one left.
//!
//! A spool lives under a transaction's spill directory, whose teardown
//! removes a file a drain never reached.

use std::path::PathBuf;

use super::space::SpillSpace;
use super::spool_io::{CRC_BYTES, FrameWriter, LEN_BYTES, check_frame, frame_checksum, frame_len};
use super::weight::SpillWeight;
use crate::data::executor::handlers::spill::error::SpillError;
use crate::data::executor::handlers::spill::run::record::{decode_value, encode_value};
use crate::data::executor::handlers::spill::run::{BlockReader, RunFile};

/// Bytes of a frame's trailer: its checksum, then its length.
const TRAILER: usize = CRC_BYTES + LEN_BYTES;

/// Bytes one backward read takes from the file, a frame's own length
/// aside.
const READ_CHUNK: usize = 64 * 1024;

/// The append file of a spool.
#[derive(Debug)]
struct RevFile {
    path: PathBuf,
    /// Bytes of whole frames the file holds. A failed spill can leave
    /// bytes past it.
    len: u64,
    /// Frames the file holds.
    entries: u64,
}

/// An append-only spool read newest first. See the module doc.
pub(in crate::data::executor) struct RevSpool<E> {
    /// RAM entries, in write order, all after the file's.
    tail: Vec<E>,
    tail_bytes: usize,
    /// `None` until the first spill.
    file: Option<RevFile>,
}

impl<E> Default for RevSpool<E> {
    fn default() -> Self {
        Self {
            tail: Vec::new(),
            tail_bytes: 0,
            file: None,
        }
    }
}

impl<E> RevSpool<E>
where
    E: SpillWeight + zerompk::ToMessagePack + for<'de> zerompk::FromMessagePack<'de>,
{
    /// Append `entry`.
    pub(in crate::data::executor) fn push(&mut self, entry: E) {
        self.tail_bytes += entry.spill_weight();
        self.tail.push(entry);
    }

    /// Entries the spool holds, in the file and in RAM.
    pub(in crate::data::executor) fn len(&self) -> u64 {
        self.spilled_len() + self.tail.len() as u64
    }

    /// Entries the spool's file holds.
    pub(in crate::data::executor) fn spilled_len(&self) -> u64 {
        self.file.as_ref().map_or(0, |file| file.entries)
    }

    /// RAM the tail entries hold.
    pub(in crate::data::executor) fn tail_bytes(&self) -> usize {
        self.tail_bytes
    }

    /// Write every tail entry at the file's recorded length, the file
    /// created in `space` tagged `tag` on the first spill. An empty tail
    /// writes nothing. A failed spill keeps the recorded length and the
    /// tail.
    pub(in crate::data::executor) fn spill(
        &mut self,
        space: &mut SpillSpace,
        tag: &str,
    ) -> crate::Result<()> {
        if self.tail.is_empty() {
            return Ok(());
        }
        let (path, len) = match &self.file {
            Some(file) => (file.path.clone(), file.len),
            None => {
                let path = space.next_path(tag)?;
                self.file = Some(RevFile {
                    path: path.clone(),
                    len: 0,
                    entries: 0,
                });
                (path, 0)
            }
        };
        let written = write_frames(&path, len, &self.tail)?;
        if let Some(file) = self.file.as_mut() {
            file.len += written;
            file.entries += self.tail.len() as u64;
        }
        self.tail.clear();
        self.tail_bytes = 0;
        Ok(())
    }

    /// A cursor that hands the entries back newest first.
    pub(in crate::data::executor) fn into_newest_first(self) -> NewestFirst<E> {
        NewestFirst {
            tail: self.tail,
            tail_bytes: self.tail_bytes,
            back: self.file.map(|file| BackReader {
                run: RunFile::new(file.path.clone()),
                reader: BlockReader::default(),
                pos: file.len,
                buf: Vec::new(),
                file,
            }),
        }
    }
}

/// Write `entries` as frames at offset `at` of `path`. Returns the bytes
/// written.
fn write_frames<E: zerompk::ToMessagePack>(
    path: &std::path::Path,
    at: u64,
    entries: &[E],
) -> crate::Result<u64> {
    let mut writer = FrameWriter::open(path, at)?;
    for entry in entries {
        let body = encode_value(path, entry)?;
        let len = frame_len(path, &body)?;
        writer.put(&body)?;
        writer.put(&frame_checksum(&body).to_le_bytes())?;
        writer.put(&len.to_le_bytes())?;
    }
    writer.finish()
}

/// The backward walk over a spool's file.
struct BackReader {
    file: RevFile,
    run: RunFile,
    reader: BlockReader,
    /// Bytes of the file before the first byte not yet handed back.
    pos: u64,
    /// The bytes just before `pos`, read and not yet handed back.
    buf: Vec<u8>,
}

impl BackReader {
    /// Hold at least `need` bytes before `pos` in `buf`.
    fn fill(&mut self, need: usize) -> crate::Result<()> {
        while self.buf.len() < need {
            let start = self.pos.saturating_sub(self.buf.len() as u64);
            if start == 0 {
                return Err(SpillError::Corrupt {
                    path: self.file.path.clone(),
                    detail: format!(
                        "a frame needs {need} bytes with {} left before it",
                        self.buf.len()
                    ),
                }
                .into());
            }
            let want = READ_CHUNK.max(need - self.buf.len()) as u64;
            let take = want.min(start);
            let at = start - take;
            let mut read = Vec::with_capacity(take as usize + self.buf.len());
            self.reader
                .read_with(&self.run, at, take as usize, |bytes| {
                    read.extend_from_slice(bytes);
                    Ok(())
                })?;
            read.extend_from_slice(&self.buf);
            self.buf = read;
        }
        Ok(())
    }

    /// The trailer field of `CRC_BYTES` or `LEN_BYTES` bytes ending `end`
    /// bytes into `buf`.
    fn word(&self, end: usize) -> u32 {
        let mut word = [0u8; 4];
        word.copy_from_slice(&self.buf[end - 4..end]);
        u32::from_le_bytes(word)
    }

    /// The body of the frame just before `pos`.
    fn previous(&mut self) -> crate::Result<Vec<u8>> {
        self.fill(TRAILER)?;
        let end = self.buf.len();
        let len = self.word(end) as usize;
        let stored = self.word(end - LEN_BYTES);
        self.fill(len + TRAILER)?;
        let end = self.buf.len();
        let body = self.buf[end - TRAILER - len..end - TRAILER].to_vec();
        check_frame(&self.file.path, &body, stored)?;
        self.buf.truncate(end - TRAILER - len);
        self.pos -= (len + TRAILER) as u64;
        self.file.entries = self.file.entries.saturating_sub(1);
        Ok(body)
    }
}

/// The entries of a spool, newest first. The file goes once every entry
/// was handed back.
pub(in crate::data::executor) struct NewestFirst<E> {
    tail: Vec<E>,
    tail_bytes: usize,
    back: Option<BackReader>,
}

impl<E> NewestFirst<E>
where
    E: SpillWeight + for<'de> zerompk::FromMessagePack<'de>,
{
    /// Entries not yet handed back.
    pub(in crate::data::executor) fn remaining(&self) -> u64 {
        self.tail.len() as u64 + self.back.as_ref().map_or(0, |back| back.file.entries)
    }

    /// RAM the cursor holds: the tail entries not yet handed back, and the
    /// file bytes read and not yet handed back.
    pub(in crate::data::executor) fn held_bytes(&self) -> usize {
        self.tail_bytes
            .saturating_add(self.back.as_ref().map_or(0, |back| back.buf.len()))
    }

    /// The newest entry not yet handed back, `None` past the oldest.
    pub(in crate::data::executor) fn next_newest(&mut self) -> crate::Result<Option<E>> {
        if let Some(entry) = self.tail.pop() {
            self.tail_bytes = self.tail_bytes.saturating_sub(entry.spill_weight());
            return Ok(Some(entry));
        }
        let Some(back) = self.back.as_mut() else {
            return Ok(None);
        };
        if back.file.entries == 0 {
            self.finish_file()?;
            return Ok(None);
        }
        let body = back.previous()?;
        let entry = decode_value(&back.file.path, &body)?;
        if back.file.entries == 0 {
            self.finish_file()?;
        }
        Ok(Some(entry))
    }

    /// Remove the file once every frame was handed back. `Corrupt` when
    /// bytes remain before the oldest frame.
    fn finish_file(&mut self) -> crate::Result<()> {
        let Some(back) = self.back.take() else {
            return Ok(());
        };
        let left = back.pos;
        let path = back.file.path.clone();
        // A removal that fails files its own faultbox report, and the
        // teardown of the spill directory removes the file.
        let _removed = back.run.remove();
        if left != 0 {
            return Err(SpillError::Corrupt {
                path,
                detail: format!("{left} bytes remain before the oldest frame"),
            }
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drained(spool: RevSpool<String>) -> Vec<String> {
        let mut cursor = spool.into_newest_first();
        let mut out = Vec::new();
        while let Some(entry) = cursor.next_newest().expect("read back") {
            out.push(entry);
        }
        assert_eq!(cursor.remaining(), 0);
        out
    }

    fn files_in(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir).map_or(0, |entries| entries.count())
    }

    /// The one file `spool` spilled into.
    fn spool_file(spool: &RevSpool<String>) -> PathBuf {
        spool.file.as_ref().expect("spilled").path.clone()
    }

    #[test]
    fn a_drain_returns_every_entry_newest_first_across_spills() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spool_dir = dir.path().join("spool");
        let mut space = SpillSpace::new(spool_dir.clone(), "test", 0);
        let mut spool: RevSpool<String> = RevSpool::default();
        let mut want = Vec::new();
        for round in 0..399u32 {
            for index in 0..30u32 {
                let entry = format!("e{round}-{index}");
                want.push(entry.clone());
                spool.push(entry);
            }
            if round % 3 != 2 {
                spool.spill(&mut space, "undo").expect("spill");
            }
        }
        assert_eq!(files_in(&spool_dir), 1, "every spill appends to one file");
        assert_eq!(spool.len(), want.len() as u64);
        assert!(spool.tail_bytes() > 0, "the last round stays in RAM");
        want.reverse();
        assert_eq!(drained(spool), want);
        assert_eq!(files_in(&spool_dir), 0, "the drain removed the file");
    }

    #[test]
    fn a_frame_larger_than_a_read_chunk_reads_back_whole() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut space = SpillSpace::new(dir.path().join("spool"), "test", 0);
        let mut spool: RevSpool<String> = RevSpool::default();
        let big = "x".repeat(3 * READ_CHUNK + 7);
        spool.push("small".into());
        spool.push(big.clone());
        spool.push("after".into());
        spool.spill(&mut space, "undo").expect("spill");
        assert_eq!(spool.spilled_len(), 3);
        assert_eq!(
            drained(spool),
            ["after".to_string(), big, "small".to_string()]
        );
    }

    #[test]
    fn a_spool_that_never_spilled_drains_its_tail() {
        let mut spool: RevSpool<String> = RevSpool::default();
        spool.push("a".into());
        spool.push("b".into());
        assert_eq!(drained(spool), ["b", "a"]);
    }

    /// Bytes a failed spill left past the recorded length are written over
    /// by the next spill, and never read: every entry drains once.
    #[test]
    fn bytes_past_the_recorded_length_are_written_over() {
        use std::io::Write;
        let dir = tempfile::tempdir().expect("tempdir");
        let mut space = SpillSpace::new(dir.path().join("spool"), "test", 0);
        let mut spool: RevSpool<String> = RevSpool::default();
        spool.push("a".into());
        spool.spill(&mut space, "undo").expect("spill");
        let mut junk = std::fs::OpenOptions::new()
            .append(true)
            .open(spool_file(&spool))
            .expect("open");
        junk.write_all(&[0xAB; 4096]).expect("junk");
        spool.push("b".into());
        spool.spill(&mut space, "undo").expect("spill");
        spool.push("c".into());
        assert_eq!(drained(spool), ["c", "b", "a"]);
    }

    /// A body whose bytes changed on disk fails its checksum.
    #[test]
    fn a_changed_frame_reads_back_as_corrupt() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut space = SpillSpace::new(dir.path().join("spool"), "test", 0);
        let mut spool: RevSpool<String> = RevSpool::default();
        spool.push("payload".into());
        spool.spill(&mut space, "undo").expect("spill");
        let path = spool_file(&spool);
        let mut bytes = std::fs::read(&path).expect("read");
        bytes[1] ^= 0xFF;
        std::fs::write(&path, &bytes).expect("write");
        let mut cursor = spool.into_newest_first();
        assert!(
            cursor.next_newest().is_err(),
            "the checksum catches the change"
        );
    }

    /// A spill that fails once its frames reached the file keeps the
    /// recorded length and the tail. The next spill writes over the frames
    /// it left, and every entry drains once, newest first.
    #[cfg(feature = "failpoints")]
    #[test]
    fn a_failed_spill_loses_no_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut space = SpillSpace::new(dir.path().join("spool"), "test", 0);
        let mut spool: RevSpool<String> = RevSpool::default();
        spool.push("a".into());
        spool.spill(&mut space, "undo").expect("spill");
        spool.push("b".into());
        {
            let _fail =
                nodedb_types::fail_point::FailGuard::fail("spill::spool::after_write", "disk full");
            assert!(spool.spill(&mut space, "undo").is_err());
        }
        assert_eq!(spool.spilled_len(), 1, "the file keeps its recorded frames");
        assert!(spool.tail_bytes() > 0, "the tail stays in RAM");
        spool.spill(&mut space, "undo").expect("spill");
        spool.push("c".into());
        assert_eq!(drained(spool), ["c", "b", "a"]);
    }
}
