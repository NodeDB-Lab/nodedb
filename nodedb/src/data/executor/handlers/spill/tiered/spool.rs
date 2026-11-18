// SPDX-License-Identifier: BUSL-1.1

//! An append-only spool: entries kept in write order, with a tail in RAM
//! and the rest in one append file.
//!
//! Every spill writes the tail at the file's recorded length as frames of
//! `[u32 LE length][u32 LE CRC32C][entry MessagePack]` (`spool_io`). A
//! drain reads the file front to back in chunks of [`READ_CHUNK`] bytes,
//! hands every entry back oldest first, then the tail, and removes the
//! file. So a spool holds at most one file, and at most one open handle,
//! while it spills or drains. RAM holds the tail, the file's path and two
//! counters.
//!
//! A failed spill keeps the recorded length and the tail, so no entry is
//! lost: the next spill writes over what the failed one left.
//!
//! A spool lives under a transaction's spill directory, whose teardown
//! removes a file a drain never reached.

use std::path::{Path, PathBuf};

use super::space::SpillSpace;
use super::spool_io::{CRC_BYTES, FrameWriter, LEN_BYTES, check_frame, frame_checksum, frame_len};
use super::weight::SpillWeight;
use crate::data::executor::handlers::spill::error::SpillError;
use crate::data::executor::handlers::spill::run::record::{decode_value, encode_value};
use crate::data::executor::handlers::spill::run::{BlockReader, RunFile};

/// Bytes of a frame's header: its length, then its checksum.
const HEADER: usize = LEN_BYTES + CRC_BYTES;

/// Bytes one forward read takes from the file, a frame's own length aside.
const READ_CHUNK: usize = 64 * 1024;

/// The append file of a spool.
#[derive(Debug)]
struct SpoolFile {
    path: PathBuf,
    /// Bytes of whole frames the file holds. A failed spill can leave
    /// bytes past it.
    len: u64,
    /// Frames the file holds.
    entries: u64,
}

/// An append-only spool. See the module doc.
pub(in crate::data::executor) struct Spool<E> {
    /// RAM entries, in write order, all after the file's.
    tail: Vec<E>,
    tail_bytes: usize,
    /// `None` until the first spill.
    file: Option<SpoolFile>,
}

impl<E> Default for Spool<E> {
    fn default() -> Self {
        Self {
            tail: Vec::new(),
            tail_bytes: 0,
            file: None,
        }
    }
}

impl<E> std::fmt::Debug for Spool<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spool")
            .field("tail", &self.tail.len())
            .field("file", &self.file)
            .finish()
    }
}

impl<E> Spool<E> {
    /// Append `entry`, which holds `weight` bytes of RAM.
    pub(in crate::data::executor) fn push_weighed(&mut self, entry: E, weight: usize) {
        self.tail_bytes += weight;
        self.tail.push(entry);
    }

    /// RAM the tail entries hold.
    pub(in crate::data::executor) fn tail_bytes(&self) -> usize {
        self.tail_bytes
    }

    /// Entries the spool's file holds.
    #[cfg(test)]
    pub(in crate::data::executor) fn spilled_len(&self) -> u64 {
        self.file.as_ref().map_or(0, |file| file.entries)
    }

    /// Write every tail entry, as `encode` writes it, at the recorded
    /// length of the spool's file, created in `space` tagged `tag` on the
    /// first spill. An empty tail writes nothing. A failed spill keeps the
    /// recorded length and the tail.
    pub(in crate::data::executor) fn spill_with(
        &mut self,
        space: &mut SpillSpace,
        tag: &str,
        encode: impl Fn(&Path, &E) -> crate::Result<Vec<u8>>,
    ) -> crate::Result<()> {
        if self.tail.is_empty() {
            return Ok(());
        }
        let (path, len) = match &self.file {
            Some(file) => (file.path.clone(), file.len),
            None => {
                let path = space.next_path(tag)?;
                self.file = Some(SpoolFile {
                    path: path.clone(),
                    len: 0,
                    entries: 0,
                });
                (path, 0)
            }
        };
        let written = self.write_tail(&path, len, &encode)?;
        if let Some(file) = self.file.as_mut() {
            file.len += written;
            file.entries += self.tail.len() as u64;
        }
        self.tail.clear();
        self.tail_bytes = 0;
        Ok(())
    }

    /// Write the tail's frames at offset `at` of `path`. Returns the bytes
    /// written.
    fn write_tail(
        &self,
        path: &Path,
        at: u64,
        encode: &impl Fn(&Path, &E) -> crate::Result<Vec<u8>>,
    ) -> crate::Result<u64> {
        let mut writer = FrameWriter::open(path, at)?;
        for entry in &self.tail {
            let body = encode(path, entry)?;
            let len = frame_len(path, &body)?;
            writer.put(&len.to_le_bytes())?;
            writer.put(&frame_checksum(&body).to_le_bytes())?;
            writer.put(&body)?;
        }
        writer.finish()
    }

    /// Hand every entry to `visit`, oldest first: the file's, each as
    /// `decode` reads it back, then the tail's. The first error stops the
    /// drain and is returned. The file is removed once every entry it holds
    /// was visited.
    pub(in crate::data::executor) fn drain_with(
        self,
        decode: impl Fn(&Path, &[u8]) -> crate::Result<E>,
        mut visit: impl FnMut(E) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let Self { tail, file, .. } = self;
        if let Some(file) = &file {
            let run = RunFile::new(file.path.clone());
            read_frames(file, &run, |bytes| visit(decode(&file.path, bytes)?))?;
            // A removal that fails files its own faultbox report, and the
            // teardown of the spill directory removes the file.
            let _removed = run.remove();
        }
        for entry in tail {
            visit(entry)?;
        }
        Ok(())
    }
}

impl<E> Spool<E>
where
    E: SpillWeight + zerompk::ToMessagePack + for<'de> zerompk::FromMessagePack<'de>,
{
    /// Append `entry`.
    pub(in crate::data::executor) fn push(&mut self, entry: E) {
        let weight = entry.spill_weight();
        self.push_weighed(entry, weight);
    }

    /// [`Self::spill_with`], each entry written as its MessagePack.
    pub(in crate::data::executor) fn spill(
        &mut self,
        space: &mut SpillSpace,
        tag: &str,
    ) -> crate::Result<()> {
        self.spill_with(space, tag, |path, entry| Ok(encode_value(path, entry)?))
    }

    /// [`Self::drain_with`], each entry read back from its MessagePack.
    pub(in crate::data::executor) fn drain(
        self,
        visit: impl FnMut(E) -> crate::Result<()>,
    ) -> crate::Result<()> {
        self.drain_with(|path, bytes| Ok(decode_value(path, bytes)?), visit)
    }
}

/// The forward walk over a spool's file, up to its recorded length.
struct FrontReader<'a> {
    file: &'a SpoolFile,
    run: &'a RunFile,
    reader: BlockReader,
    /// The file offset the reads reached.
    read: u64,
    /// Bytes read and not yet handed back, from `at` on.
    buf: Vec<u8>,
    at: usize,
}

impl FrontReader<'_> {
    /// Hold at least `need` unread bytes in `buf`. `Corrupt` when the
    /// recorded length ends first.
    fn fill(&mut self, need: usize) -> crate::Result<()> {
        if self.buf.len() - self.at >= need {
            return Ok(());
        }
        self.buf.drain(..self.at);
        self.at = 0;
        let missing = (need - self.buf.len()) as u64;
        let left = self.file.len - self.read;
        if missing > left {
            return Err(SpillError::Corrupt {
                path: self.file.path.clone(),
                detail: format!("a frame needs {missing} bytes with {left} left in the file"),
            }
            .into());
        }
        let take = (READ_CHUNK as u64).max(missing).min(left);
        let buf = &mut self.buf;
        self.reader
            .read_with(self.run, self.read, take as usize, |bytes| {
                buf.extend_from_slice(bytes);
                Ok(())
            })?;
        self.read += take;
        Ok(())
    }

    /// The `u32` at `offset` bytes past the unread start of `buf`.
    fn word(&self, offset: usize) -> u32 {
        let start = self.at + offset;
        let mut word = [0u8; 4];
        word.copy_from_slice(&self.buf[start..start + 4]);
        u32::from_le_bytes(word)
    }
}

/// Hand the bytes of every frame of `file` to `visit`, oldest first.
/// `Corrupt` on a frame whose checksum does not match, and on bytes left
/// past the last frame within the recorded length.
fn read_frames(
    file: &SpoolFile,
    run: &RunFile,
    mut visit: impl FnMut(&[u8]) -> crate::Result<()>,
) -> crate::Result<()> {
    let mut front = FrontReader {
        file,
        run,
        reader: BlockReader::default(),
        read: 0,
        buf: Vec::new(),
        at: 0,
    };
    for _ in 0..file.entries {
        front.fill(HEADER)?;
        let len = front.word(0) as usize;
        let stored = front.word(LEN_BYTES);
        front.fill(HEADER + len)?;
        let body = &front.buf[front.at + HEADER..front.at + HEADER + len];
        check_frame(&file.path, body, stored)?;
        visit(body)?;
        front.at += HEADER + len;
    }
    let left = (front.buf.len() - front.at) as u64 + (file.len - front.read);
    if left != 0 {
        return Err(SpillError::Corrupt {
            path: file.path.clone(),
            detail: format!("{left} bytes remain past the last frame"),
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drained(spool: Spool<String>) -> Vec<String> {
        let mut out = Vec::new();
        spool
            .drain(|entry| {
                out.push(entry);
                Ok(())
            })
            .expect("drain");
        out
    }

    /// Open file descriptors of this process.
    fn open_handles() -> usize {
        std::fs::read_dir("/proc/self/fd").map_or(0, |entries| entries.count())
    }

    fn files_in(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir).map_or(0, |entries| entries.count())
    }

    #[test]
    fn a_drain_returns_every_entry_in_write_order_across_spills() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut space = SpillSpace::new(dir.path().join("spool"), "test", 0);
        let mut spool: Spool<String> = Spool::default();
        let mut want = Vec::new();
        for round in 0..5u32 {
            for index in 0..300u32 {
                let entry = format!("e{round}-{index}");
                want.push(entry.clone());
                spool.push(entry);
            }
            if round % 2 == 0 {
                spool.spill(&mut space, "spool").expect("spill");
                assert_eq!(spool.tail_bytes(), 0);
            }
        }
        // Rounds 0, 2 and 4 spill everything pushed so far: 1500 entries,
        // each once, and an empty tail.
        assert_eq!(spool.spilled_len(), 1500);
        assert_eq!(spool.tail_bytes(), 0);
        assert_eq!(drained(spool), want);
    }

    #[test]
    fn many_spills_keep_one_file_and_one_handle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let spool_dir = dir.path().join("spool");
        let mut space = SpillSpace::new(spool_dir.clone(), "test", 0);
        let mut spool: Spool<String> = Spool::default();
        let baseline = open_handles();
        let mut written = 0u64;
        for spill in 0..2_000u64 {
            for index in 0..3u64 {
                spool.push(format!("s{spill}-{index}"));
                written += 1;
            }
            spool.spill(&mut space, "spool").expect("spill");
            assert!(
                open_handles() <= baseline,
                "no handle stays open after a spill"
            );
        }
        assert_eq!(files_in(&spool_dir), 1, "every spill appends to one file");
        assert_eq!(spool.spilled_len(), written);

        let mut peak = 0;
        let mut count = 0u64;
        spool
            .drain(|_| {
                peak = peak.max(open_handles().saturating_sub(baseline));
                count += 1;
                Ok(())
            })
            .expect("drain");
        assert_eq!(count, written);
        assert!(peak <= 2, "a drain holds {peak} handles past the baseline");
        assert!(open_handles() <= baseline, "the drain closed its handle");
        assert_eq!(files_in(&spool_dir), 0, "the drain removed the file");
    }

    #[test]
    fn a_spool_that_never_spilled_drains_its_tail() {
        let mut spool: Spool<String> = Spool::default();
        spool.push("a".into());
        spool.push("b".into());
        assert!(spool.tail_bytes() > 0);
        assert_eq!(drained(spool), ["a", "b"]);
    }

    /// The one file `spool` spilled into.
    fn spool_file(spool: &Spool<String>) -> PathBuf {
        spool.file.as_ref().expect("spilled").path.clone()
    }

    /// Bytes a failed spill left past the recorded length are written over
    /// by the next spill, and never read: every entry drains once.
    #[test]
    fn bytes_past_the_recorded_length_are_written_over() {
        use std::io::Write;
        let dir = tempfile::tempdir().expect("tempdir");
        let mut space = SpillSpace::new(dir.path().join("spool"), "test", 0);
        let mut spool: Spool<String> = Spool::default();
        spool.push("a".into());
        spool.spill(&mut space, "spool").expect("spill");
        let mut junk = std::fs::OpenOptions::new()
            .append(true)
            .open(spool_file(&spool))
            .expect("open");
        junk.write_all(&[0xAB; 4096]).expect("junk");
        spool.push("b".into());
        spool.spill(&mut space, "spool").expect("spill");
        spool.push("c".into());
        assert_eq!(drained(spool), ["a", "b", "c"]);
    }

    /// A body whose bytes changed on disk fails its checksum.
    #[test]
    fn a_changed_frame_reads_back_as_corrupt() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut space = SpillSpace::new(dir.path().join("spool"), "test", 0);
        let mut spool: Spool<String> = Spool::default();
        spool.push("payload".into());
        spool.spill(&mut space, "spool").expect("spill");
        let path = spool_file(&spool);
        let mut bytes = std::fs::read(&path).expect("read");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, &bytes).expect("write");
        assert!(
            spool.drain(|_| Ok(())).is_err(),
            "the checksum catches the change"
        );
    }

    /// A spill that fails once its frames reached the file keeps the
    /// recorded length and the tail, and loses no entry.
    #[cfg(feature = "failpoints")]
    #[test]
    fn a_failed_spill_loses_no_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut space = SpillSpace::new(dir.path().join("spool"), "test", 0);
        let mut spool: Spool<String> = Spool::default();
        spool.push("a".into());
        spool.spill(&mut space, "spool").expect("spill");
        spool.push("b".into());
        {
            let _fail =
                nodedb_types::fail_point::FailGuard::fail("spill::spool::after_write", "disk full");
            assert!(spool.spill(&mut space, "spool").is_err());
        }
        assert_eq!(spool.spilled_len(), 1, "the file keeps its recorded frames");
        spool.spill(&mut space, "spool").expect("spill");
        spool.push("c".into());
        assert_eq!(drained(spool), ["a", "b", "c"]);
    }
}
