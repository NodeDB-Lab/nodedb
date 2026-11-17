// SPDX-License-Identifier: Apache-2.0

//! Records larger than the write buffer.
//!
//! The write buffer batches records for group commit. It does not cap the
//! record size. A record that cannot share the buffer with its reserves goes
//! to the file alone: the batch ahead of it is flushed first, then the record
//! is written through an aligned buffer of its own size. On disk it is framed
//! as a batch of one record, closed by a padding record under O_DIRECT, so
//! every reader walks it unchanged. The fsync stays with `sync`, so group
//! commit covers it like any buffered record.

use crate::crypto::AUTH_TAG_SIZE;
use crate::error::Result;
use crate::record::{MAX_WAL_PAYLOAD_SIZE, one_shot_record_buffer};

use super::core::WalWriter;
use super::flush::pwrite_all;

/// The largest payload one record takes on every writer: the format cap, less
/// the encryption tag. It does not depend on the write buffer size or on
/// whether encryption is on, so every node agrees on it.
pub const MAX_RECORD_PAYLOAD: usize = MAX_WAL_PAYLOAD_SIZE - AUTH_TAG_SIZE;

impl WalWriter {
    /// The largest payload one record takes. See [`MAX_RECORD_PAYLOAD`].
    pub fn max_payload(&self) -> usize {
        MAX_RECORD_PAYLOAD
    }

    /// Write one framed record to the file by itself, after the buffered
    /// batch.
    ///
    /// On failure `file_offset` is unchanged, so the next write lands at the
    /// same offset and covers any partial bytes.
    pub(super) fn write_alone(&mut self, header: &[u8], payload: &[u8]) -> Result<()> {
        self.flush_buffer()?;

        // Fault injection: a full device, as for a batch flush.
        nodedb_types::fail_point_err!("wal::flush_out_of_space", |_: String| {
            const SITE: &str = "WAL segment append (failpoint)";
            let err = crate::error::WalError::OutOfSpace { context: SITE };
            let pending = (header.len() + payload.len()) as u64;
            crate::diag::out_of_space(&err, SITE, self.file_offset, pending);
            err
        });

        let one_shot = one_shot_record_buffer(
            header,
            payload,
            self.config.alignment,
            self.config.use_direct_io,
        )?;
        pwrite_all(&self.file, one_shot.as_slice(), self.file_offset)?;
        self.note_written(one_shot.len() as u64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::MAX_RECORD_PAYLOAD;
    use crate::align::DEFAULT_ALIGNMENT;
    use crate::crypto::{KeyRing, WalEncryptionKey};
    use crate::error::WalError;
    use crate::lazy_reader::LazyWalReader;
    use crate::mmap_reader::MmapWalReader;
    use crate::reader::{StopReason, WalReader};
    use crate::record::RecordType;
    use crate::writer::{WalWriter, WalWriterConfig};

    const BUFFER: usize = 64 * 1024;

    fn ring() -> KeyRing {
        KeyRing::new(WalEncryptionKey::from_bytes(&[0x5Au8; 32]).unwrap())
    }

    /// A writer with a `buffer`-byte write buffer, or `None` when this
    /// filesystem refuses O_DIRECT.
    fn open(path: &Path, buffer: usize, direct: bool, encrypted: bool) -> Option<WalWriter> {
        let config = WalWriterConfig {
            write_buffer_size: buffer,
            use_direct_io: direct,
            ..Default::default()
        };
        let mut writer = match WalWriter::open(path, config) {
            Ok(writer) => writer,
            Err(WalError::DirectIoUnsupported { .. }) => return None,
            Err(e) => panic!("open: {e}"),
        };
        if encrypted {
            writer.set_encryption_ring(ring()).unwrap();
        }
        Some(writer)
    }

    fn payloads() -> Vec<Vec<u8>> {
        let large: Vec<u8> = (0..3 * BUFFER).map(|i| (i % 251) as u8).collect();
        vec![b"before".to_vec(), large, b"after".to_vec()]
    }

    fn wal_reader_payloads(path: &Path, keys: Option<&KeyRing>) -> Vec<Vec<u8>> {
        let mut reader = WalReader::open(path, keys).unwrap();
        let mut out = Vec::new();
        while let Some(record) = reader.next_record().unwrap() {
            out.push(record.payload);
        }
        assert_eq!(reader.stop_reason(), Some(StopReason::Eof));
        out
    }

    fn mmap_reader_payloads(path: &Path, keys: Option<&KeyRing>) -> Vec<Vec<u8>> {
        let mut reader = MmapWalReader::open(path, keys).unwrap();
        let mut out = Vec::new();
        while let Some(record) = reader.next_record().unwrap() {
            out.push(record.payload);
        }
        assert_eq!(reader.stop_reason(), Some(StopReason::Eof));
        out
    }

    fn lazy_reader_payloads(path: &Path, keys: Option<&KeyRing>) -> Vec<Vec<u8>> {
        let mut reader = LazyWalReader::open(path, keys).unwrap();
        let mut out = Vec::new();
        while let Some(header) = reader.next_header().unwrap() {
            out.push(reader.read_payload(&header).unwrap());
        }
        assert_eq!(reader.stop_reason(), Some(StopReason::Eof));
        out
    }

    /// A record three times the write buffer appends between two small
    /// records, syncs, and replays in LSN order through every reader.
    fn round_trip(direct: bool, encrypted: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversize.wal");
        let Some(mut writer) = open(&path, BUFFER, direct, encrypted) else {
            return;
        };
        let want = payloads();
        for (lsn, payload) in (1u64..).zip(&want) {
            let appended = writer
                .append(RecordType::Put as u32, 1, 0, 0, payload)
                .unwrap();
            assert_eq!(appended, lsn);
        }
        writer.sync().unwrap();
        if direct {
            assert_eq!(writer.file_offset() % DEFAULT_ALIGNMENT as u64, 0);
        }
        drop(writer);

        let keys = encrypted.then(ring);
        assert_eq!(wal_reader_payloads(&path, keys.as_ref()), want);
        assert_eq!(mmap_reader_payloads(&path, keys.as_ref()), want);
        assert_eq!(lazy_reader_payloads(&path, keys.as_ref()), want);

        let info = crate::recovery::recover(&path).unwrap();
        assert_eq!(info.record_count, 3);
        assert_eq!(info.last_lsn, 3);
    }

    #[test]
    fn oversize_record_round_trips_buffered() {
        round_trip(false, false);
    }

    #[test]
    fn oversize_record_round_trips_buffered_encrypted() {
        round_trip(false, true);
    }

    #[test]
    fn oversize_record_round_trips_direct() {
        round_trip(true, false);
    }

    #[test]
    fn oversize_record_round_trips_direct_encrypted() {
        round_trip(true, true);
    }

    /// The first append of an encrypted segment is oversize: the preamble
    /// flushes ahead of it and the segment still opens and replays.
    #[test]
    fn oversize_first_record_follows_the_preamble() {
        for direct in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("first.wal");
            let Some(mut writer) = open(&path, BUFFER, direct, true) else {
                continue;
            };
            let large = payloads().swap_remove(1);
            writer
                .append(RecordType::Put as u32, 1, 0, 0, &large)
                .unwrap();
            writer.sync().unwrap();
            drop(writer);
            let keys = ring();
            assert_eq!(wal_reader_payloads(&path, Some(&keys)), vec![large]);
        }
    }

    /// A reopened segment resumes after an oversize record, and a later
    /// batch replays behind it.
    #[test]
    fn reopen_appends_after_an_oversize_record() {
        for direct in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("reopen.wal");
            let Some(mut writer) = open(&path, BUFFER, direct, false) else {
                continue;
            };
            let mut want = payloads();
            writer
                .append(RecordType::Put as u32, 1, 0, 0, &want[1])
                .unwrap();
            writer.sync().unwrap();
            drop(writer);

            let mut writer = open(&path, BUFFER, direct, false).unwrap();
            assert_eq!(writer.next_lsn(), 2);
            writer
                .append(RecordType::Put as u32, 1, 0, 0, &want[2])
                .unwrap();
            writer.sync().unwrap();
            drop(writer);

            want.remove(0);
            assert_eq!(wal_reader_payloads(&path, None), want);
        }
    }

    /// An oversize append between `begin_sync` and `finish_sync` reaches the
    /// file after the fsync started, so the next sync still fsyncs.
    #[test]
    fn oversize_append_during_a_split_sync_keeps_the_fsync_owed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("split.wal");
        let Some(mut writer) = open(&path, BUFFER, false, false) else {
            return;
        };
        writer
            .append(RecordType::Put as u32, 1, 0, 0, b"small")
            .unwrap();
        let pending = writer.begin_sync().unwrap().expect("an fsync is owed");
        assert_eq!(pending.through_lsn(), 1);
        let large = payloads().swap_remove(1);
        writer
            .append(RecordType::Put as u32, 1, 0, 0, &large)
            .unwrap();
        let fsynced = pending.fsync();
        writer.finish_sync(pending, fsynced, true).unwrap();
        let next = writer.begin_sync().unwrap();
        assert!(next.is_some(), "the oversize record still owes an fsync");
    }

    /// The record cap is the same on every node, whatever its buffer size
    /// and whether it encrypts.
    #[test]
    fn max_payload_does_not_depend_on_the_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let mut caps = Vec::new();
        for (i, buffer) in [64 * 1024, 2 * 1024 * 1024].into_iter().enumerate() {
            for encrypted in [false, true] {
                let path = dir.path().join(format!("cap-{i}-{encrypted}.wal"));
                let writer = open(&path, buffer, false, encrypted).unwrap();
                caps.push(writer.max_payload());
            }
        }
        assert!(
            caps.iter().all(|cap| *cap == MAX_RECORD_PAYLOAD),
            "{caps:?}"
        );
    }
}
