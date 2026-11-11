// SPDX-License-Identifier: Apache-2.0

use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use io_uring::IoUring;

use crate::align::AlignedBuf;
use crate::error::{Result, WalError};
use crate::record::{HEADER_SIZE, MIN_PADDING_RECORD_SIZE, WalRecord, WalRecordArgs};
use crate::writer::MAX_RECORD_PAYLOAD;
use crate::writer::durability::DurabilityState;

use super::config::UringWriterConfig;

/// WAL writer using io_uring for O_DIRECT writes.
pub struct UringWriter {
    /// The WAL file handle.
    pub(super) file: File,
    /// Aligned write buffer.
    pub(super) buffer: AlignedBuf,
    /// Current file offset.
    pub(super) file_offset: u64,
    /// Next LSN to assign.
    next_lsn: AtomicU64,
    /// io_uring instance.
    pub(super) ring: IoUring,
    /// Sealed flag.
    sealed: bool,
    /// Standing of the file relative to the last successful fsync. The write
    /// buffer is cleared once the write CQE lands, so it cannot answer "is an
    /// fsync still owed?" on its own.
    pub(super) durability: DurabilityState,
    /// Config.
    pub(super) config: UringWriterConfig,
    /// Optional encryption key for WAL-at-rest encryption.
    encryption_key: Option<crate::crypto::WalEncryptionKey>,
    /// Preamble for this segment (written at offset 0 when encryption is set).
    segment_preamble: Option<crate::preamble::SegmentPreamble>,
}

impl UringWriter {
    /// Open or create a WAL file with io_uring support.
    pub fn open(path: &Path, config: UringWriterConfig) -> Result<Self> {
        let mut opts = OpenOptions::new();
        opts.create(true).write(true).read(true);

        if config.use_direct_io {
            opts.custom_flags(libc::O_DIRECT);
        }

        let file = opts.open(path)?;
        let mut buffer = AlignedBuf::new(config.write_buffer_size, config.alignment)?;

        // Recovery: scan existing WAL for last LSN.
        let (file_offset, next_lsn) = if path.exists() && std::fs::metadata(path)?.len() > 0 {
            let info = crate::recovery::recover(path)?;
            let resume = crate::writer::config::resume_offset(
                info.end_offset,
                config.use_direct_io,
                config.alignment,
                path,
            );
            // The committed bytes of the block the writer resumes in go out
            // again with its first write.
            let carry = crate::writer::config::resume_carry(path, resume, info.end_offset)?;
            buffer.write(&carry);
            (resume, info.next_lsn())
        } else {
            (0, 1)
        };

        let ring = IoUring::new(config.ring_depth).map_err(WalError::Io)?;

        Ok(Self {
            file,
            buffer,
            file_offset,
            next_lsn: AtomicU64::new(next_lsn),
            ring,
            sealed: false,
            durability: DurabilityState::new(),
            config,
            encryption_key: None,
            segment_preamble: None,
        })
    }

    /// Open without O_DIRECT (for testing on tmpfs).
    pub fn open_without_direct_io(path: &Path) -> Result<Self> {
        Self::open(
            path,
            UringWriterConfig {
                use_direct_io: false,
                ..Default::default()
            },
        )
    }

    /// Set the encryption key for WAL-at-rest encryption.
    ///
    /// Writes the 16-byte WAL preamble at the head of the segment.
    /// Must be called before the first `append`.
    pub fn set_encryption_key(&mut self, key: crate::crypto::WalEncryptionKey) -> Result<()> {
        if self.file_offset != 0 || !self.buffer.is_empty() {
            return Err(WalError::EncryptionError {
                detail: "set_encryption_key must be called before writing any records".into(),
            });
        }
        let epoch = *key.epoch();
        let preamble = crate::preamble::SegmentPreamble::new_wal(epoch);
        self.buffer.write(&preamble.to_bytes());
        self.segment_preamble = Some(preamble);
        self.encryption_key = Some(key);
        Ok(())
    }

    /// Append a record to the in-memory buffer. Returns the assigned LSN.
    ///
    /// A record larger than the buffer is written to the file at once, after
    /// the buffered batch. Either way the record is NOT durable until
    /// `submit_and_sync()` is called.
    ///
    /// `database_id` is stored in header bytes 34-41. Pass `0` for the
    /// default database.
    pub fn append(
        &mut self,
        record_type: u32,
        tenant_id: u64,
        vshard_id: u32,
        database_id: u64,
        payload: &[u8],
    ) -> Result<u64> {
        if self.sealed {
            return Err(WalError::Sealed);
        }
        self.durability.check()?;

        let lsn = self.next_lsn.load(Ordering::Relaxed);
        let preamble_bytes = self.segment_preamble.as_ref().map(|p| p.to_bytes());
        let record = WalRecord::new(WalRecordArgs {
            record_type,
            lsn,
            tenant_id,
            vshard_id,
            database_id,
            payload: payload.to_vec(),
            encryption_key: self.encryption_key.as_ref(),
            preamble_bytes: preamble_bytes.as_ref(),
        })?;

        let header_bytes = record.header.to_bytes();
        let total_size = HEADER_SIZE + record.payload.len();
        let reserve = self.padding_reserve();

        if total_size.saturating_add(reserve) > self.buffer.capacity() {
            // No flush makes room for this record, so it goes to the file
            // alone, after the batch ahead of it.
            self.write_alone(&header_bytes, &record.payload)?;
        } else {
            if self.buffer.remaining() < total_size + reserve {
                self.submit_and_wait_write()?;
            }
            self.buffer.write(&header_bytes);
            self.buffer.write(&record.payload);
        }

        // The LSN is committed only once the record is buffered or written,
        // so a failed append leaves no hole in the sequence.
        self.next_lsn.store(lsn + 1, Ordering::Relaxed);

        Ok(lsn)
    }

    /// Bytes reserved at the tail of the write buffer for the alignment
    /// padding record that closes each O_DIRECT batch.
    fn padding_reserve(&self) -> usize {
        if self.config.use_direct_io {
            self.config.alignment + MIN_PADDING_RECORD_SIZE
        } else {
            0
        }
    }

    /// Seal the WAL.
    pub fn seal(&mut self) -> Result<()> {
        self.submit_and_sync()?;
        self.sealed = true;
        Ok(())
    }

    /// Next LSN that will be assigned.
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::Relaxed)
    }

    /// Current file offset.
    pub fn file_offset(&self) -> u64 {
        self.file_offset
    }

    /// The largest payload one record takes, the same as
    /// [`crate::writer::WalWriter::max_payload`].
    pub fn max_payload(&self) -> usize {
        MAX_RECORD_PAYLOAD
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::WalReader;
    use crate::record::RecordType;

    #[test]
    fn uring_write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uring.wal");

        {
            let mut writer = UringWriter::open_without_direct_io(&path).unwrap();
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"hello-uring")
                .unwrap();
            writer
                .append(RecordType::Put as u32, 2, 1, 0, b"world-uring")
                .unwrap();
            writer.submit_and_sync().unwrap();
        }

        let reader = WalReader::open(&path, None).unwrap();
        let records: Vec<_> = reader
            .records()
            .collect::<crate::error::Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"hello-uring");
        assert_eq!(records[1].payload, b"world-uring");
        assert_eq!(records[0].header.lsn, 1);
        assert_eq!(records[1].header.lsn, 2);
    }

    #[test]
    fn uring_group_commit_many_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uring_batch.wal");

        {
            let mut writer = UringWriter::open_without_direct_io(&path).unwrap();
            for i in 0..1000u32 {
                let payload = format!("record-{i}");
                writer
                    .append(RecordType::Put as u32, 1, 0, 0, payload.as_bytes())
                    .unwrap();
            }
            writer.submit_and_sync().unwrap();
        }

        let reader = WalReader::open(&path, None).unwrap();
        let records: Vec<_> = reader
            .records()
            .collect::<crate::error::Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 1000);
        assert_eq!(records[999].header.lsn, 1000);
    }

    #[test]
    fn uring_reopen_continues_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uring_reopen.wal");

        {
            let mut writer = UringWriter::open_without_direct_io(&path).unwrap();
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"first")
                .unwrap();
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"second")
                .unwrap();
            writer.submit_and_sync().unwrap();
        }

        {
            let mut writer = UringWriter::open_without_direct_io(&path).unwrap();
            assert_eq!(writer.next_lsn(), 3);
            let lsn = writer
                .append(RecordType::Put as u32, 1, 0, 0, b"third")
                .unwrap();
            assert_eq!(lsn, 3);
            writer.submit_and_sync().unwrap();
        }

        let reader = WalReader::open(&path, None).unwrap();
        let records: Vec<_> = reader
            .records()
            .collect::<crate::error::Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 3);
    }

    const BUFFER: usize = 64 * 1024;

    /// A writer with a small buffer, or `None` when this filesystem refuses
    /// O_DIRECT.
    fn open_small(path: &Path, direct: bool, encrypted: bool) -> Option<UringWriter> {
        let config = UringWriterConfig {
            write_buffer_size: BUFFER,
            use_direct_io: direct,
            ..Default::default()
        };
        let mut writer = match UringWriter::open(path, config) {
            Ok(writer) => writer,
            Err(_) if direct => return None,
            Err(e) => panic!("open: {e}"),
        };
        if encrypted {
            writer.set_encryption_key(key()).unwrap();
        }
        Some(writer)
    }

    fn key() -> crate::crypto::WalEncryptionKey {
        crate::crypto::WalEncryptionKey::from_bytes(&[0x6Bu8; 32]).unwrap()
    }

    /// A record three times the buffer appends between two small records,
    /// syncs, and replays in LSN order.
    fn uring_round_trip(direct: bool, encrypted: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uring_oversize.wal");
        let Some(mut writer) = open_small(&path, direct, encrypted) else {
            return;
        };
        let large: Vec<u8> = (0..3 * BUFFER).map(|i| (i % 253) as u8).collect();
        let want = vec![b"before".to_vec(), large, b"after".to_vec()];
        for payload in &want {
            writer
                .append(RecordType::Put as u32, 1, 0, 0, payload)
                .unwrap();
        }
        writer.submit_and_sync().unwrap();
        if direct {
            assert_eq!(writer.file_offset() % writer.config.alignment as u64, 0);
        }
        drop(writer);

        let keys = encrypted.then(|| crate::crypto::KeyRing::new(key()));
        let reader = WalReader::open(&path, keys.as_ref()).unwrap();
        let got: Vec<Vec<u8>> = reader
            .records()
            .map(|record| record.map(|record| record.payload))
            .collect::<crate::error::Result<_>>()
            .unwrap();
        assert_eq!(got, want);
    }

    #[test]
    fn uring_oversize_record_round_trips_buffered() {
        uring_round_trip(false, false);
    }

    #[test]
    fn uring_oversize_record_round_trips_buffered_encrypted() {
        uring_round_trip(false, true);
    }

    #[test]
    fn uring_oversize_record_round_trips_direct() {
        uring_round_trip(true, false);
    }

    #[test]
    fn uring_oversize_record_round_trips_direct_encrypted() {
        uring_round_trip(true, true);
    }

    #[test]
    fn uring_max_payload_does_not_depend_on_the_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let small = open_small(&dir.path().join("a.wal"), false, false).unwrap();
        let default = UringWriter::open_without_direct_io(&dir.path().join("b.wal")).unwrap();
        assert_eq!(small.max_payload(), default.max_payload());
        assert_eq!(small.max_payload(), MAX_RECORD_PAYLOAD);
    }
}
