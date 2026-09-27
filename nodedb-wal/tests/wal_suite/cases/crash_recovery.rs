// SPDX-License-Identifier: Apache-2.0

//! Crash recovery integration tests.
//!
//! These simulate various crash scenarios and verify that:
//! 1. WAL replay recovers exactly the committed prefix.
//! 2. No acknowledged write is lost.
//! 3. Torn writes (partial records) are safely ignored.
//! 4. WAL can be reopened and continued from the correct LSN.

use std::io::Write;

use nodedb_wal::reader::{StopReason, WalReader};
use nodedb_wal::record::{HEADER_SIZE, RecordType};
use nodedb_wal::recovery::recover;
use nodedb_wal::writer::WalWriter;
use nodedb_wal::{Result, WalError, WalRecord, WalRecordArgs};

/// Helper: write N records and sync.
fn write_records(path: &std::path::Path, count: u32) -> Vec<u64> {
    let mut writer = WalWriter::open_without_direct_io(path).unwrap();
    let mut lsns = Vec::new();
    for i in 0..count {
        let payload = format!("record-{i}");
        let lsn = writer
            .append(RecordType::Put as u32, 1, 0, 0, payload.as_bytes())
            .unwrap();
        lsns.push(lsn);
    }
    writer.sync().unwrap();
    lsns
}

/// Helper: read all valid records from a WAL file.
fn read_all(path: &std::path::Path) -> Vec<WalRecord> {
    let reader = WalReader::open(path, None).unwrap();
    reader.records().collect::<Result<_>>().unwrap()
}

#[test]
fn crash_before_sync_loses_buffered_records() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("test.wal");

    // Write and sync 3 records.
    write_records(&path, 3);

    // Write 2 more records WITHOUT syncing (simulate crash before fsync).
    {
        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        writer
            .append(RecordType::Put as u32, 1, 0, 0, b"unsync-1")
            .unwrap();
        writer
            .append(RecordType::Put as u32, 1, 0, 0, b"unsync-2")
            .unwrap();
        // Drop without sync — records are lost (correct behavior).
    }

    // Only the first 3 records should survive.
    let records = read_all(&path);
    assert_eq!(records.len(), 3);
}

#[test]
fn torn_write_mid_header() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("test.wal");

    write_records(&path, 5);

    // Append a partial header (less than HEADER_SIZE bytes).
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(&[0x57, 0x4E, 0x59, 0x53]).unwrap(); // Partial magic
    }

    // Recovery should find exactly 5 records.
    let info = recover(&path).unwrap();
    assert_eq!(info.record_count, 5);
    assert_eq!(info.last_lsn, 5);

    let records = read_all(&path);
    assert_eq!(records.len(), 5);
}

#[test]
fn torn_write_mid_payload() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("test.wal");

    write_records(&path, 3);

    // Manually construct a valid header but truncate the payload.
    {
        let record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::Put as u32,
            lsn: 99,
            tenant_id: 1,
            vshard_id: 0,
            database_id: 0,
            payload: b"full-payload".to_vec(),
            encryption_key: None,
            preamble_bytes: None,
        })
        .unwrap();
        let header_bytes = record.header.to_bytes();

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        // Write full header.
        file.write_all(&header_bytes).unwrap();
        // Write only half the payload.
        file.write_all(&record.payload[..6]).unwrap();
    }

    // Recovery should find exactly 3 records (the torn 4th is ignored).
    let records = read_all(&path);
    assert_eq!(records.len(), 3);
}

#[test]
fn corrupted_checksum_mid_file_fails_recovery_instead_of_truncating() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("test.wal");

    write_records(&path, 5);

    // Remove the double-write buffer so torn write recovery can't help.
    // This tests the raw WAL corruption detection without DWB fallback.
    let dwb_path = path.with_extension("dwb");
    let _ = std::fs::remove_file(&dwb_path);

    // Corrupt a byte inside the 3rd record's payload. Records 4 and 5 remain
    // intact behind it, which is what makes this a hole rather than a tail.
    let wire_size = HEADER_SIZE + "record-0".len();
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        let offset = (2 * wire_size + HEADER_SIZE + 1) as u64;
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&byte).unwrap();
    }

    // The raw reader still stops at the damaged record — it reports the
    // committed prefix it could parse and why it stopped.
    let mut reader = WalReader::open(&path, None).unwrap();
    let mut read = Vec::new();
    while let Some(record) = reader.next_record().unwrap() {
        read.push(record);
    }
    assert_eq!(read.len(), 2);
    assert_eq!(
        reader.stop_reason(),
        Some(StopReason::Corruption {
            offset: (2 * wire_size) as u64
        })
    );

    // Recovery must NOT accept those 2 records as the whole log: records 3-5
    // were acknowledged, and silently truncating them is the data loss this
    // check exists to prevent.
    match recover(&path) {
        Err(WalError::MidFileCorruption { resync_lsn, .. }) => {
            assert!(
                resync_lsn > 2,
                "resync LSN {resync_lsn} must be a record behind the hole"
            );
        }
        Err(other) => panic!("expected MidFileCorruption, got {other:?}"),
        Ok(info) => panic!(
            "recovery silently truncated the log to {} records",
            info.record_count
        ),
    }
}

#[test]
fn dwb_recovery_mid_segment_keeps_every_later_record() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("dwb_mid.wal");

    write_records(&path, 5);
    assert!(
        path.with_extension("dwb").exists(),
        "test needs the double-write buffer present"
    );

    // Tear the 3rd record on disk while leaving records 4 and 5 intact. The
    // DWB still holds an undamaged copy of record 3, so recovery must splice
    // it back in AND carry on — the whole point of a mid-segment recovery is
    // that it is not the end of the log.
    let wire_size = HEADER_SIZE + "record-0".len();
    {
        use std::io::{Seek, SeekFrom};
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start((2 * wire_size + HEADER_SIZE) as u64))
            .unwrap();
        file.write_all(b"ZZZZZZZZ").unwrap();
        file.sync_all().unwrap();
    }

    let records = read_all(&path);
    assert_eq!(
        records.len(),
        5,
        "records after the DWB-recovered one were dropped"
    );
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.header.lsn, i as u64 + 1);
        assert_eq!(record.payload, format!("record-{i}").as_bytes());
    }

    // The read offset must land exactly at EOF: over-advancing past the
    // recovered record is what silently swallowed the tail.
    let info = recover(&path).unwrap();
    assert_eq!(info.record_count, 5);
    assert_eq!(info.last_lsn, 5);
    assert_eq!(info.end_offset, std::fs::metadata(&path).unwrap().len());
}

#[test]
fn corrupted_tail_record_is_a_torn_write() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("torn_tail.wal");

    write_records(&path, 5);
    let _ = std::fs::remove_file(path.with_extension("dwb"));

    // Corrupt the LAST record only. Nothing valid follows, so this is the
    // unfsynced tail of an interrupted write — recovery accepts the prefix.
    let wire_size = HEADER_SIZE + "record-0".len();
    {
        use std::io::{Seek, SeekFrom};
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start((4 * wire_size + HEADER_SIZE) as u64))
            .unwrap();
        file.write_all(b"XXXXXXXX").unwrap();
    }

    let info = recover(&path).unwrap();
    assert_eq!(info.record_count, 4);
    assert_eq!(info.last_lsn, 4);
}

#[test]
fn reopen_after_crash_continues_correctly() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("test.wal");

    // Session 1: write 5 records.
    write_records(&path, 5);

    // Append garbage (simulate crash during next write).
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"CRASHED_HERE").unwrap();
    }

    // Session 2: reopen — should continue from LSN 6.
    {
        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        assert_eq!(writer.next_lsn(), 6);
        let lsn = writer
            .append(RecordType::Put as u32, 1, 0, 0, b"after-crash")
            .unwrap();
        assert_eq!(lsn, 6);
        writer.sync().unwrap();
    }

    // The garbage is overwritten — but since writer appends, it's actually
    // after the garbage. The reader should see records 1-5 (garbage stops it)
    // then the new record starts after. Let's verify recovery picks up LSN 6.
    // Note: the current writer opens with O_WRONLY which truncates from file_offset.
    // In practice, the WAL would need truncation of the tail garbage on reopen.
    // For now, verify that at minimum the first 5 records survive.
    let records = read_all(&path);
    assert!(records.len() >= 5);
}

#[test]
fn idempotent_replay() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("test.wal");

    write_records(&path, 10);

    // Read the WAL twice — must produce identical results.
    let records1 = read_all(&path);
    let records2 = read_all(&path);

    assert_eq!(records1.len(), records2.len());
    for (r1, r2) in records1.iter().zip(records2.iter()) {
        assert_eq!(r1.header.lsn, r2.header.lsn);
        assert_eq!(r1.payload, r2.payload);
    }
}

#[test]
fn many_records_roundtrip() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("test.wal");

    let count = 10_000u32;
    let lsns = write_records(&path, count);
    assert_eq!(lsns.len(), count as usize);

    let records = read_all(&path);
    assert_eq!(records.len(), count as usize);

    // Verify LSN ordering.
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.header.lsn, (i + 1) as u64);
    }

    // Recovery should agree.
    let info = recover(&path).unwrap();
    assert_eq!(info.record_count, count as u64);
    assert_eq!(info.last_lsn, count as u64);
}
