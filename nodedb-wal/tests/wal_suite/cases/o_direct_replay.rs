// SPDX-License-Identifier: Apache-2.0

//! Multi-batch O_DIRECT replay.
//!
//! Under O_DIRECT every flush is rounded up to the device block size and the
//! file offset advances by the padded length, so consecutive batches are not
//! adjacent on disk. The bytes in between are stale write-buffer content, and
//! a reader walking records back to back parses them as a record header,
//! fails, and calls that the end of the segment.
//!
//! These tests write several separately-fsynced batches to one O_DIRECT
//! segment and assert every reader recovers *all* of them — not just the
//! first. Each one fails if the padding stops being framed as a `Noop` record.

use std::path::{Path, PathBuf};

use nodedb_wal::align::{DEFAULT_ALIGNMENT, is_aligned};
use nodedb_wal::lazy_reader::LazyWalReader;
use nodedb_wal::mmap_reader::MmapWalReader;
use nodedb_wal::reader::{StopReason, WalReader};
use nodedb_wal::record::{HEADER_SIZE, RecordType};
use nodedb_wal::recovery::recover;
use nodedb_wal::writer::{WalWriter, WalWriterConfig};

fn target_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
}

fn direct_config() -> WalWriterConfig {
    WalWriterConfig {
        use_direct_io: true,
        ..Default::default()
    }
}

/// Open an O_DIRECT writer, or `None` when the filesystem cannot support it
/// (tmpfs, overlayfs) — in that case the scenario is not exercisable here.
fn open_direct(path: &Path) -> Option<WalWriter> {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("dwb"));
    WalWriter::open(path, direct_config()).ok()
}

/// Write each payload as its own fsynced batch, so each one is padded out to
/// its own block boundary.
fn write_batches(writer: &mut WalWriter, payloads: &[&str]) {
    for payload in payloads {
        writer
            .append(RecordType::Put as u32, 1, 0, 0, payload.as_bytes())
            .unwrap();
        writer.sync().unwrap();
    }
}

fn read_all(path: &Path) -> Vec<Vec<u8>> {
    let mut reader = WalReader::open(path, None).unwrap();
    let mut out = Vec::new();
    while let Some(record) = reader.next_record().unwrap() {
        out.push(record.payload);
    }
    assert_eq!(
        reader.stop_reason(),
        Some(StopReason::Eof),
        "padded segment must end cleanly, not on a parse failure"
    );
    out
}

#[test]
fn every_o_direct_batch_replays_not_just_the_first() {
    let path = target_dir().join("direct_multi_batch.wal");
    let Some(mut writer) = open_direct(&path) else {
        return; // O_DIRECT unsupported here.
    };

    let payloads = ["batch-one", "batch-two", "batch-three", "batch-four"];
    write_batches(&mut writer, &payloads);
    drop(writer);

    // Each batch was padded to its own block, so the segment is strictly
    // larger than the records it holds — this is the layout that used to
    // truncate replay after the first batch.
    let file_len = std::fs::metadata(&path).unwrap().len();
    assert_eq!(file_len, (payloads.len() * DEFAULT_ALIGNMENT) as u64);

    let recovered = read_all(&path);
    assert_eq!(
        recovered.len(),
        payloads.len(),
        "only {} of {} O_DIRECT batches replayed",
        recovered.len(),
        payloads.len()
    );
    for (got, want) in recovered.iter().zip(payloads.iter()) {
        assert_eq!(got.as_slice(), want.as_bytes());
    }
}

#[test]
fn recovery_reports_every_batch_and_resumes_on_a_boundary() {
    let path = target_dir().join("direct_recovery.wal");
    let Some(mut writer) = open_direct(&path) else {
        return;
    };

    write_batches(&mut writer, &["one", "two", "three"]);
    drop(writer);

    let info = recover(&path).unwrap();
    assert_eq!(info.record_count, 3);
    assert_eq!(info.last_lsn, 3);
    assert!(
        is_aligned(info.end_offset as usize, DEFAULT_ALIGNMENT),
        "end_offset {} is not a valid O_DIRECT resume point",
        info.end_offset
    );

    // Reopening must continue the LSN sequence and keep every earlier batch.
    let mut writer = WalWriter::open(&path, direct_config()).unwrap();
    assert_eq!(writer.next_lsn(), 4);
    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"four")
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovered = read_all(&path);
    assert_eq!(recovered.len(), 4);
    assert_eq!(recovered[3], b"four");
}

#[test]
fn mmap_and_lazy_readers_also_cross_batch_boundaries() {
    let path = target_dir().join("direct_all_readers.wal");
    let Some(mut writer) = open_direct(&path) else {
        return;
    };

    let payloads = ["alpha", "beta", "gamma"];
    write_batches(&mut writer, &payloads);
    drop(writer);

    let mut mmap = MmapWalReader::open(&path, None).unwrap();
    let mut mmap_records = Vec::new();
    while let Some(record) = mmap.next_record().unwrap() {
        mmap_records.push(record.payload);
    }
    assert_eq!(
        mmap_records.len(),
        payloads.len(),
        "mmap reader lost batches"
    );
    assert_eq!(mmap.stop_reason(), Some(StopReason::Eof));

    let mut lazy = LazyWalReader::open(&path, None).unwrap();
    let mut lazy_payloads = Vec::new();
    while let Some(header) = lazy.next_header().unwrap() {
        lazy_payloads.push(lazy.read_payload(&header).unwrap());
    }
    assert_eq!(
        lazy_payloads.len(),
        payloads.len(),
        "lazy reader lost batches"
    );
    assert_eq!(lazy.stop_reason(), Some(StopReason::Eof));
    for (got, want) in lazy_payloads.iter().zip(payloads.iter()) {
        assert_eq!(got.as_slice(), want.as_bytes());
    }
}

#[test]
fn padding_is_never_surfaced_as_a_record() {
    let path = target_dir().join("direct_no_padding_leak.wal");
    let Some(mut writer) = open_direct(&path) else {
        return;
    };

    write_batches(&mut writer, &["x", "y"]);
    drop(writer);

    let mut reader = WalReader::open(&path, None).unwrap();
    while let Some(record) = reader.next_record().unwrap() {
        assert_ne!(
            record.header.record_type,
            RecordType::Noop as u32,
            "alignment padding leaked into replay"
        );
        assert_ne!(record.header.lsn, 0, "padding LSN leaked into replay");
    }
}

/// Damage the padding record that closes the segment's last batch, as a
/// crash that tears only that block's tail leaves it. Returns the offset of
/// the padding record.
fn tear_trailing_padding(path: &Path, records: &[&[u8]]) -> u64 {
    use std::os::unix::fs::FileExt as _;

    let padding_at: usize = records.iter().map(|r| HEADER_SIZE + r.len()).sum();
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.write_all_at(&[0u8; HEADER_SIZE], padding_at as u64)
        .unwrap();
    file.sync_all().unwrap();
    padding_at as u64
}

/// A crash tears only the padding of the last batch, whose final record
/// ends mid-block. Recovery ends at that record, the writer resumes on the
/// block boundary below it, and its next write keeps the record's tail. A
/// writer that rounded down and wrote from there overwrote that tail.
#[test]
fn a_torn_padding_block_keeps_the_record_tail_it_shares() {
    let path = target_dir().join("direct_torn_padding.wal");
    let Some(mut writer) = open_direct(&path) else {
        return;
    };
    let spanning = vec![0x5Au8; DEFAULT_ALIGNMENT + 1000];
    let small = b"second".to_vec();
    for payload in [&spanning, &small] {
        writer
            .append(RecordType::Put as u32, 1, 0, 0, payload)
            .unwrap();
    }
    writer.sync().unwrap();
    drop(writer);
    let _ = std::fs::remove_file(path.with_extension("dwb"));

    let padding_at = tear_trailing_padding(&path, &[&spanning, &small]);
    let info = recover(&path).unwrap();
    assert_eq!(info.end_offset, padding_at);
    assert!(!is_aligned(padding_at as usize, DEFAULT_ALIGNMENT));

    let mut writer = WalWriter::open(&path, direct_config()).unwrap();
    assert_eq!(writer.next_lsn(), 3);
    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"third")
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    assert_eq!(
        read_all(&path),
        vec![spanning, small, b"third".to_vec()],
        "every record survives the resumed write"
    );
}
