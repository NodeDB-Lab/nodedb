// SPDX-License-Identifier: Apache-2.0

//! Torn-write protection has to be observable when it goes away.
//!
//! A double-write buffer that fails, or a record too large to mirror, never
//! fails the WAL append — so nothing in the return value tells the caller that
//! a torn record at the tail of this segment can no longer be reconstructed.
//! The writer's protection standing and the process counters are the only
//! signal, and these tests hold them to it.

use nodedb_wal::double_write::slot_record_max;
use nodedb_wal::record::{HEADER_SIZE, RecordType};
use nodedb_wal::writer::{WalWriter, WalWriterConfig};
use nodedb_wal::{DwbMode, DwbProtection, WalReader, wal_dwb_unprotected_records_total};

fn append(writer: &mut WalWriter, payload: &[u8]) -> u64 {
    writer
        .append(RecordType::Put as u32, 1, 0, 0, payload)
        .expect("the WAL append must succeed regardless of DWB health")
}

fn replayed_payloads(path: &std::path::Path) -> Vec<Vec<u8>> {
    let mut reader = WalReader::open(path, None).unwrap();
    let mut out = Vec::new();
    while let Some(record) = reader.next_record().unwrap() {
        out.push(record.payload);
    }
    out
}

/// The healthy baseline: nothing is reported unprotected when every record is
/// mirrored. Without this, the assertions below could pass on a counter that
/// simply counts every append.
#[test]
fn mirrored_records_report_full_protection() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("healthy.wal");

    let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
    assert_eq!(writer.dwb_protection(), DwbProtection::Active);

    for i in 0..4u32 {
        append(&mut writer, format!("row-{i}").as_bytes());
    }
    writer.sync().unwrap();

    assert_eq!(writer.dwb_protection(), DwbProtection::Active);
    assert_eq!(writer.dwb_unprotected_records(), 0);
}

/// A record too large for a slot cannot be mirrored. It used to be dropped
/// from the DWB with a bare `Ok(())`, leaving the caller believing the record
/// was covered.
#[test]
fn oversized_record_is_reported_as_unprotected() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("oversized.wal");

    let before = wal_dwb_unprotected_records_total();
    let mut writer = WalWriter::open_without_direct_io(&path).unwrap();

    // One byte past what a slot can hold, so the record cannot be mirrored.
    let oversized = vec![0x5au8; slot_record_max() - HEADER_SIZE + 1];
    append(&mut writer, &oversized);
    writer.sync().unwrap();

    assert_eq!(
        writer.dwb_unprotected_records(),
        1,
        "an unmirrored record must be reported, not silently skipped"
    );
    assert!(wal_dwb_unprotected_records_total() > before);
    // The buffer itself is healthy — only this record is uncovered.
    assert_eq!(writer.dwb_protection(), DwbProtection::Active);

    // A record that does fit is mirrored again, and is not counted.
    append(&mut writer, b"small");
    writer.sync().unwrap();
    assert_eq!(writer.dwb_unprotected_records(), 1);

    assert_eq!(replayed_payloads(&path), vec![oversized, b"small".to_vec()]);
}

/// A DWB that is off by configuration is not a degradation, so it must not
/// show up as one.
#[test]
fn a_disabled_dwb_is_not_reported_as_degraded() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("dwb_off.wal");

    let mut writer = WalWriter::open(
        &path,
        WalWriterConfig {
            use_direct_io: false,
            dwb_mode: Some(DwbMode::Off),
            ..Default::default()
        },
    )
    .unwrap();

    append(&mut writer, b"unprotected-by-choice");
    writer.sync().unwrap();

    assert_eq!(writer.dwb_protection(), DwbProtection::Off);
    assert_eq!(writer.dwb_unprotected_records(), 0);
    assert_eq!(writer.reattach_double_write(), DwbProtection::Off);
}

#[cfg(feature = "failpoints")]
mod injected {
    use super::*;
    use nodedb_types::fail_point::FailGuard;
    use nodedb_wal::{DwbDegradation, wal_dwb_degradations_total};

    /// A failing DWB write must not fail the append, must flip the writer into
    /// a degraded standing that a caller can read, and must keep counting the
    /// records that go unprotected afterwards — not emit one log line and
    /// pretend protection is still in place.
    #[test]
    fn dwb_write_failure_is_observable_and_the_append_still_succeeds() {
        let dir = crate::tempdir().unwrap();
        let path = dir.path().join("dwb_write_failure.wal");

        let unprotected_before = wal_dwb_unprotected_records_total();
        let degradations_before = wal_dwb_degradations_total();

        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        assert_eq!(writer.dwb_protection(), DwbProtection::Active);

        {
            let _g = FailGuard::fail("wal::dwb_write_failure", "EIO");
            append(&mut writer, b"degraded-1");
        }

        assert_eq!(
            writer.dwb_protection(),
            DwbProtection::Degraded(DwbDegradation::WriteFailed)
        );
        assert_eq!(writer.dwb_unprotected_records(), 1);
        assert!(wal_dwb_degradations_total() > degradations_before);

        // Detached, so every later record is unprotected too.
        append(&mut writer, b"degraded-2");
        assert_eq!(writer.dwb_unprotected_records(), 2);
        assert!(wal_dwb_unprotected_records_total() >= unprotected_before + 2);

        // The WAL itself is untouched by the DWB failure.
        writer.sync().unwrap();
        assert_eq!(
            replayed_payloads(&path),
            vec![b"degraded-1".to_vec(), b"degraded-2".to_vec()]
        );

        // Degradation is recoverable once the underlying fault is gone.
        assert_eq!(writer.reattach_double_write(), DwbProtection::Active);
        append(&mut writer, b"protected-again");
        writer.sync().unwrap();
        assert_eq!(writer.dwb_unprotected_records(), 2);
    }
}
