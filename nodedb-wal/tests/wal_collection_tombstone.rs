//! End-to-end tombstone pipeline: write records, write tombstone,
//! replay, assert shadowed writes are filtered.
//!
//! Exercises the full primitive chain without booting the full server:
//!
//! 1. [`CollectionTombstonePayload`] serialization.
//! 2. `RecordType::CollectionTombstoned` round-trip through
//!    [`WalWriter`] / [`WalReader`].
//! 3. [`extract_tombstones`] over a mixed record stream.
//! 4. [`TombstoneSet::is_tombstoned`] against realistic `(tenant,
//!    collection, lsn)` tuples.

use nodedb_wal::reader::WalReader;
use nodedb_wal::record::RecordType;
use nodedb_wal::writer::WalWriter;
use nodedb_wal::{CollectionTombstonePayload, TombstoneSet, WalRecord, extract_tombstones};

/// Append a record, return its LSN.
fn append_record(
    writer: &mut WalWriter,
    record_type: RecordType,
    tenant_id: u32,
    payload: &[u8],
) -> u64 {
    writer
        .append(record_type as u16, tenant_id, 0, payload)
        .unwrap()
}

fn read_all(path: &std::path::Path) -> Vec<WalRecord> {
    let reader = WalReader::open(path).unwrap();
    reader.records().map(|r| r.unwrap()).collect()
}

#[test]
fn tombstone_record_roundtrips_through_wal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");

    let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
    let payload = CollectionTombstonePayload::new("users", 500)
        .to_bytes()
        .unwrap();
    append_record(&mut writer, RecordType::CollectionTombstoned, 1, &payload);
    writer.sync().unwrap();

    let records = read_all(&path);
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].logical_record_type(),
        RecordType::CollectionTombstoned as u16
    );
    let decoded = CollectionTombstonePayload::from_bytes(&records[0].payload).unwrap();
    assert_eq!(decoded.collection, "users");
    assert_eq!(decoded.purge_lsn, 500);
}

#[test]
fn extract_and_shadow_writes_before_purge_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");

    let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
    // Three pre-purge writes on tenant 1, "users".
    let put_lsns: Vec<u64> = (0..3)
        .map(|i| {
            append_record(
                &mut writer,
                RecordType::Put,
                1,
                format!("users-row-{i}").as_bytes(),
            )
        })
        .collect();
    // A write on a DIFFERENT collection — must not be shadowed.
    let orders_lsn = append_record(&mut writer, RecordType::Put, 1, b"orders-row");
    // A write on a DIFFERENT tenant for the same name — must not be shadowed.
    let other_tenant_lsn = append_record(&mut writer, RecordType::Put, 2, b"users-row-other");
    // Tombstone at some purge_lsn > all three pre-purge LSNs.
    let purge_lsn = *put_lsns.last().unwrap() + 1;
    let tombstone_payload = CollectionTombstonePayload::new("users", purge_lsn)
        .to_bytes()
        .unwrap();
    append_record(
        &mut writer,
        RecordType::CollectionTombstoned,
        1,
        &tombstone_payload,
    );
    // A POST-purge write (re-create of "users" after the drop) — must
    // NOT be shadowed: its LSN exceeds purge_lsn.
    let post_purge_lsn = append_record(&mut writer, RecordType::Put, 1, b"users-row-reborn");
    writer.sync().unwrap();

    let records = read_all(&path);
    let set: TombstoneSet = extract_tombstones(&records);

    assert_eq!(set.len(), 1);
    assert_eq!(set.purge_lsn(1, "users"), Some(purge_lsn));

    for lsn in &put_lsns {
        assert!(
            set.is_tombstoned(1, "users", *lsn),
            "pre-purge write at lsn {lsn} must be shadowed"
        );
    }
    assert!(
        !set.is_tombstoned(1, "orders", orders_lsn),
        "different collection must not be shadowed"
    );
    assert!(
        !set.is_tombstoned(2, "users", other_tenant_lsn),
        "different tenant must not be shadowed"
    );
    assert!(
        !set.is_tombstoned(1, "users", post_purge_lsn),
        "post-purge write (lsn >= purge_lsn) must not be shadowed"
    );
}

#[test]
fn multiple_tombstones_keep_highest_purge_lsn() {
    // Simulates drop → recreate → drop on the same collection: the
    // second tombstone's purge_lsn shadows more history than the first.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");

    let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
    let first_payload = CollectionTombstonePayload::new("users", 100)
        .to_bytes()
        .unwrap();
    append_record(
        &mut writer,
        RecordType::CollectionTombstoned,
        1,
        &first_payload,
    );
    let second_payload = CollectionTombstonePayload::new("users", 500)
        .to_bytes()
        .unwrap();
    append_record(
        &mut writer,
        RecordType::CollectionTombstoned,
        1,
        &second_payload,
    );
    // Out-of-order lower purge_lsn must not regress the shadowing LSN.
    let lower_payload = CollectionTombstonePayload::new("users", 50)
        .to_bytes()
        .unwrap();
    append_record(
        &mut writer,
        RecordType::CollectionTombstoned,
        1,
        &lower_payload,
    );
    writer.sync().unwrap();

    let records = read_all(&path);
    let set = extract_tombstones(&records);
    assert_eq!(set.purge_lsn(1, "users"), Some(500));
    assert!(set.is_tombstoned(1, "users", 499));
    assert!(!set.is_tombstoned(1, "users", 500));
}

#[test]
fn extract_ignores_unrelated_record_types() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");

    let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
    append_record(&mut writer, RecordType::Put, 1, b"put-1");
    append_record(&mut writer, RecordType::Delete, 1, b"del-1");
    append_record(&mut writer, RecordType::VectorPut, 1, b"vec-1");
    append_record(&mut writer, RecordType::Checkpoint, 1, b"ckpt-1");
    writer.sync().unwrap();

    let records = read_all(&path);
    assert_eq!(records.len(), 4);
    let set = extract_tombstones(&records);
    assert!(set.is_empty());
}
