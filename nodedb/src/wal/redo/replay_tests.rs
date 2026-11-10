// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::wal::{RedoRecord, RedoSubRecord};

fn redo_wal_record(lsn: u64, tenant_id: u64, vshard_id: u32, record: &RedoRecord) -> WalRecord {
    WalRecord::new(WalRecordArgs {
        record_type: RecordType::TransactionRedo as u32,
        lsn,
        tenant_id,
        vshard_id,
        database_id: 0,
        payload: record.to_bytes().expect("encode redo record"),
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record")
}

#[test]
fn reconstitute_preserves_type_payload_and_header() {
    let redo = RedoRecord {
        version: 1,
        ops: vec![
            RedoSubRecord {
                record_type: RecordType::VectorPut as u32,
                payload: vec![1, 2, 3],
            },
            RedoSubRecord {
                record_type: RecordType::SpatialPut as u32,
                payload: vec![4, 5],
            },
        ],
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
    };
    let outer = redo_wal_record(77, 9, 3, &redo);

    let reconstituted =
        reconstitute_redo_records(std::slice::from_ref(&outer)).expect("well-formed redo");
    assert!(reconstituted.grouped_origins.contains(77));
    assert!(!reconstituted.grouped_origins.contains(78));
    let recon = reconstituted.records;
    assert_eq!(recon.len(), 2);
    assert_eq!(recon[0].logical_record_type(), RecordType::VectorPut as u32);
    assert_eq!(recon[0].payload, vec![1, 2, 3]);
    assert_eq!(
        recon[1].logical_record_type(),
        RecordType::SpatialPut as u32
    );
    assert_eq!(recon[1].payload, vec![4, 5]);
    // Enclosing header identity propagates to every sub-record.
    for r in &recon {
        assert_eq!(r.header.lsn, 77);
        assert_eq!(r.header.tenant_id, 9);
        assert_eq!(r.header.vshard_id, 3);
    }
}

/// Every sub-record replays at the system ordinal its record carries, the
/// one the record's live apply stamped.
#[test]
fn reconstitute_carries_the_record_system_ordinal() {
    let redo = RedoRecord {
        version: 1,
        ops: vec![
            RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: vec![1],
            },
            RedoSubRecord {
                record_type: RecordType::Delete as u32,
                payload: vec![2],
            },
        ],
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
    };
    let outer = WalRecord::new_stamped(
        WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: 40,
            tenant_id: 1,
            vshard_id: 0,
            database_id: 0,
            payload: redo.to_bytes().expect("encode redo record"),
            encryption_key: None,
            preamble_bytes: None,
        },
        RecordStamp {
            system_ordinal: 1_800_000_000_000_000_042,
            ..RecordStamp::NONE
        },
    )
    .expect("wal record");

    let recon = reconstitute_redo_records(std::slice::from_ref(&outer))
        .expect("well-formed redo")
        .records;
    assert_eq!(recon.len(), 2);
    for r in &recon {
        assert_eq!(r.system_ordinal(), Some(1_800_000_000_000_000_042));
    }
}

#[test]
fn reconstitute_skips_non_redo_records() {
    let put = WalRecord::new(WalRecordArgs {
        record_type: RecordType::Put as u32,
        lsn: 1,
        tenant_id: 0,
        vshard_id: 0,
        database_id: 0,
        payload: vec![9, 9, 9],
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record");

    let recon = reconstitute_redo_records(&[put])
        .expect("non-redo records are ignored")
        .records;
    assert!(recon.is_empty());
}

fn put_wal_record(lsn: u64, payload: Vec<u8>) -> WalRecord {
    WalRecord::new(WalRecordArgs {
        record_type: RecordType::Put as u32,
        lsn,
        tenant_id: 0,
        vshard_id: 0,
        database_id: 0,
        payload,
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record")
}

/// With no redo groups the merge must not copy the tail at all — the
/// ordinary boot walks the borrowed slice.
#[test]
fn merge_borrows_when_there_are_no_redo_groups() {
    let standalone = vec![put_wal_record(1, vec![1]), put_wal_record(2, vec![2])];
    let merged = merge_by_lsn(&standalone, &[]);
    assert!(matches!(merged, Cow::Borrowed(_)));
    assert_eq!(merged.len(), 2);
}

/// The defect this ordering exists to prevent: a redo op written at a LOWER
/// LSN than a later autocommit write must be applied BEFORE it, or the
/// absolute overwrite it carries resurrects the older value.
#[test]
fn lower_lsn_redo_op_is_ordered_before_a_higher_lsn_autocommit() {
    let redo = RedoRecord {
        version: 1,
        ops: vec![RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: vec![0xAA],
        }],
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
    };
    // The transaction committed at LSN 50; an autocommit overwrote the same
    // key at LSN 100.
    let standalone = vec![
        redo_wal_record(50, 0, 0, &redo),
        put_wal_record(100, vec![0xBB]),
    ];
    let redo_ops = reconstitute_redo_records(&standalone)
        .expect("well-formed redo")
        .records;
    let merged = merge_by_lsn(&standalone, &redo_ops);

    let applied: Vec<(u64, Vec<u8>)> = merged
        .iter()
        .filter(|r| r.logical_record_type() == RecordType::Put as u32)
        .map(|r| (r.header.lsn, r.payload.clone()))
        .collect();
    assert_eq!(
        applied,
        vec![(50, vec![0xAA]), (100, vec![0xBB])],
        "the LSN-50 redo op must be applied before the LSN-100 autocommit"
    );
}

/// Sub-records of one group share the enclosing record's LSN, so the sort
/// must be stable or an intra-transaction write order is lost.
#[test]
fn merge_preserves_intra_transaction_order() {
    let redo = RedoRecord {
        version: 1,
        ops: vec![
            RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: vec![1],
            },
            RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: vec![2],
            },
            RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: vec![3],
            },
        ],
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
    };
    let standalone = vec![redo_wal_record(7, 0, 0, &redo)];
    let redo_ops = reconstitute_redo_records(&standalone)
        .expect("well-formed redo")
        .records;
    let merged = merge_by_lsn(&standalone, &redo_ops);
    let payloads: Vec<Vec<u8>> = merged
        .iter()
        .filter(|r| r.logical_record_type() == RecordType::Put as u32)
        .map(|r| r.payload.clone())
        .collect();
    assert_eq!(payloads, vec![vec![1], vec![2], vec![3]]);
}

/// Merging the same records twice yields the same sequence: replay of an
/// already-replayed tail must present engines with an identical stream.
#[test]
fn merge_is_deterministic_across_repeated_replays() {
    let redo = RedoRecord {
        version: 1,
        ops: vec![RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: vec![9],
        }],
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
    };
    let standalone = vec![
        put_wal_record(1, vec![1]),
        redo_wal_record(2, 0, 0, &redo),
        put_wal_record(3, vec![3]),
    ];
    let redo_ops = reconstitute_redo_records(&standalone)
        .expect("well-formed redo")
        .records;
    let first: Vec<u64> = merge_by_lsn(&standalone, &redo_ops)
        .iter()
        .map(|r| r.header.lsn)
        .collect();
    let second: Vec<u64> = merge_by_lsn(&standalone, &redo_ops)
        .iter()
        .map(|r| r.header.lsn)
        .collect();
    assert_eq!(first, second);
    assert_eq!(first, vec![1, 2, 2, 3]);
}

/// A `TransactionRedo` record whose payload is structurally invalid but
/// CRC-valid represents a committed transaction that cannot be applied.
/// Replay must fail rather than skip it and come up missing those writes.
#[test]
fn malformed_redo_payload_aborts_replay() {
    let bad = WalRecord::new(WalRecordArgs {
        record_type: RecordType::TransactionRedo as u32,
        lsn: 2,
        tenant_id: 0,
        vshard_id: 0,
        database_id: 0,
        payload: vec![0xff, 0xff, 0xff],
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record");
    // The CRC is intact — the payload, not the bytes, is what is wrong.
    bad.verify_checksum().expect("record bytes are consistent");

    assert!(
        reconstitute_redo_records(&[bad]).is_err(),
        "a committed redo group that cannot be decoded must abort recovery"
    );
}

#[test]
fn write_group_origin_uses_the_effective_lsn_instead_of_its_part_lsn() {
    let group = WriteGroupRecord {
        schemas: Vec::new(),
        group: super::super::group::WriteGroup::part_of(7, 1, 1),
        ops: vec![RedoSubRecord {
            record_type: RecordType::FtsIndex as u32,
            payload: vec![1],
        }],
        redo: None,
    };
    let outer = WalRecord::new(WalRecordArgs {
        record_type: RecordType::WriteGroup as u32,
        lsn: 40,
        tenant_id: 1,
        vshard_id: 0,
        database_id: 0,
        payload: group.to_bytes().unwrap(),
        encryption_key: None,
        preamble_bytes: None,
    })
    .unwrap();
    let reconstituted = reconstitute_redo_records(&[outer]).unwrap();
    assert_eq!(reconstituted.records[0].header.lsn, 7);
    assert!(reconstituted.grouped_origins.contains(7));
    assert!(!reconstituted.grouped_origins.contains(40));
}
