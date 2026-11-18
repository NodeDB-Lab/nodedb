// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::wal::redo::install_cursor::{CursorState, InstallCursor};
use crate::wal::{RedoRecord, RedoSubRecord};

/// Why a fetch from disk refuses in a read that holds no WAL.
fn no_wal_to_fetch(reference: &RedoStreamRef) -> crate::Result<Option<Vec<u8>>> {
    Err(crate::Error::Internal {
        detail: format!(
            "this read holds no WAL on disk, so stream {:?} cannot be fetched",
            reference.stream
        ),
    })
}

/// Reconstitute `records`, a read with no WAL behind it: a marker whose
/// chunks the read lacks fails loudly.
fn reconstitute(records: &[WalRecord]) -> crate::Result<ReconstitutedReplay> {
    let mut fetch = no_wal_to_fetch;
    let mut contents = StreamContents::new(records, &mut fetch);
    reconstitute_redo_records(records, &mut contents, &InstallCursors::default())
}

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
        sum_targets: Vec::new(),
    };
    let outer = redo_wal_record(77, 9, 3, &redo);

    let reconstituted = reconstitute(std::slice::from_ref(&outer)).expect("well-formed redo");
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
        sum_targets: Vec::new(),
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

    let recon = reconstitute(std::slice::from_ref(&outer))
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

    let recon = reconstitute(&[put])
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
    let merged = merge_by_lsn(&standalone, &[], &[]).expect("merge");
    assert!(matches!(merged.records, Cow::Borrowed(_)));
    assert_eq!(merged.records.len(), 2);
    assert_eq!(merged.ops.of(0), None);
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
        sum_targets: Vec::new(),
    };
    // The transaction committed at LSN 50; an autocommit overwrote the same
    // key at LSN 100.
    let standalone = vec![
        redo_wal_record(50, 0, 0, &redo),
        put_wal_record(100, vec![0xBB]),
    ];
    let recon = reconstitute(&standalone).expect("well-formed redo");
    let merged = merge_by_lsn(&standalone, &recon.records, &recon.op_tags)
        .expect("merge")
        .records;

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
        sum_targets: Vec::new(),
    };
    let standalone = vec![redo_wal_record(7, 0, 0, &redo)];
    let recon = reconstitute(&standalone).expect("well-formed redo");
    let merged = merge_by_lsn(&standalone, &recon.records, &recon.op_tags)
        .expect("merge")
        .records;
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
        sum_targets: Vec::new(),
    };
    let standalone = vec![
        put_wal_record(1, vec![1]),
        redo_wal_record(2, 0, 0, &redo),
        put_wal_record(3, vec![3]),
    ];
    let recon = reconstitute(&standalone).expect("well-formed redo");
    let first: Vec<u64> = merge_by_lsn(&standalone, &recon.records, &recon.op_tags)
        .expect("merge")
        .records
        .iter()
        .map(|r| r.header.lsn)
        .collect();
    let second: Vec<u64> = merge_by_lsn(&standalone, &recon.records, &recon.op_tags)
        .expect("merge")
        .records
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
        reconstitute(&[bad]).is_err(),
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
    let reconstituted = reconstitute(&[outer]).unwrap();
    assert_eq!(reconstituted.records[0].header.lsn, 7);
    assert!(reconstituted.grouped_origins.contains(7));
    assert!(!reconstituted.grouped_origins.contains(40));
}

/// A redo sub-record carries its index among its own record's ops. A
/// write-group part at the same origin carries none, and neither does a
/// standalone record or the enclosing redo record.
#[test]
fn op_index_counts_sub_records_of_their_record_only() {
    let put = |byte: u8| RedoSubRecord {
        record_type: RecordType::Put as u32,
        payload: vec![byte],
    };
    let redo = RedoRecord {
        version: 1,
        ops: vec![put(1), put(2), put(3)],
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
        sum_targets: Vec::new(),
    };
    let group = WriteGroupRecord {
        schemas: Vec::new(),
        group: super::super::group::WriteGroup::part_of(7, 1, 1),
        ops: vec![put(8), put(9)],
        redo: None,
    };
    let part = WalRecord::new(WalRecordArgs {
        record_type: RecordType::WriteGroup as u32,
        lsn: 40,
        tenant_id: 0,
        vshard_id: 0,
        database_id: 0,
        payload: group.to_bytes().expect("encode group"),
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record");
    let standalone = vec![
        put_wal_record(5, vec![5]),
        redo_wal_record(7, 0, 0, &redo),
        part,
    ];
    let recon = reconstitute(&standalone).expect("well-formed records");
    assert_eq!(
        recon.op_tags,
        [Some(0), Some(1), Some(2), None, None],
        "the record's ops count from 0 and the part's rows carry none"
    );

    let merged = merge_by_lsn(&standalone, &recon.records, &recon.op_tags).expect("merge");
    let shape: Vec<(u64, Vec<u8>, Option<u64>)> = merged
        .records
        .iter()
        .enumerate()
        .filter(|(_, r)| r.logical_record_type() == RecordType::Put as u32)
        .map(|(position, r)| (r.header.lsn, r.payload.clone(), merged.ops.of(position)))
        .collect();
    assert_eq!(
        shape,
        [
            (5, vec![5], None),
            (7, vec![1], Some(0)),
            (7, vec![2], Some(1)),
            (7, vec![3], Some(2)),
            (7, vec![8], None),
            (7, vec![9], None),
        ]
    );
    assert_eq!(merged.ops.of(merged.records.len()), None);
}

/// A continuation of a split record continues its origin's op count, in
/// continuation order. A part journalled after apply at the same origin
/// carries none.
#[test]
fn op_index_continues_the_count_through_continuations() {
    let put = |byte: u8| RedoSubRecord {
        record_type: RecordType::Put as u32,
        payload: vec![byte],
    };
    let group_record = |lsn: u64, group: super::super::group::WriteGroup, ops| {
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::WriteGroup as u32,
            lsn,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload: WriteGroupRecord {
                schemas: Vec::new(),
                group,
                ops,
                redo: None,
            }
            .to_bytes()
            .expect("encode group"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    };
    let redo = RedoRecord {
        version: 1,
        ops: vec![put(1)],
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
        sum_targets: Vec::new(),
    };
    let records = vec![
        redo_wal_record(7, 0, 0, &redo),
        group_record(
            8,
            super::super::group::WriteGroup::continuing(7, 1),
            vec![put(2), put(3)],
        ),
        group_record(
            9,
            super::super::group::WriteGroup::continuing(7, 2),
            vec![put(4)],
        ),
        group_record(
            10,
            super::super::group::WriteGroup::part_of(7, 1, 1),
            vec![put(5)],
        ),
    ];
    let recon = reconstitute(&records).expect("well-formed records");
    assert_eq!(
        recon.op_tags,
        [Some(0), Some(1), Some(2), Some(3), None],
        "continuations count on from the origin and the part carries none"
    );
    assert!(recon.records.iter().all(|r| r.header.lsn == 7));

    // A continuation whose origin the read lacks folds nothing.
    let recon = reconstitute(&records[1..]).expect("well-formed records");
    assert_eq!(recon.op_tags, [None, None, None, None]);

    // A continuation out of order is refused.
    let swapped = vec![records[0].clone(), records[2].clone(), records[1].clone()];
    assert!(reconstitute(&swapped).is_err());
}

/// Op tags that do not match the reconstituted sub-records one to one are
/// refused, never read as untagged.
#[test]
fn merge_refuses_tags_that_do_not_match_the_sub_records() {
    let standalone = vec![put_wal_record(1, vec![1])];
    let redo_ops = vec![put_wal_record(1, vec![2]), put_wal_record(1, vec![3])];
    assert!(merge_by_lsn(&standalone, &redo_ops, &[Some(0)]).is_err());
    assert!(merge_by_lsn(&standalone, &[], &[Some(0)]).is_err());
}

/// A WAL holding one stream marker, its content's sub-records, and the
/// marker's LSN.
struct MarkerWal {
    _dir: tempfile::TempDir,
    wal: crate::wal::WalManager,
    ops: Vec<RedoSubRecord>,
    marker: u64,
}

/// Append a stream's content as chunks of 64 bytes, then its marker.
fn marker_wal() -> MarkerWal {
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::RedoChunkPlacement;
    use crate::wal::redo::frame::{RedoContentHeader, RedoFrameTag, push_frame};
    use crate::wal::{RedoChunkHeader, RedoChunkRef, RedoStreamId, chunk_crc};

    let dir = tempfile::tempdir().expect("tempdir");
    let wal = crate::wal::WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
    let ops: Vec<RedoSubRecord> = (0..4u8)
        .map(|byte| RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: vec![byte; 30],
        })
        .collect();
    let mut content = Vec::new();
    let header = RedoContentHeader {
        version: 1,
        calvin_stamp: None,
        cross_shard_applied: None,
        user_roles: Vec::new(),
        sum_targets: Vec::new(),
    };
    push_frame(&mut content, RedoFrameTag::Header, &header).expect("header");
    for op in &ops {
        push_frame(&mut content, RedoFrameTag::Op, op).expect("op");
    }
    let stream = RedoStreamId::Session {
        vshard: 0,
        idempotency_key: 9,
    };
    let placement = RedoChunkPlacement {
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        database_id: DatabaseId::DEFAULT,
    };
    let mut chunks = Vec::new();
    for (index, bytes) in (0u32..).zip(content.chunks(64)) {
        let chunk_header = RedoChunkHeader {
            stream,
            group_id: 1,
            term: 1,
            len: content.len() as u64,
            index,
        };
        let lsns = wal
            .append_redo_chunk(1 + u64::from(index), placement, chunk_header, bytes)
            .expect("chunk");
        chunks.push(RedoChunkRef {
            index,
            first_lsn: lsns.first.as_u64(),
            last_lsn: lsns.last.as_u64(),
            len: bytes.len() as u64,
            crc: chunk_crc(bytes),
        });
    }
    let reference = RedoStreamRef {
        stream,
        len: content.len() as u64,
        chunks,
        publish_position: None,
        entry: None,
        stamp_floor: None,
    };
    let marker = wal
        .appender(9)
        .with_event_source(crate::event::EventSource::User)
        .append_transaction_redo(
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &reference.marker(None, None, Vec::new()).expect("marker"),
        )
        .expect("marker record")
        .as_u64();
    wal.sync().expect("sync");
    MarkerWal {
        _dir: dir,
        wal,
        ops,
        marker,
    }
}

/// Reconstitute `records` read from `wal`, fetching a marker's missing
/// chunks from it.
fn reconstitute_from(
    wal: &crate::wal::WalManager,
    records: &[WalRecord],
) -> crate::Result<ReconstitutedReplay> {
    let mut fetch = |reference: &RedoStreamRef| wal.fetch_stream_content(reference);
    let mut contents = StreamContents::new(records, &mut fetch);
    reconstitute_redo_records(records, &mut contents, &InstallCursors::default())
}

/// A marker reconstitutes its content's sub-records at its own LSN, from the
/// chunk records its read holds.
#[test]
fn a_stream_marker_reconstitutes_its_content_from_the_read() {
    let marked = marker_wal();
    let records = marked.wal.replay().expect("replay");
    let recon = reconstitute_from(&marked.wal, &records).expect("reconstitute");
    let payloads: Vec<Vec<u8>> = recon.records.iter().map(|r| r.payload.clone()).collect();
    assert_eq!(
        payloads,
        marked
            .ops
            .iter()
            .map(|op| op.payload.clone())
            .collect::<Vec<_>>()
    );
    assert!(recon.records.iter().all(|r| r.header.lsn == marked.marker));
    assert!(recon.grouped_origins.contains(marked.marker));
}

/// A read that starts at the marker lacks the chunks: the fetch reads them
/// from the WAL on disk, and the result matches the whole read's.
#[test]
fn a_stream_marker_past_its_chunks_fetches_them_from_disk() {
    let marked = marker_wal();
    let records = marked
        .wal
        .replay_from(crate::types::Lsn::new(marked.marker))
        .expect("replay from the marker");
    let recon = reconstitute_from(&marked.wal, &records).expect("reconstitute");
    assert_eq!(recon.records.len(), marked.ops.len());
    // With no WAL to fetch from, the same read fails loudly.
    assert!(reconstitute(&records).is_err());
}

/// A marker whose chunks lie below the retained floor reconstitutes no
/// sub-record. With an install cursor at its LSN the install ended part
/// way, so its chunks missing refuses replay.
#[test]
fn a_cursor_marker_without_its_chunks_refuses_replay() {
    let marked = marker_wal();
    let records: Vec<WalRecord> = marked
        .wal
        .replay()
        .expect("replay")
        .into_iter()
        .filter(|record| record.header.lsn == marked.marker)
        .collect();
    let mut below_floor = |_: &RedoStreamRef| -> crate::Result<Option<Vec<u8>>> { Ok(None) };

    let mut contents = StreamContents::new(&records, &mut below_floor);
    let plain = reconstitute_redo_records(&records, &mut contents, &InstallCursors::default())
        .expect("a checkpointed marker reconstitutes nothing");
    assert!(plain.records.is_empty());

    let marker = RedoRecord::from_bytes(&records[0].payload).expect("decode");
    let stream = RedoStreamRef::of_marker(&marker)
        .expect("ref")
        .expect("a marker")
        .stream;
    let cursors = InstallCursors::new([InstallCursor {
        lsn: marked.marker,
        stream,
        state: CursorState::Installing {
            arm: RedoArm::Document,
            batch: 0,
            ops_through: 2,
        },
    }]);
    let mut contents = StreamContents::new(&records, &mut below_floor);
    let error = match reconstitute_redo_records(&records, &mut contents, &cursors) {
        Ok(_) => panic!("a cursor marker without its chunks reconstitutes"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains(&format!("lsn {}", marked.marker)),
        "{error}"
    );
}
