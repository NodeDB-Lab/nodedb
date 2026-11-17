// SPDX-License-Identifier: BUSL-1.1

use super::*;
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
    reconstitute_redo_records(records, &mut contents)
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
        sum_targets: Vec::new(),
    };
    // The transaction committed at LSN 50; an autocommit overwrote the same
    // key at LSN 100.
    let standalone = vec![
        redo_wal_record(50, 0, 0, &redo),
        put_wal_record(100, vec![0xBB]),
    ];
    let redo_ops = reconstitute(&standalone).expect("well-formed redo").records;
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
        sum_targets: Vec::new(),
    };
    let standalone = vec![redo_wal_record(7, 0, 0, &redo)];
    let redo_ops = reconstitute(&standalone).expect("well-formed redo").records;
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
        sum_targets: Vec::new(),
    };
    let standalone = vec![
        put_wal_record(1, vec![1]),
        redo_wal_record(2, 0, 0, &redo),
        put_wal_record(3, vec![3]),
    ];
    let redo_ops = reconstitute(&standalone).expect("well-formed redo").records;
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
    reconstitute_redo_records(records, &mut contents)
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
