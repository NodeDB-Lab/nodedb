// SPDX-License-Identifier: BUSL-1.1

//! A refused sync sub-record rolls back its whole committed redo group.

use crate::bridge::envelope::{ErrorCode, Status};
use crate::control::server::wal_dispatch::{encode_text_op_record, encode_vector_put_payload};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::types::Lsn;
use crate::wal::{RedoRecord, RedoSubRecord};
use nodedb_physical::physical_plan::TextOp;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate, TenantId};
use nodedb_wal::record::RecordType;

use super::CommittedRedo;

const PRODUCER: u64 = 71;
const STREAM: u64 = 42;

fn provenance() -> SyncProvenance {
    SyncProvenance {
        producer_id: PRODUCER,
        epoch: 1,
        stream_id: STREAM,
        seq: 1,
    }
}

fn text_record(surrogate: Surrogate, provenance: Option<SyncProvenance>) -> RedoSubRecord {
    let op = TextOp::FtsIndexDoc {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "articles"),
        surrogate,
        fields: vec![("body".to_owned(), "searchable words".to_owned())],
        provenance,
    };
    let (kind, payload) = encode_text_op_record(&op).unwrap().unwrap();
    RedoSubRecord {
        record_type: kind as u32,
        payload,
    }
}

fn sibling_vector() -> RedoSubRecord {
    let payload =
        encode_vector_put_payload("vectors", &[1.0, 2.0], 2, "", Surrogate::new(92), None).unwrap();
    RedoSubRecord {
        record_type: RecordType::VectorPut as u32,
        payload,
    }
}

fn refused_vector() -> RedoSubRecord {
    let payload = encode_vector_put_payload(
        "vectors",
        &[],
        0,
        "",
        Surrogate::new(9),
        Some(&provenance()),
    )
    .unwrap();
    RedoSubRecord {
        record_type: RecordType::VectorPut as u32,
        payload,
    }
}

fn assert_group_rolls_back(refused: RedoSubRecord) {
    let directory = tempfile::tempdir().unwrap();
    let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
    let redo = RedoRecord {
        version: 1,
        ops: vec![
            text_record(Surrogate::new(91), None),
            sibling_vector(),
            refused,
        ],
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
    }
    .to_bytes()
    .unwrap();
    let mut task = make_default_task();
    task.wal_lsn = Some(Lsn::new(210));
    let response = core.execute_apply_transaction_redo(
        &task,
        1,
        CommittedRedo {
            redo: &redo,
            collections: &[
                "articles".to_owned(),
                "vectors".to_owned(),
                "places".to_owned(),
            ],
            sum_targets: &[],
        },
    );
    assert_eq!(response.status, Status::Error);
    assert!(
        matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::RetryableRefusal { .. })
        ),
        "{:?}",
        response.error_code
    );
    assert_eq!(
        core.inverted
            .corpus_stats(0, TenantId::new(1), "articles")
            .unwrap()
            .0,
        0,
        "the preceding sibling write rolls back"
    );
    assert_eq!(
        core.sync_hwm_value(PRODUCER, STREAM),
        0,
        "the refused sub-record cannot consume outside its group"
    );
    assert_eq!(
        core.sync_classify(&provenance()),
        crate::data::executor::sync_gate::SyncAdmit::Apply
    );
    let key = CoreLoop::vector_index_key(0, 1, "vectors", "");
    assert!(!core.vector_collections.contains_key(&key));
}

#[test]
fn a_refused_sync_text_sub_record_rolls_back_its_sibling_and_frontier() {
    assert_group_rolls_back(text_record(Surrogate::ZERO, Some(provenance())));
}

#[test]
fn a_refused_sync_vector_sub_record_rolls_back_its_sibling_and_frontier() {
    assert_group_rolls_back(refused_vector());
}

fn append_group(wal: &crate::wal::manager::WalManager, refused: RedoSubRecord, write_group: bool) {
    use crate::types::VShardId;
    use crate::wal::{WriteGroup, WriteGroupRecord};
    let appender = wal
        .appender(crate::wal::manager::NO_APPLY_KEY)
        .with_event_source(crate::event::EventSource::CrdtSync);
    let sibling = text_record(Surrogate::new(91), None);
    if write_group {
        let opening = appender
            .append_write_group(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                &WriteGroupRecord {
                    schemas: Vec::new(),
                    group: WriteGroup::OPENING,
                    ops: Vec::new(),
                    redo: None,
                },
            )
            .unwrap();
        for (index, op) in [sibling, sibling_vector(), refused].into_iter().enumerate() {
            appender
                .append_write_group(
                    TenantId::new(1),
                    VShardId::new(0),
                    DatabaseId::DEFAULT,
                    &WriteGroupRecord {
                        schemas: Vec::new(),
                        group: WriteGroup::part_of(opening.as_u64(), index as u32 + 1, 3),
                        ops: vec![op],
                        redo: None,
                    },
                )
                .unwrap();
        }
    } else {
        appender
            .append_whole_transaction_redo(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                &RedoRecord {
                    version: 1,
                    ops: vec![sibling, sibling_vector(), refused],
                    calvin_stamp: None,
                    cross_shard_applied: None,
                    row_sources: Vec::new(),
                    publishes: Vec::new(),
                    row_changes: Vec::new(),
                    user_roles: Vec::new(),
                },
            )
            .unwrap();
    }
    wal.sync().unwrap();
}

fn assert_reopened_group_refuses_to_serve(refused: RedoSubRecord, write_group: bool) {
    use crate::wal::manager::WalManager;
    let directory = tempfile::tempdir().unwrap();
    let wal_path = directory.path().join("grouped.wal");
    {
        let wal = WalManager::open_for_testing(&wal_path).unwrap();
        append_group(&wal, refused, write_group);
    }
    let wal = WalManager::open_for_testing(&wal_path).unwrap();
    let records = wal.replay().unwrap();
    assert_eq!(
        records[0].logical_record_type(),
        if write_group {
            RecordType::WriteGroup
        } else {
            RecordType::TransactionRedo
        } as u32
    );
    let (mut core, _requests, _responses) = make_core_with_dir(&directory.path().join("core"));
    assert!(
        core.replay_all_wal(&records, 1, &nodedb_wal::TombstoneSet::new())
            .is_err(),
        "a grouped refusal cannot become a skipped standalone proposal"
    );
    assert!(
        core.is_fail_stopped(),
        "recovery cannot serve a partial group"
    );
    assert_eq!(
        core.sync_hwm_value(PRODUCER, STREAM),
        0,
        "the refused operation's complete frontier undo restores its prior state"
    );
}

#[test]
fn reopened_transaction_and_write_groups_refuse_invalid_sync_text() {
    for write_group in [false, true] {
        assert_reopened_group_refuses_to_serve(
            text_record(Surrogate::ZERO, Some(provenance())),
            write_group,
        );
    }
}

#[test]
fn reopened_transaction_and_write_groups_refuse_invalid_sync_vectors() {
    for write_group in [false, true] {
        assert_reopened_group_refuses_to_serve(refused_vector(), write_group);
    }
}

fn refused_spatial(unbound: bool) -> RedoSubRecord {
    let surrogate = if unbound {
        Surrogate::ZERO
    } else {
        Surrogate::new(9)
    };
    let geometry = nodedb_types::geometry::SridGeometry::new(
        nodedb_types::geometry::Srid::new(if unbound { 4326 } else { 999 }).unwrap(),
        nodedb_types::geometry::Geometry::point(1.0, 2.0),
    );
    let payload = crate::control::server::wal_dispatch::encode_spatial_put_payload(
        "places",
        "loc",
        surrogate,
        &geometry,
        &provenance(),
    )
    .unwrap()
    .to_bytes()
    .unwrap();
    RedoSubRecord {
        record_type: RecordType::SpatialPut as u32,
        payload,
    }
}

#[test]
fn a_refused_sync_spatial_sub_record_rolls_back_its_siblings_and_frontier() {
    for unbound in [false, true] {
        assert_group_rolls_back(refused_spatial(unbound));
    }
}

#[test]
fn reopened_transaction_and_write_groups_refuse_invalid_sync_spatial_entries() {
    for write_group in [false, true] {
        for unbound in [false, true] {
            assert_reopened_group_refuses_to_serve(refused_spatial(unbound), write_group);
        }
    }
}
