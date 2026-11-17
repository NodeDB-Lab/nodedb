// SPDX-License-Identifier: BUSL-1.1

//! Sync vector inputs retained before refusal markers must replay their outcome.

use super::*;
use crate::bridge::envelope::{Admission, Status};
use crate::control::server::wal_dispatch::encode_vector_put_payload;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, WalManager};
use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::sync::wire::{AckStatus, SyncAckResult, SyncOutcome, SyncProvenance};
use nodedb_types::{QualifiedCollection, Surrogate};
use nodedb_wal::{TombstoneSet, record::RecordType};

const COLLECTION: &str = "vectors";
const PRODUCER: u64 = 11;
const STREAM: u64 = 7;

fn provenance(seq: u64) -> SyncProvenance {
    SyncProvenance {
        producer_id: PRODUCER,
        epoch: 1,
        stream_id: STREAM,
        seq,
    }
}

fn append(
    wal: &WalManager,
    vector: &[f32],
    dim: usize,
    prov: Option<&SyncProvenance>,
) -> crate::types::Lsn {
    let payload =
        encode_vector_put_payload(COLLECTION, vector, dim, "", Surrogate::new(9), prov).unwrap();
    wal.appender(NO_APPLY_KEY)
        .with_event_source(crate::event::EventSource::CrdtSync)
        .append_vector_put(
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &payload,
        )
        .unwrap()
}

fn seed_declared_dimension(core: &mut CoreLoop, declared: Option<usize>) {
    if let Some(dim) = declared {
        let key = CoreLoop::vector_index_key(0, 1, COLLECTION, "");
        core.declared_dims.insert(key, dim);
    }
}

fn apply_next(core: &mut CoreLoop, lsn: crate::types::Lsn) {
    let mut request = make_default_task().request;
    request.plan = PhysicalPlan::Vector(VectorOp::Insert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        vector: vec![2.0, 3.0],
        dim: 2,
        field_name: String::new(),
        surrogate: Surrogate::new(9),
        pk_bytes: None,
        provenance: Some(provenance(2)),
    });
    request.admission = Admission::Admitted;
    request.event_source = crate::event::EventSource::CrdtSync;
    let response = core.execute(&ExecutionTask::with_wal_lsn(request, Some(lsn)));
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    let ack: SyncAckResult = zerompk::from_msgpack(&response.payload).unwrap();
    assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Applied));
    assert_eq!(ack.applied_seq, 2);
}

#[test]
fn an_uncancelled_sync_refusal_replays_and_the_next_vector_survives_another_restart() {
    for (vector, dim, declared) in [
        (vec![1.0], 2, None),
        (vec![], 0, None),
        (vec![1.0], 1, Some(2)),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let wal_path = directory.path().join("vectors.wal");
        let core_path = directory.path().join("core");
        {
            let wal = WalManager::open_for_testing(&wal_path).unwrap();
            append(&wal, &vector, dim, Some(&provenance(1)));
            wal.sync().unwrap();
        }
        {
            let wal = WalManager::open_for_testing(&wal_path).unwrap();
            let records = wal.replay().unwrap();
            assert_eq!(records.len(), 1, "crash precedes abort and HWM markers");
            assert_eq!(
                records[0].logical_record_type(),
                RecordType::VectorPut as u32
            );
            let (mut core, _requests, _responses) = make_core_with_dir(&core_path);
            seed_declared_dimension(&mut core, declared);
            core.replay_all_wal(&records, 1, &TombstoneSet::new())
                .unwrap();
            assert_eq!(core.sync_hwm_value(PRODUCER, STREAM), 1);
            assert!(core.vector_collections.is_empty());
            let lsn = append(&wal, &[2.0, 3.0], 2, Some(&provenance(2)));
            apply_next(&mut core, lsn);
            wal.sync().unwrap();
        }
        {
            let wal = WalManager::open_for_testing(&wal_path).unwrap();
            let records = wal.replay().unwrap();
            assert_eq!(records.len(), 2);
            let (mut core, _requests, _responses) = make_core_with_dir(&core_path);
            seed_declared_dimension(&mut core, declared);
            core.replay_all_wal(&records, 1, &TombstoneSet::new())
                .unwrap();
            assert_eq!(core.sync_hwm_value(PRODUCER, STREAM), 2);
            let key = CoreLoop::vector_index_key(0, 1, COLLECTION, "");
            assert_eq!(core.vector_collections[&key].live_count(), 1);
            assert_eq!(
                core.vector_collections[&key].vector_for_surrogate(Surrogate::new(9)),
                Some(vec![2.0, 3.0])
            );
        }
    }
}

#[test]
fn a_dimension_mismatch_without_sync_provenance_remains_corrupt() {
    let directory = tempfile::tempdir().unwrap();
    let wal = WalManager::open_for_testing(&directory.path().join("vectors.wal")).unwrap();
    append(&wal, &[1.0], 2, None);
    wal.sync().unwrap();
    let records = wal.replay().unwrap();
    let (mut core, _requests, _responses) = make_core_with_dir(&directory.path().join("core"));
    assert!(
        core.replay_all_wal(&records, 1, &TombstoneSet::new())
            .is_err()
    );
    assert!(core.vector_collections.is_empty());
}

#[test]
fn a_sync_vector_proposal_keeps_its_admitted_insert_shape() {
    use crate::control::wal_replication::{ReplicableWrite, ReplicatedWrite, to_replicated_entry};

    let plan = PhysicalPlan::Vector(VectorOp::Insert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        vector: vec![1.0],
        dim: 2,
        field_name: String::new(),
        surrogate: Surrogate::new(9),
        pk_bytes: None,
        provenance: Some(provenance(1)),
    });
    let replicable = ReplicableWrite::decide_for_replication(&plan).unwrap();
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &replicable,
    )
    .unwrap()
    .unwrap();
    assert!(
        matches!(entry.write, ReplicatedWrite::VectorInsert { dim: 2, vector, provenance: Some(_), .. } if vector == [1.0])
    );
}
