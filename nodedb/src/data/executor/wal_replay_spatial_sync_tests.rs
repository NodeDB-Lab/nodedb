// SPDX-License-Identifier: BUSL-1.1

//! Standalone spatial refusals retain their frontier across WAL recovery.

use super::*;
use crate::bridge::envelope::{Admission, Status};
use crate::control::server::wal_dispatch::encode_spatial_put_payload;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::types::{TenantId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, WalManager};
use nodedb_types::QualifiedCollection;
use nodedb_types::geometry::{Geometry, Srid, SridGeometry};
use nodedb_types::sync::wire::{AckStatus, SyncAckResult, SyncOutcome, SyncProvenance};
use nodedb_wal::TombstoneSet;

const PRODUCER: u64 = 11;
const STREAM: u64 = 7;
const COLLECTION: &str = "places";
const FIELD: &str = "loc";

fn provenance(seq: u64) -> SyncProvenance {
    SyncProvenance {
        producer_id: PRODUCER,
        epoch: 1,
        stream_id: STREAM,
        seq,
    }
}

fn geometry(srid: u32) -> SridGeometry {
    SridGeometry::new(Srid::new(srid).unwrap(), Geometry::point(1.0, 2.0))
}

fn append(
    wal: &WalManager,
    surrogate: Surrogate,
    geometry: &SridGeometry,
    provenance: &SyncProvenance,
) -> crate::types::Lsn {
    let payload = encode_spatial_put_payload(COLLECTION, FIELD, surrogate, geometry, provenance)
        .unwrap()
        .to_bytes()
        .unwrap();
    wal.appender(NO_APPLY_KEY)
        .with_event_source(crate::event::EventSource::CrdtSync)
        .append_spatial_put(
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &payload,
        )
        .unwrap()
}

fn count(core: &CoreLoop) -> usize {
    core.spatial_indexes
        .get(&(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            COLLECTION.to_owned(),
            FIELD.to_owned(),
        ))
        .map_or(0, crate::engine::spatial::RTree::len)
}

#[test]
fn standalone_spatial_refusals_and_the_next_geometry_survive_two_reopens() {
    for case in ["unbound", "undefined", "mixed"] {
        let directory = tempfile::tempdir().unwrap();
        let wal_path = directory.path().join("spatial.wal");
        let core_path = directory.path().join("core");
        let (surrogate, invalid) = match case {
            "unbound" => (Surrogate::ZERO, geometry(4326)),
            "undefined" => (Surrogate::new(9), geometry(999)),
            "mixed" => (Surrogate::new(9), geometry(3857)),
            _ => unreachable!(),
        };
        let initial = usize::from(case == "mixed");
        {
            let wal = WalManager::open_for_testing(&wal_path).unwrap();
            if initial != 0 {
                append(
                    &wal,
                    Surrogate::new(5),
                    &geometry(4326),
                    &SyncProvenance::default(),
                );
            }
            append(&wal, surrogate, &invalid, &provenance(1));
            wal.sync().unwrap();
        }
        {
            let wal = WalManager::open_for_testing(&wal_path).unwrap();
            let records = wal.replay().unwrap();
            assert_eq!(
                records.len(),
                initial + 1,
                "crash precedes abort and frontier markers"
            );
            let (mut core, _requests, _responses) = make_core_with_dir(&core_path);
            core.replay_all_wal(&records, 1, &TombstoneSet::new())
                .unwrap();
            assert_eq!(core.sync_hwm_value(PRODUCER, STREAM), 1);
            assert_eq!(count(&core), initial);
            let valid = geometry(4326);
            let lsn = append(&wal, Surrogate::new(10), &valid, &provenance(2));
            let mut request = make_default_task().request;
            request.plan = PhysicalPlan::Spatial(SpatialOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                field: FIELD.to_owned(),
                surrogate: Surrogate::new(10),
                geometry: valid,
                provenance: Some(provenance(2)),
            });
            request.admission = Admission::Admitted;
            request.event_source = crate::event::EventSource::CrdtSync;
            let response = core.execute(&ExecutionTask::with_wal_lsn(request, Some(lsn)));
            assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
            let ack: SyncAckResult = zerompk::from_msgpack(&response.payload).unwrap();
            assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Applied));
            assert_eq!(ack.applied_seq, 2);
            wal.sync().unwrap();
        }
        {
            let wal = WalManager::open_for_testing(&wal_path).unwrap();
            let records = wal.replay().unwrap();
            let (mut core, _requests, _responses) = make_core_with_dir(&core_path);
            core.replay_all_wal(&records, 1, &TombstoneSet::new())
                .unwrap();
            assert_eq!(core.sync_hwm_value(PRODUCER, STREAM), 2);
            assert_eq!(count(&core), initial + 1);
        }
    }
}

#[test]
fn invalid_local_spatial_records_remain_fatal_on_reopen() {
    for (surrogate, geometry) in [
        (Surrogate::ZERO, geometry(4326)),
        (Surrogate::new(9), geometry(999)),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let wal_path = directory.path().join("spatial.wal");
        {
            let wal = WalManager::open_for_testing(&wal_path).unwrap();
            append(&wal, surrogate, &geometry, &SyncProvenance::default());
            wal.sync().unwrap();
        }
        let wal = WalManager::open_for_testing(&wal_path).unwrap();
        let records = wal.replay().unwrap();
        let (mut core, _requests, _responses) = make_core_with_dir(&directory.path().join("core"));
        assert!(
            core.replay_all_wal(&records, 1, &TombstoneSet::new())
                .is_err()
        );
        assert_eq!(count(&core), 0);
    }
}
