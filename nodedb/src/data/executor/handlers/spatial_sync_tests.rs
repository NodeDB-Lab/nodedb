// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::sync::wire::{SyncAckResult, SyncOutcome};

use super::*;
use crate::bridge::envelope::Status;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use nodedb_types::geometry::Geometry;

const TID: u64 = 1;

fn prov(seq: u64) -> SyncProvenance {
    SyncProvenance {
        producer_id: 7,
        epoch: 1,
        stream_id: 42,
        seq,
    }
}

fn assert_applied(response: &Response, seq: u64) {
    assert_eq!(response.status, Status::Ok);
    let ack: SyncAckResult = zerompk::from_msgpack(&response.payload).expect("sync ack");
    assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Applied));
    assert_eq!(ack.applied_seq, seq);
}

fn insert(core: &mut CoreLoop, task: &ExecutionTask, surrogate: Surrogate) -> Response {
    core.execute_spatial_insert(SpatialInsertExec {
        task,
        tid: TID,
        collection: "places",
        field: "loc",
        surrogate,
        geometry: &SridGeometry::wgs84(Geometry::point(10.0, 20.0)),
        provenance: None,
    })
}

/// One geometry under a bound surrogate.
fn insert_geometries(core: &mut CoreLoop, task: &ExecutionTask) {
    assert_eq!(insert(core, task, Surrogate::new(5)).status, Status::Ok);
}

/// The indexed entry count and the doc-map size of `places.loc`.
fn spatial_state(core: &CoreLoop) -> (usize, usize) {
    let key = (
        nodedb_types::DatabaseId::DEFAULT,
        TenantId::new(TID),
        "places".to_string(),
        "loc".to_string(),
    );
    let indexed = core.spatial_indexes.get(&key).map_or(0, RTree::len);
    (indexed, core.spatial_doc_map.len())
}

#[test]
fn an_insert_without_a_surrogate_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();

    let response = insert(&mut core, &task, Surrogate::ZERO);
    assert!(matches!(
        response.error_code.as_deref(),
        Some(crate::bridge::envelope::ErrorCode::RejectedPrevalidation { .. })
    ));
    assert_eq!(spatial_state(&core), (0, 0), "nothing is stored");
}

#[test]
fn unbound_delete_with_provenance_commits_the_sequence_and_removes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();
    insert_geometries(&mut core, &task);
    let before = spatial_state(&core);

    let response = core.execute_spatial_delete(&task, TID, "places", "loc", None, Some(&prov(1)));
    assert_applied(&response, 1);
    assert_eq!(core.sync_hwm_value(7, 42), 1, "the sequence commits");
    assert_eq!(spatial_state(&core), before, "no geometry is removed");
}

#[test]
fn unbound_delete_without_provenance_is_ok_and_removes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();
    insert_geometries(&mut core, &task);
    let before = spatial_state(&core);

    let response = core.execute_spatial_delete(&task, TID, "places", "loc", None, None);
    assert_eq!(response.status, Status::Ok);
    assert!(response.payload.is_empty());
    assert_eq!(spatial_state(&core), before);
}

fn synced_insert(
    core: &mut CoreLoop,
    task: &ExecutionTask,
    surrogate: Surrogate,
    geometry: &SridGeometry,
    provenance: &SyncProvenance,
) -> Response {
    core.execute_spatial_insert(SpatialInsertExec {
        task,
        tid: TID,
        collection: "places",
        field: "loc",
        surrogate,
        geometry,
        provenance: Some(provenance),
    })
}

#[test]
fn deterministic_spatial_refusals_consume_then_admit_the_next_sequence() {
    use nodedb_types::geometry::Srid;
    for case in ["unbound", "undefined", "dropped", "mixed"] {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let valid = SridGeometry::wgs84(Geometry::point(10.0, 20.0));
        let mut geometry = valid.clone();
        let mut surrogate = Surrogate::new(9);
        let reason = match case {
            "unbound" => {
                surrogate = Surrogate::ZERO;
                "surrogate"
            }
            "undefined" => {
                geometry.srid = Srid::new(999).unwrap();
                "SRID 999 is not defined"
            }
            "dropped" => {
                geometry.srid = Srid::new(910_001).unwrap();
                core.crs_set = std::sync::Arc::new(
                    nodedb_crs::CustomCrsSet::from_entries(vec![nodedb_crs::CustomCrs {
                        srid: geometry.srid,
                        state: nodedb_crs::CustomCrsState::Dropped,
                    }])
                    .unwrap(),
                );
                "SRID 910001 was dropped"
            }
            "mixed" => {
                insert_geometries(&mut core, &task);
                geometry.srid = Srid::new(3857).unwrap();
                "3857"
            }
            _ => unreachable!(),
        };
        let before = spatial_state(&core);
        let response = synced_insert(&mut core, &task, surrogate, &geometry, &prov(1));
        assert!(
            matches!(response.error_code.as_deref(), Some(crate::bridge::envelope::ErrorCode::SyncRejected { applied_seq: 1, provenance, violation: nodedb_types::sync::violation::ViolationType::SchemaViolation { reason: detail, .. } }) if provenance == &prov(1) && detail.contains(reason)),
            "{case}: {:?}",
            response.error_code
        );
        assert_eq!(core.sync_hwm_value(7, 42), 1);
        assert_eq!(
            spatial_state(&core),
            before,
            "{case} changes no index state"
        );
        assert!(
            core.sparse
                .get(
                    0,
                    TID,
                    "places",
                    &nodedb_types::StorageKey::for_surrogate(surrogate)
                )
                .unwrap()
                .is_none(),
            "{case} writes no sparse body"
        );
        let next = synced_insert(&mut core, &task, Surrogate::new(9), &valid, &prov(2));
        assert_applied(&next, 2);
        assert_eq!(spatial_state(&core), (before.0 + 1, before.1 + 1));
    }
}

#[test]
fn duplicate_gap_and_fenced_statuses_precede_spatial_validation() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();
    let geometry = SridGeometry::new(
        nodedb_types::geometry::Srid::new(999).unwrap(),
        Geometry::point(1.0, 2.0),
    );
    let gap = synced_insert(&mut core, &task, Surrogate::ZERO, &geometry, &prov(2));
    let ack: SyncAckResult = zerompk::from_msgpack(&gap.payload).unwrap();
    assert_eq!(
        ack.outcome,
        SyncOutcome::Ack(AckStatus::Gap { expected: 1 })
    );
    assert_eq!(ack.applied_seq, 0);
    let valid = SridGeometry::wgs84(Geometry::point(1.0, 2.0));
    let mut current = prov(1);
    current.epoch = 2;
    assert_applied(
        &synced_insert(&mut core, &task, Surrogate::new(5), &valid, &current),
        1,
    );
    let duplicate = synced_insert(&mut core, &task, Surrogate::ZERO, &geometry, &current);
    let ack: SyncAckResult = zerompk::from_msgpack(&duplicate.payload).unwrap();
    assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Duplicate));
    assert_eq!(ack.applied_seq, 1);
    let fenced = synced_insert(&mut core, &task, Surrogate::ZERO, &geometry, &prov(2));
    let ack: SyncAckResult = zerompk::from_msgpack(&fenced.payload).unwrap();
    assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Fenced));
    assert_eq!(ack.applied_seq, 1);
    assert_eq!(core.sync_hwm_value(7, 42), 1);
    assert_eq!(spatial_state(&core), (1, 1));
}

#[test]
fn local_spatial_validation_errors_do_not_become_sync_refusals() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let task = make_default_task();
    let geometry = SridGeometry::new(
        nodedb_types::geometry::Srid::new(999).unwrap(),
        Geometry::point(1.0, 2.0),
    );
    let response = synced_insert(
        &mut core,
        &task,
        Surrogate::new(9),
        &geometry,
        &SyncProvenance::default(),
    );
    assert_eq!(response.status, Status::Error);
    assert!(!matches!(
        response.error_code.as_deref(),
        Some(crate::bridge::envelope::ErrorCode::SyncRejected { .. })
    ));
    assert_eq!(spatial_state(&core), (0, 0));
}
