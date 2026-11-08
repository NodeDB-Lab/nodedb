// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::types::{Lsn, RequestId, TraceId, VShardId};
use nodedb_bridge::buffer::RingBuffer;
use nodedb_physical::physical_plan::VectorOp;
use std::time::{Duration, Instant};

struct CoreHarness {
    core: CoreLoop,
    _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
    _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
    _dir: tempfile::TempDir,
}

fn make_core() -> CoreHarness {
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

    let dir = tempfile::tempdir().expect("tempdir");
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        dir.path(),
        std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        crate::data::executor::core_loop::test_governor(),
    )
    .expect("open core");
    CoreHarness {
        core,
        _req_tx: req_tx,
        _resp_rx: resp_rx,
        _dir: dir,
    }
}

/// A task carrying `wal_lsn` so the handler's `note_*_write` calls
/// (gated on `task.wal_lsn().is_some()`) actually fire, mirroring a live
/// write dispatched with an allocated WAL LSN.
fn make_task_with_lsn(lsn: u64) -> ExecutionTask {
    ExecutionTask::new(Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan: PhysicalPlan::Vector(VectorOp::Search {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            query_vector: Vec::new(),
            top_k: 0,
            ef_search: 0,
            metric: nodedb_types::vector_distance::DistanceMetric::L2,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: None,
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
        }),
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: crate::types::ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: Some(Lsn::new(lsn)),
        resolved_now_ms: None,
        commit_hlc: None,
        system_ordinal: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::Read,
        ),
    })
}

#[test]
fn vector_insert_notes_its_write() {
    let mut h = make_core();
    let task = make_task_with_lsn(11);
    let surrogate = Surrogate::new(42);

    let response = h.core.execute_vector_insert(VectorInsertParams {
        task: &task,
        tid: 1,
        collection: "docs",
        vector: &[1.0, 2.0, 3.0],
        dim: 3,
        field_name: "",
        surrogate,
        provenance: None,
    });
    assert_eq!(response.status, crate::bridge::envelope::Status::Ok);

    assert_eq!(
        h.core.watermark,
        Lsn::new(11),
        "vector insert must note its write"
    );
}

fn vector_prov(seq: u64) -> SyncProvenance {
    SyncProvenance {
        producer_id: 7,
        epoch: 1,
        stream_id: 42,
        seq,
    }
}

/// Two bound vectors in `docs`.
fn insert_bound(core: &mut CoreLoop, task: &ExecutionTask) {
    for surrogate in [Surrogate::new(42), Surrogate::new(43)] {
        let response = core.execute_vector_insert(VectorInsertParams {
            task,
            tid: 1,
            collection: "docs",
            vector: &[1.0, 2.0, 3.0],
            dim: 3,
            field_name: "",
            surrogate,
            provenance: None,
        });
        assert_eq!(response.status, crate::bridge::envelope::Status::Ok);
    }
}

fn docs_live_count(core: &CoreLoop) -> usize {
    let key = CoreLoop::vector_index_key(DatabaseId::DEFAULT.as_u64(), 1, "docs", "");
    core.vector_collections
        .get(&key)
        .map(|collection| collection.live_count())
        .unwrap_or(0)
}

#[test]
fn an_insert_without_a_surrogate_is_refused() {
    let mut h = make_core();
    let task = make_task_with_lsn(11);
    let response = h.core.execute_vector_insert(VectorInsertParams {
        task: &task,
        tid: 1,
        collection: "docs",
        vector: &[1.0, 2.0, 3.0],
        dim: 3,
        field_name: "",
        surrogate: Surrogate::ZERO,
        provenance: None,
    });
    assert!(matches!(
        response.error_code.as_deref(),
        Some(ErrorCode::RejectedPrevalidation { .. })
    ));
    assert_eq!(docs_live_count(&h.core), 0, "nothing is stored");
}

#[test]
fn unbound_delete_with_provenance_commits_the_sequence_and_removes_nothing() {
    let mut h = make_core();
    let task = make_task_with_lsn(11);
    insert_bound(&mut h.core, &task);
    assert_eq!(docs_live_count(&h.core), 2);

    let prov = vector_prov(1);
    let response =
        h.core
            .execute_vector_delete_by_surrogate(&task, 1, "docs", None, "", Some(&prov));
    assert_eq!(response.status, crate::bridge::envelope::Status::Ok);
    let ack: nodedb_types::sync::wire::SyncAckResult =
        zerompk::from_msgpack(&response.payload).expect("sync ack");
    assert_eq!(
        ack.outcome,
        nodedb_types::sync::wire::SyncOutcome::Ack(AckStatus::Applied)
    );
    assert_eq!(ack.applied_seq, 1);
    assert_eq!(h.core.sync_hwm_value(7, 42), 1, "the sequence commits");
    assert_eq!(
        docs_live_count(&h.core),
        2,
        "an unbound delete removes no vector"
    );
}

#[test]
fn unbound_delete_without_provenance_is_ok_and_changes_nothing() {
    let mut h = make_core();
    let task = make_task_with_lsn(11);
    insert_bound(&mut h.core, &task);

    let response = h
        .core
        .execute_vector_delete_by_surrogate(&task, 1, "docs", None, "", None);
    assert_eq!(response.status, crate::bridge::envelope::Status::Ok);
    assert!(response.payload.is_empty());
    assert_eq!(docs_live_count(&h.core), 2);
}

#[test]
fn a_deterministic_sync_dimension_refusal_consumes_sequence_without_installing_a_vector() {
    let mut h = make_core();
    let task = make_task_with_lsn(11);
    let prov = vector_prov(1);
    let response = h.core.execute_vector_insert(VectorInsertParams {
        task: &task,
        tid: 1,
        collection: "docs",
        vector: &[1.0],
        dim: 2,
        field_name: "",
        surrogate: Surrogate::new(42),
        provenance: Some(&prov),
    });
    assert!(
        matches!(response.error_code.as_deref(), Some(ErrorCode::SyncRejected {
        applied_seq: 1, provenance, ..
    }) if provenance == &prov)
    );
    assert_eq!(h.core.sync_hwm_value(7, 42), 1);
    assert!(h.core.vector_collections.is_empty());

    let duplicate = h.core.execute_vector_insert(VectorInsertParams {
        task: &task,
        tid: 1,
        collection: "docs",
        vector: &[1.0],
        dim: 2,
        field_name: "",
        surrogate: Surrogate::new(42),
        provenance: Some(&prov),
    });
    let ack: nodedb_types::sync::wire::SyncAckResult =
        zerompk::from_msgpack(&duplicate.payload).unwrap();
    assert_eq!(
        ack.outcome,
        nodedb_types::sync::wire::SyncOutcome::Ack(AckStatus::Duplicate)
    );
    assert_eq!(ack.applied_seq, 1);
    assert!(h.core.vector_collections.is_empty());

    let next = vector_prov(2);
    let response = h.core.execute_vector_insert(VectorInsertParams {
        task: &task,
        tid: 1,
        collection: "docs",
        vector: &[1.0, 2.0],
        dim: 2,
        field_name: "",
        surrogate: Surrogate::new(42),
        provenance: Some(&next),
    });
    let ack: nodedb_types::sync::wire::SyncAckResult =
        zerompk::from_msgpack(&response.payload).unwrap();
    assert_eq!(
        ack.outcome,
        nodedb_types::sync::wire::SyncOutcome::Ack(AckStatus::Applied)
    );
    assert_eq!(ack.applied_seq, 2);
    assert_eq!(docs_live_count(&h.core), 1);
}

#[test]
fn zero_and_declared_dimensions_are_refused_before_sync_index_installation() {
    for (vector, dim, declared) in [(vec![], 0, None), (vec![1.0], 1, Some(2))] {
        let mut h = make_core();
        let task = make_task_with_lsn(11);
        if let Some(dim) = declared {
            let key = CoreLoop::vector_index_key(DatabaseId::DEFAULT.as_u64(), 1, "docs", "");
            h.core.declared_dims.insert(key, dim);
        }
        let prov = vector_prov(1);
        let response = h.core.execute_vector_insert(VectorInsertParams {
            task: &task,
            tid: 1,
            collection: "docs",
            vector: &vector,
            dim,
            field_name: "",
            surrogate: Surrogate::new(42),
            provenance: Some(&prov),
        });
        assert!(matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::SyncRejected { applied_seq: 1, .. })
        ));
        assert_eq!(h.core.sync_hwm_value(7, 42), 1);
        assert!(h.core.vector_collections.is_empty());
    }
}

#[test]
fn a_sync_dimension_refusal_preserves_existing_vector_and_its_history() {
    let mut h = make_core();
    let task = make_task_with_lsn(11);
    insert_bound(&mut h.core, &task);
    let prov = vector_prov(1);
    let response = h.core.execute_vector_insert(VectorInsertParams {
        task: &task,
        tid: 1,
        collection: "docs",
        vector: &[9.0, 9.0],
        dim: 2,
        field_name: "",
        surrogate: Surrogate::new(42),
        provenance: Some(&prov),
    });
    assert!(matches!(
        response.error_code.as_deref(),
        Some(ErrorCode::SyncRejected { applied_seq: 1, .. })
    ));
    assert_eq!(docs_live_count(&h.core), 2);
    let key = CoreLoop::vector_index_key(DatabaseId::DEFAULT.as_u64(), 1, "docs", "");
    assert_eq!(
        h.core.vector_collections[&key].vector_for_surrogate(Surrogate::new(42)),
        Some(vec![1.0, 2.0, 3.0])
    );
}
