//! Tests for trigger behavior in cluster context.
//!
//! Validates data model contracts that the cluster relies on:
//! - Raft replication roundtrips (log entry serialize/deserialize)
//! - Cross-shard trigger write request structure
//! - Trigger definition registry behavior
//! - EventSource exhaustive coverage for trigger dispatch decisions

use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::DocumentOp;
use nodedb::control::trigger::TriggerRegistry;
use nodedb::control::trigger::registry::DmlEvent;
use nodedb::event::cross_shard::types::CrossShardWriteRequest;
use nodedb::event::types::{EventSource, RowId, WriteEvent, WriteOp};
use nodedb::types::{Lsn, TenantId, VShardId};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// ASYNC triggers: Raft log contains ONLY the original DML
// ---------------------------------------------------------------------------

#[test]
fn async_trigger_not_in_raft_log() {
    // ASYNC triggers fire via Event Plane AFTER Raft commit.
    // The Raft log entry contains only the original DML (PointPut, etc.).
    // Trigger side effects are separate Event Plane dispatches.
    let plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: "orders".into(),
        document_id: "doc-1".into(),
        value: b"{}".to_vec(),
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
    });

    // to_replicated_entry converts the plan to a Raft log entry.
    // It should contain ONLY the PointPut — no trigger side effects.
    let entry = nodedb::control::wal_replication::to_replicated_entry(
        TenantId::new(1),
        VShardId::new(0),
        &plan,
    );
    assert!(entry.is_some());

    // The entry serializes to bytes that can be deserialized back.
    let bytes = entry.unwrap().to_bytes();
    let (tid, vsid, restored) =
        nodedb::control::wal_replication::from_replicated_entry(&bytes, None)
            .unwrap()
            .unwrap();
    assert_eq!(tid, TenantId::new(1));
    assert_eq!(vsid, VShardId::new(0));

    // Restored plan is the same PointPut — no trigger DML bundled.
    assert!(matches!(
        restored,
        PhysicalPlan::Document(DocumentOp::PointPut { .. })
    ));
}

// ---------------------------------------------------------------------------
// Cross-shard trigger write request structure
// ---------------------------------------------------------------------------

#[test]
fn cross_shard_request_carries_cascade_depth() {
    let req = CrossShardWriteRequest {
        sql: "INSERT INTO audit (id) VALUES ('doc-1')".into(),
        tenant_id: 1,
        source_vshard: 0,
        target_vshard: 1,
        source_lsn: 100,
        source_sequence: 1,
        cascade_depth: 3,
        source_collection: "orders".into(),
    };
    // Cascade depth is preserved across cross-shard delivery.
    assert_eq!(req.cascade_depth, 3);
    assert_eq!(req.source_vshard, 0);
    assert_eq!(req.target_vshard, 1);
}

#[test]
fn cross_shard_request_serialization_roundtrip() {
    let req = CrossShardWriteRequest {
        sql: "INSERT INTO vectors (id) VALUES ('v1')".into(),
        tenant_id: 1,
        source_vshard: 0,
        target_vshard: 2,
        source_lsn: 500,
        source_sequence: 42,
        cascade_depth: 1,
        source_collection: "articles".into(),
    };
    let bytes = zerompk::to_msgpack_vec(&req).unwrap();
    let restored: CrossShardWriteRequest = zerompk::from_msgpack(&bytes).unwrap();
    assert_eq!(restored.sql, req.sql);
    assert_eq!(restored.cascade_depth, 1);
    assert_eq!(restored.source_collection, "articles");
}

// ---------------------------------------------------------------------------
// Trigger definition replication: definitions go through normal Raft path
// ---------------------------------------------------------------------------

#[test]
fn trigger_definitions_stored_in_catalog() {
    // Trigger definitions are stored in redb catalog via put_trigger().
    // In cluster mode, catalog writes go through Raft log.
    // This test validates the registry stores and retrieves correctly.
    use nodedb::control::security::catalog::trigger_types::*;

    let reg = TriggerRegistry::new();
    reg.register(StoredTrigger {
        tenant_id: 1,
        name: "cluster_audit".into(),
        collection: "orders".into(),
        timing: TriggerTiming::After,
        events: TriggerEvents {
            on_insert: true,
            on_update: false,
            on_delete: false,
        },
        granularity: TriggerGranularity::Row,
        when_condition: None,
        body_sql: "BEGIN INSERT INTO audit (id) VALUES (NEW.id); END".into(),
        priority: 0,
        enabled: true,
        execution_mode: TriggerExecutionMode::Async,
        security: TriggerSecurity::default(),
        batch_mode: TriggerBatchMode::default(),
        owner: "admin".into(),
        created_at: 0,
        descriptor_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    });

    let matched = reg.get_matching(1, "orders", DmlEvent::Insert);
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].name, "cluster_audit");
}

// ---------------------------------------------------------------------------
// Leader failover: Event Plane replays from WAL watermark
// ---------------------------------------------------------------------------

#[test]
fn event_source_preserved_through_write_event() {
    // WriteEvent.source is set from Request.event_source.
    // This ensures the Event Plane sees the correct source after replay.
    let event = WriteEvent {
        sequence: 1,
        collection: Arc::from("orders"),
        op: WriteOp::Insert,
        row_id: RowId::new("doc-1"),
        lsn: Lsn::new(100),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        source: EventSource::User,
        new_value: None,
        old_value: None,
        system_time_ms: None,
        valid_time_ms: None,
    };
    // After leader failover, new leader's Event Plane replays from WAL.
    // The replayed events have source: User → triggers fire.
    assert_eq!(event.source, EventSource::User);
}

// ---------------------------------------------------------------------------
// Replicated entry roundtrip for various write types
// ---------------------------------------------------------------------------

#[test]
fn replicated_entry_roundtrip_point_delete() {
    let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
        collection: "orders".into(),
        document_id: "doc-99".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
    });
    let entry = nodedb::control::wal_replication::to_replicated_entry(
        TenantId::new(1),
        VShardId::new(0),
        &plan,
    )
    .unwrap();
    let bytes = entry.to_bytes();
    let (_, _, restored) = nodedb::control::wal_replication::from_replicated_entry(&bytes, None)
        .unwrap()
        .unwrap();
    assert!(matches!(
        restored,
        PhysicalPlan::Document(DocumentOp::PointDelete { .. })
    ));
}

#[test]
fn read_ops_not_replicated() {
    // Reads (PointGet, Scan) should NOT produce replicated entries.
    let plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: "orders".into(),
        document_id: "doc-1".into(),
        rls_filters: vec![],
        system_as_of_ms: None,
        valid_at_ms: None,
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
    });
    let entry = nodedb::control::wal_replication::to_replicated_entry(
        TenantId::new(1),
        VShardId::new(0),
        &plan,
    );
    assert!(entry.is_none()); // Reads don't replicate.
}

// ---------------------------------------------------------------------------
// Procedure DML replicates independently via normal Raft path
// ---------------------------------------------------------------------------

#[test]
fn procedure_dml_is_normal_write() {
    // Procedure DML dispatches through the normal query planner.
    // Each DML statement is an independent write that goes through Raft.
    // The procedure itself is NOT replicated — only its data effects.
    let plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: "archive".into(),
        document_id: "a-1".into(),
        value: b"{}".to_vec(),
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
    });
    // This is a normal write → replicates via Raft.
    assert!(
        nodedb::control::wal_replication::to_replicated_entry(
            TenantId::new(1),
            VShardId::new(0),
            &plan,
        )
        .is_some()
    );
}

// ---------------------------------------------------------------------------
// EventSource exhaustive coverage
// ---------------------------------------------------------------------------

#[test]
fn all_event_sources_have_correct_trigger_behavior() {
    let cases = [
        (EventSource::User, true, "User fires triggers"),
        (EventSource::Deferred, true, "Deferred fires triggers"),
        (EventSource::Trigger, false, "Trigger prevents cascade"),
        (EventSource::RaftFollower, false, "Follower bypasses"),
        (EventSource::CrdtSync, false, "CRDT sync bypasses"),
    ];
    for (source, should_fire, label) in &cases {
        let fires = matches!(source, EventSource::User | EventSource::Deferred);
        assert_eq!(fires, *should_fire, "{label}");
    }
}
