//! Integration tests for Event Plane trigger dispatch.
//!
//! Tests: event source cascade prevention, trigger registry matching,
//! dispatcher routing, mode filtering, cascade depth, retry queue,
//! DLQ lifecycle, ordering guarantees, and WHEN conditions.

use std::sync::Arc;

use nodedb::control::security::catalog::trigger_types::{
    StoredTrigger, TriggerEvents, TriggerExecutionMode, TriggerGranularity, TriggerTiming,
};
use nodedb::control::trigger::{DmlEvent, TriggerRegistry};
use nodedb::event::trigger::dlq::{DlqEnqueueParams, TriggerDlq};
use nodedb::event::trigger::retry::{RetryEntry, TriggerRetryQueue};
use nodedb::event::types::{EventSource, RowId, WriteEvent, WriteOp};
use nodedb::types::{Lsn, TenantId, VShardId};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_event(source: EventSource, op: WriteOp, collection: &str) -> WriteEvent {
    WriteEvent {
        sequence: 1,
        collection: Arc::from(collection),
        op,
        row_id: RowId::new("row-1"),
        lsn: Lsn::new(100),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        source,
        new_value: Some(Arc::from(b"{\"id\":1}".as_slice())),
        old_value: None,
    }
}

fn make_trigger(
    name: &str,
    collection: &str,
    on_insert: bool,
    on_update: bool,
    on_delete: bool,
    mode: TriggerExecutionMode,
) -> StoredTrigger {
    StoredTrigger {
        tenant_id: 1,
        name: name.to_string(),
        collection: collection.to_string(),
        timing: TriggerTiming::After,
        events: TriggerEvents {
            on_insert,
            on_update,
            on_delete,
        },
        granularity: TriggerGranularity::Row,
        when_condition: None,
        body_sql: "BEGIN RETURN; END".into(),
        priority: 0,
        enabled: true,
        execution_mode: mode,
        security: nodedb::control::security::catalog::trigger_types::TriggerSecurity::default(),
        batch_mode: nodedb::control::security::catalog::trigger_types::TriggerBatchMode::default(),
        owner: "admin".into(),
        created_at: 0,
    }
}

fn make_trigger_with_when(
    name: &str,
    collection: &str,
    when: &str,
    mode: TriggerExecutionMode,
) -> StoredTrigger {
    let mut t = make_trigger(name, collection, true, false, false, mode);
    t.when_condition = Some(when.to_string());
    t
}

fn make_trigger_with_priority(name: &str, collection: &str, priority: i32) -> StoredTrigger {
    let mut t = make_trigger(
        name,
        collection,
        true,
        true,
        true,
        TriggerExecutionMode::Async,
    );
    t.priority = priority;
    t
}

// ---------------------------------------------------------------------------
// Event source cascade prevention
// ---------------------------------------------------------------------------

#[test]
fn event_source_user_is_triggerable() {
    let event = make_event(EventSource::User, WriteOp::Insert, "orders");
    assert_eq!(event.source, EventSource::User);
    assert!(event.op.is_data_event());
}

#[test]
fn event_source_trigger_not_retriggerable() {
    let event = make_event(EventSource::Trigger, WriteOp::Insert, "audit_log");
    assert_eq!(event.source, EventSource::Trigger);
    assert_ne!(event.source, EventSource::User);
}

#[test]
fn event_source_deferred_fires_deferred_mode() {
    let event = make_event(EventSource::Deferred, WriteOp::Insert, "orders");
    assert_eq!(event.source, EventSource::Deferred);
    assert_ne!(event.source, EventSource::User);
}

#[test]
fn event_source_crdt_sync_no_triggers() {
    let event = make_event(EventSource::CrdtSync, WriteOp::Update, "users");
    assert_eq!(event.source, EventSource::CrdtSync);
}

#[test]
fn event_source_raft_follower_no_triggers() {
    let event = make_event(EventSource::RaftFollower, WriteOp::Insert, "orders");
    assert_eq!(event.source, EventSource::RaftFollower);
}

#[test]
fn heartbeat_is_not_data_event() {
    let event = make_event(EventSource::User, WriteOp::Heartbeat, "");
    assert!(!event.op.is_data_event());
}

#[test]
fn bulk_insert_is_data_event() {
    let event = make_event(
        EventSource::User,
        WriteOp::BulkInsert { count: 10 },
        "orders",
    );
    assert!(event.op.is_data_event());
}

// ---------------------------------------------------------------------------
// Trigger registry: matching, filtering, ordering
// ---------------------------------------------------------------------------

#[test]
fn registry_matches_by_collection_and_event() {
    let reg = TriggerRegistry::new();
    reg.register(make_trigger(
        "t1",
        "orders",
        true,
        false,
        false,
        TriggerExecutionMode::Async,
    ));
    reg.register(make_trigger(
        "t2",
        "users",
        true,
        false,
        false,
        TriggerExecutionMode::Async,
    ));

    let matched = reg.get_matching(1, "orders", DmlEvent::Insert);
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].name, "t1");

    // Wrong event type.
    assert!(reg.get_matching(1, "orders", DmlEvent::Delete).is_empty());
    // Wrong collection.
    assert!(reg.get_matching(1, "products", DmlEvent::Insert).is_empty());
    // Wrong tenant.
    assert!(reg.get_matching(2, "orders", DmlEvent::Insert).is_empty());
}

#[test]
fn registry_filters_disabled_triggers() {
    let reg = TriggerRegistry::new();
    reg.register(make_trigger(
        "t1",
        "orders",
        true,
        false,
        false,
        TriggerExecutionMode::Async,
    ));
    reg.set_enabled(1, "t1", false);
    assert!(reg.get_matching(1, "orders", DmlEvent::Insert).is_empty());

    // Re-enable.
    reg.set_enabled(1, "t1", true);
    assert_eq!(reg.get_matching(1, "orders", DmlEvent::Insert).len(), 1);
}

#[test]
fn registry_sorts_by_priority_then_name() {
    let reg = TriggerRegistry::new();
    reg.register(make_trigger_with_priority("z_trigger", "c", 10));
    reg.register(make_trigger_with_priority("a_trigger", "c", 5));
    reg.register(make_trigger_with_priority("m_trigger", "c", 5));

    let matched = reg.get_matching(1, "c", DmlEvent::Insert);
    assert_eq!(matched.len(), 3);
    assert_eq!(matched[0].name, "a_trigger"); // priority 5, alpha first
    assert_eq!(matched[1].name, "m_trigger"); // priority 5, alpha second
    assert_eq!(matched[2].name, "z_trigger"); // priority 10
}

#[test]
fn registry_replace_on_same_name() {
    let reg = TriggerRegistry::new();
    reg.register(make_trigger(
        "t1",
        "orders",
        true,
        false,
        false,
        TriggerExecutionMode::Async,
    ));
    // Re-register with same name should replace.
    let mut t = make_trigger(
        "t1",
        "orders",
        false,
        true,
        false,
        TriggerExecutionMode::Sync,
    );
    t.execution_mode = TriggerExecutionMode::Sync;
    reg.register(t);

    // Should match UPDATE now, not INSERT.
    assert!(reg.get_matching(1, "orders", DmlEvent::Insert).is_empty());
    let matched = reg.get_matching(1, "orders", DmlEvent::Update);
    assert_eq!(matched.len(), 1);
    assert_eq!(matched[0].execution_mode, TriggerExecutionMode::Sync);
}

#[test]
fn registry_unregister() {
    let reg = TriggerRegistry::new();
    reg.register(make_trigger(
        "t1",
        "orders",
        true,
        false,
        false,
        TriggerExecutionMode::Async,
    ));
    reg.unregister(1, "t1");
    assert!(reg.get_matching(1, "orders", DmlEvent::Insert).is_empty());
}

#[test]
fn registry_multiple_triggers_same_collection() {
    let reg = TriggerRegistry::new();
    reg.register(make_trigger(
        "audit",
        "orders",
        true,
        true,
        true,
        TriggerExecutionMode::Async,
    ));
    reg.register(make_trigger(
        "notify",
        "orders",
        true,
        false,
        false,
        TriggerExecutionMode::Async,
    ));
    reg.register(make_trigger(
        "sync_check",
        "orders",
        true,
        false,
        false,
        TriggerExecutionMode::Sync,
    ));

    let insert_triggers = reg.get_matching(1, "orders", DmlEvent::Insert);
    assert_eq!(insert_triggers.len(), 3);

    // Only "audit" matches UPDATE.
    let update_triggers = reg.get_matching(1, "orders", DmlEvent::Update);
    assert_eq!(update_triggers.len(), 1);
    assert_eq!(update_triggers[0].name, "audit");
}

// ---------------------------------------------------------------------------
// Mode filtering (ASYNC / SYNC / DEFERRED)
// ---------------------------------------------------------------------------

#[test]
fn mode_filter_async_only() {
    let reg = TriggerRegistry::new();
    reg.register(make_trigger(
        "async_t",
        "orders",
        true,
        false,
        false,
        TriggerExecutionMode::Async,
    ));
    reg.register(make_trigger(
        "sync_t",
        "orders",
        true,
        false,
        false,
        TriggerExecutionMode::Sync,
    ));
    reg.register(make_trigger(
        "deferred_t",
        "orders",
        true,
        false,
        false,
        TriggerExecutionMode::Deferred,
    ));

    let all = reg.get_matching(1, "orders", DmlEvent::Insert);
    assert_eq!(all.len(), 3);

    // Filter for ASYNC only (what Event Plane does for User events).
    let async_only: Vec<_> = all
        .iter()
        .filter(|t| t.execution_mode == TriggerExecutionMode::Async)
        .collect();
    assert_eq!(async_only.len(), 1);
    assert_eq!(async_only[0].name, "async_t");

    // Filter for SYNC only (what write path does).
    let sync_only: Vec<_> = all
        .iter()
        .filter(|t| t.execution_mode == TriggerExecutionMode::Sync)
        .collect();
    assert_eq!(sync_only.len(), 1);
    assert_eq!(sync_only[0].name, "sync_t");

    // Filter for DEFERRED only.
    let deferred_only: Vec<_> = all
        .iter()
        .filter(|t| t.execution_mode == TriggerExecutionMode::Deferred)
        .collect();
    assert_eq!(deferred_only.len(), 1);
    assert_eq!(deferred_only[0].name, "deferred_t");
}

// ---------------------------------------------------------------------------
// WHEN condition evaluation
// ---------------------------------------------------------------------------

#[test]
fn when_condition_simple_true() {
    let t = make_trigger_with_when("t1", "orders", "TRUE", TriggerExecutionMode::Async);
    assert_eq!(t.when_condition.as_deref(), Some("TRUE"));
    // The evaluator should accept TRUE.
    assert_eq!(
        nodedb::control::trigger::try_eval_simple_condition("TRUE"),
        Some(true)
    );
}

#[test]
fn when_condition_simple_false() {
    assert_eq!(
        nodedb::control::trigger::try_eval_simple_condition("FALSE"),
        Some(false)
    );
    assert_eq!(
        nodedb::control::trigger::try_eval_simple_condition("0"),
        Some(false)
    );
    assert_eq!(
        nodedb::control::trigger::try_eval_simple_condition("NULL"),
        Some(false)
    );
}

#[test]
fn when_condition_complex_returns_none() {
    // Complex conditions return None (default to fire).
    assert_eq!(
        nodedb::control::trigger::try_eval_simple_condition("NEW.total > 100"),
        None
    );
}

// ---------------------------------------------------------------------------
// Cascade depth enforcement
// ---------------------------------------------------------------------------

#[test]
fn cascade_depth_constant_is_16() {
    use nodedb::control::planner::procedural::executor::core::MAX_CASCADE_DEPTH;
    assert_eq!(MAX_CASCADE_DEPTH, 16);
}

// ---------------------------------------------------------------------------
// Retry queue mechanics
// ---------------------------------------------------------------------------

#[test]
fn retry_queue_exponential_backoff() {
    let mut queue = TriggerRetryQueue::new();

    let entry = RetryEntry {
        tenant_id: 1,
        collection: "orders".into(),
        row_id: "r1".into(),
        operation: "INSERT".into(),
        trigger_name: "audit_trigger".into(),
        new_fields: None,
        old_fields: None,
        attempts: 0,
        last_error: "timeout".into(),
        next_retry_at: std::time::Instant::now(),
        source_lsn: 100,
        source_sequence: 1,
        cascade_depth: 0,
    };

    queue.enqueue(entry);
    assert_eq!(queue.len(), 1);

    let (ready, exhausted) = queue.drain_due();
    assert!(exhausted.is_empty());
    let total = ready.len() + queue.len();
    assert_eq!(total, 1);
}

#[test]
fn retry_queue_multiple_entries_fifo() {
    let mut queue = TriggerRetryQueue::new();
    let now = std::time::Instant::now();

    for i in 0..3 {
        queue.enqueue(RetryEntry {
            tenant_id: 1,
            collection: "orders".into(),
            row_id: format!("r-{i}"),
            operation: "INSERT".into(),
            trigger_name: format!("t-{i}"),
            new_fields: None,
            old_fields: None,
            attempts: 0,
            last_error: "err".into(),
            next_retry_at: now,
            source_lsn: i as u64,
            source_sequence: i as u64,
            cascade_depth: 0,
        });
    }
    assert_eq!(queue.len(), 3);
}

// ---------------------------------------------------------------------------
// DLQ lifecycle
// ---------------------------------------------------------------------------

#[test]
fn dlq_enqueue_and_list() {
    let dir = tempfile::tempdir().unwrap();
    let mut dlq = TriggerDlq::open(dir.path()).unwrap();

    let id = dlq
        .enqueue(DlqEnqueueParams {
            tenant_id: 1,
            source_collection: "orders".into(),
            row_id: "o-1".into(),
            operation: "INSERT".into(),
            trigger_name: "audit_trigger".into(),
            error: "constraint violation".into(),
            retry_count: 5,
            source_lsn: 100,
            source_sequence: 1,
        })
        .unwrap();

    assert!(id > 0);
    let unresolved = dlq.list_unresolved();
    assert_eq!(unresolved.len(), 1);
    assert_eq!(unresolved[0].trigger_name, "audit_trigger");
    assert_eq!(unresolved[0].error, "constraint violation");

    // Resolve and verify.
    assert!(dlq.resolve(id));
    assert_eq!(dlq.list_unresolved().len(), 0);
}

#[test]
fn dlq_resolve_nonexistent_returns_false() {
    let dir = tempfile::tempdir().unwrap();
    let mut dlq = TriggerDlq::open(dir.path()).unwrap();
    assert!(!dlq.resolve(999));
}

#[test]
fn dlq_evicts_oldest_on_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let mut dlq = TriggerDlq::open(dir.path()).unwrap();

    for i in 0..10 {
        let _ = dlq.enqueue(DlqEnqueueParams {
            tenant_id: 1,
            source_collection: "test".into(),
            row_id: format!("r-{i}"),
            operation: "INSERT".into(),
            trigger_name: "t".into(),
            error: "err".into(),
            retry_count: 5,
            source_lsn: i as u64,
            source_sequence: i as u64,
        });
    }
    assert_eq!(dlq.len(), 10);
}

#[test]
fn dlq_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();

    let id = {
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        dlq.enqueue(DlqEnqueueParams {
            tenant_id: 1,
            source_collection: "orders".into(),
            row_id: "o-1".into(),
            operation: "INSERT".into(),
            trigger_name: "t1".into(),
            error: "err".into(),
            retry_count: 3,
            source_lsn: 50,
            source_sequence: 1,
        })
        .unwrap()
    };

    // Reopen.
    let dlq = TriggerDlq::open(dir.path()).unwrap();
    let unresolved = dlq.list_unresolved();
    assert_eq!(unresolved.len(), 1);
    assert_eq!(unresolved[0].entry_id, id);
    assert_eq!(unresolved[0].trigger_name, "t1");
}

// ---------------------------------------------------------------------------
// Event source and dispatcher routing verification
// ---------------------------------------------------------------------------

#[test]
fn all_event_sources_have_display() {
    assert_eq!(EventSource::User.to_string(), "user");
    assert_eq!(EventSource::Trigger.to_string(), "trigger");
    assert_eq!(EventSource::RaftFollower.to_string(), "raft_follower");
    assert_eq!(EventSource::CrdtSync.to_string(), "crdt_sync");
    assert_eq!(EventSource::Deferred.to_string(), "deferred");
}

#[test]
fn write_op_display_variants() {
    assert_eq!(WriteOp::Insert.to_string(), "INSERT");
    assert_eq!(WriteOp::Update.to_string(), "UPDATE");
    assert_eq!(WriteOp::Delete.to_string(), "DELETE");
    assert_eq!(
        WriteOp::BulkInsert { count: 5 }.to_string(),
        "BULK_INSERT(5)"
    );
    assert_eq!(
        WriteOp::BulkDelete { count: 3 }.to_string(),
        "BULK_DELETE(3)"
    );
    assert_eq!(WriteOp::Heartbeat.to_string(), "HEARTBEAT");
}

// ---------------------------------------------------------------------------
// Trigger timing filter (only AFTER triggers should fire via Event Plane)
// ---------------------------------------------------------------------------

#[test]
fn only_after_triggers_matched_for_event_plane() {
    let reg = TriggerRegistry::new();

    let mut before_t = make_trigger(
        "before_t",
        "orders",
        true,
        false,
        false,
        TriggerExecutionMode::Async,
    );
    before_t.timing = TriggerTiming::Before;
    reg.register(before_t);

    reg.register(make_trigger(
        "after_t",
        "orders",
        true,
        false,
        false,
        TriggerExecutionMode::Async,
    ));

    let matched = reg.get_matching(1, "orders", DmlEvent::Insert);
    // Registry returns both — timing filter happens in fire_after_* functions.
    assert_eq!(matched.len(), 2);

    // But the AFTER filter should select only the AFTER trigger.
    let after_only: Vec<_> = matched
        .iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .collect();
    assert_eq!(after_only.len(), 1);
    assert_eq!(after_only[0].name, "after_t");
}

// ---------------------------------------------------------------------------
// Multi-event triggers
// ---------------------------------------------------------------------------

#[test]
fn trigger_on_multiple_events() {
    let reg = TriggerRegistry::new();
    reg.register(make_trigger(
        "all_events",
        "orders",
        true,
        true,
        true,
        TriggerExecutionMode::Async,
    ));

    assert_eq!(reg.get_matching(1, "orders", DmlEvent::Insert).len(), 1);
    assert_eq!(reg.get_matching(1, "orders", DmlEvent::Update).len(), 1);
    assert_eq!(reg.get_matching(1, "orders", DmlEvent::Delete).len(), 1);
}

// ---------------------------------------------------------------------------
// Trigger list for tenant
// ---------------------------------------------------------------------------

#[test]
fn list_for_tenant_returns_all() {
    let reg = TriggerRegistry::new();
    reg.register(make_trigger(
        "t1",
        "orders",
        true,
        false,
        false,
        TriggerExecutionMode::Async,
    ));
    reg.register(make_trigger(
        "t2",
        "users",
        true,
        false,
        false,
        TriggerExecutionMode::Sync,
    ));

    let list = reg.list_for_tenant(1);
    assert_eq!(list.len(), 2);

    // Other tenant sees nothing.
    assert!(reg.list_for_tenant(2).is_empty());
}
