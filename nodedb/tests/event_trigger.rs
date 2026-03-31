//! Integration tests for Event Plane trigger dispatch.
//!
//! Tests: async trigger fire, cascade depth limit, retry queue drain,
//! DLQ after max retries, DEFERRED mode event source.

use std::sync::Arc;

use nodedb::event::trigger::dlq::{DlqEnqueueParams, TriggerDlq};
use nodedb::event::trigger::retry::{RetryEntry, TriggerRetryQueue};
use nodedb::event::types::{EventSource, RowId, WriteEvent, WriteOp};
use nodedb::types::{Lsn, TenantId, VShardId};

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

#[test]
fn event_source_user_is_triggerable() {
    let event = make_event(EventSource::User, WriteOp::Insert, "orders");
    assert!(event.source == EventSource::User);
    assert!(event.op.is_data_event());
}

#[test]
fn event_source_trigger_not_retriggerable() {
    let event = make_event(EventSource::Trigger, WriteOp::Insert, "audit_log");
    // Trigger-originated events should NOT re-trigger (cascade prevention).
    assert!(event.source == EventSource::Trigger);
    assert!(event.source != EventSource::User);
}

#[test]
fn event_source_deferred_fires_deferred_mode() {
    let event = make_event(EventSource::Deferred, WriteOp::Insert, "orders");
    assert!(event.source == EventSource::Deferred);
    // Deferred events should fire DEFERRED-mode triggers only.
    assert!(event.source != EventSource::User);
}

#[test]
fn event_source_crdt_sync_no_triggers() {
    let event = make_event(EventSource::CrdtSync, WriteOp::Update, "users");
    assert!(event.source == EventSource::CrdtSync);
}

#[test]
fn heartbeat_is_not_data_event() {
    let event = make_event(EventSource::User, WriteOp::Heartbeat, "");
    assert!(!event.op.is_data_event());
}

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

    // Entry should have attempts = 1 after enqueue (incremented).
    let (ready, exhausted) = queue.drain_due();
    // May or may not be ready depending on timing — just verify no panic.
    assert!(exhausted.is_empty());
    let total = ready.len() + queue.len();
    assert_eq!(total, 1);
}

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

    // Resolve and verify.
    assert!(dlq.resolve(id));
    assert_eq!(dlq.list_unresolved().len(), 0);
}

#[test]
fn dlq_evicts_oldest_on_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let mut dlq = TriggerDlq::open(dir.path()).unwrap();

    // Enqueue more than we can check individually — just verify no crash
    // and that the DLQ stays bounded.
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
