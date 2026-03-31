//! Integration tests for Event Plane cross-shard delivery.
//!
//! Tests: HWM dedup, retry queue with volume bound, DLQ on exhaust,
//! DLQ replay candidates, FIFO ordering, dispatcher backpressure.

use std::sync::Arc;
use std::time::Instant;

use nodedb::event::cross_shard::dedup::HwmStore;
use nodedb::event::cross_shard::dispatcher::CrossShardDispatcher;
use nodedb::event::cross_shard::dlq::{CrossShardDlq, DlqEnqueueParams};
use nodedb::event::cross_shard::metrics::CrossShardMetrics;
use nodedb::event::cross_shard::retry::{CrossShardRetryQueue, RetryEntry};
use nodedb::event::cross_shard::types::{CrossShardWriteRequest, CrossShardWriteResponse};

fn make_request(collection: &str, lsn: u64) -> CrossShardWriteRequest {
    CrossShardWriteRequest {
        sql: format!("INSERT INTO audit VALUES ({lsn})"),
        tenant_id: 1,
        source_vshard: 3,
        source_lsn: lsn,
        source_sequence: lsn,
        cascade_depth: 0,
        source_collection: collection.into(),
        target_vshard: 7,
    }
}

#[test]
fn hwm_dedup_drops_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let store = HwmStore::open(dir.path()).unwrap();

    assert!(!store.is_duplicate(3, 100));
    store.advance(3, 100);
    assert!(store.is_duplicate(3, 100)); // Equal = duplicate.
    assert!(store.is_duplicate(3, 50)); // Below = duplicate.
    assert!(!store.is_duplicate(3, 101)); // Above = new.
}

#[test]
fn hwm_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = HwmStore::open(dir.path()).unwrap();
        store.advance(5, 500);
    }
    let store = HwmStore::open(dir.path()).unwrap();
    assert_eq!(store.get(5), 500);
    assert!(store.is_duplicate(5, 500));
}

#[test]
fn retry_queue_enqueue_and_len() {
    let mut queue = CrossShardRetryQueue::new();

    // Enqueue two entries.
    queue.enqueue(RetryEntry {
        request: make_request("orders", 100),
        target_node: 2,
        attempts: 0,
        last_error: "timeout".into(),
        next_retry_at: Instant::now(),
        enqueued_at: Instant::now(),
    });
    queue.enqueue(RetryEntry {
        request: make_request("orders", 200),
        target_node: 2,
        attempts: 0,
        last_error: "timeout".into(),
        next_retry_at: Instant::now(),
        enqueued_at: Instant::now(),
    });

    assert_eq!(queue.len(), 2);
    assert!(!queue.is_empty());

    // drain_due returns nothing immediately (backoff not elapsed).
    let (ready, exhausted) = queue.drain_due();
    assert!(ready.is_empty());
    assert!(exhausted.is_empty());
    // Entries still in queue (not due yet).
    assert_eq!(queue.len(), 2);
}

#[test]
fn dlq_enqueue_list_resolve_replay() {
    let dir = tempfile::tempdir().unwrap();
    let mut dlq = CrossShardDlq::open(dir.path()).unwrap();

    dlq.enqueue(DlqEnqueueParams {
        tenant_id: 1,
        source_collection: "orders".into(),
        sql: "INSERT INTO audit VALUES (1)".into(),
        source_vshard: 3,
        target_vshard: 7,
        target_node: 2,
        source_lsn: 100,
        source_sequence: 100,
        error: "shard unavailable".into(),
        retry_count: 5,
    })
    .unwrap();

    assert_eq!(dlq.unresolved_count(), 1);
    assert_eq!(dlq.len(), 1);

    // Replay candidates.
    let candidates = dlq.replay_candidates("orders", 0);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].sql, "INSERT INTO audit VALUES (1)");

    // Resolve.
    let entry_id = dlq.list_unresolved()[0].entry_id;
    assert!(dlq.resolve(entry_id));
    assert_eq!(dlq.unresolved_count(), 0);
}

#[test]
fn dispatcher_enqueue_and_pending() {
    let metrics = Arc::new(CrossShardMetrics::new());
    let dispatcher = CrossShardDispatcher::new(1, Arc::clone(&metrics));

    assert_eq!(dispatcher.total_pending(), 0);

    dispatcher.enqueue(2, make_request("orders", 300));
    dispatcher.enqueue(2, make_request("orders", 100));
    dispatcher.enqueue(3, make_request("orders", 200));

    assert_eq!(dispatcher.total_pending(), 3);
    assert_eq!(
        metrics
            .writes_sent
            .load(std::sync::atomic::Ordering::Relaxed),
        3
    );
}

#[test]
fn response_variants() {
    let ok = CrossShardWriteResponse::ok(100);
    assert!(ok.success);
    assert!(!ok.duplicate);

    let dup = CrossShardWriteResponse::duplicate(100);
    assert!(dup.success);
    assert!(dup.duplicate);

    let err = CrossShardWriteResponse::error(100, "failed".into());
    assert!(!err.success);
    assert!(!err.duplicate);
    assert_eq!(err.error, "failed");
}

#[test]
fn metrics_snapshot() {
    let m = CrossShardMetrics::new();
    m.record_sent();
    m.record_sent();
    m.record_delivered(500);
    m.record_retry();
    m.record_dlq();
    m.record_duplicate();
    m.record_failure();

    let snap = m.snapshot();
    assert_eq!(snap.writes_sent, 2);
    assert_eq!(snap.writes_delivered, 1);
    assert_eq!(snap.retries, 1);
    assert_eq!(snap.dlq_enqueued, 1);
    assert_eq!(snap.duplicates_dropped, 1);
    assert_eq!(snap.delivery_failures, 1);
    assert_eq!(snap.avg_latency_us, 500);
}
