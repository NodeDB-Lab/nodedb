//! Regression coverage: CDC fan-out and buffer reads must share event
//! allocations via reference counting, not deep-clone per subscriber or poll.
//!
//! Two pressure points:
//!   * Router fan-out — one WriteEvent matched by N streams must not produce
//!     N deep copies of the payload (`serde_json::Value` + diffs + strings).
//!   * Buffer reads — every consumer poll (webhook producer, Kafka producer,
//!     change-stream reader) must not deep-clone the batch it returns.
//!
//! Both paths return `Arc<CdcEvent>` so downstream cloning is a refcount bump.

use std::sync::Arc;

use nodedb::event::cdc::buffer::StreamBuffer;
use nodedb::event::cdc::event::CdcEvent;
use nodedb::event::cdc::registry::StreamRegistry;
use nodedb::event::cdc::router::CdcRouter;
use nodedb::event::cdc::stream_def::{
    ChangeStreamDef, CompactionConfig, LateDataPolicy, OpFilter, RetentionConfig, StreamFormat,
};
use nodedb::event::types::{EventSource, RowId, WriteEvent, WriteOp};
use nodedb::event::watermark_tracker::WatermarkTracker;
use nodedb::types::{Lsn, TenantId, VShardId};

fn stream_def(name: &str, collection: &str) -> ChangeStreamDef {
    ChangeStreamDef {
        tenant_id: 1,
        name: name.into(),
        collection: collection.into(),
        op_filter: OpFilter::all(),
        format: StreamFormat::Json,
        retention: RetentionConfig {
            max_events: 1000,
            max_age_secs: 3600,
        },
        compaction: CompactionConfig::default(),
        webhook: nodedb::event::webhook::WebhookConfig::default(),
        late_data: LateDataPolicy::default(),
        kafka: nodedb::event::kafka::KafkaDeliveryConfig::default(),
        owner: "admin".into(),
        created_at: 0,
    }
}

fn write_event(seq: u64) -> WriteEvent {
    let payload = serde_json::json!({
        "id": seq,
        "padding": "x".repeat(2048),
    });
    WriteEvent {
        sequence: seq,
        collection: Arc::from("orders"),
        op: WriteOp::Insert,
        row_id: RowId::new(format!("r-{seq}")),
        lsn: Lsn::new(seq * 10),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        source: EventSource::User,
        new_value: Some(Arc::from(serde_json::to_vec(&payload).unwrap().as_slice())),
        old_value: None,
        system_time_ms: None,
        valid_time_ms: None,
    }
}

#[test]
fn router_fanout_shares_event_allocation_across_streams() {
    let reg = Arc::new(StreamRegistry::new());
    for i in 0..5 {
        reg.register(stream_def(&format!("stream_{i}"), "orders"));
    }
    let router = CdcRouter::new(reg);
    router.route_event(&write_event(1), &WatermarkTracker::new());

    // Each buffer read must return Arc<CdcEvent> so fan-out across the N matching
    // streams and N consumer polls share one allocation — not a deep per-copy.
    let mut per_stream: Vec<Arc<CdcEvent>> = Vec::new();
    for i in 0..5 {
        let buf = router
            .get_buffer(1, &format!("stream_{i}"))
            .expect("buffer must exist after route_event");
        let first: Arc<CdcEvent> = buf
            .read_from_lsn(0, 10)
            .into_iter()
            .next()
            .expect("event routed");
        per_stream.push(first);
    }

    let head = &per_stream[0];
    for other in &per_stream[1..] {
        assert!(
            Arc::ptr_eq(head, other),
            "CDC fan-out must share Arc<CdcEvent> across matching streams; \
             deep-cloning the payload per subscriber is O(write_rate * subscribers * payload)"
        );
    }
}

#[test]
fn buffer_read_from_lsn_shares_event_allocation_across_polls() {
    let buf = StreamBuffer::new("s".into(), RetentionConfig::default());
    let ev = CdcEvent {
        sequence: 1,
        partition: 0,
        collection: "orders".into(),
        op: "INSERT".into(),
        row_id: "r1".into(),
        event_time: 0,
        lsn: 10,
        tenant_id: 1,
        new_value: Some(serde_json::json!({"id": 1, "pad": "y".repeat(2048)})),
        old_value: None,
        schema_version: 0,
        field_diffs: None,
        system_time_ms: None,
        valid_time_ms: None,
    };
    buf.push(ev);

    // Webhook delivery and Kafka producer both poll the same buffer repeatedly.
    // Each poll must return Arc<CdcEvent> so they share one allocation.
    let poll1: Arc<CdcEvent> = buf
        .read_from_lsn(0, 10)
        .into_iter()
        .next()
        .expect("event present");
    let poll2: Arc<CdcEvent> = buf
        .read_from_lsn(0, 10)
        .into_iter()
        .next()
        .expect("event present");

    assert!(
        Arc::ptr_eq(&poll1, &poll2),
        "consumer polls must share Arc<CdcEvent>; deep-cloning per poll is \
         O(consumers * poll_rate * batch_size)"
    );
}

#[test]
fn buffer_partition_read_shares_event_allocation() {
    // read_partition_from_lsn is called on the same hot path by Kafka
    // producer batches. It must share allocations with read_from_lsn.
    let buf = StreamBuffer::new("s".into(), RetentionConfig::default());
    let ev = CdcEvent {
        sequence: 1,
        partition: 3,
        collection: "orders".into(),
        op: "INSERT".into(),
        row_id: "r1".into(),
        event_time: 0,
        lsn: 10,
        tenant_id: 1,
        new_value: Some(serde_json::json!({"id": 1})),
        old_value: None,
        schema_version: 0,
        field_diffs: None,
        system_time_ms: None,
        valid_time_ms: None,
    };
    buf.push(ev);

    let a: Arc<CdcEvent> = buf
        .read_from_lsn(0, 10)
        .into_iter()
        .next()
        .expect("event present");
    let b: Arc<CdcEvent> = buf
        .read_partition_from_lsn(3, 0, 10)
        .into_iter()
        .next()
        .expect("event present");

    assert!(
        Arc::ptr_eq(&a, &b),
        "read_partition_from_lsn must share the same Arc<CdcEvent> as read_from_lsn"
    );
}
