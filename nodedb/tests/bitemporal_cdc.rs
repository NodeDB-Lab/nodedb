//! Bitemporal CDC: `WriteEvent` → `CdcRouter` → `StreamBuffer` carries
//! `system_time_ms` / `valid_time_ms` end-to-end.
//!
//! Verifies that:
//! 1. The producer-side extractor pulls `_ts_system` / `_ts_valid_from`
//!    out of the row payload msgpack into `WriteEvent`.
//! 2. The router copies both fields onto the emitted `CdcEvent`.
//! 3. A consumer reading from the buffer reconstructs the version
//!    timeline: system_time strictly monotonic, valid_time matches the
//!    original write, DELETE event still carries the stamps from the
//!    deleted row.

use std::sync::Arc;

use nodedb::event::bitemporal_extract::extract_stamps;
use nodedb::event::cdc::registry::StreamRegistry;
use nodedb::event::cdc::router::CdcRouter;
use nodedb::event::cdc::stream_def::{
    ChangeStreamDef, CompactionConfig, LateDataPolicy, OpFilter, RetentionConfig, StreamFormat,
};
use nodedb::event::types::{EventSource, RowId, WriteEvent, WriteOp};
use nodedb::event::watermark_tracker::WatermarkTracker;
use nodedb::types::{Lsn, TenantId, VShardId};

fn payload(name: &str, sys_ms: i64, valid_from_ms: i64) -> Vec<u8> {
    let json = serde_json::json!({
        "name": name,
        "_ts_system": sys_ms,
        "_ts_valid_from": valid_from_ms,
    });
    nodedb_types::json_to_msgpack(&json).unwrap()
}

fn write_event(seq: u64, op: WriteOp, payload_bytes: Vec<u8>, is_delete: bool) -> WriteEvent {
    let arc: Arc<[u8]> = Arc::from(payload_bytes.as_slice());
    let (system_time_ms, valid_time_ms) = extract_stamps(Some(&arc));
    WriteEvent {
        sequence: seq,
        collection: Arc::from("users"),
        op,
        row_id: RowId::new("u-1"),
        lsn: Lsn::new(seq * 10),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        source: EventSource::User,
        new_value: if is_delete { None } else { Some(arc.clone()) },
        old_value: if is_delete { Some(arc) } else { None },
        system_time_ms,
        valid_time_ms,
    }
}

fn stream_def() -> ChangeStreamDef {
    ChangeStreamDef {
        tenant_id: 1,
        name: "users_stream".into(),
        collection: "users".into(),
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

#[test]
fn cdc_event_carries_bitemporal_stamps_through_router() {
    let registry = Arc::new(StreamRegistry::new());
    registry.register(stream_def());
    let router = CdcRouter::new(registry);
    let wt = WatermarkTracker::new();

    // Three versioned writes + one delete. system_time strictly advances.
    let events = vec![
        write_event(1, WriteOp::Insert, payload("v1", 1_000, 100), false),
        write_event(2, WriteOp::Update, payload("v2", 2_000, 200), false),
        write_event(3, WriteOp::Update, payload("v3", 3_000, 300), false),
        write_event(4, WriteOp::Delete, payload("v3", 4_000, 300), true),
    ];

    for ev in &events {
        router.route_event(ev, &wt);
    }

    let buf = router
        .get_buffer(1, "users_stream")
        .expect("stream buffer present");
    let cdc = buf.read_from_lsn(0, 100);
    assert_eq!(cdc.len(), 4, "all four events routed");

    let stamps: Vec<(i64, i64)> = cdc
        .iter()
        .map(|e| {
            (
                e.system_time_ms.expect("system_time stamped"),
                e.valid_time_ms.expect("valid_time stamped"),
            )
        })
        .collect();

    assert_eq!(
        stamps,
        vec![(1_000, 100), (2_000, 200), (3_000, 300), (4_000, 300)],
        "system_time strictly monotonic, valid_time mirrors original write, \
         delete carries stamps from the deleted row"
    );

    assert_eq!(cdc[3].op, "DELETE");
}

#[test]
fn non_bitemporal_payload_yields_none_stamps() {
    let registry = Arc::new(StreamRegistry::new());
    registry.register(stream_def());
    let router = CdcRouter::new(registry);
    let wt = WatermarkTracker::new();

    // Payload without bitemporal fields.
    let plain = nodedb_types::json_to_msgpack(&serde_json::json!({"name": "alice"})).unwrap();
    router.route_event(&write_event(1, WriteOp::Insert, plain, false), &wt);

    let buf = router.get_buffer(1, "users_stream").unwrap();
    let cdc = buf.read_from_lsn(0, 10);
    assert_eq!(cdc.len(), 1);
    assert_eq!(cdc[0].system_time_ms, None);
    assert_eq!(cdc[0].valid_time_ms, None);
}
