//! Shared helpers for Event Plane integration tests.

use nodedb::event::cdc::event::CdcEvent;

/// Current time in milliseconds since UNIX epoch.
#[allow(dead_code)]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Create a [`CdcEvent`] with sensible test defaults.
#[allow(dead_code)]
pub fn make_cdc_event(seq: u64, partition: u16, collection: &str, op: &str) -> CdcEvent {
    CdcEvent {
        sequence: seq,
        partition,
        collection: collection.into(),
        op: op.into(),
        row_id: format!("r-{seq}"),
        event_time: now_ms(),
        lsn: seq * 10,
        tenant_id: 1,
        new_value: Some(serde_json::json!({"id": seq})),
        old_value: None,
        schema_version: 0,
    }
}
