//! CdcEvent: the formatted event that change stream consumers read.
//!
//! Each event contains full context — consumers never need to fetch
//! from storage to process the event.

use serde::{Deserialize, Serialize};

/// A formatted CDC event ready for consumer delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    /// Monotonic sequence within this stream's partition.
    pub sequence: u64,
    /// Partition ID (vShard). Events within a partition are strictly ordered.
    pub partition: u16,
    /// Collection that was written.
    pub collection: String,
    /// Operation type: "INSERT", "UPDATE", or "DELETE".
    pub op: String,
    /// Row identifier.
    pub row_id: String,
    /// Wall-clock time of the event (epoch milliseconds).
    /// Used for time-bucket grouping; ordering uses LSN, not wall-clock.
    pub event_time: u64,
    /// WAL LSN for this event. Used for offset tracking and ordering.
    pub lsn: u64,
    /// Tenant ID.
    pub tenant_id: u32,
    /// New row value (for INSERT and UPDATE). JSON bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_value: Option<serde_json::Value>,
    /// Old row value (for UPDATE and DELETE). JSON bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_value: Option<serde_json::Value>,
}

impl CdcEvent {
    /// Serialize to JSON bytes.
    pub fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Serialize to MessagePack bytes.
    pub fn to_msgpack_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdc_event_json_roundtrip() {
        let event = CdcEvent {
            sequence: 1,
            partition: 42,
            collection: "orders".into(),
            op: "INSERT".into(),
            row_id: "order-1".into(),
            event_time: 1700000000000,
            lsn: 100,
            tenant_id: 1,
            new_value: Some(serde_json::json!({"id": 1, "total": 99.99})),
            old_value: None,
        };

        let bytes = event.to_json_bytes();
        let parsed: CdcEvent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.collection, "orders");
        assert_eq!(parsed.sequence, 1);
        assert!(parsed.old_value.is_none());
    }

    #[test]
    fn cdc_event_msgpack_roundtrip() {
        let event = CdcEvent {
            sequence: 2,
            partition: 10,
            collection: "users".into(),
            op: "UPDATE".into(),
            row_id: "user-5".into(),
            event_time: 1700000001000,
            lsn: 200,
            tenant_id: 1,
            new_value: Some(serde_json::json!({"name": "Alice"})),
            old_value: Some(serde_json::json!({"name": "Bob"})),
        };

        let bytes = event.to_msgpack_bytes();
        let parsed: CdcEvent = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(parsed.op, "UPDATE");
        assert!(parsed.old_value.is_some());
    }
}
