//! Extract bitemporal `_ts_system` / `_ts_valid_from` from a serialized
//! row payload.
//!
//! Single source of truth so every WriteEvent / CdcEvent emit site agrees
//! on the field names and accepts the same payload encodings (MessagePack
//! or JSON) that [`crate::event::types::deserialize_event_payload`] does.
//! Strict-tuple payloads are not handled here — Data Plane converts those
//! to MessagePack via the `binary_tuple_to_msgpack` shim before emission,
//! so by the time bytes reach Event Plane they're decodeable.
//!
//! Returns `(None, None)` whenever the payload is absent, undecodable, or
//! the row simply doesn't carry bitemporal stamps (non-bitemporal
//! collection). Callers MUST treat absence as "not bitemporal", never as
//! an error — heartbeat events, schemaless point KV writes, and replayed
//! BulkInsert/BulkDelete summary events all legitimately produce `(None,
//! None)`.

use crate::event::types::deserialize_event_payload;

/// Field name carrying the receiver-stamped system time. Matches the
/// reserved column name shared across CRDT, document strict, columnar,
/// timeseries, and edge engines.
const TS_SYSTEM: &str = "_ts_system";

/// Field name carrying the user / engine-stamped valid-from time.
const TS_VALID_FROM: &str = "_ts_valid_from";

/// Extract `(system_time_ms, valid_time_ms)` from a serialized row
/// payload. See module docs for the contract.
pub fn extract_stamps(payload: Option<&[u8]>) -> (Option<i64>, Option<i64>) {
    let bytes = match payload {
        Some(b) => b,
        None => return (None, None),
    };
    let map = match deserialize_event_payload(bytes) {
        Some(m) => m,
        None => return (None, None),
    };
    (
        extract_i64(&map, TS_SYSTEM),
        extract_i64(&map, TS_VALID_FROM),
    )
}

fn extract_i64(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<i64> {
    map.get(key).and_then(|v| v.as_i64())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_both_stamps_from_msgpack() {
        let json = serde_json::json!({
            "name": "alice",
            "_ts_system": 1_700_000_000_000_i64,
            "_ts_valid_from": 1_500_000_000_000_i64,
        });
        let bytes = nodedb_types::json_to_msgpack(&json).unwrap();
        let (sys, valid) = extract_stamps(Some(&bytes));
        assert_eq!(sys, Some(1_700_000_000_000));
        assert_eq!(valid, Some(1_500_000_000_000));
    }

    #[test]
    fn missing_fields_return_none() {
        let json = serde_json::json!({"name": "alice"});
        let bytes = nodedb_types::json_to_msgpack(&json).unwrap();
        let (sys, valid) = extract_stamps(Some(&bytes));
        assert_eq!(sys, None);
        assert_eq!(valid, None);
    }

    #[test]
    fn absent_payload_returns_none_pair() {
        let (sys, valid) = extract_stamps(None);
        assert_eq!(sys, None);
        assert_eq!(valid, None);
    }

    #[test]
    fn undecodable_payload_returns_none_pair() {
        let bytes = [0xff, 0xff, 0xff, 0x00, 0x01];
        let (sys, valid) = extract_stamps(Some(&bytes));
        assert_eq!(sys, None);
        assert_eq!(valid, None);
    }

    #[test]
    fn extracts_from_json_payload() {
        let bytes = serde_json::to_vec(&serde_json::json!({
            "_ts_system": 42_i64,
            "_ts_valid_from": 7_i64,
        }))
        .unwrap();
        let (sys, valid) = extract_stamps(Some(&bytes));
        assert_eq!(sys, Some(42));
        assert_eq!(valid, Some(7));
    }
}
