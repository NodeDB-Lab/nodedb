// SPDX-License-Identifier: BUSL-1.1

//! Row-body shaping for the audit-log (`AS OF SYSTEM TIME NULL`) document scan.
//!
//! Both engines' stored bodies become one MessagePack shape here: user columns
//! plus the synthetic `_ts_system` / `_ts_valid_from` / `_ts_valid_until`
//! triple. The fetch stage in [`super::fetch`] calls these per version, before
//! the shared sort / window / computed-column / projection pipeline runs, so
//! every downstream transform sees the temporal columns as ordinary fields.

use nodedb_types::columnar::schema::{
    BITEMPORAL_RESERVED_COLUMNS, StrictSchema, TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL,
};

use crate::data::executor::strict_format;

/// Decode a strict row's Binary Tuple `body` into MessagePack via the
/// collection's schema, then strip the reserved bitemporal bookkeeping columns
/// (`__system_from_ms`, `__valid_from_ms`, `__valid_until_ms`) so the audit-log
/// output shape stays identical to the schemaless path: user columns plus the
/// synthetic temporal triple (injected by the caller via
/// [`inject_temporal_columns`]). The authoritative valid-time is taken from the
/// row's stored envelope (carried on `VersionedRow`), not from these slots, so
/// both Document engines surface identical temporal columns.
pub(super) fn strict_audit_body(body: &[u8], schema: &StrictSchema) -> crate::Result<Vec<u8>> {
    use nodedb_types::Value;

    let msgpack = strict_format::binary_tuple_to_msgpack(body, schema).ok_or_else(|| {
        crate::Error::Serialization {
            format: "binary-tuple".into(),
            detail: "decode strict document body for audit-log scan".into(),
        }
    })?;
    let value =
        nodedb_types::value_from_msgpack(&msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("decode strict document body for audit-log scan: {e}"),
        })?;
    let mut obj = match value {
        Value::Object(map) => map,
        other => {
            return Err(crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("strict audit-log body decoded to non-object value: {other:?}"),
            });
        }
    };
    for reserved in BITEMPORAL_RESERVED_COLUMNS {
        obj.remove(reserved);
    }
    nodedb_types::value_to_msgpack(&Value::Object(obj)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("re-encode stripped strict audit-log body: {e}"),
    })
}

/// Decode the MessagePack document body, insert/overwrite the three synthetic
/// user-facing audit temporal columns (`_ts_system`, `_ts_valid_from`,
/// `_ts_valid_until`) from the version's real stored temporal coordinates, and
/// re-encode. `system_from` is the version's system-time ordinal, and
/// `_ts_system` renders its millisecond (`ordinal_to_ms`), the unit
/// `AS OF SYSTEM TIME` takes. Valid-time is surfaced raw — `i64::MIN` /
/// `i64::MAX` sentinels mean "unbounded" (matching how columnar/timeseries
/// emit their real Int64 temporal columns). Non-object bodies are wrapped in a
/// fresh object carrying only the temporal columns. The triple is uniform
/// across both Document engines and columnar/timeseries.
pub(super) fn inject_temporal_columns(
    body: &[u8],
    system_from: i64,
    valid_from_ms: i64,
    valid_until_ms: i64,
) -> crate::Result<Vec<u8>> {
    use nodedb_types::Value;
    let value =
        nodedb_types::value_from_msgpack(body).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("decode document body for audit-log scan: {e}"),
        })?;
    let mut obj = match value {
        Value::Object(map) => map,
        _ => std::collections::HashMap::new(),
    };
    obj.insert(
        TS_SYSTEM.to_string(),
        Value::Integer(nodedb_types::ordinal_to_ms(system_from)),
    );
    obj.insert(TS_VALID_FROM.to_string(), Value::Integer(valid_from_ms));
    obj.insert(TS_VALID_UNTIL.to_string(), Value::Integer(valid_until_ms));
    nodedb_types::value_to_msgpack(&Value::Object(obj)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("re-encode document body with audit temporal columns: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;

    fn obj(pairs: &[(&str, Value)]) -> Vec<u8> {
        let mut m = std::collections::HashMap::new();
        for (k, v) in pairs {
            m.insert(k.to_string(), v.clone());
        }
        nodedb_types::value_to_msgpack(&Value::Object(m)).expect("encode object body")
    }

    fn decode(bytes: &[u8]) -> std::collections::HashMap<String, Value> {
        match nodedb_types::value_from_msgpack(bytes).expect("decode") {
            Value::Object(m) => m,
            other => panic!("expected object, got {other:?}"),
        }
    }

    #[test]
    fn inject_adds_temporal_columns_and_preserves_body_fields() {
        let body = obj(&[
            ("v", Value::Integer(1)),
            ("name", Value::String("alice".into())),
        ]);
        // An ordinal inside millisecond 1_700_000_000_123 renders that
        // millisecond.
        let ordinal = 1_700_000_000_123 * nodedb_types::NANOS_PER_MS + 456_789;
        let out = inject_temporal_columns(&body, ordinal, 10, 20).unwrap();
        let m = decode(&out);
        assert_eq!(m.get("v"), Some(&Value::Integer(1)));
        assert_eq!(m.get("name"), Some(&Value::String("alice".into())));
        assert_eq!(m.get(TS_SYSTEM), Some(&Value::Integer(1_700_000_000_123)));
        assert_eq!(m.get(TS_VALID_FROM), Some(&Value::Integer(10)));
        assert_eq!(m.get(TS_VALID_UNTIL), Some(&Value::Integer(20)));
    }

    #[test]
    fn inject_overwrites_any_preexisting_temporal_columns() {
        // A document that happens to carry temporal fields of its own must not
        // shadow the version's true temporal coordinates in the audit output.
        let body = obj(&[
            (TS_SYSTEM, Value::Integer(-1)),
            (TS_VALID_FROM, Value::Integer(-2)),
            (TS_VALID_UNTIL, Value::Integer(-3)),
            ("v", Value::Integer(2)),
        ]);
        let out =
            inject_temporal_columns(&body, 999 * nodedb_types::NANOS_PER_MS, 111, 222).unwrap();
        let m = decode(&out);
        assert_eq!(m.get(TS_SYSTEM), Some(&Value::Integer(999)));
        assert_eq!(m.get(TS_VALID_FROM), Some(&Value::Integer(111)));
        assert_eq!(m.get(TS_VALID_UNTIL), Some(&Value::Integer(222)));
        assert_eq!(m.get("v"), Some(&Value::Integer(2)));
    }

    #[test]
    fn inject_surfaces_unbounded_valid_time_sentinels() {
        let body = obj(&[("v", Value::Integer(1))]);
        let out = inject_temporal_columns(&body, 5, i64::MIN, i64::MAX).unwrap();
        let m = decode(&out);
        assert_eq!(m.get(TS_VALID_FROM), Some(&Value::Integer(i64::MIN)));
        assert_eq!(m.get(TS_VALID_UNTIL), Some(&Value::Integer(i64::MAX)));
    }

    #[test]
    fn inject_wraps_non_object_body_in_fresh_object() {
        let body = nodedb_types::value_to_msgpack(&Value::Integer(42)).unwrap();
        let out = inject_temporal_columns(&body, 7 * nodedb_types::NANOS_PER_MS, 8, 9).unwrap();
        let m = decode(&out);
        assert_eq!(m.get(TS_SYSTEM), Some(&Value::Integer(7)));
        assert_eq!(m.get(TS_VALID_FROM), Some(&Value::Integer(8)));
        assert_eq!(m.get(TS_VALID_UNTIL), Some(&Value::Integer(9)));
        assert_eq!(
            m.len(),
            3,
            "non-object body yields a fresh object carrying only the temporal columns"
        );
    }
}
