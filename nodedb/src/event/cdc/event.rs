//! CdcEvent: the formatted event that change stream consumers read.
//!
//! Each event contains full context — consumers never need to fetch
//! from storage to process the event.

use nodedb_types::json_msgpack::JsonValue;
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
    /// Schema version of the collection at the time of the event.
    /// Incremented on ALTER TABLE / schema changes. Consumers can detect
    /// schema changes by watching this field.
    #[serde(default)]
    pub schema_version: u64,
    /// Per-field diffs for UPDATE operations.
    /// Present when both `old_value` and `new_value` are available and differ.
    /// Each entry describes a single field-level change with its dot-path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_diffs: Option<Vec<super::super::field_diff::FieldDiff>>,
}

impl CdcEvent {
    /// Serialize to JSON bytes.
    pub fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Serialize to MessagePack bytes.
    pub fn to_msgpack_bytes(&self) -> Vec<u8> {
        zerompk::to_msgpack_vec(self).unwrap_or_default()
    }
}

// ─── zerompk impls for CdcEvent ───────────────────────────────────────────

impl zerompk::ToMessagePack for CdcEvent {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        use zerompk::ToMessagePack;
        let field_count = 9
            + usize::from(self.new_value.is_some())
            + usize::from(self.old_value.is_some())
            + usize::from(self.field_diffs.is_some());
        writer.write_map_len(field_count)?;
        writer.write_string("sequence")?;
        writer.write_u64(self.sequence)?;
        writer.write_string("partition")?;
        writer.write_u16(self.partition)?;
        writer.write_string("collection")?;
        writer.write_string(&self.collection)?;
        writer.write_string("op")?;
        writer.write_string(&self.op)?;
        writer.write_string("row_id")?;
        writer.write_string(&self.row_id)?;
        writer.write_string("event_time")?;
        writer.write_u64(self.event_time)?;
        writer.write_string("lsn")?;
        writer.write_u64(self.lsn)?;
        writer.write_string("tenant_id")?;
        writer.write_u32(self.tenant_id)?;
        writer.write_string("schema_version")?;
        writer.write_u64(self.schema_version)?;
        if let Some(ref v) = self.new_value {
            writer.write_string("new_value")?;
            JsonValue(v.clone()).write(writer)?;
        }
        if let Some(ref v) = self.old_value {
            writer.write_string("old_value")?;
            JsonValue(v.clone()).write(writer)?;
        }
        if let Some(ref diffs) = self.field_diffs {
            writer.write_string("field_diffs")?;
            diffs.write(writer)?;
        }
        Ok(())
    }
}

impl<'a> zerompk::FromMessagePack<'a> for CdcEvent {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_map_len()?;
        let mut sequence: u64 = 0;
        let mut partition: u16 = 0;
        let mut collection = String::new();
        let mut op = String::new();
        let mut row_id = String::new();
        let mut event_time: u64 = 0;
        let mut lsn: u64 = 0;
        let mut tenant_id: u32 = 0;
        let mut schema_version: u64 = 0;
        let mut new_value: Option<serde_json::Value> = None;
        let mut old_value: Option<serde_json::Value> = None;
        let mut field_diffs: Option<Vec<super::super::field_diff::FieldDiff>> = None;
        for _ in 0..len {
            let key = reader.read_string()?.into_owned();
            match key.as_str() {
                "sequence" => sequence = reader.read_u64()?,
                "partition" => partition = reader.read_u16()?,
                "collection" => collection = reader.read_string()?.into_owned(),
                "op" => op = reader.read_string()?.into_owned(),
                "row_id" => row_id = reader.read_string()?.into_owned(),
                "event_time" => event_time = reader.read_u64()?,
                "lsn" => lsn = reader.read_u64()?,
                "tenant_id" => tenant_id = reader.read_u32()?,
                "schema_version" => schema_version = reader.read_u64()?,
                "new_value" => new_value = Some(JsonValue::read(reader)?.0),
                "old_value" => old_value = Some(JsonValue::read(reader)?.0),
                "field_diffs" => {
                    field_diffs = Some(Vec::<super::super::field_diff::FieldDiff>::read(reader)?);
                }
                _ => {
                    JsonValue::read(reader)?;
                }
            }
        }
        Ok(CdcEvent {
            sequence,
            partition,
            collection,
            op,
            row_id,
            event_time,
            lsn,
            tenant_id,
            schema_version,
            new_value,
            old_value,
            field_diffs,
        })
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
            schema_version: 0,
            field_diffs: None,
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
            schema_version: 0,
            field_diffs: None,
        };

        let bytes = event.to_msgpack_bytes();
        let parsed: CdcEvent = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(parsed.op, "UPDATE");
        assert!(parsed.old_value.is_some());
    }
}
