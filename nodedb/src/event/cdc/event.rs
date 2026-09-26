// SPDX-License-Identifier: BUSL-1.1

//! CdcEvent: the formatted event that change stream consumers read.
//!
//! Each event contains full context — consumers never need to fetch
//! from storage to process the event.

use nodedb_types::json_msgpack::JsonValue;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use sonic_rs;

use crate::types::DatabaseId;

/// A formatted CDC event ready for consumer delivery.
#[derive(Debug, Clone, Deserialize)]
pub struct CdcEvent {
    /// Monotonic sequence within this stream's partition.
    pub sequence: u64,
    /// Partition ID (vShard). Events within a partition are strictly ordered.
    pub partition: u32,
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
    /// Database that owns the collection. Missing legacy payloads decode to
    /// the built-in default database.
    #[serde(default)]
    pub database_id: DatabaseId,
    /// Tenant ID.
    pub tenant_id: u64,
    /// Where the write came from, such as `user` or `restore`. A consumer
    /// tells a restored row from a client write by it.
    pub source: crate::event::EventSource,
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
    /// `_ts_system` from the row payload. `None` for non-bitemporal
    /// collections; consumers using bitemporal CDC see strictly monotonic
    /// values across events for a given (collection, partition).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_time_ms: Option<i64>,
    /// `_ts_valid_from` from the row payload. `None` for non-bitemporal
    /// collections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_time_ms: Option<i64>,
}

impl CdcEvent {
    /// Lossless offset for this event.
    pub const fn position(&self) -> super::offset::CdcOffset {
        super::offset::CdcOffset::new(self.lsn, self.sequence)
    }

    /// Canonical `<lsn>:<sequence>` token accepted by `COMMIT OFFSET`.
    pub fn offset_token(&self) -> String {
        self.position().token()
    }

    /// Serialize to JSON bytes.
    pub fn to_json_bytes(&self) -> Vec<u8> {
        sonic_rs::to_vec(self).unwrap_or_default()
    }

    /// Serialize to MessagePack bytes.
    pub fn to_msgpack_bytes(&self) -> Vec<u8> {
        zerompk::to_msgpack_vec(self).unwrap_or_default()
    }
}

/// JSON consumers receive the canonical offset alongside the position fields.
/// The token is derived at serialization time so it cannot drift from `lsn` or
/// `sequence` in memory or over the cluster MessagePack payload.
impl Serialize for CdcEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let field_count = 12
            + usize::from(self.new_value.is_some())
            + usize::from(self.old_value.is_some())
            + usize::from(self.field_diffs.is_some())
            + usize::from(self.system_time_ms.is_some())
            + usize::from(self.valid_time_ms.is_some());
        let mut state = serializer.serialize_struct("CdcEvent", field_count)?;
        state.serialize_field("sequence", &self.sequence)?;
        state.serialize_field("partition", &self.partition)?;
        state.serialize_field("collection", &self.collection)?;
        state.serialize_field("op", &self.op)?;
        state.serialize_field("row_id", &self.row_id)?;
        state.serialize_field("event_time", &self.event_time)?;
        state.serialize_field("lsn", &self.lsn)?;
        state.serialize_field("offset", &self.offset_token())?;
        state.serialize_field("database_id", &self.database_id)?;
        state.serialize_field("tenant_id", &self.tenant_id)?;
        state.serialize_field("source", &self.source)?;
        state.serialize_field("schema_version", &self.schema_version)?;
        if let Some(value) = &self.new_value {
            state.serialize_field("new_value", value)?;
        }
        if let Some(value) = &self.old_value {
            state.serialize_field("old_value", value)?;
        }
        if let Some(diffs) = &self.field_diffs {
            state.serialize_field("field_diffs", diffs)?;
        }
        if let Some(time) = self.system_time_ms {
            state.serialize_field("system_time_ms", &time)?;
        }
        if let Some(time) = self.valid_time_ms {
            state.serialize_field("valid_time_ms", &time)?;
        }
        state.end()
    }
}

// ─── zerompk impls for CdcEvent ───────────────────────────────────────────

impl zerompk::ToMessagePack for CdcEvent {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        let field_count = 11
            + usize::from(self.new_value.is_some())
            + usize::from(self.old_value.is_some())
            + usize::from(self.field_diffs.is_some())
            + usize::from(self.system_time_ms.is_some())
            + usize::from(self.valid_time_ms.is_some());
        writer.write_map_len(field_count)?;
        writer.write_string("sequence")?;
        writer.write_u64(self.sequence)?;
        writer.write_string("partition")?;
        writer.write_u32(self.partition)?;
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
        writer.write_u64(self.tenant_id)?;
        writer.write_string("database_id")?;
        writer.write_u64(self.database_id.as_u64())?;
        writer.write_string("source")?;
        writer.write_string(self.source.as_str())?;
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
        if let Some(ts) = self.system_time_ms {
            writer.write_string("system_time_ms")?;
            writer.write_i64(ts)?;
        }
        if let Some(ts) = self.valid_time_ms {
            writer.write_string("valid_time_ms")?;
            writer.write_i64(ts)?;
        }
        Ok(())
    }
}

impl<'a> zerompk::FromMessagePack<'a> for CdcEvent {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_map_len()?;
        let mut sequence: u64 = 0;
        let mut partition: u32 = 0;
        let mut collection = String::new();
        let mut op = String::new();
        let mut row_id = String::new();
        let mut event_time: u64 = 0;
        let mut lsn: u64 = 0;
        let mut tenant_id: u64 = 0;
        let mut database_id = DatabaseId::DEFAULT;
        let mut source: Option<crate::event::EventSource> = None;
        let mut schema_version: u64 = 0;
        let mut new_value: Option<serde_json::Value> = None;
        let mut old_value: Option<serde_json::Value> = None;
        let mut field_diffs: Option<Vec<super::super::field_diff::FieldDiff>> = None;
        let mut system_time_ms: Option<i64> = None;
        let mut valid_time_ms: Option<i64> = None;
        for _ in 0..len {
            let key = reader.read_string()?.into_owned();
            match key.as_str() {
                "sequence" => sequence = reader.read_u64()?,
                "partition" => partition = reader.read_u32()?,
                "collection" => collection = reader.read_string()?.into_owned(),
                "op" => op = reader.read_string()?.into_owned(),
                "row_id" => row_id = reader.read_string()?.into_owned(),
                "event_time" => event_time = reader.read_u64()?,
                "lsn" => lsn = reader.read_u64()?,
                "tenant_id" => tenant_id = reader.read_u64()?,
                "database_id" => database_id = DatabaseId::new(reader.read_u64()?),
                "source" => {
                    let name = reader.read_string()?;
                    source = Some(
                        crate::event::EventSource::from_name(&name)
                            .ok_or(zerompk::Error::InvalidMarker(0))?,
                    );
                }
                "schema_version" => schema_version = reader.read_u64()?,
                "new_value" => new_value = Some(JsonValue::read(reader)?.0),
                "old_value" => old_value = Some(JsonValue::read(reader)?.0),
                "field_diffs" => {
                    field_diffs = Some(Vec::<super::super::field_diff::FieldDiff>::read(reader)?);
                }
                "system_time_ms" => system_time_ms = Some(reader.read_i64()?),
                "valid_time_ms" => valid_time_ms = Some(reader.read_i64()?),
                _ => {
                    JsonValue::read(reader)?;
                }
            }
        }
        // Every encoder writes the source, so an event without one is not
        // a CDC event.
        let source = source.ok_or(zerompk::Error::InvalidMarker(0))?;
        Ok(CdcEvent {
            sequence,
            partition,
            collection,
            op,
            row_id,
            event_time,
            lsn,
            tenant_id,
            source,
            database_id,
            schema_version,
            new_value,
            old_value,
            field_diffs,
            system_time_ms,
            valid_time_ms,
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
            source: crate::event::EventSource::User,
            database_id: DatabaseId::DEFAULT,
            new_value: Some(serde_json::json!({"id": 1, "total": 99.99})),
            old_value: None,
            schema_version: 0,
            field_diffs: None,
            system_time_ms: None,
            valid_time_ms: None,
        };

        let bytes = event.to_json_bytes();
        let parsed: CdcEvent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.collection, "orders");
        assert_eq!(parsed.sequence, 1);
        assert_eq!(event.offset_token(), "100:1");
        let json: serde_json::Value = sonic_rs::from_slice(&bytes).unwrap();
        assert_eq!(json["offset"], "100:1");
        assert!(parsed.old_value.is_none());
    }

    #[test]
    fn remote_payload_preserves_composite_position() {
        let event = CdcEvent {
            sequence: 2,
            partition: 10,
            collection: "users".into(),
            op: "UPDATE".into(),
            row_id: "user-5".into(),
            event_time: 1700000001000,
            lsn: 200,
            tenant_id: 1,
            source: crate::event::EventSource::User,
            database_id: DatabaseId::DEFAULT,
            new_value: Some(serde_json::json!({"name": "Alice"})),
            old_value: Some(serde_json::json!({"name": "Bob"})),
            schema_version: 0,
            field_diffs: None,
            system_time_ms: None,
            valid_time_ms: None,
        };

        let bytes = event.to_msgpack_bytes();
        let parsed: CdcEvent = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(parsed.op, "UPDATE");
        assert_eq!(
            parsed.position(),
            super::super::offset::CdcOffset::new(200, 2)
        );
        assert!(parsed.old_value.is_some());
    }

    #[test]
    fn cdc_event_bitemporal_stamps_roundtrip() {
        let event = CdcEvent {
            sequence: 3,
            partition: 0,
            collection: "users".into(),
            op: "INSERT".into(),
            row_id: "u-1".into(),
            event_time: 1700000002000,
            lsn: 300,
            tenant_id: 1,
            source: crate::event::EventSource::User,
            database_id: DatabaseId::DEFAULT,
            new_value: Some(serde_json::json!({"name": "Carol"})),
            old_value: None,
            schema_version: 0,
            field_diffs: None,
            system_time_ms: Some(1_700_000_000_000),
            valid_time_ms: Some(1_500_000_000_000),
        };

        let json = event.to_json_bytes();
        let parsed_json: CdcEvent = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed_json.system_time_ms, Some(1_700_000_000_000));
        assert_eq!(parsed_json.valid_time_ms, Some(1_500_000_000_000));

        let mp = event.to_msgpack_bytes();
        let parsed_mp: CdcEvent = zerompk::from_msgpack(&mp).unwrap();
        assert_eq!(parsed_mp.system_time_ms, Some(1_700_000_000_000));
        assert_eq!(parsed_mp.valid_time_ms, Some(1_500_000_000_000));
    }

    #[test]
    fn a_restored_row_is_tagged_restore_in_json_and_msgpack() {
        let event = CdcEvent {
            sequence: 4,
            partition: 0,
            collection: "users".into(),
            op: "INSERT".into(),
            row_id: "u-2".into(),
            event_time: 1700000003000,
            lsn: 400,
            tenant_id: 1,
            source: crate::event::EventSource::Restore,
            database_id: DatabaseId::DEFAULT,
            new_value: Some(serde_json::json!({"name": "Dana"})),
            old_value: None,
            schema_version: 0,
            field_diffs: None,
            system_time_ms: None,
            valid_time_ms: None,
        };

        let json: serde_json::Value = sonic_rs::from_slice(&event.to_json_bytes()).unwrap();
        assert_eq!(json["source"], "restore");
        let parsed_json: CdcEvent = serde_json::from_slice(&event.to_json_bytes()).unwrap();
        assert_eq!(parsed_json.source, crate::event::EventSource::Restore);

        let parsed_mp: CdcEvent = zerompk::from_msgpack(&event.to_msgpack_bytes()).unwrap();
        assert_eq!(parsed_mp.source, crate::event::EventSource::Restore);
    }
}
