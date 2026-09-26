// SPDX-License-Identifier: BUSL-1.1

//! Durable topic type definitions.

use crate::event::cdc::event::CdcEvent;
use crate::event::cdc::stream_def::RetentionConfig;
use crate::types::DatabaseId;

/// Maximum UTF-8 byte length accepted for a durable topic identifier.
pub const MAX_TOPIC_NAME_BYTES: usize = 256;

/// Validate a durable topic name using the SQL identifier rules.
pub fn validate_topic_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("identifier cannot be empty");
    }
    if name.len() > MAX_TOPIC_NAME_BYTES {
        return Err("topic name exceeds maximum size of 256 bytes");
    }
    if !name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err("invalid identifier");
    }
    if name.as_bytes()[0].is_ascii_digit() {
        return Err("identifier cannot start with digit");
    }
    Ok(())
}

/// Persistent definition of a durable topic. Stored in the system catalog.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map, allow_unknown_fields)]
pub struct TopicDef {
    /// Tenant that owns this topic.
    pub tenant_id: u64,
    /// Topic name (unique per tenant).
    pub name: String,
    /// Retention configuration.
    pub retention: RetentionConfig,
    /// Owner (creator).
    pub owner: String,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
    /// Database that owns this topic. Missing map fields from legacy records
    /// decode into the built-in default database.
    #[msgpack(default)]
    pub database_id: DatabaseId,
    /// Last sequence durably assigned to this topic.
    #[msgpack(default)]
    pub last_sequence: u64,
    /// Last LSN durably assigned to this topic.
    #[msgpack(default)]
    pub last_lsn: u64,
}

/// A durably retained topic publication.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map, allow_unknown_fields)]
pub struct TopicMessage {
    /// Database that owns the topic.
    pub database_id: DatabaseId,
    /// Tenant that owns the topic.
    pub tenant_id: u64,
    /// Topic name.
    pub topic: String,
    /// Monotonic topic-local sequence.
    pub sequence: u64,
    /// Wall-clock event timestamp in milliseconds.
    pub event_time: u64,
    /// Monotonic WAL-style position for this topic.
    pub lsn: u64,
    /// Original publish payload, without JSON normalization.
    pub payload: String,
}

impl TopicMessage {
    /// Format this durable message exactly as the existing topic publisher does.
    pub fn to_cdc_event(&self) -> CdcEvent {
        let value: serde_json::Value = sonic_rs::from_str(&self.payload)
            .unwrap_or_else(|_| serde_json::json!({"message": self.payload.clone()}));
        CdcEvent {
            sequence: self.sequence,
            partition: 0,
            collection: format!("topic:{}", self.topic),
            op: "PUBLISH".into(),
            row_id: format!("msg-{}", self.sequence),
            event_time: self.event_time,
            lsn: self.lsn,
            database_id: self.database_id,
            tenant_id: self.tenant_id,
            new_value: Some(value),
            old_value: None,
            schema_version: 0,
            field_diffs: None,
            system_time_ms: None,
            valid_time_ms: None,
            source: crate::event::EventSource::User,
        }
    }
}

impl From<&TopicMessage> for CdcEvent {
    fn from(message: &TopicMessage) -> Self {
        message.to_cdc_event()
    }
}

impl From<TopicMessage> for CdcEvent {
    fn from(message: TopicMessage) -> Self {
        message.to_cdc_event()
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_TOPIC_NAME_BYTES, validate_topic_name};

    #[test]
    fn topic_names_follow_identifier_rules_and_byte_limit() {
        assert!(validate_topic_name("orders_2025").is_ok());
        assert!(validate_topic_name("1orders").is_err());
        assert!(validate_topic_name("orders-topic").is_err());
        assert!(validate_topic_name("é").is_err());
        assert!(validate_topic_name(&"a".repeat(MAX_TOPIC_NAME_BYTES)).is_ok());
        assert!(validate_topic_name(&"a".repeat(MAX_TOPIC_NAME_BYTES + 1)).is_err());
    }
}
