//! Change stream definition: persistent configuration for a CDC stream.

/// Which operations to include in the stream.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct OpFilter {
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
}

impl OpFilter {
    pub fn all() -> Self {
        Self {
            insert: true,
            update: true,
            delete: true,
        }
    }

    pub fn matches(&self, op: &str) -> bool {
        match op {
            "INSERT" => self.insert,
            "UPDATE" => self.update,
            "DELETE" => self.delete,
            _ => false,
        }
    }

    pub fn display(&self) -> String {
        let mut parts = Vec::new();
        if self.insert {
            parts.push("INSERT");
        }
        if self.update {
            parts.push("UPDATE");
        }
        if self.delete {
            parts.push("DELETE");
        }
        parts.join(",")
    }
}

impl Default for OpFilter {
    fn default() -> Self {
        Self::all()
    }
}

/// Output format for change stream events.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum StreamFormat {
    #[default]
    Json = 0,
    Msgpack = 1,
}

impl StreamFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Msgpack => "msgpack",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "json" => Some(Self::Json),
            "msgpack" | "messagepack" => Some(Self::Msgpack),
            _ => None,
        }
    }
}

/// Retention configuration for a change stream.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct RetentionConfig {
    /// Maximum events retained. Default 1M.
    pub max_events: u64,
    /// Maximum retention duration in seconds. Default 86400 (24h).
    pub max_age_secs: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_events: 1_000_000,
            max_age_secs: 86_400,
        }
    }
}

/// Policy for events that arrive below the partition's current watermark.
///
/// In a well-ordered system, events arrive in LSN order within a partition.
/// Late events can occur during WAL replay or catchup.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum LateDataPolicy {
    /// Accept and process normally. For single-partition streams, events are
    /// strictly ordered by LSN so "late" events don't occur.
    #[default]
    Allow = 0,
    /// Silently discard events with LSN below the partition watermark.
    Drop = 1,
    /// Accept the late event, process it (update streaming MV aggregates),
    /// and emit a correction event into the stream so downstream consumers
    /// know that a previously-emitted aggregate bucket was updated.
    /// The correction event has `op = "RECOMPUTE"` and carries the updated
    /// aggregate values in `new_value`.
    Recompute = 2,
}

impl LateDataPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "ALLOW",
            Self::Drop => "DROP",
            Self::Recompute => "RECOMPUTE",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ALLOW" => Some(Self::Allow),
            "DROP" => Some(Self::Drop),
            "RECOMPUTE" => Some(Self::Recompute),
            _ => None,
        }
    }
}

/// Log compaction configuration. When enabled, the buffer deduplicates
/// by key field, keeping only the latest event per key value.
#[derive(Debug, Clone, Default, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct CompactionConfig {
    /// Whether compaction is enabled.
    pub enabled: bool,
    /// Key field path used for deduplication (e.g., "id").
    pub key_field: String,
    /// Grace period for DELETE tombstones before removal (seconds). Default 24h.
    pub tombstone_grace_secs: u64,
}

impl CompactionConfig {
    pub fn key(field: impl Into<String>) -> Self {
        Self {
            enabled: true,
            key_field: field.into(),
            tombstone_grace_secs: 86_400,
        }
    }
}

/// Persistent definition of a change stream. Stored in the system catalog.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct ChangeStreamDef {
    /// Tenant that owns this stream.
    pub tenant_id: u32,
    /// Stream name (unique per tenant).
    pub name: String,
    /// Collection to watch. `"*"` means all collections in the tenant.
    pub collection: String,
    /// Which operations to include.
    pub op_filter: OpFilter,
    /// Output format.
    pub format: StreamFormat,
    /// Retention configuration.
    pub retention: RetentionConfig,
    /// Log compaction configuration (optional).
    #[msgpack(default)]
    pub compaction: CompactionConfig,
    /// Webhook delivery configuration (optional).
    #[msgpack(default)]
    pub webhook: crate::event::webhook::WebhookConfig,
    /// Policy for events that arrive below the partition watermark.
    #[msgpack(default)]
    pub late_data: LateDataPolicy,
    /// Kafka bridge delivery configuration (optional).
    #[msgpack(default)]
    pub kafka: crate::event::kafka::KafkaDeliveryConfig,
    /// Owner (creator).
    pub owner: String,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
}

impl ChangeStreamDef {
    /// Whether this stream watches all collections.
    pub fn is_wildcard(&self) -> bool {
        self.collection == "*"
    }

    /// Whether a given collection name matches this stream's filter.
    pub fn matches_collection(&self, collection: &str) -> bool {
        self.is_wildcard() || self.collection == collection
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_filter_matches() {
        let f = OpFilter {
            insert: true,
            update: false,
            delete: true,
        };
        assert!(f.matches("INSERT"));
        assert!(!f.matches("UPDATE"));
        assert!(f.matches("DELETE"));
    }

    #[test]
    fn wildcard_matches_all() {
        let def = ChangeStreamDef {
            tenant_id: 1,
            name: "all".into(),
            collection: "*".into(),
            op_filter: OpFilter::all(),
            format: StreamFormat::Json,
            retention: RetentionConfig::default(),
            compaction: CompactionConfig::default(),
            webhook: crate::event::webhook::WebhookConfig::default(),
            late_data: LateDataPolicy::default(),
            kafka: crate::event::kafka::KafkaDeliveryConfig::default(),
            owner: "admin".into(),
            created_at: 0,
        };
        assert!(def.matches_collection("orders"));
        assert!(def.matches_collection("users"));
        assert!(def.is_wildcard());
    }

    #[test]
    fn specific_collection_filter() {
        let def = ChangeStreamDef {
            tenant_id: 1,
            name: "orders_stream".into(),
            collection: "orders".into(),
            op_filter: OpFilter::all(),
            format: StreamFormat::Json,
            retention: RetentionConfig::default(),
            compaction: CompactionConfig::default(),
            webhook: crate::event::webhook::WebhookConfig::default(),
            late_data: LateDataPolicy::default(),
            kafka: crate::event::kafka::KafkaDeliveryConfig::default(),
            owner: "admin".into(),
            created_at: 0,
        };
        assert!(def.matches_collection("orders"));
        assert!(!def.matches_collection("users"));
        assert!(!def.is_wildcard());
    }
}
