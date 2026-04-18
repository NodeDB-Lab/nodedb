//! Durable topic type definitions.

use crate::event::cdc::stream_def::RetentionConfig;

/// Persistent definition of a durable topic. Stored in the system catalog.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct TopicDef {
    /// Tenant that owns this topic.
    pub tenant_id: u32,
    /// Topic name (unique per tenant).
    pub name: String,
    /// Retention configuration.
    pub retention: RetentionConfig,
    /// Owner (creator).
    pub owner: String,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
}
