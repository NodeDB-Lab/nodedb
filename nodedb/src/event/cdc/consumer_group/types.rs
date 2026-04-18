//! Consumer group type definitions.

/// Persistent definition of a consumer group. Stored in the system catalog.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ConsumerGroupDef {
    /// Tenant that owns this group.
    pub tenant_id: u32,
    /// Group name (unique per stream within a tenant).
    pub name: String,
    /// Stream this group consumes from.
    pub stream_name: String,
    /// Owner (creator).
    pub owner: String,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
}

/// A single partition offset: (partition_id, committed_lsn).
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct PartitionOffset {
    /// Partition ID (vShard ID).
    pub partition_id: u16,
    /// Last committed LSN for this partition. Events with LSN > this
    /// are unconsumed. Zero means no offset committed yet.
    pub committed_lsn: u64,
}

impl PartitionOffset {
    pub fn new(partition_id: u16, committed_lsn: u64) -> Self {
        Self {
            partition_id,
            committed_lsn,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_offset_serde_roundtrip() {
        let po = PartitionOffset::new(42, 1000);
        let bytes = zerompk::to_msgpack_vec(&po).unwrap();
        let decoded: PartitionOffset = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.partition_id, 42);
        assert_eq!(decoded.committed_lsn, 1000);
    }
}
