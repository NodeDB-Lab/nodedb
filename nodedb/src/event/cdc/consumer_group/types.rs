// SPDX-License-Identifier: BUSL-1.1

//! Consumer group type definitions.

use crate::event::cdc::offset::CdcOffset;
use crate::types::DatabaseId;

/// Persistent definition of a consumer group. Stored in the system catalog.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct ConsumerGroupDef {
    /// Tenant that owns this group.
    pub tenant_id: u64,
    /// Group name (unique per stream within a tenant).
    pub name: String,
    /// Stream this group consumes from.
    pub stream_name: String,
    /// Owner (creator).
    pub owner: String,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
    /// Database that owns this group.
    pub database_id: DatabaseId,
    /// Stamped at propose time on every put; fences a replayed delete to the
    /// incarnation it targeted.
    pub modification_hlc: nodedb_types::Hlc,
}

/// A single partition offset: (partition_id, committed composite position).
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct PartitionOffset {
    /// Partition ID (vShard ID).
    pub partition_id: u32,
    /// Last committed composite position. Events strictly after this position
    /// are unconsumed.
    pub committed_offset: CdcOffset,
}

impl PartitionOffset {
    pub fn new(partition_id: u32, committed_offset: CdcOffset) -> Self {
        Self {
            partition_id,
            committed_offset,
        }
    }
}

/// One replicated `COMMIT OFFSET`: raise a group's offsets on every node.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct OffsetCommit {
    pub database_id: DatabaseId,
    pub tenant_id: u64,
    pub stream_name: String,
    pub group_name: String,
    /// The group incarnation (`ConsumerGroupDef::modification_hlc`) the
    /// commit targets. A node whose registered group is another incarnation
    /// skips the commit.
    pub group_hlc: nodedb_types::Hlc,
    pub offsets: Vec<PartitionOffset>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_offset_serde_roundtrip() {
        let po = PartitionOffset::new(42, CdcOffset::new(1000, 7));
        let bytes = zerompk::to_msgpack_vec(&po).unwrap();
        let decoded: PartitionOffset = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.partition_id, 42);
        assert_eq!(decoded.committed_offset, CdcOffset::new(1000, 7));
    }
}
