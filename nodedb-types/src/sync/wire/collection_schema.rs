// SPDX-License-Identifier: Apache-2.0

//! Standardized collection descriptor + wire envelope for sync.

use serde::{Deserialize, Serialize};

use crate::collection::CollectionType;
use crate::collection_config::{PartitionStrategy, PrimaryEngine, VectorPrimaryConfig};
use crate::hlc::Hlc;
use crate::id::DatabaseId;

/// Standardized, engine-agnostic descriptor of a collection's identity + engine
/// config. This is the single unit that travels over sync so any peer can
/// materialize the collection in its catalog with the correct engine. Reused by
/// emit/receive/conversion paths in later units.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct CollectionDescriptor {
    /// Numeric tenant ID the collection belongs to.
    pub tenant_id: u64,
    /// Database the collection lives in.
    pub database_id: DatabaseId,
    /// Collection name.
    pub name: String,
    /// Storage engine + engine-specific configuration.
    pub collection_type: CollectionType,
    /// Whether the collection tracks system-time + valid-time versions.
    pub bitemporal: bool,
    /// Whether this collection uses CRDT (Loro) storage for offline-first sync.
    pub crdt: bool,
    /// Lightweight field type hints, e.g. `[("email", "string")]`.
    pub fields: Vec<(String, String)>,
    /// Which engine serves as the primary access path for this collection.
    pub primary: PrimaryEngine,
    /// Vector-primary configuration, present when `primary == PrimaryEngine::Vector`.
    pub vector_primary: Option<VectorPrimaryConfig>,
    /// How rows are distributed across vShards.
    pub partition_strategy: PartitionStrategy,
    /// Explicitly declared primary key field, if any.
    pub declared_primary_key: Option<String>,
    /// Monotonic version of this descriptor, bumped on schema-affecting change.
    pub descriptor_version: u64,
}

/// Wire envelope announcing a collection's descriptor to a sync peer (opcode 0x13).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CollectionSchemaSyncMsg {
    /// The collection's engine-agnostic descriptor.
    pub descriptor: CollectionDescriptor,
    /// HLC timestamp at which this descriptor was created/announced.
    pub creation_hlc: Hlc,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::wire::{SyncFrame, SyncMessageType};

    #[test]
    fn collection_descriptor_msgpack_roundtrip() {
        let descriptor = CollectionDescriptor {
            tenant_id: 7,
            database_id: DatabaseId::new(1024),
            name: "users".into(),
            collection_type: CollectionType::document(),
            bitemporal: true,
            crdt: true,
            fields: vec![("email".into(), "string".into())],
            primary: PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: PartitionStrategy::CollectionHomed,
            declared_primary_key: Some("id".into()),
            descriptor_version: 3,
        };
        let msg = CollectionSchemaSyncMsg {
            descriptor,
            creation_hlc: Hlc {
                wall_ns: 123,
                logical: 1,
            },
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::CollectionSchema, &msg).unwrap();
        let bytes = frame.to_bytes();
        let decoded = SyncFrame::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.msg_type, SyncMessageType::CollectionSchema);
        let decoded_msg: CollectionSchemaSyncMsg = decoded.decode_body().unwrap();
        assert_eq!(decoded_msg.descriptor.name, "users");
        assert_eq!(decoded_msg.descriptor.tenant_id, 7);
        assert!(decoded_msg.descriptor.bitemporal);
        assert!(decoded_msg.descriptor.crdt);
        assert_eq!(decoded_msg.descriptor.primary, PrimaryEngine::Document);
    }

    #[test]
    fn collection_schema_opcode() {
        assert_eq!(
            SyncMessageType::from_u8(0x13),
            Some(SyncMessageType::CollectionSchema)
        );
        assert_eq!(SyncMessageType::CollectionSchema as u8, 0x13);
    }
}
