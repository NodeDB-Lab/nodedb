// SPDX-License-Identifier: BUSL-1.1

//! Pre-`database_id` [`ReplicatedEntry`](super::types::ReplicatedEntry) shape.
//!
//! `ReplicatedEntry` is encoded as a plain positional zerompk array (no
//! `#[msgpack(map)]`), so decoding checks the array length exactly. Adding
//! `database_id` grew that array from 4 elements to 5, which means a Raft log
//! entry proposed by a leader running the previous binary (still emitting the
//! 4-element shape) would fail to decode on a follower running the new
//! binary. `LegacyReplicatedEntry` mirrors that old 4-field shape so
//! `ReplicatedEntry::from_bytes` can fall back to it and default
//! `database_id` to `0` (`DatabaseId::DEFAULT`), matching the same 0 → DEFAULT
//! convention the on-disk WAL header uses for its own `database_id` field.

use super::types::{ReplicatedEntry, ReplicatedWrite};

/// The `ReplicatedEntry` wire shape before `database_id` was added.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct LegacyReplicatedEntry {
    pub tenant_id: u64,
    pub vshard_id: u32,
    pub idempotency_key: u64,
    pub write: ReplicatedWrite,
}

impl LegacyReplicatedEntry {
    /// Number of positional elements in this shape's zerompk array encoding.
    /// Used by `ReplicatedEntry::from_bytes` to recognize an `ArrayLengthMismatch`
    /// against the current shape as "this is an old-leader entry", not corruption.
    pub const FIELD_COUNT: usize = 4;

    /// Upgrade to the current shape, defaulting `database_id` to `0`.
    pub fn into_current(self) -> ReplicatedEntry {
        ReplicatedEntry {
            tenant_id: self.tenant_id,
            database_id: 0,
            vshard_id: self.vshard_id,
            idempotency_key: self.idempotency_key,
            write: self.write,
            write_hlc: 0,
            metadata_floor: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::wal_replication::decode;
    use nodedb_physical::physical_plan::DocumentOp;

    #[test]
    fn pre_database_id_entry_decodes_to_default_database() {
        // Pre-database_id 4-field shape must fall back to `LegacyReplicatedEntry`
        // (database_id defaults to 0), not fail to decode.
        let legacy = LegacyReplicatedEntry {
            tenant_id: 1,
            vshard_id: 0,
            idempotency_key: 0xabcd,
            write: ReplicatedWrite::PointPut {
                collection: "c".into(),
                document_id: "d".into(),
                value: vec![9, 9, 9],
                surrogate: 1,
                resolved_sum_targets: Vec::new(),
                resolved_sum_target_bindings: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
            },
        };
        let bytes = zerompk::to_msgpack_vec(&legacy).expect("legacy entry encode failed");

        let decoded = ReplicatedEntry::from_bytes(&bytes).expect("legacy entry must decode");
        assert_eq!(decoded.tenant_id, 1);
        assert_eq!(decoded.vshard_id, 0);
        assert_eq!(decoded.idempotency_key, 0xabcd);
        assert_eq!(
            decoded.database_id, 0,
            "old-leader entries lacking database_id must decode to DatabaseId::DEFAULT (0)"
        );

        let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
            .expect("from_replicated_entry error")
            .expect("from_replicated_entry returned None");
        match decoded_plan {
            PhysicalPlan::Document(DocumentOp::PointPut { collection, .. }) => {
                assert_eq!(collection.as_str(), "c");
            }
            other => panic!("expected Document(PointPut), got {other:?}"),
        }
    }
}
