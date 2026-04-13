//! The canonical wire-type for every entry proposed to the metadata Raft group.

use serde::{Deserialize, Serialize};

use crate::metadata_group::descriptors::{DescriptorId, DescriptorLease};

/// An entry in the replicated metadata log.
///
/// Every mutation to cluster-wide state — DDL, topology, routing,
/// descriptor leases, cluster version bumps — is encoded as one of
/// these variants, proposed against the metadata Raft group, and
/// applied on every node by a
/// [`crate::metadata_group::applier::MetadataApplier`].
///
/// The `CatalogDdl` variant is the single wire shape for every DDL
/// mutation. Its `payload` is an opaque, host-serialized
/// `nodedb::control::catalog_entry::CatalogEntry` value — the
/// `nodedb-cluster` crate is deliberately ignorant of the host's
/// per-DDL-object struct shapes. This keeps the cluster crate
/// layering-clean and makes adding new DDL object types on the
/// host side a zero-wire-change operation.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MetadataEntry {
    /// Single generic DDL entry carrying an opaque host-side payload.
    /// Produced by every pgwire DDL handler via
    /// `nodedb::control::metadata_proposer::propose_catalog_entry`.
    CatalogDdl {
        payload: Vec<u8>,
    },

    // ── Topology / routing ─────────────────────────────────────────────
    TopologyChange(TopologyChange),
    RoutingChange(RoutingChange),

    // ── Cluster version ────────────────────────────────────────────────
    ClusterVersionBump {
        from: u16,
        to: u16,
    },

    // ── Descriptor leases ──────────────────────────────────────────────
    DescriptorLeaseGrant(DescriptorLease),
    DescriptorLeaseRelease {
        node_id: u64,
        descriptor_ids: Vec<DescriptorId>,
    },
}

/// Topology mutations proposed through the metadata group.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum TopologyChange {
    Join { node_id: u64, addr: String },
    Leave { node_id: u64 },
    PromoteToVoter { node_id: u64 },
    StartDecommission { node_id: u64 },
    FinishDecommission { node_id: u64 },
}

/// Routing-table mutations proposed through the metadata group.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RoutingChange {
    /// Move a vShard to a new raft group leaseholder.
    ReassignVShard {
        vshard_id: u16,
        new_group_id: u64,
        new_leaseholder_node_id: u64,
    },
    /// Record a leadership transfer within an existing group.
    LeadershipTransfer {
        group_id: u64,
        new_leader_node_id: u64,
    },
}
