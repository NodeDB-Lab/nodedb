//! The canonical wire-type for every entry proposed to the metadata Raft group.

use serde::{Deserialize, Serialize};

use nodedb_types::Hlc;

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

    /// DDL entry with attached audit context. Produced by pgwire DDL
    /// handlers that have the authenticated identity + raw statement
    /// text bound at the call site (every `CREATE`, `ALTER`, `DROP`,
    /// `GRANT`, `REVOKE` path). Applied identically to `CatalogDdl`
    /// on every node; additionally, the production applier fsync-
    /// appends an audit record to the audit segment WAL with the
    /// authenticated user, HLC at commit, descriptor versions before
    /// + after, and the raw SQL — exactly what J.4 requires.
    ///
    /// Carries its own payload so legacy proposers (internal lease
    /// and descriptor-drain flows that have no SQL text) can keep
    /// using the plain `CatalogDdl` variant without synthesizing
    /// fake audit context.
    CatalogDdlAudited {
        payload: Vec<u8>,
        /// Authenticated user id at propose time.
        auth_user_id: String,
        /// Authenticated username at propose time.
        auth_user_name: String,
        /// Raw SQL statement as the client sent it. Not parsed here —
        /// the cluster crate is opaque to SQL syntax. Persisted on
        /// every replica so post-hoc audit queries don't depend on
        /// the proposing node still being alive.
        sql_text: String,
    },

    /// Atomic batch of metadata entries proposed by a transactional
    /// DDL session (`BEGIN; CREATE ...; CREATE ...; COMMIT;`). The
    /// applier unpacks and applies each sub-entry in order at a
    /// single raft log index, so either all commit or none do.
    Batch {
        entries: Vec<MetadataEntry>,
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

    // ── Descriptor lease drain ────────────────────────────────────────
    /// Begin draining leases on a descriptor. While a drain entry
    /// is active, any `acquire_descriptor_lease` at
    /// `version <= up_to_version` must be rejected cluster-wide so
    /// the in-flight DDL that bumps the version can make progress.
    ///
    /// `expires_at` is the HLC at which this drain entry is
    /// considered stale and ignored by `is_draining` checks on
    /// read. Acts as a TTL that prevents a crashed proposer from
    /// leaving an orphaned drain that blocks the cluster forever.
    DescriptorDrainStart {
        descriptor_id: DescriptorId,
        up_to_version: u64,
        expires_at: Hlc,
    },
    /// End draining on a descriptor. Emitted explicitly on drain
    /// timeout so the cluster can make progress. On the happy
    /// path (successful `Put*` apply), the host-side applier
    /// clears drain implicitly — this variant is the escape
    /// hatch for the failure path.
    DescriptorDrainEnd {
        descriptor_id: DescriptorId,
    },

    /// Cluster-wide CA trust mutation (L.4). Proposed by
    /// `nodedb rotate-ca --stage` (to add a new CA) and
    /// `nodedb rotate-ca --finalize --remove <fp>` (to drop an old
    /// CA). Applied on every node by `MetadataCommitApplier`: writes
    /// or deletes `data_dir/tls/ca.d/<fp_hex>.crt` and triggers a
    /// live rebuild of the rustls server + client configs so the
    /// new trust set takes effect without restart.
    ///
    /// `add_ca_cert` and `remove_ca_fingerprint` are independent:
    /// the `--stage` form sets `add_ca_cert = Some(new_ca_der)` +
    /// `remove_ca_fingerprint = None`; `--finalize` flips both. A
    /// single entry carrying both performs the cutover atomically
    /// once the operator has confirmed every node has reissued.
    CaTrustChange {
        /// DER-encoded CA certificate to add to the trust set. `None`
        /// when this entry only removes.
        add_ca_cert: Option<Vec<u8>>,
        /// SHA-256 fingerprint of the CA to remove from the trust set.
        /// `None` when this entry only adds.
        remove_ca_fingerprint: Option<[u8; 32]>,
    },

    // ── Surrogate identity ────────────────────────────────────────────
    /// Advance the cluster-wide surrogate high-watermark to `hwm`.
    ///
    /// Proposed by the metadata-group leader whenever the local
    /// `SurrogateRegistry` flush threshold trips (every 1024
    /// allocations or 200 ms, whichever comes first). Applied on
    /// every node by the host-side `MetadataCommitApplier` which
    /// calls `SurrogateRegistry::restore_hwm(hwm)` — idempotent and
    /// monotonic, so out-of-order replay or duplicate delivery are
    /// both safe. Followers must never allocate surrogates locally;
    /// they only advance their in-memory HWM via these log entries.
    SurrogateAlloc {
        hwm: u32,
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
    /// Remove a node from a Raft group's member and learner sets.
    ///
    /// Used by the decommission flow to strip a draining node out of
    /// every group it belongs to. Proposing this is only safe once
    /// `safety::check_can_decommission` has confirmed the group will
    /// still satisfy the configured replication factor.
    RemoveMember { group_id: u64, node_id: u64 },
}
