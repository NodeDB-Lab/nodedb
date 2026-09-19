// SPDX-License-Identifier: BUSL-1.1

//! The canonical wire-type for every entry proposed to the metadata Raft group.

use serde::{Deserialize, Serialize};

use nodedb_types::Hlc;

use crate::metadata_group::compensation::Compensation;
use crate::metadata_group::descriptors::{DescriptorId, DescriptorLease};
use crate::metadata_group::migration_state::{MigrationCheckpointPayload, MigrationPhaseTag};

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

    /// The cluster has advanced to a new topology generation.
    ///
    /// Proposed by the metadata-group leader on acquiring leadership. Going
    /// through the log is the point: every node reaches the same generation by
    /// applying the same committed entry, so the epoch is an agreed fact rather
    /// than a number each node infers from whatever it happened to overhear on
    /// the wire.
    ///
    /// Idempotent on apply — the applied epoch only ever moves forward, so a
    /// replayed or out-of-order bump is a no-op.
    ClusterEpochBump {
        epoch: u64,
    },

    /// Atomic batch of metadata entries proposed by a transactional
    /// DDL session (`BEGIN; CREATE ...; CREATE ...; COMMIT;`). The
    /// applier unpacks and applies each sub-entry in order at a
    /// single raft log index, so either all commit or none do.
    Batch {
        entries: Vec<MetadataEntry>,
    },

    /// A catalog DDL proposal fenced by the replicated preparation owner.
    /// Appliers execute `entry` only while `token` is the current owner;
    /// superseded proposals are deterministic no-ops so they cannot wedge the
    /// Raft apply watermark.
    DdlPrepared {
        token: u64,
        entry: Box<MetadataEntry>,
    },

    /// Acquire the replicated global descriptor-preparation lease. The first
    /// unexpired owner wins; contenders observe the winner after this entry is
    /// applied and retry after its matching release or expiry.
    DdlPrepareAcquire {
        token: u64,
    },
    /// Release the descriptor-preparation lease iff `token` still owns it.
    DdlPrepareRelease {
        token: u64,
    },

    /// Reserve `objects` under `token` before any DML in the same
    /// transaction dispatches. Applied by inserting into the node-local
    /// pending-DDL table; nothing is written to the catalog yet. A crash
    /// after this applies but before the matching finalize leaves the
    /// pending record for a future reconciliation pass, never orphaned
    /// catalog state.
    DdlPendingPropose {
        token: u64,
        objects: Vec<PendingDdlObject>,
        proposed_at: Hlc,
    },
    /// Commit the objects `token`'s `DdlPendingPropose` reserved: replay
    /// each object's host-side effects, then drop the pending record.
    /// Idempotent — a record already finalized, or absent, is a no-op, so
    /// Raft re-delivery is always safe.
    DdlPendingFinalize {
        token: u64,
    },
    /// Abandon the objects `token`'s `DdlPendingPropose` reserved and drop
    /// the pending record. Idempotent for the same reason as
    /// `DdlPendingFinalize`.
    DdlPendingCancel {
        token: u64,
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
    /// `expires_at` is the HLC the proposer computed when it
    /// started the drain. `is_draining` never compares it against
    /// a checking node's own wall clock — a node must not judge
    /// another node's deadline by its own clock. The liveness
    /// backstop for a crashed proposer is the wait loop in
    /// `drain_propose.rs`, which bounds its own wait with a
    /// same-node deadline and proposes `DescriptorDrainEnd`
    /// explicitly on timeout.
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

    // ── Sync producer identity ────────────────────────────────────────
    /// Replicate a new Lite client registration to every node.
    ///
    /// On apply every node writes the same registration row verbatim and
    /// advances its producer-id allocator HWM to at least `producer_id`, so
    /// a future leader never reissues the same id after a failover. The
    /// `producer_id` and initial `epoch` are assigned by the leader before
    /// proposing, so all replicas store identical state.
    ///
    /// Applied host-side only; the cluster cache tracks the applied index.
    SyncProducerRegister {
        lite_id: String,
        producer_id: u64,
        tenant_id: u64,
        user_id: u64,
        epoch: u64,
        created_ms: i64,
    },

    /// Replicate a fencing epoch advance to every node.
    ///
    /// Applied with max-wins semantics: each node sets
    /// `current_epoch = max(stored_epoch, new_epoch)` so out-of-order
    /// or duplicate delivery cannot roll the epoch backwards. A fence
    /// entry with no prior register row is a no-op on that node
    /// (tolerated for reordering/missing idempotency).
    ///
    /// Applied host-side only; the cluster cache tracks the applied index.
    SyncProducerFence {
        lite_id: String,
        new_epoch: u64,
    },

    /// Replicate ownership of one Loro peer id to every node.
    ///
    /// A peer id is the identity every CRDT operation is attributed to, so two
    /// replicas claiming the same one write into overlapping counter ranges and
    /// the merge silently discards the second. Ownership therefore has to agree
    /// across the cluster, or a failover hands the same peer id to a different
    /// producer and the collision returns.
    ///
    /// Applied with lowest-producer-id-wins semantics: the rule is commutative
    /// and idempotent, so out-of-order or duplicate delivery converges to the
    /// same owner on every node, and a node that optimistically claimed the peer
    /// id locally before proposing is corrected by the entry that beat it.
    ///
    /// Applied host-side only; the cluster cache tracks the applied index.
    SyncPeerBind {
        database_id: u64,
        tenant_id: u64,
        collection: String,
        peer_id: u64,
        producer_id: u64,
        bound_ms: i64,
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

    /// Reserve a disjoint batch of surrogates for one node (HiLo).
    ///
    /// The carved `[start, end)` range is **not** carried in the entry:
    /// it is computed AT APPLY by advancing the cluster-wide surrogate
    /// global watermark on every node. Because every node applies the
    /// metadata log in the same order against an identical watermark,
    /// all nodes deterministically agree on which range this reservation
    /// carves — guaranteeing globally-unique, disjoint surrogate batches
    /// without any node ever minting a value another node also minted.
    ///
    /// `node_id` + `request_id` identify the requesting node and the
    /// specific in-flight reservation so the owning node's host-side
    /// applier can (a) install the carved batch locally and (b) fire the
    /// completion signal that unblocks the waiting allocation path. Only
    /// the owning node installs the batch; every other node merely
    /// advances the watermark so future reservations stay disjoint.
    SurrogateReserve {
        node_id: u64,
        request_id: u64,
        batch_size: u32,
    },

    /// Join-token lifecycle transition (L.4). Proposed by the
    /// bootstrap-listener handler on every state change so that all
    /// Raft peers can enforce single-use token semantics even after a
    /// crash-restart cycle.
    ///
    /// `token_hash` is the SHA-256 of the token hex string — the raw
    /// token is never stored in the log. `transition` encodes the
    /// direction: `Register` for first issuance, `BeginInFlight` when
    /// a joiner presents the token, `MarkConsumed` when the bundle is
    /// delivered, `RevertInFlight` when the dead-man timer fires, and
    /// `MarkExpired` / `MarkAborted` for the terminal states.
    JoinTokenTransition {
        token_hash: [u8; 32],
        transition: JoinTokenTransitionKind,
        /// Unix-ms timestamp at the time of the proposal.
        ts_ms: u64,
    },

    /// Crash-safe migration phase checkpoint. Persisted on every phase
    /// transition; on coordinator restart, recovery scans the
    /// `MigrationStateTable` and resumes from the latest committed
    /// checkpoint. Apply is idempotent on `(migration_id, phase, attempt)`
    /// — duplicate delivery is a no-op. CRC32C mismatch is fatal.
    MigrationCheckpoint {
        /// Hyphenated UUID string (zerompk does not serialize uuid::Uuid).
        migration_id: String,
        phase: MigrationPhaseTag,
        attempt: u32,
        payload: MigrationCheckpointPayload,
        crc32c: u32,
        ts_ms: u64,
    },

    /// Replicated migration abort with ordered compensations. Each
    /// compensation is applied in order; any failure is fatal (no
    /// warn-and-continue). On success, the migration's row in
    /// `MigrationStateTable` is deleted.
    MigrationAbort {
        migration_id: String,
        reason: String,
        compensations: Vec<Compensation>,
    },

    /// Bounded CA-issued enrollment identity accepted by every join endpoint
    /// until topology commit or expiry.
    EnrollmentPreauthorization {
        spki: [u8; 32],
        expires_at_ms: u64,
    },
    /// Explicitly close an enrollment exception after a failed issuance.
    EnrollmentPreauthorizationRevoke {
        spki: [u8; 32],
        expires_at_ms: u64,
    },

    /// A descriptor lease grant stamped with the holder's incarnation.
    ///
    /// Appended last on purpose: zerompk derives the variant index from
    /// position, so inserting anywhere earlier would renumber every following
    /// variant. Proposed only once the cluster reports the fencing version;
    /// mixed-version clusters keep the plain
    /// [`DescriptorLeaseGrant`](Self::DescriptorLeaseGrant) and run without
    /// the fence.
    ///
    /// The value fences the release path: a holder marked gone at incarnation
    /// `N` fences only leases stamped at `<= N`, so a node that restarted (its
    /// incarnation bumped) and re-acquired is not affected by a stale verdict.
    DescriptorLeaseGrantFenced {
        lease: DescriptorLease,
        holder_incarnation: u64,
    },
}

/// The direction of a join-token lifecycle transition.
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
pub enum JoinTokenTransitionKind {
    /// New token registered (Issued state). Carries expiry so all nodes
    /// can enforce the TTL independently.
    Register { expires_at_ms: u64 },
    /// Joiner presented the token; transitioning Issued → InFlight.
    BeginInFlight {
        node_addr: String,
        lease_id: [u8; 16],
    },
    /// Bundle delivered; transitioning InFlight → Consumed.
    MarkConsumed {
        node_addr: String,
        lease_id: [u8; 16],
        recovery_bundle: Vec<u8>,
    },
    /// Dead-man timer fired; transitioning InFlight → Issued.
    RevertInFlight { lease_id: [u8; 16] },
    /// Token TTL elapsed without consumption.
    MarkExpired,
    /// Explicitly invalidated by an operator.
    MarkAborted,
}

/// One catalog object targeted by a `DdlPendingPropose`, distinguishing a
/// brand-new object from an overwrite of an existing one so compensation
/// never has to guess which case a missing before-image means.
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
pub enum PendingDdlObject {
    /// A brand-new object: no committed prior state existed at propose
    /// time. Cancel compensation is deletion — there is nothing to restore.
    Create { entry: Box<MetadataEntry> },
    /// An existing object about to be overwritten. `before_image` is its
    /// committed value at propose time, opaque host-serialized bytes in
    /// the same shape `entry`'s payload carries. Cancel compensation
    /// re-proposes it as-is.
    Alter {
        entry: Box<MetadataEntry>,
        before_image: Vec<u8>,
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
        vshard_id: u32,
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
    /// Set the explicit placement (intended voter set) for a Raft group.
    ///
    /// Records the target voter membership authored by the placement
    /// subsystem and replicated to all nodes via the metadata Raft group.
    /// Consumers compare this against the current `members` to decide
    /// whether a learner is eligible for promotion.
    SetPlacement { group_id: u64, placement: Vec<u64> },
}
