// SPDX-License-Identifier: BUSL-1.1

//! Host-crate integration hooks for the Raft loop.
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so behaviour that
//! lives in the host crate (`nodedb`) — snapshot quarantine accounting and the
//! three cross-node shuffle stages — is reached through these `Send + Sync`
//! trait objects. The `RaftLoop` holds each as an optional field; cluster-only
//! tests leave them `None`.

use crate::error::Result;

pub use super::hooks_routed::{
    AssignRemoteSurrogate, CalvinSubmit, CalvinSubmitInbox, ReleaseReservation, ReserveRead,
};

/// Hook for building per-group snapshot payloads on the Raft snapshot SEND path.
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the snapshot
/// builder — which serializes the engine state for the vshards owned by a Raft
/// group so a lagging/new follower can be caught up — lives in the host crate
/// (`nodedb`) behind this `Send + Sync` hook. The tick loop's install-snapshot
/// dispatch (see [`super::tick`]) calls [`build_group_snapshot`](Self::build_group_snapshot)
/// before framing the chunked `InstallSnapshot` RPC.
///
/// Cluster-only tests leave the `RaftLoop` field `None`, which makes the sender
/// fall back to the stub (empty) chunk.
///
/// The hook is **async** because the host-crate implementation dispatches the
/// per-vshard snapshot build to the Data Plane through the existing SPSC bridge
/// (an awaited round-trip on the Tokio transport reactor); it never touches
/// io_uring or storage directly.
#[async_trait::async_trait]
pub trait SnapshotBuilder: Send + Sync + 'static {
    /// Build the per-group snapshot payload (serialized engine state for the
    /// group's vshards) to ship to a lagging/new follower.
    ///
    /// The capture holds every entry of the group through a cut at or above
    /// `last_included_index`, and nothing above it. The returned
    /// [`BuiltGroupSnapshot::cut_index`] names that cut, and the snapshot is sent
    /// labelled with it. So the follower resumes the log right after the cut,
    /// with no gap of entries the state already holds.
    ///
    /// Empty bytes are a valid "nothing to send" result: the caller sends the
    /// stub chunk, with the cut the builder names.
    async fn build_group_snapshot(
        &self,
        group_id: u64,
        last_included_index: u64,
        last_included_term: u64,
    ) -> std::result::Result<BuiltGroupSnapshot, Box<dyn std::error::Error + Send + Sync>>;

    /// Capture metadata group 0's state machine at `applied_index`, whose
    /// entry has term `applied_term`.
    ///
    /// Called on the tick thread, between apply batches, so the capture holds
    /// exactly the entries applied through `applied_index`. The capture must
    /// only open read views: the caller serializes it on another task.
    fn capture_metadata(
        &self,
        applied_index: u64,
        applied_term: u64,
    ) -> std::result::Result<
        Box<dyn MetadataSnapshotCapture>,
        Box<dyn std::error::Error + Send + Sync>,
    >;

    /// Capture the Calvin sequencer group's state machine at
    /// `applied_index`, encoded as the snapshot payload.
    ///
    /// Called on the tick thread, between apply batches, so the capture holds
    /// exactly the entries applied through `applied_index`. The payload is a
    /// few scalars and the open multi-part transactions, so it is encoded in
    /// place.
    fn capture_sequencer(
        &self,
        applied_index: u64,
    ) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;
}

/// A data group snapshot built by [`SnapshotBuilder::build_group_snapshot`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BuiltGroupSnapshot {
    /// The serialized payload. Empty when there is nothing to send.
    pub bytes: Vec<u8>,
    /// The highest log index the payload's state holds. At or above the
    /// `last_included_index` the build was asked for.
    pub cut_index: u64,
}

/// A group 0 state machine capture taken by
/// [`SnapshotBuilder::capture_metadata`].
pub trait MetadataSnapshotCapture: Send + 'static {
    /// Serialize the captured state into the snapshot payload.
    fn serialize(
        self: Box<Self>,
    ) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Hook for applying a received per-group snapshot to local engine state on the
/// Raft snapshot RECEIVE path.
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the follower-side
/// apply — which deserializes the per-group `TenantDataSnapshot` bytes and
/// installs them into the local Data-Plane state machine through the existing
/// SPSC bridge — lives in the host crate (`nodedb`) behind this `Send + Sync`
/// hook. The install-snapshot finalize path (see
/// [`crate::install_snapshot::finalize::commit`]) calls
/// [`apply_snapshot`](Self::apply_snapshot) AFTER staging the snapshot and
/// BEFORE advancing Raft, so the data is visible on this node before the Raft
/// log boundary moves. Boot recovery calls it again for a staged install that
/// did not finish.
///
/// Cluster-only tests leave the `RaftLoop` field `None`, which makes the
/// follower advance Raft WITHOUT restoring engine state — correct for tests
/// that ship only the empty bootstrap stub.
///
/// The hook is **async** because the host-crate implementation dispatches the
/// per-tenant restore to the Data Plane through the SPSC bridge (an awaited
/// round-trip on the Tokio transport reactor); it never touches io_uring or
/// storage directly.
#[async_trait::async_trait]
pub trait SnapshotApplier: Send + Sync + 'static {
    /// Apply a per-group snapshot to the local state machine. Called after
    /// staging, before Raft advances. `Ok` MUST mean the install is durable
    /// without the WAL: the caller then moves the durable floor past it and
    /// keeps no copy. Err MUST prevent the raft advance (follower retries).
    /// For group 0 the bytes are the host's metadata image, captured by
    /// [`SnapshotBuilder::capture_metadata`].
    async fn apply_snapshot(
        &self,
        group_id: u64,
        snapshot_bytes: &[u8],
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Called after the group adopted a snapshot at `last_included_index`,
    /// while its apply gate still excludes every applier. No entry at or below
    /// that index applies on this node afterwards, so nothing produces those
    /// entries' results here.
    fn snapshot_adopted(&self, _group_id: u64, _last_included_index: u64) {}
}

/// Hook for quarantine integration on the Raft snapshot receive path.
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the host crate
/// (`nodedb`) supplies an implementation backed by its `QuarantineRegistry`.
/// Cluster-only tests leave the field `None`, which skips all quarantine
/// accounting.
///
/// All methods take `(group_id, last_included_index)` as the snapshot identity.
pub trait SnapshotQuarantineHook: Send + Sync + 'static {
    /// Returns `true` if the chunk identified by `(group_id, index)` is
    /// already in the quarantined state and should be rejected immediately
    /// without attempting to decode it.
    fn is_quarantined(&self, group_id: u64, last_included_index: u64) -> bool;

    /// Called after a successful decode — resets the strike counter so a
    /// single transient CRC error is not held against a healthy peer.
    fn record_success(&self, group_id: u64, last_included_index: u64);

    /// Called on a CRC-class decode failure.
    ///
    /// Returns `true` when the segment has just been quarantined (second
    /// consecutive failure), and `false` on the first strike (caller should
    /// surface the framing error and allow the peer to retry).
    fn record_failure(&self, group_id: u64, last_included_index: u64, error: &str) -> bool;
}

/// Hook for the cross-node streaming-shuffle receiver registry.
///
/// `nodedb-cluster` cannot depend on `nodedb` (circular), so the receiver
/// registry — which is owned by `nodedb`'s `SharedState` and consumed by the
/// `!Send` Data Plane — lives behind this `Send + Sync` hook.
/// The transport read-loop drives a `ShufflePush` stream and calls these
/// methods; the host crate's implementation deposits payloads into the
/// per-`(shuffle_id, part, side)` inbox and advances the per-part build
/// barrier.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a `ShufflePush` stream
/// against a node with no receiver installed returns a typed error.
///
/// The hook is **async** because the host-crate implementation stages arriving
/// rows to a Control-Plane scratch file (receive-to-spill) and must NOT
/// block the transport reactor thread on a synchronous `std::fs` write. The
/// awaited `tokio::fs` write inside `on_shuffle_chunk` is what lets QUIC flow
/// control back-pressure the producer — the chunk is staged inline, never
/// detached into a spawned task.
#[async_trait::async_trait]
pub trait ShuffleReceiver: Send + Sync + 'static {
    /// First frame of a stream: lazily create the inbox for
    /// `(shuffle_id, part, side)` (carrying `producer_count` and `num_parts`)
    /// or reuse the existing one.
    async fn on_shuffle_request(&self, shuffle_id: u64, part: u32, side: u8, producer_count: u32);

    /// Stage one chunk payload to the inbox's scratch file (bounded — the
    /// awaited file write back-pressures the producer via QUIC flow control).
    /// Returns a typed error on a malformed chunk array or an I/O failure
    /// (never a silent drop).
    async fn on_shuffle_chunk(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        payload: Vec<u8>,
    ) -> Result<()>;

    /// Terminal frame for one producer: record the `End` (advancing the
    /// barrier), flush + sync the staging file when the barrier completes, and
    /// capture any terminal error.
    async fn on_shuffle_end(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        error: Option<crate::rpc_codec::TypedClusterError>,
    );
}

/// Hook for the cross-node shuffle PRODUCER.
///
/// Sibling of [`ShuffleReceiver`]: `nodedb-cluster` cannot depend on `nodedb`
/// (circular), so the produce logic — decode the local scan plan, run it through
/// the local streaming executor, hash-partition each output row, and fan the
/// rows out to the per-part owners (looping back into the local receiver
/// registry for self-owned parts) — lives in `nodedb` behind this `Send + Sync`
/// hook. The transport read-loop calls [`on_shuffle_produce`](Self::on_shuffle_produce)
/// when a `ShuffleProduceRequest` arrives and writes the returned outcome back as
/// a `ShuffleProduceResponse`.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a `ShuffleProduce`
/// request against a node with no producer installed returns a typed
/// "not configured" error.
///
/// The hook is **async** because the produce path drives QUIC fan-out streams
/// and the local streaming executor on the Tokio transport reactor. QUIC is fine
/// here (Control Plane); the local scan itself is dispatched to the Data Plane
/// through the existing SPSC bridge by the host-crate implementation.
#[async_trait::async_trait]
pub trait ShuffleProducer: Send + Sync + 'static {
    /// Run the local scan fragment, hash-partition its rows, and fan them out to
    /// the part-owners. Returns a [`ShuffleProduceResponse`] whose `error` is
    /// `None` on a clean produce or `Some(err)` on a terminal scan failure (after
    /// every part has been `End`ed with the error), and whose `read_versions`
    /// carry the read versions the local scan observed (empty on failure) for
    /// the coordinator's cross-shard OCC read validation.
    async fn on_shuffle_produce(
        &self,
        req: crate::rpc_codec::ShuffleProduceRequest,
    ) -> crate::rpc_codec::ShuffleProduceResponse;
}

/// Hook for the cross-node shuffle CONSUMER.
///
/// Sibling of [`ShuffleProducer`]: `nodedb-cluster` cannot depend on `nodedb`
/// (circular), so the consume logic — wait for both staged sides of the part to
/// finalize, resolve their local staged-file paths, run the node-local
/// grace-hash join through the Data Plane, and return the joined rows — lives in
/// `nodedb` behind this `Send + Sync` hook. The transport read-loop calls
/// [`on_shuffle_consume`](Self::on_shuffle_consume) when a `ShuffleConsumeRequest`
/// arrives and writes the returned [`ShuffleConsumeResponse`](crate::rpc_codec::ShuffleConsumeResponse)
/// back to the coordinator.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a `ShuffleConsume`
/// request against a node with no consumer installed returns a typed
/// "not configured" error.
///
/// The hook is **async** because the consume path awaits the per-side finalize
/// signal (bounded by the request deadline) on the Tokio transport reactor
/// before dispatching the grace join. The grace join itself runs on the Data
/// Plane via the host crate's local executor / SPSC bridge; this hook never
/// touches storage or io_uring directly.
#[async_trait::async_trait]
pub trait ShuffleConsumer: Send + Sync + 'static {
    /// Complete one part of a distributed shuffle join: wait for both staged
    /// sides to finalize, run the node-local grace join, and return the joined
    /// rows (or a typed error on missing inbox / finalize timeout / producer
    /// terminal error / join failure). Never hangs — the finalize wait is
    /// deadline-bounded.
    async fn on_shuffle_consume(
        &self,
        req: crate::rpc_codec::ShuffleConsumeRequest,
    ) -> crate::rpc_codec::ShuffleConsumeResponse;
}

/// Hook for the cross-node distributed GROUP BY shuffle CONSUMER.
///
/// SINGLE-SIDED aggregate sibling of [`ShuffleConsumer`]: `nodedb-cluster` cannot
/// depend on `nodedb` (circular), so the aggregate-consume logic — wait for the
/// part's ONE staged producer side (side 0) to finalize, resolve its local
/// staged-file path, merge + finalize the partial `GroupState`s through the Data
/// Plane, and return the result rows — lives in `nodedb` behind this `Send +
/// Sync` hook. The transport read-loop calls
/// [`on_shuffle_aggregate`](Self::on_shuffle_aggregate) when a
/// `ShuffleAggregateConsumeRequest` arrives and writes the returned
/// [`ShuffleAggregateConsumeResponse`](crate::rpc_codec::ShuffleAggregateConsumeResponse)
/// back to the coordinator.
///
/// Cluster-only tests leave the `RaftLoop` field `None`; a
/// `ShuffleAggregateConsume` request against a node with no aggregator installed
/// returns a typed "not configured" error.
///
/// The hook is **async** because the consume path awaits the single-side finalize
/// signal (bounded by the request deadline) on the Tokio transport reactor before
/// dispatching the merge. The merge + finalize itself runs on the Data Plane via
/// the host crate's local executor / SPSC bridge; this hook never touches storage
/// or io_uring directly. Unlike [`ShuffleConsumer`] it waits for only the single
/// producer side (`0`) — there is no probe side.
#[async_trait::async_trait]
pub trait ShuffleAggregator: Send + Sync + 'static {
    /// Complete one part of a distributed GROUP BY shuffle: wait for the part's
    /// single staged producer side to finalize, merge + finalize the partial
    /// `GroupState`s, and return the aggregate rows (or a typed error on missing
    /// inbox / finalize timeout / producer terminal error / merge failure). Never
    /// hangs — the finalize wait is deadline-bounded.
    async fn on_shuffle_aggregate(
        &self,
        req: crate::rpc_codec::ShuffleAggregateConsumeRequest,
    ) -> crate::rpc_codec::ShuffleAggregateConsumeResponse;
}
