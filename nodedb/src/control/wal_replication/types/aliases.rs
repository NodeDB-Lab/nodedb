// SPDX-License-Identifier: BUSL-1.1

//! Raft propose/compact callback type aliases and their serde defaults.

/// Type alias for the synchronous Raft propose callback.
///
/// Takes `(vshard_id, serialized_entry)` and returns `(group_id, log_index)`.
/// Works only when the current node is the group leader. Use
/// [`AsyncRaftProposer`] when proposals can originate from non-leader nodes.
pub type RaftProposer =
    dyn Fn(u32, Vec<u8>) -> std::result::Result<(u64, u64), crate::Error> + Send + Sync;

/// Type alias for the asynchronous Raft propose callback with leader forwarding.
///
/// Takes `(vshard_id, idempotency_key, serialized_entry, deadline)` and returns, on
/// success, an [`AppliedOutput`]: the Data Plane apply payload bytes and the
/// write's versions. A write's version is the data-group log position of the
/// entry that applied it, so every replica records the same version.
/// The `idempotency_key` matches the one embedded in the
/// serialized `ReplicatedEntry`; the proposer registers the tracker waiter with
/// this key so apply-side mismatch detection can surface `RetryableLeaderChange`
/// when a new leader's entry overwrites this one.
///
/// `deadline` is the caller's absolute statement deadline. A caller that
/// re-proposes passes the same instant to every attempt, so each attempt gets
/// only the time that remains. The proposer never computes a deadline of its
/// own. Past `deadline` it returns [`crate::Error::DeadlineExceeded`].
pub type AsyncRaftProposer = dyn Fn(
        u32,
        u64,
        Vec<u8>,
        tokio::time::Instant,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<AppliedOutput, crate::Error>>
                + Send,
        >,
    > + Send
    + Sync;

/// What this node's apply of a proposed write returns: the Data Plane apply
/// payload and the versions the write stamped, one per written vShard. A
/// write that names no single user collection stamps none.
pub type AppliedOutput = (Vec<u8>, crate::types::ReadVersions);

/// Where a proposed entry landed in its data group's Raft log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProposedAt {
    pub group_id: u64,
    pub log_index: u64,
}

/// The wait for this node's apply of a proposed entry: the result an
/// [`AsyncRaftProposer`] returns.
pub type AppliedWait = std::pin::Pin<
    Box<dyn std::future::Future<Output = std::result::Result<AppliedOutput, crate::Error>> + Send>,
>;

/// A proposal the group's leader accepted into its log.
pub struct ProposedWrite {
    /// Where the entry landed. `None` when the proposer applied the write
    /// before it returned, so no apply is outstanding.
    pub at: Option<ProposedAt>,
    /// This node's apply of the entry.
    pub applied: AppliedWait,
}

/// Type alias for the first phase of an [`AsyncRaftProposer`]: propose the
/// entry, and return once the leader holds it in its log, with the wait for
/// this node's apply of it.
///
/// Same arguments and deadline rule as [`AsyncRaftProposer`]. The admission
/// sequencer holds a vShard's slot across this phase only, so a later write
/// of the vShard proposes while an earlier one waits for its apply.
pub type AsyncRaftSubmit = dyn Fn(
        u32,
        u64,
        Vec<u8>,
        tokio::time::Instant,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<ProposedWrite, crate::Error>>
                + Send,
        >,
    > + Send
    + Sync;

/// Type alias for the Raft log-compaction callback.
///
/// Takes `(group_id, applied_index)` where `applied_index` is the index the
/// DATA-PLANE state machine has durably applied to (NOT raft's commit
/// index). Invoked from the apply-completion path so a log can only be
/// compacted up to an index the engines have actually persisted — never
/// past it, which corrupts a rebuilt snapshot. Returns `true` when a
/// compaction was performed. A no-op when the group's
/// `log_compaction_threshold` is `None`.
pub type RaftCompactor = dyn Fn(u64, u64) -> std::result::Result<bool, crate::Error> + Send + Sync;

/// Type alias for the durable Raft applied-index callback.
///
/// Takes `(group_id, applied_index)` and persists `applied_index` as the
/// group's durable applied floor. `applied_index` MUST name an entry whose
/// redo record the WAL has already fsynced — the next boot resumes Raft
/// delivery at `applied_index + 1`, so this is the sole thing keeping WAL
/// replay and Raft log replay from applying the same entry twice.
///
/// Distinct from raft's in-memory `last_applied`, which advances at ENQUEUE
/// time as the delivery watermark. Monotonic per group; an index at or below
/// the current floor is a no-op.
pub type RaftAppliedIndexSink =
    dyn Fn(u64, u64) -> std::result::Result<(), crate::Error> + Send + Sync;

pub(crate) fn default_pq_m() -> usize {
    crate::engine::vector::index_config::DEFAULT_PQ_M
}
/// Default `ColumnarIngest::intent` for a record written before that field
/// existed: a plain `INSERT`.
pub(crate) fn default_columnar_insert_intent()
-> nodedb_physical::physical_plan::ColumnarInsertIntent {
    nodedb_physical::physical_plan::ColumnarInsertIntent::Insert
}
/// Default `ColumnarIngest::format` for a record written before that field
/// existed: the sync path has only ever produced MessagePack payloads.
pub(crate) fn default_columnar_ingest_format() -> String {
    "msgpack".to_owned()
}
pub(crate) fn default_ivf_cells() -> usize {
    crate::engine::vector::index_config::DEFAULT_IVF_CELLS
}
pub(crate) fn default_ivf_nprobe() -> usize {
    crate::engine::vector::index_config::DEFAULT_IVF_NPROBE
}
