// SPDX-License-Identifier: BUSL-1.1

//! Raft propose/compact callback type aliases and their serde defaults.

/// Type alias for the synchronous Raft propose callback.
///
/// Takes `(vshard_id, serialized_entry)` and returns `(group_id, log_index)`.
/// Works only when the current node is the group leader. Use
/// [`AsyncRaftProposer`] when proposals may originate from non-leader nodes.
pub type RaftProposer =
    dyn Fn(u32, Vec<u8>) -> std::result::Result<(u64, u64), crate::Error> + Send + Sync;

/// Type alias for the asynchronous Raft propose callback with leader forwarding.
///
/// Takes `(vshard_id, idempotency_key, serialized_entry, deadline)` and returns, on
/// success, the Data Plane apply payload bytes together with the write's
/// per-collection version: the written collection's `coll_write_lsn` AFTER the
/// write, as the replica that applied the entry recorded it. It is a WAL LSN,
/// minted by the apply path's own WAL append — NOT the Raft log index, which is
/// a per-group counter sharing no scale with the WAL LSNs the version index is
/// otherwise fed. Callers that need the write's version (e.g. a
/// read-your-writes floor for cross-shard OCC, which validates in the WAL-LSN
/// domain) read it here without a second lookup; it is `Lsn::ZERO` for a write
/// whose plan names no single user collection, which records no such version.
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
            dyn std::future::Future<
                    Output = std::result::Result<(Vec<u8>, crate::types::Lsn), crate::Error>,
                > + Send,
        >,
    > + Send
    + Sync;

/// Type alias for the Raft log-compaction callback.
///
/// Takes `(group_id, applied_index)` where `applied_index` is the index the
/// DATA-PLANE state machine has durably applied to (NOT raft's commit
/// index). Invoked from the apply-completion path so a log can only be
/// compacted up to an index the engines have actually persisted — never
/// past it, which would corrupt a rebuilt snapshot. Returns `true` when a
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
/// existed: a plain `INSERT`, matching the value `decode_sync_engines` used to
/// hardcode.
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
