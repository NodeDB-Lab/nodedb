// SPDX-License-Identifier: BUSL-1.1

//! This node's in-process Calvin state, shared between the per-vShard
//! schedulers and the Control-Plane paths that read or fence against them.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Arc, Mutex, OnceLock};

use crate::control::cluster::calvin::scheduler::SequencerProposer;
use crate::control::cluster::calvin::scheduler::lock::HotKeyTable;
use crate::control::cluster::calvin::scheduler::lock_manager::{LockManager, TxnId};

use super::calvin_apply::CalvinApplyResult;
use super::calvin_counters::CalvinCounters;
use super::calvin_cuts::CalvinCuts;

/// Per-vShard promotion senders, keyed by vShard id.
pub type PromotionSenders = BTreeMap<u32, tokio::sync::mpsc::UnboundedSender<Vec<TxnId>>>;

/// In-process Calvin state of this node. Empty in single-node deployments
/// that run no Calvin scheduler.
pub struct CalvinLocalState {
    /// Last globally-applied Calvin epoch, advanced by the per-vShard
    /// deterministic schedulers as they apply epochs. Read at `BEGIN` to anchor
    /// a session's cross-shard snapshot version (`tx_snapshot_epoch`). `Arc` so
    /// schedulers (holding `Arc<SharedState>`) advance the same counter the
    /// session reads. 0 in single-node / no-Calvin deployments.
    pub last_applied_epoch: Arc<AtomicU64>,
    /// Node-global Calvin observability counters (write versions recorded,
    /// read-set validation failures, commits flushed/dropped).
    pub counters: CalvinCounters,
    /// Local, in-process sidecar carrying the applied Data-Plane
    /// [`Response`](crate::bridge::envelope::Response) (affected-count and any
    /// RETURNING rows) of a completed Calvin transaction, keyed by its
    /// sequencer-assigned `TxnId`.
    ///
    /// RETURNING rows are a QUERY RESULT, not replicated state, so they MUST NOT
    /// ride the sequencer Raft log. The per-vShard scheduler deposits the applied
    /// `Response` here BEFORE proposing the replicated `CompletionAck`; the
    /// coordinator's completion path (static: `submit_and_await_calvin`;
    /// dependent: `dispatch_dependent_edge_recon`) drains it once completion
    /// fires. Every primary-write participant deposits (not RETURNING-only) — a
    /// multi-collection cross-shard COMMIT can have several plain-write
    /// participants, and those coalesce without conflict. Cross-node, the rows
    /// travel via the non-Raft routed-submit RPC response instead.
    ///
    /// Value is [`CalvinApplyResult`]: `Single` for a deposited (possibly
    /// coalesced) participant, or `Conflict` only when two RETURNING-bearing
    /// participants deposit for the same `TxnId` — a cross-shard RETURNING
    /// union, drained as a loud error, never a silent partial.
    pub apply_results: Arc<Mutex<HashMap<nodedb_cluster::calvin::TxnId, CalvinApplyResult>>>,
    /// Per-vShard deterministic lock managers, lifted out of each Calvin
    /// `Scheduler` so the Control-Plane write-admission gate shares the SAME
    /// `Arc<Mutex<LockManager>>` the scheduler holds — a fast-path point write and
    /// a Calvin txn's lock validation contend on one OS mutex, no TOCTOU gap.
    /// Keyed by vShard id; empty in single-node / no-Calvin deployments.
    pub lock_managers: Arc<Mutex<BTreeMap<u32, Arc<Mutex<LockManager>>>>>,
    /// Global hot-key detector for Calvin read reservations (CP-local heuristic).
    pub hot_key_table: Arc<Mutex<HotKeyTable>>,
    /// Per-vShard promotion channels, parallel to `lock_managers`. When a
    /// fast-path write guard releases an uncontended key on drop, `LockManager`
    /// may promote a scheduler txn queued behind it; the guard (Control-Plane,
    /// not in the scheduler task) forwards the promoted `TxnId`s here for the
    /// scheduler to dispatch. Unbounded (low-volume, sent from a non-blocking
    /// `Drop`). Keyed by vShard id; empty in single-node / no-Calvin deployments.
    pub promotion_senders: Arc<Mutex<PromotionSenders>>,
    /// Monotonic `position` source for autocommit fast-path lock holders. Paired
    /// with [`TxnId::AUTOCOMMIT_EPOCH`] to mint holder identities that never
    /// collide with a real Calvin `(epoch, position)` schedule position.
    pub autocommit_lock_seq: AtomicU32,
    /// The backup cut markers each local scheduler passed.
    pub cuts: CalvinCuts,
    /// Hands this node's sequencer entries to the sequencer Raft group. Set
    /// once the schedulers start; unset on a node that runs none.
    pub sequencer_proposer: OnceLock<Arc<dyn SequencerProposer>>,
}

impl CalvinLocalState {
    /// State of a node whose schedulers have applied nothing yet.
    pub fn new() -> Self {
        Self {
            last_applied_epoch: Arc::new(AtomicU64::new(0)),
            counters: CalvinCounters {
                write_versions_recorded: Arc::new(AtomicU64::new(0)),
                read_set_validation_failures: Arc::new(AtomicU64::new(0)),
                commits_flushed: Arc::new(AtomicU64::new(0)),
                commits_dropped: Arc::new(AtomicU64::new(0)),
            },
            apply_results: Arc::new(Mutex::new(HashMap::new())),
            lock_managers: Arc::new(Mutex::new(BTreeMap::new())),
            hot_key_table: Arc::new(Mutex::new(HotKeyTable::new())),
            promotion_senders: Arc::new(Mutex::new(BTreeMap::new())),
            autocommit_lock_seq: AtomicU32::new(0),
            cuts: CalvinCuts::default(),
            sequencer_proposer: OnceLock::new(),
        }
    }
}

impl Default for CalvinLocalState {
    fn default() -> Self {
        Self::new()
    }
}
