// SPDX-License-Identifier: BUSL-1.1

//! Dependent-read barrier for Calvin scheduler.
//!
//! When a dependent-read Calvin txn arrives at an active vshard (one with
//! write keys), the scheduler inserts a [`PendingDependentBarrier`] and waits
//! for all passive vshards to deliver their read results via the per-vshard
//! Raft log (`ReplicatedWrite::CalvinReadResult`).
//!
//! Once all passive vshards have delivered, the barrier assembles the
//! `injected_reads` map and dispatches `MetaOp::CalvinExecuteActive`.
//!
//! # Determinism
//!
//! All maps here use `BTreeMap`/`BTreeSet` — never `HashMap`/`HashSet`.
//!
//! # Timing
//!
//! `Instant::now()` is used for `timeout_at` (observability / off-WAL path
//! only; barrier timeouts are never written to the WAL).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use nodedb_cluster::calvin::types::SequencedTxn;

use super::super::lock_manager::TxnId;
use crate::types::TenantId;
use nodedb_physical::physical_plan::meta::PassiveReadKeyId;
use nodedb_types::Value;

// ── ReadResultEvent ────────────────────────────────────────────────────────────

/// A committed `CalvinReadResult` Raft entry delivered to the active vshard's
/// scheduler.
///
/// Produced by the apply loop when a `ReplicatedWrite::CalvinReadResult`
/// entry commits on a vshard whose scheduler has an active dependent barrier.
#[derive(Debug)]
pub struct ReadResultEvent {
    /// Sequencer epoch of the txn.
    pub epoch: u64,
    /// Position within the epoch batch.
    pub position: u32,
    /// Vshard that performed the read.
    pub passive_vshard: u32,
    /// Tenant scope.
    pub tenant_id: TenantId,
    /// Decoded read values from the passive participant.
    pub values: Vec<(PassiveReadKeyId, Value)>,
}

/// Read values received from passive vShards, keyed by passive vShard.
/// `BTreeMap` for determinism.
pub type ReceivedReads = BTreeMap<u32, Vec<(PassiveReadKeyId, Value)>>;

// ── PendingDependentBarrier ────────────────────────────────────────────────────

/// A dependent-read barrier: tracks which passive vshards have delivered
/// their read results and assembles the `injected_reads` map once all are in.
pub struct PendingDependentBarrier {
    /// Original sequenced transaction.
    pub txn: SequencedTxn,
    /// The lock-table owner id for this txn (equals the apply-slot id unless a
    /// reservation owns the lock). Carried through the barrier so the eventual
    /// `dispatch_active_txn` call can park the correct id in `PendingTxn`.
    pub lock_owner: TxnId,
    /// Passive vshards still waiting to deliver their read results.
    /// Entries are removed as `ReadResultEvent`s arrive.
    /// `BTreeSet` for determinism.
    pub waiting_for: BTreeSet<u32>,
    /// Received read values, keyed by passive vshard.
    pub received: ReceivedReads,
    /// Deadline for passive participant delivery.
    ///
    /// `Instant::now()` used for barrier timeout (observability / off-WAL path).
    pub timeout_at: Instant,
}

impl PendingDependentBarrier {
    /// Whether all passive participants have delivered.
    pub fn is_complete(&self) -> bool {
        self.waiting_for.is_empty()
    }

    /// Assemble the full `injected_reads` map from all received values.
    ///
    /// `BTreeMap` ensures deterministic iteration order across replicas.
    pub fn assemble_injected_reads(&self) -> BTreeMap<PassiveReadKeyId, Value> {
        let mut out = BTreeMap::new();
        for values in self.received.values() {
            for (key_id, value) in values {
                out.insert(key_id.clone(), value.clone());
            }
        }
        out
    }

    /// Whether this barrier has timed out.
    ///
    /// `Instant::now()` used for timeout check (observability; off-WAL path).
    pub fn is_timed_out(&self) -> bool {
        Instant::now() > self.timeout_at // no-determinism: scheduler barrier timeout check, not Calvin WAL data
    }
}
