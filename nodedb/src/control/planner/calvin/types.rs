// SPDX-License-Identifier: BUSL-1.1

//! Calvin dispatch classification types.

use std::collections::BTreeSet;

use crate::types::VShardId;

/// Classification of a task set by the number of distinct write vShards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchClass {
    /// All write tasks target one vshard (or there are no writes).
    SingleShard { vshard: VShardId },
    /// Write tasks span two or more vShards — requires Calvin or best-effort.
    /// `BTreeSet` mandatory for determinism contract.
    MultiShard { vshards: BTreeSet<u32> },
}

/// Where in a transaction's lifecycle a cross-shard dispatch originates.
///
/// Only a single statement executed *mid-block* is rejected: a cross-shard span
/// there cannot be buffered atomically alongside the block's other writes. An
/// autocommit statement and the COMMIT-time flush of a buffered explicit block
/// each commit their whole batch atomically through Calvin, so both proceed —
/// the flush is exactly the batch the block accumulated one single-shard write
/// at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnDispatchPosition {
    /// Autocommit statement — no explicit transaction block.
    Autocommit,
    /// A single statement executed inside an explicit transaction block. A
    /// cross-shard span here is rejected with `CrossShardInExplicitTransaction`,
    /// because it cannot be buffered atomically with the block's other writes.
    MidBlockStatement,
    /// The COMMIT flush of a buffered explicit-block batch. The whole batch
    /// commits atomically through Calvin, so a cross-shard span is permitted.
    CommitFlush,
}

/// Outcome returned by `dispatch_calvin_or_fast`.
#[derive(Debug)]
pub enum DispatchOutcome {
    /// Dispatched via the single-shard fast path.
    SingleShard,
    /// Submitted to the Calvin sequencer (static path).
    CalvinStatic { inbox_seq: u64 },
    /// OLLP dependent-read path submitted successfully.
    CalvinDependent { inbox_seq: u64 },
    /// Best-effort non-atomic: each vshard dispatched independently.
    BestEffortNonAtomic,
}
