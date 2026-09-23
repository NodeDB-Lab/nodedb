// SPDX-License-Identifier: Apache-2.0

//! Typed error enum for the shared graph engine.
//!
//! Every fallible operation on `CsrIndex` (label interning, edge insert,
//! edge delete) returns `Result<T, GraphError>`. Silent casts or
//! `debug_assert!` at capacity boundaries reproduce the same class of
//! bug they are trying to catch — loud, typed errors only.

use nodedb_mem::MemError;
use thiserror::Error;

/// Hard upper bound on the number of distinct edge labels an individual
/// `CsrIndex` can intern. `u32::MAX` is the type-theoretic ceiling;
/// leaving one slot unused lets callers use `u32::MAX` as an "invalid"
/// sentinel should they need it.
pub const MAX_EDGE_LABELS: usize = (u32::MAX - 1) as usize;

/// Hard upper bound on the number of nodes a single `CsrIndex` partition
/// can hold. Each node occupies a `u32` dense index; at 4 GiB nodes the
/// index space is exhausted. `u32::MAX` is reserved as an "invalid"
/// sentinel, so the usable cap is `u32::MAX - 1`.
///
/// 4 billion nodes per collection is well beyond any realistic workload.
/// Inserts beyond this limit are rejected with `GraphError::NodeOverflow`
/// rather than silently wrapping the counter.
pub const MAX_NODES_PER_CSR: usize = (u32::MAX - 1) as usize;

/// Errors returned by graph-engine operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GraphError {
    /// The CSR's edge-label id space is exhausted. Happens only when
    /// more than `MAX_EDGE_LABELS` distinct labels have been interned
    /// — in practice unreachable because the DSL ingress caps label
    /// length and realistic workloads use orders of magnitude fewer
    /// labels. Surfaced here so the failure mode is a typed error, not
    /// a silent wrap (the bug this crate was shipping before).
    #[error("CSR edge-label id space exhausted ({used}/{MAX_EDGE_LABELS} labels interned)")]
    LabelOverflow { used: usize },

    /// The CSR's node id space is exhausted. A single `CsrIndex` partition
    /// supports at most `MAX_NODES_PER_CSR` (≈ 4 billion) distinct nodes.
    /// Inserts beyond this limit are rejected rather than silently wrapping
    /// the `u32` node index. Collections approaching this limit should be
    /// sharded across multiple partitions.
    #[error(
        "CSR node id space exhausted ({used}/{MAX_NODES_PER_CSR} nodes); \
         collection must be sharded to accept more nodes"
    )]
    NodeOverflow { used: usize },

    /// The memory governor rejected a reservation for a graph operation.
    ///
    /// Callers should apply backpressure and retry after memory is released.
    #[error("graph memory budget rejected: {0}")]
    MemoryBudget(#[from] MemError),

    /// A rollback asked to withdraw an interned node or node label that is
    /// not the newest one, or that something still refers to. Withdrawing it
    /// would renumber or orphan live state.
    #[error("cannot withdraw {kind} '{name}': it is not the newest {kind} or it is still in use")]
    WithdrawRefused { kind: &'static str, name: String },
}
