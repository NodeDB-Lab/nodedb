// SPDX-License-Identifier: BUSL-1.1

//! Distributed pattern matching — cross-shard scatter-gather for MATCH.
//!
//! When a MATCH pattern encounters a ghost edge (destination on another shard),
//! the partial binding row is sent to the target shard for continuation.
//! The target shard resumes pattern expansion from the ghost destination node.
//!
//! Protocol:
//! 1. Coordinator broadcasts MATCH query to all shards.
//! 2. Each shard executes the pattern locally on its CSR.
//! 3. When a triple crosses a shard boundary (ghost edge):
//!    - The shard packages the partial binding row + remaining pattern triples.
//!    - Sends a `PatternContinuation` to the target shard.
//! 4. Target shard resumes execution with the partial bindings.
//! 5. Results from all shards are merged by the coordinator.
//! 6. Iterate until no new continuations are pending.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// One MATCH binding row: each variable's typed value. A node binds its
/// name, an edge its text, a projected column its exact stored value.
pub type MatchBindings = HashMap<String, nodedb_types::Value>;

/// A partial MATCH result that needs continuation on another shard.
///
/// Contains the current variable bindings and the index of the next
/// triple to execute in the pattern chain. The target shard resumes
/// from `next_triple_idx` using the provided bindings.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PatternContinuation {
    /// Target shard that should continue execution.
    pub target_shard: u32,
    /// Source shard that generated this continuation.
    pub source_shard: u32,
    /// Current variable bindings (variable → typed value).
    pub bindings: MatchBindings,
    /// Index of the next triple to execute in the chain.
    pub next_triple_idx: usize,
    /// The ghost node name that the target shard should start from.
    pub start_node: String,
    /// The binding variable name for the start node.
    pub start_binding: String,
}

/// Coordinator response from a shard for distributed MATCH.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ShardMatchResult {
    /// Shard that produced these results.
    pub shard_id: u32,
    /// Completed binding rows (fully matched patterns).
    pub completed_rows: Vec<MatchBindings>,
    /// Partial rows that need continuation on other shards.
    pub continuations: Vec<PatternContinuation>,
}

/// Coordinator state for distributed MATCH execution.
///
/// Tracks pending continuations and completed results across rounds
/// of scatter-gather until all continuations are resolved.
#[derive(Debug)]
pub struct DistributedMatchCoordinator {
    /// Completed result rows from all shards.
    pub completed: Vec<MatchBindings>,
    /// Pending continuations grouped by target shard.
    pub pending: HashMap<u32, Vec<PatternContinuation>>,
    /// Round counter (for debugging / max-round termination).
    pub round: u32,
    /// Maximum rounds before forced termination (prevent infinite loops).
    pub max_rounds: u32,
}

/// Raw routing-resolved fields for [`PatternContinuation::from_resolved`].
pub struct ResolvedContinuationArgs {
    pub target_shard: u32,
    pub source_shard: u32,
    pub bindings: MatchBindings,
    pub next_triple_idx: usize,
    pub start_node: String,
    pub start_binding: String,
}

impl PatternContinuation {
    /// Construct a continuation from its raw routing-resolved fields.
    ///
    /// The Control Plane resolves `target_shard` (the owning vShard of the
    /// frontier node) and `source_shard` (the vShard that emitted the frontier
    /// entry) via its routing table, then assembles the continuation here. The
    /// routing classification itself stays in the CP layer — `nodedb-cluster`
    /// has no access to the routing table — but the field assembly lives with
    /// the type so the shape cannot drift from the struct definition.
    pub fn from_resolved(args: ResolvedContinuationArgs) -> Self {
        let ResolvedContinuationArgs {
            target_shard,
            source_shard,
            bindings,
            next_triple_idx,
            start_node,
            start_binding,
        } = args;
        Self {
            target_shard,
            source_shard,
            bindings,
            next_triple_idx,
            start_node,
            start_binding,
        }
    }
}

impl DistributedMatchCoordinator {
    pub fn new(max_rounds: u32) -> Self {
        Self {
            completed: Vec::new(),
            pending: HashMap::new(),
            round: 0,
            max_rounds,
        }
    }

    /// Ingest results from a shard.
    pub fn add_shard_result(&mut self, result: ShardMatchResult) {
        self.completed.extend(result.completed_rows);
        for cont in result.continuations {
            self.pending
                .entry(cont.target_shard)
                .or_default()
                .push(cont);
        }
    }

    /// Check if there are pending continuations to dispatch.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Take all pending continuations for a target shard.
    pub fn take_pending(&mut self, shard_id: u32) -> Vec<PatternContinuation> {
        self.pending.remove(&shard_id).unwrap_or_default()
    }

    /// Take all pending continuations, grouped by target shard.
    pub fn take_all_pending(&mut self) -> HashMap<u32, Vec<PatternContinuation>> {
        std::mem::take(&mut self.pending)
    }

    /// Advance to next round. Returns `false` if max rounds reached.
    pub fn advance(&mut self) -> bool {
        self.round += 1;
        self.round < self.max_rounds
    }

    /// Total completed rows.
    pub fn result_count(&self) -> usize {
        self.completed.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pattern_continuation_serde() {
        let cont = PatternContinuation {
            target_shard: 2,
            source_shard: 0,
            bindings: [("a".into(), "alice".into())].into_iter().collect(),
            next_triple_idx: 1,
            start_node: "bob".into(),
            start_binding: "b".into(),
        };
        let bytes = zerompk::to_msgpack_vec(&cont).unwrap();
        let decoded: PatternContinuation = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.target_shard, 2);
        assert_eq!(decoded.start_node, "bob");
        assert_eq!(decoded.bindings["a"], nodedb_types::Value::from("alice"));
    }

    #[test]
    fn shard_match_result_serde() {
        let result = ShardMatchResult {
            shard_id: 1,
            completed_rows: vec![
                [("a".into(), "alice".into()), ("b".into(), "bob".into())]
                    .into_iter()
                    .collect(),
            ],
            continuations: vec![],
        };
        let bytes = zerompk::to_msgpack_vec(&result).unwrap();
        let decoded: ShardMatchResult = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.completed_rows.len(), 1);
    }

    #[test]
    fn coordinator_collects_results() {
        let mut coord = DistributedMatchCoordinator::new(10);

        coord.add_shard_result(ShardMatchResult {
            shard_id: 0,
            completed_rows: vec![[("a".into(), "alice".into())].into_iter().collect()],
            continuations: vec![PatternContinuation {
                target_shard: 1,
                source_shard: 0,
                bindings: [("a".into(), "alice".into())].into_iter().collect(),
                next_triple_idx: 1,
                start_node: "bob".into(),
                start_binding: "b".into(),
            }],
        });

        assert_eq!(coord.result_count(), 1);
        assert!(coord.has_pending());
        assert_eq!(coord.take_pending(1).len(), 1);
        assert!(!coord.has_pending());
    }

    #[test]
    fn coordinator_multi_round() {
        let mut coord = DistributedMatchCoordinator::new(5);

        // Round 1: shard 0 produces 2 completed + 1 continuation.
        coord.add_shard_result(ShardMatchResult {
            shard_id: 0,
            completed_rows: vec![
                [("x".into(), "1".into())].into_iter().collect(),
                [("x".into(), "2".into())].into_iter().collect(),
            ],
            continuations: vec![PatternContinuation {
                target_shard: 1,
                source_shard: 0,
                bindings: HashMap::new(),
                next_triple_idx: 0,
                start_node: "n".into(),
                start_binding: "a".into(),
            }],
        });

        assert!(coord.advance()); // Round 1 → 2.

        // Round 2: shard 1 completes the continuation.
        let pending = coord.take_all_pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[&1].len(), 1);

        coord.add_shard_result(ShardMatchResult {
            shard_id: 1,
            completed_rows: vec![[("x".into(), "3".into())].into_iter().collect()],
            continuations: vec![],
        });

        assert!(!coord.has_pending());
        assert_eq!(coord.result_count(), 3);
    }

    #[test]
    fn coordinator_max_rounds() {
        let mut coord = DistributedMatchCoordinator::new(2);
        assert!(coord.advance()); // round 1
        assert!(!coord.advance()); // round 2 = max
    }

    #[test]
    fn coordinator_no_pending_initially() {
        let coord = DistributedMatchCoordinator::new(10);
        assert!(!coord.has_pending());
        assert_eq!(coord.result_count(), 0);
    }
}
