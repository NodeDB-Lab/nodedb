//! Distributed join execution: broadcast and shuffle joins across cluster nodes.
//!
//! **Broadcast join**: Control Plane serializes the small side (< 8 MiB),
//! sends it to all relevant nodes via QUIC transport. Each node performs
//! a local hash join with its local large-side data.
//!
//! **Shuffle join**: each node scans its local data, hashes on the join key,
//! routes rows to the owning node via QUIC transport. The target node
//! performs a local hash join on the repartitioned data.
//!
//! Both strategies use Arrow IPC for zero-copy batched data movement.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::routing::RoutingTable;

/// Default maximum payload size for broadcast join strategy selection.
///
/// Above this threshold, shuffle join is preferred over broadcast.
/// Corresponds to `ClusterTransportTuning::broadcast_threshold_bytes`.
pub const DEFAULT_BROADCAST_THRESHOLD_BYTES: usize = 8 * 1024 * 1024; // 8 MiB

/// A broadcast join request sent to each participating node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastJoinRequest {
    /// The small-side data serialized as MessagePack `Vec<(doc_id, doc_bytes)>`.
    pub broadcast_data: Vec<u8>,
    /// The large-side collection to scan locally.
    pub large_collection: String,
    /// Join keys: `[(large_field, small_field)]`.
    pub on_keys: Vec<(String, String)>,
    /// Join type.
    pub join_type: String,
    /// Result limit per node.
    pub limit: usize,
    /// Tenant scope.
    pub tenant_id: u32,
}

/// A shuffle join partition assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShufflePartition {
    /// Rows assigned to this partition, serialized as Arrow IPC bytes.
    pub data: Vec<u8>,
    /// Which join side this partition belongs to.
    pub side: JoinSide,
    /// Target node that owns this partition.
    pub target_node: u64,
    /// Join key hash that determined this partition.
    pub partition_id: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum JoinSide {
    Left,
    Right,
}

/// Determine the join strategy based on estimated data sizes.
///
/// Returns `Broadcast` if the smaller side fits within `broadcast_threshold_bytes`,
/// otherwise `Shuffle`. Pass `DEFAULT_BROADCAST_THRESHOLD_BYTES` when no runtime
/// config is available.
pub fn select_strategy(
    left_estimated_bytes: usize,
    right_estimated_bytes: usize,
    broadcast_threshold_bytes: usize,
) -> JoinStrategy {
    let (smaller, _larger) = if left_estimated_bytes <= right_estimated_bytes {
        (left_estimated_bytes, right_estimated_bytes)
    } else {
        (right_estimated_bytes, left_estimated_bytes)
    };

    if smaller <= broadcast_threshold_bytes {
        JoinStrategy::Broadcast {
            broadcast_side: if left_estimated_bytes <= right_estimated_bytes {
                JoinSide::Left
            } else {
                JoinSide::Right
            },
        }
    } else {
        JoinStrategy::Shuffle
    }
}

/// Selected distributed join strategy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinStrategy {
    /// Broadcast the small side to all nodes.
    Broadcast { broadcast_side: JoinSide },
    /// Shuffle both sides by join key hash.
    Shuffle,
}

/// Compute which node owns a given partition (based on join key hash).
///
/// Uses consistent hashing: `partition = hash(key) % num_nodes`.
/// The target node is selected from the routing table's active leaders.
pub fn partition_for_key(key: &str, num_partitions: usize) -> u32 {
    (crate::routing::fnv1a_hash(key) % num_partitions as u64) as u32
}

/// Plan the node assignments for a shuffle join.
///
/// Returns `(partition_id → target_node_id)` mapping.
///
/// Only data groups (group_id > METADATA_GROUP_ID) are used as shuffle
/// targets. The metadata group (0) is excluded — it does not own any vShards
/// and its leader should not receive data-plane shuffle traffic.
pub fn plan_shuffle_partitions(routing: &RoutingTable, num_partitions: usize) -> HashMap<u32, u64> {
    use crate::metadata_group::METADATA_GROUP_ID;
    let mut group_ids: Vec<u64> = routing
        .group_ids()
        .into_iter()
        .filter(|&id| id != METADATA_GROUP_ID)
        .collect();
    group_ids.sort_unstable();
    let mut partition_map = HashMap::new();

    if group_ids.is_empty() {
        return partition_map;
    }

    for p in 0..num_partitions {
        let group_idx = p % group_ids.len();
        let group_id = group_ids[group_idx];
        let leader = routing.group_info(group_id).map(|g| g.leader).unwrap_or(0);
        partition_map.insert(p as u32, leader);
    }

    debug!(
        num_partitions,
        num_groups = group_ids.len(),
        "shuffle partition plan computed"
    );
    partition_map
}

/// Estimate the serialized size of a collection's data.
///
/// This is a rough estimate based on document count × average document size.
/// Used for broadcast/shuffle strategy selection.
pub fn estimate_collection_bytes(doc_count: usize, avg_doc_bytes: usize) -> usize {
    doc_count * avg_doc_bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broadcast_selected_for_small_side() {
        let strategy = select_strategy(1_000, 100_000_000, DEFAULT_BROADCAST_THRESHOLD_BYTES);
        assert!(matches!(
            strategy,
            JoinStrategy::Broadcast {
                broadcast_side: JoinSide::Left
            }
        ));
    }

    #[test]
    fn shuffle_selected_for_large_sides() {
        let strategy = select_strategy(100_000_000, 200_000_000, DEFAULT_BROADCAST_THRESHOLD_BYTES);
        assert_eq!(strategy, JoinStrategy::Shuffle);
    }

    #[test]
    fn partition_deterministic() {
        let p1 = partition_for_key("alice", 16);
        let p2 = partition_for_key("alice", 16);
        assert_eq!(p1, p2);

        // Different keys should (usually) get different partitions.
        let p3 = partition_for_key("bob", 16);
        // Not guaranteed different, but statistically likely with 16 partitions.
        let _ = p3;
    }

    #[test]
    fn shuffle_plan_covers_all_partitions() {
        let routing = RoutingTable::uniform(4, &[1, 2, 3], 2);
        let plan = plan_shuffle_partitions(&routing, 8);
        assert_eq!(plan.len(), 8);
        // All partitions should be assigned.
        for p in 0..8u32 {
            assert!(plan.contains_key(&p));
        }
    }

    #[test]
    fn broadcast_threshold() {
        // Exactly at threshold → broadcast.
        let strategy = select_strategy(
            DEFAULT_BROADCAST_THRESHOLD_BYTES,
            100_000_000,
            DEFAULT_BROADCAST_THRESHOLD_BYTES,
        );
        assert!(matches!(strategy, JoinStrategy::Broadcast { .. }));

        // Over threshold → shuffle.
        let strategy = select_strategy(
            DEFAULT_BROADCAST_THRESHOLD_BYTES + 1,
            100_000_000,
            DEFAULT_BROADCAST_THRESHOLD_BYTES,
        );
        assert_eq!(strategy, JoinStrategy::Shuffle);
    }
}
