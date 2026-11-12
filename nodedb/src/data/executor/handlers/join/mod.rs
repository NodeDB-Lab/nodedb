// SPDX-License-Identifier: BUSL-1.1

//! Join execution handlers — hash, sort-merge, broadcast, nested-loop, and lateral.

mod budget_guard;
mod grace_drive;
mod grace_null_aware;
pub(super) mod grace_partitioner;
mod grace_probe;
mod grace_repartition;
mod grace_spill;
pub mod hash;
mod hash_handlers;
mod hash_index;
pub mod lateral;
mod merge_left_only;
mod merge_pairs;
pub mod nested_loop;
mod null_aware_tuple;
pub mod params;
mod row_source;
mod shuffle_join;
mod side_rows;
pub mod sort_merge;
mod spill;
mod support;

pub(crate) use params::{HashJoinParams, JoinParams, NestedLoopJoinParams, SortMergeJoinParams};
// Node-local shuffle-join completion inputs — reconstructed in the Data-Plane
// `QueryOp::ShuffleJoinConsume` dispatch arm (E4b).
pub(in crate::data::executor) use shuffle_join::ShuffleJoinInputs;

// Streaming frame reader over `[u32 LE len][row-bytes]` staged shuffle files.
// Shared with the distributed-aggregate consumer
// (`QueryOp::ShuffleAggregateConsume`), which reads the same frame format.
pub(in crate::data::executor) use grace_repartition::FrameStreamReader;

// Framed spill writer shared with the transaction spill tier.
pub(in crate::data::executor) use spill::SpillPartitionWriter;

// `merge_join_docs_binary` is exercised directly by an integration test, so it
// stays crate-public. The rest are join-internal helpers (private re-export,
// visible to the join submodules via `super::`).
pub use support::merge_join_docs_binary;
use support::{binary_row_matches_filters, binary_row_project, compare_join_keys};
