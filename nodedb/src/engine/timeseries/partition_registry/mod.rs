//! Partition registry — tracks all partitions for a timeseries collection.
//!
//! Maintains a `BTreeMap<i64, PartitionEntry>` keyed by partition start
//! timestamp. Supports O(log P) pruning for time-range queries, partition
//! lifecycle management (create/seal/merge/delete), and the adaptive
//! interval algorithm for AUTO mode.
//!
//! The registry is the partition manifest: all state transitions are
//! recorded here. On crash recovery, replay the manifest to reach a
//! consistent state.

pub mod entry;
pub mod lifecycle;
pub mod persistence;
pub mod query;
pub mod rate;
pub mod registry;

#[cfg(test)]
mod tests;

pub use entry::{PartitionEntry, format_partition_dir};
pub use rate::RateEstimator;
pub use registry::PartitionRegistry;
