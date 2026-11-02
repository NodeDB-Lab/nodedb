// SPDX-License-Identifier: BUSL-1.1

//! Partition registry — tracks all partitions for a timeseries collection.
//!
//! Maintains a `BTreeMap<PartitionKey, PartitionEntry>` keyed by each
//! partition's `min_ts` and directory name. Supports O(log P) pruning for time-range queries, partition
//! lifecycle management (create/seal/merge/delete), and the adaptive
//! interval algorithm for AUTO mode.
//!
//! The registry is the partition manifest: all state transitions are
//! recorded here. On crash recovery, replay the manifest to reach a
//! consistent state.

pub mod boundary;
pub mod entry;
pub mod lifecycle;
pub mod listing;
pub mod persistence;
pub mod query;
pub mod rate;
pub mod registry;

pub use boundary::interval_bounds;
pub use entry::{PartitionEntry, PartitionKey, format_partition_dir};
pub use listing::PartitionListing;
pub use rate::RateEstimator;
pub use registry::PartitionRegistry;
