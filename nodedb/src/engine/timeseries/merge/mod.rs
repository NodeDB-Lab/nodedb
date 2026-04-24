//! Partition merge executor.
//!
//! Merges multiple sealed partitions into one, combining their columnar
//! data and unifying symbol dictionaries. Used by the background merge job.
//!
//! Crash safety protocol:
//! 1. Write merged partition to new directory
//! 2. Single manifest transaction: insert merged, mark sources Deleted
//! 3. Background cleanup: physically remove source directories
//!
//! If crash at step 1: partial output cleaned on recovery (no manifest entry).
//! If crash at step 2: redb transaction is atomic.
//! If crash at step 3: manifest says Deleted, recovery cleans orphans.

pub mod cycle;
pub mod o3;
pub mod partitions;

#[cfg(test)]
mod tests;

pub use cycle::run_merge_cycle;
pub use o3::merge_o3_into_partition;
pub use partitions::{MergeResult, merge_partitions};
