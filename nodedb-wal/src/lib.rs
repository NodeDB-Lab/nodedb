//! # nodedb-wal
//!
//! Deterministic, O_DIRECT write-ahead log with group commit.
//!
//! This crate bypasses the Linux page cache entirely. Every WAL write goes
//! directly to NVMe via `O_DIRECT` (and eventually `io_uring`). This is
//! non-negotiable: if AI agents dump 10 GB of telemetry logs, the OS must NOT
//! evict hot HNSW vector indexes from RAM to cache WAL pages.
//!
//! ## Design
//!
//! - **O_DIRECT**: All writes bypass the page cache. Aligned to 4 KiB.
//! - **Group commit**: Thousands of concurrent writes are batched into a single
//!   `fsync`, maximizing NVMe IOPS.
//! - **CRC32C**: Every record has a checksum for silent bit-rot detection.
//! - **Deterministic replay**: WAL replay is idempotent — crash at any point,
//!   recover to a consistent prefix.
//!
//! ## Validation target
//!
//! Sustain 100,000+ async writes/sec with sub-millisecond p99 latency.
//! `free -m` cached memory must not move during the benchmark.

pub mod align;
pub mod crypto;
pub mod error;
pub mod group_commit;
pub mod reader;
pub mod record;
pub mod recovery;
#[cfg(feature = "io-uring")]
pub mod uring_writer;
pub mod writer;

pub use error::{Result, WalError};
pub use group_commit::GroupCommitter;
pub use record::{RecordHeader, RecordType, WalRecord};
pub use recovery::{RecoveryInfo, recover};
pub use writer::WalWriter;
