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
pub mod double_write;
pub mod error;
pub mod group_commit;
pub mod lazy_reader;
pub mod mmap_reader;
pub mod reader;
pub mod record;
pub mod recovery;
pub mod replay;
pub mod segment;
pub mod segmented;
pub mod tombstone;
#[cfg(feature = "io-uring")]
pub mod uring_writer;
pub mod writer;

pub use double_write::{DoubleWriteBuffer, DwbMode, wal_dwb_bytes_written_total};
pub use error::{Result, WalError};
pub use group_commit::GroupCommitter;
pub use lazy_reader::LazyWalReader;
pub use record::{RecordHeader, RecordType, WalRecord};
pub use recovery::{RecoveryInfo, recover};
pub use replay::{TombstoneSet, extract_tombstones};
pub use segmented::{SegmentedWal, SegmentedWalConfig};
pub use tombstone::{CollectionTombstonePayload, MAX_COLLECTION_NAME_LEN};
pub use writer::WalWriter;
