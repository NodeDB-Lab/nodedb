// SPDX-License-Identifier: Apache-2.0

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
pub mod diag;
pub mod double_write;
pub mod error;
pub mod lazy_reader;
#[cfg(not(target_arch = "wasm32"))]
pub mod mmap_reader;
pub mod preamble;
pub mod reader;
pub mod record;
pub mod recovery;
pub mod replay;
pub mod secure_mem;
pub mod segment;
mod segment_envelope;
pub mod segmented;
pub mod temporal_purge;
pub mod tombstone;
pub mod torn_tail;
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub mod uring_writer;
pub mod writer;

pub use double_write::{
    DoubleWriteBuffer, DwbDegradation, DwbMirror, DwbMode, DwbProtection, DwbSkipReason,
    wal_dwb_bytes_written_total, wal_dwb_degradations_total, wal_dwb_unprotected_records_total,
};
pub use error::{Result, WalError};
pub use lazy_reader::LazyWalReader;
pub use preamble::{
    CIPHER_AES_256_GCM, PREAMBLE_SIZE, PREAMBLE_VERSION, SEG_PREAMBLE_MAGIC, SegmentPreamble,
    WAL_PREAMBLE_MAGIC,
};
pub use reader::{StopReason, WalReader};
pub use record::{
    CalvinAppliedPayload, FtsDeletePayload, FtsIndexPayload, RecordHeader, RecordTarget,
    RecordType, SpatialDeletePayload, SpatialPutPayload, WalRecord, WalRecordArgs,
    WriteAbortedPayload,
};
pub use recovery::{RecoveryInfo, recover};
pub use replay::{
    AbortedWrites, DatabaseTombstones, ReplayFilters, TombstoneSet, drop_aborted_records,
    extract_replay_filters, extract_tombstones,
};
pub use secure_mem::SecureKey;
pub use segmented::{SegmentedWal, SegmentedWalConfig};
pub use temporal_purge::{TemporalPurgeEngine, TemporalPurgePayload};
pub use tombstone::{CollectionTombstonePayload, MAX_COLLECTION_NAME_LEN};
pub use torn_tail::{TailVerdict, verify_committed_prefix};
pub use writer::WalWriter;
