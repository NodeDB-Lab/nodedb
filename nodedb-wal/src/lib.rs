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
//!
//! ## Targets
//!
//! On wasm32 the crate builds only [`crypto`], the key and envelope code it
//! needs, [`error`], and the record-header constants in [`record::header`].
//! The writer, readers, segments, double-write buffer, replay, and recovery
//! build only on native targets.

#[cfg(not(target_arch = "wasm32"))]
pub mod align;
pub mod crypto;
#[cfg(not(target_arch = "wasm32"))]
pub mod diag;
#[cfg(not(target_arch = "wasm32"))]
pub mod double_write;
pub mod error;
#[cfg(not(target_arch = "wasm32"))]
pub mod lazy_reader;
#[cfg(not(target_arch = "wasm32"))]
pub mod mmap_reader;
#[cfg(not(target_arch = "wasm32"))]
pub mod preamble;
#[cfg(not(target_arch = "wasm32"))]
pub mod reader;
pub mod record;
#[cfg(not(target_arch = "wasm32"))]
pub mod recovery;
#[cfg(not(target_arch = "wasm32"))]
pub mod replay;
pub mod secure_mem;
#[cfg(not(target_arch = "wasm32"))]
pub mod segment;
mod segment_envelope;
#[cfg(not(target_arch = "wasm32"))]
pub mod segmented;
#[cfg(not(target_arch = "wasm32"))]
pub mod temporal_purge;
#[cfg(not(target_arch = "wasm32"))]
pub mod time_anchors;
#[cfg(not(target_arch = "wasm32"))]
pub mod tombstone;
#[cfg(not(target_arch = "wasm32"))]
pub mod torn_tail;
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub mod uring_writer;
#[cfg(not(target_arch = "wasm32"))]
pub mod writer;

#[cfg(not(target_arch = "wasm32"))]
pub use double_write::{
    DoubleWriteBuffer, DwbDegradation, DwbMirror, DwbMode, DwbProtection, DwbSkipReason,
    wal_dwb_bytes_written_total, wal_dwb_degradations_total, wal_dwb_unprotected_records_total,
};
pub use error::{Result, WalError};
#[cfg(not(target_arch = "wasm32"))]
pub use lazy_reader::LazyWalReader;
#[cfg(not(target_arch = "wasm32"))]
pub use preamble::{
    CIPHER_AES_256_GCM, PREAMBLE_SIZE, PREAMBLE_VERSION, SEG_PREAMBLE_MAGIC, SegmentPreamble,
    WAL_PREAMBLE_MAGIC,
};
#[cfg(not(target_arch = "wasm32"))]
pub use reader::{StopReason, WalReader};
#[cfg(not(target_arch = "wasm32"))]
pub use record::{
    FtsDeletePayload, FtsIndexPayload, RecordStamp, RecordTarget, RecordType, SpatialDeletePayload,
    SpatialPutPayload, WalRecord, WalRecordArgs, WriteAbortedPayload,
};
pub use record::{NO_EVENT_SOURCE, RecordHeader};
#[cfg(not(target_arch = "wasm32"))]
pub use recovery::{RecoveryInfo, recover};
#[cfg(not(target_arch = "wasm32"))]
pub use replay::{
    AbortedWrites, DatabaseTombstones, ReplayFilters, TombstoneSet, drop_aborted_records,
    extract_replay_filters, extract_tombstones,
};
pub use secure_mem::SecureKey;
#[cfg(not(target_arch = "wasm32"))]
pub use segmented::{SegmentedWal, SegmentedWalConfig};
#[cfg(not(target_arch = "wasm32"))]
pub use temporal_purge::{TemporalPurgeEngine, TemporalPurgePayload};
#[cfg(not(target_arch = "wasm32"))]
pub use time_anchors::TimeAnchors;
#[cfg(not(target_arch = "wasm32"))]
pub use tombstone::{CollectionTombstonePayload, MAX_COLLECTION_NAME_LEN};
#[cfg(not(target_arch = "wasm32"))]
pub use torn_tail::{TailVerdict, verify_committed_prefix};
#[cfg(not(target_arch = "wasm32"))]
pub use writer::WalWriter;
