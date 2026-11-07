// SPDX-License-Identifier: Apache-2.0

//! WAL writer with O_DIRECT and group commit.
//!
//! The writer accumulates records into an aligned buffer and flushes to disk
//! when the buffer is full or when an explicit sync is requested.
//!
//! ## I/O path
//!
//! 1. Caller creates a `WalRecord` and submits it to the writer.
//! 2. Writer serializes the record into the aligned write buffer.
//! 3. When the buffer is full or `sync()` is called, the buffer is written
//!    to the WAL file via `O_DIRECT` + `fsync`.
//! 4. Group commit: multiple concurrent writers can submit records between
//!    syncs, and they all share a single `fsync` call.
//!
//! ## Future: io_uring
//!
//! The current implementation uses standard `pwrite` + `fsync` with O_DIRECT.
//! io_uring submission can be added once the bridge crate provides the TPC
//! event loop integration.

mod anchor;
// Crate-visible so the io_uring writer can reach `resume_offset` directly.
// Re-exporting it here instead would be dead code whenever the `io-uring`
// feature is off, and cfg-gating the re-export just duplicates that condition.
pub(crate) mod config;
mod core;
// Crate-visible for the same reason as `config`: the io_uring writer shares
// the flushed-but-unsynced tracking without going through `WalWriter`.
pub(crate) mod durability;
mod dwb;
mod flush;
mod open;
mod split_sync;

pub use config::{DEFAULT_WRITE_BUFFER_SIZE, WalWriterConfig};
pub use core::WalWriter;
pub use split_sync::PendingSync;
