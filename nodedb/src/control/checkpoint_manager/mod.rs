// SPDX-License-Identifier: BUSL-1.1

//! Coordinated checkpoint manager.
//!
//! Periodically dispatches `PhysicalPlan::Checkpoint` to all Data Plane cores,
//! collects their checkpoint LSNs, and truncates the WAL up to the global
//! minimum LSN — but only when every core has reported.
//!
//! ## How it works
//!
//! 1. The manager sends a `Checkpoint` request to every core via the Dispatcher.
//! 2. Each core flushes its engine state (vectors, CRDTs) and responds with
//!    its watermark LSN.
//! 3. The manager collects all responses. If any core failed to dispatch,
//!    missed its response deadline, or otherwise did not report a fresh
//!    flush LSN, the whole cycle is deferred — no marker, no truncation,
//!    no tombstone GC — and retried next cycle. Only when every core has
//!    reported does the global checkpoint LSN become the **minimum**
//!    across all cores, ensuring no core has unflushed state above the
//!    truncation point.
//! 4. The Event Plane's persisted watermarks are folded into that minimum as
//!    an equal floor. Its consumers recover only from the WAL suffix above
//!    them, so a truncation past a lagging consumer silently drops every CDC
//!    row, trigger fire and streaming-MV update in the gap.
//! 5. A `RecordType::Checkpoint` WAL record is written at the global LSN.
//! 6. When cold storage is configured, the WAL archiver uploads every sealed
//!    segment the archive does not hold yet. The lowest unarchived segment
//!    bounds the truncation point, so no segment is deleted before it is in
//!    cold storage. The archiver also runs on its own shorter tick.
//! 7. `WalManager::truncate_before()` deletes old WAL segments.
//!
//! ## Frequency
//!
//! Default: every 5 minutes (matches the existing vector checkpoint interval).
//! Configurable via `CheckpointManagerConfig`.
//!
//! - [`config`]: the manager's intervals.
//! - [`truncation`]: the truncation point of one cycle.
//! - [`cycle`]: one checkpoint cycle.

pub mod config;
pub mod cycle;
pub mod truncation;

pub use config::CheckpointManagerConfig;
pub use cycle::{CheckpointCycleInputs, run_checkpoint_cycle};
