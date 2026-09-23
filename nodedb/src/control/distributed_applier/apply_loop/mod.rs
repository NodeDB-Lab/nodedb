// SPDX-License-Identifier: BUSL-1.1

//! Background apply loop — reads committed Raft entries from the mpsc channel,
//! submits them through the shared Control-Plane write funnel (which appends
//! each entry's redo record on THIS replica before the enqueue), and resolves
//! propose waiters with the result.
//!
//! Each batch advances the group's durable applied floor (see
//! [`super::applied_index`]) to its highest contiguous successfully-applied
//! entry, so the next boot replays only above it and no entry is applied by
//! both WAL replay and Raft log replay.
//!
//! Split by concern:
//! - [`driver`]: the per-batch loop that reads off the apply channel and
//!   dispatches each entry, then lands the batch's durable applied floor.
//! - [`array_dispatch`]: the Array CRDT `ArrayOp` / `ArraySchema` apply path.
//! - [`calvin_read_result`]: forwards a committed `CalvinReadResult` entry to
//!   the local Calvin scheduler.
//! - [`write_dispatch`]: the generic decode + Data-Plane `submit_write` path.
//! - [`bookkeeping`]: applied-floor persistence + Raft log compaction trigger.
//! - [`helpers`]: shared response/result classification helpers.

mod array_dispatch;
mod bookkeeping;
mod calvin_read_result;
mod driver;
mod helpers;
mod write_dispatch;

pub use driver::run_apply_loop;
