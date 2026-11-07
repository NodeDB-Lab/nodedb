// SPDX-License-Identifier: BUSL-1.1

//! Black-box recorder wiring for the Calvin sequencer's silent data-loss
//! sites.
//!
//! An epoch gap and a backpressure-dropped transaction both leave a
//! registered completion waiter with nothing that will ever satisfy it — the
//! existing `error!`/`warn!` lines and counters are the only trace, and
//! neither identifies the affected transactions well enough to act on after
//! the fact. These entry points file a structured report at the site that
//! detects the drop, without changing what that site returns or does next.
//! A Raft group's disk reports its write errors the same way, and a Calvin
//! verdict that conflicts with the stored one too.
//!
//! One report per root cause: a report is filed where the invariant is known
//! and nowhere else. Mirrors `nodedb-wal/src/diag/` — the real implementation
//! compiles only under the `diagnostics` feature and off wasm32 (where there
//! is no filesystem to write a report to); otherwise every entry point is a
//! no-op with the same signature, so call sites never need a `cfg`.
//!
//! This crate never calls `faultbox::init`. Choosing a reports directory and
//! a redactor belongs to the binary; everything here is inert until the host
//! application initializes the recorder, so a library emitting these costs
//! nothing on its own.

#[cfg(all(feature = "diagnostics", not(target_arch = "wasm32")))]
mod context;
#[cfg(all(feature = "diagnostics", not(target_arch = "wasm32")))]
mod recording;

#[cfg(not(all(feature = "diagnostics", not(target_arch = "wasm32"))))]
mod inert;

#[cfg(all(feature = "diagnostics", not(target_arch = "wasm32")))]
pub use recording::{
    calvin_verdict_conflict, raft_disk_fail_stopped, raft_disk_write_abandoned,
    raft_disk_write_retried, sequencer_backpressure_drop, sequencer_epoch_gap,
    sequencer_participants_underivable,
};

#[cfg(not(all(feature = "diagnostics", not(target_arch = "wasm32"))))]
pub use inert::{
    calvin_verdict_conflict, raft_disk_fail_stopped, raft_disk_write_abandoned,
    raft_disk_write_retried, sequencer_backpressure_drop, sequencer_epoch_gap,
    sequencer_participants_underivable,
};
