// SPDX-License-Identifier: BUSL-1.1

//! What a mirror cluster does to follow its source.
//!
//! - [`rewrite`] — what the applier checks and changes on each source entry.
//! - [`batch`] — each source group's tail, cut into batches.
//! - [`propose`] — the raw commit-wait propose.
//! - [`progress`] — the applier's epoch, and which cuts the groups applied.
//! - `session` — one leadership's fence, tail and rounds.
//! - `cut` — apply one source cut.
//! - [`driver`] — one database's applier.

mod batch;
mod cut;
pub mod driver;
pub mod progress;
pub mod propose;
mod rewrite;
mod session;

pub use driver::{MirrorStop, run_mirror_applier, spawn_mirror_applier};
pub use propose::MirrorRaft;
