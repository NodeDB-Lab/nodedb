// SPDX-License-Identifier: BUSL-1.1

//! What a mirror cluster does to follow its source.
//!
//! - [`rewrite`] — what the applier checks and changes on each source entry.
//! - [`batch`] — each source group's tail, cut into batches.
//! - [`propose`] — the raw commit-wait propose.
//! - [`progress`] — the applier's epoch, and which cuts the groups applied.
//! - `session` — one leadership's fence, tail and rounds.
//! - `cut` — apply one source cut.
//! - `bootstrap` — install a mirror's first capture.
//! - `conversions` — the capture's conversion records, on every replica.
//! - [`driver`] — one database's applier.
//! - [`link`] — a link to a source the config names.
//! - [`supervisor`] — one applier per mirror the catalog holds.

mod batch;
mod bootstrap;
mod conversions;
mod cut;
pub mod driver;
pub mod link;
pub mod progress;
pub mod propose;
mod rewrite;
mod session;
pub mod supervisor;

pub use driver::{MirrorStop, run_mirror_applier, spawn_mirror_applier};
pub use link::open_source_link;
pub use propose::MirrorRaft;
pub use supervisor::spawn_mirror_supervisor;
