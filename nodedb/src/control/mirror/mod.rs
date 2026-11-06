// SPDX-License-Identifier: BUSL-1.1

//! Mirror subsystem for the Control Plane.
//!
//! - [`fence`] — [`MirrorFence`]: the databases that refuse client writes.
//! - [`registry`] — [`MirrorLinkRegistry`]: tracks active [`CrossClusterLink`]
//!   handles per database; used by `ALTER DATABASE PROMOTE` to tear down the
//!   source link before the catalog mutation lands.
//! - [`observer`] — lag-transition logic and the
//!   `nodedb_database_mirror_lag_ms` metric update path.
//! - [`restart`] — the mirror databases that need an applier, and where each
//!   one resumes.
//! - `bootstrap_image` — what a bootstrap transfer carries.
//! - [`cut_watch`] — [`MirrorCutWatch`]: each mirror's cut state and the
//!   verdict it gives a held entry.
//! - [`applier`] — what this cluster does to follow a source.
//! - [`source`] — what this cluster does for the mirrors that follow it.
//! - [`state`] — [`MirrorState`]: what `SharedState` holds for its mirrors.

pub mod applier;
pub(crate) mod bootstrap_image;
pub mod cut_watch;
pub mod fence;
pub mod observer;
pub mod registry;
pub mod restart;
pub mod source;
pub mod state;
#[cfg(test)]
pub(crate) mod test_fixture;

pub use cut_watch::{MirrorCutWatch, MirrorVerdict};
pub use fence::MirrorFence;
pub use observer::{
    LAG_DEGRADED_MS, LAG_DISCONNECTED_MS, LagTransition, compute_lag_transition, update_lag_status,
};
pub use registry::MirrorLinkRegistry;
pub use restart::{MirrorRestartDecision, enumerate_resumable_mirrors};
pub use state::MirrorState;
