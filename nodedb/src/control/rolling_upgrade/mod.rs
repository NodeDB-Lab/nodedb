// SPDX-License-Identifier: BUSL-1.1

//! N-1 rolling upgrade compatibility.
//!
//! The on-disk and in-flight wire format is versioned so a node
//! running release N can operate in a cluster that still contains
//! nodes running release N-1. Feature flags gate on the
//! cluster-wide minimum version: a feature introduced at version
//! V only activates once every node reports `wire_version >= V`,
//! and the minimum is derived on demand from the live
//! `ClusterTopology` (see [`view::ClusterVersionView`]).
//!
//! Layout:
//!
//! - [`versions`] — wire-version constants and static compatibility
//!   helpers (`accept_message`, `should_compat_mode`).
//! - [`view`] — `ClusterVersionView` plus `compute_from_topology`
//!   and the feature-gate predicates. Pure functions, no shared
//!   mutable state.

pub mod versions;
pub mod view;

pub use versions::{
    DESCRIPTOR_DRAIN_VERSION, DESCRIPTOR_VERSIONING_VERSION, DISTRIBUTED_CATALOG_VERSION,
    accept_message, should_compat_mode,
};
pub use view::{ClusterVersionView, compute_from_topology};
