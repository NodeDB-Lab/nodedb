// SPDX-License-Identifier: BUSL-1.1

//! Wire version constants + static compatibility checks.
//!
//! See `view::ClusterVersionView` for the live-topology-derived
//! feature-gate predicates.

use super::view::ClusterVersionView;
#[cfg(test)]
use crate::version::WIRE_FORMAT_VERSION;

// PRE-1.0: every gate below is pinned to 1, the value of
// `WIRE_FORMAT_VERSION`, so each feature is unconditionally active.
//
// `MIN_WIRE_FORMAT_VERSION == WIRE_FORMAT_VERSION` (floor == ceiling), so a
// node rejects any peer whose version differs and a mixed-version cluster can
// never form. Inside a cluster that exists, every node is therefore on this
// exact version, which makes `min_version >= V` constant-true for any
// `V <= WIRE_FORMAT_VERSION` and constant-false above it. These gates cannot
// discriminate, so a value above 1 does not protect a rolling upgrade — it just
// switches the feature OFF permanently and silently routes to a legacy
// fallback.
//
// Do NOT raise these while `WIRE_FORMAT_VERSION` is 1 (see
// `nodedb_types::wire_version` for why it stays there until 1.0). The gate
// machinery is kept, not deleted, because it becomes meaningful the moment a
// real support window (`MIN_WIRE_FORMAT_VERSION < WIRE_FORMAT_VERSION`) is
// introduced post-1.0 — at which point these regain their original meanings,
// recorded below.

/// Wire-format version that introduced the replicated catalog DDL
/// path (`CatalogEntry` proposed via the metadata raft group).
///
/// Before this version, catalog DDL was applied directly on the
/// originating node and never replicated. Mixing the two paths in
/// a rolling upgrade window would silently diverge state across
/// nodes, so [`crate::control::metadata_proposer::propose_catalog_entry`]
/// gates on this constant via
/// [`ClusterVersionView::can_activate_feature`] and falls back to
/// the legacy direct-write path until every node in the cluster
/// has caught up.
pub const DISTRIBUTED_CATALOG_VERSION: u16 = 1;

/// Wire-format version that introduced monotonic descriptor
/// versioning (`descriptor_version: u64` + `modification_hlc: Hlc`
/// on every `Stored*` type stamped by the metadata applier at
/// commit time).
///
/// Before this version, `Stored*` records had no version / HLC
/// fields on the wire. In a mixed-version cluster during rolling
/// upgrade, an older applier would fail to re-stamp on
/// write-through (it has no stamp logic), so we keep the stamping
/// path disabled in compat mode and let resolvers treat
/// `descriptor_version == 0` as "unknown, always re-fetch". Once
/// every node reports `wire_version >= 3`, the applier transitions
/// to stamping.
pub const DESCRIPTOR_VERSIONING_VERSION: u16 = 1;

/// Wire version that introduced the replicated
/// `DescriptorDrainStart` / `DescriptorDrainEnd` metadata entries.
/// Mixed-version clusters below this version skip drain via the
/// compat-mode fallback in `drain_for_ddl`.
pub const DESCRIPTOR_DRAIN_VERSION: u16 = 1;

/// Check if a message from a remote node should be accepted.
///
/// Accepts only messages with the exact current wire format version.
/// Any other version is rejected (floor == ceiling; no rolling-upgrade window).
pub fn accept_message(remote_version: u16) -> crate::Result<()> {
    crate::version::check_wire_compatibility(remote_version)
}

/// Determine if this node should operate in compatibility mode.
///
/// Compat mode is active when the cluster has mixed versions. In
/// compat mode, new features that require the latest version are
/// disabled.
pub fn should_compat_mode(view: &ClusterVersionView) -> bool {
    view.is_mixed_version()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_same_version() {
        assert!(accept_message(WIRE_FORMAT_VERSION).is_ok());
    }

    #[test]
    fn reject_newer() {
        assert!(accept_message(WIRE_FORMAT_VERSION + 1).is_err());
    }

    #[test]
    fn reject_older() {
        if WIRE_FORMAT_VERSION > 0 {
            assert!(accept_message(WIRE_FORMAT_VERSION - 1).is_err());
        }
    }
}
