// SPDX-License-Identifier: BUSL-1.1

//! Wire version constants + static compatibility checks.
//!
//! See `view::ClusterVersionView` for the live-topology-derived
//! feature-gate predicates.

use super::view::ClusterVersionView;
#[cfg(test)]
use crate::version::WIRE_FORMAT_VERSION;

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
pub const DISTRIBUTED_CATALOG_VERSION: u16 = 2;

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
pub const DESCRIPTOR_VERSIONING_VERSION: u16 = 3;

/// Wire version that introduced the replicated
/// `DescriptorDrainStart` / `DescriptorDrainEnd` metadata entries.
/// Mixed-version clusters below this version skip drain via the
/// compat-mode fallback in `drain_for_ddl`.
pub const DESCRIPTOR_DRAIN_VERSION: u16 = 4;

/// Wire version that introduced the atomic `PutTenantWithAdmin` catalog entry.
/// Older appliers cannot decode this enum variant, so tenant creation must be
/// rejected rather than sent or downgraded while any node is below this gate.
pub const TENANT_ADMIN_ATOMIC_VERSION: u16 = 8;

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

    #[test]
    fn atomic_tenant_admin_requires_the_current_cluster_version() {
        assert_eq!(TENANT_ADMIN_ATOMIC_VERSION, WIRE_FORMAT_VERSION);
        let mut mixed = ClusterVersionView::single_node();
        mixed.min_version = WIRE_FORMAT_VERSION - 1;
        assert!(!mixed.can_activate_feature(TENANT_ADMIN_ATOMIC_VERSION));
        assert!(
            ClusterVersionView::single_node().can_activate_feature(TENANT_ADMIN_ATOMIC_VERSION)
        );
    }
}
