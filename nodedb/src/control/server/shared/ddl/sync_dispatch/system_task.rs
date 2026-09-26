// SPDX-License-Identifier: BUSL-1.1

//! Declared system-initiated Data-Plane work.
//!
//! The Data Plane is reachable two ways: with a capability minted by
//! authorizing a user's request (`AuthorizedTask`), or as work the server
//! started itself, where there is no user and therefore nothing to authorize —
//! retention enforcement, backup and restore, cluster snapshot transfer, DDL
//! apply, catalog maintenance.
//!
//! Nothing in an argument list distinguishes those two cases, which is how a
//! client-reachable read once reached storage through the system door with no
//! identity behind it and no row-level security applied. [`SystemTask`] makes
//! the second case something a caller has to state: constructing one requires
//! naming the [`SystemReason`] that explains why no identity exists. A
//! client-reachable path cannot name one truthfully, so it has to go through
//! authorization instead.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils::MintedRecords;
use crate::types::{DatabaseId, TenantId};

/// Why a Data-Plane dispatch carries no user identity.
///
/// Each variant marks work the server originates on its own schedule or on
/// behalf of the cluster — never work a client asked for directly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SystemReason {
    /// Retention / temporal-purge enforcement on its own timer.
    RetentionEnforcement,
    /// Backup capture or restore reissue.
    BackupRestore,
    /// Cluster snapshot build or install.
    ClusterSnapshot,
    /// Applying a committed DDL side-effect to engine state.
    DdlApply,
    /// Catalog and version-history maintenance (compaction, checkpoints,
    /// version diffs, synonym and aggregate registration).
    CatalogMaintenance,
    /// Tenant lifecycle: purge, move, cutover.
    TenantLifecycle,
    /// Event Plane work dispatched back through the Control Plane (alerts,
    /// scheduled evaluation) — driven by a rule, not by a live session.
    EventPlane,
    /// A derived leg of a request whose capability was already consumed at the
    /// entry point — CRDT admission preview and restore-delta generation, and
    /// the Raft-sequenced sync write. The authorization decision for these
    /// belongs to the parent dispatch; they are not independently reachable.
    AdmittedContinuation,
}

impl SystemReason {
    /// Stable label for tracing and audit.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::RetentionEnforcement => "retention_enforcement",
            Self::BackupRestore => "backup_restore",
            Self::ClusterSnapshot => "cluster_snapshot",
            Self::DdlApply => "ddl_apply",
            Self::CatalogMaintenance => "catalog_maintenance",
            Self::TenantLifecycle => "tenant_lifecycle",
            Self::EventPlane => "event_plane",
            Self::AdmittedContinuation => "admitted_continuation",
        }
    }

    /// The source the task's write events carry into the Event Plane.
    ///
    /// A restore re-issues rows whose AFTER triggers fired when they were
    /// first written, so its writes carry `Restore`. Every other reason
    /// carries `User`.
    pub(crate) fn event_source(self) -> crate::event::EventSource {
        match self {
            Self::BackupRestore => crate::event::EventSource::Restore,
            Self::RetentionEnforcement
            | Self::ClusterSnapshot
            | Self::DdlApply
            | Self::CatalogMaintenance
            | Self::TenantLifecycle
            | Self::EventPlane
            | Self::AdmittedContinuation => crate::event::EventSource::User,
        }
    }
}

/// A Data-Plane dispatch with no user identity behind it.
pub(crate) struct SystemTask<'a> {
    pub(super) reason: SystemReason,
    pub(super) tenant_id: TenantId,
    pub(super) database_id: DatabaseId,
    pub(super) collection: &'a str,
    pub(super) plan: PhysicalPlan,
    /// Records the caller appended for this task, under their outcome-floor
    /// window. `None` when the task appends nothing.
    pub(super) minted: Option<MintedRecords>,
}

impl<'a> SystemTask<'a> {
    /// Declare a system-initiated dispatch.
    ///
    /// `reason` is not decoration: it is the assertion that no user identity
    /// exists for this work. Do not construct one on a path a client can reach
    /// — authorize the request and dispatch the resulting capability instead.
    pub(crate) fn new(
        reason: SystemReason,
        tenant_id: TenantId,
        database_id: DatabaseId,
        collection: &'a str,
        plan: PhysicalPlan,
    ) -> Self {
        Self {
            reason,
            tenant_id,
            database_id,
            collection,
            plan,
            minted: None,
        }
    }

    /// Attach the records the caller appended for this task. The dispatch
    /// closes their outcome-floor window from the task's outcome.
    pub(crate) fn with_minted(mut self, minted: MintedRecords) -> Self {
        self.minted = Some(minted);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_restore_carries_the_restore_source() {
        assert_eq!(
            SystemReason::BackupRestore.event_source(),
            crate::event::EventSource::Restore
        );
        for reason in [
            SystemReason::RetentionEnforcement,
            SystemReason::ClusterSnapshot,
            SystemReason::DdlApply,
            SystemReason::CatalogMaintenance,
            SystemReason::TenantLifecycle,
            SystemReason::EventPlane,
            SystemReason::AdmittedContinuation,
        ] {
            assert_eq!(reason.event_source(), crate::event::EventSource::User);
        }
    }
}
