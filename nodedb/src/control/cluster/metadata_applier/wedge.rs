// SPDX-License-Identifier: BUSL-1.1

//! Classification of a failed metadata host-side apply, and the durable
//! marker a permanent failure leaves behind.
//!
//! The apply loop must never advance its watermark past an entry it cannot
//! apply — skipping a committed metadata entry is silent divergence from the
//! quorum. So both a transient and a permanent failure stop the batch. What
//! they must NOT share is the *story told to operators*:
//!
//! * A transient failure (a full disk, a redb lock contention, a subsystem
//!   handle not installed yet) clears by itself; Raft re-delivers the entry
//!   and the applier resumes. Halt-and-retry is the whole treatment.
//! * A permanent failure is a pure function of the entry and the local state,
//!   so every re-delivery reproduces it exactly. Retrying forever is a lie:
//!   the node is wedged, and it must stop advertising itself as ready or the
//!   only symptom operators ever see is an unrelated-looking lease timeout on
//!   every subsequent query.

use std::sync::Mutex;

/// Whether a failed host-side apply can plausibly succeed on re-delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyFailureClass {
    /// Can clear on its own; halt-and-retry is sufficient.
    Transient,
    /// Deterministic in the entry and local state — re-delivery re-fails.
    Permanent,
}

impl ApplyFailureClass {
    pub fn is_permanent(self) -> bool {
        matches!(self, Self::Permanent)
    }
}

/// Classify a host-side apply failure.
///
/// Deliberately an allowlist of the variants that are *provably* a pure
/// function of the entry plus local persisted state. Everything else is
/// treated as transient, because the cost of the two mistakes is asymmetric:
/// calling a transient failure permanent takes a node that heals
/// itself out of rotation, while calling a permanent failure transient only
/// costs the loud health signal — the watermark halts either way.
pub fn classify(error: &crate::Error) -> ApplyFailureClass {
    match error {
        // The carried version is compared against the persisted prior. Neither
        // side changes while the applier is stopped, so the comparison yields
        // the same verdict on every re-delivery, forever.
        crate::Error::DescriptorVersionAnomaly { .. } => ApplyFailureClass::Permanent,
        // The orphan is a pure function of the entry's own writes and the
        // applier code that ran them; re-delivery replays the same writes
        // and finds the same orphan every time.
        crate::Error::CatalogIntegrityViolation { .. } => ApplyFailureClass::Permanent,
        // The committed entry carries the row it writes, so an unstamped row
        // is unstamped on every re-delivery.
        crate::Error::CollectionUnstamped { .. } => ApplyFailureClass::Permanent,
        // The bytes being encoded/decoded are fixed by the committed entry, so
        // a codec rejection is reproducible.
        crate::Error::Serialization { .. } | crate::Error::Codec { .. } => {
            ApplyFailureClass::Permanent
        }
        // A committed entry that the host rejects as malformed will be as
        // malformed next time.
        crate::Error::BadRequest { .. } | crate::Error::TypeMismatch { .. } => {
            ApplyFailureClass::Permanent
        }
        // Not provably a pure function of the entry and persisted state.
        crate::Error::RejectedConstraint { .. }
        | crate::Error::TxnOverlayMemoryExceeded { .. }
        | crate::Error::RejectedAuthz { .. }
        | crate::Error::OffsetRegression { .. }
        | crate::Error::DeadlineExceeded { .. }
        | crate::Error::ConflictRetry { .. }
        | crate::Error::CalvinSerializationConflict
        | crate::Error::CalvinParticipantError
        | crate::Error::RejectedPrevalidation { .. }
        | crate::Error::RetryableRefusal { .. }
        | crate::Error::AppendOnlyViolation { .. }
        | crate::Error::BalanceViolation { .. }
        | crate::Error::MaterializedSumTargetNotFound { .. }
        | crate::Error::MaterializedSumResolutionMissing { .. }
        | crate::Error::PeriodLocked { .. }
        | crate::Error::PeriodLockMisconfigured { .. }
        | crate::Error::RetentionViolation { .. }
        | crate::Error::LegalHoldActive { .. }
        | crate::Error::StateTransitionViolation { .. }
        | crate::Error::TransitionCheckViolation { .. }
        | crate::Error::TypeGuardViolation { .. }
        | crate::Error::InsufficientBalance { .. }
        | crate::Error::RateExceeded { .. }
        | crate::Error::CollectionNotFound { .. }
        | crate::Error::DocumentNotFound { .. }
        | crate::Error::CollectionDeactivated { .. }
        | crate::Error::VShardAdmissionCapacityExceeded { .. }
        | crate::Error::CrdtAdmissionRetriesExhausted { .. }
        | crate::Error::CrdtAdmissionInvalidPlan { .. }
        | crate::Error::CrdtAdmissionCallerFence
        | crate::Error::CrdtApplyRequiresAdmission
        | crate::Error::CrdtApplyForbiddenInTransaction
        | crate::Error::NotInTransactionBlock { .. }
        | crate::Error::CrdtAdmissionTimeout { .. }
        | crate::Error::NoLeader { .. }
        | crate::Error::NotLeader { .. }
        | crate::Error::CrossCollectionNotColocated { .. }
        | crate::Error::CloneWriteRequiresMaterialize { .. }
        | crate::Error::BackupTenantMismatch { .. }
        | crate::Error::BackupKeyMismatch
        | crate::Error::QuotaOvercommit { .. }
        | crate::Error::PlanError { .. }
        | crate::Error::FeatureNotSupported { .. }
        | crate::Error::UndefinedFunction { .. }
        | crate::Error::UndefinedObject { .. }
        | crate::Error::ObjectNotInPrerequisiteState { .. }
        | crate::Error::UndefinedColumn { .. }
        | crate::Error::TextColumn { .. }
        | crate::Error::AmbiguousColumn { .. }
        | crate::Error::UnknownStrictField { .. }
        | crate::Error::DivisionByZero
        | crate::Error::DataException { .. }
        | crate::Error::InvalidLimitValue { .. }
        | crate::Error::RetryableSchemaChanged { .. }
        | crate::Error::RetryableLeaderChange { .. }
        | crate::Error::CommittedResultUnavailable { .. }
        | crate::Error::ProposalOutcomeUnknown { .. }
        | crate::Error::GroupQuorumUnavailable { .. }
        | crate::Error::GroupMarksUnavailable { .. }
        | crate::Error::BackupCaptureMoved { .. }
        | crate::Error::MetadataLeaderUnavailable
        | crate::Error::AuthorizationStateBehind { .. }
        | crate::Error::LinearizableReadRefused { .. }
        | crate::Error::ExecutionLimitExceeded { .. }
        | crate::Error::LimitExceeded { .. }
        | crate::Error::Wal(_)
        | crate::Error::Dispatch { .. }
        | crate::Error::DispatchCapacity { .. }
        | crate::Error::Storage { .. }
        | crate::Error::ColdStorage { .. }
        | crate::Error::SegmentCorrupted { .. }
        | crate::Error::MemoryExhausted { .. }
        | crate::Error::Backpressure { .. }
        | crate::Error::Crdt(_)
        | crate::Error::Io(_)
        | crate::Error::Config { .. }
        | crate::Error::Encryption { .. }
        | crate::Error::Bridge { .. }
        | crate::Error::VersionCompat { .. }
        | crate::Error::RestoreTargetNotEmpty { .. }
        | crate::Error::RestoreVerificationFailed { .. }
        | crate::Error::Internal { .. }
        | crate::Error::Shaping(_)
        | crate::Error::Ddl(_)
        | crate::Error::RemoteTyped { .. }
        | crate::Error::CollectionPurgeRowMissing { .. }
        | crate::Error::DataPlane(_)
        | crate::Error::Promql(_)
        | crate::Error::DependentObjectsExist { .. }
        | crate::Error::RoleInUse { .. }
        | crate::Error::CascadeCycle { .. }
        | crate::Error::CrossShardInExplicitTransaction
        | crate::Error::SequencerUnavailable
        | crate::Error::SessionCapExceeded { .. }
        | crate::Error::SessionIdleTimeout
        | crate::Error::SessionTokenExpired
        | crate::Error::SessionKilledByAdmin
        | crate::Error::SessionUserDropped
        | crate::Error::OidcProviderTenantUnbound
        | crate::Error::OidcProviderTenantUnavailable { .. }
        | crate::Error::ExternalRoleUndefined { .. }
        | crate::Error::OidcNoDefaultDatabase { .. }
        | crate::Error::TenantVectorDimExceeded { .. }
        | crate::Error::TenantGraphDepthExceeded { .. }
        | crate::Error::RoleInheritanceCycle { .. }
        | crate::Error::RoleInheritanceDepthExceeded { .. }
        | crate::Error::OllpExhausted { .. }
        | crate::Error::MirrorReadOnly { .. }
        | crate::Error::StaleReadNotLeader { .. } => ApplyFailureClass::Transient,
    }
}

/// What the applier recorded when it stopped on a permanent failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WedgeReport {
    /// Raft index of the entry that cannot be applied.
    pub raft_index: u64,
    /// Highest index whose state is guaranteed visible — one below the stall.
    pub last_applied_watermark: u64,
    /// Variant name of the undeliverable entry.
    pub entry_kind: String,
    /// Rendered error, so the readiness probe can name the real cause.
    pub error: String,
}

/// Node-wide marker set when the metadata applier stops on a permanent
/// failure, or when a cut barrier's floor write keeps failing. Read by the
/// readiness probe so a wedged node stops reporting itself healthy.
///
/// First writer wins: a stalled apply retries the same step and will
/// otherwise overwrite the original cause with an identical copy on every
/// attempt. The metadata applier never clears its report: its entry cannot
/// apply on re-delivery, so operator intervention is required. A floor write
/// that later succeeds clears its own report through [`Self::clear`]. A clear
/// never removes a report another writer recorded.
#[derive(Debug, Default)]
pub struct MetadataApplyWedge {
    report: Mutex<Option<WedgeReport>>,
}

impl MetadataApplyWedge {
    /// Record `report` unless a report is already held. Returns whether this
    /// call recorded it.
    pub fn record(&self, report: WedgeReport) -> bool {
        let mut held = self.report.lock().unwrap_or_else(|p| p.into_inner());
        if held.is_some() {
            return false;
        }
        *held = Some(report);
        true
    }

    /// Remove the held report if it equals `report`. Returns whether it did.
    pub fn clear(&self, report: &WedgeReport) -> bool {
        let mut held = self.report.lock().unwrap_or_else(|p| p.into_inner());
        if held.as_ref() != Some(report) {
            return false;
        }
        *held = None;
        true
    }

    /// The recorded failure, if this node is wedged.
    pub fn report(&self) -> Option<WedgeReport> {
        self.report
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    pub fn is_wedged(&self) -> bool {
        self.report
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_anomaly_is_permanent() {
        let error = crate::Error::DescriptorVersionAnomaly {
            descriptor: "orders".into(),
            carried: 1,
            prior: 1,
        };
        assert_eq!(classify(&error), ApplyFailureClass::Permanent);
    }

    #[test]
    fn storage_failure_is_transient() {
        let error = crate::Error::Storage {
            engine: "catalog".into(),
            detail: "no space left on device".into(),
        };
        assert_eq!(classify(&error), ApplyFailureClass::Transient);
    }

    #[test]
    fn unrecognized_failure_defaults_to_transient() {
        let error = crate::Error::Internal {
            detail: "metadata enrollment apply has no cluster transport".into(),
        };
        assert_eq!(classify(&error), ApplyFailureClass::Transient);
    }

    #[test]
    fn wedge_keeps_the_first_recorded_cause() {
        let wedge = MetadataApplyWedge::default();
        assert!(!wedge.is_wedged());
        wedge.record(WedgeReport {
            raft_index: 3,
            last_applied_watermark: 2,
            entry_kind: "DdlPrepared".into(),
            error: "first".into(),
        });
        wedge.record(WedgeReport {
            raft_index: 3,
            last_applied_watermark: 2,
            entry_kind: "DdlPrepared".into(),
            error: "second".into(),
        });
        assert!(wedge.is_wedged());
        assert_eq!(
            wedge.report().map(|report| report.error),
            Some("first".to_string())
        );
    }

    #[test]
    fn a_clear_removes_only_its_own_report() {
        let wedge = MetadataApplyWedge::default();
        let applier = WedgeReport {
            raft_index: 3,
            last_applied_watermark: 2,
            entry_kind: "DdlPrepared".into(),
            error: "applier".into(),
        };
        let floor = WedgeReport {
            raft_index: 9,
            last_applied_watermark: 8,
            entry_kind: "CutBarrier".into(),
            error: "floor".into(),
        };
        assert!(wedge.record(applier.clone()));
        assert!(!wedge.record(floor.clone()));
        assert!(!wedge.clear(&floor));
        assert_eq!(wedge.report(), Some(applier.clone()));
        assert!(wedge.clear(&applier));
        assert!(!wedge.is_wedged());
    }
}
