// SPDX-License-Identifier: BUSL-1.1

//! Transparent statement retry for `RetryableSchemaChanged`.
//!
//! A descriptor lease drain is a short barrier the DDL path runs before
//! committing the next `descriptor_version`. Statement setup can observe it in
//! two places: the planner's catalog read (`SqlCatalogError::RetryableSchemaChanged`)
//! and the post-planning lease acquisition (`SharedState::acquire_plan_lease_scope`).
//! Both surface `crate::Error::RetryableSchemaChanged`, and both must sit inside
//! the SAME retried unit — a drain that starts between them is exactly the race
//! this loop exists to absorb.
//!
//! The retry is intentionally **dumb**: it re-runs the whole setup unit,
//! including parsing. A smarter implementation will hold onto the parsed AST
//! and only re-resolve. That's a future optimisation — for the common drain
//! case (sub-second drains on clusters with short query lifetimes) the extra
//! parse cost is negligible.
//!
//! ## Retry budget
//!
//! Five attempts total with 50/100/200/400 ms backoff between them — roughly
//! 750ms of tolerance for a drain to complete. A backoff is a ceiling, not a
//! fixed sleep: the next attempt starts as soon as a drain ends on this node,
//! so a statement waits out a drain for the drain's own length and no more.
//! The `DEFAULT_DRAIN_TIMEOUT` from
//! `metadata_proposer` is 35s, so in practice either drain completes within our
//! retry budget (the proposer is actively draining and is probably close to done
//! by the time we observe it) or drain is stuck and our error helps the operator
//! diagnose.
//!
//! The budget is per statement. Nesting one retried unit inside another will
//! multiply it, so a caller wraps its setup unit exactly once.

use std::time::Duration;

use crate::control::lease::DescriptorDrainTracker;
use crate::error::Error;

/// Maximum number of attempts (including the initial call).
const MAX_ATTEMPTS: usize = 5;

/// Backoff durations BETWEEN attempts. `BACKOFFS[i]` is the sleep
/// duration before attempt `i + 1`. Length must be
/// `MAX_ATTEMPTS - 1`.
const BACKOFFS: [Duration; MAX_ATTEMPTS - 1] = [
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(200),
    Duration::from_millis(400),
];

/// Classification hook for the retry loop.
///
/// Implemented by every error type a retried setup unit can fail with, so a
/// protocol-specific error wrapper stays retry-aware without the loop having to
/// sniff rendered SQLSTATE codes.
pub trait RetryableSchemaChange {
    /// The descriptor whose schema change makes this error retryable, or
    /// `None` when the failure is terminal.
    fn retryable_descriptor(&self) -> Option<&str>;
}

impl RetryableSchemaChange for Error {
    fn retryable_descriptor(&self) -> Option<&str> {
        // Deliberately narrow: only the descriptor-version race is retryable.
        // Widening this to other transient-looking variants will silently
        // re-run statements whose failure is real.
        match self {
            Error::RetryableSchemaChanged { descriptor } => Some(descriptor.as_str()),
            Error::RejectedConstraint { .. }
            | Error::TxnOverlayMemoryExceeded { .. }
            | Error::RejectedAuthz { .. }
            | Error::OffsetRegression { .. }
            | Error::DeadlineExceeded { .. }
            | Error::ConflictRetry { .. }
            | Error::CalvinSerializationConflict
            | Error::CalvinParticipantError
            | Error::RejectedPrevalidation { .. }
            | Error::RetryableRefusal { .. }
            | Error::AppendOnlyViolation { .. }
            | Error::BalanceViolation { .. }
            | Error::MaterializedSumTargetNotFound { .. }
            | Error::MaterializedSumResolutionMissing { .. }
            | Error::PeriodLocked { .. }
            | Error::PeriodLockMisconfigured { .. }
            | Error::RetentionViolation { .. }
            | Error::LegalHoldActive { .. }
            | Error::StateTransitionViolation { .. }
            | Error::TransitionCheckViolation { .. }
            | Error::TypeGuardViolation { .. }
            | Error::TypeMismatch { .. }
            | Error::InsufficientBalance { .. }
            | Error::RateExceeded { .. }
            | Error::CollectionNotFound { .. }
            | Error::DocumentNotFound { .. }
            | Error::CollectionDeactivated { .. }
            | Error::VShardAdmissionCapacityExceeded { .. }
            | Error::CrdtAdmissionRetriesExhausted { .. }
            | Error::CrdtAdmissionInvalidPlan { .. }
            | Error::CrdtAdmissionCallerFence
            | Error::CrdtApplyRequiresAdmission
            | Error::CrdtApplyForbiddenInTransaction
            | Error::NotInTransactionBlock { .. }
            | Error::CrdtAdmissionTimeout { .. }
            | Error::NoLeader { .. }
            | Error::NotLeader { .. }
            | Error::CrossCollectionNotColocated { .. }
            | Error::CloneWriteRequiresMaterialize { .. }
            | Error::BadRequest { .. }
            | Error::BackupTenantMismatch { .. }
            | Error::BackupKeyMismatch
            | Error::QuotaOvercommit { .. }
            | Error::PlanError { .. }
            | Error::FeatureNotSupported { .. }
            | Error::UndefinedFunction { .. }
            | Error::UndefinedObject { .. }
            | Error::ObjectNotInPrerequisiteState { .. }
            | Error::UndefinedColumn { .. }
            | Error::TextColumn { .. }
            | Error::AmbiguousColumn { .. }
            | Error::UnknownStrictField { .. }
            | Error::DivisionByZero
            | Error::DataException { .. }
            | Error::NumericValueOutOfRange { .. }
            | Error::InvalidLimitValue { .. }
            | Error::RetryableLeaderChange { .. }
            | Error::CommittedResultUnavailable { .. }
            | Error::ProposalOutcomeUnknown { .. }
            | Error::GroupQuorumUnavailable { .. }
            | Error::GroupMarksUnavailable { .. }
            | Error::BackupCaptureMoved { .. }
            | Error::MetadataLeaderUnavailable
            | Error::AuthorizationStateBehind { .. }
            | Error::LinearizableReadRefused { .. }
            | Error::ExecutionLimitExceeded { .. }
            | Error::LimitExceeded { .. }
            | Error::Wal(_)
            | Error::Dispatch { .. }
            | Error::DispatchCapacity { .. }
            | Error::Storage { .. }
            | Error::ColdStorage { .. }
            | Error::Serialization { .. }
            | Error::Codec { .. }
            | Error::SegmentCorrupted { .. }
            | Error::MemoryExhausted { .. }
            | Error::Backpressure { .. }
            | Error::Crdt(_)
            | Error::Io(_)
            | Error::Config { .. }
            | Error::Encryption { .. }
            | Error::Bridge { .. }
            | Error::VersionCompat { .. }
            | Error::RestoreTargetNotEmpty { .. }
            | Error::RestoreVerificationFailed { .. }
            | Error::Internal { .. }
            | Error::Shaping(_)
            | Error::Ddl(_)
            | Error::RemoteTyped { .. }
            | Error::DescriptorVersionAnomaly { .. }
            | Error::CollectionPurgeRowMissing { .. }
            | Error::CollectionUnstamped { .. }
            | Error::CatalogIntegrityViolation { .. }
            | Error::DataPlane(_)
            | Error::Promql(_)
            | Error::DependentObjectsExist { .. }
            | Error::RoleInUse { .. }
            | Error::CascadeCycle { .. }
            | Error::CrossShardInExplicitTransaction
            | Error::SequencerUnavailable
            | Error::SessionCapExceeded { .. }
            | Error::SessionIdleTimeout
            | Error::SessionTokenExpired
            | Error::SessionKilledByAdmin
            | Error::SessionUserDropped
            | Error::OidcProviderTenantUnbound
            | Error::OidcProviderTenantUnavailable { .. }
            | Error::ExternalRoleUndefined { .. }
            | Error::OidcNoDefaultDatabase { .. }
            | Error::TenantVectorDimExceeded { .. }
            | Error::TenantGraphDepthExceeded { .. }
            | Error::RoleInheritanceCycle { .. }
            | Error::RoleInheritanceDepthExceeded { .. }
            | Error::OllpExhausted { .. }
            | Error::MirrorReadOnly { .. }
            | Error::StaleReadNotLeader { .. } => None,
        }
    }
}

/// Run `op` up to `MAX_ATTEMPTS` times. Retries only while the error classifies
/// as a retryable schema change. Any other error is returned immediately on the
/// first attempt. Returns the last error observed if every attempt was
/// retryable.
///
/// The closure takes no arguments — callers capture whatever context (sql text,
/// tenant_id, security context) they need via move semantics. The closure is
/// `async` so it can `.await` the planner.
///
/// `drains` is this node's drain tracker. A drain that ends on this node ends
/// the wait before the next attempt early.
pub async fn retry_on_schema_change<F, Fut, T, E>(
    drains: &DescriptorDrainTracker,
    mut op: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: RetryableSchemaChange + From<Error>,
{
    let mut last_err: Option<E> = None;
    for attempt in 0..MAX_ATTEMPTS {
        // Enabled before the attempt, so a drain that ends while the attempt
        // runs still wakes the wait after it.
        let drain_ended = drains.drain_ended();
        tokio::pin!(drain_ended);
        drain_ended.as_mut().enable();
        match op().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let Some(descriptor) = error.retryable_descriptor() else {
                    return Err(error);
                };
                tracing::debug!(
                    attempt,
                    descriptor,
                    "retrying statement setup after schema change"
                );
                last_err = Some(error);
                if let Some(backoff) = BACKOFFS.get(attempt) {
                    tokio::select! {
                        _ = drain_ended.as_mut() => {}
                        _ = tokio::time::sleep(*backoff) => {}
                    }
                }
            }
        }
    }
    // Exhausted retries — surface the last retryable error.
    Err(last_err.unwrap_or_else(|| {
        E::from(Error::PlanError {
            detail: "retry_on_schema_change: no attempts recorded".into(),
        })
    }))
}

/// Run `op` until it succeeds or fails terminally, or until `budget` elapses.
///
/// This is for a caller whose client cannot retry, such as an ILP stream that
/// sends no acks. Such a caller waits out a drain for as long as the drain
/// itself can last. Each wait ends early when a drain ends on this node. The
/// last retryable error is returned once `budget` elapses.
pub async fn retry_through_drain<F, Fut, T, E>(
    drains: &DescriptorDrainTracker,
    budget: Duration,
    mut op: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: RetryableSchemaChange,
{
    let deadline = tokio::time::Instant::now() + budget;
    let ceiling = BACKOFFS[BACKOFFS.len() - 1];
    let mut attempt = 0usize;
    loop {
        // Enabled before the attempt, so a drain that ends while the attempt
        // runs still wakes the wait after it.
        let drain_ended = drains.drain_ended();
        tokio::pin!(drain_ended);
        drain_ended.as_mut().enable();
        let error = match op().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        let Some(descriptor) = error.retryable_descriptor() else {
            return Err(error);
        };
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(error);
        }
        tracing::debug!(attempt, descriptor, "waiting out a descriptor drain");
        let backoff = BACKOFFS.get(attempt).copied().unwrap_or(ceiling);
        let wake = (now + backoff).min(deadline);
        tokio::select! {
            _ = drain_ended.as_mut() => {}
            _ = tokio::time::sleep_until(wake) => {}
        }
        attempt += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn drain_error_classifies_as_retryable() {
        let error = Error::RetryableSchemaChanged {
            descriptor: "orders at version 3".into(),
        };
        assert_eq!(error.retryable_descriptor(), Some("orders at version 3"));
    }

    #[test]
    fn non_drain_lease_failures_are_not_reclassified() {
        // A configuration fault and an internal fault are the shapes a
        // non-drain lease failure takes. Neither can be retried.
        assert!(
            Error::Config {
                detail: "lease grant rejected".into(),
            }
            .retryable_descriptor()
            .is_none()
        );
        assert!(
            Error::Internal {
                detail: "metadata raft unavailable".into(),
            }
            .retryable_descriptor()
            .is_none()
        );
        assert!(
            Error::PlanError {
                detail: "syntax error".into(),
            }
            .retryable_descriptor()
            .is_none()
        );
    }

    #[tokio::test]
    async fn first_attempt_success() {
        let calls = AtomicUsize::new(0);
        let drains = DescriptorDrainTracker::new();
        let result: Result<i32, Error> = retry_on_schema_change(&drains, || {
            let c = calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(c as i32) }
        })
        .await;
        assert_eq!(result.expect("first attempt succeeds"), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_on_schema_change_then_succeeds() {
        let calls = AtomicUsize::new(0);
        let drains = DescriptorDrainTracker::new();
        let result: Result<&str, Error> = retry_on_schema_change(&drains, || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(Error::RetryableSchemaChanged {
                        descriptor: format!("attempt {n}"),
                    })
                } else {
                    Ok("done")
                }
            }
        })
        .await;
        assert_eq!(result.expect("third attempt succeeds"), "done");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn surfaces_error_after_budget_exhausted() {
        let calls = AtomicUsize::new(0);
        let drains = DescriptorDrainTracker::new();
        let result: Result<(), Error> = retry_on_schema_change(&drains, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(Error::RetryableSchemaChanged {
                    descriptor: "orders".into(),
                })
            }
        })
        .await;
        assert!(matches!(result, Err(Error::RetryableSchemaChanged { .. })));
        assert_eq!(calls.load(Ordering::SeqCst), MAX_ATTEMPTS);
    }

    /// A drain that ends while an attempt runs starts the next attempt at
    /// once, with no backoff slept.
    #[tokio::test(start_paused = true)]
    async fn a_drain_end_starts_the_next_attempt_without_the_backoff() {
        use nodedb_cluster::{DescriptorId, DescriptorKind, DrainOwner};

        let drains = DescriptorDrainTracker::new();
        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders");
        drains.install_start(
            descriptor.clone(),
            DrainOwner::Ddl,
            1,
            nodedb_types::Hlc::ZERO,
            1,
        );
        let calls = AtomicUsize::new(0);
        let started = tokio::time::Instant::now();
        let result: Result<(), Error> = retry_on_schema_change(&drains, || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // The DDL's entry applies while the first attempt runs.
                drains.install_end(&descriptor, &DrainOwner::Ddl);
                drains.settle();
            }
            async move {
                if n == 0 {
                    Err(Error::RetryableSchemaChanged {
                        descriptor: "orders".into(),
                    })
                } else {
                    Ok(())
                }
            }
        })
        .await;
        result.expect("the attempt after the drain end succeeds");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            started.elapsed() < BACKOFFS[0],
            "the retry slept its backoff after the drain ended: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn non_retryable_error_surfaces_immediately() {
        let calls = AtomicUsize::new(0);
        let drains = DescriptorDrainTracker::new();
        let result: Result<(), Error> = retry_on_schema_change(&drains, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(Error::PlanError {
                    detail: "syntax error".into(),
                })
            }
        })
        .await;
        assert!(matches!(result, Err(Error::PlanError { .. })));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// A drain that outlasts the statement budget is still waited out when
    /// the caller's budget covers it.
    #[tokio::test(start_paused = true)]
    async fn a_drain_longer_than_the_statement_budget_is_waited_out() {
        let calls = AtomicUsize::new(0);
        let drains = DescriptorDrainTracker::new();
        let refusals = MAX_ATTEMPTS * 3;
        let result: Result<(), Error> =
            retry_through_drain(&drains, Duration::from_secs(35), || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n < refusals {
                        Err(Error::RetryableSchemaChanged {
                            descriptor: "orders".into(),
                        })
                    } else {
                        Ok(())
                    }
                }
            })
            .await;
        result.expect("the attempt after the drain succeeds");
        assert_eq!(calls.load(Ordering::SeqCst), refusals + 1);
    }

    /// A drain that never ends returns its error once the budget elapses.
    #[tokio::test(start_paused = true)]
    async fn a_drain_past_the_budget_surfaces_its_error_at_the_deadline() {
        let drains = DescriptorDrainTracker::new();
        let budget = Duration::from_secs(2);
        let started = tokio::time::Instant::now();
        let result: Result<(), Error> = retry_through_drain(&drains, budget, || async {
            Err(Error::RetryableSchemaChanged {
                descriptor: "orders".into(),
            })
        })
        .await;
        assert!(matches!(result, Err(Error::RetryableSchemaChanged { .. })));
        assert!(started.elapsed() >= budget);
        assert!(started.elapsed() < budget + BACKOFFS[BACKOFFS.len() - 1]);
    }

    #[tokio::test]
    async fn retry_through_drain_surfaces_a_terminal_error_at_once() {
        let calls = AtomicUsize::new(0);
        let drains = DescriptorDrainTracker::new();
        let result: Result<(), Error> =
            retry_through_drain(&drains, Duration::from_secs(35), || {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    Err(Error::PlanError {
                        detail: "syntax error".into(),
                    })
                }
            })
            .await;
        assert!(matches!(result, Err(Error::PlanError { .. })));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
