// SPDX-License-Identifier: BUSL-1.1

//! Transparent statement retry for `RetryableSchemaChanged`.
//!
//! A descriptor lease drain is a barrier the DDL path runs before committing
//! the next `descriptor_version`. Statement setup can observe it in two places:
//! the planner's catalog read (`SqlCatalogError::RetryableSchemaChanged`) and
//! the post-planning lease acquisition (`SharedState::acquire_plan_lease_scope`).
//! Both surface `crate::Error::RetryableSchemaChanged`, and both sit inside the
//! SAME retried unit. A drain that starts between them is the race this loop
//! absorbs.
//!
//! The retry re-runs the whole setup unit, parsing included.
//!
//! ## Waiting
//!
//! A drain lasts as long as its DDL needs: the drain start commits, every
//! statement holding the prior version finishes, this node's idle lease
//! release commits, and the DDL's own entry commits and applies. Each step is
//! a metadata Raft round trip, so a loaded node can take seconds. The loop
//! therefore waits until the statement deadline, never a fixed budget. It
//! ends with the last retryable error only once that deadline passes.
//!
//! Between attempts the loop waits for a drain on this node to end. A drain
//! end fires once the entry that ended it applied all its effects, so the next
//! attempt plans against the new descriptor version. A bounded pause covers a
//! retryable cause no drain end marks, such as a full lease holder table.
//!
//! ## Holds the connection keeps
//!
//! A failed attempt keeps no hold: a planner refusal takes none, and a refused
//! admission gives its reservations back before it returns. A hold the same
//! connection keeps outside the attempt is different. An open transaction's
//! buffered writes and an open cursor are examples. The drain waits for that
//! hold, and the hold cannot end while the connection waits for the drain.
//! The loop returns the error at once in that case, so the client can end the
//! transaction or cursor that holds the drain open.
//!
//! The wait is per statement. A caller wraps its setup unit exactly once.

use std::time::Duration;

use crate::control::lease::{DescriptorDrainTracker, LeaseHolders};
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::error::Error;

/// The first pause between attempts. Each later pause doubles it.
const FIRST_PAUSE: Duration = Duration::from_millis(50);

/// The longest pause between attempts. A drain end on this node ends a pause
/// early.
const MAX_PAUSE: Duration = Duration::from_millis(400);

/// What a retried unit waits on between attempts, and until when.
pub struct DrainWait<'a> {
    drains: &'a DescriptorDrainTracker,
    holders: &'a LeaseHolders,
    deadline: StatementDeadline,
    /// The connection running the statement, `None` outside a connection.
    owner: Option<u64>,
}

impl<'a> DrainWait<'a> {
    /// Wait on `state`'s drains until the running statement's deadline.
    pub fn of_statement(state: &'a SharedState) -> Self {
        Self::until(state, StatementDeadline::of_statement(state))
    }

    /// Wait on `state`'s drains until `deadline`.
    pub fn until(state: &'a SharedState, deadline: StatementDeadline) -> Self {
        Self::new(&state.lease_drain, &state.lease_runtime.holders, deadline)
    }

    /// Wait on `drains` until `deadline`. `holders` names the holds the
    /// connection running on this task keeps.
    pub fn new(
        drains: &'a DescriptorDrainTracker,
        holders: &'a LeaseHolders,
        deadline: StatementDeadline,
    ) -> Self {
        Self {
            drains,
            holders,
            deadline,
            owner: crate::control::server::shared::session::hold_owner::current(),
        }
    }

    /// Whether this connection keeps a hold that an active drain waits for.
    fn connection_holds_a_drain_open(&self) -> bool {
        self.owner
            .is_some_and(|owner| self.holders.owner_blocks_drain(owner, self.drains))
    }
}

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
            | Error::TxnSnapshotTooOld { .. }
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
            | Error::WrongObjectType { .. }
            | Error::UndefinedColumn { .. }
            | Error::TextColumn { .. }
            | Error::AmbiguousColumn { .. }
            | Error::UngroupedColumn { .. }
            | Error::UnknownStrictField { .. }
            | Error::DivisionByZero
            | Error::DataException { .. }
            | Error::MixedSrid { .. }
            | Error::UnknownSrid { .. }
            | Error::NumericValueOutOfRange { .. }
            | Error::InvalidTextRepresentation { .. }
            | Error::DatatypeMismatch { .. }
            | Error::InvalidDatetimeFormat { .. }
            | Error::InvalidArgumentForLogarithm { .. }
            | Error::InvalidArgumentForPowerFunction { .. }
            | Error::DatetimeFieldOverflow { .. }
            | Error::InvalidLimitValue { .. }
            | Error::RetryableLeaderChange { .. }
            | Error::CommittedResultUnavailable { .. }
            | Error::ProposalOutcomeUnknown { .. }
            | Error::GroupQuorumUnavailable { .. }
            | Error::GroupMarksUnavailable { .. }
            | Error::BackupCaptureMoved { .. }
            | Error::MetadataLeaderUnavailable
            | Error::MetadataGroupGone { .. }
            | Error::MetadataDdlSuperseded { .. }
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
            | Error::OidcProviderTenantUnavailable { .. }
            | Error::ExternalRoleUndefined { .. }
            | Error::OidcNoDefaultDatabase { .. }
            | Error::TenantVectorDimExceeded { .. }
            | Error::TenantGraphDepthExceeded { .. }
            | Error::StorageQuotaExceeded { .. }
            | Error::RoleInheritanceCycle { .. }
            | Error::RoleInheritanceDepthExceeded { .. }
            | Error::OllpExhausted { .. }
            | Error::MirrorReadOnly { .. }
            | Error::MirrorSyncTimeout { .. }
            | Error::StaleReadNotLeader { .. } => None,
        }
    }
}

/// Run `op` until it succeeds, fails terminally, or the deadline of `wait`
/// passes. Retries only while the error classifies as a retryable schema
/// change. Any other error is returned at once.
///
/// A retryable error is returned without a wait when the deadline has passed,
/// or when the connection keeps a hold an active drain waits for.
///
/// The closure takes no arguments. Callers capture whatever context they need
/// (sql text, tenant_id, security context). The closure is `async` so it can
/// `.await` the planner.
pub async fn retry_on_schema_change<F, Fut, T, E>(wait: DrainWait<'_>, mut op: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: RetryableSchemaChange,
{
    let deadline = wait.deadline.tokio_instant();
    let mut pause = FIRST_PAUSE;
    let mut attempt = 0usize;
    loop {
        // Enabled before the attempt, so a drain that ends while the attempt
        // runs still wakes the wait after it.
        let drain_ended = wait.drains.drain_ended();
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
        if wait.connection_holds_a_drain_open() {
            tracing::debug!(
                attempt,
                descriptor,
                "not waiting out a drain this connection holds open"
            );
            return Err(error);
        }
        tracing::debug!(attempt, descriptor, "waiting out a schema change");
        tokio::select! {
            _ = drain_ended.as_mut() => {}
            _ = tokio::time::sleep_until((now + pause).min(deadline)) => {}
        }
        pause = pause.saturating_mul(2).min(MAX_PAUSE);
        attempt = attempt.saturating_add(1);
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

    fn orders() -> nodedb_cluster::DescriptorId {
        nodedb_cluster::DescriptorId::new(
            0,
            1,
            nodedb_cluster::DescriptorKind::Collection,
            "orders",
        )
    }

    fn drain_error() -> Error {
        Error::RetryableSchemaChanged {
            descriptor: "orders".into(),
        }
    }

    /// A deadline `budget` after the Tokio clock, which a paused test
    /// advances.
    fn after(budget: Duration) -> StatementDeadline {
        StatementDeadline::at((tokio::time::Instant::now() + budget).into_std())
    }

    /// The default statement budget, so a test that must not reach its
    /// deadline never does.
    const STATEMENT_BUDGET: Duration = Duration::from_secs(30);

    #[tokio::test]
    async fn first_attempt_success() {
        let calls = AtomicUsize::new(0);
        let drains = DescriptorDrainTracker::new();
        let holders = LeaseHolders::new();
        let wait = DrainWait::new(&drains, &holders, after(STATEMENT_BUDGET));
        let result: Result<i32, Error> = retry_on_schema_change(wait, || {
            let c = calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(c as i32) }
        })
        .await;
        assert_eq!(result.expect("first attempt succeeds"), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_on_schema_change_then_succeeds() {
        let calls = AtomicUsize::new(0);
        let drains = DescriptorDrainTracker::new();
        let holders = LeaseHolders::new();
        let wait = DrainWait::new(&drains, &holders, after(STATEMENT_BUDGET));
        let result: Result<&str, Error> = retry_on_schema_change(wait, || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(drain_error())
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
    async fn non_retryable_error_surfaces_immediately() {
        let calls = AtomicUsize::new(0);
        let drains = DescriptorDrainTracker::new();
        let holders = LeaseHolders::new();
        let wait = DrainWait::new(&drains, &holders, after(STATEMENT_BUDGET));
        let result: Result<(), Error> = retry_on_schema_change(wait, || {
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

    /// A drain that ends while an attempt runs starts the next attempt at
    /// once, with no pause slept.
    #[tokio::test(start_paused = true)]
    async fn a_drain_end_starts_the_next_attempt_without_a_pause() {
        use nodedb_cluster::DrainOwner;

        let drains = DescriptorDrainTracker::new();
        let holders = LeaseHolders::new();
        let descriptor = orders();
        drains.install_start(
            descriptor.clone(),
            DrainOwner::Ddl,
            1,
            nodedb_types::Hlc::ZERO,
            1,
        );
        let calls = AtomicUsize::new(0);
        let started = tokio::time::Instant::now();
        let wait = DrainWait::new(&drains, &holders, after(STATEMENT_BUDGET));
        let result: Result<(), Error> = retry_on_schema_change(wait, || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // The DDL's entry applies while the first attempt runs.
                drains.install_end(&descriptor, &DrainOwner::Ddl);
                drains.settle();
            }
            async move { if n == 0 { Err(drain_error()) } else { Ok(()) } }
        })
        .await;
        result.expect("the attempt after the drain end succeeds");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            started.elapsed() < FIRST_PAUSE,
            "the retry slept a pause after the drain ended: {:?}",
            started.elapsed()
        );
    }

    /// A drain held open for 2 s, far past any fixed retry budget, is waited
    /// out. The attempt after the drain end starts at once, and the attempts
    /// before it are paced by the bounded pause.
    #[tokio::test(start_paused = true)]
    async fn a_drain_held_past_the_old_budget_is_waited_out() {
        use nodedb_cluster::DrainOwner;

        const HELD: Duration = Duration::from_secs(2);
        let drains = std::sync::Arc::new(DescriptorDrainTracker::new());
        let holders = LeaseHolders::new();
        let descriptor = orders();
        drains.install_start(
            descriptor.clone(),
            DrainOwner::Ddl,
            1,
            nodedb_types::Hlc::ZERO,
            1,
        );
        let ender = {
            let drains = std::sync::Arc::clone(&drains);
            let descriptor = descriptor.clone();
            tokio::spawn(async move {
                tokio::time::sleep(HELD).await;
                drains.install_end(&descriptor, &DrainOwner::Ddl);
                drains.settle();
            })
        };
        let calls = AtomicUsize::new(0);
        let started = tokio::time::Instant::now();
        let wait = DrainWait::new(&drains, &holders, after(STATEMENT_BUDGET));
        let result: Result<(), Error> = retry_on_schema_change(wait, || {
            calls.fetch_add(1, Ordering::SeqCst);
            let draining = drains.is_draining(&descriptor, 1);
            async move { if draining { Err(drain_error()) } else { Ok(()) } }
        })
        .await;
        ender.await.expect("join the drain ender");
        result.expect("the write succeeds once the drain ends");
        let waited = started.elapsed();
        assert!(
            waited >= HELD,
            "the write ran before the drain ended: {waited:?}"
        );
        assert!(
            waited < HELD + FIRST_PAUSE,
            "the write did not start at the drain end: {waited:?}"
        );
        let attempts = calls.load(Ordering::SeqCst);
        assert!(
            attempts <= 10,
            "the wait polled instead of pausing: {attempts} attempts"
        );
    }

    /// A drain that never ends returns its retryable error once the
    /// statement deadline passes, and not before.
    #[tokio::test(start_paused = true)]
    async fn a_write_whose_deadline_passes_during_a_drain_gets_its_error() {
        let drains = DescriptorDrainTracker::new();
        let holders = LeaseHolders::new();
        let budget = Duration::from_secs(2);
        let started = tokio::time::Instant::now();
        let wait = DrainWait::new(&drains, &holders, after(budget));
        let result: Result<(), Error> =
            retry_on_schema_change(wait, || async { Err(drain_error()) }).await;
        assert!(matches!(result, Err(Error::RetryableSchemaChanged { .. })));
        assert!(started.elapsed() >= budget);
        assert!(started.elapsed() < budget + MAX_PAUSE);
    }

    /// A connection that keeps a hold the drain waits for gets its error at
    /// once. Waiting would hold the drain open until one side timed out.
    #[tokio::test(start_paused = true)]
    async fn a_connection_holding_the_drained_version_does_not_wait() {
        use crate::control::server::shared::session::{conn_scope, hold_owner};
        use nodedb_cluster::DrainOwner;

        conn_scope::scoped(async {
            let drains = DescriptorDrainTracker::new();
            let holders = LeaseHolders::new();
            let descriptor = orders();
            let kept = holders
                .register(&[(descriptor.clone(), 1)], hold_owner::current())
                .expect("register the transaction's hold");
            drains.install_start(descriptor, DrainOwner::Ddl, 1, nodedb_types::Hlc::ZERO, 1);
            let calls = AtomicUsize::new(0);
            let started = tokio::time::Instant::now();
            let wait = DrainWait::new(&drains, &holders, after(STATEMENT_BUDGET));
            let result: Result<(), Error> = retry_on_schema_change(wait, || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err(drain_error()) }
            })
            .await;
            assert!(matches!(result, Err(Error::RetryableSchemaChanged { .. })));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(started.elapsed() < FIRST_PAUSE, "the write paused");
            holders.deregister(&kept);
        })
        .await;
    }

    /// A hold another connection keeps is one the drain waits out, so this
    /// connection waits too.
    #[tokio::test(start_paused = true)]
    async fn another_connections_hold_is_waited_out() {
        use crate::control::server::shared::session::conn_scope;
        use nodedb_cluster::DrainOwner;

        let drains = std::sync::Arc::new(DescriptorDrainTracker::new());
        let holders = std::sync::Arc::new(LeaseHolders::new());
        let descriptor = orders();
        let other = conn_scope::scoped(async {
            crate::control::server::shared::session::hold_owner::current()
        })
        .await;
        let kept = holders
            .register(&[(descriptor.clone(), 1)], other)
            .expect("register the other connection's hold");
        drains.install_start(
            descriptor.clone(),
            DrainOwner::Ddl,
            1,
            nodedb_types::Hlc::ZERO,
            1,
        );
        let ender = {
            let drains = std::sync::Arc::clone(&drains);
            let holders = std::sync::Arc::clone(&holders);
            let descriptor = descriptor.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                holders.deregister(&kept);
                drains.install_end(&descriptor, &DrainOwner::Ddl);
                drains.settle();
            })
        };
        conn_scope::scoped(async {
            let wait = DrainWait::new(&drains, &holders, after(STATEMENT_BUDGET));
            let result: Result<(), Error> = retry_on_schema_change(wait, || {
                let draining = drains.is_draining(&descriptor, 1);
                async move { if draining { Err(drain_error()) } else { Ok(()) } }
            })
            .await;
            result.expect("the write succeeds once the other hold ends");
        })
        .await;
        ender.await.expect("join the drain ender");
    }

    /// One metadata-group admission at version 1 of `descriptor`.
    fn version_one(
        descriptor: &nodedb_cluster::DescriptorId,
    ) -> crate::control::planner::descriptor_set::DescriptorVersionSet {
        let mut versions = crate::control::planner::descriptor_set::DescriptorVersionSet::new();
        versions.record(descriptor.clone(), 1);
        versions
    }

    /// A real drain, started and ended through the metadata group, held open
    /// for 1.5 s by the test. A concurrent write admission waits it out and
    /// succeeds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_write_admission_waits_out_a_held_drain() {
        use nodedb_cluster::DrainOwner;

        const HELD: Duration = Duration::from_millis(1500);
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = std::sync::Arc::clone(&cluster.state);
        let descriptor = orders();
        crate::control::lease::drain_for_ddl_async(
            &state,
            descriptor.clone(),
            1,
            crate::control::metadata_proposer::DEFAULT_DRAIN_TIMEOUT,
            0,
        )
        .await
        .expect("an unleased descriptor drains at once");
        assert!(state.lease_drain.is_draining(&descriptor, 1));
        let ender = {
            let state = std::sync::Arc::clone(&state);
            let descriptor = descriptor.clone();
            tokio::spawn(async move {
                tokio::time::sleep(HELD).await;
                crate::control::lease::end_drain_async(&state, descriptor, DrainOwner::Ddl)
                    .await
                    .expect("end the held drain");
            })
        };
        let versions = version_one(&descriptor);
        let started = std::time::Instant::now();
        let scope = retry_on_schema_change(DrainWait::of_statement(&state), || {
            state.acquire_plan_lease_scope(&versions)
        })
        .await
        .expect("the admission succeeds once the drain ends");
        assert!(started.elapsed() >= HELD, "admitted under the drain");
        ender.await.expect("join the drain ender");
        drop(scope);
        cluster.shutdown().await;
    }

    /// A real drain that outlasts the write's deadline returns the retryable
    /// error at that deadline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_write_admission_past_its_deadline_gets_its_error() {
        use nodedb_cluster::DrainOwner;

        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = std::sync::Arc::clone(&cluster.state);
        let descriptor = orders();
        crate::control::lease::drain_for_ddl_async(
            &state,
            descriptor.clone(),
            1,
            crate::control::metadata_proposer::DEFAULT_DRAIN_TIMEOUT,
            0,
        )
        .await
        .expect("an unleased descriptor drains at once");
        let versions = version_one(&descriptor);
        let budget = Duration::from_secs(1);
        let started = std::time::Instant::now();
        let result = retry_on_schema_change(
            DrainWait::until(&state, StatementDeadline::after(budget)),
            || state.acquire_plan_lease_scope(&versions),
        )
        .await;
        assert!(
            matches!(result, Err(Error::RetryableSchemaChanged { .. })),
            "{:?}",
            result.as_ref().err()
        );
        assert!(started.elapsed() >= budget);
        crate::control::lease::end_drain_async(&state, descriptor, DrainOwner::Ddl)
            .await
            .expect("end the drain");
        cluster.shutdown().await;
    }

    /// A connection whose own hold keeps a real drain open gets its error at
    /// once. The drain completes as soon as that hold ends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_connection_hold_never_wedges_a_real_drain() {
        use crate::control::server::shared::session::conn_scope;
        use nodedb_cluster::DrainOwner;

        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = std::sync::Arc::clone(&cluster.state);
        let descriptor = orders();
        let versions = version_one(&descriptor);
        conn_scope::scoped(async {
            let kept = state
                .acquire_plan_lease_scope(&versions)
                .await
                .expect("admit the transaction's first write");
            let drain = {
                let state = std::sync::Arc::clone(&state);
                let descriptor = descriptor.clone();
                tokio::spawn(async move {
                    crate::control::lease::drain_for_ddl_async(
                        &state,
                        descriptor,
                        1,
                        crate::control::metadata_proposer::DEFAULT_DRAIN_TIMEOUT,
                        0,
                    )
                    .await
                })
            };
            let installed = std::time::Instant::now() + Duration::from_secs(10);
            while !state.lease_drain.is_draining(&descriptor, 1) {
                assert!(
                    std::time::Instant::now() < installed,
                    "the drain never started"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            let started = std::time::Instant::now();
            let result = retry_on_schema_change(DrainWait::of_statement(&state), || {
                state.acquire_plan_lease_scope(&versions)
            })
            .await;
            assert!(matches!(result, Err(Error::RetryableSchemaChanged { .. })));
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the write waited on a drain its own connection holds open: {:?}",
                started.elapsed()
            );

            drop(kept);
            drain
                .await
                .expect("join the drain")
                .expect("the drain completes once the hold ends");
        })
        .await;
        crate::control::lease::end_drain_async(&state, descriptor, DrainOwner::Ddl)
            .await
            .expect("end the drain");
        cluster.shutdown().await;
    }
}
