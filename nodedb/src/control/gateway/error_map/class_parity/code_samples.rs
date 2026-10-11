// SPDX-License-Identifier: BUSL-1.1

//! One sample per Data-Plane `ErrorCode` variant.

use nodedb_types::sync::violation::ViolationType;
use nodedb_types::sync::wire::SyncProvenance;

use crate::bridge::envelope::{CounterFault, ErrorCode, SyncHold};

/// The number of `ErrorCode` variants [`variant_index`] numbers.
pub(super) const VARIANT_COUNT: usize = 53;

/// A dense index per variant. Exhaustive, so a new variant fails to compile
/// here until it gets an index, and [`every_variant_has_a_sample`] then fails
/// until [`samples`] carries it.
pub(super) fn variant_index(code: &ErrorCode) -> usize {
    match code {
        ErrorCode::DeadlineExceeded => 0,
        ErrorCode::RejectedConstraint { .. } => 1,
        ErrorCode::RejectedPrevalidation { .. } => 2,
        ErrorCode::RetryableRefusal { .. } => 3,
        ErrorCode::SyncRejected { .. } => 4,
        ErrorCode::SyncNotApplied { .. } => 5,
        ErrorCode::NotFound => 6,
        ErrorCode::RejectedAuthz { .. } => 7,
        ErrorCode::ConflictRetry => 8,
        ErrorCode::CrdtFrontierMismatch { .. } => 9,
        ErrorCode::ResourcesExhausted => 11,
        ErrorCode::RejectedDanglingEdge { .. } => 12,
        ErrorCode::DuplicateWrite => 13,
        ErrorCode::AppendOnlyViolation { .. } => 14,
        ErrorCode::BalanceViolation { .. } => 15,
        ErrorCode::PeriodLocked { .. } => 16,
        ErrorCode::PeriodLockMisconfigured { .. } => 17,
        ErrorCode::RetentionViolation { .. } => 18,
        ErrorCode::LegalHoldActive { .. } => 19,
        ErrorCode::StateTransitionViolation { .. } => 20,
        ErrorCode::TransitionCheckViolation { .. } => 21,
        ErrorCode::TypeGuardViolation { .. } => 22,
        ErrorCode::TypeMismatch { .. } => 23,
        ErrorCode::CounterFault { .. } => 24,
        ErrorCode::InsufficientBalance { .. } => 25,
        ErrorCode::RateExceeded { .. } => 26,
        ErrorCode::CollectionDraining { .. } => 27,
        ErrorCode::RecursionDepthExceeded { .. } => 28,
        ErrorCode::UndefinedColumn { .. } => 29,
        ErrorCode::Internal { .. } => 30,
        ErrorCode::Unsupported { .. } => 31,
        ErrorCode::RollbackFailed { .. } => 32,
        ErrorCode::OllpRetryRequired => 33,
        ErrorCode::TxnOverlayMemoryExceeded { .. } => 34,
        ErrorCode::DivisionByZero => 35,
        ErrorCode::UndefinedFunction { .. } => 36,
        ErrorCode::DataException { .. } => 37,
        ErrorCode::DispatchCapacity { .. } => 38,
        ErrorCode::ExpiredBeforeExecution => 39,
        ErrorCode::BadRequest { .. } => 40,
        ErrorCode::TransactionRollback { .. } => 41,
        ErrorCode::ActiveSqlTransaction { .. } => 42,
        ErrorCode::DependentObjectsExist { .. } => 10,
        ErrorCode::TextColumn { .. } => 43,
        ErrorCode::NumericValueOutOfRange { .. } => 44,
        ErrorCode::NodeLabelLimit { .. } => 45,
        ErrorCode::InvalidTextRepresentation { .. } => 46,
        ErrorCode::DatatypeMismatch { .. } => 47,
        ErrorCode::InvalidDatetimeFormat { .. } => 48,
        ErrorCode::DatetimeFieldOverflow { .. } => 49,
        ErrorCode::UndefinedObject { .. } => 50,
        ErrorCode::ObjectNotInPrerequisiteState { .. } => 51,
        ErrorCode::CoreFailStopped { .. } => 52,
    }
}

fn provenance() -> SyncProvenance {
    SyncProvenance {
        producer_id: 1,
        epoch: 1,
        stream_id: 1,
        seq: 1,
    }
}

/// One sample per variant, plus one per value that picks a different
/// SQLSTATE: each constraint kind and each counter fault.
pub(super) fn samples() -> Vec<ErrorCode> {
    let text = || "detail".to_owned();
    let collection = || "c".to_owned();
    let mut samples = vec![
        ErrorCode::DeadlineExceeded,
        ErrorCode::RejectedPrevalidation { reason: text() },
        ErrorCode::RetryableRefusal { reason: text() },
        ErrorCode::CoreFailStopped {
            core_id: 0,
            detail: text(),
        },
        ErrorCode::SyncRejected {
            violation: ViolationType::PermissionDenied,
            applied_seq: 1,
            provenance: provenance(),
        },
        ErrorCode::SyncRejected {
            violation: ViolationType::RateLimited,
            applied_seq: 1,
            provenance: provenance(),
        },
        ErrorCode::SyncNotApplied {
            hold: SyncHold::Gap { expected: 2 },
            applied_seq: 1,
        },
        ErrorCode::NotFound,
        ErrorCode::RejectedAuthz { resource: text() },
        ErrorCode::ConflictRetry,
        ErrorCode::CrdtFrontierMismatch {
            expected: [0; 32],
            actual: [1; 32],
        },
        ErrorCode::ResourcesExhausted,
        ErrorCode::RejectedDanglingEdge {
            missing_node: text(),
        },
        ErrorCode::DuplicateWrite,
        ErrorCode::AppendOnlyViolation {
            collection: collection(),
        },
        ErrorCode::BalanceViolation {
            collection: collection(),
            detail: text(),
        },
        ErrorCode::PeriodLocked {
            collection: collection(),
        },
        ErrorCode::PeriodLockMisconfigured {
            collection: collection(),
            ref_table: "periods".into(),
            status_column: "status".into(),
            row_identity: "p1".into(),
        },
        ErrorCode::RetentionViolation {
            collection: collection(),
        },
        ErrorCode::LegalHoldActive {
            collection: collection(),
        },
        ErrorCode::StateTransitionViolation {
            collection: collection(),
            detail: text(),
        },
        ErrorCode::TransitionCheckViolation {
            collection: collection(),
            detail: text(),
        },
        ErrorCode::TypeGuardViolation {
            collection: collection(),
            detail: text(),
        },
        ErrorCode::TypeMismatch {
            collection: collection(),
            detail: text(),
        },
        ErrorCode::InsufficientBalance {
            collection: collection(),
            detail: text(),
        },
        ErrorCode::RateExceeded {
            gate: "g".into(),
            retry_after_ms: 10,
        },
        ErrorCode::CollectionDraining {
            collection: collection(),
        },
        ErrorCode::RecursionDepthExceeded {
            cte_name: "walk".into(),
            max_depth: 100,
        },
        ErrorCode::UndefinedColumn { column: "x".into() },
        ErrorCode::TextColumn {
            collection: collection(),
            column: "x".into(),
            fault: nodedb_types::text_search::TextColumnFault::NotIndexed,
        },
        ErrorCode::TextColumn {
            collection: collection(),
            column: "x".into(),
            fault: nodedb_types::text_search::TextColumnFault::NotAColumn,
        },
        ErrorCode::Internal { detail: text() },
        ErrorCode::Unsupported { detail: text() },
        ErrorCode::RollbackFailed {
            entry_index: 0,
            detail: text(),
            cause: Some(Box::new(ErrorCode::Internal { detail: text() })),
        },
        ErrorCode::OllpRetryRequired,
        ErrorCode::TxnOverlayMemoryExceeded { limit: 1 << 20 },
        ErrorCode::DivisionByZero,
        ErrorCode::UndefinedFunction { name: "f".into() },
        ErrorCode::DataException { detail: text() },
        ErrorCode::NumericValueOutOfRange { detail: text() },
        ErrorCode::InvalidTextRepresentation { detail: text() },
        ErrorCode::DatatypeMismatch { detail: text() },
        ErrorCode::InvalidDatetimeFormat { detail: text() },
        ErrorCode::DatetimeFieldOverflow { detail: text() },
        ErrorCode::UndefinedObject {
            object: "document \"d\"".into(),
        },
        ErrorCode::ObjectNotInPrerequisiteState {
            object: "CRDT version".into(),
            detail: text(),
        },
        ErrorCode::DispatchCapacity { reason: text() },
        ErrorCode::ExpiredBeforeExecution,
        ErrorCode::BadRequest { detail: text() },
        ErrorCode::TransactionRollback { detail: text() },
        ErrorCode::ActiveSqlTransaction { detail: text() },
        ErrorCode::DependentObjectsExist {
            object: "role \"analyst\"".into(),
            detail: text(),
        },
        ErrorCode::NodeLabelLimit {
            node: "alice".into(),
            label: "Person".into(),
            limit: 64,
        },
    ];
    for constraint in [
        "not_null",
        "unique",
        "generated_always",
        "fk_missing",
        "rls_policy",
        "permission_denied",
        "check",
    ] {
        samples.push(ErrorCode::RejectedConstraint {
            constraint: constraint.into(),
            detail: text(),
        });
    }
    for fault in [
        CounterFault::NotAnInteger,
        CounterFault::NotAFloat,
        CounterFault::IntegerOverflow,
        CounterFault::NonFinite,
    ] {
        samples.push(ErrorCode::CounterFault {
            collection: collection(),
            fault,
        });
    }
    samples
}
