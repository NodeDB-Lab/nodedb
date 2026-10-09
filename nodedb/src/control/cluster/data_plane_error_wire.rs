// SPDX-License-Identifier: BUSL-1.1

//! Lossless conversion between the Data-Plane [`ErrorCode`] and its cluster
//! wire mirror [`DataPlaneErrorCode`]. The mapping every cross-node executor
//! uses to answer with a local execution error lives in
//! [`super::execution_error_wire`] and is re-exported here.
//!
//! Both matches are exhaustive with no catch-all, so a new `ErrorCode` variant
//! fails to compile here until it is mirrored on the wire instead of silently
//! degrading to `Internal` and losing its SQLSTATE at the coordinator.

use nodedb_cluster::rpc_codec::DataPlaneErrorCode;

use super::data_plane_fault_wire::{
    counter_fault_from_wire, counter_fault_to_wire, sync_hold_from_wire, sync_hold_to_wire,
    text_column_fault_from_wire, text_column_fault_to_wire,
};
pub(crate) use super::execution_error_wire::{execution_error_to_typed, numeric_typed};
use crate::bridge::envelope::ErrorCode;

/// Widen a pointer-width count to the wire's fixed `u64`.
fn to_wire_count(value: usize) -> u64 {
    value as u64
}

/// Narrow a wire count to pointer width, saturating on a 32-bit receiver
/// rather than wrapping — the value is a diagnostic bound, never an index.
fn from_wire_count(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

impl From<ErrorCode> for DataPlaneErrorCode {
    fn from(code: ErrorCode) -> Self {
        match code {
            ErrorCode::DeadlineExceeded => Self::DeadlineExceeded,
            ErrorCode::RejectedConstraint { constraint, detail } => {
                Self::RejectedConstraint { constraint, detail }
            }
            ErrorCode::RejectedPrevalidation { reason } => Self::RejectedPrevalidation { reason },
            ErrorCode::RetryableRefusal { reason } => Self::RetryableRefusal { reason },
            ErrorCode::CoreFailStopped { core_id, detail } => Self::CoreFailStopped {
                core_id: to_wire_count(core_id),
                detail,
            },
            ErrorCode::SyncRejected {
                violation,
                applied_seq,
                provenance,
            } => Self::SyncRejected {
                violation,
                applied_seq,
                producer_id: provenance.producer_id,
                epoch: provenance.epoch,
                stream_id: provenance.stream_id,
                seq: provenance.seq,
            },
            ErrorCode::SyncNotApplied { hold, applied_seq } => Self::SyncNotApplied {
                hold: sync_hold_to_wire(hold),
                applied_seq,
            },
            ErrorCode::NotFound => Self::NotFound,
            ErrorCode::RejectedAuthz { resource } => Self::RejectedAuthz { resource },
            ErrorCode::ConflictRetry => Self::ConflictRetry,
            ErrorCode::CrdtFrontierMismatch { expected, actual } => {
                Self::CrdtFrontierMismatch { expected, actual }
            }
            ErrorCode::ResourcesExhausted => Self::ResourcesExhausted,
            ErrorCode::RejectedDanglingEdge { missing_node } => {
                Self::RejectedDanglingEdge { missing_node }
            }
            ErrorCode::DuplicateWrite => Self::DuplicateWrite,
            ErrorCode::AppendOnlyViolation { collection } => {
                Self::AppendOnlyViolation { collection }
            }
            ErrorCode::BalanceViolation { collection, detail } => {
                Self::BalanceViolation { collection, detail }
            }
            ErrorCode::PeriodLocked { collection } => Self::PeriodLocked { collection },
            ErrorCode::PeriodLockMisconfigured {
                collection,
                ref_table,
                status_column,
                row_identity,
            } => Self::PeriodLockMisconfigured {
                collection,
                ref_table,
                status_column,
                row_identity,
            },
            ErrorCode::RetentionViolation { collection } => Self::RetentionViolation { collection },
            ErrorCode::LegalHoldActive { collection } => Self::LegalHoldActive { collection },
            ErrorCode::StateTransitionViolation { collection, detail } => {
                Self::StateTransitionViolation { collection, detail }
            }
            ErrorCode::TransitionCheckViolation { collection, detail } => {
                Self::TransitionCheckViolation { collection, detail }
            }
            ErrorCode::TypeGuardViolation { collection, detail } => {
                Self::TypeGuardViolation { collection, detail }
            }
            ErrorCode::TypeMismatch { collection, detail } => {
                Self::TypeMismatch { collection, detail }
            }
            ErrorCode::CounterFault { collection, fault } => Self::CounterFault {
                collection,
                fault: counter_fault_to_wire(fault),
            },
            ErrorCode::InsufficientBalance { collection, detail } => {
                Self::InsufficientBalance { collection, detail }
            }
            ErrorCode::RateExceeded {
                gate,
                retry_after_ms,
            } => Self::RateExceeded {
                gate,
                retry_after_ms,
            },
            ErrorCode::CollectionDraining { collection } => Self::CollectionDraining { collection },
            ErrorCode::RecursionDepthExceeded {
                cte_name,
                max_depth,
            } => Self::RecursionDepthExceeded {
                cte_name,
                max_depth: to_wire_count(max_depth),
            },
            ErrorCode::UndefinedColumn { column } => Self::UndefinedColumn { column },
            ErrorCode::Internal { detail } => Self::Internal { detail },
            ErrorCode::Unsupported { detail } => Self::Unsupported { detail },
            ErrorCode::RollbackFailed {
                entry_index,
                detail,
                cause,
            } => Self::RollbackFailed {
                entry_index: to_wire_count(entry_index),
                detail,
                cause: cause.map(|cause| Box::new(Self::from(*cause))),
            },
            ErrorCode::OllpRetryRequired => Self::OllpRetryRequired,
            ErrorCode::TxnOverlayMemoryExceeded { limit } => Self::TxnOverlayMemoryExceeded {
                limit: to_wire_count(limit),
            },
            ErrorCode::DivisionByZero => Self::DivisionByZero,
            ErrorCode::UndefinedFunction { name } => Self::UndefinedFunction { name },
            ErrorCode::DataException { detail } => Self::DataException { detail },
            ErrorCode::NumericValueOutOfRange { detail } => Self::NumericValueOutOfRange { detail },
            ErrorCode::DispatchCapacity { reason } => Self::DispatchCapacity { reason },
            ErrorCode::ExpiredBeforeExecution => Self::ExpiredBeforeExecution,
            ErrorCode::BadRequest { detail } => Self::BadRequest { detail },
            ErrorCode::TransactionRollback { detail } => Self::TransactionRollback { detail },
            ErrorCode::ActiveSqlTransaction { detail } => Self::ActiveSqlTransaction { detail },
            ErrorCode::DependentObjectsExist { object, detail } => {
                Self::DependentObjectsExist { object, detail }
            }
            ErrorCode::NodeLabelLimit { node, label, limit } => Self::NodeLabelLimit {
                node,
                label,
                limit: to_wire_count(limit),
            },
            ErrorCode::InvalidTextRepresentation { detail } => {
                Self::InvalidTextRepresentation { detail }
            }
            ErrorCode::DatatypeMismatch { detail } => Self::DatatypeMismatch { detail },
            ErrorCode::InvalidDatetimeFormat { detail } => Self::InvalidDatetimeFormat { detail },
            ErrorCode::DatetimeFieldOverflow { detail } => Self::DatetimeFieldOverflow { detail },
            ErrorCode::UndefinedObject { object } => Self::UndefinedObject { object },
            ErrorCode::ObjectNotInPrerequisiteState { object, detail } => {
                Self::ObjectNotInPrerequisiteState { object, detail }
            }
            ErrorCode::TextColumn {
                collection,
                column,
                fault,
            } => Self::TextColumn {
                collection,
                column,
                fault: text_column_fault_to_wire(fault),
            },
        }
    }
}

impl From<DataPlaneErrorCode> for ErrorCode {
    fn from(code: DataPlaneErrorCode) -> Self {
        match code {
            DataPlaneErrorCode::DeadlineExceeded => Self::DeadlineExceeded,
            DataPlaneErrorCode::RejectedConstraint { constraint, detail } => {
                Self::RejectedConstraint { constraint, detail }
            }
            DataPlaneErrorCode::RejectedPrevalidation { reason } => {
                Self::RejectedPrevalidation { reason }
            }
            DataPlaneErrorCode::RetryableRefusal { reason } => Self::RetryableRefusal { reason },
            DataPlaneErrorCode::CoreFailStopped { core_id, detail } => Self::CoreFailStopped {
                core_id: from_wire_count(core_id),
                detail,
            },
            DataPlaneErrorCode::SyncRejected {
                violation,
                applied_seq,
                producer_id,
                epoch,
                stream_id,
                seq,
            } => Self::SyncRejected {
                violation,
                applied_seq,
                provenance: nodedb_types::sync::wire::SyncProvenance {
                    producer_id,
                    epoch,
                    stream_id,
                    seq,
                },
            },
            DataPlaneErrorCode::SyncNotApplied { hold, applied_seq } => Self::SyncNotApplied {
                hold: sync_hold_from_wire(hold),
                applied_seq,
            },
            DataPlaneErrorCode::NotFound => Self::NotFound,
            DataPlaneErrorCode::RejectedAuthz { resource } => Self::RejectedAuthz { resource },
            DataPlaneErrorCode::ConflictRetry => Self::ConflictRetry,
            DataPlaneErrorCode::CrdtFrontierMismatch { expected, actual } => {
                Self::CrdtFrontierMismatch { expected, actual }
            }
            DataPlaneErrorCode::ResourcesExhausted => Self::ResourcesExhausted,
            DataPlaneErrorCode::RejectedDanglingEdge { missing_node } => {
                Self::RejectedDanglingEdge { missing_node }
            }
            DataPlaneErrorCode::DuplicateWrite => Self::DuplicateWrite,
            DataPlaneErrorCode::AppendOnlyViolation { collection } => {
                Self::AppendOnlyViolation { collection }
            }
            DataPlaneErrorCode::BalanceViolation { collection, detail } => {
                Self::BalanceViolation { collection, detail }
            }
            DataPlaneErrorCode::PeriodLocked { collection } => Self::PeriodLocked { collection },
            DataPlaneErrorCode::PeriodLockMisconfigured {
                collection,
                ref_table,
                status_column,
                row_identity,
            } => Self::PeriodLockMisconfigured {
                collection,
                ref_table,
                status_column,
                row_identity,
            },
            DataPlaneErrorCode::RetentionViolation { collection } => {
                Self::RetentionViolation { collection }
            }
            DataPlaneErrorCode::LegalHoldActive { collection } => {
                Self::LegalHoldActive { collection }
            }
            DataPlaneErrorCode::StateTransitionViolation { collection, detail } => {
                Self::StateTransitionViolation { collection, detail }
            }
            DataPlaneErrorCode::TransitionCheckViolation { collection, detail } => {
                Self::TransitionCheckViolation { collection, detail }
            }
            DataPlaneErrorCode::TypeGuardViolation { collection, detail } => {
                Self::TypeGuardViolation { collection, detail }
            }
            DataPlaneErrorCode::TypeMismatch { collection, detail } => {
                Self::TypeMismatch { collection, detail }
            }
            DataPlaneErrorCode::CounterFault { collection, fault } => Self::CounterFault {
                collection,
                fault: counter_fault_from_wire(fault),
            },
            DataPlaneErrorCode::InsufficientBalance { collection, detail } => {
                Self::InsufficientBalance { collection, detail }
            }
            DataPlaneErrorCode::RateExceeded {
                gate,
                retry_after_ms,
            } => Self::RateExceeded {
                gate,
                retry_after_ms,
            },
            DataPlaneErrorCode::CollectionDraining { collection } => {
                Self::CollectionDraining { collection }
            }
            DataPlaneErrorCode::RecursionDepthExceeded {
                cte_name,
                max_depth,
            } => Self::RecursionDepthExceeded {
                cte_name,
                max_depth: from_wire_count(max_depth),
            },
            DataPlaneErrorCode::UndefinedColumn { column } => Self::UndefinedColumn { column },
            DataPlaneErrorCode::Internal { detail } => Self::Internal { detail },
            DataPlaneErrorCode::Unsupported { detail } => Self::Unsupported { detail },
            DataPlaneErrorCode::RollbackFailed {
                entry_index,
                detail,
                cause,
            } => Self::RollbackFailed {
                entry_index: from_wire_count(entry_index),
                detail,
                cause: cause.map(|cause| Box::new(Self::from(*cause))),
            },
            DataPlaneErrorCode::OllpRetryRequired => Self::OllpRetryRequired,
            DataPlaneErrorCode::TxnOverlayMemoryExceeded { limit } => {
                Self::TxnOverlayMemoryExceeded {
                    limit: from_wire_count(limit),
                }
            }
            DataPlaneErrorCode::DivisionByZero => Self::DivisionByZero,
            DataPlaneErrorCode::UndefinedFunction { name } => Self::UndefinedFunction { name },
            DataPlaneErrorCode::DataException { detail } => Self::DataException { detail },
            DataPlaneErrorCode::NumericValueOutOfRange { detail } => {
                Self::NumericValueOutOfRange { detail }
            }
            DataPlaneErrorCode::DispatchCapacity { reason } => Self::DispatchCapacity { reason },
            DataPlaneErrorCode::ExpiredBeforeExecution => Self::ExpiredBeforeExecution,
            DataPlaneErrorCode::BadRequest { detail } => Self::BadRequest { detail },
            DataPlaneErrorCode::TransactionRollback { detail } => {
                Self::TransactionRollback { detail }
            }
            DataPlaneErrorCode::ActiveSqlTransaction { detail } => {
                Self::ActiveSqlTransaction { detail }
            }
            DataPlaneErrorCode::DependentObjectsExist { object, detail } => {
                Self::DependentObjectsExist { object, detail }
            }
            DataPlaneErrorCode::NodeLabelLimit { node, label, limit } => Self::NodeLabelLimit {
                node,
                label,
                limit: from_wire_count(limit),
            },
            DataPlaneErrorCode::InvalidTextRepresentation { detail } => {
                Self::InvalidTextRepresentation { detail }
            }
            DataPlaneErrorCode::DatatypeMismatch { detail } => Self::DatatypeMismatch { detail },
            DataPlaneErrorCode::InvalidDatetimeFormat { detail } => {
                Self::InvalidDatetimeFormat { detail }
            }
            DataPlaneErrorCode::DatetimeFieldOverflow { detail } => {
                Self::DatetimeFieldOverflow { detail }
            }
            DataPlaneErrorCode::UndefinedObject { object } => Self::UndefinedObject { object },
            DataPlaneErrorCode::ObjectNotInPrerequisiteState { object, detail } => {
                Self::ObjectNotInPrerequisiteState { object, detail }
            }
            DataPlaneErrorCode::TextColumn {
                collection,
                column,
                fault,
            } => Self::TextColumn {
                collection,
                column,
                fault: text_column_fault_from_wire(fault),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{CounterFault, SyncHold};
    use nodedb_cluster::rpc_codec::TypedClusterError;
    use nodedb_types::text_search::TextColumnFault;

    #[test]
    fn text_column_code_roundtrips_verbatim() {
        for fault in [
            TextColumnFault::Undeclared,
            TextColumnFault::NotText {
                data_type: "INT".into(),
            },
            TextColumnFault::NotAColumn,
            TextColumnFault::NotIndexed,
        ] {
            let original = ErrorCode::TextColumn {
                collection: "docs".into(),
                column: "title".into(),
                fault,
            };
            let wire = DataPlaneErrorCode::from(original.clone());
            assert_eq!(ErrorCode::from(wire), original);
        }
    }

    #[test]
    fn a_core_fail_stop_roundtrips_verbatim() {
        let original = ErrorCode::CoreFailStopped {
            core_id: 3,
            detail: "rollback failed".into(),
        };
        let wire = DataPlaneErrorCode::from(original.clone());
        assert_eq!(ErrorCode::from(wire), original);
    }

    #[test]
    fn division_by_zero_survives_the_wire_hop() {
        let wire = DataPlaneErrorCode::from(ErrorCode::DivisionByZero);
        assert_eq!(ErrorCode::from(wire), ErrorCode::DivisionByZero);
    }

    #[test]
    fn object_state_codes_roundtrip_verbatim() {
        for original in [
            ErrorCode::UndefinedObject {
                object: "document \"doc\" in collection \"notes\"".into(),
            },
            ErrorCode::ObjectNotInPrerequisiteState {
                object: "CRDT version".into(),
                detail: "version predates the compaction boundary".into(),
            },
        ] {
            let wire = DataPlaneErrorCode::from(original.clone());
            assert_eq!(ErrorCode::from(wire), original);
        }
    }

    #[test]
    fn payload_bearing_code_roundtrips_verbatim() {
        let original = ErrorCode::RejectedConstraint {
            constraint: "unique".into(),
            detail: "key (id)=(7) already exists".into(),
        };
        let wire = DataPlaneErrorCode::from(original.clone());
        assert_eq!(ErrorCode::from(wire), original);
    }

    /// A value refusal crosses the hop as its own verdict, from a Data-Plane
    /// code and from a Control-Plane error alike.
    #[test]
    fn value_refusals_cross_the_hop_verbatim() {
        let detail = "column 'n': cannot parse 'x' as INT".to_string();
        for original in [
            ErrorCode::InvalidTextRepresentation {
                detail: detail.clone(),
            },
            ErrorCode::DatatypeMismatch {
                detail: detail.clone(),
            },
            ErrorCode::InvalidDatetimeFormat {
                detail: detail.clone(),
            },
            ErrorCode::DatetimeFieldOverflow {
                detail: detail.clone(),
            },
        ] {
            let wire = DataPlaneErrorCode::from(original.clone());
            assert_eq!(ErrorCode::from(wire), original);
        }
        match execution_error_to_typed(crate::Error::InvalidTextRepresentation {
            detail: detail.clone(),
        }) {
            TypedClusterError::DataPlane { code } => assert_eq!(
                code,
                DataPlaneErrorCode::InvalidTextRepresentation {
                    detail: detail.clone()
                }
            ),
            other => panic!("expected DataPlane, got {other:?}"),
        }
        match execution_error_to_typed(crate::Error::DatatypeMismatch {
            detail: detail.clone(),
        }) {
            TypedClusterError::DataPlane { code } => {
                assert_eq!(code, DataPlaneErrorCode::DatatypeMismatch { detail })
            }
            other => panic!("expected DataPlane, got {other:?}"),
        }
    }

    #[test]
    fn execution_error_keeps_a_data_plane_verdict_typed() {
        let typed = execution_error_to_typed(crate::Error::DataPlane(ErrorCode::DivisionByZero));
        match typed {
            TypedClusterError::DataPlane { code } => {
                assert_eq!(code, DataPlaneErrorCode::DivisionByZero);
            }
            other => panic!("expected DataPlane, got {other:?}"),
        }
    }

    /// A non-verdict failure stays `Internal`, but with its real numeric
    /// class rather than a plan-decode code.
    #[test]
    fn execution_error_classifies_a_non_verdict_failure() {
        let typed = execution_error_to_typed(crate::Error::PlanError {
            detail: "unresolved exchange".to_owned(),
        });
        match typed {
            TypedClusterError::Internal { code, message } => {
                assert_ne!(code, nodedb_cluster::rpc_codec::PLAN_DECODE_FAILED);
                assert!(message.contains("unresolved exchange"));
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_capacity_code_roundtrips_verbatim() {
        let original = ErrorCode::DispatchCapacity {
            reason: "tenant 1 holds 64/64 in-flight requests".into(),
        };
        let wire = DataPlaneErrorCode::from(original.clone());
        assert_eq!(
            wire,
            DataPlaneErrorCode::DispatchCapacity {
                reason: "tenant 1 holds 64/64 in-flight requests".into(),
            }
        );
        assert_eq!(ErrorCode::from(wire), original);
    }

    #[test]
    fn sync_not_applied_roundtrips_verbatim() {
        for hold in [
            SyncHold::Duplicate,
            SyncHold::Fenced,
            SyncHold::Gap { expected: 7 },
        ] {
            let original = ErrorCode::SyncNotApplied {
                hold,
                applied_seq: 6,
            };
            let wire = DataPlaneErrorCode::from(original.clone());
            assert_eq!(ErrorCode::from(wire), original);
        }
    }

    #[test]
    fn expired_before_execution_roundtrips_verbatim() {
        let wire = DataPlaneErrorCode::from(ErrorCode::ExpiredBeforeExecution);
        assert_eq!(wire, DataPlaneErrorCode::ExpiredBeforeExecution);
        assert_eq!(ErrorCode::from(wire), ErrorCode::ExpiredBeforeExecution);
    }

    #[test]
    fn counter_fault_roundtrips_verbatim() {
        for fault in [
            CounterFault::NotAnInteger,
            CounterFault::NotAFloat,
            CounterFault::IntegerOverflow,
            CounterFault::NonFinite,
        ] {
            let original = ErrorCode::CounterFault {
                collection: "counters".into(),
                fault,
            };
            let wire = DataPlaneErrorCode::from(original.clone());
            assert_eq!(ErrorCode::from(wire), original);
        }
    }

    #[test]
    fn counted_code_roundtrips_across_the_u64_wire_field() {
        let original = ErrorCode::RecursionDepthExceeded {
            cte_name: "parts".into(),
            max_depth: 128,
        };
        let wire = DataPlaneErrorCode::from(original.clone());
        assert_eq!(ErrorCode::from(wire), original);
    }

    /// A failed rollback crosses the hop with the typed cause of its
    /// reverse write, nested codes included.
    #[test]
    fn rollback_failed_keeps_its_typed_cause_across_the_hop() {
        for cause in [
            None,
            Some(Box::new(ErrorCode::Internal {
                detail: "storage error (sparse): commit".into(),
            })),
            Some(Box::new(ErrorCode::RollbackFailed {
                entry_index: 1,
                detail: "inner".into(),
                cause: Some(Box::new(ErrorCode::DivisionByZero)),
            })),
        ] {
            let original = ErrorCode::RollbackFailed {
                entry_index: 4,
                detail: "restoring row r1".into(),
                cause,
            };
            let wire = DataPlaneErrorCode::from(original.clone());
            assert_eq!(ErrorCode::from(wire), original);
        }
    }
}
