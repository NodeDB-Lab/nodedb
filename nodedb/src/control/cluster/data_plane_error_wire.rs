// SPDX-License-Identifier: BUSL-1.1

//! Lossless conversion between the Data-Plane [`ErrorCode`] and its cluster
//! wire mirror [`DataPlaneErrorCode`], plus the one mapping every cross-node
//! executor uses to answer with a local execution error.
//!
//! Both matches are exhaustive with no catch-all, so a new `ErrorCode` variant
//! fails to compile here until it is mirrored on the wire instead of silently
//! degrading to `Internal` and losing its SQLSTATE at the coordinator.

use nodedb_cluster::rpc_codec::{DataPlaneErrorCode, TypedClusterError};

use crate::bridge::envelope::ErrorCode;

/// Map a local-execution [`crate::Error`] to the wire error a remote caller
/// receives.
///
/// A Data-Plane verdict crosses verbatim as `TypedClusterError::DataPlane`, so
/// the coordinator rebuilds `Error::DataPlane(code)` and renders the SQLSTATE
/// single-node execution renders. Every other error keeps its own numeric
/// classification from `NodeDbError::from(err).code()` — never a hardcoded
/// plan-decode code, which would misname what failed.
pub(crate) fn execution_error_to_typed(err: crate::Error) -> TypedClusterError {
    match err {
        crate::Error::DataPlane(code) => TypedClusterError::DataPlane { code: code.into() },
        // A statement that ran out of time keeps the wire's own deadline
        // variant, which the coordinator rebuilds as `Error::DeadlineExceeded`.
        // Folding it into `Internal` would report a client's own timeout as an
        // internal failure once it crossed a node boundary.
        crate::Error::DeadlineExceeded { .. } => {
            TypedClusterError::DeadlineExceeded { elapsed_ms: 0 }
        }
        // A Control-Plane constraint refusal crosses verbatim, same as a
        // Data-Plane verdict, so the coordinator answers 23502 vs 23505
        // instead of flattening both into one numeric class.
        crate::Error::RejectedConstraint {
            collection,
            constraint,
            detail,
        } => TypedClusterError::RejectedConstraint {
            collection,
            constraint,
            detail,
        },
        other => {
            let message = other.to_string();
            let code = u32::from(nodedb_types::error::NodeDbError::from(other).code().0);
            TypedClusterError::Internal { code, message }
        }
    }
}

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
            ErrorCode::NotFound => Self::NotFound,
            ErrorCode::RejectedAuthz { resource } => Self::RejectedAuthz { resource },
            ErrorCode::ConflictRetry => Self::ConflictRetry,
            ErrorCode::CrdtFrontierMismatch { expected, actual } => {
                Self::CrdtFrontierMismatch { expected, actual }
            }
            ErrorCode::FanOutExceeded => Self::FanOutExceeded,
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
            ErrorCode::OverflowError { collection } => Self::OverflowError { collection },
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
            } => Self::RollbackFailed {
                entry_index: to_wire_count(entry_index),
                detail,
            },
            ErrorCode::OllpRetryRequired => Self::OllpRetryRequired,
            ErrorCode::TxnOverlayMemoryExceeded { limit } => Self::TxnOverlayMemoryExceeded {
                limit: to_wire_count(limit),
            },
            ErrorCode::DivisionByZero => Self::DivisionByZero,
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
            DataPlaneErrorCode::NotFound => Self::NotFound,
            DataPlaneErrorCode::RejectedAuthz { resource } => Self::RejectedAuthz { resource },
            DataPlaneErrorCode::ConflictRetry => Self::ConflictRetry,
            DataPlaneErrorCode::CrdtFrontierMismatch { expected, actual } => {
                Self::CrdtFrontierMismatch { expected, actual }
            }
            DataPlaneErrorCode::FanOutExceeded => Self::FanOutExceeded,
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
            DataPlaneErrorCode::OverflowError { collection } => Self::OverflowError { collection },
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
            } => Self::RollbackFailed {
                entry_index: from_wire_count(entry_index),
                detail,
            },
            DataPlaneErrorCode::OllpRetryRequired => Self::OllpRetryRequired,
            DataPlaneErrorCode::TxnOverlayMemoryExceeded { limit } => {
                Self::TxnOverlayMemoryExceeded {
                    limit: from_wire_count(limit),
                }
            }
            DataPlaneErrorCode::DivisionByZero => Self::DivisionByZero,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn division_by_zero_survives_the_wire_hop() {
        let wire = DataPlaneErrorCode::from(ErrorCode::DivisionByZero);
        assert_eq!(ErrorCode::from(wire), ErrorCode::DivisionByZero);
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
    fn counted_code_roundtrips_across_the_u64_wire_field() {
        let original = ErrorCode::RecursionDepthExceeded {
            cte_name: "parts".into(),
            max_depth: 128,
        };
        let wire = DataPlaneErrorCode::from(original.clone());
        assert_eq!(ErrorCode::from(wire), original);
    }
}
