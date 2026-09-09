// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane [`ErrorCode`] to public [`NodeDbError`] conversion.
//!
//! Split out of `error_from` because the match is exhaustive over every
//! Data-Plane code and would otherwise push that file well past the size
//! limit. Exhaustiveness is the point: a code that degrades to
//! `NodeDbError::internal` reaches the client as NDB-9000, where a duplicate
//! key is indistinguishable from a crashed database — so the compiler is made
//! to name every new variant here instead of a catch-all absorbing it.

use nodedb_types::error::{ErrorCode as PublicCode, NodeDbError};

use crate::bridge::envelope::ErrorCode;

/// Convert a deterministic Data-Plane code into the public error a client
/// can classify.
///
/// Codes with a structured public counterpart use the dedicated constructor
/// so the collection / gate travels in `ErrorDetails` as well as the message.
/// The rest go through [`NodeDbError::from_wire`], which pairs the numeric
/// code with a rendered message — the same single mapping table, not a second
/// one.
pub(crate) fn data_plane_code_to_public(code: ErrorCode) -> NodeDbError {
    match code {
        ErrorCode::DeadlineExceeded => NodeDbError::deadline_exceeded(),
        // The Data Plane's `RejectedConstraint` carries no collection name,
        // only the constraint kind and detail — leave collection blank
        // rather than misreport the kind string as the collection.
        ErrorCode::RejectedConstraint { constraint, detail } => {
            NodeDbError::constraint_violation("", constraint, detail)
        }
        ErrorCode::RejectedPrevalidation { reason } => {
            NodeDbError::prevalidation_rejected("data plane", reason)
        }
        // Nothing was applied and the identical frame is expected to succeed
        // once the transient precondition resolves, so it presents as the
        // retriable class rather than a permanent refusal.
        ErrorCode::RetryableRefusal { reason } => NodeDbError::from_wire(
            PublicCode::WRITE_CONFLICT,
            format!("write refused without applying, retry: {reason}"),
        ),
        // The Data Plane cannot distinguish an absent collection from an
        // absent row through this code, and `document_not_found` is the
        // narrower of the two claims: it never asserts the collection is
        // gone. Both answer `is_not_found()`.
        ErrorCode::NotFound => NodeDbError::from_wire(PublicCode::DOCUMENT_NOT_FOUND, "not found"),
        // `resource` says what refused the request and why — an RLS policy on
        // a named collection, a missing grant. It lands in
        // `ErrorDetails::AuthorizationDenied { resource }`, so a client can
        // match on it instead of parsing prose.
        ErrorCode::RejectedAuthz { resource } => NodeDbError::authorization_denied(resource),
        ErrorCode::ConflictRetry => NodeDbError::write_conflict("", ""),
        ErrorCode::CrdtFrontierMismatch { .. } => NodeDbError::from_wire(
            PublicCode::WRITE_CONFLICT,
            "CRDT state changed after preview; retry the write",
        ),
        ErrorCode::FanOutExceeded => NodeDbError::fan_out_exceeded(0, 0),
        ErrorCode::ResourcesExhausted => NodeDbError::memory_exhausted("query"),
        // A dangling edge is a referential-integrity refusal, which the
        // public surface expresses as a constraint violation.
        ErrorCode::RejectedDanglingEdge { missing_node } => NodeDbError::constraint_violation(
            "",
            "foreign_key",
            format!("edge rejected: node '{missing_node}' does not exist"),
        ),
        ErrorCode::DuplicateWrite => NodeDbError::constraint_violation(
            "",
            "unique",
            "duplicate write detected via idempotency key",
        ),
        ErrorCode::AppendOnlyViolation { collection } => {
            NodeDbError::append_only_violation(collection, "UPDATE/DELETE not allowed")
        }
        ErrorCode::BalanceViolation { collection, detail } => {
            NodeDbError::balance_violation(collection, detail)
        }
        ErrorCode::PeriodLocked { collection } => {
            NodeDbError::period_locked(collection, "writes rejected")
        }
        ErrorCode::RetentionViolation { collection } => {
            NodeDbError::retention_violation(collection, "retention period has not expired")
        }
        ErrorCode::LegalHoldActive { collection } => {
            NodeDbError::legal_hold_active(collection, "delete rejected")
        }
        ErrorCode::StateTransitionViolation { collection, detail } => {
            NodeDbError::state_transition_violation(collection, detail)
        }
        ErrorCode::TransitionCheckViolation { collection, detail } => {
            NodeDbError::transition_check_violation(collection, detail)
        }
        ErrorCode::TypeGuardViolation { collection, detail } => {
            NodeDbError::type_guard_violation(collection, detail)
        }
        ErrorCode::TypeMismatch { collection, detail } => {
            NodeDbError::type_mismatch(collection, detail)
        }
        ErrorCode::OverflowError { collection } => {
            NodeDbError::overflow(collection, "arithmetic overflow")
        }
        ErrorCode::InsufficientBalance { collection, detail } => {
            NodeDbError::insufficient_balance(collection, detail)
        }
        ErrorCode::RateExceeded {
            gate,
            retry_after_ms,
        } => NodeDbError::rate_exceeded(gate, format!("retry after {retry_after_ms}ms")),
        ErrorCode::CollectionDraining { collection } => {
            NodeDbError::collection_draining(collection)
        }
        ErrorCode::RecursionDepthExceeded {
            cte_name,
            max_depth,
        } => NodeDbError::bad_request(format!(
            "WITH RECURSIVE CTE '{cte_name}' exceeded max recursion depth {max_depth}; \
             add a stricter termination condition or raise max_recursion_depth"
        )),
        ErrorCode::UndefinedColumn { column } => NodeDbError::undefined_column(column),
        ErrorCode::Unsupported { detail } => NodeDbError::bad_request(detail),
        ErrorCode::DivisionByZero => NodeDbError::division_by_zero(),
        ErrorCode::TxnOverlayMemoryExceeded { limit } => NodeDbError::bad_request(format!(
            "transaction staging overlay exceeded its {limit}-byte per-core budget; \
             split the transaction into smaller batches"
        )),
        // Genuinely internal: the shard is in an unknown or faulted state, or
        // a scheduler signal leaked past the layer that should have consumed
        // it. These are the only codes for which NDB-9000 is the truth.
        ErrorCode::Internal { detail } => NodeDbError::internal(detail),
        ErrorCode::RollbackFailed {
            entry_index,
            detail,
        } => NodeDbError::internal(format!(
            "transaction rollback failed at undo entry {entry_index}: {detail}; \
             shard state is unknown — restart required"
        )),
        ErrorCode::OllpRetryRequired => {
            NodeDbError::internal("optimistic predicate retry required")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_code_classifies_as_constraint_violation() {
        let e = data_plane_code_to_public(ErrorCode::RejectedConstraint {
            constraint: "unique".into(),
            detail: "unique index 'idx_users_email' violation".into(),
        });
        assert!(e.is_constraint_violation());
        assert_eq!(e.code(), PublicCode::CONSTRAINT_VIOLATION);
    }

    #[test]
    fn not_found_code_classifies_as_not_found() {
        assert!(data_plane_code_to_public(ErrorCode::NotFound).is_not_found());
    }

    #[test]
    fn authz_and_rate_codes_keep_their_categories() {
        assert!(
            data_plane_code_to_public(ErrorCode::RejectedAuthz {
                resource: "RLS write policy on 'orders'".into(),
            })
            .is_auth_denied()
        );
        assert!(
            data_plane_code_to_public(ErrorCode::RateExceeded {
                gate: "login".into(),
                retry_after_ms: 500,
            })
            .is_rate_exceeded()
        );
    }

    #[test]
    fn internal_stays_internal() {
        let e = data_plane_code_to_public(ErrorCode::Internal {
            detail: "io_uring submission failed".into(),
        });
        assert!(e.is_internal());
        assert_eq!(e.code(), PublicCode::INTERNAL);
    }
}
