// SPDX-License-Identifier: Apache-2.0

//! Reconstruct a typed [`NodeDbError`] from a SQLSTATE received over the wire.
//!
//! The native protocol's `ErrorPayload` carries a five-character SQLSTATE and a
//! human-readable message. The SQLSTATE is the server's own classification of
//! the failure, so a client that reads only the message throws away every
//! machine-matchable fact about the error and leaves callers with nothing to
//! branch on.
//!
//! # Fidelity limits
//!
//! A SQLSTATE classifies; it does not carry operands. Variants with payload
//! fields (`CollectionNotFound { collection }`, `UndefinedFunction { name }`)
//! are therefore rebuilt with those fields empty: the failing name lives in the
//! message, which is preserved verbatim, but it is not recoverable as
//! structured data. The variant, the [`ErrorCode`], and every predicate derived
//! from them ([`NodeDbError::is_retriable`], [`NodeDbError::is_not_found`], …)
//! are exact. An empty field reads as "not transmitted"; guessing the operand
//! out of the message text would be worse than leaving it blank.
//!
//! Codes are mapped only where a SQLSTATE and an [`ErrorDetails`] variant
//! correspond one-to-one. Three groups are deliberately left unmapped:
//!
//! - **Overloaded codes.** `55P03` is both `LOCK_NOT_AVAILABLE` (no leader,
//!   retriable) and `STALE_READ_NOT_LEADER` (redirect to source, not
//!   retriable); `57P03` is both a draining collection and server overload;
//!   `53400` is both quota-exceeded and quota-overcommit. Picking either side
//!   would misreport retriability for the other.
//! - **Load-bearing payloads.** `57P04` maps to `NotLeader { leader_addr }` and
//!   `54001` to `FanOutExceeded { shards_touched, limit }`. Unlike a name, a
//!   redirect address or a shard count is acted on rather than displayed, and
//!   an empty or zero value would be a fabricated instruction.
//! - **Codes with no variant.** `42P07` (duplicate table) and `0A000`
//!   (feature not supported) have no `ErrorDetails` equivalent today.
//!
//! Everything unmapped falls through to [`NodeDbError::internal`], which is
//! exactly what every server error produced before this mapping existed.

use super::super::code::ErrorCode;
use super::super::details::ErrorDetails;
use super::super::sqlstate;
use super::super::types::NodeDbError;

impl NodeDbError {
    /// Rebuild a typed error from a server-sent SQLSTATE and message.
    ///
    /// `message` is preserved verbatim on mapped codes. Unmapped codes produce
    /// the same [`NodeDbError::internal`] this function replaced, so a server
    /// emitting a SQLSTATE no client knows yet is no worse off than before.
    pub fn from_sqlstate(sqlstate: &str, message: &str) -> Self {
        let Some((code, details)) = classify(sqlstate) else {
            return Self::internal(message);
        };
        Self {
            code,
            message: message.to_owned(),
            details,
            cause: None,
        }
    }
}

/// Map a SQLSTATE onto the code/details pair that classifies it, or `None`
/// when no variant corresponds.
fn classify(sqlstate: &str) -> Option<(ErrorCode, ErrorDetails)> {
    let empty = String::new;
    let pair = match sqlstate {
        // ── Data exception ──
        sqlstate::NUMERIC_VALUE_OUT_OF_RANGE => (
            ErrorCode::OVERFLOW,
            ErrorDetails::Overflow {
                collection: empty(),
            },
        ),
        sqlstate::DIVISION_BY_ZERO => (ErrorCode::DIVISION_BY_ZERO, ErrorDetails::DivisionByZero),

        // ── Integrity constraint violation ──
        //
        // The generic class-23 codes all describe the same client-visible
        // condition: a write the collection's constraints refused.
        sqlstate::INTEGRITY_CONSTRAINT_VIOLATION
        | sqlstate::NOT_NULL_VIOLATION
        | sqlstate::FOREIGN_KEY_VIOLATION
        | sqlstate::UNIQUE_VIOLATION
        | sqlstate::CHECK_VIOLATION => (
            ErrorCode::CONSTRAINT_VIOLATION,
            ErrorDetails::ConstraintViolation {
                collection: empty(),
            },
        ),
        // NodeDB's class-23 extensions each have a dedicated variant.
        sqlstate::APPEND_ONLY_VIOLATION => (
            ErrorCode::APPEND_ONLY_VIOLATION,
            ErrorDetails::AppendOnlyViolation {
                collection: empty(),
            },
        ),
        sqlstate::BALANCE_VIOLATION => (
            ErrorCode::BALANCE_VIOLATION,
            ErrorDetails::BalanceViolation {
                collection: empty(),
            },
        ),
        sqlstate::PERIOD_LOCKED => (
            ErrorCode::PERIOD_LOCKED,
            ErrorDetails::PeriodLocked {
                collection: empty(),
            },
        ),
        sqlstate::STATE_TRANSITION_VIOLATION => (
            ErrorCode::STATE_TRANSITION_VIOLATION,
            ErrorDetails::StateTransitionViolation {
                collection: empty(),
            },
        ),
        sqlstate::TRANSITION_CHECK_VIOLATION => (
            ErrorCode::TRANSITION_CHECK_VIOLATION,
            ErrorDetails::TransitionCheckViolation {
                collection: empty(),
            },
        ),
        sqlstate::RETENTION_VIOLATION => (
            ErrorCode::RETENTION_VIOLATION,
            ErrorDetails::RetentionViolation {
                collection: empty(),
            },
        ),
        sqlstate::LEGAL_HOLD_ACTIVE => (
            ErrorCode::LEGAL_HOLD_ACTIVE,
            ErrorDetails::LegalHoldActive {
                collection: empty(),
            },
        ),
        sqlstate::TYPE_GUARD_VIOLATION => (
            ErrorCode::TYPE_GUARD_VIOLATION,
            ErrorDetails::TypeGuardViolation {
                collection: empty(),
            },
        ),

        // ── Authorization ──
        //
        // The server sends 28000 both for an expired bearer token and for an
        // unauthenticated request; `AuthExpired` covers both as a client error
        // whose remedy is to authenticate again.
        sqlstate::INVALID_AUTHORIZATION => (ErrorCode::AUTH_EXPIRED, ErrorDetails::AuthExpired),
        sqlstate::INSUFFICIENT_PRIVILEGE => (
            ErrorCode::AUTHORIZATION_DENIED,
            ErrorDetails::AuthorizationDenied { resource: empty() },
        ),

        // ── Transaction rollback ──
        //
        // Restores retriability: `WriteConflict` is in `is_retriable`, so a
        // serialization failure once again tells the caller to retry.
        sqlstate::SERIALIZATION_FAILURE => (
            ErrorCode::WRITE_CONFLICT,
            ErrorDetails::WriteConflict {
                collection: empty(),
                document_id: empty(),
            },
        ),

        // ── Syntax error or access rule violation ──
        sqlstate::SYNTAX_ERROR => (ErrorCode::BAD_REQUEST, ErrorDetails::BadRequest),
        sqlstate::CANNOT_COERCE => (
            ErrorCode::TYPE_MISMATCH,
            ErrorDetails::TypeMismatch {
                collection: empty(),
            },
        ),
        sqlstate::UNDEFINED_TABLE => (
            ErrorCode::COLLECTION_NOT_FOUND,
            ErrorDetails::CollectionNotFound {
                collection: empty(),
            },
        ),
        sqlstate::UNDEFINED_FUNCTION => (
            ErrorCode::UNDEFINED_FUNCTION,
            ErrorDetails::UndefinedFunction { name: empty() },
        ),

        // ── Insufficient resources ──
        sqlstate::OUT_OF_MEMORY => (
            ErrorCode::MEMORY_EXHAUSTED,
            ErrorDetails::MemoryExhausted { engine: empty() },
        ),
        sqlstate::TOO_MANY_CONNECTIONS => (
            ErrorCode::RATE_EXCEEDED,
            ErrorDetails::RateExceeded { gate: empty() },
        ),

        // ── Operator intervention ──
        sqlstate::QUERY_CANCELED => (ErrorCode::DEADLINE_EXCEEDED, ErrorDetails::DeadlineExceeded),

        _ => return None,
    };
    Some(pair)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The headline case: the SQLSTATE the server computes for a missing
    /// collection must survive the trip, not collapse into `Internal`.
    #[test]
    fn undefined_table_maps_to_collection_not_found() {
        let e = NodeDbError::from_sqlstate("42P01", "collection 'users' not found");

        assert_eq!(e.code(), ErrorCode::COLLECTION_NOT_FOUND);
        assert!(matches!(
            e.details(),
            ErrorDetails::CollectionNotFound { .. }
        ));
        assert!(e.is_not_found());
        assert!(!e.is_internal());
    }

    /// The message is the only place the failing operand survives, so it is
    /// carried through untouched rather than re-derived from the variant.
    #[test]
    fn message_is_preserved_verbatim_on_a_mapped_code() {
        let e = NodeDbError::from_sqlstate("42P01", "collection 'users' not found");

        assert_eq!(e.message(), "collection 'users' not found");
    }

    /// The non-regression guarantee: a SQLSTATE this mapping does not know
    /// behaves exactly as every server error did before it existed.
    #[test]
    fn unmapped_code_falls_back_to_internal() {
        let mapped = NodeDbError::from_sqlstate("XX000", "boom");
        let baseline = NodeDbError::internal("boom");

        assert_eq!(mapped.code(), ErrorCode::INTERNAL);
        assert!(mapped.is_internal());
        assert_eq!(mapped.message(), baseline.message());
        assert!(matches!(
            mapped.details(),
            ErrorDetails::Internal { component, detail }
                if component == "unspecified" && detail == "boom"
        ));
    }

    /// A code that is real but deliberately unmapped (overloaded across two
    /// variants with different retriability) must take the same fallback.
    #[test]
    fn deliberately_unmapped_code_falls_back_to_internal() {
        assert!(NodeDbError::from_sqlstate("55P03", "no leader").is_internal());
        assert!(NodeDbError::from_sqlstate("57P04", "not leader").is_internal());
    }

    /// Retriability is derived from `ErrorDetails`, so mapping the variant is
    /// what restores it: a serialization failure is retriable again.
    #[test]
    fn serialization_failure_is_retriable_again() {
        let e = NodeDbError::from_sqlstate("40001", "transaction aborted");

        assert_eq!(e.code(), ErrorCode::WRITE_CONFLICT);
        assert!(e.is_retriable());
    }

    /// A deadline is the other genuinely retriable server-side condition.
    #[test]
    fn query_canceled_is_retriable() {
        assert!(
            NodeDbError::from_sqlstate("57014", "query cancelled due to timeout").is_retriable()
        );
    }

    /// A client error must not be reported as retriable, or the caller would
    /// spin on a request that can never succeed.
    #[test]
    fn client_errors_are_not_retriable() {
        for code in ["42P01", "42601", "42883", "42501"] {
            let e = NodeDbError::from_sqlstate(code, "nope");
            assert!(!e.is_retriable(), "{code} must not be retriable");
            assert!(e.is_client_error(), "{code} must be a client error");
        }
    }

    /// Payload fields are left empty rather than filled from the message: the
    /// variant classifies, the message carries the operand.
    #[test]
    fn payload_fields_are_empty_not_guessed() {
        let e = NodeDbError::from_sqlstate("42883", "function no_such_fn(...) does not exist");

        assert!(matches!(
            e.details(),
            ErrorDetails::UndefinedFunction { name } if name.is_empty()
        ));
        assert_eq!(e.message(), "function no_such_fn(...) does not exist");
    }

    /// Every SQLSTATE the mapping claims resolves to a distinct, matching code.
    #[test]
    fn spot_check_mapped_codes() {
        let cases = [
            ("22012", ErrorCode::DIVISION_BY_ZERO),
            ("23505", ErrorCode::CONSTRAINT_VIOLATION),
            ("28000", ErrorCode::AUTH_EXPIRED),
            ("42501", ErrorCode::AUTHORIZATION_DENIED),
            ("42601", ErrorCode::BAD_REQUEST),
            ("42883", ErrorCode::UNDEFINED_FUNCTION),
            ("53200", ErrorCode::MEMORY_EXHAUSTED),
            ("53300", ErrorCode::RATE_EXCEEDED),
            ("57014", ErrorCode::DEADLINE_EXCEEDED),
        ];
        for (sqlstate, expected) in cases {
            assert_eq!(
                NodeDbError::from_sqlstate(sqlstate, "m").code(),
                expected,
                "{sqlstate} mapped to the wrong code"
            );
        }
    }
}
