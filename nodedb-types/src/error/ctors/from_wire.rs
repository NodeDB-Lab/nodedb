// SPDX-License-Identifier: Apache-2.0

//! Reconstruction of a typed [`NodeDbError`] from the numeric code a peer put
//! on the wire.
//!
//! Every other constructor in this directory is used by the side that
//! *detects* a failure and therefore still holds the structured context
//! (which collection, which gate, which document). A client decoding a
//! response frame holds neither: it has a numeric code and the server's
//! rendered message. Rebuilding the [`ErrorDetails`] variant from the code is
//! what makes `is_constraint_violation()`, `is_not_found()`, `is_auth_denied()`
//! and friends answer correctly on the client — without it every remote
//! failure collapses into `internal`, and a duplicate key is indistinguishable
//! from a crashed server.
//!
//! The reverse SQLSTATE mapping is deliberately absent: SQLSTATE is
//! many-to-one (a unique violation and a duplicate idempotency key are both
//! `23505`; every unclassified failure is `XX000`), so it cannot recover a
//! classification. The numeric code is the authoritative one.

use super::super::code::ErrorCode;
use super::super::details::ErrorDetails;
use super::super::types::NodeDbError;

impl NodeDbError {
    /// Rebuild a typed error from a wire-carried numeric `code` plus the
    /// message the originating side rendered.
    ///
    /// The message is preserved verbatim rather than re-derived: it is the
    /// only place the structured context survives (the offending value, the
    /// index name, the gate). String fields on the reconstructed
    /// [`ErrorDetails`] are therefore left empty — the code identifies *what
    /// kind* of failure occurred, which is what the category predicates match
    /// on, while the human-readable specifics stay in `message`. Populating
    /// them by parsing the message back apart would invent structure the wire
    /// never carried.
    ///
    /// Unrecognised codes fall through to [`ErrorDetails::Internal`] tagged
    /// `"remote"`, which is also where a `0` code lands: a peer older than the
    /// numeric-code field sends nothing, and guessing from SQLSTATE would be
    /// worse than admitting the classification is unavailable.
    pub fn from_wire(code: ErrorCode, message: impl Into<String>) -> Self {
        let message = message.into();
        let details = wire_details(code, &message);
        Self {
            code,
            message,
            details,
            cause: None,
        }
    }

    /// Rebuild a typed error from a wire frame that also carries the
    /// structured details the server held.
    ///
    /// The carried details are used when they belong to `code`, so a client
    /// reads the collection, gate, or document the server named. Details of
    /// another category, or none, fall back to [`NodeDbError::from_wire`].
    pub fn from_wire_with_details(
        code: ErrorCode,
        message: impl Into<String>,
        details: Option<ErrorDetails>,
    ) -> Self {
        match details {
            Some(details) if details.code() == code => Self {
                code,
                message: message.into(),
                details,
                cause: None,
            },
            _ => Self::from_wire(code, message),
        }
    }
}

/// Map a numeric code onto the details variant that carries its category,
/// via the shared [`super::super::code_table`] pairing table.
fn wire_details(code: ErrorCode, message: &str) -> ErrorDetails {
    super::super::code_table::details_for_code(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_violation_survives_the_wire() {
        let e = NodeDbError::from_wire(
            ErrorCode::CONSTRAINT_VIOLATION,
            "unique index 'idx_users_email' violation on field '$.email'",
        );
        assert!(e.is_constraint_violation());
        assert_eq!(e.code(), ErrorCode::CONSTRAINT_VIOLATION);
        assert!(e.message().contains("idx_users_email"));
    }

    #[test]
    fn not_found_survives_the_wire() {
        assert!(NodeDbError::from_wire(ErrorCode::DOCUMENT_NOT_FOUND, "not found").is_not_found());
        assert!(
            NodeDbError::from_wire(ErrorCode::COLLECTION_NOT_FOUND, "not found").is_not_found()
        );
    }

    #[test]
    fn auth_and_rate_categories_survive_the_wire() {
        assert!(NodeDbError::from_wire(ErrorCode::AUTHORIZATION_DENIED, "denied").is_auth_denied());
        assert!(NodeDbError::from_wire(ErrorCode::RATE_EXCEEDED, "slow down").is_rate_exceeded());
    }

    #[test]
    fn absent_code_is_internal_not_a_guess() {
        let e = NodeDbError::from_wire(ErrorCode(0), "boom");
        assert!(e.is_internal());
        assert!(e.message().contains("boom"));
    }

    #[test]
    fn previously_unmapped_codes_now_survive_the_wire() {
        for code in [
            ErrorCode::COLLECTION_DEACTIVATED,
            ErrorCode::ARRAY,
            ErrorCode::MOVE_TENANT_DRAIN_TIMEOUT,
            ErrorCode::MIRROR_READ_ONLY,
            ErrorCode::BACKUP_KEY_MISMATCH,
            ErrorCode::HANDSHAKE_FAILED,
            ErrorCode::ENCRYPTION,
            ErrorCode::BRIDGE,
        ] {
            let e = NodeDbError::from_wire(code, "detail");
            assert!(
                !e.is_internal(),
                "{code} should no longer collapse to Internal"
            );
            assert_eq!(e.code(), code);
        }
    }

    #[test]
    fn carried_details_keep_the_collection() {
        let e = NodeDbError::from_wire_with_details(
            ErrorCode::OVERFLOW,
            "increment or decrement would overflow on counters",
            Some(ErrorDetails::Overflow {
                collection: "counters".into(),
            }),
        );
        assert_eq!(
            e.details(),
            &ErrorDetails::Overflow {
                collection: "counters".into()
            }
        );
    }

    #[test]
    fn details_of_another_category_fall_back_to_the_code() {
        let e = NodeDbError::from_wire_with_details(
            ErrorCode::OVERFLOW,
            "overflow",
            Some(ErrorDetails::TypeMismatch {
                collection: "counters".into(),
            }),
        );
        assert_eq!(
            e.details(),
            &ErrorDetails::Overflow {
                collection: String::new()
            }
        );
    }

    #[test]
    fn genuinely_unmapped_codes_still_fall_back_to_internal() {
        assert!(NodeDbError::from_wire(ErrorCode(65000), "x").is_internal());
    }

    #[test]
    fn database_not_found_survives_the_wire() {
        let e =
            NodeDbError::from_wire(ErrorCode::DATABASE_NOT_FOUND, "database 'x' does not exist");
        assert!(matches!(e.details(), ErrorDetails::DatabaseNotFound { .. }));
        assert!(e.is_not_found());
        assert_eq!(e.code(), ErrorCode::DATABASE_NOT_FOUND);
    }
}
