// SPDX-License-Identifier: BUSL-1.1

//! SQLSTATE to HTTP status, for an error that reaches HTTP as a SQLSTATE.
//!
//! A DDL error carries a SQLSTATE and a numeric code. Several DDL SQLSTATEs
//! share one code, so the status follows the SQLSTATE. `to_http` reads this
//! table for every gateway error, so it is the one status table.

use nodedb_types::error::sqlstate;

/// Map a SQLSTATE to an HTTP status.
///
/// - 5xx for an internal or system error, and for an unavailable server
/// - 4xx per class for a request the client must change
/// - 409 for a conflict or a constraint violation
/// - 429 for a rate limit
/// - 501 for an unsupported feature
pub(super) fn sqlstate_to_http_status(state: &str) -> u16 {
    const QUERY_CANCELED: &str = sqlstate::QUERY_CANCELED.0;
    match state {
        sqlstate::INSUFFICIENT_PRIVILEGE => 403,
        sqlstate::UNDEFINED_TABLE => 404,
        // The object already exists: the request conflicts with the catalog.
        "42710" | "42P07" | "42723" => 409,
        // A database or tenant is at its storage quota. Deleting data clears it.
        sqlstate::DISK_FULL => 507,
        sqlstate::TOO_MANY_CONNECTIONS => 429,
        // A configured quota the request exceeds. A retry fails the same way.
        sqlstate::CONFIGURATION_LIMIT_EXCEEDED => 400,
        // No leader or no quorum answered. A retry succeeds later.
        sqlstate::LOCK_NOT_AVAILABLE => 503,
        QUERY_CANCELED => 504,
        _ => class_status(state),
    }
}

/// The status of a SQLSTATE class.
fn class_status(state: &str) -> u16 {
    match state.get(..2).unwrap_or(state) {
        // No data, or an unknown database.
        "02" | "3D" => 404,
        "0A" => 501,
        // Connection failure, insufficient resources, operator intervention.
        "08" | "53" | "57" => 503,
        // Data exception, invalid transaction state, syntax or access rule,
        // program limit.
        "22" | "25" | "42" | "54" => 400,
        // Constraint violation, dependent objects, transaction rollback,
        // object not in prerequisite state.
        "23" | "2B" | "40" | "55" => 409,
        "28" => 401,
        // `XX` internal error, `58` system error, and any other class.
        _ => 500,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_errors_are_server_faults() {
        assert_eq!(sqlstate_to_http_status(sqlstate::INTERNAL_ERROR), 500);
        assert_eq!(sqlstate_to_http_status(sqlstate::IO_ERROR), 500);
    }

    #[test]
    fn feature_not_supported_is_not_implemented() {
        assert_eq!(
            sqlstate_to_http_status(sqlstate::FEATURE_NOT_SUPPORTED),
            501
        );
    }

    #[test]
    fn conflicts_and_constraints_are_409() {
        for state in [
            sqlstate::UNIQUE_VIOLATION,
            sqlstate::NOT_NULL_VIOLATION,
            sqlstate::CHECK_VIOLATION,
            sqlstate::SERIALIZATION_FAILURE,
            sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
            "42P07",
            "2BP01",
        ] {
            assert_eq!(sqlstate_to_http_status(state), 409, "{state}");
        }
    }

    #[test]
    fn rate_limits_are_429() {
        assert_eq!(sqlstate_to_http_status(sqlstate::TOO_MANY_CONNECTIONS), 429);
    }

    #[test]
    fn client_errors_take_their_class_status() {
        assert_eq!(sqlstate_to_http_status(sqlstate::SYNTAX_ERROR), 400);
        assert_eq!(sqlstate_to_http_status(sqlstate::DATA_EXCEPTION), 400);
        assert_eq!(
            sqlstate_to_http_status(sqlstate::INSUFFICIENT_PRIVILEGE),
            403
        );
        assert_eq!(
            sqlstate_to_http_status(sqlstate::INVALID_AUTHORIZATION),
            401
        );
        assert_eq!(sqlstate_to_http_status(sqlstate::UNDEFINED_TABLE), 404);
        assert_eq!(sqlstate_to_http_status(sqlstate::INVALID_CATALOG_NAME), 404);
    }

    #[test]
    fn unavailability_is_503_and_a_deadline_is_504() {
        assert_eq!(sqlstate_to_http_status(sqlstate::SERVER_OVERLOAD), 503);
        assert_eq!(sqlstate_to_http_status(sqlstate::LOCK_NOT_AVAILABLE), 503);
        assert_eq!(sqlstate_to_http_status(sqlstate::QUERY_CANCELED.0), 504);
    }
}
