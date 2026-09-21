// SPDX-License-Identifier: BUSL-1.1

//! HTTP error shape: `(status_code, message)`.

use super::gateway_map::GatewayErrorMap;
use super::remote_code::remote_code_to_http_status;
use crate::Error;

impl GatewayErrorMap {
    /// Map a gateway error into `(http_status_code, message)` for HTTP.
    ///
    /// Uses standard HTTP status semantics:
    /// - 400 Bad Request for client-side errors (bad SQL, not found)
    /// - 403 Forbidden for authz errors
    /// - 409 Conflict for write-conflict / constraint violations
    /// - 503 Service Unavailable for routing/leader errors
    /// - 504 Gateway Timeout for deadline exceeded
    /// - 500 Internal Server Error as the default fallback
    pub fn to_http(err: &Error) -> (u16, String) {
        match err {
            Error::NotLeader { leader_addr, .. } => (
                503,
                format!("cluster in leader election; leader hint: {leader_addr}"),
            ),
            Error::DeadlineExceeded { .. } => (504, err.to_string()),
            Error::RetryableSchemaChanged { .. } => (503, err.to_string()),
            Error::CollectionNotFound { collection, .. } => {
                (404, format!("collection \"{collection}\" does not exist"))
            }
            Error::RejectedAuthz { .. } => (403, err.to_string()),
            Error::BadRequest { detail } => (400, detail.clone()),
            Error::PlanError { detail } => (400, detail.clone()),
            Error::RejectedConstraint { detail, .. } => (409, detail.clone()),
            Error::NoLeader { .. } => (503, err.to_string()),
            Error::Serialization { .. } | Error::Codec { .. } => (500, err.to_string()),
            Error::Internal { .. } => (500, err.to_string()),
            // 501 Not Implemented: a valid op refused because cross-core
            // source-shipping is not yet supported (fail-closed safety floor).
            Error::CrossCollectionNotColocated { .. } => (501, err.to_string()),
            Error::RemoteTyped { code, message } => {
                (remote_code_to_http_status(*code), message.clone())
            }
            Error::DataPlane(_) => {
                let public = crate::error_classify::classify(err);
                (
                    remote_code_to_http_status(public.code()),
                    public.message().to_owned(),
                )
            }
            _ => (500, err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_fixtures::{authz, deadline, internal, not_found, not_leader};
    use super::*;

    #[test]
    fn http_not_leader() {
        let (status, _) = GatewayErrorMap::to_http(&not_leader());
        assert_eq!(status, 503);
    }

    #[test]
    fn http_deadline() {
        let (status, _) = GatewayErrorMap::to_http(&deadline());
        assert_eq!(status, 504);
    }

    #[test]
    fn http_not_found() {
        let (status, _) = GatewayErrorMap::to_http(&not_found());
        assert_eq!(status, 404);
    }

    #[test]
    fn http_authz() {
        let (status, _) = GatewayErrorMap::to_http(&authz());
        assert_eq!(status, 403);
    }

    #[test]
    fn http_internal() {
        let (status, _) = GatewayErrorMap::to_http(&internal());
        assert_eq!(status, 500);
    }

    #[test]
    fn to_http_remote_typed_is_wired_to_helper() {
        use nodedb_types::error::ErrorCode;
        let err = Error::RemoteTyped {
            code: ErrorCode::AUTHORIZATION_DENIED,
            message: "remote denied write".into(),
        };
        let (status, msg) = GatewayErrorMap::to_http(&err);
        assert_eq!(status, 403);
        assert_eq!(msg, "remote denied write");
    }
}
