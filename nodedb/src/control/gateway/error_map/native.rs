// SPDX-License-Identifier: BUSL-1.1

//! Native-protocol error shape: `(numeric code, message)`.

use super::gateway_map::GatewayErrorMap;
use crate::Error;

/// Error code constants (subset matching `nodedb_types` numeric codes).
const CODE_NOT_LEADER: u32 = 10;
const CODE_DEADLINE: u32 = 20;
const CODE_SCHEMA_CHANGED: u32 = 30;
const CODE_NOT_FOUND: u32 = 40;
const CODE_AUTHZ: u32 = 50;
const CODE_BAD_REQUEST: u32 = 60;
const CODE_CONSTRAINT: u32 = 70;
const CODE_INTERNAL: u32 = 99;

impl GatewayErrorMap {
    /// Map a gateway error into `(code, message)` for the native protocol.
    ///
    /// Error codes are aligned with `nodedb_types::error::ErrorCode` numeric
    /// values so native clients can switch on the code without string matching.
    pub fn to_native(err: &Error) -> (u32, String) {
        match err {
            Error::NotLeader { leader_addr, .. } => {
                (CODE_NOT_LEADER, format!("not leader; hint: {leader_addr}"))
            }
            Error::DeadlineExceeded { .. } => (CODE_DEADLINE, err.to_string()),
            Error::RetryableSchemaChanged { .. } => (CODE_SCHEMA_CHANGED, err.to_string()),
            Error::CollectionNotFound { collection, .. } => (
                CODE_NOT_FOUND,
                format!("collection \"{collection}\" not found"),
            ),
            Error::RejectedAuthz { .. } => (CODE_AUTHZ, err.to_string()),
            Error::BadRequest { detail } | Error::PlanError { detail } => {
                (CODE_BAD_REQUEST, detail.clone())
            }
            Error::RejectedConstraint { detail, .. } => (CODE_CONSTRAINT, detail.clone()),
            Error::CrossCollectionNotColocated { .. } => (CODE_BAD_REQUEST, err.to_string()),
            Error::RemoteTyped { code, message } => {
                use nodedb_types::error::ErrorCode as Ec;
                let native_code = match *code {
                    Ec::DEADLINE_EXCEEDED => CODE_DEADLINE,
                    Ec::COLLECTION_NOT_FOUND => CODE_NOT_FOUND,
                    Ec::AUTHORIZATION_DENIED => CODE_AUTHZ,
                    Ec::BAD_REQUEST | Ec::PLAN_ERROR => CODE_BAD_REQUEST,
                    Ec::CONSTRAINT_VIOLATION => CODE_CONSTRAINT,
                    _ => CODE_INTERNAL,
                };
                (native_code, message.clone())
            }
            _ => (CODE_INTERNAL, err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_fixtures::{
        authz, deadline, internal, not_found, not_leader, schema_changed,
    };
    use super::*;

    #[test]
    fn native_not_leader() {
        let (code, msg) = GatewayErrorMap::to_native(&not_leader());
        assert_eq!(code, 10);
        assert!(msg.contains("hint:"));
    }

    #[test]
    fn native_deadline() {
        let (code, _) = GatewayErrorMap::to_native(&deadline());
        assert_eq!(code, 20);
    }

    #[test]
    fn native_schema_changed() {
        let (code, _) = GatewayErrorMap::to_native(&schema_changed());
        assert_eq!(code, 30);
    }

    #[test]
    fn native_not_found() {
        let (code, _) = GatewayErrorMap::to_native(&not_found());
        assert_eq!(code, 40);
    }

    #[test]
    fn native_authz() {
        let (code, _) = GatewayErrorMap::to_native(&authz());
        assert_eq!(code, 50);
    }

    #[test]
    fn native_internal() {
        let (code, _) = GatewayErrorMap::to_native(&internal());
        assert_eq!(code, 99);
    }
}
